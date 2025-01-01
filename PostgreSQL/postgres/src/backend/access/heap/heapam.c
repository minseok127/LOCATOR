/*-------------------------------------------------------------------------
 *
 * heapam.c
 *	  heap access method code
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam.c
 *
 *
 * INTERFACE ROUTINES
 *		heap_beginscan	- begin relation scan
 *		heap_rescan		- restart a relation scan
 *		heap_endscan	- end relation scan
 *		heap_getnext	- retrieve next tuple in scan
 *		heap_fetch		- retrieve tuple with given tid
 *		heap_insert		- insert tuple into a relation
 *		heap_multi_insert - insert multiple tuples into a relation
 *		heap_delete		- delete a tuple from a relation
 *		heap_update		- replace a tuple in a relation with another tuple
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/heaptoast.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/subtrans.h"
#include "access/syncscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"

#ifdef DIVA
#include "access/ebiam.h"
#include "storage/pleaf.h"
#include "storage/ebi_tree_buf.h"
#include "storage/ebi_sub_buf.h"
#include "storage/pleaf_bufpage.h"
#include "utils/memutils.h"
#include "utils/dynahash.h"

#ifdef LOCATOR
#include <math.h>
#include <liburing.h>
#include "storage/readbuf.h"
#include "utils/resowner_private.h"
#include "pg_refcnt.h"
#include "postmaster/locator_partition_mgr.h"
#include "locator/locator_executor.h"
#include "locator/locator_external_catalog.h"
#include "locator/locator_mempart_buf.h"
#include "locator/locator_partitioning.h"
#include "locator/locator.h"
#endif /* LOCATOR */

#ifdef IO_AMOUNT
extern uint32 read_nblocks;
extern uint32 reduced_by_HP;
extern uint32 reduced_by_VP;
#endif /* IO_AMOUNT */

/*
 * Tricky variable for passing the latest removed xid from
 * heap_hot_search_buffer_with_vc() to heap_index_delete_tuples().
 */
extern TransactionId copied_latestRemovedXid;

/* Tricky variable for passing the cmd type from ExecutePlan to heap code */
CmdType curr_cmdtype;
#endif /* DIVA */

static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup,
									 TransactionId xid, CommandId cid, int options);
static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf,
								  Buffer newbuf, HeapTuple oldtup,
								  HeapTuple newtup, HeapTuple old_key_tuple,
								  bool all_visible_cleared, bool new_all_visible_cleared);
static Bitmapset *HeapDetermineColumnsInfo(Relation relation,
										   Bitmapset *interesting_cols,
										   Bitmapset *external_cols,
										   HeapTuple oldtup, HeapTuple newtup,
										   bool *has_external);
static bool heap_acquire_tuplock(Relation relation, ItemPointer tid,
								 LockTupleMode mode, LockWaitPolicy wait_policy,
								 bool *have_tuple_lock);
static void compute_new_xmax_infomask(TransactionId xmax, uint16 old_infomask,
									  uint16 old_infomask2, TransactionId add_to_xmax,
									  LockTupleMode mode, bool is_update,
									  TransactionId *result_xmax, uint16 *result_infomask,
									  uint16 *result_infomask2);
static TM_Result heap_lock_updated_tuple(Relation rel, HeapTuple tuple,
										 ItemPointer ctid, TransactionId xid,
										 LockTupleMode mode);
static void GetMultiXactIdHintBits(MultiXactId multi, uint16 *new_infomask,
								   uint16 *new_infomask2);
static TransactionId MultiXactIdGetUpdateXid(TransactionId xmax,
											 uint16 t_infomask);
static bool DoesMultiXactIdConflict(MultiXactId multi, uint16 infomask,
									LockTupleMode lockmode, bool *current_is_member);
static void MultiXactIdWait(MultiXactId multi, MultiXactStatus status, uint16 infomask,
							Relation rel, ItemPointer ctid, XLTW_Oper oper,
							int *remaining);
static bool ConditionalMultiXactIdWait(MultiXactId multi, MultiXactStatus status,
									   uint16 infomask, Relation rel, int *remaining);
static void index_delete_sort(TM_IndexDeleteOp *delstate);
static int	bottomup_sort_and_shrink(TM_IndexDeleteOp *delstate);
static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup);
static HeapTuple ExtractReplicaIdentity(Relation rel, HeapTuple tup, bool key_required,
										bool *copy);

#ifdef DIVA
#define FetchTupleHeaderInfo(oldtup, tmpTupHeader) \
	do { \
		__sync_lock_test_and_set((uint64*)(&(oldtup->t_data->t_choice.t_heap.t_xmax)), \
							   *((uint64*)(&(tmpTupHeader.t_choice.t_heap.t_xmax)))); \
		__sync_lock_test_and_set((uint32*)(&(oldtup->t_data->t_infomask2)), \
							   *((uint32*)(&(tmpTupHeader.t_infomask2)))); \
	} while(0)
#endif /* DIVA */

/*
 * Each tuple lock mode has a corresponding heavyweight lock, and one or two
 * corresponding MultiXactStatuses (one to merely lock tuples, another one to
 * update them).  This table (and the macros below) helps us determine the
 * heavyweight lock mode and MultiXactStatus values to use for any particular
 * tuple lock strength.
 *
 * Don't look at lockstatus/updstatus directly!  Use get_mxact_status_for_lock
 * instead.
 */
static const struct
{
	LOCKMODE	hwlock;
	int			lockstatus;
	int			updstatus;
}

			tupleLockExtraInfo[MaxLockTupleMode + 1] =
{
	{							/* LockTupleKeyShare */
		AccessShareLock,
		MultiXactStatusForKeyShare,
		-1						/* KeyShare does not allow updating tuples */
	},
	{							/* LockTupleShare */
		RowShareLock,
		MultiXactStatusForShare,
		-1						/* Share does not allow updating tuples */
	},
	{							/* LockTupleNoKeyExclusive */
		ExclusiveLock,
		MultiXactStatusForNoKeyUpdate,
		MultiXactStatusNoKeyUpdate
	},
	{							/* LockTupleExclusive */
		AccessExclusiveLock,
		MultiXactStatusForUpdate,
		MultiXactStatusUpdate
	}
};

/* Get the LOCKMODE for a given MultiXactStatus */
#define LOCKMODE_from_mxstatus(status) \
			(tupleLockExtraInfo[TUPLOCK_from_mxstatus((status))].hwlock)

/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LockTupleTuplock(rel, tup, mode) \
	LockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define UnlockTupleTuplock(rel, tup, mode) \
	UnlockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleTuplock(rel, tup, mode) \
	ConditionalLockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)

#ifdef USE_PREFETCH
/*
 * heap_index_delete_tuples and index_delete_prefetch_buffer use this
 * structure to coordinate prefetching activity
 */
typedef struct
{
	BlockNumber cur_hblkno;
	int			next_item;
	int			ndeltids;
	TM_IndexDelete *deltids;
} IndexDeletePrefetchState;
#endif

/* heap_index_delete_tuples bottom-up index deletion costing constants */
#define BOTTOMUP_MAX_NBLOCKS			6
#define BOTTOMUP_TOLERANCE_NBLOCKS		3

/*
 * heap_index_delete_tuples uses this when determining which heap blocks it
 * must visit to help its bottom-up index deletion caller
 */
typedef struct IndexDeleteCounts
{
	int16		npromisingtids; /* Number of "promising" TIDs in group */
	int16		ntids;			/* Number of TIDs in group */
	int16		ifirsttid;		/* Offset to group's first deltid */
} IndexDeleteCounts;

/*
 * This table maps tuple lock strength values for each particular
 * MultiXactStatus value.
 */
static const int MultiXactStatusLock[MaxMultiXactStatus + 1] =
{
	LockTupleKeyShare,			/* ForKeyShare */
	LockTupleShare,				/* ForShare */
	LockTupleNoKeyExclusive,	/* ForNoKeyUpdate */
	LockTupleExclusive,			/* ForUpdate */
	LockTupleNoKeyExclusive,	/* NoKeyUpdate */
	LockTupleExclusive			/* Update */
};

/* Get the LockTupleMode for a given MultiXactStatus */
#define TUPLOCK_from_mxstatus(status) \
			(MultiXactStatusLock[(status)])

#ifdef LOCATOR
/*
 * is_prefetching_needed
 *		We check whether scan with prefetching is suitable or not.
 */
bool
is_prefetching_needed(Relation relation, HeapScanDesc scan)
{
	/* We use only tables with SIRO versioning. */
	if (!IsSiro(relation))
		return false;

	/* We don't use prefetching with parallel scan. */
	if (scan->rs_base.rs_parallel)
		return false;

	/* We use prefetching for sequential scan or anaylze. */
	if ((scan->rs_base.rs_flags & SO_TYPE_SEQSCAN) == 0 &&
			(scan->rs_base.rs_flags & SO_TYPE_ANALYZE) == 0)
		return false;

	/* We don't use prefetching with non-pagemode. */
	if ((scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE) == 0)
		return false;

	/* If total page # of the relation is so small, we don't use. */
	if (scan->rs_nblocks < PREFETCH_PAGE_COUNT)
		return false;
	
	/* Ok, we can. */
	return true;
}

/*
 * Allocate and initialize prefetch descriptor.
 */
PrefetchDesc
get_prefetch_desc(Relation relation, Size size)
{
	PrefetchDesc prefetch_desc = NULL;
	struct io_uring *ring;
	int res;

	/* Allocate io uring to cur transaction memory context */
	ring = (struct io_uring *) palloc0(sizeof(struct io_uring));
 
	/* TODO: We set io_uring's queue size as PREFETCH_PAGE_COUNT*2
	 * simply. We should change later to cover ebi scan's prefetch size. 
	 */

	/* Init io_uring queue */
	res = io_uring_queue_init(PREFETCH_PAGE_COUNT * 2, ring,
							  IORING_SETUP_COOP_TASKRUN);

	if (unlikely(res < 0))
	{
		elog(WARNING, "io_uring_queue_init failed, errno: %d (SeqScan will use the buffer pool)", -res);
		pfree(ring);
	}
	else
	{
		prefetch_desc = (PrefetchDesc) palloc0(size);
		prefetch_desc->ring = ring;

		prefetch_desc->prefetch_end = false;
		prefetch_desc->requested_io_num = 0;
		prefetch_desc->complete_io_num = 0;

		prefetch_desc->io_req_free = NULL;
		prefetch_desc->io_req_complete_head = NULL;
		prefetch_desc->io_req_complete_tail = NULL;

		/* 
		 * Init i/o request freelist 
		 * TODO: need to adjust prefetch size dynamically
		 */
		for (int i = 0; i < 2; ++i)
		{
			ReadBufDesc readbuf_desc = PopReadBufDesc();

			/*
			 * TODO: fix logic 
			 * We just do panic, if there is no available ReadBufDesc. 
			 * For generality, we need to fix logic later.
			 */
			Assert(readbuf_desc != NULL);

			/* append to io_req_free */
			readbuf_desc->next = prefetch_desc->io_req_free;
			prefetch_desc->io_req_free = readbuf_desc;
		}

		ResourceOwnerEnlargeIoUrings(CurrentResourceOwner);
		ResourceOwnerRememberIoUring(CurrentResourceOwner, prefetch_desc->ring);
	}

	return prefetch_desc;
}

PrefetchDesc
get_locator_prefetch_desc(Relation relation, LocatorScanDesc lscan)
{
	PrefetchPartitionDesc partition_desc = NULL;
	PrefetchDesc prefetch_desc = NULL;
	char *path;
	RelFileNode node = relation->rd_node;
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(node.relNode);
	LocatorPartInfo part_info;
	MemoryContext oldcxt;
 
	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	prefetch_desc = get_prefetch_desc(relation, sizeof(struct PrefetchPartitionDescData));

	if (likely(prefetch_desc != NULL))
	{
		partition_desc = (PrefetchPartitionDesc)prefetch_desc;

		/* Copy locator scan state */
		partition_desc->level_cnt = relation->rd_locator_level_count;
		partition_desc->part_infos_to_prefetch = lscan->part_infos_to_scan;
		partition_desc->c_level = lscan->c_level;
		partition_desc->c_lc = lscan->c_lc;
		partition_desc->c_block_cnt = lscan->c_part_info->block_cnt;
		
		partition_desc->is_columnar_layout = false;
		partition_desc->locator_level_desc = NULL;
		partition_desc->locator_group_desc = NULL;
		partition_desc->locator_last_blocknum = InvalidBlockNumber;
		partition_desc->locator_current_block_idx_in_record_zone = 0;
		partition_desc->locator_current_record_zone_id = 0;

		/* Get file name */
		part_info = lscan->c_part_info;
		path = psprintf("base/%u/%u.%u.%u.%u",
						 node.dbNode, node.relNode,
						 partition_desc->c_level,
						 part_info->partNum,
						 part_info->partGen);

		if ((partition_desc->c_fd = LocatorSimpleOpen(path, O_RDWR)) == -1)
		{
#ifdef ABORT_AT_FAIL
			Abort("get_locator_prefetch_desc, failed to open locator partition file");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file during prefetching, name: %s",
									path));
#endif
		}
		pfree(path);

		prefetch_desc->next_reqblock = lscan->c_block;

		partition_desc->locator_columnar_start_level = UINT_MAX;
		if (exCatalog != NULL)
		{
			for (int level = 0; level <= exCatalog->lastPartitionLevel; level++)
			{
				uint8 layout_id = exCatalog->layoutArray[level];

				if (layout_id != ROW_ORIENTED)
				{
					partition_desc->locator_columnar_start_level = level;
					break;
				}
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);

	return prefetch_desc;
}

PrefetchDesc
get_heap_prefetch_desc(Relation relation, HeapScanDesc scan)
{
	PrefetchHeapDesc heap_desc = NULL;
	PrefetchDesc prefetch_desc = NULL;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	prefetch_desc = get_prefetch_desc(relation, sizeof(struct PrefetchHeapDescData));

	if (likely(prefetch_desc != NULL))
	{

		heap_desc = (PrefetchHeapDesc)prefetch_desc;
 
		/* Copy heap scan state */
 		heap_desc->rs_nblocks = scan->rs_nblocks;
 		heap_desc->start_block = scan->rs_startblock;
						
		/* Set prefetch state */
 		heap_desc->is_startblock_fetched = false;
 		heap_desc->prefetch_size = PREFETCH_PAGE_COUNT;

 		prefetch_desc->next_reqblock = scan->rs_startblock;
	}

	MemoryContextSwitchTo(oldcxt);

	return prefetch_desc;
}

/*
 * Release all resources of the given prefetch descriptor.
 */
void
release_prefetch_desc(PrefetchDesc prefetch_desc)
{
	ReadBufDesc readbuf_desc;

	/* Release prefetched buffer pages */
	ReleasePrefetchedBuffers(prefetch_desc);

	/* free request freelist */
	readbuf_desc = prefetch_desc->io_req_free; 
	while (readbuf_desc != NULL)
	{
		ReadBufDesc next_readbuf_desc;
		
		next_readbuf_desc = readbuf_desc->next;

		/* return to global area */
		PushReadBufDesc(readbuf_desc);

		readbuf_desc = next_readbuf_desc;	
	}

	/* free request freelist */
	readbuf_desc = prefetch_desc->io_req_complete_head; 
	while (readbuf_desc != NULL)
	{
		ReadBufDesc next_readbuf_desc;
		
		next_readbuf_desc = readbuf_desc->next;

		/* return to global area */
		PushReadBufDesc(readbuf_desc);

		readbuf_desc = next_readbuf_desc;	
	}

	/* exit io_uring queue */
	io_uring_queue_exit(prefetch_desc->ring);

	ResourceOwnerForgetIoUring(CurrentResourceOwner, prefetch_desc->ring);

	/* free resources */
	pfree(prefetch_desc->ring);
}
#endif /* LOCATOR */

#ifdef DIVA

static bool
tuple_size_available_for_indexscan(IndexFetchHeapData *scan, HeapTuple tuple)
{
	if (scan->xs_vistuple_free &&
				scan->xs_vistuple_size >= HEAPTUPLESIZE + tuple->t_len)
		return true;

	return false;
}

static void
remove_redundant_tuple_for_indexscan(IndexFetchHeapData *scan)
{
	if (scan->xs_vistuple_free)
	{
		heap_freetuple(scan->xs_vistuple_free);
		scan->xs_vistuple_free = NULL;
	}

	scan->xs_vistuple_size = 0;
}

static bool
tuple_size_available(HeapScanDesc scan, Index idx, HeapTuple tuple)
{
	if ((scan->rs_vistuples_copied[idx] || 
			scan->rs_vistuples_freearray[idx]) && 
				scan->rs_vistuples_size[idx] >= HEAPTUPLESIZE + tuple->t_len)
		return true;

	return false;
}

static void
remove_redundant_tuple(HeapScanDesc scan, Index idx)
{
	if (scan->rs_vistuples_freearray[idx])
	{
		heap_freetuple(scan->rs_vistuples_freearray[idx]);
		scan->rs_vistuples_freearray[idx] = NULL;
	}

	if (scan->rs_vistuples_copied[idx])
	{
		heap_freetuple(scan->rs_vistuples_copied[idx]);
		scan->rs_vistuples_copied[idx] = NULL;
	}

	scan->rs_vistuples_size[idx] = 0;
}
#endif /* DIVA */

/* ----------------------------------------------------------------
 *						 heap support routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		initscan - scan code common to heap_beginscan and heap_rescan
 * ----------------
 */
static void
initscan(HeapScanDesc scan, ScanKey key, bool keep_startblock)
{
	ParallelBlockTableScanDesc bpscan = NULL;
	bool		allow_strat;
	bool		allow_sync;

	/*
	 * Determine the number of blocks we have to scan.
	 *
	 * It is sufficient to do this once at scan start, since any tuples added
	 * while the scan is in progress will be invisible to my snapshot anyway.
	 * (That is not true when using a non-MVCC snapshot.  However, we couldn't
	 * guarantee to return tuples added after scan start anyway, since they
	 * might go into pages we already scanned.  To guarantee consistent
	 * results for a non-MVCC snapshot, the caller must hold some higher-level
	 * lock that ensures the interesting tuple(s) won't change.)
	 */
	if (scan->rs_base.rs_parallel != NULL)
	{
		bpscan = (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
		scan->rs_nblocks = bpscan->phs_nblocks;
	}
	else
		scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_base.rs_rd);

	/*
	 * If the table is large relative to NBuffers, use a bulk-read access
	 * strategy and enable synchronized scanning (see syncscan.c).  Although
	 * the thresholds for these features could be different, we make them the
	 * same so that there are only two behaviors to tune rather than four.
	 * (However, some callers need to be able to disable one or both of these
	 * behaviors, independently of the size of the table; also there is a GUC
	 * variable that can disable synchronized scanning.)
	 *
	 * Note that table_block_parallelscan_initialize has a very similar test;
	 * if you change this, consider changing that one, too.
	 */
	if (!RelationUsesLocalBuffers(scan->rs_base.rs_rd) &&
		scan->rs_nblocks > NBuffers / 4)
	{
		allow_strat = (scan->rs_base.rs_flags & SO_ALLOW_STRAT) != 0;
		allow_sync = (scan->rs_base.rs_flags & SO_ALLOW_SYNC) != 0;
	}
	else
		allow_strat = allow_sync = false;

	if (allow_strat)
	{
		/* During a rescan, keep the previous strategy object. */
		if (scan->rs_strategy == NULL)
			scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
	}
	else
	{
		if (scan->rs_strategy != NULL)
			FreeAccessStrategy(scan->rs_strategy);
		scan->rs_strategy = NULL;
	}

	if (scan->rs_base.rs_parallel != NULL)
	{
		/* For parallel scan, believe whatever ParallelTableScanDesc says. */
		if (scan->rs_base.rs_parallel->phs_syncscan)
			scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
	}
	else if (keep_startblock)
	{
		/*
		 * When rescanning, we want to keep the previous startblock setting,
		 * so that rewinding a cursor doesn't generate surprising results.
		 * Reset the active syncscan setting, though.
		 */
		if (allow_sync && synchronize_seqscans)
			scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
	}
	else if (allow_sync && synchronize_seqscans)
	{
		scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		scan->rs_startblock = ss_get_location(scan->rs_base.rs_rd, scan->rs_nblocks);
	}
	else
	{
		scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
		scan->rs_startblock = 0;
	}

	scan->rs_numblocks = InvalidBlockNumber;
	scan->rs_inited = false;
	scan->rs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->rs_ctup.t_self);
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_cblock = InvalidBlockNumber;

	/* page-at-a-time fields are always invalid when not rs_inited */

	/*
	 * copy the scan key, if appropriate
	 */
	if (key != NULL && scan->rs_base.rs_nkeys > 0)
		memcpy(scan->rs_base.rs_key, key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData));

	/*
	 * Currently, we only have a stats counter for sequential heap scans (but
	 * e.g for bitmap scans the underlying bitmap index scans will be counted,
	 * and for sample scans we update stats for tuple fetches).
	 */
	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);

#ifdef DIVA
	/* Init values */
	scan->c_buf_id = InvalidEbiSubBuf;
	scan->rs_base.is_heapscan = true; 
	scan->is_first_ebi_scan = true;
	scan->unobtained_segments = NULL;
	scan->unobtained_segments_capacity = 0;
	scan->unobtained_segment_nums = 0;
	scan->unobtained_version_offsets = NULL;
	scan->unobtained_versions_capacity = 0;
	scan->unobtained_version_nums = 0;
	scan->rs_base.counter_in_heap = 0;
	scan->rs_base.counter_in_ebi = 0;
	scan->rs_base.counter_duplicates = 0;
	scan->rs_ctup_copied = NULL; 	/* Initialize the current copied tuple */

	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
		scan->rs_vistuples_copied[i] = NULL;

#ifdef LOCATOR
	if (enable_prefetch && is_prefetching_needed(scan->rs_base.rs_rd, scan))
	{
		scan->rs_base.rs_flags &= ~SO_ALLOW_STRAT;
		scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
	}
#endif /* LOCATOR */
#endif /* DIVA */
}

/*
 * heap_setscanlimits - restrict range of a heapscan
 *
 * startBlk is the page to start at
 * numBlks is number of pages to scan (InvalidBlockNumber means "all")
 */
void
heap_setscanlimits(TableScanDesc sscan, BlockNumber startBlk, BlockNumber numBlks)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	Assert(!scan->rs_inited);	/* else too late to change */
	/* else rs_startblock is significant */
	Assert(!(scan->rs_base.rs_flags & SO_ALLOW_SYNC));

	/* Check startBlk is valid (but allow case of zero blocks...) */
	Assert(startBlk == 0 || startBlk < scan->rs_nblocks);

	scan->rs_startblock = startBlk;
	scan->rs_numblocks = numBlks;
}

#ifdef LOCATOR
/*
 * 	SetPageRefUnit - increment reference count of page
 * 
 * This routine avoid the conflict with INSERT and increment reference count of
 * page.
 * 
 * Note: This routine stores the incremented reference count in page_ref_unit. 
 * The caller of this routine "must" decrement the reference count using that
 * page_ref_unit after the operation is finished.
 */
void
SetPageRefUnit(DualRefDesc drefDesc)
{
	uint64 oldval;
	uint64 ref_unit;

retry:
	/* If INSERT is blocking readers, wait for the writer */
	DRefWaitForINSERT(drefDesc->dual_ref, oldval);

	/* Increment reference count of the page */
	ref_unit = DRefIsToggled(oldval) ? DRefToggleOnUnit : DRefToggleOffUnit;

	/* If conflict occurs, decrement reference count and try again */
	if (unlikely(DRefGetState(DRefIncrRefCnt(drefDesc->dual_ref, ref_unit)) ==
				 DRefStateINSERT))
	{
		DRefDecrRefCnt(drefDesc->dual_ref, ref_unit);
		goto retry;
	}

	/* Set toggle bit and page reference count unit */
	drefDesc->prev_toggle_bit = DRefGetToggleBit(oldval);
	drefDesc->page_ref_unit = ref_unit;
}

/*
 * 	GetCheckVar - increment reference count of target record and get check_var
 * 
 * This routine gets check_var, which is information about what to check between
 * left version and right version(or, both).
 * 
 * Before checking the confilict with UPDATE/DELETE, this routine first check if
 * dual_ref was toggled. If it was, switch the reference count of page first and
 * check the conflict then.
 * (The explanation of why to switch is written as a comment in the function.)
 * 
 * Note: This routine stores the incremented reference count in rec_ref_unit. 
 * The caller of this routine "must" decrement the reference count using that
 * rec_ref_unit after the operation is finished.
 */
void
GetCheckVar(DualRefDesc drefDesc, uint64 *rec_ref_unit, uint8 *check_var,
			bool *set_hint_bits, OffsetNumber offnum)
{
	uint64	oldval;
	uint64	tmpval;
	uint64	add_unit;
	uint64	tmp_target_bits;
	uint64	tmp_ref_unit;
	uint8	tmp_check_var;
	bool	is_update;

	/* Set to default value */
	*rec_ref_unit = 0;
	*check_var = CHECK_BOTH;
	*set_hint_bits = true;

	/* If this is prefetching, skip reference counting */
	if (drefDesc->dual_ref == NULL)
	{
		return;
	}

	/* Init and read dual_ref value */
	add_unit = 0;
	is_update = false;
	oldval = DRefGetVal(drefDesc->dual_ref);

	/* Check if dual_ref was toggled */
	if ((tmpval = DRefGetToggleBit(oldval)) != drefDesc->prev_toggle_bit)
	{
		/*
		 * If dual_ref was toggled, switch reference count.
		 * 
		 * A writer transaction that UPDATE/DELETE a record toggles the bit
		 * while setting the offset number of the target record and the type of
		 * operation.
		 * 
		 * Readers who were reading the record before the writer transaction
		 * toggled the bits would not realize that the writer transaction is
		 * about to perform an UPDATE/DELETE on the page, so they would not do
		 * anything to avoid conflicts with the writer (counting references to
		 * the target record, not calling SetHintBits()).
		 * 
		 * Therefore, the writer transaction had to wait for the readers to
		 * finish reading the record they were reading at the time the bit was
		 * toggled.
		 * 
		 * This can be implemented simply by letting the readers check the
		 * toggle bit and switch the reference count each time they read the
		 * record, and letting the writer transaction wait until the reference
		 * count of the area before the bit was toggled is 0.
		 */
		if (tmpval == 0)
		{
			/* Switch reference count (on -> off) */
			Assert(drefDesc->page_ref_unit == DRefToggleOnUnit);
			add_unit = -DRefSwitchUnit;
			drefDesc->page_ref_unit = DRefToggleOffUnit;
		}
		else
		{
			/* Switch reference count (off -> on) */
			Assert(drefDesc->page_ref_unit == DRefToggleOffUnit);
			add_unit = DRefSwitchUnit;
			drefDesc->page_ref_unit = DRefToggleOnUnit;
		}
		
		/* And update previous value */
		drefDesc->prev_toggle_bit = tmpval;
		Assert((oldval & ~DRefPageRefCnt) == ((oldval + add_unit) & ~DRefPageRefCnt));
	}

	/* Check if this record is target */
	if (((OffsetNumber)DRefGetTargetOffnum(oldval)) == offnum)
	{
		tmpval = DRefGetState(oldval);

		Assert(tmpval == DRefStateUPDATE || tmpval == DRefStateDELETE);

		/*
		 * If current record is target, check whether current modification is
		 * UPDATE or DELETE.
		 * If it is UPDATE, set reference count and check_var.
		 * Or it is DELETE, just block SetHintBits().
		 */
		if (tmpval == DRefStateUPDATE)
		{
			is_update = true;

			switch (tmp_target_bits = DRefGetTargetBits(oldval))
			{
				/* Conflict with writer occurs */
				case DRefTargetLeftBit:
					tmp_check_var = CHECK_RIGHT;
					tmp_ref_unit = DRefTargetRightUnit;
					*set_hint_bits = false;
					break;
				case DRefTargetRightBit:
					tmp_check_var = CHECK_LEFT;
					tmp_ref_unit = DRefTargetLeftUnit;
					*set_hint_bits = false;
					break;
				
				/* Conflict doesn't occurs*/
				default:
					Assert(tmp_target_bits == 0);
					tmp_check_var = CHECK_BOTH;
					tmp_ref_unit = DRefTargetBothUnit;
					break;
			}

			add_unit += tmp_ref_unit;
		}
		else
			*set_hint_bits = false;
	}

	/* If there is any info to update at dual_ref, adjust that info */
	if (add_unit != 0)
	{
		tmpval = DRefIncrRefCnt(drefDesc->dual_ref, add_unit);
	
		if (is_update)
		{
			/* Conflict with writer occurs */
			if (tmp_target_bits != DRefGetTargetBits(tmpval) &&
				tmp_check_var == CHECK_BOTH)
			{
				switch (DRefGetTargetBits(tmpval))
				{
					case DRefTargetLeftBit:
						DRefDecrRefCnt(drefDesc->dual_ref, DRefTargetLeftUnit);
						tmp_check_var = CHECK_RIGHT;
						tmp_ref_unit = DRefTargetRightUnit;
						break;
					case DRefTargetRightBit:
						DRefDecrRefCnt(drefDesc->dual_ref, DRefTargetRightUnit);
						tmp_check_var = CHECK_LEFT;
						tmp_ref_unit = DRefTargetLeftUnit;
						break;
				}
				*set_hint_bits = false;
			}

			*check_var = tmp_check_var;
			*rec_ref_unit = tmp_ref_unit;
		}
	}
}
#endif /* LOCATOR */

#ifdef DIVA
/*
 * heapgetpage_DIVA - subroutine for heapgettup_DIVA()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 *
 * This function manages contention with the writer using an dual reference
 * counter.
 * 
 * If an ebi page containing a version not obtained from the heap page is still
 * in memory, it reads that version.
 * Otherwise, it records the location of that version and reads it after the
 * heap pages have been scanned.
 */
void
heapgetpage_DIVA(TableScanDesc sscan, BlockNumber page)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	bool		valid;

	Relation relation;
	Oid		 rel_id;

	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;
	EbiTreeVersionOffset version_offset;

	HeapTupleData	v_loctup;
	BlockNumber		v_block;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;
	OffsetNumber	v_offset = InvalidOffsetNumber;

#ifdef LOCATOR
	Page			readbuf_dp;
	bool			version_from_readbuf = false;
	DualRefDescData	drefDesc;
	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;
#endif /* LOCATOR */

	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	Assert(page < scan->rs_nblocks);

	relation = scan->rs_base.rs_rd;
#ifdef LOCATOR
	drefDesc.dual_ref = NULL;
#endif /* LOCATOR */

	/* release previous scan buffer, if any */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

#ifdef LOCATOR
	if (scan->rs_base.rs_prefetch != NULL)
	{
		/* Read page with prefetch and ring buffer */
		scan->rs_cbuf = ReadBufferPrefetch(scan->rs_base.rs_rd, page,
											scan->rs_base.rs_prefetch,
											scan->rs_strategy, &readbuf_dp);
		scan->c_readbuf_dp = readbuf_dp;
	
		/* 
		 * If we receive an invalid buffer from prefetching, it means it 
		 * originated from the read buffer.
		 */
		if (BufferIsInvalid(scan->rs_cbuf))
			version_from_readbuf = true;
	}
	else
#endif /* LOCATOR */
	{
		/* Read page using selected strategy */
		scan->rs_cbuf = ReadBufferExtended(scan->rs_base.rs_rd, MAIN_FORKNUM, page,
									   RBM_NORMAL, scan->rs_strategy);
	}

	scan->rs_cblock = page;

	if (!(scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE))
		return;

	buffer = scan->rs_cbuf;
	snapshot = scan->rs_base.rs_snapshot;

#ifdef LOCATOR
	/*
	 * We read the heap tuple safely by using atomic variable instead of
	 * acquiring shared lock, so don't lock the buffer.
	 */
	if (BufferIsValid(buffer))
	{
		heap_page_prune_opt(relation, buffer);

#ifdef USING_LOCK
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		drefDesc.dual_ref = NULL;
#else /* !USING_LOCK */
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buffer);
		SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK */

		dp = BufferGetPage(buffer);
	}
	else
	{
		drefDesc.dual_ref = NULL;

		dp = readbuf_dp;
	}
#else /* !LOCATOR */
	/*
	 * Prune and repair fragmentation for the whole page, if possible.
	 */
	heap_page_prune_opt(scan->rs_base.rs_rd, buffer);

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
#endif /* LOCATOR */

	TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;

	rel_id = RelationGetRelid(scan->rs_base.rs_rd);
	v_loctup.t_tableOid = rel_id;

	/*
	 *  ----------- -------------- ---------------
	 * | P-locator | left version | right version |
	 *  ----------- -------------- ---------------
	 */
	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
#ifdef LOCATOR
		bool is_record_from_other_page = false;
#endif /* LOCATOR */

		/* Don't read version directly */
		if (!LP_IS_PLEAF_FLAG(lpp))
			continue;

		/* Get p-locator of this record */
		p_locator = PageGetItem(dp, lpp);

		/* Set self pointer of tuple */
		ItemPointerSet(&(v_loctup.t_self), page, lineoff);

		/* Init checking variables */
		valid = false;
		v_buffer = buffer;

#ifdef LOCATOR
		/* Increment reference count and determine where to check */
		GetCheckVar(&drefDesc, &rec_ref_unit, &check_var,
					&set_hint_bits, lineoff);
#endif /* LOCATOR */

		/* Check visibility of left, right versions with check_var */
		for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
		{
#ifdef LOCATOR
			/* Is this version need to be checked? */
			if (!(check_var & test_var))
				continue;
#endif /* LOCATOR */

			/* Release previous version buffer */
			if (unlikely(v_buffer != buffer))
				ReleaseBuffer(v_buffer);

			/* Set block number and offset */
			v_block = page;
			v_offset = lineoff + test_var; 

			Assert(BlockNumberIsValid(v_block));
			Assert(OffsetNumberIsValid(v_offset));

			/*
			 * If the version is in the same page with p-locator, just get it.
			 * Or not, read the buffer that it is in.
			 */
			if (likely(v_block == page))
			{
				v_buffer = buffer;
				v_dp = dp;
			}
			else
			{
				v_buffer = ReadBuffer(relation, v_block);
				Assert(BufferIsValid(v_buffer));
				v_dp = BufferGetPage(v_buffer);

#ifdef LOCATOR
				/* We get a version from other page. Then, Check it. */
				is_record_from_other_page = true;
#endif /* LOCAOTR */
			}

			v_lpp = PageGetItemId(v_dp, v_offset);

			/* The target has never been updated after INSERT */
			if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
				continue;
				
			v_loctup.t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
			v_loctup.t_len = ItemIdGetLength(v_lpp);

#ifdef LOCATOR
			/* Set buffer to set hint bits */
			hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

			/* Check visibility of version */
			valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
												 hint_bits_buf);
#else /* !LOCATOR */
			/* Check visibility of version */
			valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
												 v_buffer);
#endif /* LOCATOR */

			if (valid)
				break;
		}

		/* If we found visible version from heap page, continue */
		if (valid || modification)
		{
			HeapTuple tuple;
			int aligned_tuple_size;

			/*
			 * Modification Case
			 *
			 * If the scanning is for an update, we don't need to search deeply. 
			 * Visible tuples inside the heap page cannot be found. Instead, we 
			 * copy one of the tuples in the heap page, allowing the following 
			 * ExecStoreBufferHeapTuple to proceed.
			 */
#ifdef LOCATOR
			if (version_from_readbuf && !is_record_from_other_page)
			{
				tuple = scan->rs_vistuples_copied[ntup];

				/* 
				 * When retrieving the version from a non-update page 
				 * (i.e. ReadBuffer), we avoid memory copy. 
				 */
				if (tuple)
					scan->rs_vistuples_freearray[ntup] = tuple;

				scan->rs_vistuples_copied[ntup] = NULL;
			}
			else
#endif /* LOCATOR */
			{
				/* We need to perform a memory copy to fetch a version. */

				if (tuple_size_available(scan, ntup, &v_loctup))
				{
					/* We can reuse the existing tuple structure. */
					if (scan->rs_vistuples_copied[ntup] == NULL)
					{
						scan->rs_vistuples_copied[ntup] = 
										scan->rs_vistuples_freearray[ntup];

						scan->rs_vistuples_freearray[ntup] = NULL;
					}
				}
				else
				{
					/* 
					 * We cannot reuse the existing tuple structure. In such 
					 * cases, we reallocate a new structure to fetch a version. 
					 */
					aligned_tuple_size = 
									1 << my_log2(HEAPTUPLESIZE + v_loctup.t_len);
					tuple = (HeapTuple) palloc(aligned_tuple_size);

					/* Here, we free the exsiting tuple structure. */
					remove_redundant_tuple(scan, ntup);

					scan->rs_vistuples_copied[ntup] = tuple;
					scan->rs_vistuples_size[ntup] = aligned_tuple_size;
				}

				heap_copytuple_with_recycling(&v_loctup, 
											  scan->rs_vistuples_copied[ntup]);
			}

			sscan->counter_in_heap += 1;
			if (modification)
				scan->rs_vistuples[ntup] = lineoff;
			else
				scan->rs_vistuples[ntup] = v_offset;
			ntup++;

			/* Release version buffer */
			if (unlikely(v_buffer != buffer))
				ReleaseBuffer(v_buffer);

#ifdef LOCATOR
			/* Decrement ref_cnt */
			if (rec_ref_unit != 0)
				DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);
#endif /* LOCATOR */

			continue;
		}

		/* Release version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

		/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
		if (snapshot->snapshot_type != SNAPSHOT_MVCC)
#ifdef LOCATOR
			goto failed;
#else /* !LOCATOR */
			continue;
#endif /* LOCATOR */

		/*
		 * Both versions are invisible to this transaction.
		 * Need to search from p-leaf & ebi-tree.
		 */
		l_off = PLocatorGetLeftOffset(p_locator);
		r_off = PLocatorGetRightOffset(p_locator);
		xid_bound = PLocatorGetXIDBound(p_locator);

		if (l_off == 0 && r_off == 0)
		{
			ret_id = -1;
		}
		else 
		{
			PLeafOffset p_offset;

			if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
				p_offset = l_off;
			else
				p_offset = r_off;

			ret_id = PLeafLookupTuple(rel_id, 
									  true,
									  &version_offset,
									  p_offset, 
									  snapshot, 
									  &(v_loctup.t_len), 
									  (void**) &(v_loctup.t_data));
		}

		/* If head version is visible in memory, get that version */
		if (ret_id > -1)
		{
			/* We need to perform a memory copy to fetch a version. */

			if (tuple_size_available(scan, ntup, &v_loctup))
			{
				/* We can reuse the existing tuple structure. */
				if (scan->rs_vistuples_copied[ntup] == NULL)
				{
					scan->rs_vistuples_copied[ntup] =
											scan->rs_vistuples_freearray[ntup]; 

					scan->rs_vistuples_freearray[ntup] = NULL;
				}
			}
			else
			{
				HeapTuple tuple;
				int aligned_tuple_size;
		
				/* 
				 * We cannot reuse the existing tuple structure. In such 
				 * cases, we reallocate a new structure to fetch a version. 
				 */
				aligned_tuple_size = 
								1 << my_log2(HEAPTUPLESIZE + v_loctup.t_len);
				tuple = (HeapTuple) palloc(aligned_tuple_size);

				remove_redundant_tuple(scan, ntup);

				scan->rs_vistuples_copied[ntup] = tuple;
				scan->rs_vistuples_size[ntup] = aligned_tuple_size;
			}

			heap_copytuple_with_recycling(&v_loctup, 
											scan->rs_vistuples_copied[ntup]);

			/* Unpin a EBI sub page */
			UnpinEbiSubBuffer(ret_id);

			scan->rs_vistuples[ntup] = v_offset;
			ntup++;
		
			++(sscan->counter_in_ebi);
		}
		else if (ret_id == -2)
		{
			/* 
			 * The visible version is located in the EBI page, and we will 
			 * access it at EBI scan phase. 
			 */
			InsertUnobtainedVersionOffset(scan, version_offset);

			++(sscan->counter_in_ebi);
		}
#ifdef LOCATOR
failed:
		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);
#endif /* LOCATOR */
	}
		
	if (BufferIsValid(buffer))
#if !defined(LOCATOR) || defined(USING_LOCK)
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}
#endif /* DIVA */

/*
 * heapgetpage - subroutine for heapgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
void
heapgetpage(TableScanDesc sscan, BlockNumber page)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	bool		all_visible;

	Assert(page < scan->rs_nblocks);

	/* release previous scan buffer, if any */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	scan->rs_cbuf = ReadBufferExtended(scan->rs_base.rs_rd, MAIN_FORKNUM, page,
									   RBM_NORMAL, scan->rs_strategy);
	scan->rs_cblock = page;

	if (!(scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE))
		return;

	buffer = scan->rs_cbuf;
	snapshot = scan->rs_base.rs_snapshot;

	/*
	 * Prune and repair fragmentation for the whole page, if possible.
	 */
	heap_page_prune_opt(scan->rs_base.rs_rd, buffer);

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;

	/*
	 * If the all-visible flag indicates that all tuples on the page are
	 * visible to everyone, we can skip the per-tuple visibility tests.
	 *
	 * Note: In hot standby, a tuple that's already visible to all
	 * transactions on the primary might still be invisible to a read-only
	 * transaction in the standby. We partly handle this problem by tracking
	 * the minimum xmin of visible tuples as the cut-off XID while marking a
	 * page all-visible on the primary and WAL log that along with the
	 * visibility map SET operation. In hot standby, we wait for (or abort)
	 * all transactions that can potentially may not see one or more tuples on
	 * the page. That's how index-only scans work fine in hot standby. A
	 * crucial difference between index-only scans and heap scans is that the
	 * index-only scan completely relies on the visibility map where as heap
	 * scan looks at the page-level PD_ALL_VISIBLE flag. We are not sure if
	 * the page-level flag can be trusted in the same way, because it might
	 * get propagated somehow without being explicitly WAL-logged, e.g. via a
	 * full page write. Until we can prove that beyond doubt, let's check each
	 * tuple for visibility the hard way.
	 */
	all_visible = PageIsAllVisible(dp) && !snapshot->takenDuringRecovery;

	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		if (ItemIdIsNormal(lpp))
		{
			HeapTupleData loctup;
			bool		valid;

			loctup.t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
			loctup.t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			loctup.t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(loctup.t_self), page, lineoff);

			if (all_visible)
				valid = true;
			else
				valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);

			HeapCheckForSerializableConflictOut(valid, scan->rs_base.rs_rd,
												&loctup, buffer, snapshot);

			if (valid)
				scan->rs_vistuples[ntup++] = lineoff;
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;
}

/* ----------------
 *		heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void
heapgettup(HeapScanDesc scan,
		   ScanDirection dir,
		   int nkeys,
		   ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_base.rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	int			lines;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			if (scan->rs_base.rs_parallel != NULL)
			{
				ParallelBlockTableScanDesc pbscan =
				(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
				ParallelBlockTableScanWorker pbscanwork =
				scan->rs_parallelworkerdata;

				table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				/* Other processes might have already finished the scan. */
				if (page == InvalidBlockNumber)
				{
					Assert(!BufferIsValid(scan->rs_cbuf));
					tuple->t_data = NULL;
					return;
				}
			}
			else
				page = scan->rs_startblock; /* first page */
				
			heapgetpage((TableScanDesc) scan, page);
			lineoff = FirstOffsetNumber;	/* first offnum */
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineoff =			/* next offnum */
				OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineoff now reference the physically next tid */

		linesleft = lines - lineoff + 1;
	}
	else if (backward)
	{
		/* backward parallel scan not supported */
		Assert(scan->rs_base.rs_parallel == NULL);

		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

			/*
			 * Start from last page of the scan.  Ensure we take into account
			 * rs_numblocks if it's been adjusted by heap_setscanlimits().
			 */
			if (scan->rs_numblocks != InvalidBlockNumber)
				page = (scan->rs_startblock + scan->rs_numblocks - 1) % scan->rs_nblocks;
			else if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
				
			heapgetpage((TableScanDesc) scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber(dp);

		if (!scan->rs_inited)
		{
			lineoff = lines;	/* final offnum */
			scan->rs_inited = true;
		}
		else
		{
			/*
			 * The previous returned tuple may have been vacuumed since the
			 * previous scan when we use a non-MVCC snapshot, so we must
			 * re-establish the lineoff <= PageGetMaxOffsetNumber(dp)
			 * invariant
			 */
			lineoff =			/* previous offnum */
				Min(lines,
					OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self))));
		}
		/* page and lineoff now reference the physically previous tid */

		linesleft = lineoff;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		if (page != scan->rs_cblock)
			heapgetpage((TableScanDesc) scan, page);

		/* Since the tuple was previously fetched, needn't lock page here */
		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
		/*
		 * Only continue scanning the page while we have lines left.
		 *
		 * Note that this protects us from accessing line pointers past
		 * PageGetMaxOffsetNumber(); both for forward scans when we resume the
		 * table scan, and for when we start scanning a new page.
		 */
		while (linesleft > 0)
		{
			if (ItemIdIsNormal(lpp))
			{
				bool		valid;

				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, lineoff);

				/*
				 * if current tuple qualifies, return it.
				 */
				valid = HeapTupleSatisfiesVisibility(tuple,
													 snapshot,
													 scan->rs_cbuf);

				HeapCheckForSerializableConflictOut(valid, scan->rs_base.rs_rd,
													tuple, scan->rs_cbuf,
													snapshot);

				if (valid && key != NULL)
					HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd),
								nkeys, key, valid);

				if (valid)
				{
					LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
					return;
				}
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				--lpp;			/* move back in this page's ItemId array */
				--lineoff;
			}
			else
			{
				++lpp;			/* move forward in this page's ItemId array */
				++lineoff;
			}
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/*
		 * advance to next/prior page and detect end of scan
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else if (scan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan =
			(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			ParallelBlockTableScanWorker pbscanwork =
			scan->rs_parallelworkerdata;

			page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
													 pbscanwork, pbscan);
			finished = (page == InvalidBlockNumber);
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_base.rs_flags & SO_ALLOW_SYNC)
				ss_report_location(scan->rs_base.rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		heapgetpage((TableScanDesc) scan, page);

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber((Page) dp);
		linesleft = lines;
		if (backward)
		{
			lineoff = lines;
			lpp = PageGetItemId(dp, lines);
		}
		else
		{
			lineoff = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}

#ifdef DIVA
/* ----------------
 *		heapgettup_DIVA - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup with
 * 		scan->rs_ctup_copied, or set scan->rs_ctup.t_data = NULL if no more
 * 		tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void
heapgettup_DIVA(HeapScanDesc scan,
				ScanDirection dir,
				int nkeys,
				ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_base.rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	int			lines;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;
	bool		valid;
	Buffer		buffer;

	Relation relation;
	Oid		 rel_id;

	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;

#ifdef LOCATOR
	DualRefDescData	drefDesc;
	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;
#endif /* LOCATOR */

	HeapTupleData	v_loctup;
	BlockNumber		v_block;
	OffsetNumber	v_offset;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;
	
	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	relation = scan->rs_base.rs_rd;
	rel_id = RelationGetRelid(relation);
	v_loctup.t_tableOid = rel_id;

#ifdef LOCATOR
	drefDesc.dual_ref = NULL;
#endif /* LOCATOR */

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			if (scan->rs_base.rs_parallel != NULL)
			{
				ParallelBlockTableScanDesc pbscan =
				(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
				ParallelBlockTableScanWorker pbscanwork =
				scan->rs_parallelworkerdata;

				table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				/* Other processes might have already finished the scan. */
				if (page == InvalidBlockNumber)
				{
					Assert(!BufferIsValid(scan->rs_cbuf));
					tuple->t_data = NULL;
					return;
				}
			}
			else
				page = scan->rs_startblock; /* first page */
				
			heapgetpage((TableScanDesc) scan, page);
			lineoff = FirstOffsetNumber;	/* first offnum */
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineoff =			/* next offnum */
				OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

#if !defined(LOCATOR) || defined(USING_LOCK)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
#else
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(scan->rs_cbuf);
		SetPageRefUnit(&drefDesc);
#endif

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineoff now reference the physically next tid */

		linesleft = lines - lineoff + 1;
	}
	else if (backward)
	{
		/* backward parallel scan not supported */
		Assert(scan->rs_base.rs_parallel == NULL);

		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

			/*
			 * Start from last page of the scan.  Ensure we take into account
			 * rs_numblocks if it's been adjusted by heap_setscanlimits().
			 */
			if (scan->rs_numblocks != InvalidBlockNumber)
				page = (scan->rs_startblock + scan->rs_numblocks - 1) % scan->rs_nblocks;
			else if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;

			heapgetpage((TableScanDesc) scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
		}

#if !defined(LOCATOR) || defined(USING_LOCK)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
#else
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(scan->rs_cbuf);
		SetPageRefUnit(&drefDesc);
#endif

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber(dp);

		if (!scan->rs_inited)
		{
			lineoff = lines;	/* final offnum */
			scan->rs_inited = true;
		}
		else
		{
			/*
			 * The previous returned tuple may have been vacuumed since the
			 * previous scan when we use a non-MVCC snapshot, so we must
			 * re-establish the lineoff <= PageGetMaxOffsetNumber(dp)
			 * invariant
			 */
			lineoff =			/* previous offnum */
				Min(lines,
					OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self))));
		}
		/* page and lineoff now reference the physically previous tid */

		linesleft = lineoff;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		if (page != scan->rs_cblock)
			heapgetpage((TableScanDesc) scan, page);

		*tuple = *scan->rs_ctup_copied;

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
		/*
		 * Only continue scanning the page while we have lines left.
		 *
		 * Note that this protects us from accessing line pointers past
		 * PageGetMaxOffsetNumber(); both for forward scans when we resume the
		 * table scan, and for when we start scanning a new page.
		 */
		buffer = scan->rs_cbuf;
		while (linesleft > 0)
		{
			/* Don't read version directly */
			if (ItemIdIsNormal(lpp) && LP_IS_PLEAF_FLAG(lpp))
			{
				/* Get p_locator of this record */
				p_locator = PageGetItem(dp, lpp);

				/* Set self pointer of tuple */
				ItemPointerSet(&(v_loctup.t_self), page, lineoff);

				/* Init checking variables */
				valid = false;
				v_buffer = buffer;

#ifdef LOCATOR
				/* Increment reference count and determine where to check */
				GetCheckVar(&drefDesc, &rec_ref_unit, &check_var,
							&set_hint_bits, lineoff);
#endif /* LOCATOR */

				/* Check visibility of left, right versions with check_var */
				for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
				{
#ifdef LOCATOR
					/* Is this version need to be checked? */
					if (!(check_var & test_var))
						continue;
#endif /* LOCATOR */

					/* Release previous version buffer */
					if (unlikely(v_buffer != buffer))
						ReleaseBuffer(v_buffer);

					/* Set block number and offset */
					v_block = page;
					v_offset = lineoff + test_var; 

					Assert(BlockNumberIsValid(v_block));
					Assert(OffsetNumberIsValid(v_offset));

					/*
					 * If the version is in the same page with p-locator, just
					 * get it.
					 * Or not, read the buffer that it is in.
					 */
					if (likely(v_block == page))
					{
						v_buffer = buffer;
						v_dp = dp;
					}
					else
					{
						v_buffer = ReadBuffer(relation, v_block);
						Assert(BufferIsValid(v_buffer));
						v_dp = BufferGetPage(v_buffer);
					}

					v_lpp = PageGetItemId(v_dp, v_offset);

					/* The target has never been updated after INSERT */
					if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
						continue;
						
					v_loctup.t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
					v_loctup.t_len = ItemIdGetLength(v_lpp);

#ifdef LOCATOR
					/* Set buffer to set hint bits */
					hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

					/* Check visibility of version */
					valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
														 hint_bits_buf);
#else /* !LOCATOR */
					/* Check visibility of version */
					valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
														 v_buffer);
#endif /* LOCATOR */

					if (valid && key != NULL)
						HeapKeyTest(&v_loctup, RelationGetDescr(relation),
									nkeys, key, valid);

					if (valid)
					{
						if (scan->rs_ctup_copied != NULL)
							heap_freetuple(scan->rs_ctup_copied);

						scan->rs_ctup_copied = heap_copytuple(&v_loctup);

						*tuple = *scan->rs_ctup_copied;

						/* Release version buffer */
						if (unlikely(v_buffer != buffer))
							ReleaseBuffer(v_buffer);

#ifdef LOCATOR
						/* Decrement ref_cnt */
						if (rec_ref_unit != 0)
							drefDesc.page_ref_unit += rec_ref_unit;
#endif /* LOCATOR */

#if !defined(LOCATOR) || defined(USING_LOCK)
						LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else
						DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif

						return;
					}
				}

				/* Both left and right-side versions are invisible */
				if (modification)
				{
					/*
					 * If this scanning is for update, we don't need to bother
					 * searching deeply.
					 */
					if (scan->rs_ctup_copied != NULL)
						heap_freetuple(scan->rs_ctup_copied);

					/*
					 * We cannot find visible tuple inside the heap page.
					 * Copy one of any tuple in the heap page so that
					 * following ExecStoreBufferHeapTuple can be passed.
					 */
					scan->rs_ctup_copied = heap_copytuple(&v_loctup);

					*tuple = *scan->rs_ctup_copied;

					/* Release previous version buffer */
					if (unlikely(v_buffer != buffer))
						ReleaseBuffer(v_buffer);

#ifdef LOCATOR
					/* Decrement ref_cnt */
					if (rec_ref_unit != 0)
						drefDesc.page_ref_unit += rec_ref_unit;
#endif /* LOCATOR */

#if !defined(LOCATOR) || defined(USING_LOCK)
					LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif

					return;
				}

				/* Release version buffer */
				if (unlikely(v_buffer != buffer))
					ReleaseBuffer(v_buffer);

				/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
				if (snapshot->snapshot_type != SNAPSHOT_MVCC)
#ifdef LOCATOR
					goto failed;
#else /* !LOCATOR */
					continue;
#endif /* LOCATOR */

				/*
				 * Both versions are invisible to this transaction.
				 * Need to search from p-leaf & ebi-tree.
				 */
				memcpy(&l_off, p_locator, sizeof(uint64));
				memcpy(&r_off, p_locator + sizeof(uint64), sizeof(uint64));
				memcpy(&xid_bound, p_locator + sizeof(uint64) * 2, sizeof(TransactionId));

				if (l_off == 0 && r_off == 0)
				{
					ret_id = -1;
				}
				else 
				{
					PLeafOffset p_offset;

					if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
						p_offset = l_off;
					else
						p_offset = r_off;
	
					ret_id = PLeafLookupTuple(rel_id, 
											  false,
											  NULL,
											  p_offset, 
											  snapshot, 
											  &(v_loctup.t_len), 
											  (void**) &(v_loctup.t_data));
				}

				/* If head version is visible, get that version */
				if (ret_id > -1)
				{
					if (scan->rs_ctup_copied != NULL)
						heap_freetuple(scan->rs_ctup_copied);
		
					scan->rs_ctup_copied = heap_copytuple(&v_loctup);

					*tuple = *scan->rs_ctup_copied;
					
					/* Unpin a EBI sub page */
					UnpinEbiSubBuffer(ret_id);

#ifdef LOCATOR
					/* Decrement ref_cnt */
					if (rec_ref_unit != 0)
						drefDesc.page_ref_unit += rec_ref_unit;
#endif /* LOCATOR */

#if !defined(LOCATOR) || defined(USING_LOCK)
					LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif

					return;
				}

#ifdef LOCATOR
failed:
				/* Decrement ref_cnt */
				if (rec_ref_unit != 0)
					DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);
#endif /* LOCATOR */
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				--lpp;			/* move back in this page's ItemId array */
				--lineoff;
			}
			else
			{
				++lpp;			/* move forward in this page's ItemId array */
				++lineoff;
			}
		}

#if !defined(LOCATOR) || defined(USING_LOCK)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
#else
		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif

		/*
		 * advance to next/prior page and detect end of scan
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else if (scan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan =
			(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			ParallelBlockTableScanWorker pbscanwork =
			scan->rs_parallelworkerdata;

			page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
													 pbscanwork, pbscan);
			finished = (page == InvalidBlockNumber);
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_base.rs_flags & SO_ALLOW_SYNC)
				ss_report_location(scan->rs_base.rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		heapgetpage((TableScanDesc) scan, page);

#if !defined(LOCATOR) || defined(USING_LOCK)
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
#else
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(scan->rs_cbuf);
		SetPageRefUnit(&drefDesc);
#endif

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber((Page) dp);
		linesleft = lines;
		if (backward)
		{
			lineoff = lines;
			lpp = PageGetItemId(dp, lines);
		}
		else
		{
			lineoff = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}
#endif /* DIVA */

/* ----------------
 *		heapgettup_pagemode - fetch next heap tuple in page-at-a-time mode
 *
 *		Same API as heapgettup, but used in page-at-a-time mode
 *
 * The internal logic is much the same as heapgettup's too, but there are some
 * differences: we do not take the buffer content lock (that only needs to
 * happen inside heapgetpage), and we iterate through just the tuples listed
 * in rs_vistuples[] rather than all tuples on the page.  Notice that
 * lineindex is 0-based, where the corresponding loop variable lineoff in
 * heapgettup is 1-based.
 * ----------------
 */
#ifdef DIVA
static void
heapgettup_pagemode_DIVA(HeapScanDesc scan,
						 ScanDirection dir,
						 int nkeys,
						 ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	int			lines;
	int			lineindex;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	Assert(ScanDirectionIsForward(dir));

	/*
	 * calculate next starting lineindex, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			if (scan->rs_base.rs_parallel != NULL)
			{
				ParallelBlockTableScanDesc pbscan =
				(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
				ParallelBlockTableScanWorker pbscanwork =
				scan->rs_parallelworkerdata;

				table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				/* Other processes might have already finished the scan. */
				if (page == InvalidBlockNumber)
				{
					Assert(!BufferIsValid(scan->rs_cbuf));
					tuple->t_data = NULL;
					return;
				}
			}
			else
				page = scan->rs_startblock; /* first page */
				
			heapgetpage_DIVA((TableScanDesc) scan, page);
			lineindex = 0;
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineindex = scan->rs_cindex + 1;
		}

#ifdef LOCATOR
		if (BufferIsInvalid(scan->rs_cbuf))
			dp = scan->c_readbuf_dp;
		else
#endif /* LOCATOR */
			dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineindex;
	}
	else if (backward)
	{
		/* backward parallel scan not supported */
		Assert(scan->rs_base.rs_parallel == NULL);

		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

			/*
			 * Start from last page of the scan.  Ensure we take into account
			 * rs_numblocks if it's been adjusted by heap_setscanlimits().
			 */
			if (scan->rs_numblocks != InvalidBlockNumber)
				page = (scan->rs_startblock + scan->rs_numblocks - 1) % scan->rs_nblocks;
			else if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
				
			heapgetpage_DIVA((TableScanDesc) scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
		}

#ifdef LOCATOR
		if (BufferIsInvalid(scan->rs_cbuf))
			dp = scan->c_readbuf_dp;
		else
#endif /* LOCATOR */
			dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;

		if (!scan->rs_inited)
		{
			lineindex = lines - 1;
			scan->rs_inited = true;
		}
		else
		{
			lineindex = scan->rs_cindex - 1;
		}

		/* page and lineindex now reference the previous visible tid */
		linesleft = lineindex + 1;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));

		if (page != scan->rs_cblock)
			heapgetpage_DIVA((TableScanDesc) scan, page);

#ifdef LOCATOR
		if (BufferIsInvalid(scan->rs_cbuf))
			dp = scan->c_readbuf_dp;
		else
#endif /* LOCATOR */
			dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		/* check that rs_cindex is in sync */
		Assert(scan->rs_cindex < scan->rs_ntuples);
		Assert(lineoff == scan->rs_vistuples[scan->rs_cindex]);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		while (linesleft > 0)
		{
			lineoff = scan->rs_vistuples[lineindex];
			lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsNormal(lpp));

			if (scan->rs_vistuples_copied[lineindex])
			{
				*tuple = *(scan->rs_vistuples_copied[lineindex]);
			}
			else
			{
				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
			}

			ItemPointerCopy(&(tuple->t_data->t_ctid), &(tuple->t_self));

			/*
			 * if current tuple qualifies, return it.
			 */
			if (key != NULL)
			{
				bool		valid;

				HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd),
							nkeys, key, valid);
				if (valid)
				{
					scan->rs_cindex = lineindex;
					return;
				}
			}
			else
			{
				scan->rs_cindex = lineindex;
				return;
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
				--lineindex;
			else
				++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else if (scan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan =
			(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			ParallelBlockTableScanWorker pbscanwork =
			scan->rs_parallelworkerdata;

			page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
													 pbscanwork, pbscan);
			finished = (page == InvalidBlockNumber);
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_base.rs_flags & SO_ALLOW_SYNC)
				ss_report_location(scan->rs_base.rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);

			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		heapgetpage_DIVA((TableScanDesc) scan, page);

#ifdef LOCATOR
		if (BufferIsInvalid(scan->rs_cbuf))
			dp = scan->c_readbuf_dp;
		else
#endif /* LOCATOR */
			dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		linesleft = lines;
		if (backward)
			lineindex = lines - 1;
		else
			lineindex = 0;
	}
}
#endif /* DIVA */

static void
heapgettup_pagemode(HeapScanDesc scan,
					ScanDirection dir,
					int nkeys,
					ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	int			lines;
	int			lineindex;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	/*
	 * calculate next starting lineindex, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			if (scan->rs_base.rs_parallel != NULL)
			{
				ParallelBlockTableScanDesc pbscan =
				(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
				ParallelBlockTableScanWorker pbscanwork =
				scan->rs_parallelworkerdata;

				table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
														 pbscanwork, pbscan);

				/* Other processes might have already finished the scan. */
				if (page == InvalidBlockNumber)
				{
					Assert(!BufferIsValid(scan->rs_cbuf));
					tuple->t_data = NULL;
					return;
				}
			}
			else
				page = scan->rs_startblock; /* first page */

			heapgetpage((TableScanDesc) scan, page);
			lineindex = 0;
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineindex = scan->rs_cindex + 1;
		}

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineindex;
	}
	else if (backward)
	{
		/* backward parallel scan not supported */
		Assert(scan->rs_base.rs_parallel == NULL);

		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

			/*
			 * Start from last page of the scan.  Ensure we take into account
			 * rs_numblocks if it's been adjusted by heap_setscanlimits().
			 */
			if (scan->rs_numblocks != InvalidBlockNumber)
				page = (scan->rs_startblock + scan->rs_numblocks - 1) % scan->rs_nblocks;
			else if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
				
			heapgetpage((TableScanDesc) scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
		}

		dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;

		if (!scan->rs_inited)
		{
			lineindex = lines - 1;
			scan->rs_inited = true;
		}
		else
		{
			lineindex = scan->rs_cindex - 1;
		}

		/* page and lineindex now reference the previous visible tid */
		linesleft = lineindex + 1;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));

		if (page != scan->rs_cblock)
			heapgetpage((TableScanDesc) scan, page);

		/* Since the tuple was previously fetched, needn't lock page here */
		dp = BufferGetPage(scan->rs_cbuf);

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);

		/* check that rs_cindex is in sync */
		Assert(scan->rs_cindex < scan->rs_ntuples);
		Assert(lineoff == scan->rs_vistuples[scan->rs_cindex]);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		while (linesleft > 0)
		{
			lineoff = scan->rs_vistuples[lineindex];
			lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsNormal(lpp));

			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			ItemPointerSet(&(tuple->t_self), page, lineoff);

			/*
			 * if current tuple qualifies, return it.
			 */
			if (key != NULL)
			{
				bool		valid;

				HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd),
							nkeys, key, valid);
				if (valid)
				{
					scan->rs_cindex = lineindex;
					return;
				}
			}
			else
			{
				scan->rs_cindex = lineindex;
				return;
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
				--lineindex;
			else
				++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else if (scan->rs_base.rs_parallel != NULL)
		{
			ParallelBlockTableScanDesc pbscan =
			(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			ParallelBlockTableScanWorker pbscanwork =
			scan->rs_parallelworkerdata;

			page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
													 pbscanwork, pbscan);
			finished = (page == InvalidBlockNumber);
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks == 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_base.rs_flags & SO_ALLOW_SYNC)
				ss_report_location(scan->rs_base.rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		heapgetpage((TableScanDesc) scan, page);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		linesleft = lines;
		if (backward)
			lineindex = lines - 1;
		else
			lineindex = 0;
	}
}

/* ----------------------------------------------------------------
 *					 heap access method interface
 * ----------------------------------------------------------------
 */
#ifdef LOCATOR
TableScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key,
			   ParallelTableScanDesc parallel_scan,
			   uint32 flags,
			   List *hap_partid_list) /* hap_partid_list is not used in heap_beginscan*/
#else /* !LOCATOR */
TableScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key,
			   ParallelTableScanDesc parallel_scan,
			   uint32 flags)
#endif /* LOCATOR */
{
	HeapScanDesc scan;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (HeapScanDesc) palloc(sizeof(HeapScanDescData));

#ifdef DIVA
	/* Initialize the array for copying scanned tuples */
	memset(scan->rs_vistuples_copied, 0x00, sizeof(scan->rs_vistuples_copied));
	memset(scan->rs_vistuples_size, 0x00, sizeof(int) * MaxHeapTuplesPerPage);
	memset(scan->rs_vistuples_freearray, 0x00, 
										sizeof(scan->rs_vistuples_freearray));
#endif

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;
	scan->rs_strategy = NULL;	/* set in initscan */
#ifdef LOCATOR
	/* Init values */
	scan->rs_base.rs_prefetch = NULL; /* set after initscan */
#endif /* LOCATOR */

	/*
	 * Disable page-at-a-time mode if it's not a MVCC-safe snapshot.
	 */
	if (!(snapshot && IsMVCCSnapshot(snapshot)))
		scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;

	/*
	 * For seqscan and sample scans in a serializable transaction, acquire a
	 * predicate lock on the entire relation. This is required not only to
	 * lock all the matching tuples, but also to conflict with new insertions
	 * into the table. In an indexscan, we take page locks on the index pages
	 * covering the range specified in the scan qual, but in a heap scan there
	 * is nothing more fine-grained to lock. A bitmap scan is a different
	 * story, there we have already scanned the index and locked the index
	 * pages covering the predicate. But in that case we still have to lock
	 * any matching heap tuples. For sample scan we could optimize the locking
	 * to be at least page-level granularity, but we'd need to add per-tuple
	 * locking for that.
	 */
	if (scan->rs_base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
	{
		/*
		 * Ensure a missing snapshot is noticed reliably, even if the
		 * isolation mode means predicate locking isn't performed (and
		 * therefore the snapshot isn't used here).
		 */
		Assert(snapshot);
		PredicateLockRelation(relation, snapshot);
	}

	/* we only need to set this up once */
	scan->rs_ctup.t_tableOid = RelationGetRelid(relation);

	/*
	 * Allocate memory to keep track of page allocation for parallel workers
	 * when doing a parallel scan.
	 */
	if (parallel_scan != NULL)
		scan->rs_parallelworkerdata = palloc(sizeof(ParallelBlockTableScanWorkerData));
	else
		scan->rs_parallelworkerdata = NULL;

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_base.rs_key = NULL;

	initscan(scan, key, false);

#ifdef LOCATOR
	/* 
	 * We evaluate the necessity of prefetching.
	 */
	if (enable_prefetch && is_prefetching_needed(relation, scan))
	{
		scan->rs_base.rs_prefetch = get_heap_prefetch_desc(relation, scan);
	}
#endif /* LOCATOR */

	return (TableScanDesc) scan;
}

void
heap_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	if (set_params)
	{
		if (allow_strat)
			scan->rs_base.rs_flags |= SO_ALLOW_STRAT;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_STRAT;

		if (allow_sync)
			scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

		if (allow_pagemode && scan->rs_base.rs_snapshot &&
			IsMVCCSnapshot(scan->rs_base.rs_snapshot))
			scan->rs_base.rs_flags |= SO_ALLOW_PAGEMODE;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;
	}

	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

#ifdef DIVA
	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
	{
		if (scan->rs_vistuples_copied[i] != NULL)
			heap_freetuple(scan->rs_vistuples_copied[i]);
	}

	if (scan->rs_ctup_copied != NULL)
		heap_freetuple(scan->rs_ctup_copied);

	/* Existing resource cleanup */
	if (scan->unobtained_version_offsets)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		pfree(scan->unobtained_version_offsets);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->unobtained_segments)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		for (int i = 0; i < scan->unobtained_segment_nums; ++i)
			pfree(scan->unobtained_segments[i]);

		pfree(scan->unobtained_segments);
		MemoryContextSwitchTo(oldcxt);
	}
#endif

	/*
	 * reinitialize scan descriptor
	 */
	initscan(scan, key, true);

#ifdef LOCATOR
	if (sscan->rs_prefetch)
	{
		PrefetchDesc prefetchDesc = sscan->rs_prefetch;
		PrefetchHeapDesc heap_desc = (PrefetchHeapDesc)prefetchDesc;

		/* Copy heap scan state */
 		heap_desc->rs_nblocks = scan->rs_nblocks;
 		heap_desc->start_block = scan->rs_startblock;
						
		/* Set prefetch state */
 		heap_desc->is_startblock_fetched = false;
 		heap_desc->prefetch_size = PREFETCH_PAGE_COUNT;

 		prefetchDesc->next_reqblock = scan->rs_startblock;
 		prefetchDesc->prefetch_end = false;
 		prefetchDesc->requested_io_num = 0;
 		prefetchDesc->complete_io_num = 0;

		while (prefetchDesc->io_req_complete_head)
		{
			ReadBufDesc readbuf_desc;

			Assert(prefetchDesc->io_req_complete_tail);

			/* Remove from complete list */
			readbuf_desc = DequeueReadBufDescFromCompleteList(prefetchDesc);

			/* Append to free list */
			PushReadBufDescToFreeList(prefetchDesc, readbuf_desc);
		}
	}
#endif /* LOCATOR */
}

void
heap_endscan(TableScanDesc sscan)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

	/* Note: no locking manipulations needed */

#ifdef DIVA
	/*
	 * unpin buffers
	 */
	if (sscan->is_heapscan)
	{
		if (BufferIsValid(scan->rs_cbuf))
			ReleaseBuffer(scan->rs_cbuf);
	}
	else
	{
		if (scan->c_buf_id != InvalidEbiSubBuf)
		{
			UnpinEbiSubBuffer(scan->c_buf_id);
			scan->c_buf_id = InvalidEbiSubBuf;
		}
	}

	/* Existing resource cleanup */
	if (scan->unobtained_version_offsets)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		pfree(scan->unobtained_version_offsets);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->unobtained_segments)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		for (int i = 0; i < scan->unobtained_segment_nums; ++i)
			pfree(scan->unobtained_segments[i]);

		pfree(scan->unobtained_segments);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->rs_ctup_copied != NULL)
		heap_freetuple(scan->rs_ctup_copied);

	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
	{
		if (scan->rs_vistuples_copied[i] != NULL)
			heap_freetuple(scan->rs_vistuples_copied[i]);
	}

#ifdef LOCATOR
	if (sscan->rs_prefetch != NULL)
	{
		ReadBufDesc readbuf_desc;
		PrefetchDesc prefetch_desc;

		prefetch_desc = sscan->rs_prefetch;

		if (sscan->is_heapscan)
		{
			/* Release prefetched buffer pages */
			ReleasePrefetchedBuffers(prefetch_desc);
		}
		else
		{
			/* Release prefetched ebi buffer pages */
			EbiReleasePrefetchedBuffers(prefetch_desc);
		}
	
		/* free request freelist */
		readbuf_desc = prefetch_desc->io_req_free; 
		while (readbuf_desc != NULL)
		{
			ReadBufDesc next_readbuf_desc;
			
			next_readbuf_desc = readbuf_desc->next;

			/* return to global area */
			PushReadBufDesc(readbuf_desc);

			readbuf_desc = next_readbuf_desc;	
		}

		/* free request freelist */
		readbuf_desc = prefetch_desc->io_req_complete_head; 
		while (readbuf_desc != NULL)
		{
			ReadBufDesc next_readbuf_desc;
			
			next_readbuf_desc = readbuf_desc->next;

			/* return to global area */
			PushReadBufDesc(readbuf_desc);

			readbuf_desc = next_readbuf_desc;	
		}

		/* exit io_uring queue */
		io_uring_queue_exit(prefetch_desc->ring);

		ResourceOwnerForgetIoUring(CurrentResourceOwner, prefetch_desc->ring);

		/* free resources */
		pfree(prefetch_desc->ring);
		pfree(sscan->rs_prefetch);
	}
#endif /* LOCATOR */
#else
	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);
#endif /* DIVA */

	/*
	 * decrement relation reference count and free scan descriptor storage
	 */
	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	if (scan->rs_parallelworkerdata != NULL)
		pfree(scan->rs_parallelworkerdata);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	pfree(scan);
}

HeapTuple
heap_getnext(TableScanDesc sscan, ScanDirection direction)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
#ifdef DIVA
	bool siro = IsSiro(scan->rs_base.rs_rd);
	bool pagemode = (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);
#endif /* DIVA */
#ifndef LOCATOR
	/*
	 * This is still widely used directly, without going through table AM, so
	 * add a safety check.  It's possible we should, at a later point,
	 * downgrade this to an assert. The reason for checking the AM routine,
	 * rather than the AM oid, is that this allows to write regression tests
	 * that create another AM reusing the heap handler.
	 */
	if (unlikely(sscan->rs_rd->rd_tableam != GetHeapamTableAmRoutine()))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("only heap AM is supported")));
#endif /* !LOCATOR */

	/*
	 * We don't expect direct calls to heap_getnext with valid CheckXidAlive
	 * for catalog or regular tables.  See detailed comments in xact.c where
	 * these variables are declared.  Normally we have such a check at tableam
	 * level API but this is called from many places so we need to ensure it
	 * here.
	 */
	if (unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan))
		elog(ERROR, "unexpected heap_getnext call during logical decoding");

	/* Note: no locking manipulations needed */

#ifdef DIVA
	if (siro)
	{
		if (pagemode)
			heapgettup_pagemode_DIVA(scan, direction, scan->rs_base.rs_nkeys,
									 scan->rs_base.rs_key);
		else
			heapgettup_DIVA(scan, direction, scan->rs_base.rs_nkeys,
							scan->rs_base.rs_key);
	}
	else
	{
		if (pagemode)
			heapgettup_pagemode(scan, direction, scan->rs_base.rs_nkeys,
								scan->rs_base.rs_key);
		else
			heapgettup(scan, direction, scan->rs_base.rs_nkeys,
					   scan->rs_base.rs_key);
	}
#else
	if (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE)
		heapgettup_pagemode(scan, direction,
							scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
	else
		heapgettup(scan, direction,
				   scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
#endif /* DIVA */

	if (scan->rs_ctup.t_data == NULL)
		return NULL;

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

#ifdef DIVA
	if (siro)
	{
		HeapTuple scanned_tuple;

		if (pagemode)
		{
			if (scan->rs_vistuples_copied[scan->rs_cindex])
			{
				scanned_tuple = scan->rs_vistuples_copied[scan->rs_cindex];
			}
			else
			{
				/* 
				 * We get versions from read buffer, then we don't copy the 
				 * version.
				 */
				scanned_tuple = &scan->rs_ctup;
			}
		}
		else
		{
			scanned_tuple = scan->rs_ctup_copied;
			scan->rs_ctup_copied = NULL;
		}

		return scanned_tuple;
	}
	else
		return &scan->rs_ctup;
#else /* !DIVA */
	return &scan->rs_ctup;
#endif /* DIVA */
}

bool
heap_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;

#ifdef DIVA
	Relation relation;
	bool siro;
	bool pagemode = (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);

	relation = scan->rs_base.rs_rd;
	siro = IsSiro(relation);
#endif

	/* Note: no locking manipulations needed */

#ifdef DIVA
	if (siro)
	{
		if (pagemode)
			heapgettup_pagemode_DIVA(scan, direction, scan->rs_base.rs_nkeys,
									 scan->rs_base.rs_key);
		else
			heapgettup_DIVA(scan, direction, scan->rs_base.rs_nkeys,
							scan->rs_base.rs_key);
	}
	else
	{
		if (pagemode)
			heapgettup_pagemode(scan, direction, scan->rs_base.rs_nkeys,
								scan->rs_base.rs_key);
		else
			heapgettup(scan, direction,	scan->rs_base.rs_nkeys,
					   scan->rs_base.rs_key);
	}
#else
	if (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE)
		heapgettup_pagemode(scan, direction,
							scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
	else
		heapgettup(scan, direction,
				   scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
#endif /* DIVA */

	if (scan->rs_ctup.t_data == NULL)
	{
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

#ifdef DIVA
	if (siro)
	{
		HeapTuple scanned_tuple;

		if (pagemode)
		{
			if (scan->rs_vistuples_copied[scan->rs_cindex])
			{
				scanned_tuple = scan->rs_vistuples_copied[scan->rs_cindex];
			}
			else
			{
				/* 
				 * We get versions from read buffer, then we don't copy the 
				 * version.
				 */
				scanned_tuple = &scan->rs_ctup;
			}
		}
		else
		{
			scanned_tuple = scan->rs_ctup_copied;
			scan->rs_ctup_copied = NULL;
		}

		ExecStoreBufferHeapTuple(scanned_tuple, slot, scan->rs_cbuf);
	}
	else
		ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, 
								 scan->rs_cbuf);
#else /* !DIVA */
	ExecStoreBufferHeapTuple(&scan->rs_ctup, slot,
							 scan->rs_cbuf);
#endif /* DIVA */
	return true;
}

void
heap_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
				  ItemPointer maxtid)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	BlockNumber startBlk;
	BlockNumber numBlks;
	ItemPointerData highestItem;
	ItemPointerData lowestItem;

	/*
	 * For relations without any pages, we can simply leave the TID range
	 * unset.  There will be no tuples to scan, therefore no tuples outside
	 * the given TID range.
	 */
	if (scan->rs_nblocks == 0)
		return;

	/*
	 * Set up some ItemPointers which point to the first and last possible
	 * tuples in the heap.
	 */
	ItemPointerSet(&highestItem, scan->rs_nblocks - 1, MaxOffsetNumber);
	ItemPointerSet(&lowestItem, 0, FirstOffsetNumber);

	/*
	 * If the given maximum TID is below the highest possible TID in the
	 * relation, then restrict the range to that, otherwise we scan to the end
	 * of the relation.
	 */
	if (ItemPointerCompare(maxtid, &highestItem) < 0)
		ItemPointerCopy(maxtid, &highestItem);

	/*
	 * If the given minimum TID is above the lowest possible TID in the
	 * relation, then restrict the range to only scan for TIDs above that.
	 */
	if (ItemPointerCompare(mintid, &lowestItem) > 0)
		ItemPointerCopy(mintid, &lowestItem);

	/*
	 * Check for an empty range and protect from would be negative results
	 * from the numBlks calculation below.
	 */
	if (ItemPointerCompare(&highestItem, &lowestItem) < 0)
	{
		/* Set an empty range of blocks to scan */
		heap_setscanlimits(sscan, 0, 0);
		return;
	}

	/*
	 * Calculate the first block and the number of blocks we must scan. We
	 * could be more aggressive here and perform some more validation to try
	 * and further narrow the scope of blocks to scan by checking if the
	 * lowerItem has an offset above MaxOffsetNumber.  In this case, we could
	 * advance startBlk by one.  Likewise, if highestItem has an offset of 0
	 * we could scan one fewer blocks.  However, such an optimization does not
	 * seem worth troubling over, currently.
	 */
	startBlk = ItemPointerGetBlockNumberNoCheck(&lowestItem);

	numBlks = ItemPointerGetBlockNumberNoCheck(&highestItem) -
		ItemPointerGetBlockNumberNoCheck(&lowestItem) + 1;

	/* Set the start block and number of blocks to scan */
	heap_setscanlimits(sscan, startBlk, numBlks);

	/* Finally, set the TID range in sscan */
	ItemPointerCopy(&lowestItem, &sscan->rs_mintid);
	ItemPointerCopy(&highestItem, &sscan->rs_maxtid);
}

bool
heap_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot)
{
	HeapScanDesc scan = (HeapScanDesc) sscan;
	ItemPointer mintid = &sscan->rs_mintid;
	ItemPointer maxtid = &sscan->rs_maxtid;

	/* Note: no locking manipulations needed */
	for (;;)
	{
		if (sscan->rs_flags & SO_ALLOW_PAGEMODE)
			heapgettup_pagemode(scan, direction, sscan->rs_nkeys, sscan->rs_key);
		else
			heapgettup(scan, direction, sscan->rs_nkeys, sscan->rs_key);

		if (scan->rs_ctup.t_data == NULL)
		{
			ExecClearTuple(slot);
			return false;
		}

		/*
		 * heap_set_tidrange will have used heap_setscanlimits to limit the
		 * range of pages we scan to only ones that can contain the TID range
		 * we're scanning for.  Here we must filter out any tuples from these
		 * pages that are outside of that range.
		 */
		if (ItemPointerCompare(&scan->rs_ctup.t_self, mintid) < 0)
		{
			ExecClearTuple(slot);

			/*
			 * When scanning backwards, the TIDs will be in descending order.
			 * Future tuples in this direction will be lower still, so we can
			 * just return false to indicate there will be no more tuples.
			 */
			if (ScanDirectionIsBackward(direction))
				return false;

			continue;
		}

		/*
		 * Likewise for the final page, we must filter out TIDs greater than
		 * maxtid.
		 */
		if (ItemPointerCompare(&scan->rs_ctup.t_self, maxtid) > 0)
		{
			ExecClearTuple(slot);

			/*
			 * When scanning forward, the TIDs will be in ascending order.
			 * Future tuples in this direction will be higher still, so we can
			 * just return false to indicate there will be no more tuples.
			 */
			if (ScanDirectionIsForward(direction))
				return false;
			continue;
		}

		break;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */
	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

	ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->rs_cbuf);
	return true;
}

/*
 *	heap_fetch		- retrieve tuple with given tid
 *
 * On entry, tuple->t_self is the TID to fetch.  We pin the buffer holding
 * the tuple, fill in the remaining fields of *tuple, and check the tuple
 * against the specified snapshot.
 *
 * If successful (tuple found and passes snapshot time qual), then *userbuf
 * is set to the buffer holding the tuple and true is returned.  The caller
 * must unpin the buffer when done with the tuple.
 *
 * If the tuple is not found (ie, item number references a deleted slot),
 * then tuple->t_data is set to NULL, *userbuf is set to InvalidBuffer,
 * and false is returned.
 *
 * If the tuple is found but fails the time qual check, then the behavior
 * depends on the keep_buf parameter.  If keep_buf is false, the results
 * are the same as for the tuple-not-found case.  If keep_buf is true,
 * then tuple->t_data and *userbuf are returned as for the success case,
 * and again the caller must unpin the buffer; but false is returned.
 *
 * heap_fetch does not follow HOT chains: only the exact TID requested will
 * be fetched.
 *
 * It is somewhat inconsistent that we ereport() on invalid block number but
 * return false on invalid item number.  There are a couple of reasons though.
 * One is that the caller can relatively easily check the block number for
 * validity, but cannot check the item number without reading the page
 * himself.  Another is that when we are following a t_ctid link, we can be
 * reasonably confident that the page number is valid (since VACUUM shouldn't
 * truncate off the destination page without having killed the referencing
 * tuple first), but the item number might well not be good.
 */
bool
heap_fetch(Relation relation,
		   Snapshot snapshot,
		   HeapTuple tuple,
		   Buffer *userbuf,
		   bool keep_buf)
{
	ItemPointer tid = &(tuple->t_self);
	ItemId		lp;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	bool		valid;

	/*
	 * Fetch and pin the appropriate page of the relation.
	 */
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

	/*
	 * Need share lock on buffer to examine tuple commit status.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	TestForOldSnapshot(snapshot, relation, page);

	/*
	 * We'd better check for out-of-range offnum in case of VACUUM since the
	 * TID was obtained.
	 */
	offnum = ItemPointerGetOffsetNumber(tid);
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
		*userbuf = InvalidBuffer;
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * get the item line pointer corresponding to the requested tid
	 */
	lp = PageGetItemId(page, offnum);

	/*
	 * Must check for deleted tuple.
	 */
	if (!ItemIdIsNormal(lp))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
		*userbuf = InvalidBuffer;
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * fill in *tuple fields
	 */
	tuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tuple->t_len = ItemIdGetLength(lp);
	tuple->t_tableOid = RelationGetRelid(relation);

	/*
	 * check tuple visibility, then release lock
	 */
	valid = HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);

	if (valid)
		PredicateLockTID(relation, &(tuple->t_self), snapshot,
						 HeapTupleHeaderGetXmin(tuple->t_data));

	HeapCheckForSerializableConflictOut(valid, relation, tuple, buffer, snapshot);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (valid)
	{
		/*
		 * All checks passed, so return the tuple as valid. Caller is now
		 * responsible for releasing the buffer.
		 */
		*userbuf = buffer;

		return true;
	}

	/* Tuple failed time qual, but maybe caller wants to see it anyway. */
	if (keep_buf)
		*userbuf = buffer;
	else
	{
		ReleaseBuffer(buffer);
		*userbuf = InvalidBuffer;
		tuple->t_data = NULL;
	}

	return false;
}

#ifdef LOCATOR
/*
 * LocatorRouteSynopsisGetTuplePositionForIndexscan
 */
void
LocatorRouteSynopsisGetTuplePositionForIndexscan(LocatorExternalCatalog *exCatalog, 
												 Relation relation, 
												 LocatorRouteSynopsis routeSynopsis,
												 LocatorTuplePosition tuplePosition)
{
	int startLevel = 0;
	int level;
	int levelNums = relation->rd_locator_level_count;
	int partitionId = routeSynopsis->rec_key.partKey;
	LocatorPartNumber partNum;
	LocatorPartNumber partitionNumbers[32];
	LocatorPartGenNo partitionGenerationNumbers[32];
	LocatorSeqNum seqNum;
	LocationInfo locationInfos[32];

	tuplePosition->rec_key = routeSynopsis->rec_key;
	seqNum = routeSynopsis->rec_key.seqnum;

	/* Find partition number level by level. */
	for (level = levelNums - 1, partNum = partitionId; level >= 0;
		 	--level, partNum /= relation->rd_locator_spread_factor)
	{
		/* Save target partition number from bottom. */
		partitionNumbers[level] = partNum;

		if (level == 0)
		{
			locationInfos[level].firstSequenceNumber =
				exCatalog->ReorganizedRecordCountsLevelZero[partNum].val;
		}
		else if (level + 1 != levelNums)
		{
			locationInfos[level] = exCatalog->locationInfo[level][partNum];
		}

		/* We try to find start level to optimize location calculation logic. */
		if (level + 1 != levelNums &&
				locationInfos[level].firstSequenceNumber != 0xFFFFFFFFFF && 
				seqNum < locationInfos[level].firstSequenceNumber)
		{
			startLevel = level + 1;
			break;
		}
	}

	for (level = startLevel; level < levelNums; ++level)
	{
		/* Increment target reference count. */
		IncrMetaDataCount(
			LocatorExternalCatalogRefCounter(exCatalog, level, partitionNumbers[level]));

		/* Get partition generation number and sum of repartitioned record count. */
		partNum = partitionNumbers[level];

		if (level == 0)
		{
			locationInfos[level].firstSequenceNumber =
				exCatalog->ReorganizedRecordCountsLevelZero[partNum].val;
			partitionGenerationNumbers[level] =
				routeSynopsis->rec_pos.first_lvl_tierednum;
		}
		else if (level + 1 == levelNums)
		{
			locationInfos[level].firstSequenceNumber = 0;
			partitionGenerationNumbers[level] = 
				LocatorGetPartGenerationNumber(exCatalog, level, partNum);
		}
		else
		{
			locationInfos[level] = exCatalog->locationInfo[level][partNum];
			partitionGenerationNumbers[level] = 
				LocatorGetPartGenerationNumber(exCatalog, level, partNum);
		}

		/* Decrement target reference count. */
		DecrMetaDataCount(
			LocatorExternalCatalogRefCounter(exCatalog, level, partitionNumbers[level]));

		/* 
		 * Fix location of the tuple, comparing the sequence number and 
		 * in-partition cumulative count.
		 */
		if (seqNum >= locationInfos[level].firstSequenceNumber)
		{
			/* 
			 * We get target position here. Save these information to acces 
			 * the tuple.
			 */
			tuplePosition->partitionLevel = level;
			tuplePosition->partitionNumber = partitionNumbers[level];
			tuplePosition->partitionGenerationNumber = 
											partitionGenerationNumbers[level];
	
			if (level == 0)
			{
				/* If the record is in level 0, use level 0 offset. */
				tuplePosition->partitionTuplePosition =
					routeSynopsis->rec_pos.first_lvl_pos;
			}
			else if (level + 1 == levelNums)
			{
				/* The record is in last level, use last level offset. */
				tuplePosition->partitionTuplePosition =
					routeSynopsis->rec_pos.last_lvl_pos;
			}
			else
			{
				/*
				 * Or not, use diff.
				 * (size of offset is 3-bytes and allows wrap around)
				 */
				tuplePosition->partitionTuplePosition =
					(routeSynopsis->rec_pos.in_partition_counts[level - 1].cumulative_count - 
						locationInfos[level].reorganizedRecordsCount) & CUMULATIVECOUNT_MASK;
			}

			break;
		}
		else
		{
			/* This level is not our target in here. */
			Assert(level != (levelNums - 1));
		}
	}
}

/*
 * LocatorRouteSynopsisGetTuplePositionForUpdate
 */
void
LocatorRouteSynopsisGetTuplePositionForUpdate(LocatorExternalCatalog *exCatalog, 
											  Relation relation, 
											  LocatorRouteSynopsis routeSynopsis,
											  LocatorTuplePosition tuplePosition)
{
	int startLevel = 0;
	int levelNums = relation->rd_locator_level_count;
	int partitionId = routeSynopsis->rec_key.partKey;
	volatile int level;
	LocatorPartNumber partNum;
	LocatorPartNumber partitionNumbers[32];
	LocatorPartGenNo partitionGenerationNumbers[32];
	uint64 seqNum;
	LocationInfo locationInfos[32];

	tuplePosition->rec_key = routeSynopsis->rec_key;
	seqNum = routeSynopsis->rec_key.seqnum;

	/* Find partition number level by level. */
	for (level = levelNums - 1, partNum = partitionId; level >= 0;
		 	--level, partNum /= relation->rd_locator_spread_factor)
	{
		/* Save target partition number from bottom. */
		partitionNumbers[level] = partNum;

		if (level == 0)
		{
			locationInfos[level].firstSequenceNumber =
				exCatalog->ReorganizedRecordCountsLevelZero[partNum].val;
		}
		else if (level + 1 != levelNums)
		{
			locationInfos[level] = exCatalog->locationInfo[level][partNum];
		}

		/* We try to find start level to optimize location calculation logic. */
		if (level + 1 != levelNums && 
				locationInfos[level].firstSequenceNumber != 0xFFFFFFFFFF && 
				seqNum < locationInfos[level].firstSequenceNumber)
		{
			startLevel = level + 1;
			break;
		}
	}

	for (level = startLevel; level < levelNums; ++level)
	{
		/* Increment target reference count. */
		IncrMetaDataCount(LocatorExternalCatalogRefCounter(exCatalog, level, partitionNumbers[level]));

		/* 
		 * Get partition generation number and sum of repartitioned record 
		 * count. 
		 */
		partNum = partitionNumbers[level];

		if (level == 0)
		{
			locationInfos[level].firstSequenceNumber = 
				exCatalog->ReorganizedRecordCountsLevelZero[partNum].val;
			partitionGenerationNumbers[level] = 
				routeSynopsis->rec_pos.first_lvl_tierednum;
		}
		else if (level + 1 == levelNums)
		{
			locationInfos[level].firstSequenceNumber = 0;
			partitionGenerationNumbers[level] = 
				LocatorGetPartGenerationNumber(exCatalog, level, partNum);
		}
		else
		{
			locationInfos[level] = exCatalog->locationInfo[level][partNum];
			partitionGenerationNumbers[level] = 
				LocatorGetPartGenerationNumber(exCatalog, level, partNum);
		}

		/* Decrement target reference count. */
		DecrMetaDataCount(LocatorExternalCatalogRefCounter(exCatalog, level, partitionNumbers[level]));

		/* 
		 * Fix location of the tuple, comparing the sum of repartitioned tuple 
		 * number.
		 */
		if (seqNum >= locationInfos[level].firstSequenceNumber)
		{
			LocatorPartLevel partitionLevel = level;
			LocatorPartNumber partitionNumber = partitionNumbers[level];
			bool needDecrRefCount = false;
			LocatorModificationMethod modificationMethod;

			if (level + 1 != levelNums)
			{
				pg_atomic_uint64 *targetPartitionRefCount =
					LocatorExternalCatalogRefCounter(exCatalog, partitionLevel, partitionNumber);
				LocatorSeqNum firstSeqNum;

				/* In here, it's not last level. */

				IncrMetaDataCount(targetPartitionRefCount);

				/* Get repartitioned record count and cumulative record count */
				if (level == 0)
				{
					firstSeqNum = 
						exCatalog->ReorganizedRecordCountsLevelZero[partitionNumber].val;
				}
				else
				{
					firstSeqNum = 
						exCatalog->locationInfo[level][partitionNumber].firstSequenceNumber;
					partitionGenerationNumbers[level] = 
						LocatorGetPartGenerationNumber(exCatalog, level, 
													partitionNumbers[level]);
				}

				/* Check tuple position. */
				if (seqNum >= firstSeqNum)
				{
					/* The update worker is in candidate partition. */

					/* Get modification method. */
					modificationMethod =
						GetModificationMethod(exCatalog, targetPartitionRefCount, 
												partitionLevel, partitionNumber);
					pg_memory_barrier();
						
					/* Decrement meta data count. */
					DecrMetaDataCount(targetPartitionRefCount);

					if (modificationMethod == NORMAL_MODIFICATION ||
							modificationMethod == MODIFICATION_WITH_DELTA)
					{
						/* We need to decrement refcounter after update. */
						needDecrRefCount = true;
					}
					else
					{
						/* In blocked modification, we need to go downside. */
						Assert(modificationMethod == MODIFICATION_BLOCKING);

						/* Wait until repartitioning end. */
						while (pg_atomic_read_u64(targetPartitionRefCount) &
										 REPARTITIONING_MODIFICATION_BLOCKING)
						{
							SPIN_DELAY();
						}

						/* 
						 * Navigate to child of current partition, finding correct 
						 * partition number.
						 */
						continue;
					}

					/* In here, we can get tuple position safely. */
				}
				else
				{
					DecrMetaDataCount(targetPartitionRefCount);
		
					/* 
					 * Navigate to child of current partition, finding correct 
					 * partition number.
					 */
					continue;
				}
			}
			else
			{
				/* 
				 * In here, it's last level. We can get tuple position safely. 
				 */
			}

			pg_memory_barrier();

			/* 
			 * We get target position here. Save these information to acces 
			 * the tuple.
			 */
			tuplePosition->partitionLevel = level;
			tuplePosition->partitionNumber = partitionNumbers[level];
			tuplePosition->partitionGenerationNumber = 
											partitionGenerationNumbers[level];
			tuplePosition->needDecrRefCount = needDecrRefCount;
			
			if (level == 0)
			{
				/* If the record is in level 0, use level 0 offset. */
				tuplePosition->partitionTuplePosition =
					routeSynopsis->rec_pos.first_lvl_pos;
			}
			else if (level + 1 == levelNums)
			{
				/* The record is in last level, use last level offset. */
				tuplePosition->partitionTuplePosition =
					routeSynopsis->rec_pos.last_lvl_pos;
			}
			else
			{
				/*
				 * Or not, use diff.
				 * (size of offset is 3-bytes and allows wrap around)
				 */
				tuplePosition->partitionTuplePosition =
					(routeSynopsis->rec_pos.in_partition_counts[level - 1].cumulative_count - 
						locationInfos[level].reorganizedRecordsCount) & CUMULATIVECOUNT_MASK;
				Assert(routeSynopsis->rec_pos.in_partition_counts[level - 1].cumulative_count >=
							locationInfos[level].reorganizedRecordsCount);
			}

			if (needDecrRefCount)
			{
				int lowerLevel = level + 1;

				Assert(level + 1 != levelNums && lowerLevel > 0);
			
				tuplePosition->modificationMethod = modificationMethod;
				tuplePosition->partitionId = partitionId;

				if (lowerLevel + 1 == levelNums)
				{
					/* If the record is in last level, use last level offset. */
					tuplePosition->lowerPartitionTuplePosition =
						routeSynopsis->rec_pos.last_lvl_pos;
				}
				else
				{
					/*
					 * Or not, use diff.
					 * (size of offset is 3-bytes and allows wrap around)
					 */
					tuplePosition->lowerPartitionTuplePosition =
						(routeSynopsis->rec_pos.in_partition_counts[lowerLevel - 1].cumulative_count - 
							locationInfos[lowerLevel].reorganizedRecordsCount) & CUMULATIVECOUNT_MASK;
					Assert(routeSynopsis->rec_pos.in_partition_counts[lowerLevel - 1].cumulative_count >=
							locationInfos[lowerLevel].reorganizedRecordsCount);
				}
			}
	
			return;
		}
		else
		{
			Assert(level != (levelNums - 1));
			/* This level is not our target in here. */
		}
	}
}

/*
 * locator_search_version - search version satisfying snapshot
 */
bool
locator_search_version(LocatorTuplePosition tuplePosition, 
					   Relation relation, 
					   Buffer buffer, Snapshot snapshot,
					   HeapTuple heapTuple,
					   HeapTuple *copied_tuple,
					   DualRefDesc drefDesc,
					   bool *all_dead, bool first_call,
					   IndexFetchHeapData *hscan,
					   BlockNumber locator_tuple_blocknum,
					   uint16 locator_tuple_position_in_block)
{
	Page		dp = (Page) BufferGetPage(buffer);
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		valid;
	bool		is_updated;
	Oid			rel_id;

	/* For lookup version in our system */
	ItemId		lp;
	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;

	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;

	BlockNumber		v_block;
	OffsetNumber	v_offset;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;

	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	/*
	 * Column oriented tuple can has different block number.
	 */
	if (tuplePosition == NULL)
		blkno = locator_tuple_blocknum;
	else
		blkno = tuplePosition->partitionTuplePosition / relation->records_per_block;

	/*
	 * Column oriented tuple can has different offset number.
	 */
	if (tuplePosition == NULL)
		offnum = locator_tuple_position_in_block * 3 + 1;
	else
		offnum = 
		(tuplePosition->partitionTuplePosition % 
				relation->records_per_block) * 3 + 1;

	if (hscan && hscan->xs_c_ebi_buf_id != InvalidEbiSubBuf)
	{
		UnpinEbiSubBuffer(hscan->xs_c_ebi_buf_id);
		hscan->xs_c_ebi_buf_id = InvalidEbiSubBuf;
	}

	/* check for bogus TID */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
		return false;

	lp = PageGetItemId(dp, offnum);

	/* check for unused, dead, or redirect items */
	if (!ItemIdIsNormal(lp))
	{
		Abort("item id is not normal");
		elog(ERROR, "item id is not normal");
		return false;
	}

	if (!LP_IS_PLEAF_FLAG(lp))
	{
		Abort("item id is not p-locator");
		elog(ERROR, "item id is not p-locator");
		return false;
	}

	rel_id = RelationGetRelid(relation);
	heapTuple->t_tableOid = rel_id;

	/* Get p-locator of this record */
	p_locator = (Item) PageGetItem(dp, lp);

	/* Set self pointer of tuple */
	ItemPointerSet(&(heapTuple->t_self), blkno, offnum);

	/* Init checking variables */
	valid = false;
	is_updated = true;
	v_buffer = buffer;

	/* Increment reference count and determine where to check */
	GetCheckVar(drefDesc, &rec_ref_unit, &check_var,
				&set_hint_bits, offnum);

	/* Check visibility of left, right versions with check_var */
	for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
	{
		/* Is this version need to be checked? */
		if (!(check_var & test_var))
			continue;

		/* Release previous version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

		/* Set block number and offset */
		v_block = blkno;
		v_offset = offnum + test_var;

		Assert(BlockNumberIsValid(v_block));
		Assert(OffsetNumberIsValid(v_offset));

		/*
		 * If the version is in the same page with p-locator, just get it.
		 * Or not, read the buffer that it is in.
		 */
		if (likely(v_block == blkno))
		{
			v_buffer = buffer;
			v_dp = dp;
		}
		else
		{
			Assert(false);

			v_buffer = ReadBuffer(relation, v_block);
			Assert(BufferIsValid(v_buffer));
			v_dp = BufferGetPage(v_buffer);
		}

		v_lpp = PageGetItemId(v_dp, v_offset);

		/* The target has never been updated after INSERT */
		if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
		{
			is_updated = false;
			continue;
		}
			
		heapTuple->t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
		heapTuple->t_len = ItemIdGetLength(v_lpp);

		/* Set buffer to set hint bits */
		hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

		/* Check visibility of version */
		valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot,
											 hint_bits_buf);

		if (valid)
			break;
	}

	/* Both left and right-side versions are invisible */
	if (valid || (modification && is_updated))
	{
		/*
		 * If this scanning is for update, we don't need to bother
		 * searching deeply.
		 */
		
		if (hscan)
		{
			HeapTuple recycling_tuple = NULL;

			if (tuple_size_available_for_indexscan(hscan, heapTuple))
			{
				/* We can reuse the existing tuple structure. */
				recycling_tuple = hscan->xs_vistuple_free;
			}
			else
			{
				HeapTuple tuple;
				int aligned_tuple_size;
		
				/* 
				 * We cannot reuse the existing tuple structure. In such 
				 * cases, we reallocate a new structure to fetch a version. 
				 */
				aligned_tuple_size = 
								1 << my_log2(HEAPTUPLESIZE + heapTuple->t_len);
				tuple = (HeapTuple) palloc(aligned_tuple_size);

				remove_redundant_tuple_for_indexscan(hscan);

				hscan->xs_vistuple_free = tuple;
				hscan->xs_vistuple_size = aligned_tuple_size;

				recycling_tuple = tuple;
			}

			heap_copytuple_with_recycling(heapTuple, recycling_tuple);

			*copied_tuple = recycling_tuple;
		}
		else
		{
			if (*copied_tuple != NULL)
				heap_freetuple(*copied_tuple);

			/* Copy visible tuple */
			*copied_tuple = heap_copytuple(heapTuple);
		}

		if (all_dead)
			*all_dead = false;

		/* Release version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);
		Assert((snapshot->snapshot_type != SNAPSHOT_DIRTY) || snapshot->xmin != InvalidTransactionId);
		return true;
	}

	/* Release version buffer */
	if (unlikely(v_buffer != buffer))
		ReleaseBuffer(v_buffer);

	/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
	if (snapshot->snapshot_type != SNAPSHOT_MVCC || modification)
		goto failed;

	/*
	 * Both versions are invisible to this transaction.
	 * Need to search from p-leaf & ebi-tree.
	 */
	l_off = PLocatorGetLeftOffset(p_locator);
	r_off = PLocatorGetRightOffset(p_locator);
	xid_bound = PLocatorGetXIDBound(p_locator);

	if (l_off == 0 && r_off == 0)
	{
		ret_id = -1;
	}
	else
	{
		PLeafOffset p_offset;

		if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
			p_offset = l_off;
		else
			p_offset = r_off;

		ret_id = PLeafLookupTuple(rel_id,
								  false,
								  NULL,
                				  p_offset, 
                				  snapshot, 
								  &(heapTuple->t_len), 
                				  (void**) &(heapTuple->t_data));
	}

	if (ret_id != -1)
	{
		if (hscan)
		{
			*copied_tuple = heapTuple;
			
			/* Store current ebi page id */
			hscan->xs_c_ebi_buf_id = ret_id;
		}
		else
		{
			if (*copied_tuple != NULL)
				heap_freetuple(*copied_tuple);

			*copied_tuple = heap_copytuple(heapTuple);

			/* Unpin a EBI sub page */
			UnpinEbiSubBuffer(ret_id);
		}

		if (all_dead)
			*all_dead = false;

		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);

		return true;
	}

failed:
	/* Decrement ref_cnt */
	if (rec_ref_unit != 0)
		DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);

	return false;
}
#endif /* LOCATOR */

#ifdef DIVA
/*
 *	heap_hot_search_buffer_with_vc	- search version satisfying snapshot
 *
 * On entry, *tid is the TID of a p-locator, and buffer is the buffer holding
 * this.  We check left, right versions, and traverse ebi-tree to search for the
 * version satisfying the given snapshot. If target is found, return true. If no
 * match, return false.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here. If no match is found, the contents of this buffer on return
 * are undefined.
 *
 * Unlike heap_fetch, the caller must already have pin on the buffer; it is
 * still pinned at exit.  Also unlike heap_fetch, we do not report any pgstats
 * count; caller may do so if wanted.
 */
#ifdef LOCATOR
bool
heap_hot_search_buffer_with_vc(ItemPointer tid, Relation relation, 
							   Buffer buffer, Snapshot snapshot,
							   HeapTuple heapTuple,
							   HeapTuple *copied_tuple,
							   DualRefDesc drefDesc,
							   bool *all_dead, bool first_call,
							   IndexFetchHeapData *hscan)
#else /* !LOCATOR */
bool
heap_hot_search_buffer_with_vc(ItemPointer tid, Relation relation, 
							   Buffer buffer, Snapshot snapshot,
							   HeapTuple heapTuple,
							   HeapTuple *copied_tuple,
							   bool *all_dead, bool first_call,
							   IndexFetchHeapData *hscan)
#endif /* LOCATOR */
{
	Page		dp = (Page) BufferGetPage(buffer);
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		valid;
	bool		is_updated;
	Oid			rel_id;

	/* For lookup version in our system */
	ItemId		lp;
	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;

#ifdef LOCATOR
	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;
#endif /* LOCATOR */

	BlockNumber		v_block;
	OffsetNumber	v_offset;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;

	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);

	Assert(BufferGetBlockNumber(buffer) == blkno);

	if (hscan && hscan->xs_c_ebi_buf_id != InvalidEbiSubBuf)
	{
		UnpinEbiSubBuffer(hscan->xs_c_ebi_buf_id);
		hscan->xs_c_ebi_buf_id = InvalidEbiSubBuf;
	}

	/* check for bogus TID */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
		return false;

	lp = PageGetItemId(dp, offnum);

	/* check for unused, dead, or redirect items */
	if (!ItemIdIsNormal(lp))
	{
		Abort("item id is not normal");
		elog(ERROR, "item id is not normal");
		return false;
	}

	if (!LP_IS_PLEAF_FLAG(lp))
	{
		Abort("item id is not p-locator");
		elog(ERROR, "item id is not p-locator");
		return false;
	}

	rel_id = RelationGetRelid(relation);
	heapTuple->t_tableOid = rel_id;

	/* Get p-locator of this record */
	p_locator = (Item) PageGetItem(dp, lp);

	/* Set self pointer of tuple */
	ItemPointerSet(&(heapTuple->t_self), blkno, offnum);

	/* Init checking variables */
	valid = false;
	is_updated = true;
	v_buffer = buffer;

#ifdef LOCATOR
	/* Increment reference count and determine where to check */
	GetCheckVar(drefDesc, &rec_ref_unit, &check_var,
				&set_hint_bits, offnum);
#endif /* LOCATOR */

	/* Check visibility of left, right versions with check_var */
	for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
	{
#ifdef LOCATOR
		/* Is this version need to be checked? */
		if (!(check_var & test_var))
			continue;
#endif /* LOCATOR */

		/* Release previous version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

		/* Set block number and offset */
		v_block = blkno;
		v_offset = offnum + test_var; 

		Assert(BlockNumberIsValid(v_block));
		Assert(OffsetNumberIsValid(v_offset));

		/*
		 * If the version is in the same page with p-locator, just get it.
		 * Or not, read the buffer that it is in.
		 */
		if (likely(v_block == blkno))
		{
			v_buffer = buffer;
			v_dp = dp;
		}
		else
		{
			v_buffer = ReadBuffer(relation, v_block);
			Assert(BufferIsValid(v_buffer));
			v_dp = BufferGetPage(v_buffer);
		}

		v_lpp = PageGetItemId(v_dp, v_offset);

		/* The target has never been updated after INSERT */
		if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
		{
			is_updated = false;
			continue;
		}
			
		heapTuple->t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
		heapTuple->t_len = ItemIdGetLength(v_lpp);

#ifdef LOCATOR
		/* Set buffer to set hint bits */
		hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

		/* Check visibility of version */
		valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot,
											 hint_bits_buf);
#else /* !LOCATOR */
		/* Check visibility of version */
		valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot,
											 v_buffer);
#endif /* LOCATOR */

		if (valid)
			break;
	}

	/* Both left and right-side versions are invisible */
	if (valid || (modification && is_updated))
	{
		/*
		 * If this scanning is for update, we don't need to bother
		 * searching deeply.
		 */
		
		if (hscan)
		{
			HeapTuple recycling_tuple = NULL;

			if (tuple_size_available_for_indexscan(hscan, heapTuple))
			{
				/* We can reuse the existing tuple structure. */
				recycling_tuple = hscan->xs_vistuple_free;
			}
			else
			{
				HeapTuple tuple;
				int aligned_tuple_size;
		
				/* 
				 * We cannot reuse the existing tuple structure. In such 
				 * cases, we reallocate a new structure to fetch a version. 
				 */
				aligned_tuple_size = 
								1 << my_log2(HEAPTUPLESIZE + heapTuple->t_len);
				tuple = (HeapTuple) palloc(aligned_tuple_size);

				remove_redundant_tuple_for_indexscan(hscan);

				hscan->xs_vistuple_free = tuple;
				hscan->xs_vistuple_size = aligned_tuple_size;

				recycling_tuple = tuple;
			}

			heap_copytuple_with_recycling(heapTuple, recycling_tuple);

			*copied_tuple = recycling_tuple;
		}
		else
		{
			if (*copied_tuple != NULL)
				heap_freetuple(*copied_tuple);

			/* Copy visible tuple */
			*copied_tuple = heap_copytuple(heapTuple);
		}

		if (all_dead)
			*all_dead = false;

		/* Release version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

#ifdef LOCATOR
		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);
#endif /* LOCATOR */

		return true;
	}

	/* Release version buffer */
	if (unlikely(v_buffer != buffer))
		ReleaseBuffer(v_buffer);

	/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
	if (snapshot->snapshot_type != SNAPSHOT_MVCC || modification)
#ifdef LOCATOR
		goto failed;
#else /* !LOCATOR */
		return false;
#endif /* LOCATOR */

	/*
	 * Both versions are invisible to this transaction.
	 * Need to search from p-leaf & ebi-tree.
	 */
	l_off = PLocatorGetLeftOffset(p_locator);
	r_off = PLocatorGetRightOffset(p_locator);
	xid_bound = PLocatorGetXIDBound(p_locator);

	if (l_off == 0 && r_off == 0)
	{
		ret_id = -1;
	}
	else
	{
		PLeafOffset p_offset;

		if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
			p_offset = l_off;
		else
			p_offset = r_off;

		ret_id = PLeafLookupTuple(rel_id,
								  false,
								  NULL,
                				  p_offset, 
                				  snapshot, 
								  &(heapTuple->t_len), 
                				  (void**) &(heapTuple->t_data));
	}

	if (ret_id != -1)
	{
		if (hscan)
		{
			*copied_tuple = heapTuple;
			
			/* Store current ebi page id */
			hscan->xs_c_ebi_buf_id = ret_id;
		}
		else
		{
			if (*copied_tuple != NULL)
				heap_freetuple(*copied_tuple);

			*copied_tuple = heap_copytuple(heapTuple);

			/* Unpin a EBI sub page */
			UnpinEbiSubBuffer(ret_id);
		}

		if (all_dead)
			*all_dead = false;

#ifdef LOCATOR
		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);
#endif /* LOCATOR */

		return true;
	}

#ifdef LOCATOR
failed:
	/* Decrement ref_cnt */
	if (rec_ref_unit != 0)
		DRefDecrRefCnt(drefDesc->dual_ref, rec_ref_unit);
#endif /* LOCATOR */

	return false;
}
#endif /* DIVA */
/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return true.  If no match, return false without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set true if all members of the HOT chain
 * are vacuumable, false if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call)
{
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		at_chain_start;
	bool		valid;
	bool		skip;
	GlobalVisState *vistest = NULL;

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);
	at_chain_start = first_call;
	skip = !first_call;

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));
	Assert(BufferGetBlockNumber(buffer) == blkno);

	/* Scan through possible multiple members of HOT-chain */
	for (;;)
	{
		ItemId		lp;

		/* check for bogus TID */
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
			break;

		lp = PageGetItemId(dp, offnum);

		/* check for unused, dead, or redirected items */
		if (!ItemIdIsNormal(lp))
		{
			/* We should only see a redirect at start of chain */
			if (ItemIdIsRedirected(lp) && at_chain_start)
			{
				/* Follow the redirect */
				offnum = ItemIdGetRedirect(lp);
				at_chain_start = false;
				continue;
			}
			/* else must be end of chain */
			break;
		}

		/*
		 * Update heapTuple to point to the element of the HOT chain we're
		 * currently investigating. Having t_self set correctly is important
		 * because the SSI checks and the *Satisfies routine for historical
		 * MVCC snapshots need the correct tid to decide about the visibility.
		 */
		heapTuple->t_data = (HeapTupleHeader) PageGetItem(dp, lp);
		heapTuple->t_len = ItemIdGetLength(lp);
		heapTuple->t_tableOid = RelationGetRelid(relation);
		ItemPointerSet(&heapTuple->t_self, blkno, offnum);

		/*
		 * Shouldn't see a HEAP_ONLY tuple at chain start.
		 */
		if (at_chain_start && HeapTupleIsHeapOnly(heapTuple))
			break;

		/*
		 * The xmin should match the previous xmax value, else chain is
		 * broken.
		 */
		if (TransactionIdIsValid(prev_xmax) &&
			!TransactionIdEquals(prev_xmax,
								 HeapTupleHeaderGetXmin(heapTuple->t_data)))
			break;

		/*
		 * When first_call is true (and thus, skip is initially false) we'll
		 * return the first tuple we find.  But on later passes, heapTuple
		 * will initially be pointing to the tuple we returned last time.
		 * Returning it again would be incorrect (and would loop forever), so
		 * we skip it and return the next match we find.
		 */
		if (!skip)
		{
			/* If it's visible per the snapshot, we must return it */
			valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
			HeapCheckForSerializableConflictOut(valid, relation, heapTuple,
												buffer, snapshot);

			if (valid)
			{
				ItemPointerSetOffsetNumber(tid, offnum);
				PredicateLockTID(relation, &heapTuple->t_self, snapshot,
								 HeapTupleHeaderGetXmin(heapTuple->t_data));
				if (all_dead)
					*all_dead = false;
				return true;
			}
		}
		skip = false;

		/*
		 * If we can't see it, maybe no one else can either.  At caller
		 * request, check whether all chain members are dead to all
		 * transactions.
		 *
		 * Note: if you change the criterion here for what is "dead", fix the
		 * planner's get_actual_variable_range() function to match.
		 */
		if (all_dead && *all_dead)
		{
			if (!vistest)
				vistest = GlobalVisTestFor(relation);

			if (!HeapTupleIsSurelyDead(heapTuple, vistest))
				*all_dead = false;
		}

		/*
		 * Check to see if HOT chain continues past this tuple; if so fetch
		 * the next offnum and loop around.
		 */
		if (HeapTupleIsHotUpdated(heapTuple))
		{
			Assert(ItemPointerGetBlockNumber(&heapTuple->t_data->t_ctid) ==
				   blkno);
			offnum = ItemPointerGetOffsetNumber(&heapTuple->t_data->t_ctid);
			at_chain_start = false;
			prev_xmax = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
		}
		else
			break;				/* end of chain */
	}

	return false;
}

/*
 *	heap_get_latest_tid -  get the latest tid of a specified tuple
 *
 * Actually, this gets the latest version that is visible according to the
 * scan's snapshot.  Create a scan using SnapshotDirty to get the very latest,
 * possibly uncommitted version.
 *
 * *tid is both an input and an output parameter: it is updated to
 * show the latest version of the row.  Note that it will not be changed
 * if no version of the row passes the snapshot test.
 */
void
heap_get_latest_tid(TableScanDesc sscan,
					ItemPointer tid)
{
	Relation	relation = sscan->rs_rd;
	Snapshot	snapshot = sscan->rs_snapshot;
	ItemPointerData ctid;
	TransactionId priorXmax;

	/*
	 * table_tuple_get_latest_tid() verified that the passed in tid is valid.
	 * Assume that t_ctid links are valid however - there shouldn't be invalid
	 * ones in the table.
	 */
	Assert(ItemPointerIsValid(tid));

	/*
	 * Loop to chase down t_ctid links.  At top of loop, ctid is the tuple we
	 * need to examine, and *tid is the TID we will return if ctid turns out
	 * to be bogus.
	 *
	 * Note that we will loop until we reach the end of the t_ctid chain.
	 * Depending on the snapshot passed, there might be at most one visible
	 * version of the row, but we don't try to optimize for that.
	 */
	ctid = *tid;
	priorXmax = InvalidTransactionId;	/* cannot check first XMIN */
	for (;;)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber offnum;
		ItemId		lp;
		HeapTupleData tp;
		bool		valid;

		/*
		 * Read, pin, and lock the page.
		 */
		buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&ctid));
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		TestForOldSnapshot(snapshot, relation, page);

		/*
		 * Check for bogus item number.  This is not treated as an error
		 * condition because it can happen while following a t_ctid link. We
		 * just assume that the prior tid is OK and return it unchanged.
		 */
		offnum = ItemPointerGetOffsetNumber(&ctid);
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
		{
			UnlockReleaseBuffer(buffer);
			break;
		}
		lp = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(lp))
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		/* OK to access the tuple */
		tp.t_self = ctid;
		tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
		tp.t_len = ItemIdGetLength(lp);
		tp.t_tableOid = RelationGetRelid(relation);

		/*
		 * After following a t_ctid link, we might arrive at an unrelated
		 * tuple.  Check for XMIN match.
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(tp.t_data)))
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * Check tuple visibility; if visible, set it as the new result
		 * candidate.
		 */
		valid = HeapTupleSatisfiesVisibility(&tp, snapshot, buffer);
		HeapCheckForSerializableConflictOut(valid, relation, &tp, buffer, snapshot);
		if (valid)
			*tid = ctid;

		/*
		 * If there's a valid t_ctid link, follow it, else we're done.
		 */
		if ((tp.t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HeapTupleHeaderIsOnlyLocked(tp.t_data) ||
			HeapTupleHeaderIndicatesMovedPartitions(tp.t_data) ||
			ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid))
		{
			UnlockReleaseBuffer(buffer);
			break;
		}

		ctid = tp.t_data->t_ctid;
		priorXmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
		UnlockReleaseBuffer(buffer);
	}							/* end of loop */
}


/*
 * UpdateXmaxHintBits - update tuple hint bits after xmax transaction ends
 *
 * This is called after we have waited for the XMAX transaction to terminate.
 * If the transaction aborted, we guarantee the XMAX_INVALID hint bit will
 * be set on exit.  If the transaction committed, we set the XMAX_COMMITTED
 * hint bit if possible --- but beware that that may not yet be possible,
 * if the transaction committed asynchronously.
 *
 * Note that if the transaction was a locker only, we set HEAP_XMAX_INVALID
 * even if it commits.
 *
 * Hence callers should look only at XMAX_INVALID.
 *
 * Note this is not allowed for tuples whose xmax is a multixact.
 */
static void
UpdateXmaxHintBits(HeapTupleHeader tuple, Buffer buffer, TransactionId xid)
{
	Assert(TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple), xid));
	Assert(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));

	if (!(tuple->t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)))
	{
		if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) &&
			TransactionIdDidCommit(xid))
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
								 xid);
		else
			HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
								 InvalidTransactionId);
	}
}


/*
 * GetBulkInsertState - prepare status object for a bulk insert
 */
BulkInsertState
GetBulkInsertState(void)
{
	BulkInsertState bistate;

	bistate = (BulkInsertState) palloc(sizeof(BulkInsertStateData));
	bistate->strategy = GetAccessStrategy(BAS_BULKWRITE);
	bistate->current_buf = InvalidBuffer;
	return bistate;
}

/*
 * FreeBulkInsertState - clean up after finishing a bulk insert
 */
void
FreeBulkInsertState(BulkInsertState bistate)
{
	if (bistate->current_buf != InvalidBuffer)
		ReleaseBuffer(bistate->current_buf);
	FreeAccessStrategy(bistate->strategy);
	pfree(bistate);
}

/*
 * ReleaseBulkInsertStatePin - release a buffer currently held in bistate
 */
void
ReleaseBulkInsertStatePin(BulkInsertState bistate)
{
	if (bistate->current_buf != InvalidBuffer)
		ReleaseBuffer(bistate->current_buf);
	bistate->current_buf = InvalidBuffer;
}


/*
 *	heap_insert		- insert tuple into a heap
 *
 * The new tuple is stamped with current transaction ID and the specified
 * command ID.
 *
 * See table_tuple_insert for comments about most of the input flags, except
 * that this routine directly takes a tuple rather than a slot.
 *
 * There's corresponding HEAP_INSERT_ options to all the TABLE_INSERT_
 * options, and there additionally is HEAP_INSERT_SPECULATIVE which is used to
 * implement table_tuple_insert_speculative().
 *
 * On return the header fields of *tup are updated to match the stored tuple;
 * in particular tup->t_self receives the actual TID where the tuple was
 * stored.  But note that any toasting of fields within the tuple data is NOT
 * reflected into *tup.
 */
void
heap_insert(Relation relation, HeapTuple tup, CommandId cid,
			int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple	heaptup;
	Buffer		buffer;
	Buffer		vmbuffer = InvalidBuffer;
	bool		all_visible_cleared = false;

#ifdef DIVA
	bool		siro;
	TupleDesc	tupDesc = RelationGetDescr(relation);
#endif
	/* Cheap, simplistic check that the tuple matches the rel's rowtype. */
	Assert(HeapTupleHeaderGetNatts(tup->t_data) <=
		   RelationGetNumberOfAttributes(relation));

	/*
	 * Fill in tuple header fields and toast the tuple if necessary.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

#ifdef LOCATOR
	heaptup->t_locator_route_synopsis = NULL;
#endif /* LOCATOR */

	/*
	 * Find buffer to insert this tuple into.  If the page is all visible,
	 * this will also pin the requisite visibility map page.
	 */
#ifdef DIVA
	siro = IsSiro(relation);

	Assert(tupDesc->maxlen == 0 ||
		   tupDesc->maxlen >= MAXALIGN(heaptup->t_len));

	if (siro)
	{
		/* TODO: What is this case? */
		if (unlikely(tupDesc->maxlen == 0))
			TupleDescGetMaxLen(relation);

		buffer = RelationGetBufferForTuples(relation,
											(2 * tupDesc->maxlen),
											InvalidBuffer, options, bistate,
											&vmbuffer, NULL, 2);
	}
	else
		buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
										   InvalidBuffer, options, bistate,
										   &vmbuffer, NULL);
		
#else
	buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
									   InvalidBuffer, options, bistate,
									   &vmbuffer, NULL);
#endif

	/*
	 * We're about to do the actual insert -- but check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the insert
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple insert is visible).
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.  So we don't need to specify a
	 * buffer when making the call, which makes for a faster check.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

#ifdef DIVA
	if (siro)
		RelationPutHeapTupleWithDummy(relation, buffer, heaptup,
								(options & HEAP_INSERT_SPECULATIVE) != 0);
	else
		RelationPutHeapTuple(relation, buffer, heaptup,
								(options & HEAP_INSERT_SPECULATIVE) != 0);
#else
	RelationPutHeapTuple(relation, buffer, heaptup,
						 (options & HEAP_INSERT_SPECULATIVE) != 0);
#endif
	if (PageIsAllVisible(BufferGetPage(buffer)))
	{
		all_visible_cleared = true;
		PageClearAllVisible(BufferGetPage(buffer));
		visibilitymap_clear(relation,
							ItemPointerGetBlockNumber(&(heaptup->t_self)),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}

	/*
	 * XXX Should we set PageSetPrunable on this page ?
	 *
	 * The inserting transaction may eventually abort thus making this tuple
	 * DEAD and hence available for pruning. Though we don't want to optimize
	 * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
	 * aborted tuple will never be pruned until next vacuum is triggered.
	 *
	 * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
	 */

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_insert xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr	recptr;
		Page		page = BufferGetPage(buffer);
		uint8		info = XLOG_HEAP_INSERT;
		int			bufflags = 0;
#ifdef DIVA
		OffsetNumber offset;
		OffsetNumber maxOffset;

		if (siro)
		{
			offset = FirstOffsetNumber;
			maxOffset = FirstOffsetNumber + 2;
		}
		else
		{
			offset = FirstOffsetNumber;
			maxOffset = FirstOffsetNumber;
		}
#endif /* DIVA */

		/*
		 * If this is a catalog, we need to transmit combo CIDs to properly
		 * decode, so log that as well.
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
			log_heap_new_cid(relation, heaptup);

		/*
		 * If this is the single and first tuple on page, we can reinit the
		 * page instead of restoring the whole thing.  Set flag, and hide
		 * buffer references from XLogInsert.
		 */
#ifdef DIVA
		if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == offset &&
			PageGetMaxOffsetNumber(page) == maxOffset)
#else
		if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == FirstOffsetNumber &&
			PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
#endif /* DIVA */
		{
			info |= XLOG_HEAP_INIT_PAGE;
			bufflags |= REGBUF_WILL_INIT;
		}

		xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
		xlrec.flags = 0;
		if (all_visible_cleared)
			xlrec.flags |= XLH_INSERT_ALL_VISIBLE_CLEARED;
		if (options & HEAP_INSERT_SPECULATIVE)
			xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;
		Assert(ItemPointerGetBlockNumber(&heaptup->t_self) == BufferGetBlockNumber(buffer));

		/*
		 * For logical decoding, we need the tuple even if we're doing a full
		 * page write, so make sure it's included even if we take a full-page
		 * image. (XXX We could alternatively store a pointer into the FPW).
		 */
		if (RelationIsLogicallyLogged(relation) &&
			!(options & HEAP_INSERT_NO_LOGICAL))
		{
			xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
			bufflags |= REGBUF_KEEP_DATA;

			if (IsToastRelation(relation))
				xlrec.flags |= XLH_INSERT_ON_TOAST_RELATION;
		}

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);

		xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
		xlhdr.t_infomask = heaptup->t_data->t_infomask;
		xlhdr.t_hoff = heaptup->t_data->t_hoff;

		/*
		 * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
		 * write the whole page to the xlog, we don't need to store
		 * xl_heap_header in the xlog.
		 */
		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
		XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
		/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
		XLogRegisterBufData(0,
							(char *) heaptup->t_data + SizeofHeapTupleHeader,
							heaptup->t_len - SizeofHeapTupleHeader);

		/* filtering by origin on a row level is much more efficient */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		recptr = XLogInsert(RM_HEAP_ID, info);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	/*
	 * If tuple is cachable, mark it for invalidation from the caches in case
	 * we abort.  Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}

/*
 * Subroutine for heap_insert(). Prepares a tuple for insertion. This sets the
 * tuple header fields and toasts the tuple if necessary.  Returns a toasted
 * version of the tuple if it was toasted, or the original tuple if not. Note
 * that in any case, the header fields are also set in the original tuple.
 */
static HeapTuple
heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid,
					CommandId cid, int options)
{
	/*
	 * To allow parallel inserts, we need to ensure that they are safe to be
	 * performed in workers. We have the infrastructure to allow parallel
	 * inserts in general except for the cases where inserts generate a new
	 * CommandId (eg. inserts into a table having a foreign key column).
	 */
	if (IsParallelWorker())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot insert tuples in a parallel worker")));

	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(tup->t_data);

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0); /* for cleanliness */
	tup->t_tableOid = RelationGetRelid(relation);

	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tup));
		return tup;
	}
	else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
		return heap_toast_insert_or_update(relation, tup, NULL, options);
	else
		return tup;
}

/*
 *	heap_multi_insert	- insert multiple tuples into a heap
 *
 * This is like heap_insert(), but inserts multiple tuples in one operation.
 * That's faster than calling heap_insert() in a loop, because when multiple
 * tuples can be inserted on a single page, we can write just a single WAL
 * record covering all of them, and only need to lock/unlock the page once.
 *
 * Note: this leaks memory into the current memory context. You can create a
 * temporary context before calling this, if that's a problem.
 */
void
heap_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
				  CommandId cid, int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple  *heaptuples;
	int			i;
	int			ndone;
	PGAlignedBlock scratch;
	Page		page;
	Buffer		vmbuffer = InvalidBuffer;
	bool		needwal;
	Size		saveFreeSpace;
	bool		need_tuple_data = RelationIsLogicallyLogged(relation);
	bool		need_cids = RelationIsAccessibleInLogicalDecoding(relation);

#ifdef DIVA
	bool		siro = IsSiro(relation);
	TupleDesc tupDesc = RelationGetDescr(relation);
#endif

	/* currently not needed (thus unsupported) for heap_multi_insert() */
	AssertArg(!(options & HEAP_INSERT_NO_LOGICAL));

	needwal = RelationNeedsWAL(relation);
	saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
												   HEAP_DEFAULT_FILLFACTOR);

	/* Toast and set header data in all the slots */
	heaptuples = palloc(ntuples * sizeof(HeapTuple));
	for (i = 0; i < ntuples; i++)
	{
		HeapTuple	tuple;

		tuple = ExecFetchSlotHeapTuple(slots[i], true, NULL);
		slots[i]->tts_tableOid = RelationGetRelid(relation);
		tuple->t_tableOid = slots[i]->tts_tableOid;
		heaptuples[i] = heap_prepare_insert(relation, tuple, xid, cid,
											options);
#ifdef DIVA
		if (tupDesc->maxlen < MAXALIGN(heaptuples[i]->t_len))
			tupDesc->maxlen = MAXALIGN(heaptuples[i]->t_len);
#endif /* DIVA */
	}

	/*
	 * We're about to do the actual inserts -- but check for conflict first,
	 * to minimize the possibility of having to roll back work we've just
	 * done.
	 *
	 * A check here does not definitively prevent a serialization anomaly;
	 * that check MUST be done at least past the point of acquiring an
	 * exclusive buffer content lock on every buffer that will be affected,
	 * and MAY be done after all inserts are reflected in the buffers and
	 * those locks are released; otherwise there is a race condition.  Since
	 * multiple buffers can be locked and unlocked in the loop below, and it
	 * would not be feasible to identify and lock all of those buffers before
	 * the loop, we must do a final check at the end.
	 *
	 * The check here could be omitted with no loss of correctness; it is
	 * present strictly as an optimization.
	 *
	 * For heap inserts, we only need to check for table-level SSI locks. Our
	 * new tuples can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.  So we don't need to specify a
	 * buffer when making the call, which makes for a faster check.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	ndone = 0;
	while (ndone < ntuples)
	{
		Buffer		buffer;
		bool		starting_with_empty_page;
		bool		all_visible_cleared = false;
		bool		all_frozen_set = false;
		int			nthispage;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Find buffer where at least the next tuple will fit.  If the page is
		 * all-visible, this will also pin the requisite visibility map page.
		 *
		 * Also pin visibility map page if COPY FREEZE inserts tuples into an
		 * empty page. See all_frozen_set below.
		 */
#ifdef DIVA
		if (siro)
			buffer = RelationGetBufferForTuples(relation,
												(2 * tupDesc->maxlen),
												InvalidBuffer, options, bistate,
												&vmbuffer, NULL, 2);
		else
			buffer = RelationGetBufferForTuple(relation, heaptuples[ndone]->t_len,
											   InvalidBuffer, options, bistate,
											   &vmbuffer, NULL);
#else
		buffer = RelationGetBufferForTuple(relation, heaptuples[ndone]->t_len,
										   InvalidBuffer, options, bistate,
										   &vmbuffer, NULL);
#endif
		page = BufferGetPage(buffer);

		starting_with_empty_page = PageGetMaxOffsetNumber(page) == 0;

		if (starting_with_empty_page && (options & HEAP_INSERT_FROZEN))
			all_frozen_set = true;

		/* NO EREPORT(ERROR) from here till changes are logged */
		START_CRIT_SECTION();

		/*
		 * RelationGetBufferForTuple has ensured that the first tuple fits.
		 * Put that on the page, and then as many other tuples as fit.
		 */
#ifdef DIVA
		if (siro)
			RelationPutHeapTupleWithDummy(
					relation, buffer, heaptuples[ndone], false);
		else
			RelationPutHeapTuple(relation, buffer, heaptuples[ndone], false);
#else
		RelationPutHeapTuple(relation, buffer, heaptuples[ndone], false);
#endif

		/*
		 * For logical decoding we need combo CIDs to properly decode the
		 * catalog.
		 */
		if (needwal && need_cids)
			log_heap_new_cid(relation, heaptuples[ndone]);

		for (nthispage = 1; ndone + nthispage < ntuples; nthispage++)
		{
			HeapTuple	heaptup = heaptuples[ndone + nthispage];

#ifdef DIVA
			if (siro)
			{
				if (PageGetHeapFreeSpaceWithLP(page, 3) <
							tupDesc->maxlen * 2 + MAXALIGN(PITEM_SZ) + saveFreeSpace)
					break;

				RelationPutHeapTupleWithDummy(relation, buffer, heaptup, false);
			}
			else
			{
				if (PageGetHeapFreeSpace(page) < MAXALIGN(heaptup->t_len) + saveFreeSpace)
					break;

				RelationPutHeapTuple(relation, buffer, heaptup, false);
			}
#else
			if (PageGetHeapFreeSpace(page) < MAXALIGN(heaptup->t_len) + saveFreeSpace)
				break;

			RelationPutHeapTuple(relation, buffer, heaptup, false);
#endif
			/*
			 * For logical decoding we need combo CIDs to properly decode the
			 * catalog.
			 */
			if (needwal && need_cids)
				log_heap_new_cid(relation, heaptup);
		}

		/*
		 * If the page is all visible, need to clear that, unless we're only
		 * going to add further frozen rows to it.
		 *
		 * If we're only adding already frozen rows to a previously empty
		 * page, mark it as all-visible.
		 */
		if (PageIsAllVisible(page) && !(options & HEAP_INSERT_FROZEN))
		{
			all_visible_cleared = true;
			PageClearAllVisible(page);
			visibilitymap_clear(relation,
								BufferGetBlockNumber(buffer),
								vmbuffer, VISIBILITYMAP_VALID_BITS);
		}
		else if (all_frozen_set)
			PageSetAllVisible(page);

		/*
		 * XXX Should we set PageSetPrunable on this page ? See heap_insert()
		 */

		MarkBufferDirty(buffer);

		/* XLOG stuff */
		if (needwal)
		{
			XLogRecPtr	recptr;
			xl_heap_multi_insert *xlrec;
			uint8		info = XLOG_HEAP2_MULTI_INSERT;
			char	   *tupledata;
			int			totaldatalen;
			char	   *scratchptr = scratch.data;
			bool		init;
			int			bufflags = 0;

			/*
			 * If the page was previously empty, we can reinit the page
			 * instead of restoring the whole thing.
			 */
			init = starting_with_empty_page;

			/* allocate xl_heap_multi_insert struct from the scratch area */
			xlrec = (xl_heap_multi_insert *) scratchptr;
			scratchptr += SizeOfHeapMultiInsert;

			/*
			 * Allocate offsets array. Unless we're reinitializing the page,
			 * in that case the tuples are stored in order starting at
			 * FirstOffsetNumber and we don't need to store the offsets
			 * explicitly.
			 */
			if (!init)
				scratchptr += nthispage * sizeof(OffsetNumber);

			/* the rest of the scratch space is used for tuple data */
			tupledata = scratchptr;

			/* check that the mutually exclusive flags are not both set */
			Assert(!(all_visible_cleared && all_frozen_set));

			xlrec->flags = 0;
			if (all_visible_cleared)
				xlrec->flags = XLH_INSERT_ALL_VISIBLE_CLEARED;
			if (all_frozen_set)
				xlrec->flags = XLH_INSERT_ALL_FROZEN_SET;

			xlrec->ntuples = nthispage;

			/*
			 * Write out an xl_multi_insert_tuple and the tuple data itself
			 * for each tuple.
			 */
			for (i = 0; i < nthispage; i++)
			{
				HeapTuple	heaptup = heaptuples[ndone + i];
				xl_multi_insert_tuple *tuphdr;
				int			datalen;

				if (!init)
					xlrec->offsets[i] = ItemPointerGetOffsetNumber(&heaptup->t_self);
				/* xl_multi_insert_tuple needs two-byte alignment. */
				tuphdr = (xl_multi_insert_tuple *) SHORTALIGN(scratchptr);
				scratchptr = ((char *) tuphdr) + SizeOfMultiInsertTuple;

				tuphdr->t_infomask2 = heaptup->t_data->t_infomask2;
				tuphdr->t_infomask = heaptup->t_data->t_infomask;
				tuphdr->t_hoff = heaptup->t_data->t_hoff;

				/* write bitmap [+ padding] [+ oid] + data */
				datalen = heaptup->t_len - SizeofHeapTupleHeader;
				memcpy(scratchptr,
					   (char *) heaptup->t_data + SizeofHeapTupleHeader,
					   datalen);
				tuphdr->datalen = datalen;
				scratchptr += datalen;
			}
			totaldatalen = scratchptr - tupledata;
			Assert((scratchptr - scratch.data) < BLCKSZ);

			if (need_tuple_data)
				xlrec->flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;

			/*
			 * Signal that this is the last xl_heap_multi_insert record
			 * emitted by this call to heap_multi_insert(). Needed for logical
			 * decoding so it knows when to cleanup temporary data.
			 */
			if (ndone + nthispage == ntuples)
				xlrec->flags |= XLH_INSERT_LAST_IN_MULTI;

			if (init)
			{
				info |= XLOG_HEAP_INIT_PAGE;
				bufflags |= REGBUF_WILL_INIT;
			}

			/*
			 * If we're doing logical decoding, include the new tuple data
			 * even if we take a full-page image of the page.
			 */
			if (need_tuple_data)
				bufflags |= REGBUF_KEEP_DATA;

			XLogBeginInsert();
			XLogRegisterData((char *) xlrec, tupledata - scratch.data);
			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);

			XLogRegisterBufData(0, tupledata, totaldatalen);

			/* filtering by origin on a row level is much more efficient */
			XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

			recptr = XLogInsert(RM_HEAP2_ID, info);

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/*
		 * If we've frozen everything on the page, update the visibilitymap.
		 * We're already holding pin on the vmbuffer.
		 */
		if (all_frozen_set)
		{
			Assert(PageIsAllVisible(page));
			Assert(visibilitymap_pin_ok(BufferGetBlockNumber(buffer), vmbuffer));

			/*
			 * It's fine to use InvalidTransactionId here - this is only used
			 * when HEAP_INSERT_FROZEN is specified, which intentionally
			 * violates visibility rules.
			 */
			visibilitymap_set(relation, BufferGetBlockNumber(buffer), buffer,
							  InvalidXLogRecPtr, vmbuffer,
							  InvalidTransactionId,
							  VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN);
		}

		UnlockReleaseBuffer(buffer);
		ndone += nthispage;

		/*
		 * NB: Only release vmbuffer after inserting all tuples - it's fairly
		 * likely that we'll insert into subsequent heap pages that are likely
		 * to use the same vm page.
		 */
	}

	/* We're done with inserting all tuples, so release the last vmbuffer. */
	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	/*
	 * We're done with the actual inserts.  Check for conflicts again, to
	 * ensure that all rw-conflicts in to these inserts are detected.  Without
	 * this final check, a sequential scan of the heap may have locked the
	 * table after the "before" check, missing one opportunity to detect the
	 * conflict, and then scanned the table before the new tuples were there,
	 * missing the other chance to detect the conflict.
	 *
	 * For heap inserts, we only need to check for table-level SSI locks. Our
	 * new tuples can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.  So we don't need to specify a
	 * buffer when making the call.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	/*
	 * If tuples are cachable, mark them for invalidation from the caches in
	 * case we abort.  Note it is OK to do this after releasing the buffer,
	 * because the heaptuples data structure is all in local memory, not in
	 * the shared buffer.
	 */
	if (IsCatalogRelation(relation))
	{
		for (i = 0; i < ntuples; i++)
			CacheInvalidateHeapTuple(relation, heaptuples[i], NULL);
	}

	/* copy t_self fields back to the caller's slots */
	for (i = 0; i < ntuples; i++)
		slots[i]->tts_tid = heaptuples[i]->t_self;

	pgstat_count_heap_insert(relation, ntuples);
}

/*
 *	simple_heap_insert - insert a tuple
 *
 * Currently, this routine differs from heap_insert only in supplying
 * a default command ID and not allowing access to the speedup options.
 *
 * This should be used rather than using heap_insert directly in most places
 * where we are modifying system catalogs.
 */
void
simple_heap_insert(Relation relation, HeapTuple tup)
{
	heap_insert(relation, tup, GetCurrentCommandId(true), 0, NULL);
}

/*
 * Given infomask/infomask2, compute the bits that must be saved in the
 * "infobits" field of xl_heap_delete, xl_heap_update, xl_heap_lock,
 * xl_heap_lock_updated WAL records.
 *
 * See fix_infomask_from_infobits.
 */
static uint8
compute_infobits(uint16 infomask, uint16 infomask2)
{
	return
		((infomask & HEAP_XMAX_IS_MULTI) != 0 ? XLHL_XMAX_IS_MULTI : 0) |
		((infomask & HEAP_XMAX_LOCK_ONLY) != 0 ? XLHL_XMAX_LOCK_ONLY : 0) |
		((infomask & HEAP_XMAX_EXCL_LOCK) != 0 ? XLHL_XMAX_EXCL_LOCK : 0) |
	/* note we ignore HEAP_XMAX_SHR_LOCK here */
		((infomask & HEAP_XMAX_KEYSHR_LOCK) != 0 ? XLHL_XMAX_KEYSHR_LOCK : 0) |
		((infomask2 & HEAP_KEYS_UPDATED) != 0 ?
		 XLHL_KEYS_UPDATED : 0);
}

/*
 * Given two versions of the same t_infomask for a tuple, compare them and
 * return whether the relevant status for a tuple Xmax has changed.  This is
 * used after a buffer lock has been released and reacquired: we want to ensure
 * that the tuple state continues to be the same it was when we previously
 * examined it.
 *
 * Note the Xmax field itself must be compared separately.
 */
static inline bool
xmax_infomask_changed(uint16 new_infomask, uint16 old_infomask)
{
	const uint16 interesting =
	HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;

	if ((new_infomask & interesting) != (old_infomask & interesting))
		return true;

	return false;
}

#ifdef DIVA
/*
 *	heap_delete_with_vc - delete a tuple
 *
 * See table_tuple_delete() for an explanation of the parameters, except that
 * this routine directly takes a tuple rather than a slot.
 */
TM_Result
heap_delete_with_vc(Relation relation, ItemPointer tid,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd, bool changingPart)
{
	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	ItemId		lp;
	HeapTuple	tp;
	Page		page;
	BlockNumber	block;
	OffsetNumber offnum;
	Buffer		buffer,
				targetbuf,
				vmbuffer = InvalidBuffer;
	TransactionId new_xmax;
	uint16		new_infomask,
				new_infomask2;
	bool		have_tuple_lock = false;
	bool		iscombo;
	bool		all_visible_cleared = false;
	HeapTuple	old_key_tuple = NULL; /* replica identity of the tuple */
	bool		old_key_copied = false;

	bool		is_second_old_exist;

	/* for left version */
	ItemId			l_lp;
	ItemPointerData	l_pointer_data;
	ItemPointer		l_pointer = &l_pointer_data;
	HeapTupleData	l_loctup;
	BlockNumber		l_block;
	Buffer			l_buffer;
#if 0
	Buffer			l_vmbuffer = InvalidBuffer;
#endif
	Page			l_page;

	/* for right version */
	ItemId			r_lp;
	ItemPointerData	r_pointer_data;
	ItemPointer		r_pointer = &r_pointer_data;
	HeapTupleData	r_loctup;
	BlockNumber		r_block;
	Buffer			r_buffer;
#if 0
	Buffer			r_vmbuffer = InvalidBuffer;
#endif
	Page			r_page;
	
	/* for p-leaf meta tuple*/
	HeapTupleHeaderData tmpTupHeader;

#ifdef LOCATOR
	pg_atomic_uint64 *dual_ref;
	uint64 prev_toggle_bit;
	uint64 wait_for_readers;
#endif /* LOCATOR */

	Assert(ItemPointerIsValid(tid));

	/*
	 * Forbid this during a parallel operation, lest it allocate a combocid.
	 * Other workers might need that combocid for visibility checks, and we
	 * have no provision for broadcasting it to them.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot delete tuples during a parallel operation")));

	block = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);
	buffer = ReadBuffer(relation, block);
	page = BufferGetPage(buffer);

	/*
	 * Before locking the buffer, pin the visibility map page if it appears to
	 * be necessary.  Since we haven't got the lock yet, someone else might be
	 * in the middle of changing this, so we'll need to recheck after we have
	 * the lock.
	 */
	if (PageIsAllVisible(page))
		visibilitymap_pin(relation, block, &vmbuffer);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	lp = PageGetItemId(page, offnum);
	Assert(ItemIdIsNormal(lp));

	/* Set pointers */
	ItemPointerSet(l_pointer, block, offnum + 1);
	ItemPointerSet(r_pointer, block, offnum + 2);

#ifdef LOCATOR
	/* Set dula_ref */
	dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buffer);
#endif /* LOCATOR */

#if 0
set_l_r_1:
#endif

	/* Set left version's block, buffer, page */
	l_block = ItemPointerGetBlockNumber(l_pointer);

	if (unlikely(l_block != block))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		l_buffer = ReadBuffer(relation, l_block);
		l_page = BufferGetPage(l_buffer);

		if (PageIsAllVisible(l_page))
			visibilitymap_pin(relation, l_block, &l_vmbuffer);

		/* TODO: locking order must to be fixed to avoid deadlock */
		LockBuffer(l_buffer, BUFFER_LOCK_EXCLUSIVE);
#endif
	}
	else
	{
		l_buffer = buffer;
		l_page = page;
	}

	/* Set left version */
	l_lp = PageGetItemId(l_page, ItemPointerGetOffsetNumber(l_pointer));
	Assert(ItemIdIsNormal(l_lp));

	l_loctup.t_tableOid = RelationGetRelid(relation);
	l_loctup.t_data = (HeapTupleHeader) PageGetItem(l_page, l_lp);
	l_loctup.t_len = ItemIdGetLength(l_lp);

	l_loctup.t_self = *l_pointer;
	
	/* Set right version's block, buffer, page */
	r_block = ItemPointerGetBlockNumber(r_pointer);

	if (unlikely(r_block != block))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		if (likely(r_block != l_block))
		{
			r_buffer = ReadBuffer(relation, r_block);
			r_page = BufferGetPage(r_buffer);

			if (PageIsAllVisible(r_page))
				visibilitymap_pin(relation, r_block, &r_vmbuffer);

			/* TODO: locking order must to be fixed to avoid deadlock */
			LockBuffer(r_buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		else
		{
			r_buffer = l_buffer;
			r_page = l_page;
		}
#endif
	}
	else
	{
		r_buffer = buffer;
		r_page = page;
	}

	/* Set right version  */
	r_lp = PageGetItemId(r_page, ItemPointerGetOffsetNumber(r_pointer));
	Assert(ItemIdIsNormal(r_lp));

	if (unlikely(LP_OVR_IS_UNUSED(r_lp)))
		is_second_old_exist = false;
	else
		is_second_old_exist = true;

	r_loctup.t_tableOid = RelationGetRelid(relation);
	r_loctup.t_data = (HeapTupleHeader) PageGetItem(r_page, r_lp);
	r_loctup.t_len = ItemIdGetLength(r_lp);

	r_loctup.t_self = *r_pointer;

l1:

	if (unlikely(l_block != ItemPointerGetBlockNumber(l_pointer) ||
				 r_block != ItemPointerGetBlockNumber(r_pointer)))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
		// elog(ERROR, "TODO: append-update have to be implemented");
#else
		goto set_l_r_1;
#endif
	}

	/*
	 * If we didn't pin the visibility map page and the page has become all
	 * visible while we were busy locking the buffer, we'll have to unlock and
	 * re-lock, to avoid holding the buffer lock across an I/O.  That's a bit
	 * unfortunate, but hopefully shouldn't happen often.
	 */
	if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		visibilitymap_pin(relation, block, &vmbuffer);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	}

	if (LP_OVR_IS_LEFT(lp))
	{
		tp = &l_loctup;
		targetbuf = l_buffer;
	}
	else
	{
		tp = &r_loctup;
		targetbuf = r_buffer;
	}

	/*
	 * In DIVA, Since the tid passed as a parameter is the tid of the
	 * p-locator, the actual target version of the DELETE must be checked,
	 * it is necessary to double-check the actual DELETE target version.
	 */
	if (HeapTupleSatisfiesVisibility(tp, snapshot, targetbuf))
		result = HeapTupleSatisfiesUpdate(tp, cid, targetbuf);
	else if (likely(is_second_old_exist))
	{
		/* Set 2nd oldest version */
		if (tp == &l_loctup)
		{
			tp = &r_loctup;
			targetbuf = r_buffer;
		}
		else
		{
			tp = &l_loctup;
			targetbuf = l_buffer;
		}

		if (HeapTupleSatisfiesVisibility(tp, snapshot, targetbuf))
			result = HeapTupleSatisfiesUpdate(tp, cid, targetbuf);
		else
			result = TM_Invisible;
	}
	else
		result = TM_Invisible;

	if (result == TM_Invisible)
	{
		/*
		 * Because we are doing in-place update for siro implementation,
		 * oldtup could be invisible in this cases.
		 *
		 * 1. In a predecessor heap tuple search routine, we delivered any
		 *    one of tuple in the heap page, if there was no visible tuple.
		 *    It means that the transaction already found out the conflict,
		 *    and this result TM_Invisible is intended.
		 * 2. We doesn't acquire the page latch when we find a visible tuple,
		 *    so the tuple we found might be overwritten by another concurrent
		 *    updater.
		 *
		 * Both cases are acceptible, and we can treat it as TM_Update so that
		 * transaction should be aborted.
		 */
		result = TM_Updated;
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16    infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tp->t_data);
		infomask = tp->t_data->t_infomask;

		/*
		 * Sleep until concurrent transaction ends -- except when there's a
		 * single locker and it's our own transaction.  Note we don't care
		 * which lock mode the locker has, because we need the strongest one.
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			bool    current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										LockTupleExclusive, &current_is_member))
			{
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, tid, LockTupleExclusive,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, MultiXactStatusUpdate, infomask,
								relation, tid, XLTW_Delete,
								NULL);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if ((vmbuffer == InvalidBuffer && PageIsAllVisible(page)) ||
					xmax_infomask_changed(tp->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp->t_data),
										 xwait))
				{
					is_second_old_exist = true;
					goto l1;
				}
			}

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.  It is legal for us to delete
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).  We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else if (!TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			heap_acquire_tuplock(relation, tid, LockTupleExclusive,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, tid, XLTW_Delete);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if ((vmbuffer == InvalidBuffer && PageIsAllVisible(page)) ||
				xmax_infomask_changed(tp->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp->t_data),
									 xwait))
			{
				is_second_old_exist = true;
				goto l1;
			}

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(tp->t_data, buffer, xwait);
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if ((tp->t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tp->t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tp->t_data))
			result = TM_Ok;
		else
			result = TM_Updated;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		if (!HeapTupleSatisfiesVisibility(tp, crosscheck, buffer))
			result = TM_Updated;
	}

	/*
	 * Buffer latch could had been release if it acquired tuplock so that
	 * we need to check the visibility again here.
	 */
	if (result == TM_Ok)
	{
		if (HeapTupleSatisfiesVisibility(tp, snapshot, buffer))
			result = HeapTupleSatisfiesUpdate(tp, cid, buffer);
		else
			result = TM_Updated;

		if (result == TM_Invisible)
			result = TM_Updated;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		tmfd->ctid = tp->t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(tp->t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(tp->t_data);
		else
			tmfd->cmax = InvalidCommandId;
		UnlockReleaseBuffer(buffer);
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, tid, LockTupleExclusive);
		if (vmbuffer != InvalidBuffer)
			ReleaseBuffer(vmbuffer);

		return result;
	}

#if defined(LOCATOR) && !defined(USING_LOCK)
	/* Toggle the bit, set type of operation and offset number of target */
	prev_toggle_bit = DRefSetModicationInfo(dual_ref, DRefStateDELETE, offnum);
	wait_for_readers = prev_toggle_bit == 0 ? DRefToggleOffRefCnt :
											  DRefToggleOnRefCnt;

	pg_memory_barrier();
#endif /* LOCATOR */

	/*
	 * We're about to do the actual delete -- check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the delete
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple delete is visible).
	 */
	CheckForSerializableConflictIn(relation, tid, BufferGetBlockNumber(buffer));

	/* replace cid with a combo cid if necessary */
	HeapTupleHeaderAdjustCmax(tp->t_data, &cid, &iscombo);

	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 */
	old_key_tuple = ExtractReplicaIdentity(relation, tp, true,
										   &old_key_copied);

	/*
	 * If this is the first possibly-multixact-able operation in the current
	 * transaction, set my per-backend OldestMemberMXactId setting. We can be
	 * certain that the transaction will never become a member of any older
	 * MultiXactIds than that.  (We have to do this even if we end up just
	 * using our own TransactionId below, since some other backend could
	 * incorporate our XID into a MultiXact immediately afterwards.)
	 */
	MultiXactIdSetOldestMember();

	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp->t_data),
							  tp->t_data->t_infomask, tp->t_data->t_infomask2,
							  xid, LockTupleExclusive, true,
							  &new_xmax, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();

	/*
	 * If this transaction commits, the tuple will become DEAD sooner or
	 * later.  Set flag that this page is a candidate for pruning once our xid
	 * falls below the OldestXmin horizon.  If the transaction finally aborts,
	 * the subsequent page pruning will be a no-op and the hint will be
	 * cleared.
	 */
	PageSetPrunable(page, xid);

	if (PageIsAllVisible(page))
	{
		all_visible_cleared = true;
		PageClearAllVisible(page);
		visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}

	/* Copy the original header */
	tmpTupHeader = *(tp->t_data);

	/* Store transaction information of xact deleting the tuple */
	tmpTupHeader.t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tmpTupHeader.t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tmpTupHeader.t_infomask |= new_infomask;
	tmpTupHeader.t_infomask2 |= new_infomask2;
	HeapTupleHeaderClearHotUpdated(&tmpTupHeader);
	HeapTupleHeaderSetXmax(&tmpTupHeader, new_xmax);
	HeapTupleHeaderSetCmax(&tmpTupHeader, cid, iscombo);

#if defined(LOCATOR) && !defined(USING_LOCK)
	/*
	 * The writer transaction had to wait for the readers to finish reading the
	 * record they were reading at the time the bit was toggled.
	 * (Detail description is in GetCheckVar().)
	 */
	DRefWaitForReaders(dual_ref, wait_for_readers);

	/* Writer must delete version after readers passed by */
	pg_memory_barrier();
#endif /* LOCATOR */

	/* Paste info atomically */
	FetchTupleHeaderInfo(tp, tmpTupHeader);

	/* Signal that this is actually a move into another partition */
	if (changingPart)
		HeapTupleHeaderSetMovedPartitions(&tmpTupHeader);

#if defined(LOCATOR) && !defined(USING_LOCK)
	pg_memory_barrier();

	/* Turns off modification bit */
	DRefClearModicationInfo(dual_ref);
#endif /* LOCATOR */

	if (unlikely(l_buffer != targetbuf))
		MarkBufferDirty(l_buffer);
	if (unlikely(r_buffer != targetbuf))
		MarkBufferDirty(r_buffer);
	MarkBufferDirty(targetbuf);

	/*
	 * XLOG stuff
	 *
	 * NB: heap_abort_speculative() uses the same xlog record and replay
	 * routines.
	 */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_delete xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr  recptr;

		/*
		 * For logical decode we need combo CIDs to properly decode the
		 * catalog
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
			log_heap_new_cid(relation, tp);

		xlrec.flags = 0;
		if (all_visible_cleared)
			xlrec.flags |= XLH_DELETE_ALL_VISIBLE_CLEARED;
		if (changingPart)
			xlrec.flags |= XLH_DELETE_IS_PARTITION_MOVE;
		xlrec.infobits_set = compute_infobits(tp->t_data->t_infomask,
											  tp->t_data->t_infomask2);
		xlrec.offnum = ItemPointerGetOffsetNumber(&(tp->t_self));
		xlrec.xmax = new_xmax;

		if (old_key_tuple != NULL)
		{
			if (relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
				xlrec.flags |= XLH_DELETE_CONTAINS_OLD_TUPLE;
			else
				xlrec.flags |= XLH_DELETE_CONTAINS_OLD_KEY;
		}

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);

		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		/*
		 * Log replica identity of the deleted tuple if there is one
		 */
		if (old_key_tuple != NULL)
		{
			xlhdr.t_infomask2 = old_key_tuple->t_data->t_infomask2;
			xlhdr.t_infomask = old_key_tuple->t_data->t_infomask;
			xlhdr.t_hoff = old_key_tuple->t_data->t_hoff;

			XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
			XLogRegisterData((char *) old_key_tuple->t_data
							 + SizeofHeapTupleHeader,
							 old_key_tuple->t_len
							 - SizeofHeapTupleHeader);
		}

		/* filtering by origin on a row level is much more efficient */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	/*
	 * If the tuple has toasted out-of-line attributes, we need to delete
	 * those items too.  We have to do this before releasing the buffer
	 * because we need to look at the contents of the tuple, but it's OK to
	 * release the content lock on the buffer first.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tp));
	}
	else if (HeapTupleHasExternal(tp))
		heap_toast_delete(relation, tp, false);

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. We have to do this before releasing the buffer because we
	 * need to look at the contents of the tuple.
	 */
	CacheInvalidateHeapTuple(relation, tp, NULL);

	/* Now we can release the buffer(s) */
	if (l_buffer != buffer)
		ReleaseBuffer(l_buffer);
	if (r_buffer != buffer)
		ReleaseBuffer(r_buffer);
	ReleaseBuffer(buffer);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, tid, LockTupleExclusive);

	pgstat_count_heap_delete(relation);

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	return TM_Ok;
}
#endif /* DIVA */

/*
 *	heap_delete - delete a tuple
 *
 * See table_tuple_delete() for an explanation of the parameters, except that
 * this routine directly takes a tuple rather than a slot.
 *
 * In the failure cases, the routine fills *tmfd with the tuple's t_ctid,
 * t_xmax (resolving a possible MultiXact, if necessary), and t_cmax (the last
 * only for TM_SelfModified, since we cannot obtain cmax from a combo CID
 * generated by another transaction).
 */
TM_Result
heap_delete(Relation relation, ItemPointer tid,
			CommandId cid, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, bool changingPart)
{
	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	ItemId		lp;
	HeapTupleData tp;
	Page		page;
	BlockNumber block;
	Buffer		buffer;
	Buffer		vmbuffer = InvalidBuffer;
	TransactionId new_xmax;
	uint16		new_infomask,
				new_infomask2;
	bool		have_tuple_lock = false;
	bool		iscombo;
	bool		all_visible_cleared = false;
	HeapTuple	old_key_tuple = NULL;	/* replica identity of the tuple */
	bool		old_key_copied = false;

	Assert(ItemPointerIsValid(tid));

	/*
	 * Forbid this during a parallel operation, lest it allocate a combo CID.
	 * Other workers might need that combo CID for visibility checks, and we
	 * have no provision for broadcasting it to them.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot delete tuples during a parallel operation")));

	block = ItemPointerGetBlockNumber(tid);
	buffer = ReadBuffer(relation, block);
	page = BufferGetPage(buffer);

	/*
	 * Before locking the buffer, pin the visibility map page if it appears to
	 * be necessary.  Since we haven't got the lock yet, someone else might be
	 * in the middle of changing this, so we'll need to recheck after we have
	 * the lock.
	 */
	if (PageIsAllVisible(page))
		visibilitymap_pin(relation, block, &vmbuffer);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	Assert(ItemIdIsNormal(lp));

	tp.t_tableOid = RelationGetRelid(relation);
	tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tp.t_len = ItemIdGetLength(lp);
	tp.t_self = *tid;

l1:
	/*
	 * If we didn't pin the visibility map page and the page has become all
	 * visible while we were busy locking the buffer, we'll have to unlock and
	 * re-lock, to avoid holding the buffer lock across an I/O.  That's a bit
	 * unfortunate, but hopefully shouldn't happen often.
	 */
	if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		visibilitymap_pin(relation, block, &vmbuffer);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	}

	result = HeapTupleSatisfiesUpdate(&tp, cid, buffer);

	if (result == TM_Invisible)
	{
		UnlockReleaseBuffer(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to delete invisible tuple")));
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tp.t_data);
		infomask = tp.t_data->t_infomask;

		/*
		 * Sleep until concurrent transaction ends -- except when there's a
		 * single locker and it's our own transaction.  Note we don't care
		 * which lock mode the locker has, because we need the strongest one.
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			bool		current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										LockTupleExclusive, &current_is_member))
			{
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, MultiXactStatusUpdate, infomask,
								relation, &(tp.t_self), XLTW_Delete,
								NULL);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 *
				 * We also must start over if we didn't pin the VM page, and
				 * the page has become all visible.
				 */
				if ((vmbuffer == InvalidBuffer && PageIsAllVisible(page)) ||
					xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
										 xwait))
					goto l1;
			}

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.  It is legal for us to delete
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).  We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else if (!TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, &(tp.t_self), XLTW_Delete);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 *
			 * We also must start over if we didn't pin the VM page, and the
			 * page has become all visible.
			 */
			if ((vmbuffer == InvalidBuffer && PageIsAllVisible(page)) ||
				xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
									 xwait))
				goto l1;

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(tp.t_data, buffer, xwait);
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if ((tp.t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tp.t_data))
			result = TM_Ok;
		else if (!ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		if (!HeapTupleSatisfiesVisibility(&tp, crosscheck, buffer))
			result = TM_Updated;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		Assert(!(tp.t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid));
		tmfd->ctid = tp.t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(tp.t_data);
		else
			tmfd->cmax = InvalidCommandId;
		UnlockReleaseBuffer(buffer);
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);
		if (vmbuffer != InvalidBuffer)
			ReleaseBuffer(vmbuffer);
		return result;
	}

	/*
	 * We're about to do the actual delete -- check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the delete
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple delete is visible).
	 */
	CheckForSerializableConflictIn(relation, tid, BufferGetBlockNumber(buffer));

	/* replace cid with a combo CID if necessary */
	HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &iscombo);

	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 */
	old_key_tuple = ExtractReplicaIdentity(relation, &tp, true, &old_key_copied);

	/*
	 * If this is the first possibly-multixact-able operation in the current
	 * transaction, set my per-backend OldestMemberMXactId setting. We can be
	 * certain that the transaction will never become a member of any older
	 * MultiXactIds than that.  (We have to do this even if we end up just
	 * using our own TransactionId below, since some other backend could
	 * incorporate our XID into a MultiXact immediately afterwards.)
	 */
	MultiXactIdSetOldestMember();

	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp.t_data),
							  tp.t_data->t_infomask, tp.t_data->t_infomask2,
							  xid, LockTupleExclusive, true,
							  &new_xmax, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();

	/*
	 * If this transaction commits, the tuple will become DEAD sooner or
	 * later.  Set flag that this page is a candidate for pruning once our xid
	 * falls below the OldestXmin horizon.  If the transaction finally aborts,
	 * the subsequent page pruning will be a no-op and the hint will be
	 * cleared.
	 */
	PageSetPrunable(page, xid);

	if (PageIsAllVisible(page))
	{
		all_visible_cleared = true;
		PageClearAllVisible(page);
		visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}

	/* store transaction information of xact deleting the tuple */
	tp.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tp.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tp.t_data->t_infomask |= new_infomask;
	tp.t_data->t_infomask2 |= new_infomask2;
	HeapTupleHeaderClearHotUpdated(tp.t_data);
	HeapTupleHeaderSetXmax(tp.t_data, new_xmax);
	HeapTupleHeaderSetCmax(tp.t_data, cid, iscombo);
	/* Make sure there is no forward chain link in t_ctid */
	tp.t_data->t_ctid = tp.t_self;

	/* Signal that this is actually a move into another partition */
	if (changingPart)
		HeapTupleHeaderSetMovedPartitions(tp.t_data);

	MarkBufferDirty(buffer);

	/*
	 * XLOG stuff
	 *
	 * NB: heap_abort_speculative() uses the same xlog record and replay
	 * routines.
	 */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_delete xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr	recptr;

		/*
		 * For logical decode we need combo CIDs to properly decode the
		 * catalog
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
			log_heap_new_cid(relation, &tp);

		xlrec.flags = 0;
		if (all_visible_cleared)
			xlrec.flags |= XLH_DELETE_ALL_VISIBLE_CLEARED;
		if (changingPart)
			xlrec.flags |= XLH_DELETE_IS_PARTITION_MOVE;
		xlrec.infobits_set = compute_infobits(tp.t_data->t_infomask,
											  tp.t_data->t_infomask2);
		xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);
		xlrec.xmax = new_xmax;

		if (old_key_tuple != NULL)
		{
			if (relation->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
				xlrec.flags |= XLH_DELETE_CONTAINS_OLD_TUPLE;
			else
				xlrec.flags |= XLH_DELETE_CONTAINS_OLD_KEY;
		}

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);

		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		/*
		 * Log replica identity of the deleted tuple if there is one
		 */
		if (old_key_tuple != NULL)
		{
			xlhdr.t_infomask2 = old_key_tuple->t_data->t_infomask2;
			xlhdr.t_infomask = old_key_tuple->t_data->t_infomask;
			xlhdr.t_hoff = old_key_tuple->t_data->t_hoff;

			XLogRegisterData((char *) &xlhdr, SizeOfHeapHeader);
			XLogRegisterData((char *) old_key_tuple->t_data
							 + SizeofHeapTupleHeader,
							 old_key_tuple->t_len
							 - SizeofHeapTupleHeader);
		}

		/* filtering by origin on a row level is much more efficient */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	/*
	 * If the tuple has toasted out-of-line attributes, we need to delete
	 * those items too.  We have to do this before releasing the buffer
	 * because we need to look at the contents of the tuple, but it's OK to
	 * release the content lock on the buffer first.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(&tp));
	}
	else if (HeapTupleHasExternal(&tp))
		heap_toast_delete(relation, &tp, false);

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. We have to do this before releasing the buffer because we
	 * need to look at the contents of the tuple.
	 */
	CacheInvalidateHeapTuple(relation, &tp, NULL);

	/* Now we can release the buffer */
	ReleaseBuffer(buffer);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);

	pgstat_count_heap_delete(relation);

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	return TM_Ok;
}

/*
 *	simple_heap_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
void
simple_heap_delete(Relation relation, ItemPointer tid)
{
	TM_Result	result;
	TM_FailureData tmfd;

	result = heap_delete(relation, tid,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &tmfd, false /* changingPart */ );
	switch (result)
	{
		case TM_SelfModified:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case TM_Ok:
			/* done successfully */
			break;

		case TM_Updated:
			elog(ERROR, "tuple concurrently updated");
			break;

		case TM_Deleted:
			elog(ERROR, "tuple concurrently deleted");
			break;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			break;
	}
}
#ifdef DIVA
/*
 *	heap_update_with_vc - replace a tuple
 *
 * See table_tuple_update() for an explanation of the parameters, except that
 * this routine directly takes a tuple rather than a slot.
 */
#ifdef LOCATOR
TM_Result
heap_update_with_vc(Relation relation,
					ItemPointer otid, 
					LocatorTuplePosition tuplePosition,
					LocatorExecutor *locator_executor,
					HeapTuple newtup,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode)
#else /* !LOCATOR */
TM_Result
heap_update_with_vc(Relation relation, ItemPointer otid, 
					HeapTuple newtup,
					CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode)
#endif /* LOCATOR */
{
	TM_Result result;
	TransactionId xid = GetCurrentTransactionId();
	TupleDesc	tupDesc = RelationGetDescr(relation);
	Bitmapset  *hot_attrs;
	Bitmapset  *key_attrs;
	Bitmapset  *id_attrs;
	Bitmapset  *interesting_attrs;
	Bitmapset  *modified_attrs;
	ItemId		lp;
	HeapTuple	oldtup;
	HeapTuple	targettup;
	HeapTuple	heaptup;
	HeapTuple	old_key_tuple = NULL;
	bool		old_key_copied = false;
	Page		page;
	BlockNumber	block;
	OffsetNumber offnum;
	MultiXactStatus mxact_status;
	Buffer		buffer,
				oldbuf = InvalidBuffer,
				targetbuf = InvalidBuffer,
				vmbuffer = InvalidBuffer;
	bool		need_toast;
	bool		have_tuple_lock = false;
	bool		iscombo;
	bool		key_intact;
	bool		all_visible_cleared = false;
	bool		all_visible_cleared_new = false;
	bool		checked_lockers;
	bool		locker_remains;
	bool		id_has_external = false;
	TransactionId xmax_new_tuple,
				xmax_old_tuple;
	uint16		infomask_old_tuple,
				infomask2_old_tuple,
				infomask_new_tuple,
				infomask2_new_tuple;
	HeapTupleHeaderData tmpTupHeader;

	/* variables for siro */
	/*
	 *                     newtup
	 *                       |
	 *                       V
	 *  ----------- -------------------- --------------------
	 * | P-locator | 2nd oldest version | 1st oldest version |
	 *  ----------- -------------------- --------------------
	 *                       |
	 *                       V
	 *               Provisional Index
	 */
	TransactionId xmin;
	TransactionId xmax = 0;
	bool      target_is_left;
	bool      is_second_old_exist;
	bool	  is_repeated_update;
	uint16	  target_maxlen;
	ItemId	  target_lp;

	ItemPointerData dummy_tid = *otid;
	ItemPointer tid_for_locking = &dummy_tid;

	/* for left version */
	ItemId			l_lp;
	ItemPointerData	l_pointer_data;
	ItemPointer		l_pointer = &l_pointer_data;
	HeapTupleData	l_loctup;
	BlockNumber		l_block;
	Buffer			l_buffer;
#if 0
	Buffer			l_vmbuffer = InvalidBuffer;
#endif
	Page			l_page;

	/* for right version */
	ItemId			r_lp;
	ItemPointerData	r_pointer_data;
	ItemPointer		r_pointer = &r_pointer_data;
	HeapTupleData	r_loctup;
	BlockNumber		r_block;
	Buffer			r_buffer;
#if 0
	Buffer			r_vmbuffer = InvalidBuffer;
#endif
	Page			r_page;
	
	/* for p-leaf meta tuple*/
	Item p_locator;

#ifdef LOCATOR
	LocatorExecutorLevelDesc *level_desc = NULL;
	LocatorExecutorColumnGroupDesc *siro_group_desc = NULL;

	pg_atomic_uint64 *dual_ref;
	uint64 prev_toggle_bit;
	uint64 wait_for_readers;
	bool is_appended_pleaf = false;
	uint32 pd_gen;
	uint32 *p_pd_gen;

	bool isLocator = IsLocator(relation);
	LocatorExternalCatalog *exCatalog = NULL;
	int		mempart_id;
	LWLock *mempartition_lock = NULL;

	if (isLocator)
	{
		Assert(tuplePosition != NULL);

		exCatalog = LocatorGetExternalCatalog(relation->rd_node.relNode);
	}
#endif /* LOCATOR */

	/* TODO: What is this case? */
	if (unlikely(tupDesc->maxlen == 0))
		TupleDescGetMaxLen(relation);

	Assert(ItemPointerIsValid(otid));

	/*
	 * Forbid this during a parallel operation, lest it allocate a combocid.
	 * Other workers might need that combocid for visibility checks, and we
	 * have no provision for broadcasting it to them.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot update tuples during a parallel operation")));

	/*
	 * Fetch the list of attributes to be checked for various operations.
	 *
	 * For HOT considerations, this is wasted effort if we fail to update or
	 * have to put the new tuple on a different page.  But we must compute the
	 * list before obtaining buffer lock --- in the worst case, if we are
	 * doing an update on one of the relevant system catalogs, we could
	 * deadlock if we try to fetch the list later.  In any case, the relcache
	 * caches the data so this is usually pretty cheap.
	 *
	 * We also need columns used by the replica identity and columns that are
	 * considered the "key" of rows in the table.
	 *
	 * Note that we get copies of each bitmap, so we need not worry about
	 * relcache flush happening midway through.
	 */
	hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
	key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
	id_attrs = RelationGetIndexAttrBitmap(relation,
										  INDEX_ATTR_BITMAP_IDENTITY_KEY);
	interesting_attrs = NULL;
	interesting_attrs = bms_add_members(interesting_attrs, hot_attrs);
	interesting_attrs = bms_add_members(interesting_attrs, key_attrs);
	interesting_attrs = bms_add_members(interesting_attrs, id_attrs);

#ifdef LOCATOR
	if (tuplePosition)
	{
		Assert(IsLocator(relation));

		if (locator_executor)
			level_desc = locator_get_level_columnar_desc(locator_executor,
												tuplePosition->partitionLevel);

		if (level_desc)
		{
			siro_group_desc = level_desc->siro_group_desc;

#ifdef LOCATOR_DEBUG
			Assert(siro_group_desc != NULL);
#endif /* LOCATOR_DEBUG */

			block = locator_calculate_columnar_tuple_blocknum(
											level_desc,
											siro_group_desc,
											tuplePosition->partitionTuplePosition);
			offnum
				= locator_calculate_tuple_position_in_block(
									level_desc,
									siro_group_desc,
									tuplePosition->partitionTuplePosition) * 3 + 1;
		}
		else
		{
			block = tuplePosition->partitionTuplePosition / relation->records_per_block;
			offnum = 
				(tuplePosition->partitionTuplePosition % 
						relation->records_per_block) * 3 + 1;
		}

		buffer = ReadPartitionBufferExtended(relation, 
										tuplePosition->partitionLevel, 
										tuplePosition->partitionNumber, 
										tuplePosition->partitionGenerationNumber,
										block, RBM_NORMAL, NULL);
		page = BufferGetPage(buffer);
	}
	else
	{
		/* otid is tid of p-locator */
		block = ItemPointerGetBlockNumber(otid);
		offnum = ItemPointerGetOffsetNumber(otid);
		buffer = ReadBuffer(relation, block);
		page = BufferGetPage(buffer);
	}
#else /* !LOCATOR */
	/* otid is tid of p-locator */
	block = ItemPointerGetBlockNumber(otid);
	offnum = ItemPointerGetOffsetNumber(otid);
	buffer = ReadBuffer(relation, block);
	page = BufferGetPage(buffer);
#endif /* LOCATOR */

	/*
	 * Before locking the buffer, pin the visibility map page if it appears to
	 * be necessary.  Since we haven't got the lock yet, someone else might be
	 * in the middle of changing this, so we'll need to recheck after we have
	 * the lock.
	 */
	if (PageIsAllVisible(page))
		visibilitymap_pin(relation, block, &vmbuffer);

#ifdef LOCATOR
	if (isLocator && tuplePosition->partitionLevel == 0)
	{
		/* Get mempartition lock */
		mempart_id = LocatorGetMempartIdFromBufId(buffer - 1);
		mempartition_lock = LocatorGetMempartLock(mempart_id);

		/* Acquire and release locks */
		LWLockAcquire(mempartition_lock, LW_SHARED);
	}
#endif /* LOCATOR */
	
	/* Lock buffer of p-locator */
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	/* the new tuple is ready, except for this: */
	newtup->t_tableOid = RelationGetRelid(relation);

#ifdef LOCATOR
	/* Get pd_gen */
	pd_gen = ((PageHeader)page)->pd_gen;
#endif /* LOCATOR */

	lp = PageGetItemId(page, offnum);
	Assert(ItemIdIsNormal(lp));

	/* Set p_locator */
	p_locator = (Item) PageGetItem(page, lp);

#ifdef LOCATOR
	Assert(!isLocator || PGetLocatorRecordKey(p_locator)->seqnum ==
						 tuplePosition->rec_key.seqnum);

	if (isLocator)
	{
		/*
		 * In LOCATOR, the heap relation consists of multiple partition files.
		 * So if a lock is acquired using tid as usual, the lock is acquired for
		 * tuples of all partitions.
		 * Therefore, in this case, LOCATOR uses the sequence number of the
		 * record instead of the tid, and simply adds 1 to bypass the validation
		 * checking of the ItemPointer.
		 */

		uint64 key = PGetLocatorRecordKey(p_locator)->seqnum + 1;

		dummy_tid.ip_posid = (uint16)(key & 0x000000000000FFFFUL);
		key >>= 16;
		dummy_tid.ip_blkid.bi_lo = (uint16)(key & 0x000000000000FFFFUL);
		key >>= 16;
		dummy_tid.ip_blkid.bi_hi = (uint16)(key & 0x000000000000FFFFUL);
	}
#endif /* LOCATOR */

	/* Set pointers */
	ItemPointerSet(l_pointer, block, offnum + 1);
	ItemPointerSet(r_pointer, block, offnum + 2);

#ifdef LOCATOR
	/* Set pd_gen of p-locator */
	p_pd_gen = (uint32*)(p_locator + PITEM_GEN_OFF);
	
	/* Set dula_ref */
	dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buffer);
#endif /* LOCATOR */

#if 0
set_l_r_2:
#endif

	/* Set left version's block, buffer, page */
	l_block = ItemPointerGetBlockNumber(l_pointer);

	if (unlikely(l_block != block))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		l_buffer = ReadBuffer(relation, l_block);
		l_page = BufferGetPage(l_buffer);

		if (PageIsAllVisible(l_page))
			visibilitymap_pin(relation, l_block, &l_vmbuffer);

		/* TODO: locking order must to be fixed to avoid deadlock */
		LockBuffer(l_buffer, BUFFER_LOCK_EXCLUSIVE);
#endif
	}
	else
	{
		l_buffer = buffer;
		l_page = page;
	}

#ifdef LOCATOR
	Assert(!isLocator || (ItemPointerGetOffsetNumber(l_pointer) == offnum + 1));
#endif /* LOCATOR */

	/* Set left version */
	l_lp = PageGetItemId(l_page, ItemPointerGetOffsetNumber(l_pointer));

	Assert(ItemIdIsNormal(l_lp));

	l_loctup.t_tableOid = RelationGetRelid(relation);
	l_loctup.t_data = (HeapTupleHeader) PageGetItem(l_page, l_lp);
	l_loctup.t_len = ItemIdGetLength(l_lp);

	l_loctup.t_self = *l_pointer;
	
	/* Set right version's block, buffer, page */
	r_block = ItemPointerGetBlockNumber(r_pointer);

	if (unlikely(r_block != block))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		if (likely(r_block != l_block))
		{
			r_buffer = ReadBuffer(relation, r_block);
			r_page = BufferGetPage(r_buffer);

			if (PageIsAllVisible(r_page))
				visibilitymap_pin(relation, r_block, &r_vmbuffer);

			/* TODO: locking order must to be fixed to avoid deadlock */
			LockBuffer(r_buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		else
		{
			r_buffer = l_buffer;
			r_page = l_page;
		}
#endif
	}
	else
	{
		r_buffer = buffer;
		r_page = page;
	}
#ifdef LOCATOR
	Assert(!isLocator || (ItemPointerGetOffsetNumber(r_pointer) == offnum + 2));
#endif /* LOCATOR */

	/* Set right version  */
	r_lp = PageGetItemId(r_page, ItemPointerGetOffsetNumber(r_pointer));

	Assert(ItemIdIsNormal(r_lp));

	if (unlikely(LP_OVR_IS_UNUSED(r_lp)))
		is_second_old_exist = false;
	else
		is_second_old_exist = true;

	r_loctup.t_tableOid = RelationGetRelid(relation);
#ifdef LOCATOR
	r_loctup.t_data = (HeapTupleHeader) PageGetItemNoCheck(r_page, r_lp);
#else /* !LOCATOR */
	r_loctup.t_data = (HeapTupleHeader) PageGetItem(r_page, r_lp);
#endif /* LOCATOR */
	r_loctup.t_len = ItemIdGetLength(r_lp);

	r_loctup.t_self = *r_pointer;

l2:
	if (unlikely(l_block != ItemPointerGetBlockNumber(l_pointer) ||
				 r_block != ItemPointerGetBlockNumber(r_pointer)))
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		goto set_l_r_2;
#endif
	}

	checked_lockers = false;
	locker_remains = false;

	if (LP_OVR_IS_LEFT(lp))
	{
		oldtup = &l_loctup;
		oldbuf = l_buffer;

		target_is_left = false;
	}
	else
	{
		oldtup = &r_loctup;
		oldbuf = r_buffer;

		target_is_left = true;
	}

	/*
	 * In LOCATOR, Since the tid passed as a parameter is the tid of the
	 * p-locator, the actual target version of the UPDATE must be checked,
	 * it is necessary to double-check the actual UPDATE target version.
	 */
	if (HeapTupleSatisfiesVisibility(oldtup, snapshot, oldbuf))
		result = HeapTupleSatisfiesUpdate(oldtup, cid, oldbuf);
	else if (likely(is_second_old_exist))
	{
		/* Set 2nd oldest version */
		if (oldtup == &l_loctup)
		{
			oldtup = &r_loctup;
			oldbuf = r_buffer;
		}
		else
		{
			oldtup = &l_loctup;
			oldbuf = l_buffer;
		}

		target_is_left = !target_is_left;

		if (HeapTupleSatisfiesVisibility(oldtup, snapshot, oldbuf))
			result = HeapTupleSatisfiesUpdate(oldtup, cid, oldbuf);
		else
			result = TM_Invisible;
		
		/*
		 * If the 1st oldest version is invisible and 2nd oldest version is
		 * visible, it means that 1st oldest version is aborted.
		 * In that case, the aborted 1st oldest version doesn't have to be
		 * appended into p-leaf and ebi-tree.
		 */
		is_second_old_exist = false;
	}
	else
		result = TM_Invisible;

	/*
	 * Determine columns modified by the update.  Additionally, identify
	 * whether any of the unmodified replica identity key attributes in the
	 * old tuple is externally stored or not.  This is required because for
	 * such attributes the flattened value won't be WAL logged as part of the
	 * new tuple so we must include it as part of the old_key_tuple.  See
	 * ExtractReplicaIdentity.
	 */
	modified_attrs = HeapDetermineColumnsInfo(relation, interesting_attrs,
											  id_attrs, oldtup,
											  newtup, &id_has_external);

	/*
	 * If we're not updating any "key" column, we can grab a weaker lock type.
	 * This allows for more concurrency when we are running simultaneously
	 * with foreign key checks.
	 *
	 * Note that if a column gets detoasted while executing the update, but
	 * the value ends up being the same, this test will fail and we will use
	 * the stronger lock.  This is acceptable; the important case to optimize
	 * is updates that don't manipulate key columns, not those that
	 * serendipitously arrive at the same key values.
	 */
	if (!bms_overlap(modified_attrs, key_attrs))
	{
		*lockmode = LockTupleNoKeyExclusive;
		mxact_status = MultiXactStatusNoKeyUpdate;
		key_intact = true;

		/*
		 * If this is the first possibly-multixact-able operation in the
		 * current transaction, set my per-backend OldestMemberMXactId
		 * setting. We can be certain that the transaction will never become a
		 * member of any older MultiXactIds than that.  (We have to do this
		 * even if we end up just using our own TransactionId below, since
		 * some other backend could incorporate our XID into a MultiXact
		 * immediately afterwards.)
		 */
		MultiXactIdSetOldestMember();
	}
	else
	{
#if 1
		Abort("TODO: append-update have to be implemented");
		// elog(ERROR, "TODO: key_update have to be implemented");
#else
		*lockmode = LockTupleExclusive;
		mxact_status = MultiXactStatusUpdate;
		key_intact = false;
#endif
	}

	/* see below about the "no wait" case */
	Assert(result != TM_BeingModified || wait);

	/* DIVA treats TM_Deleted as TM_Updated */
	if (result == TM_Invisible)
	{
		/*
		 * Because we are doing in-place update for siro implementation,
		 * oldtup could be invisible in this cases.
		 *
		 * 1. In a predecessor heap tuple search routine, we delivered any
		 *    one of tuple in the heap page, if there was no visible tuple.
		 *    It means that the transaction already found out the conflict,
		 *    and this result TM_Invisible is intended.
		 * 2. We doesn't acquire the page latch when we find a visible tuple,
		 *    so the tuple we found might be overwritten by another concurrent
		 *    updater.
		 *
		 * Both cases are acceptible, and we can treat it as TM_Update so that
		 * transaction should be aborted.
		 */
		result = TM_Updated;
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16    infomask;
		bool    can_continue = false;

		/*
		 * XXX note that we don't consider the "no wait" case here.  This
		 * isn't a problem currently because no caller uses that case, but it
		 * should be fixed if such a caller is introduced.  It wasn't a
		 * problem previously because this code would always wait, but now
		 * that some tuple locks do not conflict with one of the lock modes we
		 * use, it is possible that this case is interesting to handle
		 * specially.
		 *
		 * This may cause failures with third-party code that calls
		 * heap_update directly.
		 */

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(oldtup->t_data);
		infomask = oldtup->t_data->t_infomask;

		/*
		 * Now we have to do something about the existing locker.  If it's a
		 * multi, sleep on it; we might be awakened before it is completely
		 * gone (or even not sleep at all in some cases); we need to preserve
		 * it as locker, unless it is gone completely.
		 *
		 * If it's not a multi, we need to check for sleeping conditions
		 * before actually going to sleep.  If the update doesn't conflict
		 * with the locks, we just continue without sleeping (but making sure
		 * it is preserved).
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.  Note we must
		 * not acquire the tuple lock until we're sure we're going to sleep;
		 * otherwise we're open for race conditions with other transactions
		 * holding the tuple lock which sleep on us.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			TransactionId update_xact;
			int     remain;
			bool    current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										*lockmode, &current_is_member))
			{
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, tid_for_locking, *lockmode,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, mxact_status, infomask,
								relation, tid_for_locking, XLTW_Update,
								&remain);
				checked_lockers = true;
				locker_remains = remain != 0;
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(oldtup->t_data->t_infomask,
										  infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(oldtup->t_data),
										 xwait))
				{
					is_second_old_exist = true;
					goto l2;
				}
			}
			/*
			 * Note that the multixact may not be done by now.  It could have
			 * surviving members; our own xact or other subxacts of this
			 * backend, and also any other concurrent transaction that locked
			 * the tuple with KeyShare if we only got TupleLockUpdate.  If
			 * this is the case, we have to be careful to mark the updated
			 * tuple with the surviving members in Xmax.
			 *
			 * Note that there could have been another update in the
			 * MultiXact. In that case, we need to check whether it committed
			 * or aborted. If it aborted we are safe to update it again;
			 * otherwise there is an update conflict, and we have to return
			 * TableTuple{Deleted, Updated} below.
			 *
			 * In the LockTupleExclusive case, we still need to preserve the
			 * surviving members: those would include the tuple locks we had
			 * before this one, which are important to keep in case this
			 * subxact aborts.
			 */
			if (!HEAP_XMAX_IS_LOCKED_ONLY(oldtup->t_data->t_infomask))
				update_xact = HeapTupleGetUpdateXid(oldtup->t_data);
			else
				update_xact = InvalidTransactionId;

			/*
			 * There was no UPDATE in the MultiXact; or it aborted. No
			 * TransactionIdIsInProgress() call needed here, since we called
			 * MultiXactIdWait() above.
			 */
			if (!TransactionIdIsValid(update_xact) ||
				TransactionIdDidAbort(update_xact))
				can_continue = true;
		}
		else if (TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * The only locker is ourselves; we can avoid grabbing the tuple
			 * lock here, but must preserve our locking information.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && key_intact)
		{
			/*
			 * If it's just a key-share locker, and we're not changing the key
			 * columns, we don't need to wait for it to end; but we need to
			 * preserve it as locker.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			heap_acquire_tuplock(relation, tid_for_locking, *lockmode,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, tid_for_locking,
							  XLTW_Update);
			checked_lockers = true;
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(oldtup->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(xwait,
									 HeapTupleHeaderGetRawXmax(oldtup->t_data)))
			{
				is_second_old_exist = true;
				goto l2;
			}

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(oldtup->t_data, buffer, xwait);
			if (oldtup->t_data->t_infomask & HEAP_XMAX_INVALID)
			{
				/* xwait was  UPDATE, so second old is in ebi now */
				if (oldtup->t_data->t_infomask & HEAP_UPDATED)
					is_second_old_exist = false;
				can_continue = true;
			}
		}

		if (can_continue)
			result = TM_Ok;
		else
			result = TM_Updated;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		if (!HeapTupleSatisfiesVisibility(oldtup, crosscheck, oldbuf))
		{
			result = TM_Updated;
		}
	}

	/*
	 * Buffer latch could had been release if it acquired tuplock so that
	 * we need to check the visibility again here.
	 */
	if (result == TM_Ok)
	{
		if (HeapTupleSatisfiesVisibility(oldtup, snapshot, oldbuf))
			result = HeapTupleSatisfiesUpdate(oldtup, cid, oldbuf);
		else
			result = TM_Updated;

		if (result == TM_Invisible)
			result = TM_Updated;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
				result == TM_Updated ||
				result == TM_Deleted ||
				result == TM_BeingModified);
		tmfd->ctid = oldtup->t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(oldtup->t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(oldtup->t_data);
		else
			tmfd->cmax = InvalidCommandId;
		UnlockReleaseBuffer(buffer);
#ifdef LOCATOR
		if (isLocator && tuplePosition->partitionLevel == 0)
			LWLockRelease(mempartition_lock);
#endif /* LOCATOR */
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, tid_for_locking, *lockmode);
		if (vmbuffer != InvalidBuffer)
			ReleaseBuffer(vmbuffer);
		bms_free(hot_attrs);
		bms_free(key_attrs);
		bms_free(id_attrs);
		bms_free(modified_attrs);
		bms_free(interesting_attrs);

#ifdef LOCATOR
		if (isLocator && tuplePosition->needDecrRefCount)
		{
			LocatorPartLevel partLvl = tuplePosition->partitionLevel;
			LocatorPartNumber partNum = tuplePosition->partitionNumber;

			/* Decrement modification count. */
			DecrModificationCount(
				LocatorExternalCatalogRefCounter(exCatalog, partLvl, partNum), 
				tuplePosition->modificationMethod);
		}
#endif /* LOCATOR */

		return result;
	}

	/*
	 * If we didn't pin the visibility map page and the page has become all
	 * visible while we were busy locking the buffer, or during some
	 * subsequent window during which we had it unlocked, we'll have to unlock
	 * and re-lock, to avoid holding the buffer lock across an I/O.  That's a
	 * bit unfortunate, especially since we'll now have to recheck whether the
	 * tuple has been locked or updated under us, but hopefully it won't
	 * happen very often.
	 */
	if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		visibilitymap_pin(relation, block, &vmbuffer);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		is_second_old_exist = true;
		goto l2;
	}

	/* Fill in transaction status data */

	/*
	 * If the tuple we're updating is locked, we need to preserve the locking
	 * info in the old tuple's Xmax.  Prepare a new Xmax value for this.
	 */
	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup->t_data),
														oldtup->t_data->t_infomask,
														oldtup->t_data->t_infomask2,
														xid, *lockmode, true,
														&xmax_old_tuple, &infomask_old_tuple,
														&infomask2_old_tuple);
	/*
	 * And also prepare an Xmax value for the new copy of the tuple.  If there
	 * was no xmax previously, or there was one but all lockers are now gone,
	 * then use InvalidXid; otherwise, get the xmax from the old tuple.  (In
	 * rare cases that might also be InvalidXid and yet not have the
	 * HEAP_XMAX_INVALID bit set; that's fine.)
	 */
	if ((oldtup->t_data->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_LOCKED_UPGRADED(oldtup->t_data->t_infomask) ||
		(checked_lockers && !locker_remains))
		xmax_new_tuple = InvalidTransactionId;
	else
		xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup->t_data);

	if (!TransactionIdIsValid(xmax_new_tuple))
	{
		infomask_new_tuple = HEAP_XMAX_INVALID;
		infomask2_new_tuple = 0;
	}
	else
	{
		/*
		 * If we found a valid Xmax for the new tuple, then the infomask bits
		 * to use on the new tuple depend on what was there on the old one.
		 * Note that since we're doing an update, the only possibility is that
		 * the lockers had FOR KEY SHARE lock.
		 */
		if (oldtup->t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			GetMultiXactIdHintBits(xmax_new_tuple, &infomask_new_tuple,
								   &infomask2_new_tuple);
		}
		else
		{
			infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
			infomask2_new_tuple = 0;
		}
	}
	/*
	 * Prepare the new tuple with the appropriate initial values of Xmin and
	 * Xmax, as well as initial infomask bits as computed above.
	 */
	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	HeapTupleHeaderSetXmin(newtup->t_data, xid);
	HeapTupleHeaderSetCmin(newtup->t_data, cid);
	newtup->t_data->t_infomask |= HEAP_UPDATED | infomask_new_tuple;
	newtup->t_data->t_infomask2 |= infomask2_new_tuple;
	HeapTupleHeaderSetXmax(newtup->t_data, xmax_new_tuple);

	/*
	 * If xmin of the old tuple is same with current transaction id,
	 * it is repeated operation on a same record in a transaction.
	 */
	if (HeapTupleHeaderGetXmin(oldtup->t_data) == xid) {
		is_repeated_update = true;
		target_is_left = !target_is_left;
	} else {
		is_repeated_update = false;
	}

	/* Set target tuple */
	if (target_is_left)
	{
		targettup = &l_loctup;
		target_maxlen = tupDesc->maxlen;
		target_lp = l_lp;
		targetbuf = l_buffer;
	}
	else
	{
		targettup = &r_loctup;
		target_maxlen = tupDesc->maxlen;
		target_lp = r_lp;
		targetbuf = r_buffer;
	}

	/*
	 * Replace cid with a combo cid if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	HeapTupleHeaderAdjustCmax(oldtup->t_data, &cid, &iscombo);

	/*
	 * If the toaster needs to be activated, OR if the new tuple will not fit
	 * on the same page as the old, then we need to release the content lock
	 * (but not the pin!) on the old tuple's buffer while we are off doing
	 * TOAST and/or table-file-extension work.  We must mark the old tuple to
	 * show that it's locked, else other processes may try to update it
	 * themselves.
	 *
	 * We need to invoke the toaster if there are already any out-of-line
	 * toasted values present, or if the new tuple is over-threshold.
	 */
	Assert(relation->rd_rel->relkind == RELKIND_RELATION ||
		   relation->rd_rel->relkind == RELKIND_MATVIEW);
		   
	need_toast = (HeapTupleHasExternal(oldtup) ||
				  HeapTupleHasExternal(newtup) ||
				  newtup->t_len > TOAST_TUPLE_THRESHOLD);

	if (need_toast)
	{
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		TransactionId xmax_lock_old_tuple;
		uint16		infomask_lock_old_tuple,
					infomask2_lock_old_tuple;
		bool		cleared_all_frozen = false;

		/*
		 * To prevent concurrent sessions from updating the tuple, we have to
		 * temporarily mark it locked, while we release the page-level lock.
		 *
		 * To satisfy the rule that any xid potentially appearing in a buffer
		 * written out to disk, we unfortunately have to WAL log this
		 * temporary modification.  We can reuse xl_heap_lock for this
		 * purpose.  If we crash/error before following through with the
		 * actual update, xmax will be of an aborted transaction, allowing
		 * other sessions to proceed.
		 */

		/*
		 * Compute xmax / infomask appropriate for locking the tuple. This has
		 * to be done separately from the combo that's going to be used for
		 * updating, because the potentially created multixact would otherwise
		 * be wrong.
		 */
		compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup->t_data),
								  oldtup->t_data->t_infomask,
								  oldtup->t_data->t_infomask2,
								  xid, *lockmode, false,
								  &xmax_lock_old_tuple, &infomask_lock_old_tuple,
								  &infomask2_lock_old_tuple);

		Assert(HEAP_XMAX_IS_LOCKED_ONLY(infomask_lock_old_tuple));

		START_CRIT_SECTION();

		/* Clear obsolete visibility flags ... */
		oldtup->t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		oldtup->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleClearHotUpdated(oldtup);
		/* ... and store info about transaction updating this tuple */
		Assert(TransactionIdIsValid(xmax_lock_old_tuple));
		HeapTupleHeaderSetXmax(oldtup->t_data, xmax_lock_old_tuple);
		oldtup->t_data->t_infomask |= infomask_lock_old_tuple;
		oldtup->t_data->t_infomask2 |= infomask2_lock_old_tuple;
		HeapTupleHeaderSetCmax(oldtup->t_data, cid, iscombo);

		/*
		 * Clear all-frozen bit on visibility map if needed. We could
		 * immediately reset ALL_VISIBLE, but given that the WAL logging
		 * overhead would be unchanged, that doesn't seem necessarily
		 * worthwhile.
		 */
		if (PageIsAllVisible(page) &&
			visibilitymap_clear(relation, block, vmbuffer,
								VISIBILITYMAP_ALL_FROZEN))
			cleared_all_frozen = true;

		if (RelationNeedsWAL(relation))
		{
			xl_heap_lock xlrec;
			XLogRecPtr	recptr;

			XLogBeginInsert();
			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			xlrec.offnum = ItemPointerGetOffsetNumber(&(oldtup->t_self));
			xlrec.locking_xid = xmax_lock_old_tuple;
			xlrec.infobits_set = compute_infobits(oldtup->t_data->t_infomask,
												  oldtup->t_data->t_infomask2);
			xlrec.flags =
				cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;
			XLogRegisterData((char *) &xlrec, SizeOfHeapLock);
			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * Let the toaster do its thing.
		 *
		 * Note: below this point, heaptup is the data we actually intend to
		 * store into the relation; newtup is the caller's original untoasted
		 * data.
		 */

		/* Note we always use WAL and FSM during updates */
		heaptup = heap_toast_insert_or_update(relation, newtup, oldtup, 0);
		newtupsize = MAXALIGN(heaptup->t_len);

		/*
		 * Now, do we need a new page for the tuple, or not?  This is a bit
		 * tricky since someone else could have added tuples to the page while
		 * we weren't looking.  We have to recheck the available space after
		 * reacquiring the buffer lock.  But don't bother to do that if the
		 * former amount of free space is still not enough; it's unlikely
		 * there's more free now than before.
		 *
		 * What's more, if we need to get a new page, we will need to acquire
		 * buffer locks on both old and new pages.  To avoid deadlock against
		 * some other backend trying to get the same two locks in the other
		 * order, we must be consistent about the order we get the locks in.
		 * We use the rule "lock the lower-numbered page of the relation
		 * first".  To implement this, we must do RelationGetBufferForTuple
		 * while not holding the lock on the old page, and we must rely on it
		 * to get the locks on both pages in the correct order.
		 *
		 * Another consideration is that we need visibility map page pin(s) if
		 * we will have to clear the all-visible flag on either page.  If we
		 * call RelationGetBufferForTuple, we rely on it to acquire any such
		 * pins; but if we don't, we have to handle that here.  Hence we need
		 * a loop.
		 */
		for (;;)
		{
			if (newtupsize > pagefree)
			{
				/* It doesn't fit, must use RelationGetBufferForTuple. */
				// newbuf = RelationGetBufferForTuple(relation, heaptup->t_len,
				// 								   buffer, 0, NULL,
				// 								   &vmbuffer_new, &vmbuffer);
				/* We're all done. */
				break;
			}
			/* Acquire VM page pin if needed and we don't have it. */
			if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
				visibilitymap_pin(relation, block, &vmbuffer);
			/* Re-acquire the lock on the old tuple's page. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			/* Re-check using the up-to-date free space */
			pagefree = PageGetHeapFreeSpace(page);
			if (newtupsize > pagefree ||
				(vmbuffer == InvalidBuffer && PageIsAllVisible(page)))
			{
				/*
				 * Rats, it doesn't fit anymore, or somebody just now set the
				 * all-visible flag.  We must now unlock and loop to avoid
				 * deadlock.  Fortunately, this path should seldom be taken.
				 */
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			}
			else
			{
				/* We're all done. */
				// newbuf = buffer;
				break;
			}
		}
#endif
	}
	else
	{
		/* No TOAST work needed, and it'll fit on same page */
		heaptup = newtup;
	}

	/* Set ctid to its p-leaf meta tuple pointer */
	heaptup->t_data->t_ctid = *otid;
	heaptup->t_self = targettup->t_self;

	/*
	 * We're about to do the actual update -- check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the pages between this check and the update
	 * being visible to the scan (i.e., exclusive buffer content lock(s) are
	 * continuously held from this point until the tuple update is visible).
	 *
	 * For the new tuple the only check needed is at the relation level, but
	 * since both tuples are in the same relation and the check for oldtup
	 * will include checking the relation level, there is no benefit to a
	 * separate check for the new tuple.
	 */
	CheckForSerializableConflictIn(relation, &(oldtup->t_self),
								   BufferGetBlockNumber(buffer));

	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 * ExtractReplicaIdentity() will return NULL if nothing needs to be
	 * logged.
	 */
	old_key_tuple = ExtractReplicaIdentity(relation, oldtup,
										   bms_overlap(modified_attrs, id_attrs) ||
										   id_has_external,
										   &old_key_copied);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/*
	 * If this transaction commits, the old tuple will become DEAD sooner or
	 * later.  Set flag that this page is a candidate for pruning once our xid
	 * falls below the OldestXmin horizon.  If the transaction finally aborts,
	 * the subsequent page pruning will be a no-op and the hint will be
	 * cleared.
	 *
	 * XXX Should we set hint on newbuf as well?  If the transaction aborts,
	 * there would be a prunable tuple in the newbuf; but for now we choose
	 * not to optimize for aborts.  Note that heap_xlog_update must be kept in
	 * sync if this decision changes.
	 */
	PageSetPrunable(page, xid);

	/* Make sure tuples are correctly marked as not-HOT */
	HeapTupleClearHeapOnly(heaptup);
	HeapTupleClearHeapOnly(newtup);

#if defined(LOCATOR) && !defined(USING_LOCK)
	/* Toggle the bit, set type of operation and offset number of target */
	prev_toggle_bit = DRefSetModicationInfo(dual_ref, DRefStateUPDATE, offnum);
	wait_for_readers = prev_toggle_bit == 0 ? DRefToggleOffRefCnt :
											  DRefToggleOnRefCnt;

	pg_memory_barrier();
#endif /* LOCATOR */

	/* Recheck */
	if (is_second_old_exist)
	{
		is_second_old_exist = (oldtup->t_data->t_choice.t_heap.t_xmax ==
							   InvalidTransactionId);
	}

	pg_memory_barrier();

	/*
	 * IMPORTANT: APPENDING AN OLD TUPLE INTO VCLUSTER
	 * Copy the old tuple into the version cluster before overwriting it with
	 * new tuple, only if the second old version is exist.
	 */

	if (is_second_old_exist && !is_repeated_update)
	{
		int ret_status;
		bool is_left;
		uint64 l_off, r_off;
		LWLock* lwlock;

		// END_CRIT_SECTION();

		Assert(IsVersionOld(targettup));

		xmin = targettup->t_data->t_choice.t_heap.t_xmin;
		xmax = targettup->t_data->t_choice.t_heap.t_xmax;

		Assert(xmax >= xmin);

		memcpy(&l_off, p_locator, sizeof(uint64));
		memcpy(&r_off, p_locator + sizeof(uint64), sizeof(uint64));

		is_left = PLeafIsLeftUpdate(l_off, r_off, &ret_status);

		/* Change record's xid value as needed */
		switch (ret_status)
		{
			case PLEAF_RESET:
				memset(p_locator + sizeof(uint64) * 2, 0x00, sizeof(TransactionId));
				break;
			case PLEAF_SWITCH:
				memcpy(p_locator + sizeof(uint64) * 2, &xmin, sizeof(TransactionId));
				break;
			case PLEAF_NORMAL:
				/* nothing to do */
				break;
			default:
				Assert(false);
		}

		/* Append the version to VCluster */
		Assert(!BufferIsLocal(buffer));

		/*
		 * !!!
		 * In PLeafAppendTuple, we only acquire this latch(buffer) in specific
		 * cases.
		 */
		/* Get lwlock pointer of buffer */
		lwlock = (LWLock*) GetBufferLock(buffer);

#ifdef LOCATOR
		if (is_left)
		{
			is_appended_pleaf =
				PLeafAppendTuple(targettup->t_tableOid,
          						 l_off, (uint64*) p_locator, xmin, xmax,
          						 targettup->t_len, targettup->t_data, 
								 lwlock);
		}
		else
		{
			is_appended_pleaf =
				PLeafAppendTuple(targettup->t_tableOid,
          				 		 r_off, (uint64*) (p_locator + sizeof(uint64)), 
          				 		 xmin, xmax, targettup->t_len, targettup->t_data, 
						 		 lwlock);
		}
#else /* !LOCATOR */
		if (is_left)
		{
			PLeafAppendTuple(targettup->t_tableOid,
          					 l_off, (uint64*) p_locator, xmin, xmax,
          					 targettup->t_len, targettup->t_data, 
							 lwlock);
		}
		else
		{
			PLeafAppendTuple(targettup->t_tableOid,
          					 r_off, (uint64*) (p_locator + sizeof(uint64)), 
          					 xmin, xmax, targettup->t_len, targettup->t_data, 
							 lwlock);
		}
#endif /* LOCATOR */

		EbiMarkTupleSize(target_maxlen);

		// START_CRIT_SECTION();
	}

#if defined(LOCATOR) && !defined(USING_LOCK)
	/*
	 * The writer transaction had to wait for the readers to finish reading the
	 * record they were reading at the time the bit was toggled.
	 * (Detail description is in GetCheckVar().)
	 */
	DRefWaitForReaders(dual_ref, wait_for_readers);

	/* Writer must update version after PLeafAppendTuple() */
	pg_memory_barrier();
#endif /* LOCATOR */

	/*
	 * IMPORTANT: OVERWRITE THE 2nd OLD TUPLE WITH A NEW TUPLE
	 */

#ifdef LOCATOR
#ifndef USING_LOCK
	/*
	 * In LOCATOR, the share lock of buffer isn't acquired during SeqScan, so


	 * the contention between reader and writer is managed using an dual
	 * reference counter.
	 *
	 * When the writer performs an in-place update, it first sets the MSB of the
	 * area of reference counter of target record to be updated, then waits for
	 * the reader that references the area to finish reading before doing update.
	 */
	if (target_is_left)
		DRefSetAndWaitForLeft(dual_ref);
	else
		DRefSetAndWaitForRight(dual_ref);

	/* Writer must update version after readers passed by */
	pg_memory_barrier();
#endif /* !USING_LOCK*/

	if (is_appended_pleaf)
		PLeafReleaseHoldingResources(pd_gen, p_pd_gen);
#endif /* LOCATOR */

	if (likely(heaptup->t_len <= target_maxlen))
	{
		memcpy(targettup->t_data, heaptup->t_data, heaptup->t_len);
		target_lp->lp_len = heaptup->t_len;

		/* Set ItemId to USING */
		LP_OVR_SET_USING(target_lp);
	}
	else
	{
		/* cannot perform in-place update */
#if 1
		Abort("TODO: append-update have to be implemented");
#else
		*target_maxlen = MAXALIGN(heaptup->t_len);

		is_appended_update = true;
#endif
	}

	tmpTupHeader = *(oldtup->t_data);

	Assert(targettup != oldtup);

	/* Set xmax and infomask of copied tuple header */

	/* Clear obsolete visibility flags, possibly set by ourselves above... */
	tmpTupHeader.t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tmpTupHeader.t_infomask2 &= ~HEAP_KEYS_UPDATED;
	/* ... and store info about transaction updating this tuple */
	Assert(TransactionIdIsValid(xmax_old_tuple));
	HeapTupleHeaderSetXmax(&tmpTupHeader, xmax_old_tuple);
	tmpTupHeader.t_infomask |= infomask_old_tuple;
	tmpTupHeader.t_infomask2 |= infomask2_old_tuple;
	HeapTupleHeaderSetCmax(&tmpTupHeader, cid, iscombo);

	/* Paste info atomically */
	FetchTupleHeaderInfo(oldtup, tmpTupHeader);

	if (target_is_left)
		LP_OVR_SET_LEFT(lp);
	else
		LP_OVR_SET_RIGHT(lp);

	pg_memory_barrier();

#if defined(LOCATOR) && !defined(USING_LOCK)
	/* Turns off modification bit */
	DRefClearModicationInfo(dual_ref);
#endif /* LOCATOR */

	/* clear PD_ALL_VISIBLE flags, reset all visibilitymap bits */
	if (PageIsAllVisible(BufferGetPage(buffer)))
	{
		all_visible_cleared = true;
		PageClearAllVisible(BufferGetPage(buffer));
		visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}
#if 0 /* DIVA doesn't use visibility map */
	if (v_buffer != buffer && PageIsAllVisible(BufferGetPage(v_buffer)))
	{
		all_visible_cleared_new = true;
		PageClearAllVisible(BufferGetPage(v_buffer));
		visibilitymap_clear  (relation, BufferGetBlockNumber(v_buffer),
							vmbuffer_new, VISIBILITYMAP_VALID_BITS);
	}
	if (d_buffer != buffer && PageIsAllVisible(BufferGetPage(d_buffer)))
	{
		all_visible_cleared_new = true;
		PageClearAllVisible(BufferGetPage(d_buffer));
		visibilitymap_clear(relation, BufferGetBlockNumber(d_buffer),
							vmbuffer_new, VISIBILITYMAP_VALID_BITS);
	}
#endif

	if (buffer != targetbuf && !is_repeated_update)
		MarkBufferDirty(buffer);
	if (oldbuf != targetbuf && !is_repeated_update)
		MarkBufferDirty(oldbuf);
	MarkBufferDirty(targetbuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr  recptr;

		/*
		 * For logical decoding we need combocids to properly decode the
		 * catalog.
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
		{
			log_heap_new_cid(relation, oldtup);
			log_heap_new_cid(relation, heaptup);
		}

		recptr = log_heap_update(relation, oldbuf,
								 targetbuf, oldtup, heaptup,
								 old_key_tuple,
								 all_visible_cleared,
								 all_visible_cleared_new);
		if (targetbuf != oldbuf)
		{
			PageSetLSN(BufferGetPage(targetbuf), recptr);
		}
		PageSetLSN(BufferGetPage(oldbuf), recptr);
	}

	END_CRIT_SECTION();

	pg_memory_barrier();

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#ifdef LOCATOR
	if (isLocator && tuplePosition->partitionLevel == 0)
		LWLockRelease(mempartition_lock);
#endif /* LOCATOR */

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort. We
	 * have to do this before releasing the buffer because oldtup is in the
	 * buffer.  (heaptup is all in local memory, but it's necessary to process
	 * both tuple versions in one call to inval.c so we can avoid redundant
	 * sinval messages.)
	 */
	CacheInvalidateHeapTuple(relation, oldtup, heaptup);

	/* Now we can release the buffer(s) */
	if (l_buffer != buffer)
		ReleaseBuffer(l_buffer);
	if (r_buffer != buffer)
		ReleaseBuffer(r_buffer);
	ReleaseBuffer(buffer);
#if 0
	if (BufferIsValid(vmbuffer_new))
		ReleaseBuffer(vmbuffer_new);
#endif
	if (BufferIsValid(vmbuffer))
		ReleaseBuffer(vmbuffer);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, tid_for_locking, *lockmode);

	pgstat_count_heap_update(relation, true);

	heaptup->t_self = *otid;

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != newtup)
	{
		newtup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	bms_free(hot_attrs);
	bms_free(key_attrs);
	bms_free(id_attrs);
	bms_free(modified_attrs);
	bms_free(interesting_attrs);

#ifdef LOCATOR
	if (isLocator && tuplePosition->needDecrRefCount)
	{
		LocatorPartLevel partLvl = tuplePosition->partitionLevel;
		LocatorPartNumber partNum = tuplePosition->partitionNumber;
		LocatorPartGenNo partGen = tuplePosition->partitionGenerationNumber;

		if (tuplePosition->modificationMethod == MODIFICATION_WITH_DELTA &&
			(partLvl != 0 || CheckPartitioningInLevelZero(exCatalog, partNum, partGen)))
		{
			bool isLevelZero = (partLvl == 0 ? true : false);

			/* Add delta to dsa space. */
			AppendLocatorDelta(locatorPartitionDsaArea, exCatalog, 
												tuplePosition, isLevelZero);
		}

		/* Decrement modification count. */
		DecrModificationCount(
			LocatorExternalCatalogRefCounter(exCatalog, partLvl, partNum), 
			tuplePosition->modificationMethod);
	}
#endif /* LOCATOR */

	return TM_Ok;
}
#endif /* LOCATOR */

/*
 *	heap_update - replace a tuple
 *
 * See table_tuple_update() for an explanation of the parameters, except that
 * this routine directly takes a tuple rather than a slot.
 *
 * In the failure cases, the routine fills *tmfd with the tuple's t_ctid,
 * t_xmax (resolving a possible MultiXact, if necessary), and t_cmax (the last
 * only for TM_SelfModified, since we cannot obtain cmax from a combo CID
 * generated by another transaction).
 */
TM_Result
heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
			CommandId cid, Snapshot crosscheck, bool wait,
			TM_FailureData *tmfd, LockTupleMode *lockmode)
{
	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	Bitmapset  *hot_attrs;
	Bitmapset  *key_attrs;
	Bitmapset  *id_attrs;
	Bitmapset  *interesting_attrs;
	Bitmapset  *modified_attrs;
	ItemId		lp;
	HeapTupleData oldtup;
	HeapTuple	heaptup;
	HeapTuple	old_key_tuple = NULL;
	bool		old_key_copied = false;
	Page		page;
	BlockNumber block;
	MultiXactStatus mxact_status;
	Buffer		buffer,
				newbuf,
				vmbuffer = InvalidBuffer,
				vmbuffer_new = InvalidBuffer;
	bool		need_toast;
	Size		newtupsize,
				pagefree;
	bool		have_tuple_lock = false;
	bool		iscombo;
	bool		use_hot_update = false;
	bool		key_intact;
	bool		all_visible_cleared = false;
	bool		all_visible_cleared_new = false;
	bool		checked_lockers;
	bool		locker_remains;
	bool		id_has_external = false;
	TransactionId xmax_new_tuple,
				xmax_old_tuple;
	uint16		infomask_old_tuple,
				infomask2_old_tuple,
				infomask_new_tuple,
				infomask2_new_tuple;

	Assert(ItemPointerIsValid(otid));

	/* Cheap, simplistic check that the tuple matches the rel's rowtype. */
	Assert(HeapTupleHeaderGetNatts(newtup->t_data) <=
		   RelationGetNumberOfAttributes(relation));

	/*
	 * Forbid this during a parallel operation, lest it allocate a combo CID.
	 * Other workers might need that combo CID for visibility checks, and we
	 * have no provision for broadcasting it to them.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot update tuples during a parallel operation")));

	/*
	 * Fetch the list of attributes to be checked for various operations.
	 *
	 * For HOT considerations, this is wasted effort if we fail to update or
	 * have to put the new tuple on a different page.  But we must compute the
	 * list before obtaining buffer lock --- in the worst case, if we are
	 * doing an update on one of the relevant system catalogs, we could
	 * deadlock if we try to fetch the list later.  In any case, the relcache
	 * caches the data so this is usually pretty cheap.
	 *
	 * We also need columns used by the replica identity and columns that are
	 * considered the "key" of rows in the table.
	 *
	 * Note that we get copies of each bitmap, so we need not worry about
	 * relcache flush happening midway through.
	 */
	hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
	key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
	id_attrs = RelationGetIndexAttrBitmap(relation,
										  INDEX_ATTR_BITMAP_IDENTITY_KEY);
	interesting_attrs = NULL;
	interesting_attrs = bms_add_members(interesting_attrs, hot_attrs);
	interesting_attrs = bms_add_members(interesting_attrs, key_attrs);
	interesting_attrs = bms_add_members(interesting_attrs, id_attrs);

	block = ItemPointerGetBlockNumber(otid);
	buffer = ReadBuffer(relation, block);
	page = BufferGetPage(buffer);

	/*
	 * Before locking the buffer, pin the visibility map page if it appears to
	 * be necessary.  Since we haven't got the lock yet, someone else might be
	 * in the middle of changing this, so we'll need to recheck after we have
	 * the lock.
	 */
	if (PageIsAllVisible(page))
		visibilitymap_pin(relation, block, &vmbuffer);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
	Assert(ItemIdIsNormal(lp));

	/*
	 * Fill in enough data in oldtup for HeapDetermineColumnsInfo to work
	 * properly.
	 */
	oldtup.t_tableOid = RelationGetRelid(relation);
	oldtup.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	oldtup.t_len = ItemIdGetLength(lp);
	oldtup.t_self = *otid;

	/* the new tuple is ready, except for this: */
	newtup->t_tableOid = RelationGetRelid(relation);

	/*
	 * Determine columns modified by the update.  Additionally, identify
	 * whether any of the unmodified replica identity key attributes in the
	 * old tuple is externally stored or not.  This is required because for
	 * such attributes the flattened value won't be WAL logged as part of the
	 * new tuple so we must include it as part of the old_key_tuple.  See
	 * ExtractReplicaIdentity.
	 */
	modified_attrs = HeapDetermineColumnsInfo(relation, interesting_attrs,
											  id_attrs, &oldtup,
											  newtup, &id_has_external);

	/*
	 * If we're not updating any "key" column, we can grab a weaker lock type.
	 * This allows for more concurrency when we are running simultaneously
	 * with foreign key checks.
	 *
	 * Note that if a column gets detoasted while executing the update, but
	 * the value ends up being the same, this test will fail and we will use
	 * the stronger lock.  This is acceptable; the important case to optimize
	 * is updates that don't manipulate key columns, not those that
	 * serendipitously arrive at the same key values.
	 */
	if (!bms_overlap(modified_attrs, key_attrs))
	{
		*lockmode = LockTupleNoKeyExclusive;
		mxact_status = MultiXactStatusNoKeyUpdate;
		key_intact = true;

		/*
		 * If this is the first possibly-multixact-able operation in the
		 * current transaction, set my per-backend OldestMemberMXactId
		 * setting. We can be certain that the transaction will never become a
		 * member of any older MultiXactIds than that.  (We have to do this
		 * even if we end up just using our own TransactionId below, since
		 * some other backend could incorporate our XID into a MultiXact
		 * immediately afterwards.)
		 */
		MultiXactIdSetOldestMember();
	}
	else
	{
		*lockmode = LockTupleExclusive;
		mxact_status = MultiXactStatusUpdate;
		key_intact = false;
	}

	/*
	 * Note: beyond this point, use oldtup not otid to refer to old tuple.
	 * otid may very well point at newtup->t_self, which we will overwrite
	 * with the new tuple's location, so there's great risk of confusion if we
	 * use otid anymore.
	 */

l2:
	checked_lockers = false;
	locker_remains = false;
	result = HeapTupleSatisfiesUpdate(&oldtup, cid, buffer);

	/* see below about the "no wait" case */
	Assert(result != TM_BeingModified || wait);

	if (result == TM_Invisible)
	{
		UnlockReleaseBuffer(buffer);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("attempted to update invisible tuple")));
	}
	else if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16		infomask;
		bool		can_continue = false;

		/*
		 * XXX note that we don't consider the "no wait" case here.  This
		 * isn't a problem currently because no caller uses that case, but it
		 * should be fixed if such a caller is introduced.  It wasn't a
		 * problem previously because this code would always wait, but now
		 * that some tuple locks do not conflict with one of the lock modes we
		 * use, it is possible that this case is interesting to handle
		 * specially.
		 *
		 * This may cause failures with third-party code that calls
		 * heap_update directly.
		 */

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(oldtup.t_data);
		infomask = oldtup.t_data->t_infomask;

		/*
		 * Now we have to do something about the existing locker.  If it's a
		 * multi, sleep on it; we might be awakened before it is completely
		 * gone (or even not sleep at all in some cases); we need to preserve
		 * it as locker, unless it is gone completely.
		 *
		 * If it's not a multi, we need to check for sleeping conditions
		 * before actually going to sleep.  If the update doesn't conflict
		 * with the locks, we just continue without sleeping (but making sure
		 * it is preserved).
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.  Note we must
		 * not acquire the tuple lock until we're sure we're going to sleep;
		 * otherwise we're open for race conditions with other transactions
		 * holding the tuple lock which sleep on us.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			TransactionId update_xact;
			int			remain;
			bool		current_is_member = false;

			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										*lockmode, &current_is_member))
			{
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

				/*
				 * Acquire the lock, if necessary (but skip it when we're
				 * requesting a lock and already have one; avoids deadlock).
				 */
				if (!current_is_member)
					heap_acquire_tuplock(relation, &(oldtup.t_self), *lockmode,
										 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, mxact_status, infomask,
								relation, &oldtup.t_self, XLTW_Update,
								&remain);
				checked_lockers = true;
				locker_remains = remain != 0;
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(oldtup.t_data->t_infomask,
										  infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(oldtup.t_data),
										 xwait))
					goto l2;
			}

			/*
			 * Note that the multixact may not be done by now.  It could have
			 * surviving members; our own xact or other subxacts of this
			 * backend, and also any other concurrent transaction that locked
			 * the tuple with LockTupleKeyShare if we only got
			 * LockTupleNoKeyExclusive.  If this is the case, we have to be
			 * careful to mark the updated tuple with the surviving members in
			 * Xmax.
			 *
			 * Note that there could have been another update in the
			 * MultiXact. In that case, we need to check whether it committed
			 * or aborted. If it aborted we are safe to update it again;
			 * otherwise there is an update conflict, and we have to return
			 * TableTuple{Deleted, Updated} below.
			 *
			 * In the LockTupleExclusive case, we still need to preserve the
			 * surviving members: those would include the tuple locks we had
			 * before this one, which are important to keep in case this
			 * subxact aborts.
			 */
			if (!HEAP_XMAX_IS_LOCKED_ONLY(oldtup.t_data->t_infomask))
				update_xact = HeapTupleGetUpdateXid(oldtup.t_data);
			else
				update_xact = InvalidTransactionId;

			/*
			 * There was no UPDATE in the MultiXact; or it aborted. No
			 * TransactionIdIsInProgress() call needed here, since we called
			 * MultiXactIdWait() above.
			 */
			if (!TransactionIdIsValid(update_xact) ||
				TransactionIdDidAbort(update_xact))
				can_continue = true;
		}
		else if (TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * The only locker is ourselves; we can avoid grabbing the tuple
			 * lock here, but must preserve our locking information.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) && key_intact)
		{
			/*
			 * If it's just a key-share locker, and we're not changing the key
			 * columns, we don't need to wait for it to end; but we need to
			 * preserve it as locker.
			 */
			checked_lockers = true;
			locker_remains = true;
			can_continue = true;
		}
		else
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			heap_acquire_tuplock(relation, &(oldtup.t_self), *lockmode,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, &oldtup.t_self,
							  XLTW_Update);
			checked_lockers = true;
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(oldtup.t_data->t_infomask, infomask) ||
				!TransactionIdEquals(xwait,
									 HeapTupleHeaderGetRawXmax(oldtup.t_data)))
				goto l2;

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(oldtup.t_data, buffer, xwait);
			if (oldtup.t_data->t_infomask & HEAP_XMAX_INVALID)
				can_continue = true;
		}

		if (can_continue)
			result = TM_Ok;
		else if (!ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		if (!HeapTupleSatisfiesVisibility(&oldtup, crosscheck, buffer))
		{
			result = TM_Updated;
			Assert(!ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid));
		}
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		Assert(!(oldtup.t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid));
		tmfd->ctid = oldtup.t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(oldtup.t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(oldtup.t_data);
		else
			tmfd->cmax = InvalidCommandId;
		UnlockReleaseBuffer(buffer);
		if (have_tuple_lock)
			UnlockTupleTuplock(relation, &(oldtup.t_self), *lockmode);
		if (vmbuffer != InvalidBuffer)
			ReleaseBuffer(vmbuffer);
		bms_free(hot_attrs);
		bms_free(key_attrs);
		bms_free(id_attrs);
		bms_free(modified_attrs);
		bms_free(interesting_attrs);
		return result;
	}

	/*
	 * If we didn't pin the visibility map page and the page has become all
	 * visible while we were busy locking the buffer, or during some
	 * subsequent window during which we had it unlocked, we'll have to unlock
	 * and re-lock, to avoid holding the buffer lock across an I/O.  That's a
	 * bit unfortunate, especially since we'll now have to recheck whether the
	 * tuple has been locked or updated under us, but hopefully it won't
	 * happen very often.
	 */
	if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		visibilitymap_pin(relation, block, &vmbuffer);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		goto l2;
	}

	/* Fill in transaction status data */

	/*
	 * If the tuple we're updating is locked, we need to preserve the locking
	 * info in the old tuple's Xmax.  Prepare a new Xmax value for this.
	 */
	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup.t_data),
							  oldtup.t_data->t_infomask,
							  oldtup.t_data->t_infomask2,
							  xid, *lockmode, true,
							  &xmax_old_tuple, &infomask_old_tuple,
							  &infomask2_old_tuple);

	/*
	 * And also prepare an Xmax value for the new copy of the tuple.  If there
	 * was no xmax previously, or there was one but all lockers are now gone,
	 * then use InvalidTransactionId; otherwise, get the xmax from the old
	 * tuple.  (In rare cases that might also be InvalidTransactionId and yet
	 * not have the HEAP_XMAX_INVALID bit set; that's fine.)
	 */
	if ((oldtup.t_data->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_LOCKED_UPGRADED(oldtup.t_data->t_infomask) ||
		(checked_lockers && !locker_remains))
		xmax_new_tuple = InvalidTransactionId;
	else
		xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup.t_data);

	if (!TransactionIdIsValid(xmax_new_tuple))
	{
		infomask_new_tuple = HEAP_XMAX_INVALID;
		infomask2_new_tuple = 0;
	}
	else
	{
		/*
		 * If we found a valid Xmax for the new tuple, then the infomask bits
		 * to use on the new tuple depend on what was there on the old one.
		 * Note that since we're doing an update, the only possibility is that
		 * the lockers had FOR KEY SHARE lock.
		 */
		if (oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI)
		{
			GetMultiXactIdHintBits(xmax_new_tuple, &infomask_new_tuple,
								   &infomask2_new_tuple);
		}
		else
		{
			infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
			infomask2_new_tuple = 0;
		}
	}

	/*
	 * Prepare the new tuple with the appropriate initial values of Xmin and
	 * Xmax, as well as initial infomask bits as computed above.
	 */
	newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
	newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	HeapTupleHeaderSetXmin(newtup->t_data, xid);
	HeapTupleHeaderSetCmin(newtup->t_data, cid);
	newtup->t_data->t_infomask |= HEAP_UPDATED | infomask_new_tuple;
	newtup->t_data->t_infomask2 |= infomask2_new_tuple;
	HeapTupleHeaderSetXmax(newtup->t_data, xmax_new_tuple);

	/*
	 * Replace cid with a combo CID if necessary.  Note that we already put
	 * the plain cid into the new tuple.
	 */
	HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &iscombo);

	/*
	 * If the toaster needs to be activated, OR if the new tuple will not fit
	 * on the same page as the old, then we need to release the content lock
	 * (but not the pin!) on the old tuple's buffer while we are off doing
	 * TOAST and/or table-file-extension work.  We must mark the old tuple to
	 * show that it's locked, else other processes may try to update it
	 * themselves.
	 *
	 * We need to invoke the toaster if there are already any out-of-line
	 * toasted values present, or if the new tuple is over-threshold.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(&oldtup));
		Assert(!HeapTupleHasExternal(newtup));
		need_toast = false;
	}
	else
		need_toast = (HeapTupleHasExternal(&oldtup) ||
					  HeapTupleHasExternal(newtup) ||
					  newtup->t_len > TOAST_TUPLE_THRESHOLD);

	pagefree = PageGetHeapFreeSpace(page);

	newtupsize = MAXALIGN(newtup->t_len);

	if (need_toast || newtupsize > pagefree)
	{
		TransactionId xmax_lock_old_tuple;
		uint16		infomask_lock_old_tuple,
					infomask2_lock_old_tuple;
		bool		cleared_all_frozen = false;

		/*
		 * To prevent concurrent sessions from updating the tuple, we have to
		 * temporarily mark it locked, while we release the page-level lock.
		 *
		 * To satisfy the rule that any xid potentially appearing in a buffer
		 * written out to disk, we unfortunately have to WAL log this
		 * temporary modification.  We can reuse xl_heap_lock for this
		 * purpose.  If we crash/error before following through with the
		 * actual update, xmax will be of an aborted transaction, allowing
		 * other sessions to proceed.
		 */

		/*
		 * Compute xmax / infomask appropriate for locking the tuple. This has
		 * to be done separately from the combo that's going to be used for
		 * updating, because the potentially created multixact would otherwise
		 * be wrong.
		 */
		compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(oldtup.t_data),
								  oldtup.t_data->t_infomask,
								  oldtup.t_data->t_infomask2,
								  xid, *lockmode, false,
								  &xmax_lock_old_tuple, &infomask_lock_old_tuple,
								  &infomask2_lock_old_tuple);

		Assert(HEAP_XMAX_IS_LOCKED_ONLY(infomask_lock_old_tuple));

		START_CRIT_SECTION();

		/* Clear obsolete visibility flags ... */
		oldtup.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		oldtup.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleClearHotUpdated(&oldtup);
		/* ... and store info about transaction updating this tuple */
		Assert(TransactionIdIsValid(xmax_lock_old_tuple));
		HeapTupleHeaderSetXmax(oldtup.t_data, xmax_lock_old_tuple);
		oldtup.t_data->t_infomask |= infomask_lock_old_tuple;
		oldtup.t_data->t_infomask2 |= infomask2_lock_old_tuple;
		HeapTupleHeaderSetCmax(oldtup.t_data, cid, iscombo);

		/* temporarily make it look not-updated, but locked */
		oldtup.t_data->t_ctid = oldtup.t_self;

		/*
		 * Clear all-frozen bit on visibility map if needed. We could
		 * immediately reset ALL_VISIBLE, but given that the WAL logging
		 * overhead would be unchanged, that doesn't seem necessarily
		 * worthwhile.
		 */
		if (PageIsAllVisible(page) &&
			visibilitymap_clear(relation, block, vmbuffer,
								VISIBILITYMAP_ALL_FROZEN))
			cleared_all_frozen = true;

		MarkBufferDirty(buffer);

		if (RelationNeedsWAL(relation))
		{
			xl_heap_lock xlrec;
			XLogRecPtr	recptr;

			XLogBeginInsert();
			XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

			xlrec.offnum = ItemPointerGetOffsetNumber(&oldtup.t_self);
			xlrec.locking_xid = xmax_lock_old_tuple;
			xlrec.infobits_set = compute_infobits(oldtup.t_data->t_infomask,
												  oldtup.t_data->t_infomask2);
			xlrec.flags =
				cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;
			XLogRegisterData((char *) &xlrec, SizeOfHeapLock);
			recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * Let the toaster do its thing, if needed.
		 *
		 * Note: below this point, heaptup is the data we actually intend to
		 * store into the relation; newtup is the caller's original untoasted
		 * data.
		 */
		if (need_toast)
		{
			/* Note we always use WAL and FSM during updates */
			heaptup = heap_toast_insert_or_update(relation, newtup, &oldtup, 0);
			newtupsize = MAXALIGN(heaptup->t_len);
		}
		else
			heaptup = newtup;

		/*
		 * Now, do we need a new page for the tuple, or not?  This is a bit
		 * tricky since someone else could have added tuples to the page while
		 * we weren't looking.  We have to recheck the available space after
		 * reacquiring the buffer lock.  But don't bother to do that if the
		 * former amount of free space is still not enough; it's unlikely
		 * there's more free now than before.
		 *
		 * What's more, if we need to get a new page, we will need to acquire
		 * buffer locks on both old and new pages.  To avoid deadlock against
		 * some other backend trying to get the same two locks in the other
		 * order, we must be consistent about the order we get the locks in.
		 * We use the rule "lock the lower-numbered page of the relation
		 * first".  To implement this, we must do RelationGetBufferForTuple
		 * while not holding the lock on the old page, and we must rely on it
		 * to get the locks on both pages in the correct order.
		 *
		 * Another consideration is that we need visibility map page pin(s) if
		 * we will have to clear the all-visible flag on either page.  If we
		 * call RelationGetBufferForTuple, we rely on it to acquire any such
		 * pins; but if we don't, we have to handle that here.  Hence we need
		 * a loop.
		 */
		for (;;)
		{
			if (newtupsize > pagefree)
			{
				/* It doesn't fit, must use RelationGetBufferForTuple. */
				newbuf = RelationGetBufferForTuple(relation, heaptup->t_len,
												   buffer, 0, NULL,
												   &vmbuffer_new, &vmbuffer);
				/* We're all done. */
				break;
			}
			/* Acquire VM page pin if needed and we don't have it. */
			if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
				visibilitymap_pin(relation, block, &vmbuffer);
			/* Re-acquire the lock on the old tuple's page. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			/* Re-check using the up-to-date free space */
			pagefree = PageGetHeapFreeSpace(page);
			if (newtupsize > pagefree ||
				(vmbuffer == InvalidBuffer && PageIsAllVisible(page)))
			{
				/*
				 * Rats, it doesn't fit anymore, or somebody just now set the
				 * all-visible flag.  We must now unlock and loop to avoid
				 * deadlock.  Fortunately, this path should seldom be taken.
				 */
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			}
			else
			{
				/* We're all done. */
				newbuf = buffer;
				break;
			}
		}
	}
	else
	{
		/* No TOAST work needed, and it'll fit on same page */
		newbuf = buffer;
		heaptup = newtup;
	}

	/*
	 * We're about to do the actual update -- check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the pages between this check and the update
	 * being visible to the scan (i.e., exclusive buffer content lock(s) are
	 * continuously held from this point until the tuple update is visible).
	 *
	 * For the new tuple the only check needed is at the relation level, but
	 * since both tuples are in the same relation and the check for oldtup
	 * will include checking the relation level, there is no benefit to a
	 * separate check for the new tuple.
	 */
	CheckForSerializableConflictIn(relation, &oldtup.t_self,
								   BufferGetBlockNumber(buffer));

	/*
	 * At this point newbuf and buffer are both pinned and locked, and newbuf
	 * has enough space for the new tuple.  If they are the same buffer, only
	 * one pin is held.
	 */

	if (newbuf == buffer)
	{
		/*
		 * Since the new tuple is going into the same page, we might be able
		 * to do a HOT update.  Check if any of the index columns have been
		 * changed.
		 */
		if (!bms_overlap(modified_attrs, hot_attrs))
			use_hot_update = true;
	}
	else
	{
		/* Set a hint that the old page could use prune/defrag */
		PageSetFull(page);
	}

	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 * ExtractReplicaIdentity() will return NULL if nothing needs to be
	 * logged.  Pass old key required as true only if the replica identity key
	 * columns are modified or it has external data.
	 */
	old_key_tuple = ExtractReplicaIdentity(relation, &oldtup,
										   bms_overlap(modified_attrs, id_attrs) ||
										   id_has_external,
										   &old_key_copied);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/*
	 * If this transaction commits, the old tuple will become DEAD sooner or
	 * later.  Set flag that this page is a candidate for pruning once our xid
	 * falls below the OldestXmin horizon.  If the transaction finally aborts,
	 * the subsequent page pruning will be a no-op and the hint will be
	 * cleared.
	 *
	 * XXX Should we set hint on newbuf as well?  If the transaction aborts,
	 * there would be a prunable tuple in the newbuf; but for now we choose
	 * not to optimize for aborts.  Note that heap_xlog_update must be kept in
	 * sync if this decision changes.
	 */
	PageSetPrunable(page, xid);

	if (use_hot_update)
	{
		/* Mark the old tuple as HOT-updated */
		HeapTupleSetHotUpdated(&oldtup);
		/* And mark the new tuple as heap-only */
		HeapTupleSetHeapOnly(heaptup);
		/* Mark the caller's copy too, in case different from heaptup */
		HeapTupleSetHeapOnly(newtup);
	}
	else
	{
		/* Make sure tuples are correctly marked as not-HOT */
		HeapTupleClearHotUpdated(&oldtup);
		HeapTupleClearHeapOnly(heaptup);
		HeapTupleClearHeapOnly(newtup);
	}

	RelationPutHeapTuple(relation, newbuf, heaptup, false); /* insert new tuple */


	/* Clear obsolete visibility flags, possibly set by ourselves above... */
	oldtup.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	oldtup.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	/* ... and store info about transaction updating this tuple */
	Assert(TransactionIdIsValid(xmax_old_tuple));
	HeapTupleHeaderSetXmax(oldtup.t_data, xmax_old_tuple);
	oldtup.t_data->t_infomask |= infomask_old_tuple;
	oldtup.t_data->t_infomask2 |= infomask2_old_tuple;
	HeapTupleHeaderSetCmax(oldtup.t_data, cid, iscombo);

	/* record address of new tuple in t_ctid of old one */
	oldtup.t_data->t_ctid = heaptup->t_self;

	/* clear PD_ALL_VISIBLE flags, reset all visibilitymap bits */
	if (PageIsAllVisible(BufferGetPage(buffer)))
	{
		all_visible_cleared = true;
		PageClearAllVisible(BufferGetPage(buffer));
		visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}
	if (newbuf != buffer && PageIsAllVisible(BufferGetPage(newbuf)))
	{
		all_visible_cleared_new = true;
		PageClearAllVisible(BufferGetPage(newbuf));
		visibilitymap_clear(relation, BufferGetBlockNumber(newbuf),
							vmbuffer_new, VISIBILITYMAP_VALID_BITS);
	}

	if (newbuf != buffer)
		MarkBufferDirty(newbuf);
	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;

		/*
		 * For logical decoding we need combo CIDs to properly decode the
		 * catalog.
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
		{
			log_heap_new_cid(relation, &oldtup);
			log_heap_new_cid(relation, heaptup);
		}

		recptr = log_heap_update(relation, buffer,
								 newbuf, &oldtup, heaptup,
								 old_key_tuple,
								 all_visible_cleared,
								 all_visible_cleared_new);
		if (newbuf != buffer)
		{
			PageSetLSN(BufferGetPage(newbuf), recptr);
		}
		PageSetLSN(BufferGetPage(buffer), recptr);
	}

	END_CRIT_SECTION();

	if (newbuf != buffer)
		LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	/*
	 * Mark old tuple for invalidation from system caches at next command
	 * boundary, and mark the new tuple for invalidation in case we abort. We
	 * have to do this before releasing the buffer because oldtup is in the
	 * buffer.  (heaptup is all in local memory, but it's necessary to process
	 * both tuple versions in one call to inval.c so we can avoid redundant
	 * sinval messages.)
	 */
	CacheInvalidateHeapTuple(relation, &oldtup, heaptup);

	/* Now we can release the buffer(s) */
	if (newbuf != buffer)
		ReleaseBuffer(newbuf);
	ReleaseBuffer(buffer);
	if (BufferIsValid(vmbuffer_new))
		ReleaseBuffer(vmbuffer_new);
	if (BufferIsValid(vmbuffer))
		ReleaseBuffer(vmbuffer);

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, &(oldtup.t_self), *lockmode);

	pgstat_count_heap_update(relation, use_hot_update);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != newtup)
	{
		newtup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	bms_free(hot_attrs);
	bms_free(key_attrs);
	bms_free(id_attrs);
	bms_free(modified_attrs);
	bms_free(interesting_attrs);

	return TM_Ok;
}

/*
 * Check if the specified attribute's values are the same.  Subroutine for
 * HeapDetermineColumnsInfo.
 */
static bool
heap_attr_equals(TupleDesc tupdesc, int attrnum, Datum value1, Datum value2,
				 bool isnull1, bool isnull2)
{
	Form_pg_attribute att;

	/*
	 * If one value is NULL and other is not, then they are certainly not
	 * equal
	 */
	if (isnull1 != isnull2)
		return false;

	/*
	 * If both are NULL, they can be considered equal.
	 */
	if (isnull1)
		return true;

	/*
	 * We do simple binary comparison of the two datums.  This may be overly
	 * strict because there can be multiple binary representations for the
	 * same logical value.  But we should be OK as long as there are no false
	 * positives.  Using a type-specific equality operator is messy because
	 * there could be multiple notions of equality in different operator
	 * classes; furthermore, we cannot safely invoke user-defined functions
	 * while holding exclusive buffer lock.
	 */
	if (attrnum <= 0)
	{
		/* The only allowed system columns are OIDs, so do this */
		return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
	}
	else
	{
		Assert(attrnum <= tupdesc->natts);
		att = TupleDescAttr(tupdesc, attrnum - 1);
		return datumIsEqual(value1, value2, att->attbyval, att->attlen);
	}
}

/*
 * Check which columns are being updated.
 *
 * Given an updated tuple, determine (and return into the output bitmapset),
 * from those listed as interesting, the set of columns that changed.
 *
 * has_external indicates if any of the unmodified attributes (from those
 * listed as interesting) of the old tuple is a member of external_cols and is
 * stored externally.
 *
 * The input interesting_cols bitmapset is destructively modified; that is OK
 * since this is invoked at most once in heap_update.
 */
static Bitmapset *
HeapDetermineColumnsInfo(Relation relation,
						 Bitmapset *interesting_cols,
						 Bitmapset *external_cols,
						 HeapTuple oldtup, HeapTuple newtup,
						 bool *has_external)
{
	int			attrnum;
	Bitmapset  *modified = NULL;
	TupleDesc	tupdesc = RelationGetDescr(relation);

	while ((attrnum = bms_first_member(interesting_cols)) >= 0)
	{
		Datum		value1,
					value2;
		bool		isnull1,
					isnull2;

		attrnum += FirstLowInvalidHeapAttributeNumber;

		/*
		 * If it's a whole-tuple reference, say "not equal".  It's not really
		 * worth supporting this case, since it could only succeed after a
		 * no-op update, which is hardly a case worth optimizing for.
		 */
		if (attrnum == 0)
		{
			modified = bms_add_member(modified,
									  attrnum -
									  FirstLowInvalidHeapAttributeNumber);
			continue;
		}

		/*
		 * Likewise, automatically say "not equal" for any system attribute
		 * other than tableOID; we cannot expect these to be consistent in a
		 * HOT chain, or even to be set correctly yet in the new tuple.
		 */
		if (attrnum < 0)
		{
			if (attrnum != TableOidAttributeNumber)
			{
				modified = bms_add_member(modified,
										  attrnum -
										  FirstLowInvalidHeapAttributeNumber);
				continue;
			}
		}

		/*
		 * Extract the corresponding values.  XXX this is pretty inefficient
		 * if there are many indexed columns.  Should we do a single
		 * heap_deform_tuple call on each tuple, instead?	But that doesn't
		 * work for system columns ...
		 */
		value1 = heap_getattr(oldtup, attrnum, tupdesc, &isnull1);
		value2 = heap_getattr(newtup, attrnum, tupdesc, &isnull2);

		if (!heap_attr_equals(tupdesc, attrnum, value1,
							  value2, isnull1, isnull2))
		{
			modified = bms_add_member(modified,
									  attrnum -
									  FirstLowInvalidHeapAttributeNumber);
			continue;
		}

		/*
		 * No need to check attributes that can't be stored externally. Note
		 * that system attributes can't be stored externally.
		 */
		if (attrnum < 0 || isnull1 ||
			TupleDescAttr(tupdesc, attrnum - 1)->attlen != -1)
			continue;

		/*
		 * Check if the old tuple's attribute is stored externally and is a
		 * member of external_cols.
		 */
		if (VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(value1)) &&
			bms_is_member(attrnum - FirstLowInvalidHeapAttributeNumber,
						  external_cols))
			*has_external = true;
	}

	return modified;
}

/*
 *	simple_heap_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
void
simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{
	TM_Result	result;
	TM_FailureData tmfd;
	LockTupleMode lockmode;

	result = heap_update(relation, otid, tup,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &tmfd, &lockmode);
	switch (result)
	{
		case TM_SelfModified:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case TM_Ok:
			/* done successfully */
			break;

		case TM_Updated:
			elog(ERROR, "tuple concurrently updated");
			break;

		case TM_Deleted:
			elog(ERROR, "tuple concurrently deleted");
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}
}


/*
 * Return the MultiXactStatus corresponding to the given tuple lock mode.
 */
static MultiXactStatus
get_mxact_status_for_lock(LockTupleMode mode, bool is_update)
{
	int			retval;

	if (is_update)
		retval = tupleLockExtraInfo[mode].updstatus;
	else
		retval = tupleLockExtraInfo[mode].lockstatus;

	if (retval == -1)
		elog(ERROR, "invalid lock tuple mode %d/%s", mode,
			 is_update ? "true" : "false");

	return (MultiXactStatus) retval;
}

/*
 *	heap_lock_tuple - lock a tuple in shared or exclusive mode
 *
 * Note that this acquires a buffer pin, which the caller must release.
 *
 * Input parameters:
 *	relation: relation containing tuple (caller must hold suitable lock)
 *	tid: TID of tuple to lock
 *	cid: current command ID (used for visibility test, and stored into
 *		tuple's cmax if lock is successful)
 *	mode: indicates if shared or exclusive tuple lock is desired
 *	wait_policy: what to do if tuple lock is not available
 *	follow_updates: if true, follow the update chain to also lock descendant
 *		tuples.
 *
 * Output parameters:
 *	*tuple: all fields filled in
 *	*buffer: set to buffer holding tuple (pinned but not locked at exit)
 *	*tmfd: filled in failure cases (see below)
 *
 * Function results are the same as the ones for table_tuple_lock().
 *
 * In the failure cases other than TM_Invisible, the routine fills
 * *tmfd with the tuple's t_ctid, t_xmax (resolving a possible MultiXact,
 * if necessary), and t_cmax (the last only for TM_SelfModified,
 * since we cannot obtain cmax from a combo CID generated by another
 * transaction).
 * See comments for struct TM_FailureData for additional info.
 *
 * See README.tuplock for a thorough explanation of this mechanism.
 */
TM_Result
heap_lock_tuple(Relation relation, HeapTuple tuple,
				CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				bool follow_updates,
				Buffer *buffer, TM_FailureData *tmfd)
{
	TM_Result	result;
	ItemPointer tid = &(tuple->t_self);
	ItemId		lp;
	Page		page;
	Buffer		vmbuffer = InvalidBuffer;
	BlockNumber block;
	TransactionId xid,
				xmax;
	uint16		old_infomask,
				new_infomask,
				new_infomask2;
	bool		first_time = true;
	bool		skip_tuple_lock = false;
	bool		have_tuple_lock = false;
	bool		cleared_all_frozen = false;

	*buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	block = ItemPointerGetBlockNumber(tid);

	/*
	 * Before locking the buffer, pin the visibility map page if it appears to
	 * be necessary.  Since we haven't got the lock yet, someone else might be
	 * in the middle of changing this, so we'll need to recheck after we have
	 * the lock.
	 */
	if (PageIsAllVisible(BufferGetPage(*buffer)))
		visibilitymap_pin(relation, block, &vmbuffer);

	LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(*buffer);
	lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	Assert(ItemIdIsNormal(lp));

	tuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tuple->t_len = ItemIdGetLength(lp);
	tuple->t_tableOid = RelationGetRelid(relation);

l3:
	result = HeapTupleSatisfiesUpdate(tuple, cid, *buffer);

	if (result == TM_Invisible)
	{
		/*
		 * This is possible, but only when locking a tuple for ON CONFLICT
		 * UPDATE.  We return this value here rather than throwing an error in
		 * order to give that case the opportunity to throw a more specific
		 * error.
		 */
		result = TM_Invisible;
		goto out_locked;
	}
	else if (result == TM_BeingModified ||
			 result == TM_Updated ||
			 result == TM_Deleted)
	{
		TransactionId xwait;
		uint16		infomask;
		uint16		infomask2;
		bool		require_sleep;
		ItemPointerData t_ctid;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tuple->t_data);
		infomask = tuple->t_data->t_infomask;
		infomask2 = tuple->t_data->t_infomask2;
		ItemPointerCopy(&tuple->t_data->t_ctid, &t_ctid);

		LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * If any subtransaction of the current top transaction already holds
		 * a lock as strong as or stronger than what we're requesting, we
		 * effectively hold the desired lock already.  We *must* succeed
		 * without trying to take the tuple lock, else we will deadlock
		 * against anyone wanting to acquire a stronger lock.
		 *
		 * Note we only do this the first time we loop on the HTSU result;
		 * there is no point in testing in subsequent passes, because
		 * evidently our own transaction cannot have acquired a new lock after
		 * the first time we checked.
		 */
		if (first_time)
		{
			first_time = false;

			if (infomask & HEAP_XMAX_IS_MULTI)
			{
				int			i;
				int			nmembers;
				MultiXactMember *members;

				/*
				 * We don't need to allow old multixacts here; if that had
				 * been the case, HeapTupleSatisfiesUpdate would have returned
				 * MayBeUpdated and we wouldn't be here.
				 */
				nmembers =
					GetMultiXactIdMembers(xwait, &members, false,
										  HEAP_XMAX_IS_LOCKED_ONLY(infomask));

				for (i = 0; i < nmembers; i++)
				{
					/* only consider members of our own transaction */
					if (!TransactionIdIsCurrentTransactionId(members[i].xid))
						continue;

					if (TUPLOCK_from_mxstatus(members[i].status) >= mode)
					{
						pfree(members);
						result = TM_Ok;
						goto out_unlocked;
					}
					else
					{
						/*
						 * Disable acquisition of the heavyweight tuple lock.
						 * Otherwise, when promoting a weaker lock, we might
						 * deadlock with another locker that has acquired the
						 * heavyweight tuple lock and is waiting for our
						 * transaction to finish.
						 *
						 * Note that in this case we still need to wait for
						 * the multixact if required, to avoid acquiring
						 * conflicting locks.
						 */
						skip_tuple_lock = true;
					}
				}

				if (members)
					pfree(members);
			}
			else if (TransactionIdIsCurrentTransactionId(xwait))
			{
				switch (mode)
				{
					case LockTupleKeyShare:
						Assert(HEAP_XMAX_IS_KEYSHR_LOCKED(infomask) ||
							   HEAP_XMAX_IS_SHR_LOCKED(infomask) ||
							   HEAP_XMAX_IS_EXCL_LOCKED(infomask));
						result = TM_Ok;
						goto out_unlocked;
					case LockTupleShare:
						if (HEAP_XMAX_IS_SHR_LOCKED(infomask) ||
							HEAP_XMAX_IS_EXCL_LOCKED(infomask))
						{
							result = TM_Ok;
							goto out_unlocked;
						}
						break;
					case LockTupleNoKeyExclusive:
						if (HEAP_XMAX_IS_EXCL_LOCKED(infomask))
						{
							result = TM_Ok;
							goto out_unlocked;
						}
						break;
					case LockTupleExclusive:
						if (HEAP_XMAX_IS_EXCL_LOCKED(infomask) &&
							infomask2 & HEAP_KEYS_UPDATED)
						{
							result = TM_Ok;
							goto out_unlocked;
						}
						break;
				}
			}
		}

		/*
		 * Initially assume that we will have to wait for the locking
		 * transaction(s) to finish.  We check various cases below in which
		 * this can be turned off.
		 */
		require_sleep = true;
		if (mode == LockTupleKeyShare)
		{
			/*
			 * If we're requesting KeyShare, and there's no update present, we
			 * don't need to wait.  Even if there is an update, we can still
			 * continue if the key hasn't been modified.
			 *
			 * However, if there are updates, we need to walk the update chain
			 * to mark future versions of the row as locked, too.  That way,
			 * if somebody deletes that future version, we're protected
			 * against the key going away.  This locking of future versions
			 * could block momentarily, if a concurrent transaction is
			 * deleting a key; or it could return a value to the effect that
			 * the transaction deleting the key has already committed.  So we
			 * do this before re-locking the buffer; otherwise this would be
			 * prone to deadlocks.
			 *
			 * Note that the TID we're locking was grabbed before we unlocked
			 * the buffer.  For it to change while we're not looking, the
			 * other properties we're testing for below after re-locking the
			 * buffer would also change, in which case we would restart this
			 * loop above.
			 */
			if (!(infomask2 & HEAP_KEYS_UPDATED))
			{
				bool		updated;

				updated = !HEAP_XMAX_IS_LOCKED_ONLY(infomask);

				/*
				 * If there are updates, follow the update chain; bail out if
				 * that cannot be done.
				 */
				if (follow_updates && updated)
				{
					TM_Result	res;

					res = heap_lock_updated_tuple(relation, tuple, &t_ctid,
												  GetCurrentTransactionId(),
												  mode);
					if (res != TM_Ok)
					{
						result = res;
						/* recovery code expects to have buffer lock held */
						LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
						goto failed;
					}
				}

				LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * Make sure it's still an appropriate lock, else start over.
				 * Also, if it wasn't updated before we released the lock, but
				 * is updated now, we start over too; the reason is that we
				 * now need to follow the update chain to lock the new
				 * versions.
				 */
				if (!HeapTupleHeaderIsOnlyLocked(tuple->t_data) &&
					((tuple->t_data->t_infomask2 & HEAP_KEYS_UPDATED) ||
					 !updated))
					goto l3;

				/* Things look okay, so we can skip sleeping */
				require_sleep = false;

				/*
				 * Note we allow Xmax to change here; other updaters/lockers
				 * could have modified it before we grabbed the buffer lock.
				 * However, this is not a problem, because with the recheck we
				 * just did we ensure that they still don't conflict with the
				 * lock we want.
				 */
			}
		}
		else if (mode == LockTupleShare)
		{
			/*
			 * If we're requesting Share, we can similarly avoid sleeping if
			 * there's no update and no exclusive lock present.
			 */
			if (HEAP_XMAX_IS_LOCKED_ONLY(infomask) &&
				!HEAP_XMAX_IS_EXCL_LOCKED(infomask))
			{
				LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * Make sure it's still an appropriate lock, else start over.
				 * See above about allowing xmax to change.
				 */
				if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_data->t_infomask) ||
					HEAP_XMAX_IS_EXCL_LOCKED(tuple->t_data->t_infomask))
					goto l3;
				require_sleep = false;
			}
		}
		else if (mode == LockTupleNoKeyExclusive)
		{
			/*
			 * If we're requesting NoKeyExclusive, we might also be able to
			 * avoid sleeping; just ensure that there no conflicting lock
			 * already acquired.
			 */
			if (infomask & HEAP_XMAX_IS_MULTI)
			{
				if (!DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
											 mode, NULL))
				{
					/*
					 * No conflict, but if the xmax changed under us in the
					 * meantime, start over.
					 */
					LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
					if (xmax_infomask_changed(tuple->t_data->t_infomask, infomask) ||
						!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data),
											 xwait))
						goto l3;

					/* otherwise, we're good */
					require_sleep = false;
				}
			}
			else if (HEAP_XMAX_IS_KEYSHR_LOCKED(infomask))
			{
				LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

				/* if the xmax changed in the meantime, start over */
				if (xmax_infomask_changed(tuple->t_data->t_infomask, infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data),
										 xwait))
					goto l3;
				/* otherwise, we're good */
				require_sleep = false;
			}
		}

		/*
		 * As a check independent from those above, we can also avoid sleeping
		 * if the current transaction is the sole locker of the tuple.  Note
		 * that the strength of the lock already held is irrelevant; this is
		 * not about recording the lock in Xmax (which will be done regardless
		 * of this optimization, below).  Also, note that the cases where we
		 * hold a lock stronger than we are requesting are already handled
		 * above by not doing anything.
		 *
		 * Note we only deal with the non-multixact case here; MultiXactIdWait
		 * is well equipped to deal with this situation on its own.
		 */
		if (require_sleep && !(infomask & HEAP_XMAX_IS_MULTI) &&
			TransactionIdIsCurrentTransactionId(xwait))
		{
			/* ... but if the xmax changed in the meantime, start over */
			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
			if (xmax_infomask_changed(tuple->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data),
									 xwait))
				goto l3;
			Assert(HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_data->t_infomask));
			require_sleep = false;
		}

		/*
		 * Time to sleep on the other transaction/multixact, if necessary.
		 *
		 * If the other transaction is an update/delete that's already
		 * committed, then sleeping cannot possibly do any good: if we're
		 * required to sleep, get out to raise an error instead.
		 *
		 * By here, we either have already acquired the buffer exclusive lock,
		 * or we must wait for the locking transaction or multixact; so below
		 * we ensure that we grab buffer lock after the sleep.
		 */
		if (require_sleep && (result == TM_Updated || result == TM_Deleted))
		{
			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
			goto failed;
		}
		else if (require_sleep)
		{
			/*
			 * Acquire tuple lock to establish our priority for the tuple, or
			 * die trying.  LockTuple will release us when we are next-in-line
			 * for the tuple.  We must do this even if we are share-locking,
			 * but not if we already have a weaker lock on the tuple.
			 *
			 * If we are forced to "start over" below, we keep the tuple lock;
			 * this arranges that we stay at the head of the line while
			 * rechecking tuple state.
			 */
			if (!skip_tuple_lock &&
				!heap_acquire_tuplock(relation, tid, mode, wait_policy,
									  &have_tuple_lock))
			{
				/*
				 * This can only happen if wait_policy is Skip and the lock
				 * couldn't be obtained.
				 */
				result = TM_WouldBlock;
				/* recovery code expects to have buffer lock held */
				LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
				goto failed;
			}

			if (infomask & HEAP_XMAX_IS_MULTI)
			{
				MultiXactStatus status = get_mxact_status_for_lock(mode, false);

				/* We only ever lock tuples, never update them */
				if (status >= MultiXactStatusNoKeyUpdate)
					elog(ERROR, "invalid lock mode in heap_lock_tuple");

				/* wait for multixact to end, or die trying  */
				switch (wait_policy)
				{
					case LockWaitBlock:
						MultiXactIdWait((MultiXactId) xwait, status, infomask,
										relation, &tuple->t_self, XLTW_Lock, NULL);
						break;
					case LockWaitSkip:
						if (!ConditionalMultiXactIdWait((MultiXactId) xwait,
														status, infomask, relation,
														NULL))
						{
							result = TM_WouldBlock;
							/* recovery code expects to have buffer lock held */
							LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
							goto failed;
						}
						break;
					case LockWaitError:
						if (!ConditionalMultiXactIdWait((MultiXactId) xwait,
														status, infomask, relation,
														NULL))
							ereport(ERROR,
									(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
									 errmsg("could not obtain lock on row in relation \"%s\"",
											RelationGetRelationName(relation))));

						break;
				}

				/*
				 * Of course, the multixact might not be done here: if we're
				 * requesting a light lock mode, other transactions with light
				 * locks could still be alive, as well as locks owned by our
				 * own xact or other subxacts of this backend.  We need to
				 * preserve the surviving MultiXact members.  Note that it
				 * isn't absolutely necessary in the latter case, but doing so
				 * is simpler.
				 */
			}
			else
			{
				/* wait for regular transaction to end, or die trying */
				switch (wait_policy)
				{
					case LockWaitBlock:
						XactLockTableWait(xwait, relation, &tuple->t_self,
										  XLTW_Lock);
						break;
					case LockWaitSkip:
						if (!ConditionalXactLockTableWait(xwait))
						{
							result = TM_WouldBlock;
							/* recovery code expects to have buffer lock held */
							LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
							goto failed;
						}
						break;
					case LockWaitError:
						if (!ConditionalXactLockTableWait(xwait))
							ereport(ERROR,
									(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
									 errmsg("could not obtain lock on row in relation \"%s\"",
											RelationGetRelationName(relation))));
						break;
				}
			}

			/* if there are updates, follow the update chain */
			if (follow_updates && !HEAP_XMAX_IS_LOCKED_ONLY(infomask))
			{
				TM_Result	res;

				res = heap_lock_updated_tuple(relation, tuple, &t_ctid,
											  GetCurrentTransactionId(),
											  mode);
				if (res != TM_Ok)
				{
					result = res;
					/* recovery code expects to have buffer lock held */
					LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
					goto failed;
				}
			}

			LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(tuple->t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple->t_data),
									 xwait))
				goto l3;

			if (!(infomask & HEAP_XMAX_IS_MULTI))
			{
				/*
				 * Otherwise check if it committed or aborted.  Note we cannot
				 * be here if the tuple was only locked by somebody who didn't
				 * conflict with us; that would have been handled above.  So
				 * that transaction must necessarily be gone by now.  But
				 * don't check for this in the multixact case, because some
				 * locker transactions might still be running.
				 */
				UpdateXmaxHintBits(tuple->t_data, *buffer, xwait);
			}
		}

		/* By here, we're certain that we hold buffer exclusive lock again */

		/*
		 * We may lock if previous xmax aborted, or if it committed but only
		 * locked the tuple without updating it; or if we didn't have to wait
		 * at all for whatever reason.
		 */
		if (!require_sleep ||
			(tuple->t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tuple->t_data))
			result = TM_Ok;
		else if (!ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

failed:
	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified || result == TM_Updated ||
			   result == TM_Deleted || result == TM_WouldBlock);

		/*
		 * When locking a tuple under LockWaitSkip semantics and we fail with
		 * TM_WouldBlock above, it's possible for concurrent transactions to
		 * release the lock and set HEAP_XMAX_INVALID in the meantime.  So
		 * this assert is slightly different from the equivalent one in
		 * heap_delete and heap_update.
		 */
		Assert((result == TM_WouldBlock) ||
			   !(tuple->t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid));
		tmfd->ctid = tuple->t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(tuple->t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(tuple->t_data);
		else
			tmfd->cmax = InvalidCommandId;
		goto out_locked;
	}

	/*
	 * If we didn't pin the visibility map page and the page has become all
	 * visible while we were busy locking the buffer, or during some
	 * subsequent window during which we had it unlocked, we'll have to unlock
	 * and re-lock, to avoid holding the buffer lock across I/O.  That's a bit
	 * unfortunate, especially since we'll now have to recheck whether the
	 * tuple has been locked or updated under us, but hopefully it won't
	 * happen very often.
	 */
	if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
	{
		LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
		visibilitymap_pin(relation, block, &vmbuffer);
		LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
		goto l3;
	}

	xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
	old_infomask = tuple->t_data->t_infomask;

	/*
	 * If this is the first possibly-multixact-able operation in the current
	 * transaction, set my per-backend OldestMemberMXactId setting. We can be
	 * certain that the transaction will never become a member of any older
	 * MultiXactIds than that.  (We have to do this even if we end up just
	 * using our own TransactionId below, since some other backend could
	 * incorporate our XID into a MultiXact immediately afterwards.)
	 */
	MultiXactIdSetOldestMember();

	/*
	 * Compute the new xmax and infomask to store into the tuple.  Note we do
	 * not modify the tuple just yet, because that would leave it in the wrong
	 * state if multixact.c elogs.
	 */
	compute_new_xmax_infomask(xmax, old_infomask, tuple->t_data->t_infomask2,
							  GetCurrentTransactionId(), mode, false,
							  &xid, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();

	/*
	 * Store transaction information of xact locking the tuple.
	 *
	 * Note: Cmax is meaningless in this context, so don't set it; this avoids
	 * possibly generating a useless combo CID.  Moreover, if we're locking a
	 * previously updated tuple, it's important to preserve the Cmax.
	 *
	 * Also reset the HOT UPDATE bit, but only if there's no update; otherwise
	 * we would break the HOT chain.
	 */
	tuple->t_data->t_infomask &= ~HEAP_XMAX_BITS;
	tuple->t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tuple->t_data->t_infomask |= new_infomask;
	tuple->t_data->t_infomask2 |= new_infomask2;
	if (HEAP_XMAX_IS_LOCKED_ONLY(new_infomask))
		HeapTupleHeaderClearHotUpdated(tuple->t_data);
	HeapTupleHeaderSetXmax(tuple->t_data, xid);

	/*
	 * Make sure there is no forward chain link in t_ctid.  Note that in the
	 * cases where the tuple has been updated, we must not overwrite t_ctid,
	 * because it was set by the updater.  Moreover, if the tuple has been
	 * updated, we need to follow the update chain to lock the new versions of
	 * the tuple as well.
	 */
	if (HEAP_XMAX_IS_LOCKED_ONLY(new_infomask))
		tuple->t_data->t_ctid = *tid;

	/* Clear only the all-frozen bit on visibility map if needed */
	if (PageIsAllVisible(page) &&
		visibilitymap_clear(relation, block, vmbuffer,
							VISIBILITYMAP_ALL_FROZEN))
		cleared_all_frozen = true;


	MarkBufferDirty(*buffer);

	/*
	 * XLOG stuff.  You might think that we don't need an XLOG record because
	 * there is no state change worth restoring after a crash.  You would be
	 * wrong however: we have just written either a TransactionId or a
	 * MultiXactId that may never have been seen on disk before, and we need
	 * to make sure that there are XLOG entries covering those ID numbers.
	 * Else the same IDs might be re-used after a crash, which would be
	 * disastrous if this page made it to disk before the crash.  Essentially
	 * we have to enforce the WAL log-before-data rule even in this case.
	 * (Also, in a PITR log-shipping or 2PC environment, we have to have XLOG
	 * entries for everything anyway.)
	 */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_lock xlrec;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, *buffer, REGBUF_STANDARD);

		xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);
		xlrec.locking_xid = xid;
		xlrec.infobits_set = compute_infobits(new_infomask,
											  tuple->t_data->t_infomask2);
		xlrec.flags = cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;
		XLogRegisterData((char *) &xlrec, SizeOfHeapLock);

		/* we don't decode row locks atm, so no need to log the origin */

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	result = TM_Ok;

out_locked:
	LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

out_unlocked:
	if (BufferIsValid(vmbuffer))
		ReleaseBuffer(vmbuffer);

	/*
	 * Don't update the visibility map here. Locking a tuple doesn't change
	 * visibility info.
	 */

	/*
	 * Now that we have successfully marked the tuple as locked, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, tid, mode);

	return result;
}

/*
 * Acquire heavyweight lock on the given tuple, in preparation for acquiring
 * its normal, Xmax-based tuple lock.
 *
 * have_tuple_lock is an input and output parameter: on input, it indicates
 * whether the lock has previously been acquired (and this function does
 * nothing in that case).  If this function returns success, have_tuple_lock
 * has been flipped to true.
 *
 * Returns false if it was unable to obtain the lock; this can only happen if
 * wait_policy is Skip.
 */
static bool
heap_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
					 LockWaitPolicy wait_policy, bool *have_tuple_lock)
{
	if (*have_tuple_lock)
		return true;

	switch (wait_policy)
	{
		case LockWaitBlock:
			LockTupleTuplock(relation, tid, mode);
			break;

		case LockWaitSkip:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				return false;
			break;

		case LockWaitError:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(relation))));
			break;
	}
	*have_tuple_lock = true;

	return true;
}

/*
 * Given an original set of Xmax and infomask, and a transaction (identified by
 * add_to_xmax) acquiring a new lock of some mode, compute the new Xmax and
 * corresponding infomasks to use on the tuple.
 *
 * Note that this might have side effects such as creating a new MultiXactId.
 *
 * Most callers will have called HeapTupleSatisfiesUpdate before this function;
 * that will have set the HEAP_XMAX_INVALID bit if the xmax was a MultiXactId
 * but it was not running anymore. There is a race condition, which is that the
 * MultiXactId may have finished since then, but that uncommon case is handled
 * either here, or within MultiXactIdExpand.
 *
 * There is a similar race condition possible when the old xmax was a regular
 * TransactionId.  We test TransactionIdIsInProgress again just to narrow the
 * window, but it's still possible to end up creating an unnecessary
 * MultiXactId.  Fortunately this is harmless.
 */
static void
compute_new_xmax_infomask(TransactionId xmax, uint16 old_infomask,
						  uint16 old_infomask2, TransactionId add_to_xmax,
						  LockTupleMode mode, bool is_update,
						  TransactionId *result_xmax, uint16 *result_infomask,
						  uint16 *result_infomask2)
{
	TransactionId new_xmax;
	uint16		new_infomask,
				new_infomask2;

	Assert(TransactionIdIsCurrentTransactionId(add_to_xmax));

l5:
	new_infomask = 0;
	new_infomask2 = 0;
	if (old_infomask & HEAP_XMAX_INVALID)
	{
		/*
		 * No previous locker; we just insert our own TransactionId.
		 *
		 * Note that it's critical that this case be the first one checked,
		 * because there are several blocks below that come back to this one
		 * to implement certain optimizations; old_infomask might contain
		 * other dirty bits in those cases, but we don't really care.
		 */
		if (is_update)
		{
			new_xmax = add_to_xmax;
			if (mode == LockTupleExclusive)
				new_infomask2 |= HEAP_KEYS_UPDATED;
		}
		else
		{
			new_infomask |= HEAP_XMAX_LOCK_ONLY;
			switch (mode)
			{
				case LockTupleKeyShare:
					new_xmax = add_to_xmax;
					new_infomask |= HEAP_XMAX_KEYSHR_LOCK;
					break;
				case LockTupleShare:
					new_xmax = add_to_xmax;
					new_infomask |= HEAP_XMAX_SHR_LOCK;
					break;
				case LockTupleNoKeyExclusive:
					new_xmax = add_to_xmax;
					new_infomask |= HEAP_XMAX_EXCL_LOCK;
					break;
				case LockTupleExclusive:
					new_xmax = add_to_xmax;
					new_infomask |= HEAP_XMAX_EXCL_LOCK;
					new_infomask2 |= HEAP_KEYS_UPDATED;
					break;
				default:
					new_xmax = InvalidTransactionId;	/* silence compiler */
					elog(ERROR, "invalid lock mode");
			}
		}
	}
	else if (old_infomask & HEAP_XMAX_IS_MULTI)
	{
		MultiXactStatus new_status;

		/*
		 * Currently we don't allow XMAX_COMMITTED to be set for multis, so
		 * cross-check.
		 */
		Assert(!(old_infomask & HEAP_XMAX_COMMITTED));

		/*
		 * A multixact together with LOCK_ONLY set but neither lock bit set
		 * (i.e. a pg_upgraded share locked tuple) cannot possibly be running
		 * anymore.  This check is critical for databases upgraded by
		 * pg_upgrade; both MultiXactIdIsRunning and MultiXactIdExpand assume
		 * that such multis are never passed.
		 */
		if (HEAP_LOCKED_UPGRADED(old_infomask))
		{
			old_infomask &= ~HEAP_XMAX_IS_MULTI;
			old_infomask |= HEAP_XMAX_INVALID;
			goto l5;
		}

		/*
		 * If the XMAX is already a MultiXactId, then we need to expand it to
		 * include add_to_xmax; but if all the members were lockers and are
		 * all gone, we can do away with the IS_MULTI bit and just set
		 * add_to_xmax as the only locker/updater.  If all lockers are gone
		 * and we have an updater that aborted, we can also do without a
		 * multi.
		 *
		 * The cost of doing GetMultiXactIdMembers would be paid by
		 * MultiXactIdExpand if we weren't to do this, so this check is not
		 * incurring extra work anyhow.
		 */
		if (!MultiXactIdIsRunning(xmax, HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)))
		{
			if (HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) ||
				!TransactionIdDidCommit(MultiXactIdGetUpdateXid(xmax,
																old_infomask)))
			{
				/*
				 * Reset these bits and restart; otherwise fall through to
				 * create a new multi below.
				 */
				old_infomask &= ~HEAP_XMAX_IS_MULTI;
				old_infomask |= HEAP_XMAX_INVALID;
				goto l5;
			}
		}

		new_status = get_mxact_status_for_lock(mode, is_update);

		new_xmax = MultiXactIdExpand((MultiXactId) xmax, add_to_xmax,
									 new_status);
		GetMultiXactIdHintBits(new_xmax, &new_infomask, &new_infomask2);
	}
	else if (old_infomask & HEAP_XMAX_COMMITTED)
	{
		/*
		 * It's a committed update, so we need to preserve him as updater of
		 * the tuple.
		 */
		MultiXactStatus status;
		MultiXactStatus new_status;

		if (old_infomask2 & HEAP_KEYS_UPDATED)
			status = MultiXactStatusUpdate;
		else
			status = MultiXactStatusNoKeyUpdate;

		new_status = get_mxact_status_for_lock(mode, is_update);

		/*
		 * since it's not running, it's obviously impossible for the old
		 * updater to be identical to the current one, so we need not check
		 * for that case as we do in the block above.
		 */
		new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
		GetMultiXactIdHintBits(new_xmax, &new_infomask, &new_infomask2);
	}
	else if (TransactionIdIsInProgress(xmax))
	{
		/*
		 * If the XMAX is a valid, in-progress TransactionId, then we need to
		 * create a new MultiXactId that includes both the old locker or
		 * updater and our own TransactionId.
		 */
		MultiXactStatus new_status;
		MultiXactStatus old_status;
		LockTupleMode old_mode;

		if (HEAP_XMAX_IS_LOCKED_ONLY(old_infomask))
		{
			if (HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask))
				old_status = MultiXactStatusForKeyShare;
			else if (HEAP_XMAX_IS_SHR_LOCKED(old_infomask))
				old_status = MultiXactStatusForShare;
			else if (HEAP_XMAX_IS_EXCL_LOCKED(old_infomask))
			{
				if (old_infomask2 & HEAP_KEYS_UPDATED)
					old_status = MultiXactStatusForUpdate;
				else
					old_status = MultiXactStatusForNoKeyUpdate;
			}
			else
			{
				/*
				 * LOCK_ONLY can be present alone only when a page has been
				 * upgraded by pg_upgrade.  But in that case,
				 * TransactionIdIsInProgress() should have returned false.  We
				 * assume it's no longer locked in this case.
				 */
				elog(WARNING, "LOCK_ONLY found for Xid in progress %u", xmax);
				old_infomask |= HEAP_XMAX_INVALID;
				old_infomask &= ~HEAP_XMAX_LOCK_ONLY;
				goto l5;
			}
		}
		else
		{
			/* it's an update, but which kind? */
			if (old_infomask2 & HEAP_KEYS_UPDATED)
				old_status = MultiXactStatusUpdate;
			else
				old_status = MultiXactStatusNoKeyUpdate;
		}

		old_mode = TUPLOCK_from_mxstatus(old_status);

		/*
		 * If the lock to be acquired is for the same TransactionId as the
		 * existing lock, there's an optimization possible: consider only the
		 * strongest of both locks as the only one present, and restart.
		 */
		if (xmax == add_to_xmax)
		{
			/*
			 * Note that it's not possible for the original tuple to be
			 * updated: we wouldn't be here because the tuple would have been
			 * invisible and we wouldn't try to update it.  As a subtlety,
			 * this code can also run when traversing an update chain to lock
			 * future versions of a tuple.  But we wouldn't be here either,
			 * because the add_to_xmax would be different from the original
			 * updater.
			 */
			Assert(HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));

			/* acquire the strongest of both */
			if (mode < old_mode)
				mode = old_mode;
			/* mustn't touch is_update */

			old_infomask |= HEAP_XMAX_INVALID;
			goto l5;
		}

		/* otherwise, just fall back to creating a new multixact */
		new_status = get_mxact_status_for_lock(mode, is_update);
		new_xmax = MultiXactIdCreate(xmax, old_status,
									 add_to_xmax, new_status);
		GetMultiXactIdHintBits(new_xmax, &new_infomask, &new_infomask2);
	}
	else if (!HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) &&
			 TransactionIdDidCommit(xmax))
	{
		/*
		 * It's a committed update, so we gotta preserve him as updater of the
		 * tuple.
		 */
		MultiXactStatus status;
		MultiXactStatus new_status;

		if (old_infomask2 & HEAP_KEYS_UPDATED)
			status = MultiXactStatusUpdate;
		else
			status = MultiXactStatusNoKeyUpdate;

		new_status = get_mxact_status_for_lock(mode, is_update);

		/*
		 * since it's not running, it's obviously impossible for the old
		 * updater to be identical to the current one, so we need not check
		 * for that case as we do in the block above.
		 */
		new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
		GetMultiXactIdHintBits(new_xmax, &new_infomask, &new_infomask2);
	}
	else
	{
		/*
		 * Can get here iff the locking/updating transaction was running when
		 * the infomask was extracted from the tuple, but finished before
		 * TransactionIdIsInProgress got to run.  Deal with it as if there was
		 * no locker at all in the first place.
		 */
		old_infomask |= HEAP_XMAX_INVALID;
		goto l5;
	}

	*result_infomask = new_infomask;
	*result_infomask2 = new_infomask2;
	*result_xmax = new_xmax;
}

/*
 * Subroutine for heap_lock_updated_tuple_rec.
 *
 * Given a hypothetical multixact status held by the transaction identified
 * with the given xid, does the current transaction need to wait, fail, or can
 * it continue if it wanted to acquire a lock of the given mode?  "needwait"
 * is set to true if waiting is necessary; if it can continue, then TM_Ok is
 * returned.  If the lock is already held by the current transaction, return
 * TM_SelfModified.  In case of a conflict with another transaction, a
 * different HeapTupleSatisfiesUpdate return code is returned.
 *
 * The held status is said to be hypothetical because it might correspond to a
 * lock held by a single Xid, i.e. not a real MultiXactId; we express it this
 * way for simplicity of API.
 */
static TM_Result
test_lockmode_for_conflict(MultiXactStatus status, TransactionId xid,
						   LockTupleMode mode, HeapTuple tup,
						   bool *needwait)
{
	MultiXactStatus wantedstatus;

	*needwait = false;
	wantedstatus = get_mxact_status_for_lock(mode, false);

	/*
	 * Note: we *must* check TransactionIdIsInProgress before
	 * TransactionIdDidAbort/Commit; see comment at top of heapam_visibility.c
	 * for an explanation.
	 */
	if (TransactionIdIsCurrentTransactionId(xid))
	{
		/*
		 * The tuple has already been locked by our own transaction.  This is
		 * very rare but can happen if multiple transactions are trying to
		 * lock an ancient version of the same tuple.
		 */
		return TM_SelfModified;
	}
	else if (TransactionIdIsInProgress(xid))
	{
		/*
		 * If the locking transaction is running, what we do depends on
		 * whether the lock modes conflict: if they do, then we must wait for
		 * it to finish; otherwise we can fall through to lock this tuple
		 * version without waiting.
		 */
		if (DoLockModesConflict(LOCKMODE_from_mxstatus(status),
								LOCKMODE_from_mxstatus(wantedstatus)))
		{
			*needwait = true;
		}

		/*
		 * If we set needwait above, then this value doesn't matter;
		 * otherwise, this value signals to caller that it's okay to proceed.
		 */
		return TM_Ok;
	}
	else if (TransactionIdDidAbort(xid))
		return TM_Ok;
	else if (TransactionIdDidCommit(xid))
	{
		/*
		 * The other transaction committed.  If it was only a locker, then the
		 * lock is completely gone now and we can return success; but if it
		 * was an update, then what we do depends on whether the two lock
		 * modes conflict.  If they conflict, then we must report error to
		 * caller. But if they don't, we can fall through to allow the current
		 * transaction to lock the tuple.
		 *
		 * Note: the reason we worry about ISUPDATE here is because as soon as
		 * a transaction ends, all its locks are gone and meaningless, and
		 * thus we can ignore them; whereas its updates persist.  In the
		 * TransactionIdIsInProgress case, above, we don't need to check
		 * because we know the lock is still "alive" and thus a conflict needs
		 * always be checked.
		 */
		if (!ISUPDATE_from_mxstatus(status))
			return TM_Ok;

		if (DoLockModesConflict(LOCKMODE_from_mxstatus(status),
								LOCKMODE_from_mxstatus(wantedstatus)))
		{
			/* bummer */
			if (!ItemPointerEquals(&tup->t_self, &tup->t_data->t_ctid))
				return TM_Updated;
			else
				return TM_Deleted;
		}

		return TM_Ok;
	}

	/* Not in progress, not aborted, not committed -- must have crashed */
	return TM_Ok;
}


/*
 * Recursive part of heap_lock_updated_tuple
 *
 * Fetch the tuple pointed to by tid in rel, and mark it as locked by the given
 * xid with the given mode; if this tuple is updated, recurse to lock the new
 * version as well.
 */
static TM_Result
heap_lock_updated_tuple_rec(Relation rel, ItemPointer tid, TransactionId xid,
							LockTupleMode mode)
{
	TM_Result	result;
	ItemPointerData tupid;
	HeapTupleData mytup;
	Buffer		buf;
	uint16		new_infomask,
				new_infomask2,
				old_infomask,
				old_infomask2;
	TransactionId xmax,
				new_xmax;
	TransactionId priorXmax = InvalidTransactionId;
	bool		cleared_all_frozen = false;
	bool		pinned_desired_page;
	Buffer		vmbuffer = InvalidBuffer;
	BlockNumber block;

	ItemPointerCopy(tid, &tupid);

	for (;;)
	{
		new_infomask = 0;
		new_xmax = InvalidTransactionId;
		block = ItemPointerGetBlockNumber(&tupid);
		ItemPointerCopy(&tupid, &(mytup.t_self));

		if (!heap_fetch(rel, SnapshotAny, &mytup, &buf, false))
		{
			/*
			 * if we fail to find the updated version of the tuple, it's
			 * because it was vacuumed/pruned away after its creator
			 * transaction aborted.  So behave as if we got to the end of the
			 * chain, and there's no further tuple to lock: return success to
			 * caller.
			 */
			result = TM_Ok;
			goto out_unlocked;
		}

l4:
		CHECK_FOR_INTERRUPTS();

		/*
		 * Before locking the buffer, pin the visibility map page if it
		 * appears to be necessary.  Since we haven't got the lock yet,
		 * someone else might be in the middle of changing this, so we'll need
		 * to recheck after we have the lock.
		 */
		if (PageIsAllVisible(BufferGetPage(buf)))
		{
			visibilitymap_pin(rel, block, &vmbuffer);
			pinned_desired_page = true;
		}
		else
			pinned_desired_page = false;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * If we didn't pin the visibility map page and the page has become
		 * all visible while we were busy locking the buffer, we'll have to
		 * unlock and re-lock, to avoid holding the buffer lock across I/O.
		 * That's a bit unfortunate, but hopefully shouldn't happen often.
		 *
		 * Note: in some paths through this function, we will reach here
		 * holding a pin on a vm page that may or may not be the one matching
		 * this page.  If this page isn't all-visible, we won't use the vm
		 * page, but we hold onto such a pin till the end of the function.
		 */
		if (!pinned_desired_page && PageIsAllVisible(BufferGetPage(buf)))
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			visibilitymap_pin(rel, block, &vmbuffer);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		}

		/*
		 * Check the tuple XMIN against prior XMAX, if any.  If we reached the
		 * end of the chain, we're done, so return success.
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(mytup.t_data),
								 priorXmax))
		{
			result = TM_Ok;
			goto out_locked;
		}

		/*
		 * Also check Xmin: if this tuple was created by an aborted
		 * (sub)transaction, then we already locked the last live one in the
		 * chain, thus we're done, so return success.
		 */
		if (TransactionIdDidAbort(HeapTupleHeaderGetXmin(mytup.t_data)))
		{
			result = TM_Ok;
			goto out_locked;
		}

		old_infomask = mytup.t_data->t_infomask;
		old_infomask2 = mytup.t_data->t_infomask2;
		xmax = HeapTupleHeaderGetRawXmax(mytup.t_data);

		/*
		 * If this tuple version has been updated or locked by some concurrent
		 * transaction(s), what we do depends on whether our lock mode
		 * conflicts with what those other transactions hold, and also on the
		 * status of them.
		 */
		if (!(old_infomask & HEAP_XMAX_INVALID))
		{
			TransactionId rawxmax;
			bool		needwait;

			rawxmax = HeapTupleHeaderGetRawXmax(mytup.t_data);
			if (old_infomask & HEAP_XMAX_IS_MULTI)
			{
				int			nmembers;
				int			i;
				MultiXactMember *members;

				/*
				 * We don't need a test for pg_upgrade'd tuples: this is only
				 * applied to tuples after the first in an update chain.  Said
				 * first tuple in the chain may well be locked-in-9.2-and-
				 * pg_upgraded, but that one was already locked by our caller,
				 * not us; and any subsequent ones cannot be because our
				 * caller must necessarily have obtained a snapshot later than
				 * the pg_upgrade itself.
				 */
				Assert(!HEAP_LOCKED_UPGRADED(mytup.t_data->t_infomask));

				nmembers = GetMultiXactIdMembers(rawxmax, &members, false,
												 HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));
				for (i = 0; i < nmembers; i++)
				{
					result = test_lockmode_for_conflict(members[i].status,
														members[i].xid,
														mode,
														&mytup,
														&needwait);

					/*
					 * If the tuple was already locked by ourselves in a
					 * previous iteration of this (say heap_lock_tuple was
					 * forced to restart the locking loop because of a change
					 * in xmax), then we hold the lock already on this tuple
					 * version and we don't need to do anything; and this is
					 * not an error condition either.  We just need to skip
					 * this tuple and continue locking the next version in the
					 * update chain.
					 */
					if (result == TM_SelfModified)
					{
						pfree(members);
						goto next;
					}

					if (needwait)
					{
						LockBuffer(buf, BUFFER_LOCK_UNLOCK);
						XactLockTableWait(members[i].xid, rel,
										  &mytup.t_self,
										  XLTW_LockUpdated);
						pfree(members);
						goto l4;
					}
					if (result != TM_Ok)
					{
						pfree(members);
						goto out_locked;
					}
				}
				if (members)
					pfree(members);
			}
			else
			{
				MultiXactStatus status;

				/*
				 * For a non-multi Xmax, we first need to compute the
				 * corresponding MultiXactStatus by using the infomask bits.
				 */
				if (HEAP_XMAX_IS_LOCKED_ONLY(old_infomask))
				{
					if (HEAP_XMAX_IS_KEYSHR_LOCKED(old_infomask))
						status = MultiXactStatusForKeyShare;
					else if (HEAP_XMAX_IS_SHR_LOCKED(old_infomask))
						status = MultiXactStatusForShare;
					else if (HEAP_XMAX_IS_EXCL_LOCKED(old_infomask))
					{
						if (old_infomask2 & HEAP_KEYS_UPDATED)
							status = MultiXactStatusForUpdate;
						else
							status = MultiXactStatusForNoKeyUpdate;
					}
					else
					{
						/*
						 * LOCK_ONLY present alone (a pg_upgraded tuple marked
						 * as share-locked in the old cluster) shouldn't be
						 * seen in the middle of an update chain.
						 */
						elog(ERROR, "invalid lock status in tuple");
					}
				}
				else
				{
					/* it's an update, but which kind? */
					if (old_infomask2 & HEAP_KEYS_UPDATED)
						status = MultiXactStatusUpdate;
					else
						status = MultiXactStatusNoKeyUpdate;
				}

				result = test_lockmode_for_conflict(status, rawxmax, mode,
													&mytup, &needwait);

				/*
				 * If the tuple was already locked by ourselves in a previous
				 * iteration of this (say heap_lock_tuple was forced to
				 * restart the locking loop because of a change in xmax), then
				 * we hold the lock already on this tuple version and we don't
				 * need to do anything; and this is not an error condition
				 * either.  We just need to skip this tuple and continue
				 * locking the next version in the update chain.
				 */
				if (result == TM_SelfModified)
					goto next;

				if (needwait)
				{
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					XactLockTableWait(rawxmax, rel, &mytup.t_self,
									  XLTW_LockUpdated);
					goto l4;
				}
				if (result != TM_Ok)
				{
					goto out_locked;
				}
			}
		}

		/* compute the new Xmax and infomask values for the tuple ... */
		compute_new_xmax_infomask(xmax, old_infomask, mytup.t_data->t_infomask2,
								  xid, mode, false,
								  &new_xmax, &new_infomask, &new_infomask2);

		if (PageIsAllVisible(BufferGetPage(buf)) &&
			visibilitymap_clear(rel, block, vmbuffer,
								VISIBILITYMAP_ALL_FROZEN))
			cleared_all_frozen = true;

		START_CRIT_SECTION();

		/* ... and set them */
		HeapTupleHeaderSetXmax(mytup.t_data, new_xmax);
		mytup.t_data->t_infomask &= ~HEAP_XMAX_BITS;
		mytup.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		mytup.t_data->t_infomask |= new_infomask;
		mytup.t_data->t_infomask2 |= new_infomask2;

		MarkBufferDirty(buf);

		/* XLOG stuff */
		if (RelationNeedsWAL(rel))
		{
			xl_heap_lock_updated xlrec;
			XLogRecPtr	recptr;
			Page		page = BufferGetPage(buf);

			XLogBeginInsert();
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

			xlrec.offnum = ItemPointerGetOffsetNumber(&mytup.t_self);
			xlrec.xmax = new_xmax;
			xlrec.infobits_set = compute_infobits(new_infomask, new_infomask2);
			xlrec.flags =
				cleared_all_frozen ? XLH_LOCK_ALL_FROZEN_CLEARED : 0;

			XLogRegisterData((char *) &xlrec, SizeOfHeapLockUpdated);

			recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED);

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

next:
		/* if we find the end of update chain, we're done. */
		if (mytup.t_data->t_infomask & HEAP_XMAX_INVALID ||
			HeapTupleHeaderIndicatesMovedPartitions(mytup.t_data) ||
			ItemPointerEquals(&mytup.t_self, &mytup.t_data->t_ctid) ||
			HeapTupleHeaderIsOnlyLocked(mytup.t_data))
		{
			result = TM_Ok;
			goto out_locked;
		}

		/* tail recursion */
		priorXmax = HeapTupleHeaderGetUpdateXid(mytup.t_data);
		ItemPointerCopy(&(mytup.t_data->t_ctid), &tupid);
		UnlockReleaseBuffer(buf);
	}

	result = TM_Ok;

out_locked:
	UnlockReleaseBuffer(buf);

out_unlocked:
	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	return result;
}

/*
 * heap_lock_updated_tuple
 *		Follow update chain when locking an updated tuple, acquiring locks (row
 *		marks) on the updated versions.
 *
 * The initial tuple is assumed to be already locked.
 *
 * This function doesn't check visibility, it just unconditionally marks the
 * tuple(s) as locked.  If any tuple in the updated chain is being deleted
 * concurrently (or updated with the key being modified), sleep until the
 * transaction doing it is finished.
 *
 * Note that we don't acquire heavyweight tuple locks on the tuples we walk
 * when we have to wait for other transactions to release them, as opposed to
 * what heap_lock_tuple does.  The reason is that having more than one
 * transaction walking the chain is probably uncommon enough that risk of
 * starvation is not likely: one of the preconditions for being here is that
 * the snapshot in use predates the update that created this tuple (because we
 * started at an earlier version of the tuple), but at the same time such a
 * transaction cannot be using repeatable read or serializable isolation
 * levels, because that would lead to a serializability failure.
 */
static TM_Result
heap_lock_updated_tuple(Relation rel, HeapTuple tuple, ItemPointer ctid,
						TransactionId xid, LockTupleMode mode)
{
	/*
	 * If the tuple has not been updated, or has moved into another partition
	 * (effectively a delete) stop here.
	 */
	if (!HeapTupleHeaderIndicatesMovedPartitions(tuple->t_data) &&
		!ItemPointerEquals(&tuple->t_self, ctid))
	{
		/*
		 * If this is the first possibly-multixact-able operation in the
		 * current transaction, set my per-backend OldestMemberMXactId
		 * setting. We can be certain that the transaction will never become a
		 * member of any older MultiXactIds than that.  (We have to do this
		 * even if we end up just using our own TransactionId below, since
		 * some other backend could incorporate our XID into a MultiXact
		 * immediately afterwards.)
		 */
		MultiXactIdSetOldestMember();

		return heap_lock_updated_tuple_rec(rel, ctid, xid, mode);
	}

	/* nothing to lock */
	return TM_Ok;
}

/*
 *	heap_finish_speculative - mark speculative insertion as successful
 *
 * To successfully finish a speculative insertion we have to clear speculative
 * token from tuple.  To do so the t_ctid field, which will contain a
 * speculative token value, is modified in place to point to the tuple itself,
 * which is characteristic of a newly inserted ordinary tuple.
 *
 * NB: It is not ok to commit without either finishing or aborting a
 * speculative insertion.  We could treat speculative tuples of committed
 * transactions implicitly as completed, but then we would have to be prepared
 * to deal with speculative tokens on committed tuples.  That wouldn't be
 * difficult - no-one looks at the ctid field of a tuple with invalid xmax -
 * but clearing the token at completion isn't very expensive either.
 * An explicit confirmation WAL record also makes logical decoding simpler.
 */
void
heap_finish_speculative(Relation relation, ItemPointer tid)
{
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	offnum = ItemPointerGetOffsetNumber(tid);
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(ERROR, "invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	/* SpecTokenOffsetNumber should be distinguishable from any real offset */
	StaticAssertStmt(MaxOffsetNumber < SpecTokenOffsetNumber,
					 "invalid speculative token constant");

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	Assert(HeapTupleHeaderIsSpeculative(htup));

	MarkBufferDirty(buffer);

	/*
	 * Replace the speculative insertion token with a real t_ctid, pointing to
	 * itself like it does on regular tuples.
	 */
	htup->t_ctid = *tid;

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_confirm xlrec;
		XLogRecPtr	recptr;

		xlrec.offnum = ItemPointerGetOffsetNumber(tid);

		XLogBeginInsert();

		/* We want the same filtering on this as on a plain insert */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		XLogRegisterData((char *) &xlrec, SizeOfHeapConfirm);
		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_CONFIRM);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
}

/*
 *	heap_abort_speculative - kill a speculatively inserted tuple
 *
 * Marks a tuple that was speculatively inserted in the same command as dead,
 * by setting its xmin as invalid.  That makes it immediately appear as dead
 * to all transactions, including our own.  In particular, it makes
 * HeapTupleSatisfiesDirty() regard the tuple as dead, so that another backend
 * inserting a duplicate key value won't unnecessarily wait for our whole
 * transaction to finish (it'll just wait for our speculative insertion to
 * finish).
 *
 * Killing the tuple prevents "unprincipled deadlocks", which are deadlocks
 * that arise due to a mutual dependency that is not user visible.  By
 * definition, unprincipled deadlocks cannot be prevented by the user
 * reordering lock acquisition in client code, because the implementation level
 * lock acquisitions are not under the user's direct control.  If speculative
 * inserters did not take this precaution, then under high concurrency they
 * could deadlock with each other, which would not be acceptable.
 *
 * This is somewhat redundant with heap_delete, but we prefer to have a
 * dedicated routine with stripped down requirements.  Note that this is also
 * used to delete the TOAST tuples created during speculative insertion.
 *
 * This routine does not affect logical decoding as it only looks at
 * confirmation records.
 */
void
heap_abort_speculative(Relation relation, ItemPointer tid)
{
	TransactionId xid = GetCurrentTransactionId();
	ItemId		lp;
	HeapTupleData tp;
	Page		page;
	BlockNumber block;
	Buffer		buffer;
	TransactionId prune_xid;

	Assert(ItemPointerIsValid(tid));

	block = ItemPointerGetBlockNumber(tid);
	buffer = ReadBuffer(relation, block);
	page = BufferGetPage(buffer);

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Page can't be all visible, we just inserted into it, and are still
	 * running.
	 */
	Assert(!PageIsAllVisible(page));

	lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	Assert(ItemIdIsNormal(lp));

	tp.t_tableOid = RelationGetRelid(relation);
	tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tp.t_len = ItemIdGetLength(lp);
	tp.t_self = *tid;

	/*
	 * Sanity check that the tuple really is a speculatively inserted tuple,
	 * inserted by us.
	 */
	if (tp.t_data->t_choice.t_heap.t_xmin != xid)
		elog(ERROR, "attempted to kill a tuple inserted by another transaction");
	if (!(IsToastRelation(relation) || HeapTupleHeaderIsSpeculative(tp.t_data)))
		elog(ERROR, "attempted to kill a non-speculative tuple");
	Assert(!HeapTupleHeaderIsHeapOnly(tp.t_data));

	/*
	 * No need to check for serializable conflicts here.  There is never a
	 * need for a combo CID, either.  No need to extract replica identity, or
	 * do anything special with infomask bits.
	 */

	START_CRIT_SECTION();

	/*
	 * The tuple will become DEAD immediately.  Flag that this page is a
	 * candidate for pruning by setting xmin to TransactionXmin. While not
	 * immediately prunable, it is the oldest xid we can cheaply determine
	 * that's safe against wraparound / being older than the table's
	 * relfrozenxid.  To defend against the unlikely case of a new relation
	 * having a newer relfrozenxid than our TransactionXmin, use relfrozenxid
	 * if so (vacuum can't subsequently move relfrozenxid to beyond
	 * TransactionXmin, so there's no race here).
	 */
	Assert(TransactionIdIsValid(TransactionXmin));
	if (TransactionIdPrecedes(TransactionXmin, relation->rd_rel->relfrozenxid))
		prune_xid = relation->rd_rel->relfrozenxid;
	else
		prune_xid = TransactionXmin;
	PageSetPrunable(page, prune_xid);

	/* store transaction information of xact deleting the tuple */
	tp.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tp.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;

	/*
	 * Set the tuple header xmin to InvalidTransactionId.  This makes the
	 * tuple immediately invisible everyone.  (In particular, to any
	 * transactions waiting on the speculative token, woken up later.)
	 */
	HeapTupleHeaderSetXmin(tp.t_data, InvalidTransactionId);

	/* Clear the speculative insertion token too */
	tp.t_data->t_ctid = tp.t_self;

	MarkBufferDirty(buffer);

	/*
	 * XLOG stuff
	 *
	 * The WAL records generated here match heap_delete().  The same recovery
	 * routines are used.
	 */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_delete xlrec;
		XLogRecPtr	recptr;

		xlrec.flags = XLH_DELETE_IS_SUPER;
		xlrec.infobits_set = compute_infobits(tp.t_data->t_infomask,
											  tp.t_data->t_infomask2);
		xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);
		xlrec.xmax = xid;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapDelete);
		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		/* No replica identity & replication origin logged */

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (HeapTupleHasExternal(&tp))
	{
		Assert(!IsToastRelation(relation));
		heap_toast_delete(relation, &tp, true);
	}

	/*
	 * Never need to mark tuple for invalidation, since catalogs don't support
	 * speculative insertion
	 */

	/* Now we can release the buffer */
	ReleaseBuffer(buffer);

	/* count deletion, as we counted the insertion too */
	pgstat_count_heap_delete(relation);
}

/*
 * heap_inplace_update - update a tuple "in place" (ie, overwrite it)
 *
 * Overwriting violates both MVCC and transactional safety, so the uses
 * of this function in Postgres are extremely limited.  Nonetheless we
 * find some places to use it.
 *
 * The tuple cannot change size, and therefore it's reasonable to assume
 * that its null bitmap (if any) doesn't change either.  So we just
 * overwrite the data portion of the tuple without touching the null
 * bitmap or any of the header fields.
 *
 * tuple is an in-memory tuple structure containing the data to be written
 * over the target tuple.  Also, tuple->t_self identifies the target tuple.
 *
 * Note that the tuple updated here had better not come directly from the
 * syscache if the relation has a toast relation as this tuple could
 * include toast values that have been expanded, causing a failure here.
 */
void
heap_inplace_update(Relation relation, HeapTuple tuple)
{
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	uint32		newlen;

	/*
	 * For now, we don't allow parallel updates.  Unlike a regular update,
	 * this should never create a combo CID, so it might be possible to relax
	 * this restriction, but not without more thought and testing.  It's not
	 * clear that it would be useful, anyway.
	 */
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot update tuples during a parallel operation")));

	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&(tuple->t_self)));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	offnum = ItemPointerGetOffsetNumber(&(tuple->t_self));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(ERROR, "invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	newlen = tuple->t_len - tuple->t_data->t_hoff;
	if (oldlen != newlen || htup->t_hoff != tuple->t_data->t_hoff)
		elog(ERROR, "wrong tuple length");

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	memcpy((char *) htup + htup->t_hoff,
		   (char *) tuple->t_data + tuple->t_data->t_hoff,
		   newlen);

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_inplace xlrec;
		XLogRecPtr	recptr;

		xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapInplace);

		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
		XLogRegisterBufData(0, (char *) htup + htup->t_hoff, newlen);

		/* inplace updates aren't decoded atm, don't log the origin */

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INPLACE);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);

	/*
	 * Send out shared cache inval if necessary.  Note that because we only
	 * pass the new version of the tuple, this mustn't be used for any
	 * operations that could change catcache lookup keys.  But we aren't
	 * bothering with index updates either, so that's true a fortiori.
	 */
	if (!IsBootstrapProcessingMode())
		CacheInvalidateHeapTuple(relation, tuple, NULL);
}

#define		FRM_NOOP				0x0001
#define		FRM_INVALIDATE_XMAX		0x0002
#define		FRM_RETURN_IS_XID		0x0004
#define		FRM_RETURN_IS_MULTI		0x0008
#define		FRM_MARK_COMMITTED		0x0010

/*
 * FreezeMultiXactId
 *		Determine what to do during freezing when a tuple is marked by a
 *		MultiXactId.
 *
 * "flags" is an output value; it's used to tell caller what to do on return.
 *
 * "mxid_oldest_xid_out" is an output value; it's used to track the oldest
 * extant Xid within any Multixact that will remain after freezing executes.
 *
 * Possible values that we can set in "flags":
 * FRM_NOOP
 *		don't do anything -- keep existing Xmax
 * FRM_INVALIDATE_XMAX
 *		mark Xmax as InvalidTransactionId and set XMAX_INVALID flag.
 * FRM_RETURN_IS_XID
 *		The Xid return value is a single update Xid to set as xmax.
 * FRM_MARK_COMMITTED
 *		Xmax can be marked as HEAP_XMAX_COMMITTED
 * FRM_RETURN_IS_MULTI
 *		The return value is a new MultiXactId to set as new Xmax.
 *		(caller must obtain proper infomask bits using GetMultiXactIdHintBits)
 *
 * "mxid_oldest_xid_out" is only set when "flags" contains either FRM_NOOP or
 * FRM_RETURN_IS_MULTI, since we only leave behind a MultiXactId for these.
 *
 * NB: Creates a _new_ MultiXactId when FRM_RETURN_IS_MULTI is set in "flags".
 */
static TransactionId
FreezeMultiXactId(MultiXactId multi, uint16 t_infomask,
				  TransactionId relfrozenxid, TransactionId relminmxid,
				  TransactionId cutoff_xid, MultiXactId cutoff_multi,
				  uint16 *flags, TransactionId *mxid_oldest_xid_out)
{
	TransactionId xid = InvalidTransactionId;
	int			i;
	MultiXactMember *members;
	int			nmembers;
	bool		need_replace;
	int			nnewmembers;
	MultiXactMember *newmembers;
	bool		has_lockers;
	TransactionId update_xid;
	bool		update_committed;
	TransactionId temp_xid_out;

	*flags = 0;

	/* We should only be called in Multis */
	Assert(t_infomask & HEAP_XMAX_IS_MULTI);

	if (!MultiXactIdIsValid(multi) ||
		HEAP_LOCKED_UPGRADED(t_infomask))
	{
		/* Ensure infomask bits are appropriately set/reset */
		*flags |= FRM_INVALIDATE_XMAX;
		return InvalidTransactionId;
	}
	else if (MultiXactIdPrecedes(multi, relminmxid))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("found multixact %u from before relminmxid %u",
								 multi, relminmxid)));
	else if (MultiXactIdPrecedes(multi, cutoff_multi))
	{
		/*
		 * This old multi cannot possibly have members still running, but
		 * verify just in case.  If it was a locker only, it can be removed
		 * without any further consideration; but if it contained an update,
		 * we might need to preserve it.
		 */
		if (MultiXactIdIsRunning(multi,
								 HEAP_XMAX_IS_LOCKED_ONLY(t_infomask)))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("multixact %u from before cutoff %u found to be still running",
									 multi, cutoff_multi)));

		if (HEAP_XMAX_IS_LOCKED_ONLY(t_infomask))
		{
			*flags |= FRM_INVALIDATE_XMAX;
			xid = InvalidTransactionId;
		}
		else
		{
			/* replace multi by update xid */
			xid = MultiXactIdGetUpdateXid(multi, t_infomask);

			/* wasn't only a lock, xid needs to be valid */
			Assert(TransactionIdIsValid(xid));

			if (TransactionIdPrecedes(xid, relfrozenxid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("found update xid %u from before relfrozenxid %u",
										 xid, relfrozenxid)));

			/*
			 * If the xid is older than the cutoff, it has to have aborted,
			 * otherwise the tuple would have gotten pruned away.
			 */
			if (TransactionIdPrecedes(xid, cutoff_xid))
			{
				if (TransactionIdDidCommit(xid))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg_internal("cannot freeze committed update xid %u", xid)));
				*flags |= FRM_INVALIDATE_XMAX;
				xid = InvalidTransactionId;
			}
			else
			{
				*flags |= FRM_RETURN_IS_XID;
			}
		}

		/*
		 * Don't push back mxid_oldest_xid_out using FRM_RETURN_IS_XID Xid, or
		 * when no Xids will remain
		 */
		return xid;
	}

	/*
	 * This multixact might have or might not have members still running, but
	 * we know it's valid and is newer than the cutoff point for multis.
	 * However, some member(s) of it may be below the cutoff for Xids, so we
	 * need to walk the whole members array to figure out what to do, if
	 * anything.
	 */

	nmembers =
		GetMultiXactIdMembers(multi, &members, false,
							  HEAP_XMAX_IS_LOCKED_ONLY(t_infomask));
	if (nmembers <= 0)
	{
		/* Nothing worth keeping */
		*flags |= FRM_INVALIDATE_XMAX;
		return InvalidTransactionId;
	}

	/* is there anything older than the cutoff? */
	need_replace = false;
	temp_xid_out = *mxid_oldest_xid_out;	/* init for FRM_NOOP */
	for (i = 0; i < nmembers; i++)
	{
		if (TransactionIdPrecedes(members[i].xid, cutoff_xid))
		{
			need_replace = true;
			break;
		}
		if (TransactionIdPrecedes(members[i].xid, temp_xid_out))
			temp_xid_out = members[i].xid;
	}

	/*
	 * In the simplest case, there is no member older than the cutoff; we can
	 * keep the existing MultiXactId as-is, avoiding a more expensive second
	 * pass over the multi
	 */
	if (!need_replace)
	{
		/*
		 * When mxid_oldest_xid_out gets pushed back here it's likely that the
		 * update Xid was the oldest member, but we don't rely on that
		 */
		*flags |= FRM_NOOP;
		*mxid_oldest_xid_out = temp_xid_out;
		pfree(members);
		return multi;
	}

	/*
	 * Do a more thorough second pass over the multi to figure out which
	 * member XIDs actually need to be kept.  Checking the precise status of
	 * individual members might even show that we don't need to keep anything.
	 */
	nnewmembers = 0;
	newmembers = palloc(sizeof(MultiXactMember) * nmembers);
	has_lockers = false;
	update_xid = InvalidTransactionId;
	update_committed = false;
	temp_xid_out = *mxid_oldest_xid_out;	/* init for FRM_RETURN_IS_MULTI */

	for (i = 0; i < nmembers; i++)
	{
		/*
		 * Determine whether to keep this member or ignore it.
		 */
		if (ISUPDATE_from_mxstatus(members[i].status))
		{
			TransactionId xid = members[i].xid;

			Assert(TransactionIdIsValid(xid));
			if (TransactionIdPrecedes(xid, relfrozenxid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("found update xid %u from before relfrozenxid %u",
										 xid, relfrozenxid)));

			/*
			 * It's an update; should we keep it?  If the transaction is known
			 * aborted or crashed then it's okay to ignore it, otherwise not.
			 * Note that an updater older than cutoff_xid cannot possibly be
			 * committed, because HeapTupleSatisfiesVacuum would have returned
			 * HEAPTUPLE_DEAD and we would not be trying to freeze the tuple.
			 *
			 * As with all tuple visibility routines, it's critical to test
			 * TransactionIdIsInProgress before TransactionIdDidCommit,
			 * because of race conditions explained in detail in
			 * heapam_visibility.c.
			 */
			if (TransactionIdIsCurrentTransactionId(xid) ||
				TransactionIdIsInProgress(xid))
			{
				Assert(!TransactionIdIsValid(update_xid));
				update_xid = xid;
			}
			else if (TransactionIdDidCommit(xid))
			{
				/*
				 * The transaction committed, so we can tell caller to set
				 * HEAP_XMAX_COMMITTED.  (We can only do this because we know
				 * the transaction is not running.)
				 */
				Assert(!TransactionIdIsValid(update_xid));
				update_committed = true;
				update_xid = xid;
			}
			else
			{
				/*
				 * Not in progress, not committed -- must be aborted or
				 * crashed; we can ignore it.
				 */
			}

			/*
			 * Since the tuple wasn't totally removed when vacuum pruned, the
			 * update Xid cannot possibly be older than the xid cutoff. The
			 * presence of such a tuple would cause corruption, so be paranoid
			 * and check.
			 */
			if (TransactionIdIsValid(update_xid) &&
				TransactionIdPrecedes(update_xid, cutoff_xid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("found update xid %u from before xid cutoff %u",
										 update_xid, cutoff_xid)));

			/*
			 * We determined that this is an Xid corresponding to an update
			 * that must be retained -- add it to new members list for later.
			 *
			 * Also consider pushing back temp_xid_out, which is needed when
			 * we later conclude that a new multi is required (i.e. when we go
			 * on to set FRM_RETURN_IS_MULTI for our caller because we also
			 * need to retain a locker that's still running).
			 */
			if (TransactionIdIsValid(update_xid))
			{
				newmembers[nnewmembers++] = members[i];
				if (TransactionIdPrecedes(members[i].xid, temp_xid_out))
					temp_xid_out = members[i].xid;
			}
		}
		else
		{
			/* We only keep lockers if they are still running */
			if (TransactionIdIsCurrentTransactionId(members[i].xid) ||
				TransactionIdIsInProgress(members[i].xid))
			{
				/*
				 * Running locker cannot possibly be older than the cutoff.
				 *
				 * The cutoff is <= VACUUM's OldestXmin, which is also the
				 * initial value used for top-level relfrozenxid_out tracking
				 * state.  A running locker cannot be older than VACUUM's
				 * OldestXmin, either, so we don't need a temp_xid_out step.
				 */
				Assert(TransactionIdIsNormal(members[i].xid));
				Assert(!TransactionIdPrecedes(members[i].xid, cutoff_xid));
				Assert(!TransactionIdPrecedes(members[i].xid,
											  *mxid_oldest_xid_out));
				newmembers[nnewmembers++] = members[i];
				has_lockers = true;
			}
		}
	}

	pfree(members);

	/*
	 * Determine what to do with caller's multi based on information gathered
	 * during our second pass
	 */
	if (nnewmembers == 0)
	{
		/* nothing worth keeping!? Tell caller to remove the whole thing */
		*flags |= FRM_INVALIDATE_XMAX;
		xid = InvalidTransactionId;
		/* Don't push back mxid_oldest_xid_out -- no Xids will remain */
	}
	else if (TransactionIdIsValid(update_xid) && !has_lockers)
	{
		/*
		 * If there's a single member and it's an update, pass it back alone
		 * without creating a new Multi.  (XXX we could do this when there's a
		 * single remaining locker, too, but that would complicate the API too
		 * much; moreover, the case with the single updater is more
		 * interesting, because those are longer-lived.)
		 */
		Assert(nnewmembers == 1);
		*flags |= FRM_RETURN_IS_XID;
		if (update_committed)
			*flags |= FRM_MARK_COMMITTED;
		xid = update_xid;
		/* Don't push back mxid_oldest_xid_out using FRM_RETURN_IS_XID Xid */
	}
	else
	{
		/*
		 * Create a new multixact with the surviving members of the previous
		 * one, to set as new Xmax in the tuple.  The oldest surviving member
		 * might push back mxid_oldest_xid_out.
		 */
		xid = MultiXactIdCreateFromMembers(nnewmembers, newmembers);
		*flags |= FRM_RETURN_IS_MULTI;
		*mxid_oldest_xid_out = temp_xid_out;
	}

	pfree(newmembers);

	return xid;
}

/*
 * heap_prepare_freeze_tuple
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID and cutoff MultiXactId.  If so,
 * setup enough state (in the *frz output argument) to later execute and
 * WAL-log what we would need to do, and return true.  Return false if nothing
 * is to be changed.  In addition, set *totally_frozen to true if the tuple
 * will be totally frozen after these operations are performed and false if
 * more freezing will eventually be required.
 *
 * Caller must set frz->offset itself, before heap_execute_freeze_tuple call.
 *
 * It is assumed that the caller has checked the tuple with
 * HeapTupleSatisfiesVacuum() and determined that it is not HEAPTUPLE_DEAD
 * (else we should be removing the tuple, not freezing it).
 *
 * The *relfrozenxid_out and *relminmxid_out arguments are the current target
 * relfrozenxid and relminmxid for VACUUM caller's heap rel.  Any and all
 * unfrozen XIDs or MXIDs that remain in caller's rel after VACUUM finishes
 * _must_ have values >= the final relfrozenxid/relminmxid values in pg_class.
 * This includes XIDs that remain as MultiXact members from any tuple's xmax.
 * Each call here pushes back *relfrozenxid_out and/or *relminmxid_out as
 * needed to avoid unsafe final values in rel's authoritative pg_class tuple.
 *
 * NB: cutoff_xid *must* be <= VACUUM's OldestXmin, to ensure that any
 * XID older than it could neither be running nor seen as running by any
 * open transaction.  This ensures that the replacement will not change
 * anyone's idea of the tuple state.
 * Similarly, cutoff_multi must be <= VACUUM's OldestMxact.
 *
 * NB: This function has side effects: it might allocate a new MultiXactId.
 * It will be set as tuple's new xmax when our *frz output is processed within
 * heap_execute_freeze_tuple later on.  If the tuple is in a shared buffer
 * then caller had better have an exclusive lock on it already.
 *
 * NB: It is not enough to set hint bits to indicate an XID committed/aborted.
 * The *frz WAL record we output completely removes all old XIDs during REDO.
 */
bool
heap_prepare_freeze_tuple(HeapTupleHeader tuple,
						  TransactionId relfrozenxid, TransactionId relminmxid,
						  TransactionId cutoff_xid, TransactionId cutoff_multi,
						  xl_heap_freeze_tuple *frz, bool *totally_frozen,
						  TransactionId *relfrozenxid_out,
						  MultiXactId *relminmxid_out)
{
	bool		changed = false;
	bool		xmax_already_frozen = false;
	bool		xmin_frozen;
	bool		freeze_xmax;
	TransactionId xid;

	frz->frzflags = 0;
	frz->t_infomask2 = tuple->t_infomask2;
	frz->t_infomask = tuple->t_infomask;
	frz->xmax = HeapTupleHeaderGetRawXmax(tuple);

	/*
	 * Process xmin.  xmin_frozen has two slightly different meanings: in the
	 * !XidIsNormal case, it means "the xmin doesn't need any freezing" (it's
	 * already a permanent value), while in the block below it is set true to
	 * mean "xmin won't need freezing after what we do to it here" (false
	 * otherwise).  In both cases we're allowed to set totally_frozen, as far
	 * as xmin is concerned.  Both cases also don't require relfrozenxid_out
	 * handling, since either way the tuple's xmin will be a permanent value
	 * once we're done with it.
	 */
	xid = HeapTupleHeaderGetXmin(tuple);
	if (!TransactionIdIsNormal(xid))
		xmin_frozen = true;
	else
	{
		if (TransactionIdPrecedes(xid, relfrozenxid))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("found xmin %u from before relfrozenxid %u",
									 xid, relfrozenxid)));

		xmin_frozen = TransactionIdPrecedes(xid, cutoff_xid);
		if (xmin_frozen)
		{
			if (!TransactionIdDidCommit(xid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("uncommitted xmin %u from before xid cutoff %u needs to be frozen",
										 xid, cutoff_xid)));

			frz->t_infomask |= HEAP_XMIN_FROZEN;
			changed = true;
		}
		else
		{
			/* xmin to remain unfrozen.  Could push back relfrozenxid_out. */
			if (TransactionIdPrecedes(xid, *relfrozenxid_out))
				*relfrozenxid_out = xid;
		}
	}

	/*
	 * Process xmax.  To thoroughly examine the current Xmax value we need to
	 * resolve a MultiXactId to its member Xids, in case some of them are
	 * below the given cutoff for Xids.  In that case, those values might need
	 * freezing, too.  Also, if a multi needs freezing, we cannot simply take
	 * it out --- if there's a live updater Xid, it needs to be kept.
	 *
	 * Make sure to keep heap_tuple_would_freeze in sync with this.
	 */
	xid = HeapTupleHeaderGetRawXmax(tuple);

	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		TransactionId newxmax;
		uint16		flags;
		TransactionId mxid_oldest_xid_out = *relfrozenxid_out;

		newxmax = FreezeMultiXactId(xid, tuple->t_infomask,
									relfrozenxid, relminmxid,
									cutoff_xid, cutoff_multi,
									&flags, &mxid_oldest_xid_out);

		freeze_xmax = (flags & FRM_INVALIDATE_XMAX);

		if (flags & FRM_RETURN_IS_XID)
		{
			/*
			 * xmax will become an updater Xid (original MultiXact's updater
			 * member Xid will be carried forward as a simple Xid in Xmax).
			 * Might have to ratchet back relfrozenxid_out here, though never
			 * relminmxid_out.
			 */
			Assert(!freeze_xmax);
			Assert(TransactionIdIsValid(newxmax));
			if (TransactionIdPrecedes(newxmax, *relfrozenxid_out))
				*relfrozenxid_out = newxmax;

			/*
			 * NB -- some of these transformations are only valid because we
			 * know the return Xid is a tuple updater (i.e. not merely a
			 * locker.) Also note that the only reason we don't explicitly
			 * worry about HEAP_KEYS_UPDATED is because it lives in
			 * t_infomask2 rather than t_infomask.
			 */
			frz->t_infomask &= ~HEAP_XMAX_BITS;
			frz->xmax = newxmax;
			if (flags & FRM_MARK_COMMITTED)
				frz->t_infomask |= HEAP_XMAX_COMMITTED;
			changed = true;
		}
		else if (flags & FRM_RETURN_IS_MULTI)
		{
			uint16		newbits;
			uint16		newbits2;

			/*
			 * xmax is an old MultiXactId that we have to replace with a new
			 * MultiXactId, to carry forward two or more original member XIDs.
			 * Might have to ratchet back relfrozenxid_out here, though never
			 * relminmxid_out.
			 */
			Assert(!freeze_xmax);
			Assert(MultiXactIdIsValid(newxmax));
			Assert(!MultiXactIdPrecedes(newxmax, *relminmxid_out));
			Assert(TransactionIdPrecedesOrEquals(mxid_oldest_xid_out,
												 *relfrozenxid_out));
			*relfrozenxid_out = mxid_oldest_xid_out;

			/*
			 * We can't use GetMultiXactIdHintBits directly on the new multi
			 * here; that routine initializes the masks to all zeroes, which
			 * would lose other bits we need.  Doing it this way ensures all
			 * unrelated bits remain untouched.
			 */
			frz->t_infomask &= ~HEAP_XMAX_BITS;
			frz->t_infomask2 &= ~HEAP_KEYS_UPDATED;
			GetMultiXactIdHintBits(newxmax, &newbits, &newbits2);
			frz->t_infomask |= newbits;
			frz->t_infomask2 |= newbits2;

			frz->xmax = newxmax;

			changed = true;
		}
		else if (flags & FRM_NOOP)
		{
			/*
			 * xmax is a MultiXactId, and nothing about it changes for now.
			 * Might have to ratchet back relminmxid_out, relfrozenxid_out, or
			 * both together.
			 */
			Assert(!freeze_xmax);
			Assert(MultiXactIdIsValid(newxmax) && xid == newxmax);
			Assert(TransactionIdPrecedesOrEquals(mxid_oldest_xid_out,
												 *relfrozenxid_out));
			if (MultiXactIdPrecedes(xid, *relminmxid_out))
				*relminmxid_out = xid;
			*relfrozenxid_out = mxid_oldest_xid_out;
		}
		else
		{
			/*
			 * Keeping nothing (neither an Xid nor a MultiXactId) in xmax.
			 * Won't have to ratchet back relminmxid_out or relfrozenxid_out.
			 */
			Assert(freeze_xmax);
			Assert(!TransactionIdIsValid(newxmax));
		}
	}
	else if (TransactionIdIsNormal(xid))
	{
		if (TransactionIdPrecedes(xid, relfrozenxid))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg_internal("found xmax %u from before relfrozenxid %u",
									 xid, relfrozenxid)));

		if (TransactionIdPrecedes(xid, cutoff_xid))
		{
			/*
			 * If we freeze xmax, make absolutely sure that it's not an XID
			 * that is important.  (Note, a lock-only xmax can be removed
			 * independent of committedness, since a committed lock holder has
			 * released the lock).
			 */
			if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask) &&
				TransactionIdDidCommit(xid))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg_internal("cannot freeze committed xmax %u",
										 xid)));
			freeze_xmax = true;
			/* No need for relfrozenxid_out handling, since we'll freeze xmax */
		}
		else
		{
			freeze_xmax = false;
			if (TransactionIdPrecedes(xid, *relfrozenxid_out))
				*relfrozenxid_out = xid;
		}
	}
	else if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
			 !TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
	{
		freeze_xmax = false;
		xmax_already_frozen = true;
		/* No need for relfrozenxid_out handling for already-frozen xmax */
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("found xmax %u (infomask 0x%04x) not frozen, not multi, not normal",
								 xid, tuple->t_infomask)));

	if (freeze_xmax)
	{
		Assert(!xmax_already_frozen);

		frz->xmax = InvalidTransactionId;

		/*
		 * The tuple might be marked either XMAX_INVALID or XMAX_COMMITTED +
		 * LOCKED.  Normalize to INVALID just to be sure no one gets confused.
		 * Also get rid of the HEAP_KEYS_UPDATED bit.
		 */
		frz->t_infomask &= ~HEAP_XMAX_BITS;
		frz->t_infomask |= HEAP_XMAX_INVALID;
		frz->t_infomask2 &= ~HEAP_HOT_UPDATED;
		frz->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		changed = true;
	}

	/*
	 * Old-style VACUUM FULL is gone, but we have to keep this code as long as
	 * we support having MOVED_OFF/MOVED_IN tuples in the database.
	 */
	if (tuple->t_infomask & HEAP_MOVED)
	{
		xid = HeapTupleHeaderGetXvac(tuple);

		/*
		 * For Xvac, we ignore the cutoff_xid and just always perform the
		 * freeze operation.  The oldest release in which such a value can
		 * actually be set is PostgreSQL 8.4, because old-style VACUUM FULL
		 * was removed in PostgreSQL 9.0.  Note that if we were to respect
		 * cutoff_xid here, we'd need to make surely to clear totally_frozen
		 * when we skipped freezing on that basis.
		 *
		 * No need for relfrozenxid_out handling, since we always freeze xvac.
		 */
		if (TransactionIdIsNormal(xid))
		{
			/*
			 * If a MOVED_OFF tuple is not dead, the xvac transaction must
			 * have failed; whereas a non-dead MOVED_IN tuple must mean the
			 * xvac transaction succeeded.
			 */
			if (tuple->t_infomask & HEAP_MOVED_OFF)
				frz->frzflags |= XLH_INVALID_XVAC;
			else
				frz->frzflags |= XLH_FREEZE_XVAC;

			/*
			 * Might as well fix the hint bits too; usually XMIN_COMMITTED
			 * will already be set here, but there's a small chance not.
			 */
			Assert(!(tuple->t_infomask & HEAP_XMIN_INVALID));
			frz->t_infomask |= HEAP_XMIN_COMMITTED;
			changed = true;
		}
	}

	*totally_frozen = (xmin_frozen &&
					   (freeze_xmax || xmax_already_frozen));
	return changed;
}

/*
 * heap_execute_freeze_tuple
 *		Execute the prepared freezing of a tuple.
 *
 * Caller is responsible for ensuring that no other backend can access the
 * storage underlying this tuple, either by holding an exclusive lock on the
 * buffer containing it (which is what lazy VACUUM does), or by having it be
 * in private storage (which is what CLUSTER and friends do).
 *
 * Note: it might seem we could make the changes without exclusive lock, since
 * TransactionId read/write is assumed atomic anyway.  However there is a race
 * condition: someone who just fetched an old XID that we overwrite here could
 * conceivably not finish checking the XID against pg_xact before we finish
 * the VACUUM and perhaps truncate off the part of pg_xact he needs.  Getting
 * exclusive lock ensures no other backend is in process of checking the
 * tuple status.  Also, getting exclusive lock makes it safe to adjust the
 * infomask bits.
 *
 * NB: All code in here must be safe to execute during crash recovery!
 */
void
heap_execute_freeze_tuple(HeapTupleHeader tuple, xl_heap_freeze_tuple *frz)
{
	HeapTupleHeaderSetXmax(tuple, frz->xmax);

	if (frz->frzflags & XLH_FREEZE_XVAC)
		HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);

	if (frz->frzflags & XLH_INVALID_XVAC)
		HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);

	tuple->t_infomask = frz->t_infomask;
	tuple->t_infomask2 = frz->t_infomask2;
}

/*
 * heap_freeze_tuple
 *		Freeze tuple in place, without WAL logging.
 *
 * Useful for callers like CLUSTER that perform their own WAL logging.
 */
bool
heap_freeze_tuple(HeapTupleHeader tuple,
				  TransactionId relfrozenxid, TransactionId relminmxid,
				  TransactionId cutoff_xid, TransactionId cutoff_multi)
{
	xl_heap_freeze_tuple frz;
	bool		do_freeze;
	bool		tuple_totally_frozen;
	TransactionId relfrozenxid_out = cutoff_xid;
	MultiXactId relminmxid_out = cutoff_multi;

	do_freeze = heap_prepare_freeze_tuple(tuple,
										  relfrozenxid, relminmxid,
										  cutoff_xid, cutoff_multi,
										  &frz, &tuple_totally_frozen,
										  &relfrozenxid_out, &relminmxid_out);

	/*
	 * Note that because this is not a WAL-logged operation, we don't need to
	 * fill in the offset in the freeze record.
	 */

	if (do_freeze)
		heap_execute_freeze_tuple(tuple, &frz);
	return do_freeze;
}

/*
 * For a given MultiXactId, return the hint bits that should be set in the
 * tuple's infomask.
 *
 * Normally this should be called for a multixact that was just created, and
 * so is on our local cache, so the GetMembers call is fast.
 */
static void
GetMultiXactIdHintBits(MultiXactId multi, uint16 *new_infomask,
					   uint16 *new_infomask2)
{
	int			nmembers;
	MultiXactMember *members;
	int			i;
	uint16		bits = HEAP_XMAX_IS_MULTI;
	uint16		bits2 = 0;
	bool		has_update = false;
	LockTupleMode strongest = LockTupleKeyShare;

	/*
	 * We only use this in multis we just created, so they cannot be values
	 * pre-pg_upgrade.
	 */
	nmembers = GetMultiXactIdMembers(multi, &members, false, false);

	for (i = 0; i < nmembers; i++)
	{
		LockTupleMode mode;

		/*
		 * Remember the strongest lock mode held by any member of the
		 * multixact.
		 */
		mode = TUPLOCK_from_mxstatus(members[i].status);
		if (mode > strongest)
			strongest = mode;

		/* See what other bits we need */
		switch (members[i].status)
		{
			case MultiXactStatusForKeyShare:
			case MultiXactStatusForShare:
			case MultiXactStatusForNoKeyUpdate:
				break;

			case MultiXactStatusForUpdate:
				bits2 |= HEAP_KEYS_UPDATED;
				break;

			case MultiXactStatusNoKeyUpdate:
				has_update = true;
				break;

			case MultiXactStatusUpdate:
				bits2 |= HEAP_KEYS_UPDATED;
				has_update = true;
				break;
		}
	}

	if (strongest == LockTupleExclusive ||
		strongest == LockTupleNoKeyExclusive)
		bits |= HEAP_XMAX_EXCL_LOCK;
	else if (strongest == LockTupleShare)
		bits |= HEAP_XMAX_SHR_LOCK;
	else if (strongest == LockTupleKeyShare)
		bits |= HEAP_XMAX_KEYSHR_LOCK;

	if (!has_update)
		bits |= HEAP_XMAX_LOCK_ONLY;

	if (nmembers > 0)
		pfree(members);

	*new_infomask = bits;
	*new_infomask2 = bits2;
}

/*
 * MultiXactIdGetUpdateXid
 *
 * Given a multixact Xmax and corresponding infomask, which does not have the
 * HEAP_XMAX_LOCK_ONLY bit set, obtain and return the Xid of the updating
 * transaction.
 *
 * Caller is expected to check the status of the updating transaction, if
 * necessary.
 */
static TransactionId
MultiXactIdGetUpdateXid(TransactionId xmax, uint16 t_infomask)
{
	TransactionId update_xact = InvalidTransactionId;
	MultiXactMember *members;
	int			nmembers;

	Assert(!(t_infomask & HEAP_XMAX_LOCK_ONLY));
	Assert(t_infomask & HEAP_XMAX_IS_MULTI);

	/*
	 * Since we know the LOCK_ONLY bit is not set, this cannot be a multi from
	 * pre-pg_upgrade.
	 */
	nmembers = GetMultiXactIdMembers(xmax, &members, false, false);

	if (nmembers > 0)
	{
		int			i;

		for (i = 0; i < nmembers; i++)
		{
			/* Ignore lockers */
			if (!ISUPDATE_from_mxstatus(members[i].status))
				continue;

			/* there can be at most one updater */
			Assert(update_xact == InvalidTransactionId);
			update_xact = members[i].xid;
#ifndef USE_ASSERT_CHECKING

			/*
			 * in an assert-enabled build, walk the whole array to ensure
			 * there's no other updater.
			 */
			break;
#endif
		}

		pfree(members);
	}

	return update_xact;
}

/*
 * HeapTupleGetUpdateXid
 *		As above, but use a HeapTupleHeader
 *
 * See also HeapTupleHeaderGetUpdateXid, which can be used without previously
 * checking the hint bits.
 */
TransactionId
HeapTupleGetUpdateXid(HeapTupleHeader tuple)
{
	return MultiXactIdGetUpdateXid(HeapTupleHeaderGetRawXmax(tuple),
								   tuple->t_infomask);
}

/*
 * Does the given multixact conflict with the current transaction grabbing a
 * tuple lock of the given strength?
 *
 * The passed infomask pairs up with the given multixact in the tuple header.
 *
 * If current_is_member is not NULL, it is set to 'true' if the current
 * transaction is a member of the given multixact.
 */
static bool
DoesMultiXactIdConflict(MultiXactId multi, uint16 infomask,
						LockTupleMode lockmode, bool *current_is_member)
{
	int			nmembers;
	MultiXactMember *members;
	bool		result = false;
	LOCKMODE	wanted = tupleLockExtraInfo[lockmode].hwlock;

	if (HEAP_LOCKED_UPGRADED(infomask))
		return false;

	nmembers = GetMultiXactIdMembers(multi, &members, false,
									 HEAP_XMAX_IS_LOCKED_ONLY(infomask));
	if (nmembers >= 0)
	{
		int			i;

		for (i = 0; i < nmembers; i++)
		{
			TransactionId memxid;
			LOCKMODE	memlockmode;

			if (result && (current_is_member == NULL || *current_is_member))
				break;

			memlockmode = LOCKMODE_from_mxstatus(members[i].status);

			/* ignore members from current xact (but track their presence) */
			memxid = members[i].xid;
			if (TransactionIdIsCurrentTransactionId(memxid))
			{
				if (current_is_member != NULL)
					*current_is_member = true;
				continue;
			}
			else if (result)
				continue;

			/* ignore members that don't conflict with the lock we want */
			if (!DoLockModesConflict(memlockmode, wanted))
				continue;

			if (ISUPDATE_from_mxstatus(members[i].status))
			{
				/* ignore aborted updaters */
				if (TransactionIdDidAbort(memxid))
					continue;
			}
			else
			{
				/* ignore lockers-only that are no longer in progress */
				if (!TransactionIdIsInProgress(memxid))
					continue;
			}

			/*
			 * Whatever remains are either live lockers that conflict with our
			 * wanted lock, and updaters that are not aborted.  Those conflict
			 * with what we want.  Set up to return true, but keep going to
			 * look for the current transaction among the multixact members,
			 * if needed.
			 */
			result = true;
		}
		pfree(members);
	}

	return result;
}

/*
 * Do_MultiXactIdWait
 *		Actual implementation for the two functions below.
 *
 * 'multi', 'status' and 'infomask' indicate what to sleep on (the status is
 * needed to ensure we only sleep on conflicting members, and the infomask is
 * used to optimize multixact access in case it's a lock-only multi); 'nowait'
 * indicates whether to use conditional lock acquisition, to allow callers to
 * fail if lock is unavailable.  'rel', 'ctid' and 'oper' are used to set up
 * context information for error messages.  'remaining', if not NULL, receives
 * the number of members that are still running, including any (non-aborted)
 * subtransactions of our own transaction.
 *
 * We do this by sleeping on each member using XactLockTableWait.  Any
 * members that belong to the current backend are *not* waited for, however;
 * this would not merely be useless but would lead to Assert failure inside
 * XactLockTableWait.  By the time this returns, it is certain that all
 * transactions *of other backends* that were members of the MultiXactId
 * that conflict with the requested status are dead (and no new ones can have
 * been added, since it is not legal to add members to an existing
 * MultiXactId).
 *
 * But by the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * Note that in case we return false, the number of remaining members is
 * not to be trusted.
 */
static bool
Do_MultiXactIdWait(MultiXactId multi, MultiXactStatus status,
				   uint16 infomask, bool nowait,
				   Relation rel, ItemPointer ctid, XLTW_Oper oper,
				   int *remaining)
{
	bool		result = true;
	MultiXactMember *members;
	int			nmembers;
	int			remain = 0;

	/* for pre-pg_upgrade tuples, no need to sleep at all */
	nmembers = HEAP_LOCKED_UPGRADED(infomask) ? -1 :
		GetMultiXactIdMembers(multi, &members, false,
							  HEAP_XMAX_IS_LOCKED_ONLY(infomask));

	if (nmembers >= 0)
	{
		int			i;

		for (i = 0; i < nmembers; i++)
		{
			TransactionId memxid = members[i].xid;
			MultiXactStatus memstatus = members[i].status;

			if (TransactionIdIsCurrentTransactionId(memxid))
			{
				remain++;
				continue;
			}

			if (!DoLockModesConflict(LOCKMODE_from_mxstatus(memstatus),
									 LOCKMODE_from_mxstatus(status)))
			{
				if (remaining && TransactionIdIsInProgress(memxid))
					remain++;
				continue;
			}

			/*
			 * This member conflicts with our multi, so we have to sleep (or
			 * return failure, if asked to avoid waiting.)
			 *
			 * Note that we don't set up an error context callback ourselves,
			 * but instead we pass the info down to XactLockTableWait.  This
			 * might seem a bit wasteful because the context is set up and
			 * tore down for each member of the multixact, but in reality it
			 * should be barely noticeable, and it avoids duplicate code.
			 */
			if (nowait)
			{
				result = ConditionalXactLockTableWait(memxid);
				if (!result)
					break;
			}
			else
				XactLockTableWait(memxid, rel, ctid, oper);
		}

		pfree(members);
	}

	if (remaining)
		*remaining = remain;

	return result;
}

/*
 * MultiXactIdWait
 *		Sleep on a MultiXactId.
 *
 * By the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * We return (in *remaining, if not NULL) the number of members that are still
 * running, including any (non-aborted) subtransactions of our own transaction.
 */
static void
MultiXactIdWait(MultiXactId multi, MultiXactStatus status, uint16 infomask,
				Relation rel, ItemPointer ctid, XLTW_Oper oper,
				int *remaining)
{
	(void) Do_MultiXactIdWait(multi, status, infomask, false,
							  rel, ctid, oper, remaining);
}

/*
 * ConditionalMultiXactIdWait
 *		As above, but only lock if we can get the lock without blocking.
 *
 * By the time we finish sleeping, someone else may have changed the Xmax
 * of the containing tuple, so the caller needs to iterate on us somehow.
 *
 * If the multixact is now all gone, return true.  Returns false if some
 * transactions might still be running.
 *
 * We return (in *remaining, if not NULL) the number of members that are still
 * running, including any (non-aborted) subtransactions of our own transaction.
 */
static bool
ConditionalMultiXactIdWait(MultiXactId multi, MultiXactStatus status,
						   uint16 infomask, Relation rel, int *remaining)
{
	return Do_MultiXactIdWait(multi, status, infomask, true,
							  rel, NULL, XLTW_None, remaining);
}

/*
 * heap_tuple_needs_eventual_freeze
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * will eventually require freezing (if tuple isn't removed by pruning first).
 */
bool
heap_tuple_needs_eventual_freeze(HeapTupleHeader tuple)
{
	TransactionId xid;

	/*
	 * If xmin is a normal transaction ID, this tuple is definitely not
	 * frozen.
	 */
	xid = HeapTupleHeaderGetXmin(tuple);
	if (TransactionIdIsNormal(xid))
		return true;

	/*
	 * If xmax is a valid xact or multixact, this tuple is also not frozen.
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
	{
		MultiXactId multi;

		multi = HeapTupleHeaderGetRawXmax(tuple);
		if (MultiXactIdIsValid(multi))
			return true;
	}
	else
	{
		xid = HeapTupleHeaderGetRawXmax(tuple);
		if (TransactionIdIsNormal(xid))
			return true;
	}

	if (tuple->t_infomask & HEAP_MOVED)
	{
		xid = HeapTupleHeaderGetXvac(tuple);
		if (TransactionIdIsNormal(xid))
			return true;
	}

	return false;
}

/*
 * heap_tuple_would_freeze
 *
 * Return value indicates if heap_prepare_freeze_tuple sibling function would
 * freeze any of the XID/XMID fields from the tuple, given the same cutoffs.
 * We must also deal with dead tuples here, since (xmin, xmax, xvac) fields
 * could be processed by pruning away the whole tuple instead of freezing.
 *
 * The *relfrozenxid_out and *relminmxid_out input/output arguments work just
 * like the heap_prepare_freeze_tuple arguments that they're based on.  We
 * never freeze here, which makes tracking the oldest extant XID/MXID simple.
 */
bool
heap_tuple_would_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
						MultiXactId cutoff_multi,
						TransactionId *relfrozenxid_out,
						MultiXactId *relminmxid_out)
{
	TransactionId xid;
	MultiXactId multi;
	bool		would_freeze = false;

	/* First deal with xmin */
	xid = HeapTupleHeaderGetXmin(tuple);
	if (TransactionIdIsNormal(xid))
	{
		if (TransactionIdPrecedes(xid, *relfrozenxid_out))
			*relfrozenxid_out = xid;
		if (TransactionIdPrecedes(xid, cutoff_xid))
			would_freeze = true;
	}

	/* Now deal with xmax */
	xid = InvalidTransactionId;
	multi = InvalidMultiXactId;
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		multi = HeapTupleHeaderGetRawXmax(tuple);
	else
		xid = HeapTupleHeaderGetRawXmax(tuple);

	if (TransactionIdIsNormal(xid))
	{
		/* xmax is a non-permanent XID */
		if (TransactionIdPrecedes(xid, *relfrozenxid_out))
			*relfrozenxid_out = xid;
		if (TransactionIdPrecedes(xid, cutoff_xid))
			would_freeze = true;
	}
	else if (!MultiXactIdIsValid(multi))
	{
		/* xmax is a permanent XID or invalid MultiXactId/XID */
	}
	else if (HEAP_LOCKED_UPGRADED(tuple->t_infomask))
	{
		/* xmax is a pg_upgrade'd MultiXact, which can't have updater XID */
		if (MultiXactIdPrecedes(multi, *relminmxid_out))
			*relminmxid_out = multi;
		/* heap_prepare_freeze_tuple always freezes pg_upgrade'd xmax */
		would_freeze = true;
	}
	else
	{
		/* xmax is a MultiXactId that may have an updater XID */
		MultiXactMember *members;
		int			nmembers;

		if (MultiXactIdPrecedes(multi, *relminmxid_out))
			*relminmxid_out = multi;
		if (MultiXactIdPrecedes(multi, cutoff_multi))
			would_freeze = true;

		/* need to check whether any member of the mxact is old */
		nmembers = GetMultiXactIdMembers(multi, &members, false,
										 HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

		for (int i = 0; i < nmembers; i++)
		{
			xid = members[i].xid;
			Assert(TransactionIdIsNormal(xid));
			if (TransactionIdPrecedes(xid, *relfrozenxid_out))
				*relfrozenxid_out = xid;
			if (TransactionIdPrecedes(xid, cutoff_xid))
				would_freeze = true;
		}
		if (nmembers > 0)
			pfree(members);
	}

	if (tuple->t_infomask & HEAP_MOVED)
	{
		xid = HeapTupleHeaderGetXvac(tuple);
		if (TransactionIdIsNormal(xid))
		{
			if (TransactionIdPrecedes(xid, *relfrozenxid_out))
				*relfrozenxid_out = xid;
			/* heap_prepare_freeze_tuple always freezes xvac */
			would_freeze = true;
		}
	}

	return would_freeze;
}

/*
 * If 'tuple' contains any visible XID greater than latestRemovedXid,
 * ratchet forwards latestRemovedXid to the greatest one found.
 * This is used as the basis for generating Hot Standby conflicts, so
 * if a tuple was never visible then removing it should not conflict
 * with queries.
 */
void
HeapTupleHeaderAdvanceLatestRemovedXid(HeapTupleHeader tuple,
									   TransactionId *latestRemovedXid)
{
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetUpdateXid(tuple);
	TransactionId xvac = HeapTupleHeaderGetXvac(tuple);

	if (tuple->t_infomask & HEAP_MOVED)
	{
		if (TransactionIdPrecedes(*latestRemovedXid, xvac))
			*latestRemovedXid = xvac;
	}

	/*
	 * Ignore tuples inserted by an aborted transaction or if the tuple was
	 * updated/deleted by the inserting transaction.
	 *
	 * Look for a committed hint bit, or if no xmin bit is set, check clog.
	 */
	if (HeapTupleHeaderXminCommitted(tuple) ||
		(!HeapTupleHeaderXminInvalid(tuple) && TransactionIdDidCommit(xmin)))
	{
		if (xmax != xmin &&
			TransactionIdFollows(xmax, *latestRemovedXid))
			*latestRemovedXid = xmax;
	}

	/* *latestRemovedXid may still be invalid at end */
}

#ifdef USE_PREFETCH
/*
 * Helper function for heap_index_delete_tuples.  Issues prefetch requests for
 * prefetch_count buffers.  The prefetch_state keeps track of all the buffers
 * we can prefetch, and which have already been prefetched; each call to this
 * function picks up where the previous call left off.
 *
 * Note: we expect the deltids array to be sorted in an order that groups TIDs
 * by heap block, with all TIDs for each block appearing together in exactly
 * one group.
 */
static void
index_delete_prefetch_buffer(Relation rel,
							 IndexDeletePrefetchState *prefetch_state,
							 int prefetch_count)
{
	BlockNumber cur_hblkno = prefetch_state->cur_hblkno;
	int			count = 0;
	int			i;
	int			ndeltids = prefetch_state->ndeltids;
	TM_IndexDelete *deltids = prefetch_state->deltids;

	for (i = prefetch_state->next_item;
		 i < ndeltids && count < prefetch_count;
		 i++)
	{
		ItemPointer htid = &deltids[i].tid;

		if (cur_hblkno == InvalidBlockNumber ||
			ItemPointerGetBlockNumber(htid) != cur_hblkno)
		{
			cur_hblkno = ItemPointerGetBlockNumber(htid);
			PrefetchBuffer(rel, MAIN_FORKNUM, cur_hblkno);
			count++;
		}
	}

	/*
	 * Save the prefetch position so that next time we can continue from that
	 * position.
	 */
	prefetch_state->next_item = i;
	prefetch_state->cur_hblkno = cur_hblkno;
}
#endif

/*
 * Helper function for heap_index_delete_tuples.  Checks for index corruption
 * involving an invalid TID in index AM caller's index page.
 *
 * This is an ideal place for these checks.  The index AM must hold a buffer
 * lock on the index page containing the TIDs we examine here, so we don't
 * have to worry about concurrent VACUUMs at all.  We can be sure that the
 * index is corrupt when htid points directly to an LP_UNUSED item or
 * heap-only tuple, which is not the case during standard index scans.
 */
static inline void
index_delete_check_htid(TM_IndexDeleteOp *delstate,
						Page page, OffsetNumber maxoff,
						ItemPointer htid, TM_IndexStatus *istatus)
{
	OffsetNumber indexpagehoffnum = ItemPointerGetOffsetNumber(htid);
	ItemId		iid;

	Assert(OffsetNumberIsValid(istatus->idxoffnum));

	if (unlikely(indexpagehoffnum > maxoff))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("heap tid from index tuple (%u,%u) points past end of heap page line pointer array at offset %u of block %u in index \"%s\"",
								 ItemPointerGetBlockNumber(htid),
								 indexpagehoffnum,
								 istatus->idxoffnum, delstate->iblknum,
								 RelationGetRelationName(delstate->irel))));

	iid = PageGetItemId(page, indexpagehoffnum);
	if (unlikely(!ItemIdIsUsed(iid)))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("heap tid from index tuple (%u,%u) points to unused heap page item at offset %u of block %u in index \"%s\"",
								 ItemPointerGetBlockNumber(htid),
								 indexpagehoffnum,
								 istatus->idxoffnum, delstate->iblknum,
								 RelationGetRelationName(delstate->irel))));

	if (ItemIdHasStorage(iid))
	{
		HeapTupleHeader htup;

		Assert(ItemIdIsNormal(iid));
		htup = (HeapTupleHeader) PageGetItem(page, iid);

		if (unlikely(HeapTupleHeaderIsHeapOnly(htup)))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("heap tid from index tuple (%u,%u) points to heap-only tuple at offset %u of block %u in index \"%s\"",
									 ItemPointerGetBlockNumber(htid),
									 indexpagehoffnum,
									 istatus->idxoffnum, delstate->iblknum,
									 RelationGetRelationName(delstate->irel))));
	}
}

/*
 * heapam implementation of tableam's index_delete_tuples interface.
 *
 * This helper function is called by index AMs during index tuple deletion.
 * See tableam header comments for an explanation of the interface implemented
 * here and a general theory of operation.  Note that each call here is either
 * a simple index deletion call, or a bottom-up index deletion call.
 *
 * It's possible for this to generate a fair amount of I/O, since we may be
 * deleting hundreds of tuples from a single index block.  To amortize that
 * cost to some degree, this uses prefetching and combines repeat accesses to
 * the same heap block.
 */
TransactionId
heap_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	/* Initial assumption is that earlier pruning took care of conflict */
	TransactionId latestRemovedXid = InvalidTransactionId;
	BlockNumber blkno = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page = NULL;
	OffsetNumber maxoff = InvalidOffsetNumber;
	TransactionId priorXmax;
#ifdef USE_PREFETCH
	IndexDeletePrefetchState prefetch_state;
	int			prefetch_distance;
#endif
	SnapshotData SnapshotNonVacuumable;
	int			finalndeltids = 0,
				nblocksaccessed = 0;

	/* State that's only used in bottom-up index deletion case */
	int			nblocksfavorable = 0;
	int			curtargetfreespace = delstate->bottomupfreespace,
				lastfreespace = 0,
				actualfreespace = 0;
	bool		bottomup_final_block = false;
#ifdef DIVA
	bool			siro = IsSiro(rel);
	HeapTuple		tmp_tup = NULL;
#ifdef LOCATOR
	DualRefDescData	drefDesc;
#endif /* LOCATOR */
#endif /* DIVA */

	InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel));

	/* Sort caller's deltids array by TID for further processing */
	index_delete_sort(delstate);

	/*
	 * Bottom-up case: resort deltids array in an order attuned to where the
	 * greatest number of promising TIDs are to be found, and determine how
	 * many blocks from the start of sorted array should be considered
	 * favorable.  This will also shrink the deltids array in order to
	 * eliminate completely unfavorable blocks up front.
	 */
	if (delstate->bottomup)
		nblocksfavorable = bottomup_sort_and_shrink(delstate);

#ifdef USE_PREFETCH
	/* Initialize prefetch state. */
	prefetch_state.cur_hblkno = InvalidBlockNumber;
	prefetch_state.next_item = 0;
	prefetch_state.ndeltids = delstate->ndeltids;
	prefetch_state.deltids = delstate->deltids;

	/*
	 * Determine the prefetch distance that we will attempt to maintain.
	 *
	 * Since the caller holds a buffer lock somewhere in rel, we'd better make
	 * sure that isn't a catalog relation before we call code that does
	 * syscache lookups, to avoid risk of deadlock.
	 */
	if (IsCatalogRelation(rel))
		prefetch_distance = maintenance_io_concurrency;
	else
		prefetch_distance =
			get_tablespace_maintenance_io_concurrency(rel->rd_rel->reltablespace);

	/* Cap initial prefetch distance for bottom-up deletion caller */
	if (delstate->bottomup)
	{
		Assert(nblocksfavorable >= 1);
		Assert(nblocksfavorable <= BOTTOMUP_MAX_NBLOCKS);
		prefetch_distance = Min(prefetch_distance, nblocksfavorable);
	}

	/* Start prefetching. */
	index_delete_prefetch_buffer(rel, &prefetch_state, prefetch_distance);
#endif

	/* Iterate over deltids, determine which to delete, check their horizon */
	Assert(delstate->ndeltids > 0);
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *ideltid = &delstate->deltids[i];
		TM_IndexStatus *istatus = delstate->status + ideltid->id;
		ItemPointer htid = &ideltid->tid;
		OffsetNumber offnum;

		/*
		 * Read buffer, and perform required extra steps each time a new block
		 * is encountered.  Avoid refetching if it's the same block as the one
		 * from the last htid.
		 */
		if (blkno == InvalidBlockNumber ||
			ItemPointerGetBlockNumber(htid) != blkno)
		{
			/*
			 * Consider giving up early for bottom-up index deletion caller
			 * first. (Only prefetch next-next block afterwards, when it
			 * becomes clear that we're at least going to access the next
			 * block in line.)
			 *
			 * Sometimes the first block frees so much space for bottom-up
			 * caller that the deletion process can end without accessing any
			 * more blocks.  It is usually necessary to access 2 or 3 blocks
			 * per bottom-up deletion operation, though.
			 */
			if (delstate->bottomup)
			{
				/*
				 * We often allow caller to delete a few additional items
				 * whose entries we reached after the point that space target
				 * from caller was satisfied.  The cost of accessing the page
				 * was already paid at that point, so it made sense to finish
				 * it off.  When that happened, we finalize everything here
				 * (by finishing off the whole bottom-up deletion operation
				 * without needlessly paying the cost of accessing any more
				 * blocks).
				 */
				if (bottomup_final_block)
					break;

				/*
				 * Give up when we didn't enable our caller to free any
				 * additional space as a result of processing the page that we
				 * just finished up with.  This rule is the main way in which
				 * we keep the cost of bottom-up deletion under control.
				 */
				if (nblocksaccessed >= 1 && actualfreespace == lastfreespace)
					break;
				lastfreespace = actualfreespace;	/* for next time */

				/*
				 * Deletion operation (which is bottom-up) will definitely
				 * access the next block in line.  Prepare for that now.
				 *
				 * Decay target free space so that we don't hang on for too
				 * long with a marginal case. (Space target is only truly
				 * helpful when it allows us to recognize that we don't need
				 * to access more than 1 or 2 blocks to satisfy caller due to
				 * agreeable workload characteristics.)
				 *
				 * We are a bit more patient when we encounter contiguous
				 * blocks, though: these are treated as favorable blocks.  The
				 * decay process is only applied when the next block in line
				 * is not a favorable/contiguous block.  This is not an
				 * exception to the general rule; we still insist on finding
				 * at least one deletable item per block accessed.  See
				 * bottomup_nblocksfavorable() for full details of the theory
				 * behind favorable blocks and heap block locality in general.
				 *
				 * Note: The first block in line is always treated as a
				 * favorable block, so the earliest possible point that the
				 * decay can be applied is just before we access the second
				 * block in line.  The Assert() verifies this for us.
				 */
				Assert(nblocksaccessed > 0 || nblocksfavorable > 0);
				if (nblocksfavorable > 0)
					nblocksfavorable--;
				else
					curtargetfreespace /= 2;
			}

			/* release old buffer */
#ifdef LOCATOR
			if (BufferIsValid(buf))
			{
#ifndef USING_LOCK
				if (siro)
				{
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
					ReleaseBuffer(buf);
				}
				else
#endif /* !USING_LOCK*/
					UnlockReleaseBuffer(buf);
			}
#else /* !LOCATOR */
			if (BufferIsValid(buf))
				UnlockReleaseBuffer(buf);
#endif /* LOCATOR */

			blkno = ItemPointerGetBlockNumber(htid);
			buf = ReadBuffer(rel, blkno);
			nblocksaccessed++;
			Assert(!delstate->bottomup ||
				   nblocksaccessed <= BOTTOMUP_MAX_NBLOCKS);

#ifdef USE_PREFETCH

			/*
			 * To maintain the prefetch distance, prefetch one more page for
			 * each page we read.
			 */
			index_delete_prefetch_buffer(rel, &prefetch_state, 1);
#endif

#ifdef LOCATOR
#ifdef USING_LOCK
			drefDesc.dual_ref = NULL;
#else /* !USING_LOCK */
			if (siro)
			{
				drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buf);
				SetPageRefUnit(&drefDesc);
			}
			else
#endif /* USING_LOCK*/
				LockBuffer(buf, BUFFER_LOCK_SHARE);
#else /* !LOCATOR */
			LockBuffer(buf, BUFFER_LOCK_SHARE);
#endif /* LOCATOR */

			page = BufferGetPage(buf);
			maxoff = PageGetMaxOffsetNumber(page);
		}

		/*
		 * In passing, detect index corruption involving an index page with a
		 * TID that points to a location in the heap that couldn't possibly be
		 * correct.  We only do this with actual TIDs from caller's index page
		 * (not items reached by traversing through a HOT chain).
		 */
		index_delete_check_htid(delstate, page, maxoff, htid, istatus);

		if (istatus->knowndeletable)
			Assert(!delstate->bottomup && !istatus->promising);
		else
		{
			ItemPointerData tmp = *htid;
			HeapTupleData heapTuple;

			/* Are any tuples from this HOT chain non-vacuumable? */
#ifdef DIVA
			if (siro)
			{
				copied_latestRemovedXid = InvalidTransactionId;
#ifdef LOCATOR
				if (heap_hot_search_buffer_with_vc(&tmp, rel, buf, &SnapshotNonVacuumable,
												   &heapTuple, &tmp_tup, &drefDesc,
												   NULL, true, NULL))
#else /* !LOCATOR */
				if (heap_hot_search_buffer_with_vc(&tmp, rel, buf, &SnapshotNonVacuumable,
												   &heapTuple, &tmp_tup,
												   NULL, true, NULL))
#endif /* LOCATOR */
					continue;		/* can't delete entry */
			}
			else
			{
				if (heap_hot_search_buffer(&tmp, rel, buf, &SnapshotNonVacuumable,
										   &heapTuple, NULL, true))
					continue;		/* can't delete entry */
			}
#else /* !DIVA */
			if (heap_hot_search_buffer(&tmp, rel, buf, &SnapshotNonVacuumable,
									   &heapTuple, NULL, true))
				continue;		/* can't delete entry */
#endif /* DIVA*/

			/* Caller will delete, since whole HOT chain is vacuumable */
			istatus->knowndeletable = true;

			/* Maintain index free space info for bottom-up deletion case */
			if (delstate->bottomup)
			{
				Assert(istatus->freespace > 0);
				actualfreespace += istatus->freespace;
				if (actualfreespace >= curtargetfreespace)
					bottomup_final_block = true;
			}
		}

		/*
		 * Maintain latestRemovedXid value for deletion operation as a whole
		 * by advancing current value using heap tuple headers.  This is
		 * loosely based on the logic for pruning a HOT chain.
		 */
#ifdef DIVA
		if (siro)
		{
			/*
			 * latestRemovedXid was already gotten in
			 * heap_hot_search_buffer_with_vc(), using non-vacuumable snapshot.
			 */
			if (latestRemovedXid < copied_latestRemovedXid)
				latestRemovedXid = copied_latestRemovedXid;
		}
		else
		{
			offnum = ItemPointerGetOffsetNumber(htid);
			priorXmax = InvalidTransactionId;	/* cannot check first XMIN */
			for (;;)
			{
				ItemId		lp;
				HeapTupleHeader htup;

				/* Sanity check (pure paranoia) */
				if (offnum < FirstOffsetNumber)
					break;

				/*
				 * An offset past the end of page's line pointer array is possible
				 * when the array was truncated
				 */
				if (offnum > maxoff)
					break;

				lp = PageGetItemId(page, offnum);
				if (ItemIdIsRedirected(lp))
				{
					offnum = ItemIdGetRedirect(lp);
					continue;
				}

				/*
				 * We'll often encounter LP_DEAD line pointers (especially with an
				 * entry marked knowndeletable by our caller up front).  No heap
				 * tuple headers get examined for an htid that leads us to an
				 * LP_DEAD item.  This is okay because the earlier pruning
				 * operation that made the line pointer LP_DEAD in the first place
				 * must have considered the original tuple header as part of
				 * generating its own latestRemovedXid value.
				 *
				 * Relying on XLOG_HEAP2_PRUNE records like this is the same
				 * strategy that index vacuuming uses in all cases.  Index VACUUM
				 * WAL records don't even have a latestRemovedXid field of their
				 * own for this reason.
				 */
				if (!ItemIdIsNormal(lp))
					break;

				htup = (HeapTupleHeader) PageGetItem(page, lp);

				/*
				 * Check the tuple XMIN against prior XMAX, if any
				 */
				if (TransactionIdIsValid(priorXmax) &&
					!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
					break;

				HeapTupleHeaderAdvanceLatestRemovedXid(htup, &latestRemovedXid);

				/*
				 * If the tuple is not HOT-updated, then we are at the end of this
				 * HOT-chain.  No need to visit later tuples from the same update
				 * chain (they get their own index entries) -- just move on to
				 * next htid from index AM caller.
				 */
				if (!HeapTupleHeaderIsHotUpdated(htup))
					break;

				/* Advance to next HOT chain member */
				Assert(ItemPointerGetBlockNumber(&htup->t_ctid) == blkno);
				offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
				priorXmax = HeapTupleHeaderGetUpdateXid(htup);
			}
		}
#else /* !DIVA */
		offnum = ItemPointerGetOffsetNumber(htid);
		priorXmax = InvalidTransactionId;	/* cannot check first XMIN */
		for (;;)
		{
			ItemId		lp;
			HeapTupleHeader htup;

			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, offnum);
			if (ItemIdIsRedirected(lp))
			{
				offnum = ItemIdGetRedirect(lp);
				continue;
			}

			/*
			 * We'll often encounter LP_DEAD line pointers (especially with an
			 * entry marked knowndeletable by our caller up front).  No heap
			 * tuple headers get examined for an htid that leads us to an
			 * LP_DEAD item.  This is okay because the earlier pruning
			 * operation that made the line pointer LP_DEAD in the first place
			 * must have considered the original tuple header as part of
			 * generating its own latestRemovedXid value.
			 *
			 * Relying on XLOG_HEAP2_PRUNE records like this is the same
			 * strategy that index vacuuming uses in all cases.  Index VACUUM
			 * WAL records don't even have a latestRemovedXid field of their
			 * own for this reason.
			 */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check the tuple XMIN against prior XMAX, if any
			 */
			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
				break;

			HeapTupleHeaderAdvanceLatestRemovedXid(htup, &latestRemovedXid);

			/*
			 * If the tuple is not HOT-updated, then we are at the end of this
			 * HOT-chain.  No need to visit later tuples from the same update
			 * chain (they get their own index entries) -- just move on to
			 * next htid from index AM caller.
			 */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* Advance to next HOT chain member */
			Assert(ItemPointerGetBlockNumber(&htup->t_ctid) == blkno);
			offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
#endif /* DIVA */

		/* Enable further/final shrinking of deltids for caller */
		finalndeltids = i + 1;
	}

#ifdef LOCATOR
#ifndef USING_LOCK
	if (siro)
	{
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
		ReleaseBuffer(buf);
	}
	else
#endif /* !USING_LOCK */
		UnlockReleaseBuffer(buf);
#else /* !LOCATOR */
	UnlockReleaseBuffer(buf);
#endif /* LOCATOR */

	/*
	 * Shrink deltids array to exclude non-deletable entries at the end.  This
	 * is not just a minor optimization.  Final deltids array size might be
	 * zero for a bottom-up caller.  Index AM is explicitly allowed to rely on
	 * ndeltids being zero in all cases with zero total deletable entries.
	 */
	Assert(finalndeltids > 0 || delstate->bottomup);
	delstate->ndeltids = finalndeltids;

	return latestRemovedXid;
}

/*
 * Specialized inlineable comparison function for index_delete_sort()
 */
static inline int
index_delete_sort_cmp(TM_IndexDelete *deltid1, TM_IndexDelete *deltid2)
{
	ItemPointer tid1 = &deltid1->tid;
	ItemPointer tid2 = &deltid2->tid;

	{
		BlockNumber blk1 = ItemPointerGetBlockNumber(tid1);
		BlockNumber blk2 = ItemPointerGetBlockNumber(tid2);

		if (blk1 != blk2)
			return (blk1 < blk2) ? -1 : 1;
	}
	{
		OffsetNumber pos1 = ItemPointerGetOffsetNumber(tid1);
		OffsetNumber pos2 = ItemPointerGetOffsetNumber(tid2);

		if (pos1 != pos2)
			return (pos1 < pos2) ? -1 : 1;
	}

	Assert(false);

	return 0;
}

/*
 * Sort deltids array from delstate by TID.  This prepares it for further
 * processing by heap_index_delete_tuples().
 *
 * This operation becomes a noticeable consumer of CPU cycles with some
 * workloads, so we go to the trouble of specialization/micro optimization.
 * We use shellsort for this because it's easy to specialize, compiles to
 * relatively few instructions, and is adaptive to presorted inputs/subsets
 * (which are typical here).
 */
static void
index_delete_sort(TM_IndexDeleteOp *delstate)
{
	TM_IndexDelete *deltids = delstate->deltids;
	int			ndeltids = delstate->ndeltids;
	int			low = 0;

	/*
	 * Shellsort gap sequence (taken from Sedgewick-Incerpi paper).
	 *
	 * This implementation is fast with array sizes up to ~4500.  This covers
	 * all supported BLCKSZ values.
	 */
	const int	gaps[9] = {1968, 861, 336, 112, 48, 21, 7, 3, 1};

	/* Think carefully before changing anything here -- keep swaps cheap */
	StaticAssertStmt(sizeof(TM_IndexDelete) <= 8,
					 "element size exceeds 8 bytes");

	for (int g = 0; g < lengthof(gaps); g++)
	{
		for (int hi = gaps[g], i = low + hi; i < ndeltids; i++)
		{
			TM_IndexDelete d = deltids[i];
			int			j = i;

			while (j >= hi && index_delete_sort_cmp(&deltids[j - hi], &d) >= 0)
			{
				deltids[j] = deltids[j - hi];
				j -= hi;
			}
			deltids[j] = d;
		}
	}
}

/*
 * Returns how many blocks should be considered favorable/contiguous for a
 * bottom-up index deletion pass.  This is a number of heap blocks that starts
 * from and includes the first block in line.
 *
 * There is always at least one favorable block during bottom-up index
 * deletion.  In the worst case (i.e. with totally random heap blocks) the
 * first block in line (the only favorable block) can be thought of as a
 * degenerate array of contiguous blocks that consists of a single block.
 * heap_index_delete_tuples() will expect this.
 *
 * Caller passes blockgroups, a description of the final order that deltids
 * will be sorted in for heap_index_delete_tuples() bottom-up index deletion
 * processing.  Note that deltids need not actually be sorted just yet (caller
 * only passes deltids to us so that we can interpret blockgroups).
 *
 * You might guess that the existence of contiguous blocks cannot matter much,
 * since in general the main factor that determines which blocks we visit is
 * the number of promising TIDs, which is a fixed hint from the index AM.
 * We're not really targeting the general case, though -- the actual goal is
 * to adapt our behavior to a wide variety of naturally occurring conditions.
 * The effects of most of the heuristics we apply are only noticeable in the
 * aggregate, over time and across many _related_ bottom-up index deletion
 * passes.
 *
 * Deeming certain blocks favorable allows heapam to recognize and adapt to
 * workloads where heap blocks visited during bottom-up index deletion can be
 * accessed contiguously, in the sense that each newly visited block is the
 * neighbor of the block that bottom-up deletion just finished processing (or
 * close enough to it).  It will likely be cheaper to access more favorable
 * blocks sooner rather than later (e.g. in this pass, not across a series of
 * related bottom-up passes).  Either way it is probably only a matter of time
 * (or a matter of further correlated version churn) before all blocks that
 * appear together as a single large batch of favorable blocks get accessed by
 * _some_ bottom-up pass.  Large batches of favorable blocks tend to either
 * appear almost constantly or not even once (it all depends on per-index
 * workload characteristics).
 *
 * Note that the blockgroups sort order applies a power-of-two bucketing
 * scheme that creates opportunities for contiguous groups of blocks to get
 * batched together, at least with workloads that are naturally amenable to
 * being driven by heap block locality.  This doesn't just enhance the spatial
 * locality of bottom-up heap block processing in the obvious way.  It also
 * enables temporal locality of access, since sorting by heap block number
 * naturally tends to make the bottom-up processing order deterministic.
 *
 * Consider the following example to get a sense of how temporal locality
 * might matter: There is a heap relation with several indexes, each of which
 * is low to medium cardinality.  It is subject to constant non-HOT updates.
 * The updates are skewed (in one part of the primary key, perhaps).  None of
 * the indexes are logically modified by the UPDATE statements (if they were
 * then bottom-up index deletion would not be triggered in the first place).
 * Naturally, each new round of index tuples (for each heap tuple that gets a
 * heap_update() call) will have the same heap TID in each and every index.
 * Since these indexes are low cardinality and never get logically modified,
 * heapam processing during bottom-up deletion passes will access heap blocks
 * in approximately sequential order.  Temporal locality of access occurs due
 * to bottom-up deletion passes behaving very similarly across each of the
 * indexes at any given moment.  This keeps the number of buffer misses needed
 * to visit heap blocks to a minimum.
 */
static int
bottomup_nblocksfavorable(IndexDeleteCounts *blockgroups, int nblockgroups,
						  TM_IndexDelete *deltids)
{
	int64		lastblock = -1;
	int			nblocksfavorable = 0;

	Assert(nblockgroups >= 1);
	Assert(nblockgroups <= BOTTOMUP_MAX_NBLOCKS);

	/*
	 * We tolerate heap blocks that will be accessed only slightly out of
	 * physical order.  Small blips occur when a pair of almost-contiguous
	 * blocks happen to fall into different buckets (perhaps due only to a
	 * small difference in npromisingtids that the bucketing scheme didn't
	 * quite manage to ignore).  We effectively ignore these blips by applying
	 * a small tolerance.  The precise tolerance we use is a little arbitrary,
	 * but it works well enough in practice.
	 */
	for (int b = 0; b < nblockgroups; b++)
	{
		IndexDeleteCounts *group = blockgroups + b;
		TM_IndexDelete *firstdtid = deltids + group->ifirsttid;
		BlockNumber block = ItemPointerGetBlockNumber(&firstdtid->tid);

		if (lastblock != -1 &&
			((int64) block < lastblock - BOTTOMUP_TOLERANCE_NBLOCKS ||
			 (int64) block > lastblock + BOTTOMUP_TOLERANCE_NBLOCKS))
			break;

		nblocksfavorable++;
		lastblock = block;
	}

	/* Always indicate that there is at least 1 favorable block */
	Assert(nblocksfavorable >= 1);

	return nblocksfavorable;
}

/*
 * qsort comparison function for bottomup_sort_and_shrink()
 */
static int
bottomup_sort_and_shrink_cmp(const void *arg1, const void *arg2)
{
	const IndexDeleteCounts *group1 = (const IndexDeleteCounts *) arg1;
	const IndexDeleteCounts *group2 = (const IndexDeleteCounts *) arg2;

	/*
	 * Most significant field is npromisingtids (which we invert the order of
	 * so as to sort in desc order).
	 *
	 * Caller should have already normalized npromisingtids fields into
	 * power-of-two values (buckets).
	 */
	if (group1->npromisingtids > group2->npromisingtids)
		return -1;
	if (group1->npromisingtids < group2->npromisingtids)
		return 1;

	/*
	 * Tiebreak: desc ntids sort order.
	 *
	 * We cannot expect power-of-two values for ntids fields.  We should
	 * behave as if they were already rounded up for us instead.
	 */
	if (group1->ntids != group2->ntids)
	{
		uint32		ntids1 = pg_nextpower2_32((uint32) group1->ntids);
		uint32		ntids2 = pg_nextpower2_32((uint32) group2->ntids);

		if (ntids1 > ntids2)
			return -1;
		if (ntids1 < ntids2)
			return 1;
	}

	/*
	 * Tiebreak: asc offset-into-deltids-for-block (offset to first TID for
	 * block in deltids array) order.
	 *
	 * This is equivalent to sorting in ascending heap block number order
	 * (among otherwise equal subsets of the array).  This approach allows us
	 * to avoid accessing the out-of-line TID.  (We rely on the assumption
	 * that the deltids array was sorted in ascending heap TID order when
	 * these offsets to the first TID from each heap block group were formed.)
	 */
	if (group1->ifirsttid > group2->ifirsttid)
		return 1;
	if (group1->ifirsttid < group2->ifirsttid)
		return -1;

	pg_unreachable();

	return 0;
}

/*
 * heap_index_delete_tuples() helper function for bottom-up deletion callers.
 *
 * Sorts deltids array in the order needed for useful processing by bottom-up
 * deletion.  The array should already be sorted in TID order when we're
 * called.  The sort process groups heap TIDs from deltids into heap block
 * groupings.  Earlier/more-promising groups/blocks are usually those that are
 * known to have the most "promising" TIDs.
 *
 * Sets new size of deltids array (ndeltids) in state.  deltids will only have
 * TIDs from the BOTTOMUP_MAX_NBLOCKS most promising heap blocks when we
 * return.  This often means that deltids will be shrunk to a small fraction
 * of its original size (we eliminate many heap blocks from consideration for
 * caller up front).
 *
 * Returns the number of "favorable" blocks.  See bottomup_nblocksfavorable()
 * for a definition and full details.
 */
static int
bottomup_sort_and_shrink(TM_IndexDeleteOp *delstate)
{
	IndexDeleteCounts *blockgroups;
	TM_IndexDelete *reordereddeltids;
	BlockNumber curblock = InvalidBlockNumber;
	int			nblockgroups = 0;
	int			ncopied = 0;
	int			nblocksfavorable = 0;

	Assert(delstate->bottomup);
	Assert(delstate->ndeltids > 0);

	/* Calculate per-heap-block count of TIDs */
	blockgroups = palloc(sizeof(IndexDeleteCounts) * delstate->ndeltids);
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *ideltid = &delstate->deltids[i];
		TM_IndexStatus *istatus = delstate->status + ideltid->id;
		ItemPointer htid = &ideltid->tid;
		bool		promising = istatus->promising;

		if (curblock != ItemPointerGetBlockNumber(htid))
		{
			/* New block group */
			nblockgroups++;

			Assert(curblock < ItemPointerGetBlockNumber(htid) ||
				   !BlockNumberIsValid(curblock));

			curblock = ItemPointerGetBlockNumber(htid);
			blockgroups[nblockgroups - 1].ifirsttid = i;
			blockgroups[nblockgroups - 1].ntids = 1;
			blockgroups[nblockgroups - 1].npromisingtids = 0;
		}
		else
		{
			blockgroups[nblockgroups - 1].ntids++;
		}

		if (promising)
			blockgroups[nblockgroups - 1].npromisingtids++;
	}

	/*
	 * We're about ready to sort block groups to determine the optimal order
	 * for visiting heap blocks.  But before we do, round the number of
	 * promising tuples for each block group up to the next power-of-two,
	 * unless it is very low (less than 4), in which case we round up to 4.
	 * npromisingtids is far too noisy to trust when choosing between a pair
	 * of block groups that both have very low values.
	 *
	 * This scheme divides heap blocks/block groups into buckets.  Each bucket
	 * contains blocks that have _approximately_ the same number of promising
	 * TIDs as each other.  The goal is to ignore relatively small differences
	 * in the total number of promising entries, so that the whole process can
	 * give a little weight to heapam factors (like heap block locality)
	 * instead.  This isn't a trade-off, really -- we have nothing to lose. It
	 * would be foolish to interpret small differences in npromisingtids
	 * values as anything more than noise.
	 *
	 * We tiebreak on nhtids when sorting block group subsets that have the
	 * same npromisingtids, but this has the same issues as npromisingtids,
	 * and so nhtids is subject to the same power-of-two bucketing scheme. The
	 * only reason that we don't fix nhtids in the same way here too is that
	 * we'll need accurate nhtids values after the sort.  We handle nhtids
	 * bucketization dynamically instead (in the sort comparator).
	 *
	 * See bottomup_nblocksfavorable() for a full explanation of when and how
	 * heap locality/favorable blocks can significantly influence when and how
	 * heap blocks are accessed.
	 */
	for (int b = 0; b < nblockgroups; b++)
	{
		IndexDeleteCounts *group = blockgroups + b;

		/* Better off falling back on nhtids with low npromisingtids */
		if (group->npromisingtids <= 4)
			group->npromisingtids = 4;
		else
			group->npromisingtids =
				pg_nextpower2_32((uint32) group->npromisingtids);
	}

	/* Sort groups and rearrange caller's deltids array */
	qsort(blockgroups, nblockgroups, sizeof(IndexDeleteCounts),
		  bottomup_sort_and_shrink_cmp);
	reordereddeltids = palloc(delstate->ndeltids * sizeof(TM_IndexDelete));

	nblockgroups = Min(BOTTOMUP_MAX_NBLOCKS, nblockgroups);
	/* Determine number of favorable blocks at the start of final deltids */
	nblocksfavorable = bottomup_nblocksfavorable(blockgroups, nblockgroups,
												 delstate->deltids);

	for (int b = 0; b < nblockgroups; b++)
	{
		IndexDeleteCounts *group = blockgroups + b;
		TM_IndexDelete *firstdtid = delstate->deltids + group->ifirsttid;

		memcpy(reordereddeltids + ncopied, firstdtid,
			   sizeof(TM_IndexDelete) * group->ntids);
		ncopied += group->ntids;
	}

	/* Copy final grouped and sorted TIDs back into start of caller's array */
	memcpy(delstate->deltids, reordereddeltids,
		   sizeof(TM_IndexDelete) * ncopied);
	delstate->ndeltids = ncopied;

	pfree(reordereddeltids);
	pfree(blockgroups);

	return nblocksfavorable;
}

/*
 * Perform XLogInsert for a heap-freeze operation.  Caller must have already
 * modified the buffer and marked it dirty.
 */
XLogRecPtr
log_heap_freeze(Relation reln, Buffer buffer, TransactionId cutoff_xid,
				xl_heap_freeze_tuple *tuples, int ntuples)
{
	xl_heap_freeze_page xlrec;
	XLogRecPtr	recptr;

	/* Caller should not call me on a non-WAL-logged relation */
	Assert(RelationNeedsWAL(reln));
	/* nor when there are no tuples to freeze */
	Assert(ntuples > 0);

	xlrec.cutoff_xid = cutoff_xid;
	xlrec.ntuples = ntuples;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfHeapFreezePage);

	/*
	 * The freeze plan array is not actually in the buffer, but pretend that
	 * it is.  When XLogInsert stores the whole buffer, the freeze plan need
	 * not be stored too.
	 */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
	XLogRegisterBufData(0, (char *) tuples,
						ntuples * sizeof(xl_heap_freeze_tuple));

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_FREEZE_PAGE);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-visible operation.  'block' is the block
 * being marked all-visible, and vm_buffer is the buffer containing the
 * corresponding visibility map block.  Both should have already been modified
 * and dirtied.
 *
 * If checksums are enabled, we also generate a full-page image of
 * heap_buffer, if necessary.
 */
XLogRecPtr
log_heap_visible(RelFileNode rnode, Buffer heap_buffer, Buffer vm_buffer,
				 TransactionId cutoff_xid, uint8 vmflags)
{
	xl_heap_visible xlrec;
	XLogRecPtr	recptr;
	uint8		flags;

	Assert(BufferIsValid(heap_buffer));
	Assert(BufferIsValid(vm_buffer));

	xlrec.cutoff_xid = cutoff_xid;
	xlrec.flags = vmflags;
	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfHeapVisible);

	XLogRegisterBuffer(0, vm_buffer, 0);

	flags = REGBUF_STANDARD;
	if (!XLogHintBitIsNeeded())
		flags |= REGBUF_NO_IMAGE;
	XLogRegisterBuffer(1, heap_buffer, flags);

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-update operation.  Caller must already
 * have modified the buffer(s) and marked them dirty.
 */
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf,
				Buffer newbuf, HeapTuple oldtup, HeapTuple newtup,
				HeapTuple old_key_tuple,
				bool all_visible_cleared, bool new_all_visible_cleared)
{
	xl_heap_update xlrec;
	xl_heap_header xlhdr;
	xl_heap_header xlhdr_idx;
	uint8		info;
	uint16		prefix_suffix[2];
	uint16		prefixlen = 0,
				suffixlen = 0;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(newbuf);
	bool		need_tuple_data = RelationIsLogicallyLogged(reln);
	bool		init;
	int			bufflags;

	/* Caller should not call me on a non-WAL-logged relation */
	Assert(RelationNeedsWAL(reln));

	XLogBeginInsert();

	if (HeapTupleIsHeapOnly(newtup))
		info = XLOG_HEAP_HOT_UPDATE;
	else
		info = XLOG_HEAP_UPDATE;

	/*
	 * If the old and new tuple are on the same page, we only need to log the
	 * parts of the new tuple that were changed.  That saves on the amount of
	 * WAL we need to write.  Currently, we just count any unchanged bytes in
	 * the beginning and end of the tuple.  That's quick to check, and
	 * perfectly covers the common case that only one field is updated.
	 *
	 * We could do this even if the old and new tuple are on different pages,
	 * but only if we don't make a full-page image of the old page, which is
	 * difficult to know in advance.  Also, if the old tuple is corrupt for
	 * some reason, it would allow the corruption to propagate the new page,
	 * so it seems best to avoid.  Under the general assumption that most
	 * updates tend to create the new tuple version on the same page, there
	 * isn't much to be gained by doing this across pages anyway.
	 *
	 * Skip this if we're taking a full-page image of the new page, as we
	 * don't include the new tuple in the WAL record in that case.  Also
	 * disable if wal_level='logical', as logical decoding needs to be able to
	 * read the new tuple in whole from the WAL record alone.
	 */
	if (oldbuf == newbuf && !need_tuple_data &&
		!XLogCheckBufferNeedsBackup(newbuf))
	{
		char	   *oldp = (char *) oldtup->t_data + oldtup->t_data->t_hoff;
		char	   *newp = (char *) newtup->t_data + newtup->t_data->t_hoff;
		int			oldlen = oldtup->t_len - oldtup->t_data->t_hoff;
		int			newlen = newtup->t_len - newtup->t_data->t_hoff;

		/* Check for common prefix between old and new tuple */
		for (prefixlen = 0; prefixlen < Min(oldlen, newlen); prefixlen++)
		{
			if (newp[prefixlen] != oldp[prefixlen])
				break;
		}

		/*
		 * Storing the length of the prefix takes 2 bytes, so we need to save
		 * at least 3 bytes or there's no point.
		 */
		if (prefixlen < 3)
			prefixlen = 0;

		/* Same for suffix */
		for (suffixlen = 0; suffixlen < Min(oldlen, newlen) - prefixlen; suffixlen++)
		{
			if (newp[newlen - suffixlen - 1] != oldp[oldlen - suffixlen - 1])
				break;
		}
		if (suffixlen < 3)
			suffixlen = 0;
	}

	/* Prepare main WAL data chain */
	xlrec.flags = 0;
	if (all_visible_cleared)
		xlrec.flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
	if (new_all_visible_cleared)
		xlrec.flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
	if (prefixlen > 0)
		xlrec.flags |= XLH_UPDATE_PREFIX_FROM_OLD;
	if (suffixlen > 0)
		xlrec.flags |= XLH_UPDATE_SUFFIX_FROM_OLD;
	if (need_tuple_data)
	{
		xlrec.flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
		if (old_key_tuple)
		{
			if (reln->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
				xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_TUPLE;
			else
				xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_KEY;
		}
	}

	/* If new tuple is the single and first tuple on page... */
	if (ItemPointerGetOffsetNumber(&(newtup->t_self)) == FirstOffsetNumber &&
		PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
	{
		info |= XLOG_HEAP_INIT_PAGE;
		init = true;
	}
	else
		init = false;

	/* Prepare WAL data for the old page */
	xlrec.old_offnum = ItemPointerGetOffsetNumber(&oldtup->t_self);
	xlrec.old_xmax = HeapTupleHeaderGetRawXmax(oldtup->t_data);
	xlrec.old_infobits_set = compute_infobits(oldtup->t_data->t_infomask,
											  oldtup->t_data->t_infomask2);

	/* Prepare WAL data for the new page */
	xlrec.new_offnum = ItemPointerGetOffsetNumber(&newtup->t_self);
	xlrec.new_xmax = HeapTupleHeaderGetRawXmax(newtup->t_data);

	bufflags = REGBUF_STANDARD;
	if (init)
		bufflags |= REGBUF_WILL_INIT;
	if (need_tuple_data)
		bufflags |= REGBUF_KEEP_DATA;

	XLogRegisterBuffer(0, newbuf, bufflags);
	if (oldbuf != newbuf)
		XLogRegisterBuffer(1, oldbuf, REGBUF_STANDARD);

	XLogRegisterData((char *) &xlrec, SizeOfHeapUpdate);

	/*
	 * Prepare WAL data for the new tuple.
	 */
	if (prefixlen > 0 || suffixlen > 0)
	{
		if (prefixlen > 0 && suffixlen > 0)
		{
			prefix_suffix[0] = prefixlen;
			prefix_suffix[1] = suffixlen;
			XLogRegisterBufData(0, (char *) &prefix_suffix, sizeof(uint16) * 2);
		}
		else if (prefixlen > 0)
		{
			XLogRegisterBufData(0, (char *) &prefixlen, sizeof(uint16));
		}
		else
		{
			XLogRegisterBufData(0, (char *) &suffixlen, sizeof(uint16));
		}
	}

	xlhdr.t_infomask2 = newtup->t_data->t_infomask2;
	xlhdr.t_infomask = newtup->t_data->t_infomask;
	xlhdr.t_hoff = newtup->t_data->t_hoff;
	Assert(SizeofHeapTupleHeader + prefixlen + suffixlen <= newtup->t_len);

	/*
	 * PG73FORMAT: write bitmap [+ padding] [+ oid] + data
	 *
	 * The 'data' doesn't include the common prefix or suffix.
	 */
	XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
	if (prefixlen == 0)
	{
		XLogRegisterBufData(0,
							((char *) newtup->t_data) + SizeofHeapTupleHeader,
							newtup->t_len - SizeofHeapTupleHeader - suffixlen);
	}
	else
	{
		/*
		 * Have to write the null bitmap and data after the common prefix as
		 * two separate rdata entries.
		 */
		/* bitmap [+ padding] [+ oid] */
		if (newtup->t_data->t_hoff - SizeofHeapTupleHeader > 0)
		{
			XLogRegisterBufData(0,
								((char *) newtup->t_data) + SizeofHeapTupleHeader,
								newtup->t_data->t_hoff - SizeofHeapTupleHeader);
		}

		/* data after common prefix */
		XLogRegisterBufData(0,
							((char *) newtup->t_data) + newtup->t_data->t_hoff + prefixlen,
							newtup->t_len - newtup->t_data->t_hoff - prefixlen - suffixlen);
	}

	/* We need to log a tuple identity */
	if (need_tuple_data && old_key_tuple)
	{
		/* don't really need this, but its more comfy to decode */
		xlhdr_idx.t_infomask2 = old_key_tuple->t_data->t_infomask2;
		xlhdr_idx.t_infomask = old_key_tuple->t_data->t_infomask;
		xlhdr_idx.t_hoff = old_key_tuple->t_data->t_hoff;

		XLogRegisterData((char *) &xlhdr_idx, SizeOfHeapHeader);

		/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
		XLogRegisterData((char *) old_key_tuple->t_data + SizeofHeapTupleHeader,
						 old_key_tuple->t_len - SizeofHeapTupleHeader);
	}

	/* filtering by origin on a row level is much more efficient */
	XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

	recptr = XLogInsert(RM_HEAP_ID, info);

	return recptr;
}

/*
 * Perform XLogInsert of an XLOG_HEAP2_NEW_CID record
 *
 * This is only used in wal_level >= WAL_LEVEL_LOGICAL, and only for catalog
 * tuples.
 */
static XLogRecPtr
log_heap_new_cid(Relation relation, HeapTuple tup)
{
	xl_heap_new_cid xlrec;

	XLogRecPtr	recptr;
	HeapTupleHeader hdr = tup->t_data;

	Assert(ItemPointerIsValid(&tup->t_self));
	Assert(tup->t_tableOid != InvalidOid);

	xlrec.top_xid = GetTopTransactionId();
	xlrec.target_node = relation->rd_node;
	xlrec.target_tid = tup->t_self;

	/*
	 * If the tuple got inserted & deleted in the same TX we definitely have a
	 * combo CID, set cmin and cmax.
	 */
	if (hdr->t_infomask & HEAP_COMBOCID)
	{
		Assert(!(hdr->t_infomask & HEAP_XMAX_INVALID));
		Assert(!HeapTupleHeaderXminInvalid(hdr));
		xlrec.cmin = HeapTupleHeaderGetCmin(hdr);
		xlrec.cmax = HeapTupleHeaderGetCmax(hdr);
		xlrec.combocid = HeapTupleHeaderGetRawCommandId(hdr);
	}
	/* No combo CID, so only cmin or cmax can be set by this TX */
	else
	{
		/*
		 * Tuple inserted.
		 *
		 * We need to check for LOCK ONLY because multixacts might be
		 * transferred to the new tuple in case of FOR KEY SHARE updates in
		 * which case there will be an xmax, although the tuple just got
		 * inserted.
		 */
		if (hdr->t_infomask & HEAP_XMAX_INVALID ||
			HEAP_XMAX_IS_LOCKED_ONLY(hdr->t_infomask))
		{
			xlrec.cmin = HeapTupleHeaderGetRawCommandId(hdr);
			xlrec.cmax = InvalidCommandId;
		}
		/* Tuple from a different tx updated or deleted. */
		else
		{
			xlrec.cmin = InvalidCommandId;
			xlrec.cmax = HeapTupleHeaderGetRawCommandId(hdr);
		}
		xlrec.combocid = InvalidCommandId;
	}

	/*
	 * Note that we don't need to register the buffer here, because this
	 * operation does not modify the page. The insert/update/delete that
	 * called us certainly did, but that's WAL-logged separately.
	 */
	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfHeapNewCid);

	/* will be looked at irrespective of origin */

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_NEW_CID);

	return recptr;
}

/*
 * Build a heap tuple representing the configured REPLICA IDENTITY to represent
 * the old tuple in an UPDATE or DELETE.
 *
 * Returns NULL if there's no need to log an identity or if there's no suitable
 * key defined.
 *
 * Pass key_required true if any replica identity columns changed value, or if
 * any of them have any external data.  Delete must always pass true.
 *
 * *copy is set to true if the returned tuple is a modified copy rather than
 * the same tuple that was passed in.
 */
static HeapTuple
ExtractReplicaIdentity(Relation relation, HeapTuple tp, bool key_required,
					   bool *copy)
{
	TupleDesc	desc = RelationGetDescr(relation);
	char		replident = relation->rd_rel->relreplident;
	Bitmapset  *idattrs;
	HeapTuple	key_tuple;
	bool		nulls[MaxHeapAttributeNumber];
	Datum		values[MaxHeapAttributeNumber];

	*copy = false;

	if (!RelationIsLogicallyLogged(relation))
		return NULL;

	if (replident == REPLICA_IDENTITY_NOTHING)
		return NULL;

	if (replident == REPLICA_IDENTITY_FULL)
	{
		/*
		 * When logging the entire old tuple, it very well could contain
		 * toasted columns. If so, force them to be inlined.
		 */
		if (HeapTupleHasExternal(tp))
		{
			*copy = true;
			tp = toast_flatten_tuple(tp, desc);
		}
		return tp;
	}

	/* if the key isn't required and we're only logging the key, we're done */
	if (!key_required)
		return NULL;

	/* find out the replica identity columns */
	idattrs = RelationGetIndexAttrBitmap(relation,
										 INDEX_ATTR_BITMAP_IDENTITY_KEY);

	/*
	 * If there's no defined replica identity columns, treat as !key_required.
	 * (This case should not be reachable from heap_update, since that should
	 * calculate key_required accurately.  But heap_delete just passes
	 * constant true for key_required, so we can hit this case in deletes.)
	 */
	if (bms_is_empty(idattrs))
		return NULL;

	/*
	 * Construct a new tuple containing only the replica identity columns,
	 * with nulls elsewhere.  While we're at it, assert that the replica
	 * identity columns aren't null.
	 */
	heap_deform_tuple(tp, desc, values, nulls);

	for (int i = 0; i < desc->natts; i++)
	{
		if (bms_is_member(i + 1 - FirstLowInvalidHeapAttributeNumber,
						  idattrs))
			Assert(!nulls[i]);
		else
			nulls[i] = true;
	}

	key_tuple = heap_form_tuple(desc, values, nulls);
	*copy = true;

	bms_free(idattrs);

	/*
	 * If the tuple, which by here only contains indexed columns, still has
	 * toasted columns, force them to be inlined. This is somewhat unlikely
	 * since there's limits on the size of indexed columns, so we don't
	 * duplicate toast_flatten_tuple()s functionality in the above loop over
	 * the indexed columns, even if it would be more efficient.
	 */
	if (HeapTupleHasExternal(key_tuple))
	{
		HeapTuple	oldtup = key_tuple;

		key_tuple = toast_flatten_tuple(oldtup, desc);
		heap_freetuple(oldtup);
	}

	return key_tuple;
}

/*
 * Handles XLOG_HEAP2_PRUNE record type.
 *
 * Acquires a full cleanup lock.
 */
static void
heap_xlog_prune(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_prune *xlrec = (xl_heap_prune *) XLogRecGetData(record);
	Buffer		buffer;
	RelFileNode rnode;
	BlockNumber blkno;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

	/*
	 * We're about to remove tuples. In Hot Standby mode, ensure that there's
	 * no queries running for which the removed tuples are still visible.
	 */
	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, rnode);

	/*
	 * If we have a full-page image, restore it (using a cleanup lock) and
	 * we're done.
	 */
	action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true,
										   &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(buffer);
		OffsetNumber *end;
		OffsetNumber *redirected;
		OffsetNumber *nowdead;
		OffsetNumber *nowunused;
		int			nredirected;
		int			ndead;
		int			nunused;
		Size		datalen;

		redirected = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

		nredirected = xlrec->nredirected;
		ndead = xlrec->ndead;
		end = (OffsetNumber *) ((char *) redirected + datalen);
		nowdead = redirected + (nredirected * 2);
		nowunused = nowdead + ndead;
		nunused = (end - nowunused);
		Assert(nunused >= 0);

		/* Update all line pointers per the record, and repair fragmentation */
		heap_page_prune_execute(buffer,
								redirected, nredirected,
								nowdead, ndead,
								nowunused, nunused);

		/*
		 * Note: we don't worry about updating the page's prunability hints.
		 * At worst this will cause an extra prune cycle to occur soon.
		 */

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
	{
		Size		freespace = PageGetHeapFreeSpace(BufferGetPage(buffer));

		UnlockReleaseBuffer(buffer);

		/*
		 * After pruning records from a page, it's useful to update the FSM
		 * about it, as it may cause the page become target for insertions
		 * later even if vacuum decides not to visit it (which is possible if
		 * gets marked all-visible.)
		 *
		 * Do this regardless of a full-page image being applied, since the
		 * FSM data is not in the page anyway.
		 */
		XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
	}
}

/*
 * Handles XLOG_HEAP2_VACUUM record type.
 *
 * Acquires an ordinary exclusive lock only.
 */
static void
heap_xlog_vacuum(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_vacuum *xlrec = (xl_heap_vacuum *) XLogRecGetData(record);
	Buffer		buffer;
	BlockNumber blkno;
	XLogRedoAction action;

	/*
	 * If we have a full-page image, restore it	(without using a cleanup lock)
	 * and we're done.
	 */
	action = XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, false,
										   &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(buffer);
		OffsetNumber *nowunused;
		Size		datalen;
		OffsetNumber *offnum;

		nowunused = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

		/* Shouldn't be a record unless there's something to do */
		Assert(xlrec->nunused > 0);

		/* Update all now-unused line pointers */
		offnum = nowunused;
		for (int i = 0; i < xlrec->nunused; i++)
		{
			OffsetNumber off = *offnum++;
			ItemId		lp = PageGetItemId(page, off);

			Assert(ItemIdIsDead(lp) && !ItemIdHasStorage(lp));
			ItemIdSetUnused(lp);
		}

		/* Attempt to truncate line pointer array now */
		PageTruncateLinePointerArray(page);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
	{
		Size		freespace = PageGetHeapFreeSpace(BufferGetPage(buffer));
		RelFileNode rnode;

		XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

		UnlockReleaseBuffer(buffer);

		/*
		 * After vacuuming LP_DEAD items from a page, it's useful to update
		 * the FSM about it, as it may cause the page become target for
		 * insertions later even if vacuum decides not to visit it (which is
		 * possible if gets marked all-visible.)
		 *
		 * Do this regardless of a full-page image being applied, since the
		 * FSM data is not in the page anyway.
		 */
		XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
	}
}

/*
 * Replay XLOG_HEAP2_VISIBLE record.
 *
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear.  If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
static void
heap_xlog_visible(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_visible *xlrec = (xl_heap_visible *) XLogRecGetData(record);
	Buffer		vmbuffer = InvalidBuffer;
	Buffer		buffer;
	Page		page;
	RelFileNode rnode;
	BlockNumber blkno;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 1, &rnode, NULL, &blkno);

	/*
	 * If there are any Hot Standby transactions running that have an xmin
	 * horizon old enough that this page isn't all-visible for them, they
	 * might incorrectly decide that an index-only scan can skip a heap fetch.
	 *
	 * NB: It might be better to throw some kind of "soft" conflict here that
	 * forces any index-only scan that is in flight to perform heap fetches,
	 * rather than killing the transaction outright.
	 */
	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshot(xlrec->cutoff_xid, rnode);

	/*
	 * Read the heap page, if it still exists. If the heap file has dropped or
	 * truncated later in recovery, we don't need to update the page, but we'd
	 * better still update the visibility map.
	 */
	action = XLogReadBufferForRedo(record, 1, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		/*
		 * We don't bump the LSN of the heap page when setting the visibility
		 * map bit (unless checksums or wal_hint_bits is enabled, in which
		 * case we must). This exposes us to torn page hazards, but since
		 * we're not inspecting the existing page contents in any way, we
		 * don't care.
		 *
		 * However, all operations that clear the visibility map bit *do* bump
		 * the LSN, and those operations will only be replayed if the XLOG LSN
		 * follows the page LSN.  Thus, if the page LSN has advanced past our
		 * XLOG record's LSN, we mustn't mark the page all-visible, because
		 * the subsequent update won't be replayed to clear the flag.
		 */
		page = BufferGetPage(buffer);

		PageSetAllVisible(page);

		if (XLogHintBitIsNeeded())
			PageSetLSN(page, lsn);

		MarkBufferDirty(buffer);
	}
	else if (action == BLK_RESTORED)
	{
		/*
		 * If heap block was backed up, we already restored it and there's
		 * nothing more to do. (This can only happen with checksums or
		 * wal_log_hints enabled.)
		 */
	}

	if (BufferIsValid(buffer))
	{
		Size		space = PageGetFreeSpace(BufferGetPage(buffer));

		UnlockReleaseBuffer(buffer);

		/*
		 * Since FSM is not WAL-logged and only updated heuristically, it
		 * easily becomes stale in standbys.  If the standby is later promoted
		 * and runs VACUUM, it will skip updating individual free space
		 * figures for pages that became all-visible (or all-frozen, depending
		 * on the vacuum mode,) which is troublesome when FreeSpaceMapVacuum
		 * propagates too optimistic free space values to upper FSM layers;
		 * later inserters try to use such pages only to find out that they
		 * are unusable.  This can cause long stalls when there are many such
		 * pages.
		 *
		 * Forestall those problems by updating FSM's idea about a page that
		 * is becoming all-visible or all-frozen.
		 *
		 * Do this regardless of a full-page image being applied, since the
		 * FSM data is not in the page anyway.
		 */
		if (xlrec->flags & VISIBILITYMAP_VALID_BITS)
			XLogRecordPageWithFreeSpace(rnode, blkno, space);
	}

	/*
	 * Even if we skipped the heap page update due to the LSN interlock, it's
	 * still safe to update the visibility map.  Any WAL record that clears
	 * the visibility map bit does so before checking the page LSN, so any
	 * bits that need to be cleared will still be cleared.
	 */
	if (XLogReadBufferForRedoExtended(record, 0, RBM_ZERO_ON_ERROR, false,
									  &vmbuffer) == BLK_NEEDS_REDO)
	{
		Page		vmpage = BufferGetPage(vmbuffer);
		Relation	reln;

		/* initialize the page if it was read as zeros */
		if (PageIsNew(vmpage))
			PageInit(vmpage, BLCKSZ, 0);

		/*
		 * XLogReadBufferForRedoExtended locked the buffer. But
		 * visibilitymap_set will handle locking itself.
		 */
		LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);

		reln = CreateFakeRelcacheEntry(rnode);
		visibilitymap_pin(reln, blkno, &vmbuffer);

		/*
		 * Don't set the bit if replay has already passed this point.
		 *
		 * It might be safe to do this unconditionally; if replay has passed
		 * this point, we'll replay at least as far this time as we did
		 * before, and if this bit needs to be cleared, the record responsible
		 * for doing so should be again replayed, and clear it.  For right
		 * now, out of an abundance of conservatism, we use the same test here
		 * we did for the heap page.  If this results in a dropped bit, no
		 * real harm is done; and the next VACUUM will fix it.
		 */
		if (lsn > PageGetLSN(vmpage))
			visibilitymap_set(reln, blkno, InvalidBuffer, lsn, vmbuffer,
							  xlrec->cutoff_xid, xlrec->flags);

		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}
	else if (BufferIsValid(vmbuffer))
		UnlockReleaseBuffer(vmbuffer);
}

/*
 * Replay XLOG_HEAP2_FREEZE_PAGE records
 */
static void
heap_xlog_freeze_page(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_freeze_page *xlrec = (xl_heap_freeze_page *) XLogRecGetData(record);
	TransactionId cutoff_xid = xlrec->cutoff_xid;
	Buffer		buffer;
	int			ntup;

	/*
	 * In Hot Standby mode, ensure that there's no queries running which still
	 * consider the frozen xids as running.
	 */
	if (InHotStandby)
	{
		RelFileNode rnode;
		TransactionId latestRemovedXid = cutoff_xid;

		TransactionIdRetreat(latestRemovedXid);

		XLogRecGetBlockTag(record, 0, &rnode, NULL, NULL);
		ResolveRecoveryConflictWithSnapshot(latestRemovedXid, rnode);
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		xl_heap_freeze_tuple *tuples;

		tuples = (xl_heap_freeze_tuple *) XLogRecGetBlockData(record, 0, NULL);

		/* now execute freeze plan for each frozen tuple */
		for (ntup = 0; ntup < xlrec->ntuples; ntup++)
		{
			xl_heap_freeze_tuple *xlrec_tp;
			ItemId		lp;
			HeapTupleHeader tuple;

			xlrec_tp = &tuples[ntup];
			lp = PageGetItemId(page, xlrec_tp->offset); /* offsets are one-based */
			tuple = (HeapTupleHeader) PageGetItem(page, lp);

			heap_execute_freeze_tuple(tuple, xlrec_tp);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Given an "infobits" field from an XLog record, set the correct bits in the
 * given infomask and infomask2 for the tuple touched by the record.
 *
 * (This is the reverse of compute_infobits).
 */
static void
fix_infomask_from_infobits(uint8 infobits, uint16 *infomask, uint16 *infomask2)
{
	*infomask &= ~(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
				   HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	*infomask2 &= ~HEAP_KEYS_UPDATED;

	if (infobits & XLHL_XMAX_IS_MULTI)
		*infomask |= HEAP_XMAX_IS_MULTI;
	if (infobits & XLHL_XMAX_LOCK_ONLY)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
	if (infobits & XLHL_XMAX_EXCL_LOCK)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	/* note HEAP_XMAX_SHR_LOCK isn't considered here */
	if (infobits & XLHL_XMAX_KEYSHR_LOCK)
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;

	if (infobits & XLHL_KEYS_UPDATED)
		*infomask2 |= HEAP_KEYS_UPDATED;
}

static void
heap_xlog_delete(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BlockNumber blkno;
	RelFileNode target_node;
	ItemPointerData target_tid;

	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(target_node);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(buffer);

		if (PageGetMaxOffsetNumber(page) >= xlrec->offnum)
			lp = PageGetItemId(page, xlrec->offnum);

		if (PageGetMaxOffsetNumber(page) < xlrec->offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		HeapTupleHeaderClearHotUpdated(htup);
		fix_infomask_from_infobits(xlrec->infobits_set,
								   &htup->t_infomask, &htup->t_infomask2);
		if (!(xlrec->flags & XLH_DELETE_IS_SUPER))
			HeapTupleHeaderSetXmax(htup, xlrec->xmax);
		else
			HeapTupleHeaderSetXmin(htup, InvalidTransactionId);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

		/* Mark the page as a candidate for pruning */
		PageSetPrunable(page, XLogRecGetXid(record));

		if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		/* Make sure t_ctid is set correctly */
		if (xlrec->flags & XLH_DELETE_IS_PARTITION_MOVE)
			HeapTupleHeaderSetMovedPartitions(htup);
		else
			htup->t_ctid = target_tid;
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_insert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	uint32		newlen;
	Size		freespace = 0;
	RelFileNode target_node;
	BlockNumber blkno;
	ItemPointerData target_tid;
	XLogRedoAction action;

	XLogRecGetBlockTag(record, 0, &target_node, NULL, &blkno);
	ItemPointerSetBlockNumber(&target_tid, blkno);
	ItemPointerSetOffsetNumber(&target_tid, xlrec->offnum);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(target_node);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/*
	 * If we inserted the first and only tuple on the page, re-initialize the
	 * page from scratch.
	 */
	if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogInitBufferForRedo(record, 0);
		page = BufferGetPage(buffer);
		PageInit(page, BufferGetPageSize(buffer), 0);
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 0, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		Size		datalen;
		char	   *data;

		page = BufferGetPage(buffer);

		if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
			elog(PANIC, "invalid max offset number");

		data = XLogRecGetBlockData(record, 0, &datalen);

		newlen = datalen - SizeOfHeapHeader;
		Assert(datalen > SizeOfHeapHeader && newlen <= MaxHeapTupleSize);
		memcpy((char *) &xlhdr, data, SizeOfHeapHeader);
		data += SizeOfHeapHeader;

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + SizeofHeapTupleHeader,
			   data,
			   newlen);
		newlen += SizeofHeapTupleHeader;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;
		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		htup->t_ctid = target_tid;

		if (PageAddItem(page, (Item) htup, newlen, xlrec->offnum,
						true, true) == InvalidOffsetNumber)
			elog(PANIC, "failed to add tuple");

		freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

		PageSetLSN(page, lsn);

		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
		if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
			PageSetAllVisible(page);

		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * XXX: Don't do this if the page was restored from full page image. We
	 * don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(target_node, blkno, freespace);
}

/*
 * Handles MULTI_INSERT record type.
 */
static void
heap_xlog_multi_insert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_multi_insert *xlrec;
	RelFileNode rnode;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	uint32		newlen;
	Size		freespace = 0;
	int			i;
	bool		isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
	XLogRedoAction action;

	/*
	 * Insertion doesn't overwrite MVCC data, so no conflict processing is
	 * required.
	 */
	xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

	/* check that the mutually exclusive flags are not both set */
	Assert(!((xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) &&
			 (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)));

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(rnode);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	if (isinit)
	{
		buffer = XLogInitBufferForRedo(record, 0);
		page = BufferGetPage(buffer);
		PageInit(page, BufferGetPageSize(buffer), 0);
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 0, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		char	   *tupdata;
		char	   *endptr;
		Size		len;

		/* Tuples are stored as block data */
		tupdata = XLogRecGetBlockData(record, 0, &len);
		endptr = tupdata + len;

		page = (Page) BufferGetPage(buffer);

		for (i = 0; i < xlrec->ntuples; i++)
		{
			OffsetNumber offnum;
			xl_multi_insert_tuple *xlhdr;

			/*
			 * If we're reinitializing the page, the tuples are stored in
			 * order from FirstOffsetNumber. Otherwise there's an array of
			 * offsets in the WAL record, and the tuples come after that.
			 */
			if (isinit)
				offnum = FirstOffsetNumber + i;
			else
				offnum = xlrec->offsets[i];
			if (PageGetMaxOffsetNumber(page) + 1 < offnum)
				elog(PANIC, "invalid max offset number");

			xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
			tupdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

			newlen = xlhdr->datalen;
			Assert(newlen <= MaxHeapTupleSize);
			htup = &tbuf.hdr;
			MemSet((char *) htup, 0, SizeofHeapTupleHeader);
			/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
			memcpy((char *) htup + SizeofHeapTupleHeader,
				   (char *) tupdata,
				   newlen);
			tupdata += newlen;

			newlen += SizeofHeapTupleHeader;
			htup->t_infomask2 = xlhdr->t_infomask2;
			htup->t_infomask = xlhdr->t_infomask;
			htup->t_hoff = xlhdr->t_hoff;
			HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
			HeapTupleHeaderSetCmin(htup, FirstCommandId);
			ItemPointerSetBlockNumber(&htup->t_ctid, blkno);
			ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

			offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
			if (offnum == InvalidOffsetNumber)
				elog(PANIC, "failed to add tuple");
		}
		if (tupdata != endptr)
			elog(PANIC, "total tuple length mismatch");

		freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

		PageSetLSN(page, lsn);

		if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		/* XLH_INSERT_ALL_FROZEN_SET implies that all tuples are visible */
		if (xlrec->flags & XLH_INSERT_ALL_FROZEN_SET)
			PageSetAllVisible(page);

		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * XXX: Don't do this if the page was restored from full page image. We
	 * don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
}

/*
 * Handles UPDATE and HOT_UPDATE
 */
static void
heap_xlog_update(XLogReaderState *record, bool hot_update)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	RelFileNode rnode;
	BlockNumber oldblk;
	BlockNumber newblk;
	ItemPointerData newtid;
	Buffer		obuffer,
				nbuffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleData oldtup;
	HeapTupleHeader htup;
	uint16		prefixlen = 0,
				suffixlen = 0;
	char	   *newp;
	union
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header xlhdr;
	uint32		newlen;
	Size		freespace = 0;
	XLogRedoAction oldaction;
	XLogRedoAction newaction;

	/* initialize to keep the compiler quiet */
	oldtup.t_data = NULL;
	oldtup.t_len = 0;

	XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
	if (XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &oldblk, NULL))
	{
		/* HOT updates are never done across pages */
		Assert(!hot_update);
	}
	else
		oldblk = newblk;

	ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(rnode);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, oldblk, &vmbuffer);
		visibilitymap_clear(reln, oldblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/*
	 * In normal operation, it is important to lock the two pages in
	 * page-number order, to avoid possible deadlocks against other update
	 * operations going the other way.  However, during WAL replay there can
	 * be no other update happening, so we don't need to worry about that. But
	 * we *do* need to worry that we don't expose an inconsistent state to Hot
	 * Standby queries --- so the original page can't be unlocked before we've
	 * added the new tuple to the new page.
	 */

	/* Deal with old tuple version */
	oldaction = XLogReadBufferForRedo(record, (oldblk == newblk) ? 0 : 1,
									  &obuffer);
	if (oldaction == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(obuffer);
		offnum = xlrec->old_offnum;
		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		oldtup.t_data = htup;
		oldtup.t_len = ItemIdGetLength(lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		if (hot_update)
			HeapTupleHeaderSetHotUpdated(htup);
		else
			HeapTupleHeaderClearHotUpdated(htup);
		fix_infomask_from_infobits(xlrec->old_infobits_set, &htup->t_infomask,
								   &htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->old_xmax);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		/* Set forward chain link in t_ctid */
		htup->t_ctid = newtid;

		/* Mark the page as a candidate for pruning */
		PageSetPrunable(page, XLogRecGetXid(record));

		if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		PageSetLSN(page, lsn);
		MarkBufferDirty(obuffer);
	}

	/*
	 * Read the page the new tuple goes into, if different from old.
	 */
	if (oldblk == newblk)
	{
		nbuffer = obuffer;
		newaction = oldaction;
	}
	else if (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE)
	{
		nbuffer = XLogInitBufferForRedo(record, 0);
		page = (Page) BufferGetPage(nbuffer);
		PageInit(page, BufferGetPageSize(nbuffer), 0);
		newaction = BLK_NEEDS_REDO;
	}
	else
		newaction = XLogReadBufferForRedo(record, 0, &nbuffer);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
	{
		Relation	reln = CreateFakeRelcacheEntry(rnode);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, newblk, &vmbuffer);
		visibilitymap_clear(reln, newblk, vmbuffer, VISIBILITYMAP_VALID_BITS);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/* Deal with new tuple */
	if (newaction == BLK_NEEDS_REDO)
	{
		char	   *recdata;
		char	   *recdata_end;
		Size		datalen;
		Size		tuplen;

		recdata = XLogRecGetBlockData(record, 0, &datalen);
		recdata_end = recdata + datalen;

		page = BufferGetPage(nbuffer);

		offnum = xlrec->new_offnum;
		if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			elog(PANIC, "invalid max offset number");

		if (xlrec->flags & XLH_UPDATE_PREFIX_FROM_OLD)
		{
			Assert(newblk == oldblk);
			memcpy(&prefixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}
		if (xlrec->flags & XLH_UPDATE_SUFFIX_FROM_OLD)
		{
			Assert(newblk == oldblk);
			memcpy(&suffixlen, recdata, sizeof(uint16));
			recdata += sizeof(uint16);
		}

		memcpy((char *) &xlhdr, recdata, SizeOfHeapHeader);
		recdata += SizeOfHeapHeader;

		tuplen = recdata_end - recdata;
		Assert(tuplen <= MaxHeapTupleSize);

		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, SizeofHeapTupleHeader);

		/*
		 * Reconstruct the new tuple using the prefix and/or suffix from the
		 * old tuple, and the data stored in the WAL record.
		 */
		newp = (char *) htup + SizeofHeapTupleHeader;
		if (prefixlen > 0)
		{
			int			len;

			/* copy bitmap [+ padding] [+ oid] from WAL record */
			len = xlhdr.t_hoff - SizeofHeapTupleHeader;
			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;

			/* copy prefix from old tuple */
			memcpy(newp, (char *) oldtup.t_data + oldtup.t_data->t_hoff, prefixlen);
			newp += prefixlen;

			/* copy new tuple data from WAL record */
			len = tuplen - (xlhdr.t_hoff - SizeofHeapTupleHeader);
			memcpy(newp, recdata, len);
			recdata += len;
			newp += len;
		}
		else
		{
			/*
			 * copy bitmap [+ padding] [+ oid] + data from record, all in one
			 * go
			 */
			memcpy(newp, recdata, tuplen);
			recdata += tuplen;
			newp += tuplen;
		}
		Assert(recdata == recdata_end);

		/* copy suffix from old tuple */
		if (suffixlen > 0)
			memcpy(newp, (char *) oldtup.t_data + oldtup.t_len - suffixlen, suffixlen);

		newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
		htup->t_infomask2 = xlhdr.t_infomask2;
		htup->t_infomask = xlhdr.t_infomask;
		htup->t_hoff = xlhdr.t_hoff;

		HeapTupleHeaderSetXmin(htup, XLogRecGetXid(record));
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		HeapTupleHeaderSetXmax(htup, xlrec->new_xmax);
		/* Make sure there is no forward chain link in t_ctid */
		htup->t_ctid = newtid;

		offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
		if (offnum == InvalidOffsetNumber)
			elog(PANIC, "failed to add tuple");

		if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED)
			PageClearAllVisible(page);

		freespace = PageGetHeapFreeSpace(page); /* needed to update FSM below */

		PageSetLSN(page, lsn);
		MarkBufferDirty(nbuffer);
	}

	if (BufferIsValid(nbuffer) && nbuffer != obuffer)
		UnlockReleaseBuffer(nbuffer);
	if (BufferIsValid(obuffer))
		UnlockReleaseBuffer(obuffer);

	/*
	 * If the new page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * However, don't update the FSM on HOT updates, because after crash
	 * recovery, either the old or the new tuple will certainly be dead and
	 * prunable. After pruning, the page will have roughly as much free space
	 * as it did before the update, assuming the new tuple is about the same
	 * size as the old one.
	 *
	 * XXX: Don't do this if the page was restored from full page image. We
	 * don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (newaction == BLK_NEEDS_REDO && !hot_update && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(rnode, newblk, freespace);
}

static void
heap_xlog_confirm(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_confirm *xlrec = (xl_heap_confirm *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(buffer);

		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		/*
		 * Confirm tuple as actually inserted
		 */
		ItemPointerSet(&htup->t_ctid, BufferGetBlockNumber(buffer), offnum);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_lock(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
	{
		RelFileNode rnode;
		Buffer		vmbuffer = InvalidBuffer;
		BlockNumber block;
		Relation	reln;

		XLogRecGetBlockTag(record, 0, &rnode, NULL, &block);
		reln = CreateFakeRelcacheEntry(rnode);

		visibilitymap_pin(reln, block, &vmbuffer);
		visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		page = (Page) BufferGetPage(buffer);

		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
								   &htup->t_infomask2);

		/*
		 * Clear relevant update flags, but only if the modified infomask says
		 * there's no update.
		 */
		if (HEAP_XMAX_IS_LOCKED_ONLY(htup->t_infomask))
		{
			HeapTupleHeaderClearHotUpdated(htup);
			/* Make sure there is no forward chain link in t_ctid */
			ItemPointerSet(&htup->t_ctid,
						   BufferGetBlockNumber(buffer),
						   offnum);
		}
		HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
		HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_lock_updated(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_lock_updated *xlrec;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	xlrec = (xl_heap_lock_updated *) XLogRecGetData(record);

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->flags & XLH_LOCK_ALL_FROZEN_CLEARED)
	{
		RelFileNode rnode;
		Buffer		vmbuffer = InvalidBuffer;
		BlockNumber block;
		Relation	reln;

		XLogRecGetBlockTag(record, 0, &rnode, NULL, &block);
		reln = CreateFakeRelcacheEntry(rnode);

		visibilitymap_pin(reln, block, &vmbuffer);
		visibilitymap_clear(reln, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN);

		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(buffer);

		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		htup->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
		htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		fix_infomask_from_infobits(xlrec->infobits_set, &htup->t_infomask,
								   &htup->t_infomask2);
		HeapTupleHeaderSetXmax(htup, xlrec->xmax);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_inplace(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	Size		newlen;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		char	   *newtup = XLogRecGetBlockData(record, 0, &newlen);

		page = BufferGetPage(buffer);

		offnum = xlrec->offnum;
		if (PageGetMaxOffsetNumber(page) >= offnum)
			lp = PageGetItemId(page, offnum);

		if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
			elog(PANIC, "invalid lp");

		htup = (HeapTupleHeader) PageGetItem(page, lp);

		oldlen = ItemIdGetLength(lp) - htup->t_hoff;
		if (oldlen != newlen)
			elog(PANIC, "wrong tuple length");

		memcpy((char *) htup + htup->t_hoff, newtup, newlen);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

void
heap_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/*
	 * These operations don't overwrite MVCC data so no conflict processing is
	 * required. The ones in heap2 rmgr do.
	 */

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			heap_xlog_insert(record);
			break;
		case XLOG_HEAP_DELETE:
			heap_xlog_delete(record);
			break;
		case XLOG_HEAP_UPDATE:
			heap_xlog_update(record, false);
			break;
		case XLOG_HEAP_TRUNCATE:

			/*
			 * TRUNCATE is a no-op because the actions are already logged as
			 * SMGR WAL records.  TRUNCATE WAL record only exists for logical
			 * decoding.
			 */
			break;
		case XLOG_HEAP_HOT_UPDATE:
			heap_xlog_update(record, true);
			break;
		case XLOG_HEAP_CONFIRM:
			heap_xlog_confirm(record);
			break;
		case XLOG_HEAP_LOCK:
			heap_xlog_lock(record);
			break;
		case XLOG_HEAP_INPLACE:
			heap_xlog_inplace(record);
			break;
		default:
			elog(PANIC, "heap_redo: unknown op code %u", info);
	}
}

void
heap2_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_PRUNE:
			heap_xlog_prune(record);
			break;
		case XLOG_HEAP2_VACUUM:
			heap_xlog_vacuum(record);
			break;
		case XLOG_HEAP2_FREEZE_PAGE:
			heap_xlog_freeze_page(record);
			break;
		case XLOG_HEAP2_VISIBLE:
			heap_xlog_visible(record);
			break;
		case XLOG_HEAP2_MULTI_INSERT:
			heap_xlog_multi_insert(record);
			break;
		case XLOG_HEAP2_LOCK_UPDATED:
			heap_xlog_lock_updated(record);
			break;
		case XLOG_HEAP2_NEW_CID:

			/*
			 * Nothing to do on a real replay, only used during logical
			 * decoding.
			 */
			break;
		case XLOG_HEAP2_REWRITE:
			heap_xlog_logical_rewrite(record);
			break;
		default:
			elog(PANIC, "heap2_redo: unknown op code %u", info);
	}
}

/*
 * Mask a heap page before performing consistency checks on it.
 */
void
heap_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;
	OffsetNumber off;

	mask_page_lsn_and_checksum(page);

	mask_page_hint_bits(page);
	mask_unused_space(page);

	for (off = 1; off <= PageGetMaxOffsetNumber(page); off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		char	   *page_item;

		page_item = (char *) (page + ItemIdGetOffset(iid));

		if (ItemIdIsNormal(iid))
		{
			HeapTupleHeader page_htup = (HeapTupleHeader) page_item;

			/*
			 * If xmin of a tuple is not yet frozen, we should ignore
			 * differences in hint bits, since they can be set without
			 * emitting WAL.
			 */
			if (!HeapTupleHeaderXminFrozen(page_htup))
				page_htup->t_infomask &= ~HEAP_XACT_MASK;
			else
			{
				/* Still we need to mask xmax hint bits. */
				page_htup->t_infomask &= ~HEAP_XMAX_INVALID;
				page_htup->t_infomask &= ~HEAP_XMAX_COMMITTED;
			}

			/*
			 * During replay, we set Command Id to FirstCommandId. Hence, mask
			 * it. See heap_xlog_insert() for details.
			 */
			page_htup->t_choice.t_heap.t_field3.t_cid = MASK_MARKER;

			/*
			 * For a speculative tuple, heap_insert() does not set ctid in the
			 * caller-passed heap tuple itself, leaving the ctid field to
			 * contain a speculative token value - a per-backend monotonically
			 * increasing identifier. Besides, it does not WAL-log ctid under
			 * any circumstances.
			 *
			 * During redo, heap_xlog_insert() sets t_ctid to current block
			 * number and self offset number. It doesn't care about any
			 * speculative insertions on the primary. Hence, we set t_ctid to
			 * current block number and self offset number to ignore any
			 * inconsistency.
			 */
			if (HeapTupleHeaderIsSpeculative(page_htup))
				ItemPointerSet(&page_htup->t_ctid, blkno, off);

			/*
			 * NB: Not ignoring ctid changes due to the tuple having moved
			 * (i.e. HeapTupleHeaderIndicatesMovedPartitions), because that's
			 * important information that needs to be in-sync between primary
			 * and standby, and thus is WAL logged.
			 */
		}

		/*
		 * Ignore any padding bytes after the tuple, when the length of the
		 * item is not MAXALIGNed.
		 */
		if (ItemIdHasStorage(iid))
		{
			int			len = ItemIdGetLength(iid);
			int			padlen = MAXALIGN(len) - len;

			if (padlen > 0)
				memset(page_item + len, MASK_MARKER, padlen);
		}
	}
}

/*
 * HeapCheckForSerializableConflictOut
 *		We are reading a tuple.  If it's not visible, there may be a
 *		rw-conflict out with the inserter.  Otherwise, if it is visible to us
 *		but has been deleted, there may be a rw-conflict out with the deleter.
 *
 * We will determine the top level xid of the writing transaction with which
 * we may be in conflict, and ask CheckForSerializableConflictOut() to check
 * for overlap with our own transaction.
 *
 * This function should be called just about anywhere in heapam.c where a
 * tuple has been read. The caller must hold at least a shared lock on the
 * buffer, because this function might set hint bits on the tuple. There is
 * currently no known reason to call this function from an index AM.
 */
void
HeapCheckForSerializableConflictOut(bool visible, Relation relation,
									HeapTuple tuple, Buffer buffer,
									Snapshot snapshot)
{
	TransactionId xid;
	HTSV_Result htsvResult;

	if (!CheckForSerializableConflictOutNeeded(relation, snapshot))
		return;

	/*
	 * Check to see whether the tuple has been written to by a concurrent
	 * transaction, either to create it not visible to us, or to delete it
	 * while it is visible to us.  The "visible" bool indicates whether the
	 * tuple is visible to us, while HeapTupleSatisfiesVacuum checks what else
	 * is going on with it.
	 *
	 * In the event of a concurrently inserted tuple that also happens to have
	 * been concurrently updated (by a separate transaction), the xmin of the
	 * tuple will be used -- not the updater's xid.
	 */
	htsvResult = HeapTupleSatisfiesVacuum(tuple, TransactionXmin, buffer);
	switch (htsvResult)
	{
		case HEAPTUPLE_LIVE:
			if (visible)
				return;
			xid = HeapTupleHeaderGetXmin(tuple->t_data);
			break;
		case HEAPTUPLE_RECENTLY_DEAD:
		case HEAPTUPLE_DELETE_IN_PROGRESS:
			if (visible)
				xid = HeapTupleHeaderGetUpdateXid(tuple->t_data);
			else
				xid = HeapTupleHeaderGetXmin(tuple->t_data);

			if (TransactionIdPrecedes(xid, TransactionXmin))
			{
				/* This is like the HEAPTUPLE_DEAD case */
				Assert(!visible);
				return;
			}
			break;
		case HEAPTUPLE_INSERT_IN_PROGRESS:
			xid = HeapTupleHeaderGetXmin(tuple->t_data);
			break;
		case HEAPTUPLE_DEAD:
			Assert(!visible);
			return;
		default:

			/*
			 * The only way to get to this default clause is if a new value is
			 * added to the enum type without adding it to this switch
			 * statement.  That's a bug, so elog.
			 */
			elog(ERROR, "unrecognized return value from HeapTupleSatisfiesVacuum: %u", htsvResult);

			/*
			 * In spite of having all enum values covered and calling elog on
			 * this default, some compilers think this is a code path which
			 * allows xid to be used below without initialization. Silence
			 * that warning.
			 */
			xid = InvalidTransactionId;
	}

	Assert(TransactionIdIsValid(xid));
	Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

	/*
	 * Find top level xid.  Bail out if xid is too early to be a conflict, or
	 * if it's our own xid.
	 */
	if (TransactionIdEquals(xid, GetTopTransactionIdIfAny()))
		return;
	xid = SubTransGetTopmostTransaction(xid);
	if (TransactionIdPrecedes(xid, TransactionXmin))
		return;

	CheckForSerializableConflictOut(relation, xid, snapshot);
}

#ifdef LOCATOR
/* TODO for index */
#if 0
/*
 * locatoram implementation of tableam's index_delete_tuples interface.
 *
 * This helper function is called by index AMs during index tuple deletion.
 * See tableam header comments for an explanation of the interface implemented
 * here and a general theory of operation.  Note that each call here is either
 * a simple index deletion call, or a bottom-up index deletion call.
 *
 * It's possible for this to generate a fair amount of I/O, since we may be
 * deleting hundreds of tuples from a single index block.  To amortize that
 * cost to some degree, this uses prefetching and combines repeat accesses to
 * the same heap block.
 */
TransactionId
locator_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	/* Initial assumption is that earlier pruning took care of conflict */
	TransactionId latestRemovedXid = InvalidTransactionId;
	BlockNumber blkno = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page = NULL;
	OffsetNumber maxoff = InvalidOffsetNumber;
	TransactionId priorXmax;
#ifdef USE_PREFETCH
	IndexDeletePrefetchState prefetch_state;
	int			prefetch_distance;
#endif
	SnapshotData SnapshotNonVacuumable;
	int			finalndeltids = 0,
				nblocksaccessed = 0;

	/* State that's only used in bottom-up index deletion case */
	int			nblocksfavorable = 0;
	int			curtargetfreespace = delstate->bottomupfreespace,
				lastfreespace = 0,
				actualfreespace = 0;
	bool		bottomup_final_block = false;

	DualRefDescData	drefDesc;
	HeapTuple		tmp_tup = NULL;

	InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel));

	/* Sort caller's deltids array by TID for further processing */
	index_delete_sort(delstate);

	/*
	 * Bottom-up case: resort deltids array in an order attuned to where the
	 * greatest number of promising TIDs are to be found, and determine how
	 * many blocks from the start of sorted array should be considered
	 * favorable.  This will also shrink the deltids array in order to
	 * eliminate completely unfavorable blocks up front.
	 */
	if (delstate->bottomup)
		nblocksfavorable = bottomup_sort_and_shrink(delstate);

#ifdef USE_PREFETCH
	/* Initialize prefetch state. */
	prefetch_state.cur_hblkno = InvalidBlockNumber;
	prefetch_state.next_item = 0;
	prefetch_state.ndeltids = delstate->ndeltids;
	prefetch_state.deltids = delstate->deltids;

	/*
	 * Determine the prefetch distance that we will attempt to maintain.
	 *
	 * Since the caller holds a buffer lock somewhere in rel, we'd better make
	 * sure that isn't a catalog relation before we call code that does
	 * syscache lookups, to avoid risk of deadlock.
	 */
	if (IsCatalogRelation(rel))
		prefetch_distance = maintenance_io_concurrency;
	else
		prefetch_distance =
			get_tablespace_maintenance_io_concurrency(rel->rd_rel->reltablespace);

	/* Cap initial prefetch distance for bottom-up deletion caller */
	if (delstate->bottomup)
	{
		Assert(nblocksfavorable >= 1);
		Assert(nblocksfavorable <= BOTTOMUP_MAX_NBLOCKS);
		prefetch_distance = Min(prefetch_distance, nblocksfavorable);
	}

	/* Start prefetching. */
	index_delete_prefetch_buffer(rel, &prefetch_state, prefetch_distance);
#endif

	/* Iterate over deltids, determine which to delete, check their horizon */
	Assert(delstate->ndeltids > 0);
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *ideltid = &delstate->deltids[i];
		TM_IndexStatus *istatus = delstate->status + ideltid->id;
		ItemPointer htid = &ideltid->tid;
		OffsetNumber offnum;

		/*
		 * Read buffer, and perform required extra steps each time a new block
		 * is encountered.  Avoid refetching if it's the same block as the one
		 * from the last htid.
		 */
		if (blkno == InvalidBlockNumber ||
			ItemPointerGetBlockNumber(htid) != blkno)
		{
			/*
			 * Consider giving up early for bottom-up index deletion caller
			 * first. (Only prefetch next-next block afterwards, when it
			 * becomes clear that we're at least going to access the next
			 * block in line.)
			 *
			 * Sometimes the first block frees so much space for bottom-up
			 * caller that the deletion process can end without accessing any
			 * more blocks.  It is usually necessary to access 2 or 3 blocks
			 * per bottom-up deletion operation, though.
			 */
			if (delstate->bottomup)
			{
				/*
				 * We often allow caller to delete a few additional items
				 * whose entries we reached after the point that space target
				 * from caller was satisfied.  The cost of accessing the page
				 * was already paid at that point, so it made sense to finish
				 * it off.  When that happened, we finalize everything here
				 * (by finishing off the whole bottom-up deletion operation
				 * without needlessly paying the cost of accessing any more
				 * blocks).
				 */
				if (bottomup_final_block)
					break;

				/*
				 * Give up when we didn't enable our caller to free any
				 * additional space as a result of processing the page that we
				 * just finished up with.  This rule is the main way in which
				 * we keep the cost of bottom-up deletion under control.
				 */
				if (nblocksaccessed >= 1 && actualfreespace == lastfreespace)
					break;
				lastfreespace = actualfreespace;	/* for next time */

				/*
				 * Deletion operation (which is bottom-up) will definitely
				 * access the next block in line.  Prepare for that now.
				 *
				 * Decay target free space so that we don't hang on for too
				 * long with a marginal case. (Space target is only truly
				 * helpful when it allows us to recognize that we don't need
				 * to access more than 1 or 2 blocks to satisfy caller due to
				 * agreeable workload characteristics.)
				 *
				 * We are a bit more patient when we encounter contiguous
				 * blocks, though: these are treated as favorable blocks.  The
				 * decay process is only applied when the next block in line
				 * is not a favorable/contiguous block.  This is not an
				 * exception to the general rule; we still insist on finding
				 * at least one deletable item per block accessed.  See
				 * bottomup_nblocksfavorable() for full details of the theory
				 * behind favorable blocks and heap block locality in general.
				 *
				 * Note: The first block in line is always treated as a
				 * favorable block, so the earliest possible point that the
				 * decay can be applied is just before we access the second
				 * block in line.  The Assert() verifies this for us.
				 */
				Assert(nblocksaccessed > 0 || nblocksfavorable > 0);
				if (nblocksfavorable > 0)
					nblocksfavorable--;
				else
					curtargetfreespace /= 2;
			}

			/* release old buffer */
			if (BufferIsValid(buf))
			{
#ifdef USING_LOCK
				UnlockReleaseBuffer(buf);
#else
				DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
				ReleaseBuffer(buf);
#endif
			}

			blkno = ItemPointerGetBlockNumber(htid);
			buf = ReadBuffer(rel, blkno);
			nblocksaccessed++;
			Assert(!delstate->bottomup ||
				   nblocksaccessed <= BOTTOMUP_MAX_NBLOCKS);

#ifdef USE_PREFETCH

			/*
			 * To maintain the prefetch distance, prefetch one more page for
			 * each page we read.
			 */
			index_delete_prefetch_buffer(rel, &prefetch_state, 1);
#endif

#ifdef USING_LOCK
			drefDesc.dual_ref = NULL;
			LockBuffer(buf, BUFFER_LOCK_SHARE);
#else
			drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buf);
			SetPageRefUnit(&drefDesc);
#endif

			page = BufferGetPage(buf);
			maxoff = PageGetMaxOffsetNumber(page);
		}

		/*
		 * In passing, detect index corruption involving an index page with a
		 * TID that points to a location in the heap that couldn't possibly be
		 * correct.  We only do this with actual TIDs from caller's index page
		 * (not items reached by traversing through a HOT chain).
		 */
		index_delete_check_htid(delstate, page, maxoff, htid, istatus);

		if (istatus->knowndeletable)
			Assert(!delstate->bottomup && !istatus->promising);
		else
		{
			ItemPointerData tmp = *htid;
			HeapTupleData heapTuple;

			/* Are any tuples from this HOT chain non-vacuumable? */
			copied_latestRemovedXid = InvalidTransactionId;
			if (heap_hot_search_buffer_with_vc(&tmp, rel, buf, &SnapshotNonVacuumable,
											   &heapTuple, &tmp_tup, &drefDesc,
											   NULL, true, NULL))
				continue;		/* can't delete entry */

			/* Caller will delete, since whole HOT chain is vacuumable */
			istatus->knowndeletable = true;

			/* Maintain index free space info for bottom-up deletion case */
			if (delstate->bottomup)
			{
				Assert(istatus->freespace > 0);
				actualfreespace += istatus->freespace;
				if (actualfreespace >= curtargetfreespace)
					bottomup_final_block = true;
			}
		}

		/*
		 * Maintain latestRemovedXid value for deletion operation as a whole
		 * by advancing current value using heap tuple headers.  This is
		 * loosely based on the logic for pruning a HOT chain.
		 */
		
		/*
		 * latestRemovedXid was already gotten in
		 * heap_hot_search_buffer_with_vc(), using non-vacuumable snapshot.
		 */
		if (latestRemovedXid < copied_latestRemovedXid)
			latestRemovedXid = copied_latestRemovedXid;

		/* Enable further/final shrinking of deltids for caller */
		finalndeltids = i + 1;
	}

#ifdef USING_LOCK
	UnlockReleaseBuffer(buf);
#else
	DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
	ReleaseBuffer(buf);
#endif

	/*
	 * Shrink deltids array to exclude non-deletable entries at the end.  This
	 * is not just a minor optimization.  Final deltids array size might be
	 * zero for a bottom-up caller.  Index AM is explicitly allowed to rely on
	 * ndeltids being zero in all cases with zero total deletable entries.
	 */
	Assert(finalndeltids > 0 || delstate->bottomup);
	delstate->ndeltids = finalndeltids;

	return latestRemovedXid;
}
#endif
/*
 *	locator_insert		- insert tuple into a mempartition
 *
 * The new tuple is stamped with current transaction ID and the specified
 * command ID.
 *
 * See table_tuple_insert for comments about most of the input flags, except
 * that this routine directly takes a tuple rather than a slot.
 *
 * There's corresponding HEAP_INSERT_ options to all the TABLE_INSERT_
 * options, and there additionally is HEAP_INSERT_SPECULATIVE which is used to
 * implement table_tuple_insert_speculative().
 *
 * On return the header fields of *tup are updated to match the stored tuple;
 * in particular tup->t_self receives the actual TID where the tuple was
 * stored.  But note that any toasting of fields within the tuple data is NOT
 * reflected into *tup.
 */
void
locator_insert(Relation relation, HeapTuple tup, CommandId cid,
			   int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple	heaptup;
	Buffer		buffer;
	Page		page;
	BlockNumber	targetBlock;
	bool		isInit;
	uint32					partKey;
	int32					partLevel;
	uint64					seqNum;
	uint64					recCnt;
	LocatorPartNumber		partNum;
	LocatorPartNumber		partNumLevelZero;
	LocatorTieredNumber		tieredNumLevelZero;
	LocatorRouteSynopsis		locator_route_synopsis;
	LocatorExternalCatalog	   *exCatalog;
	LWLock				   *exCatalog_mempartition_lock;
	uint32					max_recCnt;
	int						mempart_id;
	LWLock				   *mempartition_lock;
	OffsetNumber			offnum;

	/* Init values */
	locator_route_synopsis = tup->t_locator_route_synopsis;
	partKey = locator_route_synopsis->rec_key.partKey; 
	exCatalog = LocatorGetExternalCatalog(RelationGetRelid(relation));
    partNumLevelZero = 
		GetPartitionNumberFromPartitionKey(
			exCatalog->spreadFactor, 0, exCatalog->lastPartitionLevel, partKey);
	exCatalog_mempartition_lock =
		LocatorExternalCatalogMemPartitionLock(exCatalog, partNumLevelZero);
	max_recCnt = LocatorMempartPageCount * relation->records_per_block;

	/* Cheap, simplistic check that the tuple matches the rel's rowtype. */
	Assert(HeapTupleHeaderGetNatts(tup->t_data) <=
		   RelationGetNumberOfAttributes(relation));

	/*
	 * Fill in tuple header fields and toast the tuple if necessary.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);
	heaptup->t_locator_route_synopsis = tup->t_locator_route_synopsis;

	Assert(IsSiro(relation));
	Assert(RelationGetDescr(relation)->maxlen >= MAXALIGN(heaptup->t_len));

	/* Find buffer to insert this tuple into */

	/* Acquire the lock of level 0 to read and write meta data */
	LWLockAcquire(exCatalog_mempartition_lock, LW_EXCLUSIVE);

	/* Issue the insertion sequence number */
	seqNum = exCatalog->sequenceNumberCounter[partNumLevelZero].val++;
	locator_route_synopsis->rec_key.seqnum = seqNum;

	/* Get the position of new record in level 0 */
	tieredNumLevelZero = 
		exCatalog->CurrentTieredNumberLevelZero[partNumLevelZero];
	recCnt = exCatalog->CurrentRecordCountsLevelZero[partNumLevelZero].val;
	offnum = 3 * (recCnt % relation->records_per_block) + 1;

	Assert(recCnt < max_recCnt);

	/* Increment current record count */
	exCatalog->CurrentRecordCountsLevelZero[partNumLevelZero].val += 1;

	/* This is the last record */
	if (unlikely(recCnt == (max_recCnt - 1)))
	{
		/* Init info for next insertion */
		exCatalog->CurrentTieredNumberLevelZero[partNumLevelZero] += 1;

		exCatalog->TieredRecordCountsLevelZero[partNumLevelZero].val += 
			exCatalog->CurrentRecordCountsLevelZero[partNumLevelZero].val;
		exCatalog->CurrentRecordCountsLevelZero[partNumLevelZero].val = 0;
	}

	partLevel = relation->rd_locator_level_count - 1;
	partNum = partKey;

	/* Set routeSynopsis info. */
	while (partLevel >= 0)
	{
		if (partLevel == 0)
		{
			/* First level setup. */
			locator_route_synopsis->rec_pos.first_lvl_tierednum = tieredNumLevelZero;
			locator_route_synopsis->rec_pos.first_lvl_pos = recCnt;
		}
		else if (partLevel == (relation->rd_locator_level_count - 1))
		{
			/* Last level setup. */
			locator_route_synopsis->rec_pos.last_lvl_pos = 
				exCatalog->lastCumulativeCount[partNum];
			exCatalog->lastCumulativeCount[partNum] += 1;
		}
		else
		{
			/* Internal level setup. */
			locator_route_synopsis->rec_pos.in_partition_counts[partLevel - 1].cumulative_count =
				(exCatalog->internalCumulativeCount[partLevel][partNum].cumulative_count)++;
		} 

		partLevel -= 1;
		partNum /= relation->rd_locator_spread_factor;
	}

	LWLockRelease(exCatalog_mempartition_lock);

	if ((recCnt % relation->records_per_block) == 0)
		IncrBlockCount(exCatalog, 1);

	/* Get target block number */
	if (unlikely(recCnt == 0))
		targetBlock = P_NEW;
	else
		targetBlock = (recCnt / relation->records_per_block);

	/* Get the partition buffer */
	buffer = 
		LocatorReadBufferForInsert(relation, partNumLevelZero, 
								   tieredNumLevelZero, targetBlock);

	/* Get mempartition lock */
	mempart_id = LocatorGetMempartIdFromBufId(buffer - 1);
	mempartition_lock = LocatorGetMempartLock(mempart_id);

	/* Acquire buffer mempartition lock */
	LWLockAcquire(mempartition_lock, LW_SHARED);

	Assert(buffer != InvalidBuffer);

	if (likely(targetBlock != P_NEW))
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	else
		targetBlock = 0;

	/* Initialize page if necessary */
	page = BufferGetPage(buffer);
	if (PageIsNew(page))
		PageInit(page, BufferGetPageSize(buffer), 0);

	/*
	 * We're about to do the actual insert -- but check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the insert
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple insert is visible).
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.  So we don't need to specify a
	 * buffer when making the call, which makes for a faster check.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	isInit = PartitionPutHeapTupleWithDummy(relation, buffer, heaptup,
											(options & HEAP_INSERT_SPECULATIVE) != 0,
											targetBlock, offnum);

	/*
	 * XXX Should we set PageSetPrunable on this page ?
	 *
	 * The inserting transaction may eventually abort thus making this tuple
	 * DEAD and hence available for pruning. Though we don't want to optimize
	 * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
	 * aborted tuple will never be pruned until next vacuum is triggered.
	 *
	 * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
	 */

	MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_insert xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr	recptr;
		Page		page = BufferGetPage(buffer);
		uint8		info = XLOG_HEAP_INSERT;
		int			bufflags = 0;

		/*
		 * If this is a catalog, we need to transmit combo CIDs to properly
		 * decode, so log that as well.
		 */
		if (RelationIsAccessibleInLogicalDecoding(relation))
			log_heap_new_cid(relation, heaptup);

		/*
		 * If this is the single and first tuple on page, we can reinit the
		 * page instead of restoring the whole thing.  Set flag, and hide
		 * buffer references from XLogInsert.
		 */
		if (isInit)
		{
			info |= XLOG_HEAP_INIT_PAGE;
			bufflags |= REGBUF_WILL_INIT;
		}

		xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
		xlrec.flags = 0;
		if (options & HEAP_INSERT_SPECULATIVE)
			xlrec.flags |= XLH_INSERT_IS_SPECULATIVE;
		Assert(ItemPointerGetBlockNumber(&heaptup->t_self) == BufferGetBlockNumber(buffer));

		/*
		 * For logical decoding, we need the tuple even if we're doing a full
		 * page write, so make sure it's included even if we take a full-page
		 * image. (XXX We could alternatively store a pointer into the FPW).
		 */
		if (RelationIsLogicallyLogged(relation) &&
			!(options & HEAP_INSERT_NO_LOGICAL))
		{
			xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
			bufflags |= REGBUF_KEEP_DATA;

			if (IsToastRelation(relation))
				xlrec.flags |= XLH_INSERT_ON_TOAST_RELATION;
		}

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapInsert);

		xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
		xlhdr.t_infomask = heaptup->t_data->t_infomask;
		xlhdr.t_hoff = heaptup->t_data->t_hoff;

		/*
		 * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
		 * write the whole page to the xlog, we don't need to store
		 * xl_heap_header in the xlog.
		 */
		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
		XLogRegisterBufData(0, (char *) &xlhdr, SizeOfHeapHeader);
		/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
		XLogRegisterBufData(0,
							(char *) heaptup->t_data + SizeofHeapTupleHeader,
							heaptup->t_len - SizeofHeapTupleHeader);

		/* filtering by origin on a row level is much more efficient */
		XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

		recptr = XLogInsert(RM_HEAP_ID, info);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Increment the count of inserted records */
	IncrInsertedRecordsCount(buffer);
#ifdef LOCATOR_DEBUG
	if (GetInsertedRecordsCount(buffer) == relation->records_per_block * LocatorMempartPageCount)
		fprintf(stderr, "[LOCATOR] incr, rel: %u, cnt: %u, buf: %p, pid: %d\n",
						relation->rd_id,
						GetInsertedRecordsCount(buffer),
						GetBufferDescriptor(LocatorGetMempartBufId(buffer - 1)),
						(int)getpid());
#endif /* LOCATOR_DEBUG */
	UnlockReleaseBuffer(buffer);
	LWLockRelease(mempartition_lock);

	/*
	 * If tuple is cachable, mark it for invalidation from the caches in case
	 * we abort.  Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}
}

/* ----------------
 *		initlocatorscan - scan code common to locator_beginscan and
 *						  locator_rescan
 * ----------------
 */
static void
initlocatorscan(LocatorScanDesc lscan)
{
	HeapScanDesc scan = (HeapScanDesc) lscan;
	Relation	relation = scan->rs_base.rs_rd;
	BlockNumber nblocks = 0;
	bool		allow_strat;
	bool		allow_sync;
	MemoryContext oldcxt;
	LocatorExternalCatalog *exCatalog;
	LWLock *exCatalogMempartitionLock;
	ListCell *lc;
	LocatorPartLevel last_level;
	LocatorPartNumber part_num;
	LocatorTieredNumber tiered_num;
	LocatorTieredNumber tiered_num_prev[MAX_SPREAD_FACTOR];
	LocatorTieredNumber tiered_num_curr[MAX_SPREAD_FACTOR];
	uint64 rec_cnt;
	uint64 level_zero_rec_cnt[MAX_SPREAD_FACTOR];
	int level_zero_part_cnt = 0;

	ColumnarLayout *columnarLayout;
	uint32 recordsPerRecordZone;
	uint32 blocksPerRecordZone;
	int internalIndex; /* 0: internal level, 1: last level */

	/* LocatorPartInfo array for level 0 */
	LocatorPartInfo part_infos;

	/* Iterator of LocatorPartInfo array */
	LocatorPartInfo part_info;

#ifdef LOCATOR_DEBUG
	int cntr;
#endif /* LOCATOR_DEBUG */

#ifdef IO_AMOUNT
	int fulledRecordZones;
	int remainedRecords;
	int columnarBlocksPerRecordZone;
	uint8 columnGroupCount;
	int targetAttrCnt = 0;
	int targetAttrs[32] = { 0 };
	int targetColumnGroups[MAX_COLUMN_GROUP_COUNT] = { 0 };

	uint32 target_nblocks = 0;
	uint32 target_nblocks_partitions = 0;
	uint32 target_nblocks_column_groups = 0;

	foreach(lc, relation->locator_required_attidx_list)
	{
		targetAttrs[targetAttrCnt++] = lfirst_int(lc);
	}
#endif /* IO_AMOUNT */

	/* Set initial values */
	exCatalog = LocatorGetExternalCatalog(relation->rd_id);
	last_level = relation->rd_locator_level_count - 1;
	
	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	/* Init level 0 */
	if (unlikely(lscan->part_infos_to_scan[0] != NIL))
	{
		pfree(linitial(lscan->part_infos_to_scan[0]));
		list_free(lscan->part_infos_to_scan[0]);
		lscan->part_infos_to_scan[0] = NIL;
	}

	/* Acquire lock, and increment refcounts */

	/* We lock all exCatalog mempartition lock here. */
	for (int i = 0; i < lscan->level_zero_part_cnt; ++i)
	{
		int part_num_level_zero = lscan->level_zero_part_nums[i];

		exCatalogMempartitionLock = 
			LocatorExternalCatalogMemPartitionLock(exCatalog, part_num_level_zero);

		LWLockAcquire(exCatalogMempartitionLock, LW_SHARED);
	}

	for (int i = 1; i < relation->rd_locator_level_count; ++i)
	{
		foreach(lc, lscan->part_infos_to_scan[i])
		{
			part_num = ((LocatorPartInfo) lfirst(lc))->partNum;
			IncrMetaDataCount(LocatorExternalCatalogRefCounter(exCatalog, i, part_num));
		}
	}

	/* Get scan range of level 0 */
	for (int i = 0; i < lscan->level_zero_part_cnt; ++i)
	{
		int part_num_level_zero = lscan->level_zero_part_nums[i];

		tiered_num_prev[part_num_level_zero] = 
			exCatalog->PreviousTieredNumberLevelZero[part_num_level_zero];
		tiered_num_curr[part_num_level_zero] = 
			exCatalog->CurrentTieredNumberLevelZero[part_num_level_zero];
		level_zero_rec_cnt[part_num_level_zero] = 
			exCatalog->CurrentRecordCountsLevelZero[part_num_level_zero].val;
		
		/* Get the count of mempartition */
		level_zero_part_cnt += 
			level_zero_rec_cnt[part_num_level_zero] == 0 ?
			tiered_num_curr[part_num_level_zero] - tiered_num_prev[part_num_level_zero] :
			tiered_num_curr[part_num_level_zero] - tiered_num_prev[part_num_level_zero] + 1;
	}

	/* Get scan range of internal levels */
	for (int i = 1; i < relation->rd_locator_level_count; ++i)
	{
		foreach(lc, lscan->part_infos_to_scan[i])
		{
			/* Set partition info (partition number) */
			part_info = ((LocatorPartInfo) lfirst(lc));
			part_num = part_info->partNum;

			/* Last level only has generation number 0 */
			part_info->partGen = i == last_level ?
								 0 : exCatalog->generationNums[i][part_num];

			/* Get whole count temporary */
			part_info->last_recCnt = exCatalog->realRecordsCount[i][part_num];

			part_info->recCnt = part_info->last_recCnt;
		}
	}

#ifdef IO_AMOUNT
	target_nblocks = pg_atomic_read_u64(exCatalog->nblocks);
#endif /* IO_AMOUNT */

	/* Decrement refcounts */
	for (int i = relation->rd_locator_level_count - 1; i > 0; --i)
	{
		foreach(lc, lscan->part_infos_to_scan[i])
		{
			part_num = ((LocatorPartInfo) lfirst(lc))->partNum;
			DecrMetaDataCount(LocatorExternalCatalogRefCounter(exCatalog, i, part_num));
		}
	}

	/* Release all locks. */
	for (int i = 0; i < lscan->level_zero_part_cnt; ++i)
	{
		int part_num_level_zero = lscan->level_zero_part_nums[i];

		exCatalogMempartitionLock = 
			LocatorExternalCatalogMemPartitionLock(exCatalog, part_num_level_zero);

		LWLockRelease(exCatalogMempartitionLock);
	}

	if (level_zero_part_cnt > 0)
	{
		/* Init array of LocatorPartInfoData and set curr iterator */
		part_infos = palloc0_array(LocatorPartInfoData, level_zero_part_cnt);
		part_info = part_infos;

		columnarLayout = exCatalog->columnarLayoutArray + exCatalog->layoutArray[0];
		recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
		blocksPerRecordZone = columnarLayout->blocksPerRecordZone[0];

#ifdef LOCATOR_DEBUG
		cntr = 0;
#endif /* LOCATOR_DEBUG */

		/* Set scan range of level 0 */
		for (int i = 0; i < lscan->level_zero_part_cnt; ++i)
		{
			int part_num_level_zero = lscan->level_zero_part_nums[i];

			for (tiered_num = tiered_num_curr[part_num_level_zero];
				 (tiered_num - tiered_num_prev[part_num_level_zero]) != InvalidLocatorTieredNumber;
				 --tiered_num)
			{
				if ((tiered_num == tiered_num_curr[part_num_level_zero]) && 
						(level_zero_rec_cnt[part_num_level_zero] == 0))
					continue;

				/* Set partition nubmer */
				part_info->partNum = part_num_level_zero;

				/* Set generation number (= tiered number) */
				part_info->partGen = tiered_num;

				/* Get block count by checking file size */
				if (tiered_num == tiered_num_curr[part_num_level_zero])
				{
					part_info->block_cnt =
						NRecordZonesUsingRecordCount(
							level_zero_rec_cnt[part_num_level_zero], 
							recordsPerRecordZone) * blocksPerRecordZone;
					part_info->last_recCnt = 0;
				}
				else
				{
					/*
				 	 * Tiered level 0 mempartitions always have the same number of
				 	 * records.
				 	 */
					part_info->block_cnt = LocatorMempartPageCount;
					part_info->last_recCnt = relation->records_per_block;
				}

				/* Add block count */
				nblocks += part_info->block_cnt;

#ifdef LOCATOR_DEBUG
				if (tiered_num == tiered_num_curr[part_num_level_zero])
					part_info->recCnt = level_zero_rec_cnt[part_num_level_zero];
				else
					part_info->recCnt = 0;
#endif /* LOCATOR_DEBUG */

#ifdef IO_AMOUNT
				target_nblocks_partitions += part_info->block_cnt;
				target_nblocks_column_groups += part_info->block_cnt;
#endif /* IO_AMOUNT */

				/* Append this newest in-memory partition into list */
				lscan->part_infos_to_scan[0] =
					lappend(lscan->part_infos_to_scan[0], part_info);

				/* Move iterator. */
				++part_info;

#ifdef LOCATOR_DEBUG
				++cntr;
#endif /* LOCATOR_DEBUG */
			}
		}

		/* Set starting block */
		if (likely(lscan->c_lc == NULL))
		{
			lscan->c_lc = list_head(lscan->part_infos_to_scan[0]);
			lscan->c_part_info = part_infos;
			lscan->c_level = 0;
		}

#ifdef LOCATOR_DEBUG
		Assert(cntr == level_zero_part_cnt);
#endif /* LOCATOR_DEBUG */
	}

	/* Set scan range of internal levels */
	for (int i = 1; i < relation->rd_locator_level_count; ++i)
	{
#ifdef LOCATOR_DEBUG
		BlockNumber tmp = 0;
#endif /* LOCATOR_DEBUG */
		internalIndex = i == last_level ? 1 : 0;
		columnarLayout = exCatalog->columnarLayoutArray + exCatalog->layoutArray[i];
		recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
		blocksPerRecordZone = columnarLayout->blocksPerRecordZone[internalIndex];
#ifdef IO_AMOUNT
		columnGroupCount = *(columnarLayout->columnGroupCount);
		columnarBlocksPerRecordZone = 0;

		if (columnGroupCount > 1)
		{
			memset(targetColumnGroups, 0, sizeof(int) * MAX_COLUMN_GROUP_COUNT);

			for (int j = 0; j < targetAttrCnt; ++j)
				targetColumnGroups[columnarLayout->columnGroupArray[targetAttrs[j]]] = 1;

			for (int j = 0; j < columnGroupCount; ++j)
			{
				if (targetColumnGroups[j])
				{
					columnarBlocksPerRecordZone += 
						((recordsPerRecordZone - 1) / (columnarLayout->columnGroupMetadataArray[j].tuplesPerBlock) + 1);
				}
			}
		}
#endif /* IO_AMOUNT */

		foreach(lc, lscan->part_infos_to_scan[i])
		{
			part_info = ((LocatorPartInfo)lfirst(lc));

			/* Set partition number */
			part_num = part_info->partNum;

			/* If this partition has any records */
			if ((rec_cnt = part_info->last_recCnt) != 0)
			{
				/* Get block count and record count of last block */
				part_info->block_cnt =
					NRecordZonesUsingRecordCount(rec_cnt, recordsPerRecordZone) * blocksPerRecordZone;
				part_info->last_recCnt =
					LastRecordCountUsingRecordCount(rec_cnt, relation->records_per_block);

				/* Add block count */
				nblocks += part_info->block_cnt;

#ifdef LOCATOR_DEBUG
				tmp += part_info->block_cnt;
#endif /* LOCATOR_DEBUG */

				/* Set starting block */
				if (unlikely(lscan->c_lc == NULL))
				{
					lscan->c_lc = lc;
					lscan->c_part_info = part_info;
					lscan->c_level = i;
				}

#ifdef IO_AMOUNT
				if (columnGroupCount > 1) /* MIXED layout */
				{
					fulledRecordZones = rec_cnt / recordsPerRecordZone;
					remainedRecords = rec_cnt % recordsPerRecordZone;

					target_nblocks_partitions +=
						(fulledRecordZones * blocksPerRecordZone);
					if (remainedRecords != 0)
					{
						for (int j = 0; j < columnGroupCount; ++j)
						{
							target_nblocks_partitions +=
								((remainedRecords - 1) / (columnarLayout->columnGroupMetadataArray[j].tuplesPerBlock) + 1);
						}
					}

					target_nblocks_column_groups +=
						(fulledRecordZones * columnarBlocksPerRecordZone);
					if (remainedRecords != 0)
					{
						for (int j = 0; j < columnGroupCount; ++j)
						{
							if (targetColumnGroups[j])
							{
								target_nblocks_column_groups +=
									((remainedRecords - 1) / (columnarLayout->columnGroupMetadataArray[j].tuplesPerBlock) + 1);
							}
						}
					}
				}
				else /* ROW_ORIENTED layout */
				{
					target_nblocks_partitions += part_info->block_cnt;
					target_nblocks_column_groups += part_info->block_cnt;
				}
#endif /* IO_AMOUNT */
			}
			else
			{
				/* This partition has no record */
				part_info->block_cnt = 0;
				part_info->last_recCnt = 0;
			}
		}

#ifdef LOCATOR_DEBUG
		fprintf(stderr, "level: %d (nblocks: %u)\n", i, tmp);
#endif /* LOCATOR_DEBUG */
	}

#ifdef LOCATOR_DEBUG
	lscan->c_scanned_records = 0;
	lscan->c_valid_versions = 0;
#endif /* LOCATOR_DEBUG */

	MemoryContextSwitchTo(oldcxt);

	lscan->c_block = 0;

	/*
	 * Determine the number of blocks we have to scan.
	 *
	 * It is sufficient to do this once at scan start, since any tuples added
	 * while the scan is in progress will be invisible to my snapshot anyway.
	 * (That is not true when using a non-MVCC snapshot.  However, we couldn't
	 * guarantee to return tuples added after scan start anyway, since they
	 * might go into pages we already scanned.  To guarantee consistent
	 * results for a non-MVCC snapshot, the caller must hold some higher-level
	 * lock that ensures the interesting tuple(s) won't change.)
	 */
	Assert(scan->rs_base.rs_parallel == NULL);
	scan->rs_nblocks = nblocks;

	/*
	 * If the table is large relative to NBuffers, use a bulk-read access
	 * strategy and enable synchronized scanning (see syncscan.c).  Although
	 * the thresholds for these features could be different, we make them the
	 * same so that there are only two behaviors to tune rather than four.
	 * (However, some callers need to be able to disable one or both of these
	 * behaviors, independently of the size of the table; also there is a GUC
	 * variable that can disable synchronized scanning.)
	 *
	 * Note that table_block_parallelscan_initialize has a very similar test;
	 * if you change this, consider changing that one, too.
	 */
	if (!RelationUsesLocalBuffers(scan->rs_base.rs_rd) &&
		scan->rs_nblocks > NBuffers / 4)
	{
		allow_strat = (scan->rs_base.rs_flags & SO_ALLOW_STRAT) != 0;
		allow_sync = false;
	}
	else
		allow_strat = allow_sync = false;

	if (allow_strat)
	{
		/* During a rescan, keep the previous strategy object. */
		if (scan->rs_strategy == NULL)
			scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
	}
	else
	{
		if (scan->rs_strategy != NULL)
			FreeAccessStrategy(scan->rs_strategy);
		scan->rs_strategy = NULL;
	}

	if (scan->rs_base.rs_parallel != NULL)
	{
		/* For parallel scan, believe whatever ParallelTableScanDesc says. */
		if (scan->rs_base.rs_parallel->phs_syncscan)
			scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
	}
	else if (allow_sync && synchronize_seqscans)
	{
		scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		scan->rs_startblock = ss_get_location(scan->rs_base.rs_rd, scan->rs_nblocks);
	}
	else
	{
		scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
		scan->rs_startblock = 0;
	}

	scan->rs_numblocks = InvalidBlockNumber;
	scan->rs_inited = false;
	scan->rs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->rs_ctup.t_self);
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_cblock = InvalidBlockNumber;

	/* page-at-a-time fields are always invalid when not rs_inited */
	/*
	 * Currently, we only have a stats counter for sequential heap scans (but
	 * e.g for bitmap scans the underlying bitmap index scans will be counted,
	 * and for sample scans we update stats for tuple fetches).
	 */
	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);

	/* Init values */
	scan->c_buf_id = InvalidEbiSubBuf;
	scan->rs_base.is_heapscan = true; 
	scan->is_first_ebi_scan = true;
	scan->unobtained_segments = NULL;
	scan->unobtained_segments_capacity = 0;
	scan->unobtained_segment_nums = 0;
	scan->unobtained_version_offsets = NULL;
	scan->unobtained_versions_capacity = 0;
	scan->unobtained_version_nums = 0;
	scan->rs_base.counter_in_heap = 0;
	scan->rs_base.counter_in_ebi = 0;
	scan->rs_base.counter_duplicates = 0;
	scan->rs_ctup_copied = NULL; 	/* Initialize the current copied tuple */

	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
		scan->rs_vistuples_copied[i] = NULL;

	if (enable_prefetch && is_prefetching_needed(scan->rs_base.rs_rd, scan))
	{
		scan->rs_base.rs_flags &= ~SO_ALLOW_STRAT;
		scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;
	}

#ifdef IO_AMOUNT
	reduced_by_HP += (target_nblocks - target_nblocks_partitions);
	reduced_by_VP += (target_nblocks_partitions - target_nblocks_column_groups);
#endif /* IO_AMOUNT */
}

/* ----------------------------------------------------------------
 *					 locator access method interface
 * ----------------------------------------------------------------
 */
TableScanDesc
locator_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key,
				  ParallelTableScanDesc parallel_scan,
				  uint32 flags,
				  List *hap_partid_list)
{
	LocatorScanDesc lscan;
	HeapScanDesc scan;
	MemoryContext oldcxt;

	List **part_infos_to_scan;
	ListCell *lc;

	int level_cnt;
	LocatorPartLevel max_level;

	LocatorPartNumber prev_part_nums[32];
	LocatorPartNumber curr_part_num;
	int pow_len, max_len;
	int16 spread_factor = relation->rd_locator_spread_factor;

	/* LocatorPartInfo arrays for each level */
	LocatorPartInfo part_infos[32];

	/* Iterator of each LocatorPartInfo array */
	LocatorPartInfo part_info[32];

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	lscan = (LocatorScanDesc) palloc(sizeof(LocatorScanDescData));
	scan = (HeapScanDesc) lscan;

	/* Initialize the array for copying scanned tuples */
	MemSet(scan->rs_vistuples_copied, 0x00, sizeof(scan->rs_vistuples_copied));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;
	scan->rs_strategy = NULL;	/* set in initscan */

	/* Init values */
	scan->rs_base.rs_prefetch = NULL; /* set after initscan */
	MemSet(scan->rs_vistuples_size, 0x00, sizeof(int) * MaxHeapTuplesPerPage);
	MemSet(scan->rs_vistuples_freearray, 0x00, 
										sizeof(scan->rs_vistuples_freearray));

	/*
	 * Disable page-at-a-time mode if it's not a MVCC-safe snapshot.
	 */
	if (!(snapshot && IsMVCCSnapshot(snapshot)))
		scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;

	/*
	 * For seqscan and sample scans in a serializable transaction, acquire a
	 * predicate lock on the entire relation. This is required not only to
	 * lock all the matching tuples, but also to conflict with new insertions
	 * into the table. In an indexscan, we take page locks on the index pages
	 * covering the range specified in the scan qual, but in a heap scan there
	 * is nothing more fine-grained to lock. A bitmap scan is a different
	 * story, there we have already scanned the index and locked the index
	 * pages covering the predicate. But in that case we still have to lock
	 * any matching heap tuples. For sample scan we could optimize the locking
	 * to be at least page-level granularity, but we'd need to add per-tuple
	 * locking for that.
	 */
	if (scan->rs_base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
	{
		/*
		 * Ensure a missing snapshot is noticed reliably, even if the
		 * isolation mode means predicate locking isn't performed (and
		 * therefore the snapshot isn't used here).
		 */
		Assert(snapshot);
		PredicateLockRelation(relation, snapshot);
	}

	/* we only need to set this up once */
	scan->rs_ctup.t_tableOid = RelationGetRelid(relation);

	/*
	 * Allocate memory to keep track of page allocation for parallel workers
	 * when doing a parallel scan.
	 */
	if (parallel_scan != NULL)
		scan->rs_parallelworkerdata = palloc(sizeof(ParallelBlockTableScanWorkerData));
	else
		scan->rs_parallelworkerdata = NULL;

	/*
	 * we do this here instead of in initlocatorscan() because heap_rescan also
	 * calls initlocatorscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_base.rs_key = NULL;

	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	/* Init variables */
	level_cnt = relation->rd_locator_level_count;
	max_level = level_cnt - 1;
	part_infos_to_scan = palloc0_array(List*, level_cnt);
	MemSet(prev_part_nums, -1, sizeof(LocatorPartNumber) * level_cnt);
	lscan->level_zero_part_cnt = 0;
	lscan->c_level = 0;

	/* Target of SeqScan is passed */
	if (hap_partid_list != NULL)
	{
		/* Level 0 will be set during initlocatorscan() */
		max_len = list_length(hap_partid_list);
		for (int i = max_level; i > 0; --i)
		{
			/* Get smaller max length */
			if ((pow_len = (int)pow(spread_factor, i + 1)) < max_len)
				max_len = pow_len;

			/* Alloc the space of LocatorPartInfoData and set curr iterator */
			part_infos[i] = palloc0_array(LocatorPartInfoData, max_len);
			part_info[i] = part_infos[i];
		}

		/* Set partition numbers of each level that should be scanned */
		foreach(lc, hap_partid_list)
		{
			/* Get partid */
			curr_part_num = lfirst_int(lc);

			/* Append it to the list of last level */
			part_info[max_level]->partNum = curr_part_num;
			part_infos_to_scan[max_level] =
				lappend(part_infos_to_scan[max_level], part_info[max_level]);
			++part_info[max_level];

			/* Get and append the appropriate partition number of each level */
			for (int i = max_level - 1; i >= 0; --i)
			{
				/* Get partition number of upper level */
				curr_part_num /= spread_factor;
				
				/* This is new partition number of the level */
				if (prev_part_nums[i] != curr_part_num)
				{
					if (i == 0)
					{
						/* We store level zero's partition number separately. */
						lscan->level_zero_part_nums[lscan->level_zero_part_cnt] =
																 curr_part_num;
						lscan->level_zero_part_cnt += 1;
					}
					else
					{
						part_info[i]->partNum = curr_part_num;
						part_infos_to_scan[i] =
							lappend(part_infos_to_scan[i], part_info[i]);
						++part_info[i];

					}

					prev_part_nums[i] = curr_part_num;
				}
				/* Or not, skip left levels */
				else
					break;
			}
		}
	}
	/* Full scan */
	else
	{
		/* Level 0 will be set during initlocatorscan() */
		max_len = relation->rd_locator_part_count;
		for (int i = max_level; i > 0; --i)
		{
			/* Get smaller max length */
			if ((pow_len = (int)pow(spread_factor, i + 1)) < max_len)
				max_len = pow_len;

			/* Alloc the space of LocatorPartInfoData and set curr iterator */
			part_infos[i] = palloc0_array(LocatorPartInfoData, max_len);
			part_info[i] = part_infos[i];
		}

		/* Set partition numbers of each level that should be scanned */
		for (LocatorPartNumber curr = 0;
			 curr < relation->rd_locator_part_count;
			 ++curr)
		{
			/* Get partid */
			curr_part_num = curr;

			/* Append it to the list of last level */
			part_info[max_level]->partNum = curr_part_num;
			part_infos_to_scan[max_level] =
				lappend(part_infos_to_scan[max_level], part_info[max_level]);
			++part_info[max_level];

			/* Get and append the appropriate partition number of each level */
			for (int i = max_level - 1; i >= 0; --i)
			{
				/* Get partition number of upper level */
				curr_part_num /= spread_factor;
				
				/* This is new partition number of the level */
				if (prev_part_nums[i] != curr_part_num)
				{
					if (i == 0)
					{
						/* We store level zero's partition number separately. */
						lscan->level_zero_part_nums[lscan->level_zero_part_cnt] =
																 curr_part_num;
						lscan->level_zero_part_cnt += 1;
					}
					else
					{
						part_info[i]->partNum = curr_part_num;
						part_infos_to_scan[i] =
							lappend(part_infos_to_scan[i], part_info[i]);
						++part_info[i];
					}

					prev_part_nums[i] = curr_part_num;
				}
				/* Or not, skip left levels */
				else
					break;
			}
		}
	}
	lscan->part_infos_to_scan = part_infos_to_scan;
	lscan->c_part_info = NULL;
	lscan->c_lc = NULL;

	MemoryContextSwitchTo(oldcxt);

	initlocatorscan(lscan);

	/* 
	 * We evaluate the necessity of prefetching.
	 */
	if (enable_prefetch && is_prefetching_needed(relation, scan))
	{
		scan->rs_base.rs_prefetch = get_locator_prefetch_desc(relation, lscan);
	}

	lscan->locator_columnar_layout_scan_started = false;
	lscan->c_rec_position = 0;

	return (TableScanDesc) scan;
}

void
locator_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
			   bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc scan = (HeapScanDesc) sscan;
	PrefetchDesc prefetchDesc = sscan->rs_prefetch;
	PrefetchPartitionDesc partition_desc;
	char *path;
	RelFileNode node;
	LocatorPartInfo part_info;

#ifdef LOCATOR_DEBUG
	part_info = lscan->c_part_info;
	if (part_info != NULL)
	{
		fprintf(stderr, "[SeqScan] rel: %u, level: %u, partNum: %u, partGen: %u, recCnt: %u, scanned records: %u, valid versions: %u, pid: %d\n",
						sscan->rs_rd->rd_node.relNode, lscan->c_level, part_info->partNum, part_info->partGen,
						part_info->recCnt, lscan->c_scanned_records, lscan->c_valid_versions, getpid());
	}
#endif /* LOCATOR_DEBUG */

	if (set_params)
	{
		if (allow_strat)
			scan->rs_base.rs_flags |= SO_ALLOW_STRAT;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_STRAT;

		if (allow_sync)
			scan->rs_base.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_SYNC;

		if (allow_pagemode && scan->rs_base.rs_snapshot &&
			IsMVCCSnapshot(scan->rs_base.rs_snapshot))
			scan->rs_base.rs_flags |= SO_ALLOW_PAGEMODE;
		else
			scan->rs_base.rs_flags &= ~SO_ALLOW_PAGEMODE;
	}

	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
	{
		if (scan->rs_vistuples_copied[i] != NULL)
			heap_freetuple(scan->rs_vistuples_copied[i]);
	}

	if (prefetchDesc)
	{
		partition_desc = (PrefetchPartitionDesc)prefetchDesc;
		node = sscan->rs_rd->rd_node;

		/* Close previous file */
		if (LocatorSimpleClose(partition_desc->c_fd) == -1)
		{
			/* For error log */
			part_info = lscan->c_part_info;
			path = psprintf("base/%u/%u.%u.%u.%u",
							node.dbNode, node.relNode,
							partition_desc->c_level,
							part_info->partNum,
							part_info->partGen);

#ifdef ABORT_AT_FAIL
			Abort("locator_rescan, Failed to close locator partition file during prefetching");
#else
			ereport(ERROR, errmsg("Failed to close locator partition file during prefetching, name: %s",
								path));
#endif
		}
	}

	/*
	 * reinitialize scan descriptor
	 */
	initlocatorscan(lscan);

	if (scan->rs_ctup_copied != NULL)
		heap_freetuple(scan->rs_ctup_copied);

	/* Existing resource cleanup */
	if (scan->unobtained_version_offsets)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		pfree(scan->unobtained_version_offsets);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->unobtained_segments)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		for (int i = 0; i < scan->unobtained_segment_nums; ++i)
			pfree(scan->unobtained_segments[i]);

		pfree(scan->unobtained_segments);
		MemoryContextSwitchTo(oldcxt);
	}

	/* Init values */
	scan->rs_base.is_heapscan = true; 
	scan->is_first_ebi_scan = true;
	scan->unobtained_segments = NULL;
	scan->unobtained_segments_capacity = 0;
	scan->unobtained_segment_nums = 0;
	scan->unobtained_version_offsets = NULL;
	scan->unobtained_versions_capacity = 0;
	scan->unobtained_version_nums = 0;

	if (prefetchDesc)
	{ 
		/* Copy locator scan state */
		partition_desc->level_cnt = sscan->rs_rd->rd_locator_level_count;
 		partition_desc->part_infos_to_prefetch = lscan->part_infos_to_scan;
 		partition_desc->c_level = lscan->c_level;
 		partition_desc->c_lc = lscan->c_lc;
 		partition_desc->c_block_cnt = lscan->c_part_info->block_cnt;
				
		/* Get file name */
		part_info = lfirst(partition_desc->c_lc);
		path = psprintf("base/%u/%u.%u.%u.%u",
						node.dbNode, node.relNode,
						partition_desc->c_level,
						part_info->partNum,
						part_info->partGen);

		if ((partition_desc->c_fd = LocatorSimpleOpen(path, O_RDWR)) == -1)
		{
#ifdef ABORT_AT_FAIL
			Abort("locator_rescan, Failed to open locator partition file during prefetching");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file during prefetching, name: %s",
									path));
#endif
		}
		pfree(path);

 		prefetchDesc->next_reqblock = scan->rs_startblock;
 		prefetchDesc->prefetch_end = false;
 		prefetchDesc->requested_io_num = 0;
 		prefetchDesc->complete_io_num = 0;

		while (prefetchDesc->io_req_complete_head)
		{
			ReadBufDesc readbuf_desc;

			Assert(prefetchDesc->io_req_complete_tail);

			/* Remove from complete list */
			readbuf_desc = DequeueReadBufDescFromCompleteList(prefetchDesc);

			/* Append to free list */
			PushReadBufDescToFreeList(prefetchDesc, readbuf_desc);
		}
	}

	lscan->locator_columnar_layout_scan_started = false;
	lscan->c_rec_position = 0;
}

void
locator_endscan(TableScanDesc sscan)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc scan = (HeapScanDesc) sscan;
	MemoryContext oldcxt;
#ifdef LOCATOR_DEBUG
	LocatorPartInfo part_info;

	part_info = lscan->c_part_info;
	if (part_info != NULL)
	{
		fprintf(stderr, "[SeqScan] rel: %u, level: %u, partNum: %u, partGen: %u, recCnt: %u, scanned records: %u, valid versions: %u, pid: %d\n",
						sscan->rs_rd->rd_node.relNode, lscan->c_level, part_info->partNum, part_info->partGen,
						part_info->recCnt, lscan->c_scanned_records, lscan->c_valid_versions, getpid());
	}
#endif /* LOCATOR_DEBUG */

	/* Note: no locking manipulations needed */

	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	/* Free level 0 */
	if (lscan->part_infos_to_scan[0] != NIL)
	{
		pfree(linitial(lscan->part_infos_to_scan[0]));
		list_free(lscan->part_infos_to_scan[0]);
	}

	/* Free internal levels */
	for (int i = 1; i < sscan->rs_rd->rd_locator_level_count; i++)
	{
		pfree(linitial(lscan->part_infos_to_scan[i]));
		list_free(lscan->part_infos_to_scan[i]);
	}

	/* Free part_info list*/
	pfree(lscan->part_infos_to_scan);

	MemoryContextSwitchTo(oldcxt);

	/*
	 * unpin buffers
	 */
	if (sscan->is_heapscan)
	{
		if (BufferIsValid(scan->rs_cbuf))
			ReleaseBuffer(scan->rs_cbuf);
	}
	else
	{
		if (scan->c_buf_id != InvalidEbiSubBuf)
		{
			UnpinEbiSubBuffer(scan->c_buf_id);
			scan->c_buf_id = InvalidEbiSubBuf;
		}
	}

	/* Existing resource cleanup */
	if (scan->unobtained_version_offsets)
	{
		oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		pfree(scan->unobtained_version_offsets);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->unobtained_segments)
	{
		oldcxt = MemoryContextSwitchTo(CurTransactionContext);

		for (int i = 0; i < scan->unobtained_segment_nums; ++i)
			pfree(scan->unobtained_segments[i]);

		pfree(scan->unobtained_segments);
		MemoryContextSwitchTo(oldcxt);
	}

	if (scan->rs_ctup_copied != NULL)
		heap_freetuple(scan->rs_ctup_copied);

	for (int i = 0; i < MaxHeapTuplesPerPage; i++)
	{
		if (scan->rs_vistuples_copied[i] != NULL)
			heap_freetuple(scan->rs_vistuples_copied[i]);
	}

	if (sscan->rs_prefetch != NULL)
		ReleasePrefetchDesc(sscan->rs_prefetch, sscan->is_heapscan);

	/*
	 * decrement relation reference count and free scan descriptor storage
	 */
	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	if (scan->rs_parallelworkerdata != NULL)
		pfree(scan->rs_parallelworkerdata);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	pfree(lscan);
}

/*
 * locatorsetpage - subroutine for locatorgettup()
 *
 * This routine checks and sets the page that should be scanned at this time,
 * out of the scope stored in LocatorScanDesc.
 */
static void
locatorsetpage(LocatorScanDesc lscan)
{
	List *c_list;
	ListCell *c_lc;
	LocatorPartInfo c_part_info;
	int c_level = lscan->c_level;

	/*
	 * If the current partition hasn't been entirely scanned, continue to
	 * scan the rest of the partition.
	 */
	if (likely((++(lscan->c_block)) < lscan->c_part_info->block_cnt))
		return;

#ifdef LOCATOR_DEBUG
	c_part_info = lscan->c_part_info;
	fprintf(stderr, "[SeqScan] rel: %u, level: %u, partNum: %u, partGen: %u, recCnt: %u, scanned records: %u, valid versions: %u, pid: %d\n",
					lscan->base.rs_base.rs_rd->rd_id, c_level, c_part_info->partNum, c_part_info->partGen,
					c_part_info->recCnt, lscan->c_scanned_records, lscan->c_valid_versions, getpid());

	lscan->c_scanned_records = 0;
	lscan->c_valid_versions = 0;
#endif /* LOCATOR_DEBUG */

	/* Set next ListCell and LocatorPartInfo */
	c_list = lscan->part_infos_to_scan[c_level];

	if ((c_lc = lnext(c_list, lscan->c_lc)) == NULL)
	{
		c_list = lscan->part_infos_to_scan[++c_level];
		c_lc = list_head(c_list);	
	}

	c_part_info = (LocatorPartInfo)lfirst(c_lc);

	/* Partition was changed, need to be checked */
	while (c_part_info->block_cnt == 0)
	{
		if ((c_lc = lnext(c_list, c_lc)) == NULL)
		{
			c_list = lscan->part_infos_to_scan[++c_level];
			c_lc = list_head(c_list);
		}

		c_part_info = (LocatorPartInfo)lfirst(c_lc);
	}

	/* Store the variables */
	lscan->c_lc = c_lc;
	lscan->c_part_info = c_part_info;
	lscan->c_level = c_level;
	lscan->c_block = 0;
}

/*
 * locatorgetpage - subroutine for locatorgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 *
 * This function manages contention with the writer using an dual reference
 * counter.
 * 
 * If an ebi page containing a version not obtained from the heap page is still
 * in memory, it reads that version.
 * Otherwise, it records the location of that version and reads it after the
 * heap pages have been scanned.
 */
void
locatorgetpage(TableScanDesc sscan,
			   bool is_columnar,
			   LocatorExecutorLevelDesc *level_desc,
			   LocatorExecutorColumnGroupDesc *group_desc)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc scan = (HeapScanDesc) sscan;

	LocatorPartInfo		part_info;
	LocatorPartLevel	part_level;
	LocatorPartNumber	part_number;
	LocatorPartGenNo	part_gen;
	uint32				rec_cnt;

	BlockNumber	c_block;
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	Page		readbuf_dp;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	bool		valid;

	Relation relation;
	Oid		 rel_id;

	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;
	EbiTreeVersionOffset version_offset;

	DualRefDescData	drefDesc;
	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;

	HeapTupleData	v_loctup;
	BlockNumber		v_block;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;
	OffsetNumber	v_offset = InvalidOffsetNumber;
	bool			version_from_readbuf = false;
	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	int record_idx_in_block = -1;

#ifdef LOCATOR_DEBUG
	int				valid_records = 0;
#endif /* LOCATOR_DEBUG */

	relation = scan->rs_base.rs_rd;
	drefDesc.dual_ref = NULL;

	c_block = lscan->c_block;
	part_info = lscan->c_part_info;
	part_level = lscan->c_level;
	part_number = part_info->partNum;
	part_gen = part_info->partGen;

	if (is_columnar)
	{
		buffer = group_desc->c_buf;
		dp = group_desc->c_dp;
		c_block = group_desc->c_blocknum;
		version_from_readbuf = group_desc->c_buf_is_readbuf;

		if (version_from_readbuf)
			readbuf_dp = group_desc->c_dp;
		else
			readbuf_dp = NULL;

		rec_cnt = PageGetMaxOffsetNumber(group_desc->c_dp) / 3;

		goto start_copying_siro_tuples;
	}

	/*
	 * If this block is last, get last_recCnt from part_info.
	 * Or not, get count of records per each block.
	 */
	rec_cnt = c_block == (part_info->block_cnt - 1) ?
			  part_info->last_recCnt : relation->records_per_block;

	/* release previous scan buffer, if any */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	if (scan->rs_base.rs_prefetch != NULL)
	{
		/* Read page with prefetch and ring buffer */
		scan->rs_cbuf =
			ReadPartitionBufferPrefetch(scan->rs_base.rs_rd, c_block,
										scan->rs_base.rs_prefetch,
										scan->rs_strategy, &readbuf_dp);
		scan->c_readbuf_dp = readbuf_dp;
	
		/* 
		 * If we receive an invalid buffer from prefetching, it means it 
		 * originated from the read buffer.
		 */
		if (BufferIsInvalid(scan->rs_cbuf))
			version_from_readbuf = true;
	}
	else
	{
		/* Read page using selected strategy */
		scan->rs_cbuf =
			ReadPartitionBufferExtended(relation, part_level, part_number,
										part_gen, c_block, RBM_NORMAL,
										scan->rs_strategy);
	}

	++(scan->rs_cblock);

	lscan->c_rec_cnt = rec_cnt;

	if (!(scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE))
		return;

	buffer = scan->rs_cbuf;

start_copying_siro_tuples:

	/*
	 * We read the heap tuple safely by using atomic variable instead of
	 * acquiring shared lock, so don't lock the buffer.
	 */
	if (BufferIsValid(buffer))
	{
		heap_page_prune_opt(relation, buffer);

#ifdef USING_LOCK
		drefDesc.dual_ref = NULL;
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
#else /* !USING_LOCK */
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(buffer);
		SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK */

		dp = BufferGetPage(buffer);
	}
	else
	{
		drefDesc.dual_ref = NULL;

		dp = readbuf_dp;
	}

	snapshot = scan->rs_base.rs_snapshot;
	TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
	/*
	 * rec_cnt of 0 means that this block is the last block of a tiered
	 * mempartition at level 0. (See initlocatorscan())
	 * So, use the max offset of the page to get record count.
	 */
	lines = rec_cnt == 0 ? PageGetMaxOffsetNumber(dp) : 3 * rec_cnt; /* p-locator, left, right */
	ntup = 0;

#ifdef LOCATOR_DEBUG
	lscan->c_scanned_records += lines / 3;
#endif /* LOCATOR_DEBUG */

	rel_id = RelationGetRelid(scan->rs_base.rs_rd);
	v_loctup.t_tableOid = rel_id;

	/*
	 *  ----------- -------------- ---------------
	 * | P-locator | left version | right version |
	 *  ----------- -------------- ---------------
	 */
	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		bool is_record_from_other_page = false;

		/* Don't read version directly */
		if (!LP_IS_PLEAF_FLAG(lpp))
			continue;

		/* Get p-locator of this record */
		p_locator = PageGetItem(dp, lpp);

		/* Set self pointer of tuple */
		ItemPointerSet(&(v_loctup.t_self), c_block, lineoff);

		/* Init checking variables */
		valid = false;
		v_buffer = buffer;

		/* Increment reference count and determine where to check */
		GetCheckVar(&drefDesc, &rec_ref_unit, &check_var,
					&set_hint_bits, lineoff);

		/* Check visibility of left, right versions with check_var */
		for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
		{
			/* Is this version need to be checked? */
			if (!(check_var & test_var))
				continue;

			/* Release previous version buffer */
			if (unlikely(v_buffer != buffer))
				ReleaseBuffer(v_buffer);

			/* Set block number and offset */
			v_block = c_block; 
			v_offset = lineoff + test_var;

			Assert(BlockNumberIsValid(v_block));
			Assert(OffsetNumberIsValid(v_offset));

			/*
			 * If the version is in the same page with p-locator, just get it.
			 * Or not, read the buffer that it is in.
			 */
			if (likely(v_block == c_block))
			{
				v_buffer = buffer;
				v_dp = dp;
			}
			else
			{
				Assert(false);
				v_buffer = ReadPartitionBuffer(relation, part_level,
											   part_number, part_gen, v_block);
				Assert(BufferIsValid(v_buffer));
				v_dp = BufferGetPage(v_buffer);

				/* We get a version from other page. Then, Check it. */
				is_record_from_other_page = true;
			}

			v_lpp = PageGetItemId(v_dp, v_offset);

			/* The target has never been updated after INSERT */
			if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
				continue;
				
			v_loctup.t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
			v_loctup.t_len = ItemIdGetLength(v_lpp);

			/* Set buffer to set hint bits */
			hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

			/* Check visibility of version */
			valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
												 hint_bits_buf);

			if (valid)
				break;
		}

		/*
		 * Set the location of the current tuple.
		 */
		record_idx_in_block++;

		/* If we found visible version from heap page, continue */
		if (valid || modification)
		{
			HeapTuple tuple = scan->rs_vistuples_copied[ntup];
			int aligned_tuple_size;

#ifdef LOCATOR_DEBUG
			if (valid)
			{
				++(lscan->c_valid_versions);
				++valid_records;
			}
#endif /* LOCATOR_DEBUG */

			/*
			 * Modification Case
			 *
			 * If the scanning is for an update, we don't need to search deeply. 
			 * Visible tuples inside the heap page cannot be found. Instead, we 
			 * copy one of the tuples in the heap page, allowing the following 
			 * ExecStoreBufferHeapTuple to proceed.
			 */

			if (version_from_readbuf && !is_record_from_other_page)
			{
				/* 
				 * When retrieving the version from a non-update page 
				 * (i.e. ReadBuffer), we avoid memory copy. 
				 */
				if (tuple)
					scan->rs_vistuples_freearray[ntup] = tuple;

				scan->rs_vistuples_copied[ntup] = NULL;
			}
			else
			{
				/* We need to perform a memory copy to fetch a version. */

				if (tuple_size_available(scan, ntup, &v_loctup))
				{
					/* We can reuse the existing tuple structure. */
					if (scan->rs_vistuples_copied[ntup] == NULL)
					{
						scan->rs_vistuples_copied[ntup] = 
										scan->rs_vistuples_freearray[ntup];

						scan->rs_vistuples_freearray[ntup] = NULL;
					}
				}
				else
				{
					/* 
					 * We cannot reuse the existing tuple structure. In such 
					 * cases, we reallocate a new structure to fetch a version. 
					 */
					aligned_tuple_size = 
									1 << my_log2(HEAPTUPLESIZE + v_loctup.t_len);
					tuple = (HeapTuple) palloc(aligned_tuple_size);

					/* Here, we free the exsiting tuple structure. */
					remove_redundant_tuple(scan, ntup);

					scan->rs_vistuples_copied[ntup] = tuple;
					scan->rs_vistuples_size[ntup] = aligned_tuple_size;
				}

				heap_copytuple_with_recycling(&v_loctup, 
											scan->rs_vistuples_copied[ntup]);
			}

			sscan->counter_in_heap += 1;
			if (modification)
				scan->rs_vistuples[ntup] = lineoff;
			else
				scan->rs_vistuples[ntup] = v_offset;

			/*
			 * Remember visible tuple's location in the page.
			 */
			scan->rs_vistuples_pos_in_block[ntup] = record_idx_in_block;

			ntup++;

			/* Release version buffer */
			if (unlikely(v_buffer != buffer))
				ReleaseBuffer(v_buffer);

			/* Decrement ref_cnt */
			if (rec_ref_unit != 0)
				DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);

			continue;
		}

		/* Release version buffer */
		if (unlikely(v_buffer != buffer))
			ReleaseBuffer(v_buffer);

		/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
		if (snapshot->snapshot_type != SNAPSHOT_MVCC)
			goto failed;

		/*
		 * Both versions are invisible to this transaction.
		 * Need to search from p-leaf & ebi-tree.
		 */
		l_off = PLocatorGetLeftOffset(p_locator);
		r_off = PLocatorGetRightOffset(p_locator);
		xid_bound = PLocatorGetXIDBound(p_locator);

		if (l_off == 0 && r_off == 0)
		{
			ret_id = -1;
		}
		else 
		{
			PLeafOffset p_offset;

			if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
				p_offset = l_off;
			else
				p_offset = r_off;

			ret_id = PLeafLookupTuple(rel_id, 
									  true,
									  &version_offset,
									  p_offset, 
									  snapshot, 
									  &(v_loctup.t_len), 
									  (void**) &(v_loctup.t_data));
		}

		/* If head version is visible in memory, get that version */
		if (ret_id > -1)
		{
			/* We need to perform a memory copy to fetch a version. */

			if (tuple_size_available(scan, ntup, &v_loctup))
			{
				/* We can reuse the existing tuple structure. */
				if (scan->rs_vistuples_copied[ntup] == NULL)
				{
					scan->rs_vistuples_copied[ntup] =
											scan->rs_vistuples_freearray[ntup]; 

					scan->rs_vistuples_freearray[ntup] = NULL;
				}
			}
			else
			{
				HeapTuple tuple;
				int aligned_tuple_size;
		
				/* 
				 * We cannot reuse the existing tuple structure. In such 
				 * cases, we reallocate a new structure to fetch a version. 
				 */
				aligned_tuple_size = 
								1 << my_log2(HEAPTUPLESIZE + v_loctup.t_len);
				tuple = (HeapTuple) palloc(aligned_tuple_size);

				remove_redundant_tuple(scan, ntup);

				scan->rs_vistuples_copied[ntup] = tuple;
				scan->rs_vistuples_size[ntup] = aligned_tuple_size;
			}

			heap_copytuple_with_recycling(&v_loctup, 
											scan->rs_vistuples_copied[ntup]);

			/* Unpin a EBI sub page */
			UnpinEbiSubBuffer(ret_id);

			scan->rs_vistuples[ntup] = v_offset;
			ntup++;
		
			++(sscan->counter_in_ebi);
		}
		else if (ret_id == -2)
		{
			/* 
			 * The visible version is located in the EBI page, and we will 
			 * access it at EBI scan phase. 
			 */
			InsertUnobtainedVersionOffset(scan, version_offset);

			++(sscan->counter_in_ebi);
		}

failed:
		/* Decrement ref_cnt */
		if (rec_ref_unit != 0)
			DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);
	}
		
	if (BufferIsValid(buffer))
#ifdef USING_LOCK
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

	Assert(ntup <= MaxHeapTuplesPerPage);
	scan->rs_ntuples = ntup;

#ifdef LOCATOR_DEBUG
	if ((lines / 3) != valid_records)
		fprintf(stderr, "[locatorgetpage] level: %u, partNum: %u, partGen: %u, block: %u, recCnt: %u, valid versions: %u\n",
					part_level, part_number, part_gen,
					c_block, lines/3, valid_records);
#endif /* LOCATOR_DEBUG */
}

/* ----------------
 *		locatorgettup - fetch next heap tuple from LOCATOR partition
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup with
 * 		scan->rs_ctup_copied, or set scan->rs_ctup.t_data = NULL if no more
 * 		tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void
locatorgettup(LocatorScanDesc lscan,
			  ScanDirection dir,
			  int nkeys,
			  ScanKey key,
			  LocatorExecutor *locator_executor)
{
	HeapScanDesc scan = (HeapScanDesc) lscan;

	LocatorPartInfo		part_info;
	LocatorPartLevel	part_level;
	LocatorPartNumber	part_number;
	LocatorPartGenNo	part_gen;

	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_base.rs_snapshot;
	bool		finished;
	Page		dp;
	int			lines;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;
	bool		valid;

	Relation relation;
	Oid		 rel_id;

	Item p_locator;
	uint64 l_off, r_off;
	TransactionId xid_bound;
	int ret_id;

	BlockNumber		c_block;
	Buffer			buffer;
	DualRefDescData	drefDesc;
	uint64			rec_ref_unit;
	uint8			check_var;
	bool			set_hint_bits;
	Buffer			hint_bits_buf;

	HeapTupleData	v_loctup;
	BlockNumber		v_block;
	OffsetNumber	v_offset;
	Buffer			v_buffer;
	Page			v_dp;
	ItemId			v_lpp;
	
	bool modification = (curr_cmdtype == CMD_UPDATE ||
						 curr_cmdtype == CMD_DELETE);

	relation = scan->rs_base.rs_rd;
	rel_id = RelationGetRelid(relation);
	v_loctup.t_tableOid = rel_id;

#ifdef USING_LOCK
	drefDesc.dual_ref = NULL;
#endif /* USING_LOCK */

	if (!scan->rs_inited)
	{
		/*
		 * return null immediately if relation is empty
		 */
		if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}
		if (scan->rs_base.rs_parallel != NULL)
		{
			// ParallelBlockTableScanDesc pbscan =
			// (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			// ParallelBlockTableScanWorker pbscanwork =
			// scan->rs_parallelworkerdata;

			// table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
			// 											pbscanwork, pbscan);

			// page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
			// 											pbscanwork, pbscan);

			// /* Other processes might have already finished the scan. */
			// if (page == InvalidBlockNumber)
			// {
			// 	Assert(!BufferIsValid(scan->rs_cbuf));
			// 	tuple->t_data = NULL;
			// 	return;
			// }
		}

		/*
		 * If the target level a columar layout level, we must use
		 * the locator's call path. Notice it.
		 */
		if (locator_is_columnar_layout(locator_executor, lscan->c_level))
		{
			if (BufferIsValid(scan->rs_cbuf))
			{
				ReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
			}

			lscan->locator_columnar_layout_scan_started = true;
			lscan->c_rec_position = 0;
			return;
		}

		locatorgetpage((TableScanDesc) lscan, false, NULL, NULL);

		lineoff = FirstOffsetNumber;	/* first offnum */
		scan->rs_inited = true;
	}
	else
	{
		/* continue from previously returned page/tuple */
		lineoff =			/* next offnum */
			OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
	}

	c_block = lscan->c_block; /* current page */
	part_info = lscan->c_part_info;
	part_level = lscan->c_level;
	part_number = part_info->partNum;
	part_gen = part_info->partGen;

#ifdef USING_LOCK
	LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
#else /* !USING_LOCK */
	/* Get dual_ref for avoiding race with heap_insert() */
	drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(scan->rs_cbuf);
	SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK */

	dp = BufferGetPage(scan->rs_cbuf);
	TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
	/*
	 * rec_cnt of 0 means that this block is the last block of a tiered
	 * mempartition at level 0. (See initlocatorscan())
	 * So, use the max offset of the page to get record count.
	 */
	lines = lscan->c_rec_cnt == 0 ? PageGetMaxOffsetNumber(dp) :
									3 * lscan->c_rec_cnt;
	/* page and lineoff now reference the physically next tid */

	linesleft = lines - lineoff + 1;

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
		/*
		 * Only continue scanning the page while we have lines left.
		 *
		 * Note that this protects us from accessing line pointers past
		 * PageGetMaxOffsetNumber(); both for forward scans when we resume the
		 * table scan, and for when we start scanning a new page.
		 */
		buffer = scan->rs_cbuf;
		while (linesleft > 0)
		{
			/* Don't read version directly */
			if (ItemIdIsNormal(lpp) && LP_IS_PLEAF_FLAG(lpp))
			{
				/* Get p_locator of this record */
				p_locator = PageGetItem(dp, lpp);

				/* Set self pointer of tuple */
				ItemPointerSet(&(v_loctup.t_self), c_block, lineoff);

				/* Init checking variables */
				valid = false;
				v_buffer = buffer;

				/* Increment reference count and determine where to check */
				GetCheckVar(&drefDesc, &rec_ref_unit, &check_var,
							&set_hint_bits, lineoff);

				/* Check visibility of left, right versions with check_var */
				for (uint8 test_var = CHECK_RIGHT; test_var > CHECK_NONE; test_var--)
				{
					/* Is this version need to be checked? */
					if (!(check_var & test_var))
						continue;

					/* Release previous version buffer */
					if (unlikely(v_buffer != buffer))
						ReleaseBuffer(v_buffer);

					/* Set block number and offset */
					v_block = c_block;
					v_offset = lineoff + test_var;

					Assert(BlockNumberIsValid(v_block));
					Assert(OffsetNumberIsValid(v_offset));

					/*
					 * If the version is in the same page with p-locator, just
					 * get it.
					 * Or not, read the buffer that it is in.
					 */
					if (likely(v_block == c_block))
					{
						v_buffer = buffer;
						v_dp = dp;
					}
					else
					{
						Assert(false);
						v_buffer = ReadPartitionBuffer(relation, part_level,
													   part_number, part_gen,
													   v_block);
						Assert(BufferIsValid(v_buffer));
						v_dp = BufferGetPage(v_buffer);
					}

					v_lpp = PageGetItemId(v_dp, v_offset);

					/* The target has never been updated after INSERT */
					if (unlikely(LP_OVR_IS_UNUSED(v_lpp)))
						continue;
						
					v_loctup.t_data = (HeapTupleHeader) PageGetItem(v_dp, v_lpp);
					v_loctup.t_len = ItemIdGetLength(v_lpp);

					/* Set buffer to set hint bits */
					hint_bits_buf = set_hint_bits ? v_buffer : InvalidBuffer;

					/* Check visibility of version */
					valid = HeapTupleSatisfiesVisibility(&v_loctup, snapshot,
														hint_bits_buf);

					if (valid && key != NULL)
						HeapKeyTest(&v_loctup, RelationGetDescr(relation),
									nkeys, key, valid);

					if (valid)
					{
						if (scan->rs_ctup_copied != NULL)
							heap_freetuple(scan->rs_ctup_copied);

						scan->rs_ctup_copied = heap_copytuple(&v_loctup);

						*tuple = *scan->rs_ctup_copied;

						/* Release version buffer */
						if (unlikely(v_buffer != buffer))
							ReleaseBuffer(v_buffer);

						/* Decrement ref_cnt */
						if (rec_ref_unit != 0)
							drefDesc.page_ref_unit += rec_ref_unit;

#ifdef USING_LOCK
						LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
						DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

						return;
					}
				}

				/* Both left and right-side versions are invisible */
				if (modification)
				{
					/*
					 * If this scanning is for update, we don't need to bother
					 * searching deeply.
					 */
					if (scan->rs_ctup_copied != NULL)
						heap_freetuple(scan->rs_ctup_copied);

					/*
					 * We cannot find visible tuple inside the heap page.
					 * Copy one of any tuple in the heap page so that
					 * following ExecStoreBufferHeapTuple can be passed.
					 */
					scan->rs_ctup_copied = heap_copytuple(&v_loctup);

					*tuple = *scan->rs_ctup_copied;

					/* Release previous version buffer */
					if (unlikely(v_buffer != buffer))
						ReleaseBuffer(v_buffer);

					/* Decrement ref_cnt */
					if (rec_ref_unit != 0)
						drefDesc.page_ref_unit += rec_ref_unit;

#ifdef USING_LOCK
					LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

					return;
				}

				/* Release version buffer */
				if (unlikely(v_buffer != buffer))
					ReleaseBuffer(v_buffer);

				/* Only MVCC snapshot can traverse p-leaf & ebi-tree */
				if (snapshot->snapshot_type != SNAPSHOT_MVCC)
					goto failed;

				/*
				 * Both versions are invisible to this transaction.
				 * Need to search from p-leaf & ebi-tree.
				 */
				memcpy(&l_off, p_locator, sizeof(uint64));
				memcpy(&r_off, p_locator + sizeof(uint64), sizeof(uint64));
				memcpy(&xid_bound, p_locator + sizeof(uint64) * 2, sizeof(TransactionId));

				if (l_off == 0 && r_off == 0)
				{
					ret_id = -1;
				}
				else 
				{
					PLeafOffset p_offset;

					if (PLeafIsLeftLookup(l_off, r_off, xid_bound, snapshot))
						p_offset = l_off;
					else
						p_offset = r_off;
	
					ret_id = PLeafLookupTuple(rel_id, 
											  false,
											  NULL,
											  p_offset, 
											  snapshot, 
											  &(v_loctup.t_len), 
											  (void**) &(v_loctup.t_data));
				}

				/* If head version is visible, get that version */
				if (ret_id > -1)
				{
					if (scan->rs_ctup_copied != NULL)
						heap_freetuple(scan->rs_ctup_copied);
		
					scan->rs_ctup_copied = heap_copytuple(&v_loctup);

					*tuple = *scan->rs_ctup_copied;
					
					/* Unpin a EBI sub page */
					UnpinEbiSubBuffer(ret_id);

					/* Decrement ref_cnt */
					if (rec_ref_unit != 0)
						drefDesc.page_ref_unit += rec_ref_unit;

#ifdef USING_LOCK
					LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

					return;
				}

failed:
				/* Decrement ref_cnt */
				if (rec_ref_unit != 0)
					DRefDecrRefCnt(drefDesc.dual_ref, rec_ref_unit);
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			++lpp;			/* move forward in this page's ItemId array */
			++lineoff;
		}

#ifdef USING_LOCK
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

		/*
		 * advance to next/prior page and detect end of scan
		 */
		if (scan->rs_base.rs_parallel != NULL)
		{
			// ParallelBlockTableScanDesc pbscan =
			// (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			// ParallelBlockTableScanWorker pbscanwork =
			// scan->rs_parallelworkerdata;

			// page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
			// 										 pbscanwork, pbscan);
			// finished = (page == InvalidBlockNumber);
		}
		else
		{
			/* LOCATOR uses rs_cblock only to determine if scan is finished */
			finished = (scan->rs_cblock == (scan->rs_nblocks - 1));
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		locatorsetpage(lscan);

		/*
		 * If this is the first change to a columar layout level, we must use
		 * the locator's call path. Notice it.
		 */
		if (locator_is_columnar_layout(locator_executor, lscan->c_level))
		{
			if (BufferIsValid(scan->rs_cbuf))
			{
				ReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
			}

			lscan->locator_columnar_layout_scan_started = true;
			lscan->c_rec_position = 0;
			return;
		}

		locatorgetpage((TableScanDesc) scan, false, NULL, NULL);

		c_block = lscan->c_block;
		part_info = lscan->c_part_info;
		part_level = lscan->c_level;
		part_number = part_info->partNum;
		part_gen = part_info->partGen;

#ifdef USING_LOCK
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
#else /* !USING_LOCK*/
		/* Get dual_ref for avoiding race with heap_insert() */
		drefDesc.dual_ref = (pg_atomic_uint64*)GetBufferDualRef(scan->rs_cbuf);
		SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK*/

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = lscan->c_rec_cnt == 0 ? PageGetMaxOffsetNumber(dp) :
										3 * lscan->c_rec_cnt;
		linesleft = lines;
		lineoff = FirstOffsetNumber;
		lpp = PageGetItemId(dp, FirstOffsetNumber);
	}
}

/* ----------------
 *		locatorgettup_pagemode - fetch next heap tuple from locator partition
 *								 in page-at-a-time mode
 *
 *		Same API as locatorgettup, but used in page-at-a-time mode
 *
 * The internal logic is much the same as locatorgettup's too, but there are
 * some differences: we do not take the buffer content lock (that only needs to
 * happen inside locatorgetpage), and we iterate through just the tuples listed
 * in rs_vistuples[] rather than all tuples on the page.  Notice that
 * lineindex is 0-based, where the corresponding loop variable lineoff in
 * heapgettup is 1-based.
 * ----------------
 */
static void
locatorgettup_pagemode(LocatorScanDesc lscan,
					   ScanDirection dir,
					   int nkeys,
					   ScanKey key,
					   LocatorExecutor *locator_executor)
{
	HeapScanDesc scan = (HeapScanDesc) lscan;
	HeapTuple	tuple = &(scan->rs_ctup);
	bool		finished;
	Page		dp;
	int			lines;
	int			lineindex;
	OffsetNumber lineoff;
	int			linesleft;
	ItemId		lpp;

	if (!scan->rs_inited)
	{
		/*
		 * return null immediately if relation is empty
		 */
		if (scan->rs_nblocks == 0 || scan->rs_numblocks == 0)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}
		if (scan->rs_base.rs_parallel != NULL)
		{
			// ParallelBlockTableScanDesc pbscan =
			// (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			// ParallelBlockTableScanWorker pbscanwork =
			// scan->rs_parallelworkerdata;

			// table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
			// 											pbscanwork, pbscan);

			// page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
			// 										 pbscanwork, pbscan);

			/* Other processes might have already finished the scan. */
			// if (page == InvalidBlockNumber)
			// {
			// 	Assert(!BufferIsValid(scan->rs_cbuf));
			// 	tuple->t_data = NULL;
			// 	return;
			// }
		}

		/*
		 * If the target level a columar layout level, we must use
		 * the locator's call path. Notice it.
		 */
		if (locator_is_columnar_layout(locator_executor, lscan->c_level))
		{
			if (BufferIsValid(scan->rs_cbuf))
			{
				ReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
			}

			lscan->locator_columnar_layout_scan_started = true;
			lscan->c_rec_position = 0;
			return;
		}

		locatorgetpage((TableScanDesc) lscan, false, NULL, NULL);

		lineindex = 0;
		scan->rs_inited = true;
	}
	else
	{
		/* continue from previously returned page/tuple */
		lineindex = scan->rs_cindex + 1;
	}

	if (BufferIsValid(scan->rs_cbuf))
		dp = BufferGetPage(scan->rs_cbuf);
	else
		dp = scan->c_readbuf_dp;

	TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
	lines = scan->rs_ntuples;
	/* page and lineindex now reference the next visible tid */

	linesleft = lines - lineindex;

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		while (linesleft > 0)
		{
			lineoff = scan->rs_vistuples[lineindex];
			lpp = PageGetItemId(dp, lineoff);
			Assert(ItemIdIsNormal(lpp));

			if (scan->rs_vistuples_copied[lineindex])
			{
				*tuple = *(scan->rs_vistuples_copied[lineindex]);
			}
			else
			{
				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
			}

			ItemPointerCopy(&(tuple->t_data->t_ctid), &(tuple->t_self));

			/*
			 * if current tuple qualifies, return it.
			 */
			if (key != NULL)
			{
				bool		valid;

				HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd),
							nkeys, key, valid);
				if (valid)
				{
					scan->rs_cindex = lineindex;
					return;
				}
			}
			else
			{
				scan->rs_cindex = lineindex;
				return;
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (scan->rs_base.rs_parallel != NULL)
		{
			// ParallelBlockTableScanDesc pbscan =
			// (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
			// ParallelBlockTableScanWorker pbscanwork =
			// scan->rs_parallelworkerdata;

			// page = table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
			// 										 pbscanwork, pbscan);
			// finished = (page == InvalidBlockNumber);
		}
		else
		{
			/* LOCATOR uses rs_cblock only to determine if scan is finished */
			finished = (scan->rs_cblock == (scan->rs_nblocks - 1));
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);

			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		locatorsetpage(lscan);

		/*
		 * If this is the first change to a columar layout level, we must use
		 * the locator's call path. Notice it.
		 */
		if (locator_is_columnar_layout(locator_executor, lscan->c_level))
		{
			if (BufferIsValid(scan->rs_cbuf))
			{
				ReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
			}

			lscan->locator_columnar_layout_scan_started = true;
			lscan->c_rec_position = 0;
			return;
		}

		locatorgetpage((TableScanDesc) scan, false, NULL, NULL);

		if (BufferIsValid(scan->rs_cbuf))
			dp = BufferGetPage(scan->rs_cbuf);
		else
			dp = scan->c_readbuf_dp;

		TestForOldSnapshot(scan->rs_base.rs_snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		linesleft = lines;
		lineindex = 0;
	}
}

HeapTuple
locator_getnext(TableScanDesc sscan, ScanDirection direction)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc scan = (HeapScanDesc) sscan;
	bool pagemode = (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);
	HeapTuple scanned_tuple;

	/*
	 * We don't expect direct calls to locator_getnext with valid CheckXidAlive
	 * for catalog or regular tables.  See detailed comments in xact.c where
	 * these variables are declared.  Normally we have such a check at tableam
	 * level API but this is called from many places so we need to ensure it
	 * here.
	 */
	if (unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan))
		elog(ERROR, "unexpected heap_getnext call during logical decoding");

	/* Note: no locking manipulations needed */

	if (pagemode)
		locatorgettup_pagemode(lscan, direction,
				   			   scan->rs_base.rs_nkeys, scan->rs_base.rs_key,
							   NULL);
	else
		locatorgettup(lscan, direction,
					  scan->rs_base.rs_nkeys, scan->rs_base.rs_key,
					  NULL);

	if (scan->rs_ctup.t_data == NULL)
		return NULL;

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

	if (pagemode)
	{
		if (scan->rs_vistuples_copied[scan->rs_cindex])
		{
			scanned_tuple = scan->rs_vistuples_copied[scan->rs_cindex];
		}
		else
		{
			/* 
			 * We get versions from read buffer, then we don't copy the 
			 * version.
			 */
			scanned_tuple = &scan->rs_ctup;
		}
	}
	else
	{
		scanned_tuple = scan->rs_ctup_copied;
		scan->rs_ctup_copied = NULL;
	}

	return scanned_tuple;
}

/*
 * Initialize an I/O prefetch descriptor to be used in sequential scan.
 *
 * The size of tuple is differnt for each column group. So the number of pages
 * required for each group is also differnt.
 *
 * If we manage the I/O of all column groups with one prefetch descriptor, the
 * implementaion becomes complicated, so we use differnt prefetch descriptor for
 * each column group. 
 *
 * Column groups may be configured differently for each level. So we initialize
 * the prefetch descriptors at each level seperately.
 */
static void
locator_init_prefetch_desc(LocatorScanDesc lscan,
						   Relation rel,
						   LocatorExecutorLevelDesc *level_desc)
{
	ListCell *lc;

	level_desc->prefetch_inited = true;

	foreach(lc, level_desc->required_column_group_desc_list)
	{
		LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc);
		PrefetchDesc prefetch_desc = get_locator_prefetch_desc(rel, lscan);
		PrefetchPartitionDesc prefetch_part_desc;
		LocatorPartInfo part_info;

		/* When io_uring is not supported */ 
		if (prefetch_desc == NULL)
			return;

		prefetch_part_desc = (PrefetchPartitionDesc) prefetch_desc;
		part_info = lfirst(prefetch_part_desc->c_lc);

		/*
	 	 * Currently, prefetch descriptor is created for ROW-ORIENTED layout.
		 * So we must change it to match the layout of the current level.
		 */
		prefetch_part_desc->is_columnar_layout = true;
		prefetch_part_desc->locator_last_blocknum
			= locator_calculate_columnar_tuple_blocknum(level_desc,
														group_desc,
														part_info->recCnt - 1);
		prefetch_part_desc->locator_level_desc = level_desc;
		prefetch_part_desc->locator_group_desc = group_desc;
		prefetch_part_desc->locator_current_block_idx_in_record_zone = 0;
		prefetch_part_desc->locator_current_record_zone_id = 0;

		prefetch_desc->next_reqblock
			= group_desc->start_block_offset_in_record_zone;

		group_desc->prefetch_partition_desc = prefetch_part_desc;
	}
}

/*
 * Release prefetch descriptor.
 */
static void
locator_release_prefetch_desc_of_level(LocatorExecutor *executor,
									   int level)
{
	LocatorExecutorLevelDesc *level_desc;
	ListCell *lc;

	level_desc = locator_get_level_columnar_desc(executor, level);

#ifdef LOCATOR_DEBUG
	Assert(level_desc != NULL);
#endif /* LOCATOR_DEBUG */

	foreach(lc, level_desc->required_column_group_desc_list)
	{
		LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc);
		PrefetchDesc prefetch_desc
			= (PrefetchDesc) group_desc->prefetch_partition_desc;

		if (prefetch_desc != NULL)
		{
			/*
			 * TODO LOCATORs columnar heapscan does not support EBI scan yet.
			 */
			ReleasePrefetchDesc(prefetch_desc, true);
			group_desc->prefetch_partition_desc = NULL;
		}
	}

	level_desc->prefetch_inited = false;
}

/*
 * If we've finished reading the current partition,
 * find a new partition and set it to the current partition.
 */
static void
locator_set_partition_to_scan(LocatorScanDesc lscan,
							  LocatorExecutor *locator_executor)
{
	Relation rel = lscan->base.rs_base.rs_rd;
	LocatorPartInfo next_part_info;
	ListCell *next_part_info_lc;
	List *part_info_list;

#ifdef LOCATOR_DEBUG
	Assert(lscan->c_part_info != NULL);
#endif /* LOCATOR_DEBUG */

	/* 
	 * This partition has not yet been fully read. No need to move.
	 */
	if (lscan->c_rec_position < lscan->c_part_info->recCnt)
		return;

	part_info_list = lscan->part_infos_to_scan[lscan->c_level];
	next_part_info_lc = lnext(part_info_list, lscan->c_lc);

	/*
	 * If there are no more partitions to read at this level,
	 * move to the next level.
	 */
	if (next_part_info_lc == NULL)
	{
		if (enable_prefetch)
			locator_release_prefetch_desc_of_level(locator_executor,
												   lscan->c_level);

		lscan->c_level++;

		/*
		 * If the next level is larger than the last level, set the current
		 * partition info to NULL.
		 *
		 * Then the caller will know that there is no partition to read anymore. 
		 */
		if (lscan->c_level == rel->rd_locator_level_count)
		{
			lscan->c_part_info = NULL;
			return;
		}

		part_info_list = lscan->part_infos_to_scan[lscan->c_level];
		next_part_info_lc = list_head(part_info_list);
	}

	next_part_info = (LocatorPartInfo) lfirst(next_part_info_lc);

	/* Search valid partition */
	while (next_part_info->block_cnt == 0)
	{
		next_part_info_lc = lnext(part_info_list, next_part_info_lc);

		/* This level has no valid partitions. Move to the next level */
		if (next_part_info_lc == NULL)
		{
			if (enable_prefetch)
				locator_release_prefetch_desc_of_level(locator_executor,
													   lscan->c_level);

			lscan->c_level++;

			/*
			 * If the next level is larger than the last level, set the current
			 * partition info to NULL.
			 *
			 * Then the caller will know that there is no partition to read anymore. 
			 */
			if (lscan->c_level == rel->rd_locator_level_count)
			{
				lscan->c_part_info = NULL;
				return;
			}

			part_info_list = lscan->part_infos_to_scan[lscan->c_level];
			next_part_info_lc = list_head(part_info_list);
		}

		next_part_info = (LocatorPartInfo) lfirst(next_part_info_lc);
	}

	lscan->c_lc = next_part_info_lc;
	lscan->c_part_info = next_part_info;
	lscan->c_rec_position = 0;
}

/*
 * Function for columnar layout corresponding to locator_getnextslot().
 */
static inline bool
locator_columnar_getnextslot(TableScanDesc sscan, TupleTableSlot *slot,
							 bool pagemode)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	LocatorExecutor *executor = slot->tts_locator_executor;
	LocatorExecutorLevelDesc *level_desc;
	bool found = false;
	ListCell *lc;

#ifdef LOCATOR_DEBUG
	Assert(lscan->c_level > 0);
	Assert(pagemode);
#endif /* LOCATOR_DEBUG */

move_partition:

	/*
	 * Has the scan for the current partition finished?
	 * If so, change the target partition.
	 */
	locator_set_partition_to_scan(lscan, executor);

	/* There is no partition to read anymore */
	if (lscan->c_part_info == NULL)
	{
		/* slot->clear() will release all buffers acquired by LOCATOR */
		ExecClearTuple(slot);
		return false;		
	}

	/*
	 * locator_set_partition_to_scan() can change level.
	 * So get the level descriptor after that.
	 */
	level_desc = locator_get_level_columnar_desc(executor, lscan->c_level);

	if (!level_desc->prefetch_inited && enable_prefetch)
		locator_init_prefetch_desc(lscan, executor->rel, level_desc);

#ifdef LOCATOR_DEBUG
	Assert(level_desc != NULL);
#endif /* LOCATOR_DEBUG */

	found = locator_gettup_and_deform(sscan, level_desc, slot);

	/*
	 * There may be no valid records in the partition we choose.
	 * If so, move to the next partition.
	 */
	if (!found)
	{
		locator_release_all_buffer(executor);
		goto move_partition;
	}

	/*
	 * We found the record.
	 *
	 * Since the SIRO page has already been copied, the buffer can be released.
	 *
	 * But non-SIRO page must remain in the buffer. This is because the slot
	 * points to the tuple without copying it.
	 */
	foreach(lc, level_desc->required_column_group_desc_list)
	{
		LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc);

		if (!group_desc->isSiro)
			continue;

		if (BufferIsValid(group_desc->c_buf))
		{
			ReleaseBuffer(group_desc->c_buf);
			group_desc->c_buf = InvalidBuffer;
		}
	}

	return true;
}

bool
locator_getnextslot(TableScanDesc sscan, ScanDirection direction,
					TupleTableSlot *slot)
{
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc scan = (HeapScanDesc) lscan;
	LocatorExecutor *locator_executor = slot->tts_locator_executor;

	bool pagemode = (scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);

	HeapTuple scanned_tuple;

#ifdef LOCATOR_DEBUG
	Oid relid = lscan->base.rs_base.rs_rd->rd_id;
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(relid);

	if (exCatalog != NULL)
	{
		for (int level = 0; level <= exCatalog->lastPartitionLevel; level++)
		{
			uint8 layout_id = exCatalog->layoutArray[level];

			if (layout_id != ROW_ORIENTED)
			{
				Assert(locator_executor != NULL);
				break;
			}
		}
	}
#endif /* LOCATOR_DEBUG */

	Assert(IsSiro(scan->rs_base.rs_rd));
	Assert(ScanDirectionIsForward(direction));

	/* Note: no locking manipulations needed */

	if (lscan->locator_columnar_layout_scan_started)
		return locator_columnar_getnextslot(sscan, slot, pagemode);

	if (pagemode)
		locatorgettup_pagemode(lscan, direction,
				   			   scan->rs_base.rs_nkeys, scan->rs_base.rs_key,
							   locator_executor);
	else
		locatorgettup(lscan, direction,
					  scan->rs_base.rs_nkeys, scan->rs_base.rs_key,
					  locator_executor);

	/*
	 * In the above functions, scan level can be changed. So check again.
	 *
	 * This branch occurs only once in a sequential scan.
	 */
	if (unlikely(lscan->locator_columnar_layout_scan_started))
		return locator_columnar_getnextslot(sscan, slot, pagemode);

	if (scan->rs_ctup.t_data == NULL)
	{
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

	if (pagemode)
	{
		if (scan->rs_vistuples_copied[scan->rs_cindex])
		{
			scanned_tuple = scan->rs_vistuples_copied[scan->rs_cindex];
		}
		else
		{
			/* 
			 * We get versions from read buffer, then we don't copy the 
			 * version.
			 */
			scanned_tuple = &scan->rs_ctup;
		}
	}
	else
	{
		scanned_tuple = scan->rs_ctup_copied;
		scan->rs_ctup_copied = NULL;
	}

	ExecStoreBufferHeapTuple(scanned_tuple, slot, scan->rs_cbuf);

	return true;
}
#endif /* LOCATOR */
