/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAM_H
#define HEAPAM_H

#include "access/relation.h"	/* for backward compatibility */
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"		/* for backward compatibility */
#include "access/tableam.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "storage/ebi_tree.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/dsm.h"
#include "storage/lockdefs.h"
#include "storage/shm_toc.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#ifdef LOCATOR
#include <liburing.h>
#include "pg_refcnt.h"
#include "locator/locator_external_catalog.h"
#include "locator/locator_partitioning.h"
#endif /* LOCATOR */

#ifdef LOCATOR
#include "locator/locator_executor.h"
#endif /* LOCATOR */

#define PLocatorGetLeftOffset(p_locator) (*((uint64 *) p_locator))
#define PLocatorGetRightOffset(p_locator) \
			(*((uint64 *) (p_locator + sizeof(uint64))))
#define PLocatorGetXIDBound(p_locator) \
			(*((TransactionId *) (p_locator + sizeof(uint64) * 2)))

/* "options" flag bits for heap_insert */
#define HEAP_INSERT_SKIP_FSM	TABLE_INSERT_SKIP_FSM
#define HEAP_INSERT_FROZEN		TABLE_INSERT_FROZEN
#define HEAP_INSERT_NO_LOGICAL	TABLE_INSERT_NO_LOGICAL
#define HEAP_INSERT_SPECULATIVE 0x0010

typedef struct BulkInsertStateData *BulkInsertState;
struct TupleTableSlot;

#define MaxLockTupleMode	LockTupleExclusive

/*
 * Descriptor for heap table scans.
 */
typedef struct HeapScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* state set up at initscan time */
	BlockNumber rs_nblocks;		/* total number of blocks in rel */
	BlockNumber rs_startblock;	/* block # to start at */
	BlockNumber rs_numblocks;	/* max number of blocks to scan */
	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */

	/* scan current state */
	bool		rs_inited;		/* false = scan not init'd yet */
	BlockNumber rs_cblock;		/* current block # in scan, if any */
	Buffer		rs_cbuf;		/* current buffer in scan, if any */
	/* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

	/* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */
	BufferAccessStrategy rs_strategy;	/* access strategy for reads */

	HeapTupleData rs_ctup;		/* current tuple in scan, if any */

	/*
	 * For parallel scans to store page allocation data.  NULL when not
	 * performing a parallel scan.
	 */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;

	/* these fields only used in page-at-a-time mode and for bitmap scans */
	int			rs_cindex;		/* current tuple's index in vistuples */
	int			rs_ntuples;		/* number of visible tuples on page */
	OffsetNumber rs_vistuples[MaxHeapTuplesPerPage];	/* their offsets */

#ifdef DIVA
	/*
	 * Original postgres does not in-place update for making a new version   
	 * tuple when executing update query. Because of this, a transaction     
	 * finding a visible version in a heap page releases the page latch      
	 * right after it finds the tuple.                                       
	 * For the implementation of the siro, we do in-place update on     
	 * a heap tuple, and it incurs another race problem. A reader transaction
	 * only stores the pointer of the visible heap tuple and then releases   
	 * the page latch. Before retrieving the actual contents of the tuple,   
	 * another transaction can overwrite the tuple with a new one, so the    
	 * reader could see a different tuple from it actually found.            
	 * So we need to memcpy (heap_copytup) the visible heap tuple into the   
	 * copied_tuple here before releasing the page latch.                    
	 *                                                                       
	 * This is for range query, and same approach is applied to point lookup.
	 * This array is initialized at heap_beginscan.                          
	 */
	HeapTuple 	rs_vistuples_copied[MaxHeapTuplesPerPage];

#ifdef LOCATOR
	int			rs_vistuples_pos_in_block[MaxHeapTuplesPerPage];
#endif /* LOCATOR */

	/* We use free array for recycling. */
	HeapTuple 	rs_vistuples_freearray[MaxHeapTuplesPerPage];
	int 		rs_vistuples_size[MaxHeapTuplesPerPage]; /* size */

	bool 		is_first_ebi_scan;
	int 		c_buf_id;
	int 		c_page_id;
	int 		c_max_page_id;
	int 		seg_index;
	int 		max_seg_index;
	uint64		c_offset;

	/*
	 * We store a summary of the version offsets information, processed in 
	 * segment units. If the # of segments exceeds capacity, we double its size 
	 * and copy the data into a new array.
	 */
	EbiUnobtainedSegment *unobtained_segments;	/* array of pointer */
	uint64_t			unobtained_segments_capacity; /* maximum # */
	uint64_t			unobtained_segment_nums; /* # of segments */

	/*
	 * We record both unobtained and visible version offsets during the heap 
	 * page scanning process. If the # of version offsets exceeds capacity, 
	 * we double its size and copy the data into a new array.
	 */
	EbiSubVersionOffset *unobtained_version_offsets; /* array of version offset */
	uint64_t			unobtained_versions_capacity; /* maximum # */
	uint64_t 			unobtained_version_nums; /* # of version offsets */

	HeapTuple	rs_ctup_copied;		/* current copied tuple (for heapgettup) */
#ifdef LOCATOR
	Page		c_readbuf_dp;
#endif /* LOCATOR */
#endif /* DIVA */
} HeapScanDescData;

typedef struct HeapScanDescData *HeapScanDesc;

#ifdef LOCATOR
/*
 * Descriptor for locator partition scans.
 */
typedef struct LocatorScanDescData
{
	struct HeapScanDescData base;

	List **part_infos_to_scan;			/* partition infos of each level */

	ListCell *c_lc;
	LocatorPartInfo c_part_info;
	LocatorPartLevel c_level;
	BlockNumber c_block;
	int c_rec_cnt;

	LocatorPartNumber level_zero_part_nums[MAX_SPREAD_FACTOR];
	int level_zero_part_cnt;
#ifdef LOCATOR
	bool locator_columnar_layout_scan_started;
	LocatorSeqNum c_rec_position;
#endif /* LOCATOR */
#ifdef LOCATOR_DEBUG
	uint32 c_scanned_records;
	uint32 c_valid_versions;
#endif /* LOCATOR_DEBUG */
} LocatorScanDescData;
typedef struct LocatorScanDescData *LocatorScanDesc;

typedef enum VersionVisibilityStatus
{
	VERSION_INPROGRESS, /*	xmin: in-progress,		xmax: ----				*/
	VERSION_INSERTED,	/*	xmin: committed,		xmax: ----				*/
	VERSION_ABORTED		/*	xmin: invalid(aborted),	xmax: ----				*/
} VersionVisibilityStatus;
#endif /* LOCATOR */

/*
 * Descriptor for fetches from heap via an index.
 */
typedef struct IndexFetchHeapData
{
	IndexFetchTableData xs_base;	/* AM independent part of the descriptor */

	Buffer		xs_cbuf;		/* current heap buffer in scan, if any */
	/* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

#ifdef DIVA
	int			xs_c_ebi_buf_id;

	HeapTuple 	xs_vistuple_free; /* for recyling of tuple structure*/
	int			xs_vistuple_size;
#endif /* DIVA */
} IndexFetchHeapData;

/* Result codes for HeapTupleSatisfiesVacuum */
typedef enum
{
	HEAPTUPLE_DEAD,				/* tuple is dead and deletable */
	HEAPTUPLE_LIVE,				/* tuple is live (committed, no deleter) */
	HEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	HEAPTUPLE_INSERT_IN_PROGRESS,	/* inserting xact is still in progress */
	HEAPTUPLE_DELETE_IN_PROGRESS	/* deleting xact is still in progress */
} HTSV_Result;


#ifdef DIVA 
/* Tricky variable for passing the cmd type from ExecutePlan to heap code */
extern CmdType curr_cmdtype;
#endif

/* ----------------
 *		function prototypes for heap access method
 *
 * heap_create, heap_create_with_catalog, and heap_drop_with_catalog
 * are declared in catalog/heap.h
 * ----------------
 */


/*
 * HeapScanIsValid
 *		True iff the heap scan is valid.
 */
#define HeapScanIsValid(scan) PointerIsValid(scan)

#ifdef LOCATOR
extern bool is_prefetching_needed(Relation relation, HeapScanDesc scan);
extern PrefetchDesc get_prefetch_desc(Relation relation, Size size);
extern PrefetchDesc get_locator_prefetch_desc(Relation relation,
											  LocatorScanDesc lscan);
extern PrefetchDesc get_heap_prefetch_desc(Relation relation, HeapScanDesc scan);
extern void release_prefetch_desc(PrefetchDesc prefetch_desc);
extern TableScanDesc heap_beginscan(Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key,
									ParallelTableScanDesc parallel_scan,
									uint32 flags,
									List *hap_partid_list);
#else /* !LOCATOR */
extern TableScanDesc heap_beginscan(Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key,
									ParallelTableScanDesc parallel_scan,
									uint32 flags);
#endif /* LOCATOR */
extern void heap_setscanlimits(TableScanDesc scan, BlockNumber startBlk,
							   BlockNumber numBlks);
#ifdef DIVA
extern void heapgetpage_DIVA(TableScanDesc sscan, BlockNumber page);
#endif /* DIVA */
extern void heapgetpage(TableScanDesc scan, BlockNumber page);
extern void heap_rescan(TableScanDesc scan, ScanKey key, bool set_params,
						bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void heap_endscan(TableScanDesc scan);
extern HeapTuple heap_getnext(TableScanDesc scan, ScanDirection direction);
extern bool heap_getnextslot(TableScanDesc sscan,
							 ScanDirection direction, struct TupleTableSlot *slot);
extern void heap_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
							  ItemPointer maxtid);
extern bool heap_getnextslot_tidrange(TableScanDesc sscan,
									  ScanDirection direction,
									  TupleTableSlot *slot);
extern bool heap_fetch(Relation relation, Snapshot snapshot,
					   HeapTuple tuple, Buffer *userbuf, bool keep_buf);

#ifdef LOCATOR
extern void LocatorRouteSynopsisGetTuplePositionForIndexscan(
									LocatorExternalCatalog *exCatalog, 
									Relation relation, 
									LocatorRouteSynopsis routeSynopsis,
									LocatorTuplePosition tuplePosition);

extern void LocatorRouteSynopsisGetTuplePositionForUpdate(LocatorExternalCatalog *exCatalog, 
									Relation relation, 
									LocatorRouteSynopsis routeSynopsis,
									LocatorTuplePosition tuplePosition);

#ifdef LOCATOR
extern bool locator_search_version(LocatorTuplePosition tuplePosition, 
								   Relation relation, 
								   Buffer buffer, Snapshot snapshot,
								   HeapTuple heapTuple,
								   HeapTuple *copied_tuple,
								   DualRefDesc drefDesc,
								   bool *all_dead, bool first_call,
								   IndexFetchHeapData *hscan,
								   BlockNumber locator_tuple_blocknum,
								   uint16 locator_tuple_position_in_block);
#else /* !LOCATOR */
extern bool locator_search_version(LocatorTuplePosition tuplePosition, 
								   Relation relation, 
								   Buffer buffer, Snapshot snapshot,
								   HeapTuple heapTuple,
								   HeapTuple *copied_tuple,
								   DualRefDesc drefDesc,
								   bool *all_dead, bool first_call,
								   IndexFetchHeapData *hscan);
#endif /* LOCATOR */
#endif
#ifdef LOCATOR
extern bool locator_search_and_deform_version(Relation rel,
											  LocatorExecutorLevelDesc *desc,
											  TupleTableSlot *slot,
											  Snapshot snapshot,
											  LocatorTuplePosition tuppos, 
											  IndexFetchHeapData *hscan,
											  bool is_modification);

extern bool locator_gettup_and_deform(TableScanDesc sscan,
									  LocatorExecutorLevelDesc *level_desc,
									  TupleTableSlot *slot);
#endif /* LOCATOR */

#ifdef DIVA
#ifdef LOCATOR
extern bool heap_hot_search_buffer_with_vc(ItemPointer tid, Relation relation,
										   Buffer buffer, Snapshot snapshot,
										   HeapTuple heapTuple,
										   HeapTuple *copied_tuple,
										   DualRefDesc drefDesc,
										   bool *all_dead,
										   bool first_call,
										   IndexFetchHeapData *hscan);
#else /* !LOCATOR */
extern bool heap_hot_search_buffer_with_vc(ItemPointer tid, Relation relation,
										   Buffer buffer, Snapshot snapshot,
										   HeapTuple heapTuple,
										   HeapTuple *copied_tuple,
										   bool *all_dead,
										   bool first_call,
										   IndexFetchHeapData *hscan);

#endif /* LOCATOR */
#endif /* DIVA */

extern bool heap_hot_search_buffer(ItemPointer tid, Relation relation,
								   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
								   bool *all_dead, bool first_call);

extern void heap_get_latest_tid(TableScanDesc scan, ItemPointer tid);

extern BulkInsertState GetBulkInsertState(void);
extern void FreeBulkInsertState(BulkInsertState);
extern void ReleaseBulkInsertStatePin(BulkInsertState bistate);

extern void heap_insert(Relation relation, HeapTuple tup, CommandId cid,
						int options, BulkInsertState bistate);
extern void heap_multi_insert(Relation relation, struct TupleTableSlot **slots,
							  int ntuples, CommandId cid, int options,
							  BulkInsertState bistate);
							  
#ifdef DIVA
extern TM_Result heap_delete_with_vc(Relation relation, ItemPointer tid,
									 CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait, 
									 struct TM_FailureData *tmfd, bool changingPart);
#endif

extern TM_Result heap_delete(Relation relation, ItemPointer tid,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, bool changingPart);
extern void heap_finish_speculative(Relation relation, ItemPointer tid);
extern void heap_abort_speculative(Relation relation, ItemPointer tid);

#ifdef DIVA
extern TM_Result heap_update_with_vc(Relation relation, ItemPointer otid,
#ifdef LOCATOR
									 LocatorTuplePosition tuplePosition,
#endif
#ifdef LOCATOR
									 LocatorExecutor *locator_executor,
#endif /* LOCATOR */
									 HeapTuple newtup, CommandId cid,
									 Snapshot snapshot, Snapshot crosscheck,
									 bool wait, TM_FailureData *tmfd,
									 LockTupleMode *lockmode);
#endif

extern TM_Result heap_update(Relation relation, ItemPointer otid,
							 HeapTuple newtup,
							 CommandId cid, Snapshot crosscheck, bool wait,
							 struct TM_FailureData *tmfd, LockTupleMode *lockmode);
extern TM_Result heap_lock_tuple(Relation relation, HeapTuple tuple,
								 CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
								 bool follow_update,
								 Buffer *buffer, struct TM_FailureData *tmfd);

extern void heap_inplace_update(Relation relation, HeapTuple tuple);
extern bool heap_freeze_tuple(HeapTupleHeader tuple,
							  TransactionId relfrozenxid, TransactionId relminmxid,
							  TransactionId cutoff_xid, TransactionId cutoff_multi);
extern bool heap_tuple_would_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
									MultiXactId cutoff_multi,
									TransactionId *relfrozenxid_out,
									MultiXactId *relminmxid_out);
extern bool heap_tuple_needs_eventual_freeze(HeapTupleHeader tuple);

extern void simple_heap_insert(Relation relation, HeapTuple tup);
extern void simple_heap_delete(Relation relation, ItemPointer tid);
extern void simple_heap_update(Relation relation, ItemPointer otid,
							   HeapTuple tup);

extern TransactionId heap_index_delete_tuples(Relation rel,
											  TM_IndexDeleteOp *delstate);

/* in heap/pruneheap.c */
struct GlobalVisState;
extern void heap_page_prune_opt(Relation relation, Buffer buffer);
extern int	heap_page_prune(Relation relation, Buffer buffer,
							struct GlobalVisState *vistest,
							TransactionId old_snap_xmin,
							TimestampTz old_snap_ts_ts,
							int *nnewlpdead,
							OffsetNumber *off_loc);
extern void heap_page_prune_execute(Buffer buffer,
									OffsetNumber *redirected, int nredirected,
									OffsetNumber *nowdead, int ndead,
									OffsetNumber *nowunused, int nunused);
extern void heap_get_root_tuples(Page page, OffsetNumber *root_offsets);

/* in heap/vacuumlazy.c */
struct VacuumParams;
extern void heap_vacuum_rel(Relation rel,
							struct VacuumParams *params, BufferAccessStrategy bstrategy);

/* in heap/heapam_visibility.c */
extern bool HeapTupleSatisfiesVisibility(HeapTuple stup, Snapshot snapshot,
										 Buffer buffer);
extern bool IsVersionOld(HeapTuple htup);
extern TM_Result HeapTupleSatisfiesUpdate(HeapTuple stup, CommandId curcid,
										  Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuum(HeapTuple stup, TransactionId OldestXmin,
											Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuumWithoutWrap(HeapTuple stup, 
													   TransactionId OldestXmin,
													   Buffer buffer);
extern HTSV_Result HeapTupleSatisfiesVacuumHorizon(HeapTuple stup, Buffer buffer,
												   TransactionId *dead_after);
extern void HeapTupleSetHintBits(HeapTupleHeader tuple, Buffer buffer,
								 uint16 infomask, TransactionId xid);
extern bool HeapTupleHeaderIsOnlyLocked(HeapTupleHeader tuple);
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);
extern bool HeapTupleIsSurelyDead(HeapTuple htup,
								  struct GlobalVisState *vistest);

/*
 * To avoid leaking too much knowledge about reorderbuffer implementation
 * details this is implemented in reorderbuffer.c not heapam_visibility.c
 */
struct HTAB;
extern bool ResolveCminCmaxDuringDecoding(struct HTAB *tuplecid_data,
										  Snapshot snapshot,
										  HeapTuple htup,
										  Buffer buffer,
										  CommandId *cmin, CommandId *cmax);
extern void HeapCheckForSerializableConflictOut(bool valid, Relation relation, HeapTuple tuple,
												Buffer buffer, Snapshot snapshot);

#ifdef LOCATOR
extern void SetPageRefUnit(DualRefDesc drefDesc);
extern void GetCheckVar(DualRefDesc drefDesc, uint64 *rec_ref_unit, uint8 *check_var,
						bool *set_hint_bits, OffsetNumber offnum);
extern enum VersionVisibilityStatus
HeapTupleGetVisibilityStatus(HeapTuple htup, Buffer buffer);
extern void LocatorRouteSynopsisGetTuplePosition(LocatorExternalCatalog *exCatalog,
												 Relation relation, 
												 LocatorRouteSynopsis routeSynopsis,
												 LocatorTuplePosition tuplePosition);
extern void locator_insert(Relation relation, HeapTuple tup, CommandId cid,
						   int options, BulkInsertState bistate);
extern TableScanDesc locator_beginscan(Relation relation, Snapshot snapshot,
									   int nkeys, ScanKey key,
									   ParallelTableScanDesc parallel_scan,
									   uint32 flags,
									   List *hap_partid_list);
extern void locator_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
						   bool allow_strat, bool allow_sync, bool allow_pagemode);
extern void locator_endscan(TableScanDesc sscan);
extern void locatorgetpage(TableScanDesc sscan, bool is_locator,
						   LocatorExecutorLevelDesc *level_desc,
						   LocatorExecutorColumnGroupDesc *group_desc);
extern HeapTuple locator_getnext(TableScanDesc sscan, ScanDirection direction);
extern bool locator_getnextslot(TableScanDesc sscan, ScanDirection direction,
								TupleTableSlot *slot);
#endif /* LOCATOR */
#endif							/* HEAPAM_H */
