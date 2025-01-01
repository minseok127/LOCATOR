/*-------------------------------------------------------------------------
 *
 * ebiam.c
 *	  ebi access method code
 *
 * Portions Copyright (c) 2023-2023, Database Operating System Lab
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/ebi/ebiam.c
 *
 *
 * INTERFACE ROUTINES
 *		ebi_getnext		- retrieve next tuple in scan
 *
 * NOTES
 *	  This file contains the ebi_ routines which implement
 *	  the POSTGRES ebi access method used for all ebi tree nodes.
 *
 *-------------------------------------------------------------------------
 */

#ifdef DIVA

#include "postgres.h"
#include "access/ebiam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "storage/block.h"
#include "utils/relcache.h"
#include "storage/pleaf.h"
#include "storage/ebi_sub_buf.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/ebi_tree_process.h"
#include "utils/dynahash.h"
#include "port.h"
#include "storage/ebi_tree_buf.h"
#include "utils/memutils.h"

#ifdef LOCATOR
#include <liburing.h>
#include "storage/readbuf.h"
#endif /* LOCATOR */

#define SEG_ID_MASK (0xFFFFFFFF00000000ULL)
#define SEG_OFFSET_MASK (0x00000000FFFFFFFFULL)
#define VERSION_OFFSET_TO_SEG_ID(version_offset) \
	((version_offset & SEG_ID_MASK) >> 32)
#define VERSION_OFFSET_TO_SEG_OFFSET(version_offset) \
	(version_offset & SEG_OFFSET_MASK)
#define SEG_TO_VERSION_OFFSET(seg_id, seg_offset) \
	((((uint64)(seg_id)) << 32) | seg_offset)
#define SEG_OFFSET_TO_PAGE_ID(off) \
	(uint32)((off) / (EBI_TREE_SEG_PAGESZ))
#define SEG_OFFSET_TO_PAGE_OFFSET(off) \
	(uint32)((off) % (EBI_TREE_SEG_PAGESZ))

/* Private functions */
static void EnlargeUnobtainedVersionOffsetArray(HeapScanDesc scan);
static void EbiSortUnobtainedVersions(HeapScanDesc scan);
static void EnlargeUnobtainedSegmentArray(HeapScanDesc scan);

/*
 * InsertUnobtainedVersionOffset -- insert visible version offset
 */
void
InsertUnobtainedVersionOffset(HeapScanDesc scan, 
							  EbiSubVersionOffset versionOffset)
{
	/* 
	 * When the number of version offsets exceeds the capacity, we expand the 
	 * array. 
	 */
	if (scan->unobtained_version_nums >= scan->unobtained_versions_capacity)
		EnlargeUnobtainedVersionOffsetArray(scan);

	Assert(scan->unobtained_version_nums < scan->unobtained_versions_capacity);

	/* Insert new version offset into the array. */
	scan->unobtained_version_offsets[scan->unobtained_version_nums] =
															versionOffset;
	scan->unobtained_version_nums += 1;
}

/*
 * EnlargeUnobtainedVersionOffsetArray
 */
static void
EnlargeUnobtainedVersionOffsetArray(HeapScanDesc scan)
{
	EbiSubVersionOffset *old_array = scan->unobtained_version_offsets;
	EbiSubVersionOffset *new_array;
	uint64_t new_capacity;
	MemoryContext oldcxt;

	/* Move to current transaction context. */
	oldcxt = MemoryContextSwitchTo(CurTransactionContext);

	Assert(scan->unobtained_version_nums == scan->unobtained_versions_capacity);

	if (scan->unobtained_versions_capacity == 0)
	{
		Assert(old_array == NULL);

		/* 1 MiB */
		new_capacity = (1024 * 1024ULL) / sizeof(EbiSubVersionOffset);
	}
	else
	{
		Assert(old_array != NULL);

		/* Doubling */
		new_capacity = scan->unobtained_versions_capacity * 2;
	}

	/* Make new array */
	new_array = (EbiSubVersionOffset *)
					palloc0(sizeof(EbiSubVersionOffset) * new_capacity);

	if (old_array)
	{
		memcpy(new_array, old_array, 
			sizeof(EbiSubVersionOffset) * scan->unobtained_versions_capacity);

		/* Resource cleanup */
		pfree(old_array);
	}

	/* Init values */
	scan->unobtained_version_offsets = new_array;
	scan->unobtained_versions_capacity = new_capacity;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * ebigetpage - subroutine for heapgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
void
ebigetpage(HeapScanDesc scan)
{
	int 	page_id = scan->c_page_id;
	Oid 	rel_id = RelationGetRelid(scan->rs_base.rs_rd);
	EbiSubSegmentId seg_id = scan->unobtained_segments[scan->seg_index]->seg_id;
#ifdef LOCATOR
	char	*buffer = NULL;
#endif
	int 	ntup = 0;
	int 	page_offset;
	uint32	tuple_size;
	Size	aligned_tuple_size;
	char 	*ebi_sub_page;
	
	/* Unref previous scan ebi sub buffer, if any */
	if (scan->c_buf_id != InvalidEbiSubBuf)
	{
		UnpinEbiSubBuffer(scan->c_buf_id);
		scan->c_buf_id = InvalidEbiSubBuf;
	}

	/* 
	 * Read Ebi Buffers
	 */
#ifdef LOCATOR
	if (scan->rs_base.rs_prefetch)
		scan->c_buf_id = ReadEbiBufferPrefetch(scan->rs_base.rs_rd,
											   scan->rs_base.rs_prefetch, &buffer);
	else
		scan->c_buf_id = EbiSubBufGetBufRef(seg_id, rel_id,
	 										page_id * EBI_SUB_SEG_PAGESZ, false);

	if (scan->c_buf_id != InvalidEbiSubBuf)
	{	
		/* The page is in buffer pool, reader can use it */
		ebi_sub_page = EbiSubGetPage(scan->c_buf_id);
		scan->c_readbuf_dp = NULL;
	}
	else
	{
		/* The page is not in buffer pool, reader does prefetching */
		Assert(buffer);

		ebi_sub_page = buffer;
		scan->c_readbuf_dp = buffer;
	}
#else /* !LOCATOR */
	scan->c_buf_id = EbiSubBufGetBufRef(seg_id, rel_id,
 										page_id * EBI_SUB_SEG_PAGESZ, false);
	ebi_sub_page = EbiSubGetPage(scan->c_buf_id);
#endif /* LOCATOR */

	page_offset = 0; // ! available only if versions are fixed_size

	while (page_offset < EBI_SUB_SEG_PAGESZ)
	{
		/* 
		 * TODO: Need to optimize logic. Our goal is to investigate data integrity.
		 */
		EbiTreeVersionOffset target_version_offset = 
			scan->unobtained_version_offsets[scan->c_offset];
		uint32 target_seg_id = VERSION_OFFSET_TO_SEG_ID(target_version_offset);
		uint32 target_seg_offset = VERSION_OFFSET_TO_SEG_OFFSET(target_version_offset);
		uint32 target_page_id = SEG_OFFSET_TO_PAGE_ID(target_seg_offset);
		uint32 target_page_offset = SEG_OFFSET_TO_PAGE_OFFSET(target_seg_offset);

		/* If this page id is different target page id, reader can skip this page */
		if (target_page_id != page_id)
			break;

		/* Read tuple size and check */
		tuple_size = *((uint32 *) &ebi_sub_page[page_offset]);
		if (tuple_size == 0) 
			break;

		/* Align tuple size */
		aligned_tuple_size =
			1 << my_log2(tuple_size + EBI_SUB_VERSION_METADATA);
		
		/* Check to see if it's a tuple to read */
		if (target_seg_id != seg_id || 
			target_page_offset != page_offset)
		{
			page_offset += aligned_tuple_size;
			/* This tuple is not target */
			continue;
		}
		else
		{
			scan->c_offset += 1;
			scan->unobtained_segments[scan->seg_index]->match_tuple_cnt += 1;
		}
		
		/* In here, target version is always visible. */

		/* Set tuple infos */
		scan->rs_vistuples[ntup] = page_offset;
		ntup++;

		/* TODO: remove it later */
		scan->unobtained_segments[scan->seg_index]->match_visible_cnt += 1;
		page_offset += aligned_tuple_size;
	}

	scan->rs_ntuples = ntup;
}

static HeapTuple
ebigettup(HeapScanDesc scan, ScanDirection dir)
{
#ifdef LOCATOR
	PrefetchDesc prefetch_desc = scan->rs_base.rs_prefetch;
#endif /* LOCATOR */
	HeapTuple tuple = &(scan->rs_ctup);
	int			lines;
	int			lineindex;
	int			linesleft;

	if (unlikely(scan->is_first_ebi_scan))
	{
		/* Init variables */
		scan->rs_cbuf = InvalidBuffer;
		scan->c_buf_id = InvalidEbiSubBuf;
		scan->c_page_id = 0;
		scan->c_max_page_id = 0;
		scan->seg_index = 0;
		scan->max_seg_index = 0;
		scan->c_offset = 0;

		/* 
		 * We sort the unobtained versions. We retrieve the information for a 
		 * segment to be used later, iterating all version offsets.
		 */
		EbiSortUnobtainedVersions(scan);

		/* If there are no ebi pages to scan, return */
		if (scan->max_seg_index == 0)
			return NULL;

#ifdef LOCATOR
		if (prefetch_desc)
		{
			/* 
			 * we do not initialization for ebi scan inside heap_beginscan yet.
			 * TODO: move initialization into heap_beginscan
			 */
			prefetch_desc->ebi_next_reqblock = 0;
			prefetch_desc->ebi_max_page_id = 0;
			prefetch_desc->ebi_seg_index = 0;
			prefetch_desc->ebi_seg_num = scan->max_seg_index;
			prefetch_desc->ebi_prefetch_end = false;

			/* copy the pointer of ebi scan state */
			prefetch_desc->ebi_unobtained_segments_p = 
							(void **) scan->unobtained_segments;
		}
#endif /* LOCATOR */
	
		/* Init variables with using fetched info */
		scan->c_max_page_id = 
					scan->unobtained_segments[0]->max_page_id + 1;

		/* Get first page and set lineindex */
		ebigetpage(scan);
		lineindex = 0;

		scan->is_first_ebi_scan = false;
	}
	else
	{
		/* Continue from previously returned page/tuple */
		lineindex = scan->rs_cindex + 1;
	}

	lines = scan->rs_ntuples;
	linesleft = lines - lineindex;
	
	for(;;)
	{
		if (linesleft != 0)
		{
			Offset page_offset;
			char *ebi_sub_page;
			int tuple_size;

			/* Return current tuple */
			scan->rs_cindex = lineindex;
			page_offset = scan->rs_vistuples[lineindex];

#ifdef LOCATOR
			if (scan->c_buf_id == InvalidEbiSubBuf)
				ebi_sub_page = scan->c_readbuf_dp;
			else
#endif /* LOCATOR */
				ebi_sub_page = EbiSubGetPage(scan->c_buf_id); 
			Assert(ebi_sub_page);

			/* Read tuple size and check */
			tuple_size = *((uint32 *) &ebi_sub_page[page_offset]);
			Assert(tuple_size > 0);

			tuple->t_data = (HeapTupleHeader) 
						&ebi_sub_page[page_offset + EBI_SUB_VERSION_METADATA];
			tuple->t_len = tuple_size;
			ItemPointerCopy(&(tuple->t_data->t_ctid), &(tuple->t_self));

			return &scan->rs_ctup;
		}

		/*
		 * If we exhausted this page, set current page id to next.
		 * If we exhausted all pages in this segment, set current segment index
		 * to next.
		 * If we exhausted all segments in this tree, return.
		 */
		if (++(scan->c_page_id) == scan->c_max_page_id)
		{
			if (++(scan->seg_index) == scan->max_seg_index)
			{
				/* Before finish scan, check ebi sub buffer */
				if (scan->c_buf_id != InvalidEbiSubBuf)
				{
					UnpinEbiSubBuffer(scan->c_buf_id);
					scan->c_buf_id = InvalidEbiSubBuf;
				}

				return NULL;
			}

			scan->c_page_id = 0;
			scan->c_max_page_id = 
						scan->unobtained_segments[scan->seg_index]->max_page_id + 1;
		}
		
		/* Get next page after setting current page id*/
		ebigetpage(scan);

		/* and reset variables */
		lineindex = 0;
		lines = scan->rs_ntuples;
		linesleft = lines - lineindex;
	}
}

/*
 * EbiSortUnobtainedVersions -- sort and get unobtained version offsets
 */
static void
EbiSortUnobtainedVersions(HeapScanDesc scan)
{
	EbiSubSegmentId seg_id;
	uint64 offset_idx;
	off_t seg_offset, next_seg_offset;
	EbiUnobtainedSegment curSeg = NULL;
	int cur_seg_idx = 0;
	
	/* If there is no unobtained version, just return */
	if (scan->unobtained_version_nums == 0)
		return;

	/* Sort unobtained version */
	if (scan->unobtained_version_nums > 1)
		qsort(scan->unobtained_version_offsets, scan->unobtained_version_nums,
			  sizeof(EbiTreeVersionOffset), EbiTreeVersionCompare);

	/* Enlarge unobtained segment array. */
	EnlargeUnobtainedSegmentArray(scan);
	
	/* Maps the versions to EbiUnobtVersSegment with for-loop */
	for (offset_idx = 0; offset_idx < scan->unobtained_version_nums; offset_idx++)
	{
		if (unlikely(offset_idx == 0))
		{
			/* Get info of first version */
			seg_id = 
				VERSION_OFFSET_TO_SEG_ID(scan->unobtained_version_offsets[offset_idx]);
			seg_offset = 
				VERSION_OFFSET_TO_SEG_OFFSET(scan->unobtained_version_offsets[offset_idx]);

			/* Init with first version */
			curSeg = scan->unobtained_segments[cur_seg_idx];
			curSeg->seg_id = seg_id;
			curSeg->head = &scan->unobtained_version_offsets[0];
			curSeg->min_page_id = SEG_OFFSET_TO_PAGE_ID(seg_offset);
			curSeg->match_tuple_cnt = 0;
			curSeg->match_visible_cnt = 0;
			scan->max_seg_index = 1;
			
			cur_seg_idx += 1;
		}
		else
		{
			seg_id = 
				VERSION_OFFSET_TO_SEG_ID(scan->unobtained_version_offsets[offset_idx]);

			if (curSeg->seg_id != seg_id)
			{
				/* Enlarge array. */
				EnlargeUnobtainedSegmentArray(scan);

				/* Init with last version offset */
				seg_offset = 
					VERSION_OFFSET_TO_SEG_OFFSET(scan->unobtained_version_offsets[offset_idx - 1]);

				curSeg->tail = &scan->unobtained_version_offsets[offset_idx - 1];
				curSeg->max_page_id = SEG_OFFSET_TO_PAGE_ID(seg_offset);

				/* Move to next segment */
				curSeg = scan->unobtained_segments[cur_seg_idx];
				scan->max_seg_index += 1;

				/* Init with first version offset */
				next_seg_offset = 
					VERSION_OFFSET_TO_SEG_OFFSET(scan->unobtained_version_offsets[offset_idx]);

				curSeg->seg_id = seg_id;
				curSeg->head = &scan->unobtained_version_offsets[offset_idx];
				curSeg->min_page_id = SEG_OFFSET_TO_PAGE_ID(next_seg_offset);
				curSeg->match_tuple_cnt = 0;
				curSeg->match_visible_cnt = 0;
		
				cur_seg_idx += 1;
			}
		}
	}

	seg_offset = 
		VERSION_OFFSET_TO_SEG_OFFSET(scan->unobtained_version_offsets[offset_idx - 1]);

	Assert(curSeg != NULL);

	/* Init with last version offset */
	curSeg->max_page_id = 
		SEG_OFFSET_TO_PAGE_ID(seg_offset);
	curSeg->tail = &scan->unobtained_version_offsets[offset_idx - 1];
}

/*
 * EnlargeUnobtainedSegmentArray
 */
static void
EnlargeUnobtainedSegmentArray(HeapScanDesc scan)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);
	EbiUnobtainedSegment *old_array = scan->unobtained_segments;
	EbiUnobtainedSegment *new_array;
	uint64_t new_capacity;
	int 	i;
	
	/* If the capacity allows, we do not require a new array. */
	if (scan->unobtained_segment_nums < scan->unobtained_segments_capacity)
	{
		scan->unobtained_segment_nums += 1;
		return;
	}

	if (scan->unobtained_segments_capacity == 0)
	{
		Assert(old_array == NULL);

		/* Init value. */
		new_capacity = 32;
	}
	else
	{
		Assert(old_array != NULL);

		/* Doubling. */
		new_capacity = scan->unobtained_segments_capacity * 2;
	}

	/* Make new array */
	new_array = (EbiUnobtainedSegment *)
					palloc0(sizeof(EbiUnobtainedSegment) * new_capacity);

	/* Copy data of old array. */
	if (old_array)
	{
		for (i = 0; i < scan->unobtained_segment_nums; ++i)
			new_array[i] = old_array[i];	
	
		pfree(old_array);
	}

	/* Allocate new element within empty. */
	for (i = scan->unobtained_segment_nums; i < new_capacity; ++i)
	{
		new_array[i] = (EbiUnobtainedSegment) 
							palloc0(sizeof(EbiUnobtainedSegmentData));
	}

	/* Init values */
	scan->unobtained_segments = new_array;
	scan->unobtained_segments_capacity = new_capacity;

	/* Increment # of segments */
	scan->unobtained_segment_nums += 1;

	MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 *					 ebi access method interface
 * ----------------------------------------------------------------
 */

bool
ebi_getnextslot(TableScanDesc sscan, ScanDirection direction,
							 TupleTableSlot *slot)
{
	HeapScanDesc scan = (HeapScanDesc)sscan;
	HeapTuple tuple;

	tuple = ebigettup(scan, direction);
	if (tuple == NULL)
	{
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * If we get here it means we have a new current scan tuple, so return
	 * the tuple.
	 */
	pgstat_count_heap_getnext(scan->rs_base.rs_rd);
	ExecStoreBufferHeapTuple(tuple, slot, scan->rs_cbuf);

	return true;
}
#endif /* DIVA */
