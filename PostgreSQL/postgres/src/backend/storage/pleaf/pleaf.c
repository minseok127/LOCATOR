/*-------------------------------------------------------------------------
 *
 * pleaf.c
 * 		PLeaf API implementation. 
 *
 * 
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include "utils/snapmgr.h"
#include "storage/lwlock.h"

#include "storage/ebi_tree.h"
#include "storage/ebi_tree_buf.h"
#include "storage/pleaf_stack_helper.h"
#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_buf.h"
#include "storage/pleaf_internals.h"
#include "storage/pleaf.h"

#ifdef DIVA_PRINT
#include "optimizer/cost.h"
#endif

#ifdef LOCATOR
PLeafHoldingResources holding_resources;
#endif /* LOCATOR */

/*
 * PLeafLookupTuple
 *
 * Lookup a visible version locator in p-leaf, 
 * and then get version data from EBI-tree.
 */
int
PLeafLookupTuple(Oid rel_id,
				 bool fast_memory_lookup,
				 EbiTreeVersionOffset* ret_vers_offset,
				 PLeafOffset offset,
				 Snapshot snapshot,
				 uint32* tuple_size,
				 void** ret_value)
{

	PLeafPageId page_id;
	PLeafGenNumber gen_no;
	PLeafPage page;
	int frame_id;
	PLeafOffset	internal_offset;
	PLeafVersionOffset version_offset;
	bool version_found;
	int ebi_page_frame_id;

	/*
	 * Offset value in record
	 * ---- 2 byte -------------- 6 byte -----------
	 * | Generation Number | Page Id + Page Offset |
	 * ---------------------------------------------
	 *  The only one including generation number is the offset in the record
	 *  (not in version locator or internal p-leaf offset).
	 */
	internal_offset = offset;

	gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(internal_offset);
	internal_offset = PLEAF_OFFSET_TO_INTERNAL_OFFSET(offset);

	/* Assertion */
	PLeafIsGenerationNumberValidInLookup(gen_no);
	
	/*
	 * Search a version locator(EBI-tree locator) in p-leaf version array.
	 * The version locator should be in the direct version array.
	 * If the current version array is indirect, keep going to find direct one.
	 */
	while (true) {
		page_id = PLEAF_OFFSET_TO_PAGE_ID(internal_offset);
		page = PLeafGetPage(page_id, gen_no, false, &frame_id);

		/* 
		 * If the value of version_found is true,
		 * it means we find the version locator successfully or fail to find 
		 * the version locator(or offset) with transaction's snapshot.
		 *
		 * Else, it means that we find the version offset in the indirect array.
		 */
		version_found =
			PLeafLookupVersion(page, &internal_offset, snapshot);

		PLeafReleasePage(frame_id);

		if (version_found) {
			version_offset = internal_offset;
			break;
		}
	}

	/* Fail to find the visible version locator */
	if (version_offset == PLEAF_INVALID_VERSION_OFFSET) {
		return -1;
	}

	/* If this is fast_memory_lookup, set return offset value */
	if (fast_memory_lookup)
	{
		*ret_vers_offset = version_offset;
	}
	
	/* Read value from EBI-Tree */
	ebi_page_frame_id = EbiLookupVersion(
                        version_offset,
                        rel_id,
						fast_memory_lookup,
                        tuple_size, 
                        ret_value);

	// Return ebi-page-frame-id
	return ebi_page_frame_id;
}

/*
 * PLeafAppendTuple
 *
 * Append new version to p-leaf version array and new version data to EBI-tree.
 * If the version is invisible, return immediately
 */
#ifdef LOCATOR
bool
#else
void
#endif /* LOCATOR */
PLeafAppendTuple(Oid rel_id,
				 PLeafOffset offset,
				 PLeafOffset* ret_offset,
				 TransactionId xmin,
				 TransactionId xmax,
				 uint32 tuple_size,
				 const void* tuple,
				 LWLock* rwlock)
{

	PLeafVersionOffset version_offset;

	/*
	 * Get the version offset from EBI-tree.
     * In DIVA PLUS version, we save versions 
     * in subnode and get version offset in subnode.
	 * It can be already obsolete version
	 * version_offset = EBI-APPEND-VERSION
	 */
	version_offset = EbiSiftAndBindWithSub(
                    					   rel_id,
                    					   xmin, 
                    					   xmax, 
                    					   tuple_size, 
                    					   tuple, 
                    					   rwlock);

	if (version_offset == PLEAF_INVALID_VERSION_OFFSET) {
#ifdef LOCATOR
		return false;
#else
		return;
#endif /* LOCATOR */
	}

	PLeafAppendVersion(offset, ret_offset, xmin, xmax, version_offset, rwlock);

#ifdef LOCATOR
	return true;
#endif /* LOCATOR */
}

/*
 * PLeafIsLeftLookup
 * called by read transactions only 
 */
bool
PLeafIsLeftLookup(uint64 left_offset,
				  uint64 right_offset,
				  TransactionId xid_bound,
				  Snapshot snapshot)
{
	
	PLeafGenNumber left_gen_no, right_gen_no;
	PLeafGenNumber old_gen_no;
	left_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(left_offset);
	right_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(right_offset);

	Assert(!(left_gen_no == 0 && right_gen_no == 0));
	Assert(left_gen_no != right_gen_no);

	/*
	 * Update : xid-bound -> offset value
	 */
	if (xid_bound == 0)
		return true;

	/*
	 * If either left's or right's generation number is 0,
	 * read operation should be done in non-zero generation number
	 */
	if (right_gen_no == 0)
		return true;
	else if (left_gen_no == 0)
		return false;
	else
	{
		/* Get old generation number */
		old_gen_no = PLeafGetOldGenerationNumber();
		
		if (XidInMVCCSnapshot(xid_bound, snapshot))
			/*
			 * If xid bound is visible to transaction's snapshot, it means that the
			 * visible version is located in old generation.
			 */
			return left_gen_no == old_gen_no; 
		else
			return left_gen_no > right_gen_no;
	}	
}

/*
 * PLeafIsLeftUpdate
 * called by update transaction only
 */
bool
PLeafIsLeftUpdate(uint64 left_offset,
						uint64 right_offset,
						int* ret_status)
{
	PLeafGenNumber left_gen_no, right_gen_no, global_gen_no;

	/*
	 * Get the latest generation number without PLeafBufferIOLWLockArray.
	 * In here, we can get the latest generation number created at the moment,
	 * or the old generation number. However, generation number in record's offset
	 * cannot same be as the latest generation number mentioned above.
	 */
	global_gen_no = PLeafGetLatestGenerationNumber();
	Assert(global_gen_no != 0);

	/* Get both generation numbers */
	left_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(left_offset);
	right_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(right_offset);

	/* Set return status PLEAF_NORMAL 
	 * PLEAF_NORMAL means it doesn't change xid_bound value in meta-tuple.
	 */
	*ret_status = PLEAF_NORMAL;

	/*
	 * If the latest generation number is same as either left or right
	 * generation number, return its direction(i.e. left or right)
	 */
	if (global_gen_no == left_gen_no)
		/* Left side */
		return true;
	else if (global_gen_no == right_gen_no)
		/* Right side */
		return false;
	else if (left_gen_no == 0 && right_gen_no == 0)
		/* Also, it doesn't change xid_bound value in the first update */
		return true;

	/*
	 * PLEAF_SWITCH means it should change xid_bound value in meta-tuple.
	 */
	*ret_status = PLEAF_SWITCH;

	if (PLeafIsGenerationNumberValidInUpdate(left_gen_no, right_gen_no))
	{
		/*
		 * Smaller generation number will be target in this case.
		 * We should guarantee that old one is already cleaned.
		 * See PLeafIsGenerationNumberValidInUpdate()
		 */
		return left_gen_no < right_gen_no;
	}
	else
	{
		/*
		 * If both generation number in the meta-tuple is not same as the 
		 * generation number in PLeafMetadata, it means both of them were cleaned.
		 * Therefore, reset xid_bound value.
		 */
		*ret_status = PLEAF_RESET;
		return true;
	}
}

#ifdef LOCATOR
void
PLeafReleaseHoldingResources(uint32 pd_gen, uint32 *p_pd_gen)
{
	PLeafPage page;
	int frame_id;
	PLeafFreePool free_pool;
	int array_index;
	int slot_cnt = holding_resources.slot_count;

	if (slot_cnt > 0)
	{
		/*
		 * GC the old array "only if" this modification is not the first since
		 * the heap page was read into the buffer pool.
		 * 
		 * If a p-leaf array pointer has been flushed to disk, the p-leaf array
		 * pointed to by that pointer can be referenced through the read buffer,
		 * so it should be safely GC'd later by macro compaction.
		 * 
		 * If this modification of p-leaf array pointer is the first time after
		 * the heap page is read into a buffer pool, it means that the old
		 * pointer to the old array has been flushed.
		 * 
		 * The pd_gen of a heap page is incremented by one each time that heap
		 * page is read into the buffer pool. Therefore, if p-locator copies the
		 * heap page's pd_gen when modifying a p-leaf array pointer, it can be
		 * determined at which point the pointer was modified by comparing its
		 * pd_gen to the heap page's pd_gen.
		 */
		if (likely(pd_gen == *p_pd_gen))
		{
			/*
			 * If pd_gen of p-locator is same with pd_gen of heap page, it means
			 * that the p-leaf array pointer of the p-locator was modifed
			 * already.
			 * So at this time, the old array can be GC'd.
			 */
			for (int i = 0; i < slot_cnt; i++)
			{
				page = holding_resources.slots[i].page;
				frame_id = holding_resources.slots[i].frame_id;
				free_pool = holding_resources.slots[i].free_pool;
				array_index = holding_resources.slots[i].array_index;

				PLeafReleaseFreeSlot(page, frame_id, free_pool, 0, array_index);
			}
		}
		else
		{
			/*
			 * Or not, it means that this is the first modification of p-leaf
			 * array pointer, and this in turn means that the old pointer is
			 * stored on disk. 
			 * 
			 * So for readers using read buffer, don't GC the old array, just
			 * copy the pd_gen of heap page to indicate that a modification of
			 * the p-leaf array pointer has occurred.
			 */
			*p_pd_gen = pd_gen;
		}
	}
	
	for (int i = 0; i < holding_resources.page_count; i++)
	{
		PLeafReleasePage(holding_resources.pages[i]);
	}
}
#endif /* LOCATOR */

#endif
