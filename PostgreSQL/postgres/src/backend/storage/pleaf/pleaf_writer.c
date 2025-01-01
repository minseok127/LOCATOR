/*-------------------------------------------------------------------------
 *
 * pleaf_writer.c
 * 		Implement functions called by writer  
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
 *    src/backend/storage/pleaf/pleaf_writer.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/snapmgr.h"
#include "access/transam.h"

#include "storage/pleaf_stack_helper.h"
#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_buf.h"

#include "storage/pleaf_internals.h"

#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>

#ifdef LOCATOR
extern PLeafHoldingResources holding_resources;

static void
PLeafInitHoldingResources(void)
{
	holding_resources.page_count = 0;
	holding_resources.slot_count = 0;
}

static void
PLeafHoldPage(int frame_id)
{
	Assert(holding_resources.page_count < MAX_HOLDING_PAGES);
	holding_resources.pages[holding_resources.page_count++] = frame_id;
}

static void
PLeafHoldFreeSlot(PLeafPage page,
			  int frame_id,
			  PLeafFreePool free_pool,
			  int array_index)
{
	int index;

	Assert(holding_resources.slot_count < MAX_HOLDING_PAGES);

	index = holding_resources.slot_count;
	holding_resources.slots[index].page = page;
	holding_resources.slots[index].frame_id = frame_id;
	holding_resources.slots[index].free_pool = free_pool;
	holding_resources.slots[index].array_index = array_index;

	holding_resources.slot_count++;
}
#endif /* LOCATOR */

#ifndef LOCATOR 
/*
 * PLeafCheckContraction
 *
 * Return true if it can be contracted.
 */
static bool
PLeafCheckContraction(int cap_index, 
							int cap, 
							int head, 
							int tail) 
{
	int version_count;

	/* Always false when capacity is the smallest */
	if (cap_index == 0) {
		return false;
	}

	version_count = GetVersionCount(head, tail, cap);
	if (cap_index == 1)
	{
		return (version_count <= (PLeafGetCapacity(cap_index - 1) / 2));
	} 
	else 
	{
		return version_count <= ((PLeafGetCapacity(cap_index - 2) + 
					PLeafGetCapacity(cap_index - 1)) / 2);
	}
}

/*
 * PLeafGetNewCapacityIndex
 *
 * Get new capacity index in contraction
 */
static int
PLeafGetNewCapacityIndex(int cap_index, 
							int cap, 
							int version_count, 
							int head, 
							int tail) 
{
	int new_cap_index;

	Assert(cap_index != 0);
	new_cap_index = PLeafGetProperCapacityIndex(version_count);

	Assert(new_cap_index < cap_index);
	return new_cap_index;
}
#endif

/* 
 * PLeafCopyVersion
 *
 * Copy single-version
 */
static inline void
PLeafCopyVersion(PLeafVersion dest, 
							PLeafVersion src) {
	memcpy(dest, src, sizeof(PLeafVersionData));
}

/*
 * PLeafCopyVersions
 *
 * Copy multi-versions
 */
static void
PLeafCopyVersions(PLeafVersion dest, 
							PLeafVersion src, 
							uint16_t version_head, 
							uint16_t version_tail, 
							uint16_t version_count, 
							int cap) 
{
	uint16_t temp_count;

	if (version_head < version_tail) 
	{
		memcpy(dest, PLeafPageGetVersion(src, version_head),
				sizeof(PLeafVersionData) * version_count);

	} 
	else 
	{
		temp_count = cap - version_head;
		memcpy(dest, PLeafPageGetVersion(src, version_head),
				sizeof(PLeafVersionData) * temp_count);

		memcpy(PLeafPageGetVersion(dest, temp_count), src,
				sizeof(PLeafVersionData) * (version_count - temp_count));
	}
}

/*
 * PLeafAppendTempInfo
 *
 * Save temporary information for recursive update case.
 */
static inline void
PLeafAppendTempInfo(PLeafTempInfo temp_info, 
							int index, 
							PLeafPage page, 
							int frame_id,
							int array_index) 
{
	temp_info[index].page = page;
	temp_info[index].frame_id = frame_id;
	temp_info[index].array_index = array_index;
}

/*
 * PLeafGetTempInfo
 *
 * Get saved information
 */
static inline void
PLeafGetTempInfo(PLeafTempInfo temp_info, 
						int index,
						PLeafPage* page, 
						int* frame_id,
						int* array_index) 
{
	*page = temp_info[index].page;
	*frame_id = temp_info[index].frame_id;
	*array_index = temp_info[index].array_index;
}

/*
 * PLeafSearchDirectArray
 *
 * Search the direct version array to append new version, and keep all 
 * information of accessed version-array.
 *
 * Update operations may need information of entirely accessed version-array
 * for further actions(compaction, expansion, ...)
 */
static int
PLeafSearchDirectArray(PLeafTempInfo temp_info, 
							PLeafPage page, 
							int frame_id,
							PLeafGenNumber gen_no,
							PLeafOffset offset) 
{
	int array_index;
	int capacity;
	int page_count;
	PLeafVersion first_version, version;
	PLeafVersionIndex version_index;
	PLeafPageId page_id;
	page_count = 0;

for_loop:
	if (page_count >= MAX_ARRAY_ACCESSED) 
	{
		Assert(false);
	}

	capacity = PLeafPageGetCapacity(page);
	ASSERT_OFFSET(offset, capacity);
	// if (((offset % PLEAF_PAGE_SIZE) - PLEAF_PAGE_HEADER_SIZE) % (capacity * PLEAF_VERSION_DATA_SIZE) != 0)
	// {
	// 	ereport(LOG, (errmsg("@@@ Invalid Offset1, %lu, capacity: %d", offset, capacity)));
	// 	while(true) {sleep(3000);}
	// } else if (offset == 0) {
	// 	ereport(LOG, (errmsg("@@@ Invalid Offset2, 0, capacity: %d", capacity)));
	// 	while(true) {sleep(3000);}
	// }

	array_index = PLeafPageGetArrayIndex(capacity, offset);
	ASSERT_ARR_IDX(array_index);

	version_index = PLeafPageGetVersionIndex(page, array_index);

	page_count++;

	PLeafAppendTempInfo(temp_info, page_count, page, frame_id, array_index);

#ifdef LOCATOR
	PLeafHoldPage(frame_id);
#endif /* LOCATOR */

	if (PLeafGetVersionType(*version_index) == PLEAF_DIRECT_ARRAY) 
	{
		goto func_exit;
	} 

	/*
	 * In indirect version-array, we should go through version offset of tail.
	 * Appending new version always happen in the last direct version-array.
	 */
	first_version =
		PLeafPageGetFirstVersion(page, array_index, capacity);

	version = PLeafPageGetVersion(first_version,
			(PLeafGetVersionIndexTail(*version_index) + capacity - 1) % capacity);

	offset = PLeafGetVersionOffset(version);

	page_id = PLEAF_OFFSET_TO_PAGE_ID(offset);
	page = PLeafGetPage(page_id, gen_no, false, &frame_id);

	goto for_loop;

func_exit:
	/* All pages accessed should be pinned */
	return page_count;
}

/*
 * PLeafAppendVersionFast
 *
 * Real action, append new version, happens in this function
 */
static void
PLeafAppendVersionFast(PLeafVersion first_version, 
							PLeafVersionIndex version_index,
							uint16_t version_head, 
							uint16_t version_tail, 
							int cap,
							TransactionId xmin, 
							TransactionId xmax, 
							PLeafVersionOffset version_offset) 
{
	uint16_t new_version_tail;
	PLeafVersion new_version;

	Assert(PLeafCheckAppendness(cap, version_head, version_tail));
	new_version = PLeafPageGetVersion(first_version, version_tail % cap);

	PLeafPageSetVersion(new_version, xmin, xmax, version_offset);

	new_version_tail = (version_tail + 1) % (2 * cap);

	PLeafSetVersionIndexTail(version_index, new_version_tail);
}

/*
 * PLeafReleaseAllFreeSlots
 *
 * If garbage collection of indirect version index happens,
 * we should free all used slot(bitmap in pages).
 */
static void
PLeafReleaseAllFreeSlots(PLeafVersion version, 
							PLeafFreePool free_pool) 
{
	PLeafVersionOffset version_offset;
	PLeafPage page;
	PLeafPageId page_id;
	int frame_id;
	int array_index;
	PLeafVersionIndex version_index;
	PLeafVersion first_version;
	uint16_t version_head;

	version_offset = PLeafGetVersionOffset(version);
	page_id = PLEAF_OFFSET_TO_PAGE_ID(version_offset);
	page = PLeafGetPage(page_id, free_pool->gen_no, false, &frame_id);

	Assert(PLeafPageGetCapacity(page) == PLEAF_MAX_CAPACITY); 

	array_index = PLeafPageGetArrayIndex(PLEAF_MAX_CAPACITY, version_offset);
	ASSERT_ARR_IDX(array_index);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	version_head = PLeafGetVersionIndexHead(*version_index);

	first_version = 
				PLeafPageGetFirstVersion(page, array_index, PLEAF_MAX_CAPACITY);

	if (PLeafGetVersionType(*version_index) == PLEAF_DIRECT_ARRAY) 
	{
		PLeafReleaseFreeSlot(page, frame_id, free_pool, 
								PLEAF_MAX_CAPACITY, array_index);
		PLeafReleasePage(frame_id);
		return;
	}

	while (version_head != PLeafGetVersionIndexTail(*version_index)) 
	{
		PLeafReleaseAllFreeSlots(PLeafPageGetVersion(
					first_version, version_head % PLEAF_MAX_CAPACITY), free_pool);
		version_head = (version_head + 1) % (2 * PLEAF_MAX_CAPACITY);
	}
	PLeafReleaseFreeSlot(page, frame_id, free_pool,
							PLEAF_MAX_CAPACITY, array_index);
	PLeafReleasePage(frame_id);
	return;
}

/*
 * PLeafCompactVersions
 *
 * The second way of garbage collection, check visiblity of all versions
 */
#ifdef LOCATOR
static bool
PLeafCompactVersions(int* new_cap_index,
							PLeafFreePool free_pool,
							PLeafVersion first_version,
							PLeafVersionIndex version_index,
							uint16_t version_head, 
							uint16_t version_tail, 
							int capacity_index,
							PLeafOldArray old_array) 
{
	PLeafVersion version;
	int current_index;
	int version_count;
	int capacity;
	bool is_direct;
	uint16_t new_version_head;
	TransactionId xmin, xmax;

	capacity = PLeafGetCapacity(capacity_index);

	is_direct = (PLeafGetVersionType(*version_index) == PLEAF_DIRECT_ARRAY);

	new_version_head = version_head;

	/* Version array must be full */
	Assert(!PLeafCheckAppendness(capacity, version_head, version_tail));
	Assert(!PLeafCheckEmptiness(version_head, version_tail));

	/* Increment head until accessing the live version */
	while (new_version_head != version_tail) 
	{
		version = PLeafPageGetVersion(first_version, new_version_head % capacity); 
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);

		Assert(xmin <= xmax);

		if (PLeafCheckVisibility(xmin, xmax))
		{
			break;
		}
		/* Only in the indirect array */
		if (!is_direct) 
		{
			PLeafReleaseAllFreeSlots(version, free_pool);
		}
		new_version_head = (new_version_head + 1) % (2 * capacity);
	}

	/* Try to move old head to new head if possible */
	if (version_head != new_version_head) 
	{
		PLeafSetVersionIndexHead(version_index, new_version_head);
		return true;

#if 0
		/* Compress eagerly */
		version_count = version_tail >= new_version_head ?
						version_tail - new_version_head :
						version_tail + 2 * capacity - new_version_head;

		Assert(version_count < capacity);

		/* 1 is for new version */
		*new_cap_index = PLeafGetProperCapacityIndex(version_count + 1);

		/* Don't expand in here */
		if (*new_cap_index >= capacity_index)
			return true;
#endif
	}

	/*
	 * If we reach here, it means we need to allocate new array.
	 * So, for copying versions, we have to count visible versions.
	 */
	current_index = new_version_head;
	version_count = 0;

	/* Check visibility of all versions from new version head */
	while (current_index != version_tail) 
	{
		version = PLeafPageGetVersion(first_version, current_index % capacity);
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
		
		if (PLeafCheckVisibility(xmin, xmax)) 
		{
			old_array->visible_index[version_count] = current_index % capacity;
			
			/* The number of live versions */
			version_count++;
		}
		else
		{
			/* If this is indirect array, free all invisible arrays */
			if (!is_direct) 
			{
				PLeafReleaseAllFreeSlots(version, free_pool);
			}
		}

		current_index = (current_index + 1) % (2 * capacity);
	}

	old_array->version_count = version_count;

	/* If this array is full */
	if (version_count == capacity)
	{
		*new_cap_index = capacity_index + 1;
		return false;
	}

	/* 1 is for new version */
	*new_cap_index = PLeafGetProperCapacityIndex(version_count + 1);

	/* Don't expand in here */
	if (*new_cap_index > capacity_index)
		*new_cap_index = capacity_index;

	return false;
}
#else /* !LOCATOR */
static bool
PLeafCompactVersions(bool is_break,
							PLeafFreePool free_pool,
							PLeafVersion first_version,
							PLeafVersionIndex version_index,
							uint16_t version_head, 
							uint16_t version_tail, 
							int capacity,
							LWLock* rwlock) 
{
	PLeafVersion version;
	PLeafVersion real_version;
	int current_index, real_index;
	int version_count;
	bool ret_value; /* true if some versions are cleaned */
	bool lock_acquired;
	bool is_direct;
	uint16_t new_version_head;
	uint16_t new_version_tail;
	TransactionId xmin, xmax;

	ret_value = false;
	lock_acquired = false;
	is_direct = (PLeafGetVersionType(*version_index) == PLEAF_DIRECT_ARRAY);

	new_version_head = version_head;

	/* Version array must be full */
	Assert(!PLeafCheckAppendness(capacity, version_head, version_tail));
	Assert(!PLeafCheckEmptiness(version_head, version_tail));

	/* Increment head until accessing the live version */
	while (new_version_head != version_tail) 
	{
		version = PLeafPageGetVersion(first_version, new_version_head % capacity); 
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);

		Assert(xmin <= xmax);

		if (PLeafCheckVisibility(xmin, xmax))
		{
			break;
		}
		/* Only in the indirect array */
		if (!is_direct) 
		{
			PLeafReleaseAllFreeSlots(version, free_pool);
		}
		new_version_head = (new_version_head + 1) % (2 * capacity);
	}

	/* Try to move old head to new head if possible */
	if (version_head != new_version_head) 
	{
		PLeafSetVersionIndexHead(version_index, new_version_head);
		ret_value = true;
	}

	/* 
	 * If the version array is empty or the value of is_break is true,
	 * return immediately
	 */
	if (is_break || PLeafCheckEmptiness(new_version_head, version_tail)) 
	{
		return ret_value;
	}

	current_index = new_version_head;
	real_index = current_index;
	version_count = 0;

	/* Check visibility of all versions from new version head */
	while (current_index != version_tail) 
	{
		version = PLeafPageGetVersion(first_version, current_index % capacity);
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
		
		if (PLeafCheckVisibility(xmin, xmax)) 
		{
			/* The number of live versions */
			version_count++;

			if (current_index != real_index) 
			{
				if (!lock_acquired) 
				{
					//lock_acquired = true;
					/* Acquire latch in here */
					//LWLockAcquire(rwlock, LW_EXCLUSIVE);
				}

				if (!is_direct) 
				{
					real_version = PLeafPageGetVersion(first_version, real_index % capacity);

					if ((real_version->version_id != 0) && 
							(real_version->version_offset != 0))
						PLeafReleaseAllFreeSlots(real_version, free_pool);
				}
				Assert((real_index % capacity) != (current_index % capacity));

				PLeafCopyVersion(
						PLeafPageGetVersion(first_version, real_index % capacity),
						PLeafPageGetVersion(first_version, current_index % capacity));

				memset(PLeafPageGetVersion(first_version, current_index % capacity),
						0x00, sizeof(PLeafVersionData));
			}
			real_index = (real_index + 1) % (2 * capacity);
		}
		current_index = (current_index + 1) % (2 * capacity);
	}

	new_version_tail = (new_version_head + version_count) % (2 * capacity);

	if (new_version_tail != version_tail) 
	{
		ret_value = true;
	}

	/* Update version tail */
	PLeafSetVersionIndexTail(version_index, new_version_tail);

	/* Release latch in here */
	//if (lock_acquired) 
	//{
	//	LWLockRelease(rwlock);
	//}
	return ret_value;
}
#endif /* LOCATOR */

/*
 * PLeafChangeUpperXmax
 *
 * When new indirect version index is needed in expansion,
 * changing upper layer's xmax value is necessary
 */
static void
PLeafChangeUpperXmax(PLeafTempInfo temp_info, 
						int index, 
						TransactionId xmax) 
{
	PLeafPage page;
	int frame_id;
	int array_index;
	int capacity;
	uint16_t version_tail;
	PLeafVersion version, first_version;
	PLeafVersionIndex version_index;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_tail = PLeafGetVersionIndexTail(*version_index);

	version = PLeafPageGetVersion(first_version, 
			(version_tail + capacity - 1) % capacity);

	PLeafPageSetVersionXmax(version, xmax);

	PLeafMarkDirtyPage(frame_id);
}

/*
 * PLeafChangeUpperOffset
 *
 * When resizing the version array, 
 * changing upper layer's offset value is necessary
 */
static void
PLeafChangeUpperOffset(PLeafTempInfo temp_info, 
						int index, 
						PLeafOffset offset) 
{
	PLeafPage page;
	int frame_id;
	int array_index;
	int capacity;
	uint16_t version_tail;
	PLeafVersion version, first_version;
	PLeafVersionIndex version_index;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_tail = PLeafGetVersionIndexTail(*version_index);

	version = PLeafPageGetVersion(first_version, 
			(version_tail + capacity - 1) % capacity);
	
	PLeafPageSetVersionOffset(version, offset);

	PLeafMarkDirtyPage(frame_id);
}

/*
 * PLeafContractVersions
 *
 * Main routine of contraction(version-array migration)
 */
#ifdef LOCATOR
static void
PLeafCompressVersions(int new_capacity_index,
							PLeafTempInfo temp_info, 
							int index, 
							PLeafFreePool free_pool,
							PLeafOffset* ret_offset, 
							TransactionId xmin,
							TransactionId xmax,
							PLeafVersionOffset version_offset,
							PLeafOldArray old_array)
{
	PLeafPage page, new_page;
	int frame_id, new_frame_id;
	int array_index, new_array_index;
	int capacity, new_capacity;
	uint16_t version_count;
	PLeafVersion first_version, new_first_version;
	PLeafVersionIndex version_index, new_version_index;
	PLeafOffset new_offset;

	Assert(index != 0);

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_count = old_array->version_count;
		
	/*
	 * If the number of versions in indirect array is one,
	 * remove this indirect array.
	 */
	if ((PLeafGetVersionType(*version_index) == PLEAF_INDIRECT_ARRAY) &&
			version_count == 1) 
	{
		new_offset = PLeafGetVersionOffset(PLeafPageGetVersion(first_version,
										   old_array->visible_index[0]));

		if (index == LAST_ARRAY_ACCESSED)
		{
			__sync_lock_test_and_set(ret_offset, 
				PLeafMakeOffset(free_pool->gen_no, new_offset));
		}
		else
		{
			PLeafChangeUpperOffset(temp_info, index - 1, new_offset);
		}

		goto func_exit;
	}

	new_capacity = PLeafGetCapacity(new_capacity_index);
	Assert(new_capacity >= version_count && new_capacity <= capacity);

	/* Get new version array with the capacity */
	new_page = PLeafGetFreePageWithCapacity(
			&new_frame_id, free_pool, new_capacity_index, PLeafPageGetInstNo(page));

	new_offset = PLeafFindFreeSlot(
			new_page, new_frame_id, free_pool, PLeafGetVersionType(*version_index));
	ASSERT_OFFSET(new_offset, new_capacity);

	new_array_index = PLeafPageGetArrayIndex(new_capacity, new_offset);
	ASSERT_ARR_IDX(new_array_index);

	new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

	new_first_version =
		PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

	/* Copy version data array */
	for (int i = 0; i < version_count; i++)
	{
		PLeafCopyVersion(
			PLeafPageGetVersion(new_first_version, i),
			PLeafPageGetVersion(first_version, old_array->visible_index[i]));
	}

	PLeafAppendVersionFast(new_first_version, new_version_index, 0,
						   version_count, new_capacity, xmin, xmax,
						   version_offset);

	Assert(!PLeafCheckEmptiness(
				PLeafGetVersionIndexHead(*new_version_index),
				PLeafGetVersionIndexTail(*new_version_index)));

	/* 
	 * Change offset value after contraction based on current circumstance
	 * If this version-array is directly connected to record, change record's
	 * offset (with generation number).
	 */
	if (index == LAST_ARRAY_ACCESSED) 
	{
		__sync_lock_test_and_set(ret_offset,
			PLeafMakeOffset(free_pool->gen_no, new_offset));
	} 
	else 
	{
		/*
		 * In this case, there is an indirect array connected to this version array
		 * Change the indirect array element's offset value.
		 */
		PLeafChangeUpperOffset(temp_info, index - 1, new_offset);
	}

	PLeafHoldPage(new_frame_id);

func_exit:

	PLeafHoldFreeSlot(page, frame_id, free_pool, array_index);	
}
#else /* !LOCATOR */
static void
PLeafContractVersions(PLeafTempInfo temp_info, 
							int index, 
							PLeafFreePool free_pool,
							PLeafOffset* ret_offset, 
							LWLock* rwlock) 
{
	PLeafPage page, new_page;
	int frame_id, new_frame_id;
	int array_index, new_array_index;
	int capacity, new_capacity;
	int capacity_index, new_capacity_index;
	uint16_t version_count;
	uint16_t version_head, version_tail;
	PLeafVersion first_version, new_first_version;
	PLeafVersionIndex version_index, new_version_index;
	PLeafOffset new_offset;

	Assert(index != 0);

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);
	capacity_index = PLeafPageGetCapacityIndex(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_head = PLeafGetVersionIndexHead(*version_index);
	version_tail = PLeafGetVersionIndexTail(*version_index);

	Assert(!PLeafCheckEmptiness(version_head, version_tail));

	version_count = GetVersionCount(version_head, version_tail, capacity);

	Assert(version_count != 0);
	/* Get new capacity */
	new_capacity_index = 
		PLeafGetNewCapacityIndex(capacity_index, capacity, version_count,
				version_head, version_tail);

	new_capacity = PLeafGetCapacity(new_capacity_index);
	Assert(new_capacity >= version_count && new_capacity < capacity);

	/* Get new version array with the capacity */
	new_page = PLeafGetFreePageWithCapacity(
			&new_frame_id, free_pool, new_capacity_index, PLeafPageGetInstNo(page));

	new_offset = PLeafFindFreeSlot(
			new_page, new_frame_id, free_pool, PLeafGetVersionType(*version_index));
	ASSERT_OFFSET(new_offset, new_capacity);

	new_array_index = PLeafPageGetArrayIndex(new_capacity, new_offset);
	ASSERT_ARR_IDX(new_array_index);

	new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

	new_first_version =
		PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

	/* Copy version data array */
	PLeafCopyVersions(new_first_version, first_version, 
			version_head % capacity, version_tail % capacity, 
			version_count, capacity);

	PLeafSetVersionIndexTail(new_version_index, version_count);
	Assert(!PLeafCheckEmptiness(
				PLeafGetVersionIndexHead(*new_version_index),
				PLeafGetVersionIndexTail(*new_version_index)));


	PLeafMarkDirtyPage(new_frame_id);

	//LWLockAcquire(rwlock, LW_EXCLUSIVE);

	/* Change offset value after contraction based on current circumstance
	 * If this version-array is directly connected to record,
	 * change record's offset (with generation number).
	 */
	if (index == LAST_ARRAY_ACCESSED) 
	{
		
		if ((PLeafGetVersionType(*new_version_index) == PLEAF_INDIRECT_ARRAY) &&
				version_count == 1) 
		{
			/*
			 * If the number of versions in indirect array is one,
			 * remove this indirect array.
			 */
			ereport(LOG, (errmsg("kk")));
			*ret_offset = 
				PLeafMakeOffset(free_pool->gen_no, 
						PLeafGetVersionOffset(new_first_version));
			
			goto func_exit;
		} 
		else
		{
			*ret_offset = PLeafMakeOffset(free_pool->gen_no, new_offset);
		}
	} 
	else 
	{
		/*
		 * In this case, there is an indirect array connected to this version array
		 * Change the indirect array element's offset value.
		 */
		PLeafChangeUpperOffset(temp_info, index - 1, new_offset);
	}

	//LWLockRelease(rwlock);
	PLeafReleaseFreeSlot(page, frame_id, free_pool, capacity, array_index);	

	PLeafReleasePage(new_frame_id);
	return;

func_exit:

	//LWLockRelease(rwlock);
	PLeafReleaseFreeSlot(page, frame_id, free_pool, capacity, array_index);	
	PLeafReleaseFreeSlot(new_page, new_frame_id, 
					free_pool, new_capacity, new_array_index);

	PLeafReleasePage(new_frame_id);
}
#endif /* LOCATOR */

/*
 * PLeafExpandVersions
 *
 * Main routine of expansion 
 */
#ifdef LOCATOR
static bool
PLeafExpandVersions(PLeafTempInfo temp_info, 
						int index,
						PLeafFreePool free_pool,
						TransactionId* xmin, 
						TransactionId* xmax,
						PLeafVersionOffset* version_offset, 
						PLeafOffset* ret_offset,
						PLeafOldArray old_array) 
{
	PLeafPage page, new_page;
	PLeafPageId page_id;
	int frame_id, new_frame_id;
	int array_index, new_array_index;
	int version_count;
	int capacity, capacity_index;
	int new_capacity;
	uint16_t version_head, version_tail;
	uint16_t new_version_head, new_version_tail;
	bool is_max_capacity;
	PLeafVersion first_version, version;
	PLeafVersion new_first_version;
	PLeafVersionIndex version_index, new_version_index;
	TransactionId closed_xmin, closed_xmax;
	PLeafOffset closed_offset;
	PLeafOffset new_offset, new_ret_offset;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	/*
	 * We may need to append new indirect version that represents current 
	 * version array(regardless of version type),
	 * if current version array's capacity is maximum.
	 * Therefore, keep xmin, xmax, offset value.
	 */
	capacity = PLeafPageGetCapacity(page);

	/* True if current version array's capacity is maximum */
	is_max_capacity = (capacity == PLEAF_MAX_CAPACITY);

	capacity_index = PLeafPageGetCapacityIndex(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_head = PLeafGetVersionIndexHead(*version_index);
	version_tail = PLeafGetVersionIndexTail(*version_index);

	version = PLeafPageGetVersion(first_version, version_head % capacity);
	closed_xmin = PLeafGetXmin(PLeafGetVersionId(version));

	version = PLeafPageGetVersion(
			first_version, (version_tail + capacity - 1) % capacity);
	closed_xmax = PLeafGetXmax(PLeafGetVersionId(version));

	Assert(closed_xmax <= *xmin);

	page_id = PLeafFrameToPageId(frame_id);
	closed_offset = PLEAF_ARRAY_INDEX_TO_OFFSET(page_id, capacity, array_index);
	ASSERT_OFFSET(closed_offset, capacity);

	/* Get new page for appending new version */
	if (is_max_capacity) 
	{
		/*
		 * If current version array's capacity is maximum,
		 * get new page with minimum capacity
		 */
		new_page = PLeafGetFreePage(&new_frame_id, free_pool);
	} 
	else 
	{
		/*
		 * If current version array's capacity is not maximum,
		 * get new page with the next capacity
		 */
		new_page = PLeafGetFreePageWithCapacity(&new_frame_id, free_pool,
												capacity_index + 1, PLeafPageGetInstNo(page));
	}

	new_capacity = PLeafPageGetCapacity(new_page);

	/* Find free slot in a new page */
	new_offset = PLeafFindFreeSlot(new_page, new_frame_id, 
										free_pool, PLeafGetVersionType(*version_index));

	ASSERT_OFFSET(new_offset, new_capacity);

	new_array_index = PLeafPageGetArrayIndex(new_capacity, new_offset);
	new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

	new_first_version = 
		PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

	new_version_head = PLeafGetVersionIndexHead(*new_version_index);
	new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

	Assert(PLeafCheckEmptiness(new_version_head, new_version_tail));

	if (is_max_capacity) 
	{	
		/*
		 * In this case, we request a new page with the smallest capacity above.
		 * Therefore, append new version only.
		 */
		PLeafAppendVersionFast(new_first_version, new_version_index,
				new_version_head, new_version_tail, new_capacity, 
				*xmin, *xmax, *version_offset);
	} 
	else 
	{
		/*
		 * In this case, append new version after copying all versions in
		 * the old page.
		 */
		version_count = GetVersionCount(version_head, version_tail, capacity);

		PLeafCopyVersions(new_first_version, first_version,
				version_head % capacity, version_tail % capacity,
				version_count, capacity);

		PLeafSetVersionIndexTail(new_version_index, version_count);
		new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

		PLeafAppendVersionFast(new_first_version, new_version_index,
				new_version_head, new_version_tail, new_capacity, 
				*xmin, *xmax, *version_offset);
	}


	PLeafMarkDirtyPage(new_frame_id);
	PLeafHoldPage(new_frame_id);

	/* It should be different based on capacity and existence of upper layer */
	if (!is_max_capacity) 
	{
		/* Not maximum capacity */
		//LWLockAcquire(rwlock, LW_EXCLUSIVE);

		if (index == LAST_ARRAY_ACCESSED) 
		{
			/* Change record's offset */
			__sync_lock_test_and_set(ret_offset, 
				PLeafMakeOffset(free_pool->gen_no, new_offset));
		} 
		else 
		{
			/* Change upper layer's offset */
			PLeafChangeUpperOffset(temp_info, index - 1, new_offset);
		}

		//LWLockRelease(rwlock);
		
		/* Release used slot in the previous page */
		PLeafHoldFreeSlot(page, frame_id, free_pool, array_index);
		PLeafMarkDirtyPage(frame_id);
		return true;
	}
	else 
	{
		/* Maximum capacity */
		if (index == LAST_ARRAY_ACCESSED) 
		{
			/*
			 * In this case, we should get a new indirect version array with
			 * the smallest capacity
			 */
			new_page = PLeafGetFreePage(&new_frame_id, free_pool);
			new_capacity = PLeafPageGetCapacity(new_page);

			new_ret_offset = PLeafFindFreeSlot(
					new_page, new_frame_id, free_pool, PLEAF_INDIRECT_ARRAY);

			ASSERT_OFFSET(new_ret_offset, new_capacity);

			new_array_index = PLeafPageGetArrayIndex(new_capacity, new_ret_offset);
			new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

			new_first_version =
				PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

			new_version_head = PLeafGetVersionIndexHead(*new_version_index);
			new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

			Assert(PLeafCheckEmptiness(new_version_head, new_version_tail));

			/* closed_xmin, closed_xmax, closed_offset */
			PLeafAppendVersionFast(new_first_version, new_version_index,
					new_version_head, new_version_tail, new_capacity,
					closed_xmin, closed_xmax, closed_offset);

			new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

			PLeafAppendVersionFast(
					new_first_version, new_version_index,
					new_version_head, new_version_tail, new_capacity, 
					*xmin, MyMaxTransactionId, new_offset); 

			
			PLeafMarkDirtyPage(new_frame_id);	
			PLeafHoldPage(new_frame_id);

			__sync_lock_test_and_set(ret_offset, 
				PLeafMakeOffset(free_pool->gen_no, new_ret_offset));

			return true;
		} 
		else 
		{
			/* No need to acquire or release the latch */
			// PLeafChangeUpperXmin(temp_info, index - 1, closed_xmin);
			PLeafChangeUpperXmax(temp_info, index - 1, closed_xmax);

			*xmax = MyMaxTransactionId;
			*version_offset = new_offset;

			/* Append new version to upper layer */
			return false;
		}
	}
}
#else /* !LOCATOR */
static bool
PLeafExpandVersions(PLeafTempInfo 
						temp_info, 
						int index,
						PLeafFreePool free_pool,
						TransactionId* xmin, 
						TransactionId* xmax,
						PLeafVersionOffset* version_offset, 
						PLeafOffset* ret_offset,
						LWLock* rwlock) 
{
	PLeafPage page, new_page;
	PLeafPageId page_id;
	int frame_id, new_frame_id;
	int array_index, new_array_index;
	int version_count;
	int capacity, capacity_index;
	int new_capacity;
	uint16_t version_head, version_tail;
	uint16_t new_version_head, new_version_tail;
	bool flag;
	PLeafVersion first_version, version;
	PLeafVersion new_first_version;
	PLeafVersionIndex version_index, new_version_index;
	TransactionId closed_xmin, closed_xmax;
	PLeafOffset closed_offset;
	PLeafOffset new_offset, new_ret_offset;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	/*
	 * We may need to append new indirect version that represents current 
	 * version array(regardless of version type),
	 * if current version array's capacity is maximum.
	 * Therefore, keep xmin, xmax, offset value.
	 */
	capacity = PLeafPageGetCapacity(page);

	/* True if current version array's capacity is maximum */
	flag = (capacity == PLEAF_MAX_CAPACITY);

	capacity_index = PLeafPageGetCapacityIndex(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_head = PLeafGetVersionIndexHead(*version_index);
	version_tail = PLeafGetVersionIndexTail(*version_index);

	version = PLeafPageGetVersion(first_version, version_head % capacity);
	closed_xmin = PLeafGetXmin(PLeafGetVersionId(version));

	version = PLeafPageGetVersion(
			first_version, (version_tail + capacity - 1) % capacity);
	closed_xmax = PLeafGetXmax(PLeafGetVersionId(version));

	Assert(closed_xmax <= *xmin);

	page_id = PLeafFrameToPageId(frame_id);
	closed_offset = PLEAF_ARRAY_INDEX_TO_OFFSET(page_id, capacity, array_index);
	ASSERT_OFFSET(closed_offset, capacity);

	/* Get new page for appending new version */
	if (flag) 
	{
		/*
		 * If current version array's capacity is maximum,
		 * get new page with minimum capacity
		 */
		new_page = PLeafGetFreePage(&new_frame_id, free_pool);
	} 
	else 
	{
		/*
		 * If current version array's capacity is not maximum,
		 * get new page with the next capacity
		 */
		new_page = PLeafGetFreePageWithCapacity(&new_frame_id, free_pool,
												capacity_index + 1, PLeafPageGetInstNo(page));
	}

	new_capacity = PLeafPageGetCapacity(new_page);

	/* Find free slot in a new page */
	new_offset = PLeafFindFreeSlot(new_page, new_frame_id, 
										free_pool, PLeafGetVersionType(*version_index));

	ASSERT_OFFSET(new_offset, new_capacity);

	new_array_index = PLeafPageGetArrayIndex(new_capacity, new_offset);
	new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

	new_first_version = 
		PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

	new_version_head = PLeafGetVersionIndexHead(*new_version_index);
	new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

	Assert(PLeafCheckEmptiness(new_version_head, new_version_tail));

	if (flag) 
	{	
		/*
		 * In this case, we request a new page with the smallest capacity above.
		 * Therefore, append new version only.
		 */
		PLeafAppendVersionFast(new_first_version, new_version_index,
				new_version_head, new_version_tail, new_capacity, 
				*xmin, *xmax, *version_offset);
	} 
	else 
	{
		/*
		 * In this case, append new version after copying all versions in
		 * the old page.
		 */
		version_count = GetVersionCount(version_head, version_tail, capacity);

		PLeafCopyVersions(new_first_version, first_version,
				version_head % capacity, version_tail % capacity,
				version_count, capacity);

		PLeafSetVersionIndexTail(new_version_index, version_count);
		new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

		PLeafAppendVersionFast(new_first_version, new_version_index,
				new_version_head, new_version_tail, new_capacity, 
				*xmin, *xmax, *version_offset);
	}


	PLeafMarkDirtyPage(new_frame_id);
	PLeafReleasePage(new_frame_id);

	/* It should be different based on capacity and existence of upper layer */
	if (!flag) 
	{
		/* Not maximum capacity */
		//LWLockAcquire(rwlock, LW_EXCLUSIVE);

		if (index == LAST_ARRAY_ACCESSED) 
		{
			/* Change record's offset */
			*ret_offset = PLeafMakeOffset(free_pool->gen_no, new_offset);
		} 
		else 
		{
			/* Change upper layer's offset */
			PLeafChangeUpperOffset(temp_info, index - 1, new_offset);
		}

		//LWLockRelease(rwlock);
		
		/* Release used slot in the previous page */
		PLeafReleaseFreeSlot(page, frame_id, free_pool, capacity, array_index);
		PLeafMarkDirtyPage(frame_id);
		return true;
	}
	else 
	{
		/* Maximum capacity */
		if (index == LAST_ARRAY_ACCESSED) 
		{
			/*
			 * In this case, we should get a new indirect version array with
			 * the smallest capacity
			 */
			new_page = PLeafGetFreePage(&new_frame_id, free_pool);
			new_capacity = PLeafPageGetCapacity(new_page);

			new_ret_offset = PLeafFindFreeSlot(
					new_page, new_frame_id, free_pool, PLEAF_INDIRECT_ARRAY);

			ASSERT_OFFSET(new_ret_offset, new_capacity);

			new_array_index = PLeafPageGetArrayIndex(new_capacity, new_ret_offset);
			new_version_index = PLeafPageGetVersionIndex(new_page, new_array_index);

			new_first_version =
				PLeafPageGetFirstVersion(new_page, new_array_index, new_capacity);

			new_version_head = PLeafGetVersionIndexHead(*new_version_index);
			new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

			Assert(PLeafCheckEmptiness(new_version_head, new_version_tail));

			/* closed_xmin, closed_xmax, closed_offset */
			PLeafAppendVersionFast(new_first_version, new_version_index,
					new_version_head, new_version_tail, new_capacity,
					closed_xmin, closed_xmax, closed_offset);

			new_version_tail = PLeafGetVersionIndexTail(*new_version_index);

			PLeafAppendVersionFast(
					new_first_version, new_version_index,
					new_version_head, new_version_tail, new_capacity, 
					*xmin, MyMaxTransactionId, new_offset); 

			
			PLeafMarkDirtyPage(new_frame_id);	
			PLeafReleasePage(new_frame_id);

			*ret_offset = PLeafMakeOffset(free_pool->gen_no, new_ret_offset);

			return true;
		} 
		else 
		{
			/* No need to acquire or release the latch */
			// PLeafChangeUpperXmin(temp_info, index - 1, closed_xmin);
			PLeafChangeUpperXmax(temp_info, index - 1, closed_xmax);

			*xmax = MyMaxTransactionId;
			*version_offset = new_offset;

			/* Append new version to upper layer */
			return false;
		}
	}
}
#endif /* LOCATOR */

/*
 * PLeafAppendVersionSlow
 *
 * Main sub-routine for appending new version.
 */
#ifdef LOCATOR
static int
PLeafAppendVersionSlow(int* new_cap_index,
							PLeafTempInfo temp_info, 
							int index, 
							PLeafFreePool free_pool,
							TransactionId xmin, 
							TransactionId xmax, 
							PLeafVersionOffset version_offset,
							PLeafOldArray old_array) 
{
	PLeafPage page;
	int frame_id;
	int array_index;
	int capacity, capacity_index;
	uint16_t version_head, version_tail;
	PLeafVersion first_version;
	PLeafVersionIndex version_index;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);
	capacity_index = PLeafPageGetCapacityIndex(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_head = PLeafGetVersionIndexHead(*version_index);
	version_tail = PLeafGetVersionIndexTail(*version_index);

	if (PLeafCheckAppendness(capacity, version_head, version_tail)) 
	{
		/* The version array has enough space */
		PLeafAppendVersionFast(first_version, version_index, 
				version_head, version_tail, capacity, xmin, xmax, version_offset);
		
		PLeafMarkDirtyPage(frame_id);
		Assert(!PLeafCheckEmptiness(
					version_head, PLeafGetVersionIndexTail(*version_index)));

		return PLEAF_APPEND_DONE;
	}

	/* Do cleaning by moving head, or count and record invisible versions */
	if (PLeafCompactVersions(new_cap_index, free_pool, first_version, 
				version_index, version_head, version_tail, capacity_index, old_array)) 
	{
		/* Success to clean some obsolete versions from head */
		Assert(version_head != PLeafGetVersionIndexHead(*version_index));

		version_head = PLeafGetVersionIndexHead(*version_index);
		Assert(PLeafCheckAppendness(capacity, version_head, version_tail));

		PLeafAppendVersionFast(first_version, version_index,
				version_head, version_tail, capacity, xmin, xmax, version_offset);

		PLeafMarkDirtyPage(frame_id);
		version_tail = PLeafGetVersionIndexTail(*version_index);
		Assert(!PLeafCheckEmptiness(version_head, version_tail));

		return PLEAF_APPEND_DONE;
	}

	if (*new_cap_index <= capacity_index)
		return PLEAF_APPEND_COMPRESS;

	/*
	 * If fail to append new version, return PLEAF_APPEND_EXPAND
	 * See PLeafExpandVersions()
	 */
	return PLEAF_APPEND_EXPAND;
}
#else /* !LOCATOR */
static int
PLeafAppendVersionSlow(PLeafTempInfo temp_info, 
							int index, 
							PLeafFreePool free_pool,
							TransactionId xmin, 
							TransactionId xmax, 
							PLeafVersionOffset version_offset,
							LWLock* rwlock) 
{
	PLeafPage page;
	int frame_id;
	int array_index;
	int capacity, capacity_index;
	uint16_t version_head, version_tail;
	PLeafVersion first_version;
	PLeafVersionIndex version_index;

	PLeafGetTempInfo(temp_info, index, &page, &frame_id, &array_index);

	capacity = PLeafPageGetCapacity(page);
	capacity_index = PLeafPageGetCapacityIndex(page);

	version_index = PLeafPageGetVersionIndex(page, array_index);
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity);

	version_head = PLeafGetVersionIndexHead(*version_index);
	version_tail = PLeafGetVersionIndexTail(*version_index);

	if (PLeafCheckAppendness(capacity, version_head, version_tail)) 
	{
		/* The version array has enough space */
		PLeafAppendVersionFast(first_version, version_index, 
				version_head, version_tail, capacity, xmin, xmax, version_offset);
		
		PLeafMarkDirtyPage(frame_id);
		Assert(!PLeafCheckEmptiness(
					version_head, PLeafGetVersionIndexTail(*version_index)));
		return PLEAF_APPEND_DONE;
	}

	/* Do cleaning from the head until accessing the live version */
	if (PLeafCompactVersions(true, free_pool, first_version, 
				version_index, version_head, version_tail, capacity, rwlock)) 
	{
		/* Success to clean some obsolete versions from head */
		Assert(version_head != PLeafGetVersionIndexHead(*version_index));

		version_head = PLeafGetVersionIndexHead(*version_index);
		Assert(PLeafCheckAppendness(capacity, version_head, version_tail));

		PLeafAppendVersionFast(first_version, version_index,
				version_head, version_tail, capacity, xmin, xmax, version_offset);

		PLeafMarkDirtyPage(frame_id);
		version_tail = PLeafGetVersionIndexTail(*version_index);
		Assert(!PLeafCheckEmptiness(version_head, version_tail));

		if (PLeafCheckContraction(
					capacity_index, capacity, version_head, version_tail)) 
		{
			return PLEAF_APPEND_CONTRACT;
		}
		return PLEAF_APPEND_COMPACT;
	}

	/* Do cleaning for all versions */
	if (PLeafCompactVersions(false, free_pool, first_version,
				version_index, version_head, version_tail, capacity, rwlock)) 
	{
		/* Success to clean some obsolete versions and compact version array */
		Assert((version_tail != PLeafGetVersionIndexTail(*version_index))
				|| (version_head != PLeafGetVersionIndexHead(*version_index)));

		version_head = PLeafGetVersionIndexHead(*version_index);
		version_tail = PLeafGetVersionIndexTail(*version_index);
		Assert(PLeafCheckAppendness(capacity, version_head, version_tail));

		PLeafAppendVersionFast(first_version, version_index,
				version_head, version_tail, capacity, xmin, xmax, version_offset);


		PLeafMarkDirtyPage(frame_id);
		version_tail = PLeafGetVersionIndexTail(*version_index);
		Assert(!PLeafCheckEmptiness(version_head, version_tail));

		if (PLeafCheckContraction(
					capacity_index, capacity, version_head, version_tail)) 
		{
			return PLEAF_APPEND_CONTRACT;
		}
		return PLEAF_APPEND_COMPACT;
	}

	/*
	 * If fail to append new version, return PLEAF_APPEND_EXPAND
	 * See PLeafExpandVersions()
	 */
	return PLEAF_APPEND_EXPAND;
}
#endif /* LOCATOR */

/*
 * PLeafAppendVersion
 *
 * Append new version index to p-leaf version array.
 */
void
PLeafAppendVersion(PLeafOffset offset,
						PLeafOffset* ret_offset,
						TransactionId xmin,
						TransactionId xmax,
						PLeafVersionOffset version_offset,
						LWLock* rwlock)
{
	PLeafPage page;
	PLeafPageId	page_id;
	PLeafFreePool free_pool;
	int frame_id;
	int status;
#ifdef LOCATOR
	int page_count;
#else
	int page_count, max_page_count;
#endif /* LOCATOR */
	bool stop;
	TransactionId current_xmin, current_xmax;
	PLeafVersionOffset current_version_offset;
	PLeafOffset internal_offset = 0;
	PLeafTempInfoData temp_info[MAX_ARRAY_ACCESSED];
	PLeafGenNumber gen_no;
#ifdef LOCATOR
	int new_cap_index;
	PLeafOldArrayData old_array;
#endif /* LOCATOR */
	
	memset(temp_info, 0x00, sizeof(PLeafTempInfoData) * MAX_ARRAY_ACCESSED);
	/*
	 * If offset is invalid, allocate a new array with the smallest capacity.
	 * It only happens in the first update
	 */
	if (unlikely(!PLeafIsOffsetValid(offset)))
	{
		/* Get recent generation number */
		gen_no = PLeafGetLatestGenerationNumber();
		free_pool = PLeafGetFreePool(gen_no);

		/* Get free page from pool */
		page = PLeafGetFreePage(&frame_id, free_pool);

		internal_offset = 
			PLeafFindFreeSlot(page, frame_id, free_pool, PLEAF_DIRECT_ARRAY); 
		/*
		 * In the first update, modification of record's offset in here is safe.
		 * Because it must be guaranteed that no reader accesses p-leaf at
		 * this moment under all circumstances.
		 */
		*ret_offset = PLeafMakeOffset(gen_no, internal_offset);
	}
	else
	{
		gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(offset);
		free_pool = PLeafGetFreePool(gen_no);

		internal_offset = PLEAF_OFFSET_TO_INTERNAL_OFFSET(offset);

		page_id = PLEAF_OFFSET_TO_PAGE_ID(internal_offset);
		page = PLeafGetPage(page_id, gen_no, false, &frame_id);
	}

#ifdef LOCATOR
	PLeafInitHoldingResources();
#endif /* LOCATOR */

	/*
	 * Search the direct version array for appending the verison
	 * We should pin our pages accessed and keep its information temporarily
	 */
	page_count = 
		PLeafSearchDirectArray(temp_info, page, frame_id, gen_no, internal_offset);

	/*
	 * The steps are as follows
	 *
	 * We may change version information(xmin or max) in the upper layer after
	 * contraction (not available now)
	 *
	 * 1. Check Appendness (version-array has enough space to append) 
	 * If success, append its new version to version array.
	 * Other things don't need to consider in both direct and indirect array.
	 * Else, go to next step
	 *
	 * 2. Do garbage collection until accessing the live version
	 * If success to clean any obsolete versions, append its new version
	 * 	If condition of contraction is satisfied, go to 3-(2)
	 *  [ See PLeafCheckContraction() ] 
	 *	Else, nothing to do
	 * Else, go to next step
	 * 
	 * 3. Do garbage collection of all versions
	 * After garbage collection, there will be three cases
	 *
	 * 1) If fail to clean any versions, do expansion its version array.
	 *		And then append its new version
	 *
	 * 1-1) The current version array's capacity is not maximum
	 * (A) It has upper indirect version array
	 * (B) It does not have upper indirect version array
	 * 
	 * (A+B)	Get new version array with the next capacity, 
	 * 				and then copy all versions and append
	 * 				new version to new version array
	 *
	 * (A)		Change the offset of the last one in upper indirect version array 
	 * (B)		Change the offset of record directly 
	 *
	 * 1-2) The current version array's capacity is maximum
	 * (A) It has upper indirect version array
	 * (B) It does not have upper indirect version array
	 *
	 * (A+B)	Get a version array with smallest capcity, 
	 * 				and then append new version to new version array.
	 *
	 * (A)		Change xmax value of last one in upper indirect version array,
	 * 				and then append the version that represents version array to
	 * 				upper indirect version array recursively
	 *
	 * (B)		Get an indirect version array to append two versions
	 * 				that represent version arrays
	 * 				After that, change the offset of record directly
	 *
	 * We should consider about version offset, xmin, xmax, and return offset.
	 * Each case may have different details, so do carefully.
	 *
	 * 2) The condition of contraction is satisfied
	 *
	 * Get a version array with proper capacity, 
	 * and then copy its all versions to it
	 * It changes the offset in upper layer (record's or last element's).
	 *
	 * 3) Append its version as normal case
	 * Append its new version to version array
	 *
	 */
	current_xmin = xmin;
	current_xmax = xmax;
	current_version_offset = version_offset;
#ifndef LOCATOR
	max_page_count = page_count;
#endif /* LOCATOR */

#ifdef LOCATOR
	/*
	 * If the version can be appended directly, do so.
	 * Or not, move head pointer if available and check and set new capacity.
	 * If the array needs to be compressed or expanded, do so.
	 */
	while (true) 
	{
		stop = true;

		/* Append version directly, or move head pointer and set new capacity */
		status = PLeafAppendVersionSlow(&new_cap_index, temp_info, page_count, free_pool, 
								current_xmin, current_xmax, current_version_offset,
								&old_array);

		switch (status) 
		{
			case PLEAF_APPEND_DONE:
				break;
			case PLEAF_APPEND_COMPRESS:
				/* Compress the array (new capacity <= current capacity) */
				PLeafCompressVersions(new_cap_index, temp_info, page_count, 
									  free_pool, ret_offset,
									  current_xmin, current_xmax,
									  current_version_offset, &old_array);
				break;
			case PLEAF_APPEND_EXPAND:
				/* Expand the array (new capacity > current capacity) */
				stop =
					PLeafExpandVersions(temp_info, page_count, free_pool,
										&current_xmin, &current_xmax, 
										&current_version_offset, ret_offset,
										&old_array);
				break;
			default:
        		return;
		}

		if (stop)
			break;

		page_count--;
		Assert(page_count != 0);
	}
#else
	while (true) 
	{
		stop = true;
		status = PLeafAppendVersionSlow(temp_info, page_count, free_pool, 
								current_xmin, current_xmax, current_version_offset, rwlock);

		switch (status) 
		{
			case PLEAF_APPEND_DONE:
			case PLEAF_APPEND_COMPACT:
				/* Nothing to do */
				break;
			case PLEAF_APPEND_CONTRACT:
				PLeafContractVersions(temp_info, page_count, 
												free_pool, ret_offset, rwlock);
				break;
			case PLEAF_APPEND_EXPAND:
				stop =
					PLeafExpandVersions(temp_info, page_count, free_pool,
							&current_xmin, &current_xmax, 
							&current_version_offset, ret_offset, rwlock);
				break;
			default:
				return;
		}

		if (stop)
			break;

		page_count--;
		Assert(page_count != 0);
	}
#endif /* LOCATOR */

#ifndef LOCATOR
	/* Release all pages in temporarily information */
	for (int i = 1; i <= max_page_count; ++i)
	{
		PLeafReleasePage(temp_info[i].frame_id);
	}
#endif /* LOCATOR */
}

#endif
