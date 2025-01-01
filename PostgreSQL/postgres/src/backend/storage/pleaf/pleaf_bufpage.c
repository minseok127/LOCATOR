/*-------------------------------------------------------------------------
 *
 * pleaf_bufpage.c
 * 		Internal operation in pleaf buffer pages 
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
 *    src/backend/storage/pleaf/pleaf_bufpage.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include <stdbool.h>
#include <unistd.h>

#include "storage/pleaf_bufpage.h"

/* Helper arrays about capacity */
int CAP[N_PAGE_CAP_ARR] = {4, 8, 16, 30, 60, 120, 240};
int CONT_CAP[N_PAGE_CAP_ARR] = {2, 6, 12, 23, 45, 90, INT32_MAX};
int BIT_CAP[N_PAGE_CAP_ARR] = {60, 30, 15, 8, 4, 2, 1};
/*
 * PLeafPageSetNextPageId
 *
 * Set next page id field in a given page
 */
void 
PLeafPageSetNextPageId(PLeafPage page, PLeafPageId page_id) 
{
	PLeafPageMetadata* next_page_id = 
										&(((PLeafPageHeader)(page))->next_page_id);
	PLeafPageMetadata new_page_id = 
										((*next_page_id & ~PLEAF_PAGE_ID_MASK) | page_id);

	*next_page_id = new_page_id;
}

/*
 * PLeafPageSetCapAndInstNo
 *
 * When initializing the new page, set its capacity index and instance number
 *
 * Called only in initialization phase
 */
void
PLeafPageSetCapAndInstNo(PLeafPage page, 
								int cap_index, 
								int inst_no) 
{
	PLeafPageMetadata new_page_id;
	PLeafPageMetadata* next_page_id = &(((PLeafPageHeader)(page))->next_page_id);
	*next_page_id = 0;

	/* next page id: PLEAF_INVALID_PAGE_ID in initialization */
	new_page_id = 
		(PLEAF_INVALID_PAGE_ID | 
		 (((uint64_t)(cap_index & PLEAF_PAGE_CAP_MASK)) << 60) |									
		 (((uint64_t)(inst_no & PLEAF_PAGE_INST_MASK)) << 56));

	*next_page_id = new_page_id;
}

void PLeafPageInitBitmap(PLeafPage page, 
								int cap_index)
{
	PLeafBitmap* bitmap = &(((PLeafPageHeader)(page))->bitmap);
	*bitmap = ((uint64_t)(-1)) >> (64 - BIT_CAP[cap_index]);
}

/*
 * PLeafPageSetBitmap
 *
 * Set bitmap value when allocating new slot in a given page
 */
bool 
PLeafPageSetBitmap(PLeafPage page, 
						PLeafPageId page_id, 
						PLeafOffset* offset) 
{
	/* No concurrent users set bitmap in the same page */
	PLeafBitmap* bitmap = PLeafPageGetBitmap(page);

	int pos = ffsll(*bitmap);
	int cap = PLeafPageGetCapacity(page);
	PLeafBitmap ret_bitmap;

	Assert(*bitmap != 0);
	Assert(pos != 0);
	Assert(pos <= BIT_CAP[PLeafPageGetCapacityIndex(page)] + 1);
	Assert((*bitmap & ((uint64_t)(1) << (pos - 1))) != 0);

	ret_bitmap = __sync_and_and_fetch(bitmap, ~((uint64_t)(1) << (pos - 1)));

	*offset = PLEAF_ARRAY_INDEX_TO_OFFSET(page_id, cap, (pos - 1));

	return (ret_bitmap == PLEAF_BITMAP_FULL);
}

/*
 * PLeafPageUnsetBitmap
 *
 * Unset bitmap value when deallocating used slot in a given page
 */
bool 
PLeafPageUnsetBitmap(PLeafPage page, int array_index) 
{
	/* Concurrent users may unset bitmap in the same page */
	PLeafBitmap* bitmap = PLeafPageGetBitmap(page);
	PLeafBitmap ret_bitmap;

	if ((*bitmap & ((uint64_t)(1) << array_index)) != 0)
	{
		sleep(20);
	}
	Assert((*bitmap & ((uint64_t)(1) << array_index)) == 0);

	ret_bitmap = __sync_fetch_and_or(bitmap, ((uint64_t)(1) << array_index));

	return (ret_bitmap == PLEAF_BITMAP_FULL);
}

bool
PLeafPageIsSetBitmap(PLeafPage page, int array_index)
{
	PLeafBitmap* bitmap = PLeafPageGetBitmap(page);
	return ((*bitmap & ((uint64_t)(1) << array_index)) == 0);
}

int
PLeafGetCapacity(int cap_index)
{
	return CAP[cap_index];
}

int 
PLeafGetProperCapacityIndex(int version_count) {
	for (int i = 0; i < N_PAGE_CAP_ARR; ++i) {
		if (version_count <= CONT_CAP[i])
			return i;
	}
	Assert(false);
	return -1;
}

int
PLeafPageGetCapacity(PLeafPage page)
{
	return CAP[PLeafPageGetCapacityIndex(page)];
}

#ifdef LOCATOR 
PLeafVersionIndexData
PLeafSetVersionIndexHead(PLeafVersionIndex version_index,
						 uint16_t version_head) {
	PLeafVersionIndexData tmp;
	tmp = ((version_head << 16) | (*version_index & 
		  (PLEAF_VERSION_TYPE_MASK | PLEAF_VERSION_TAIL_MASK)));
	return __sync_lock_test_and_set(version_index, tmp);
}

PLeafVersionIndexData
PLeafSetVersionIndexTail(PLeafVersionIndex version_index,
						 uint16_t version_tail) {
	PLeafVersionIndexData tmp;
	tmp = ((*version_index & ~PLEAF_VERSION_TAIL_MASK) |
			version_tail);
	return __sync_lock_test_and_set(version_index, tmp);
}

PLeafVersionId
PLeafPageSetVersionXmax(PLeafVersion version, TransactionId xmax) {
	PLeafVersionId tmp;
	tmp = ((version->version_id & ~PLEAF_VERSION_ID_MASK) | xmax);
	return __sync_lock_test_and_set(&(version->version_id), tmp);
}
#endif /* LOCATOR */

#endif
