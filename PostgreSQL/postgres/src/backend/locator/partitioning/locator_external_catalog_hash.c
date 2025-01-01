/*-------------------------------------------------------------------------
 *
 * locator_external_catalog_hash.c
 *
 * Hash table implementation for mapping relation structure.
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
 *    src/backend/locator/partitioning/locator_external_catalog_hash.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef LOCATOR
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"

#include "locator/locator_external_catalog.h"

static HTAB *LocatorSharedExternalCatalogHash;

/*
 * LocatorExternalCatalogHashShmemSize
 *
 * compute the size of shared memory for locator external catalog hash
 */
Size
LocatorExternalCatalogHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(LocatorExternalCatalogLookupEntry));
}

/*
 * LocatorExternalCatalogHashInit
 *
 * Initialize locator's external catalog hash in shared memory
 */
void
LocatorExternalCatalogHashInit(int size)
{
	HASHCTL info;
	long num_partitions;

	/* See next_pow2_long(long num) in dynahash.c */
	num_partitions = 1L << my_log2(NUM_LOCATOR_EXTERNAL_CATALOG_PARTITIONS);

	/* LocatorExternalCatalogTag maps to LocatorSharedExternalCatalogHash */
	info.keysize = sizeof(LocatorExternalCatalogTag);
	info.entrysize = sizeof(LocatorExternalCatalogLookupEntry);
	info.num_partitions = num_partitions;

	LocatorSharedExternalCatalogHash = ShmemInitHash(
									  "Shared Locator External Catalog Lookup Table",
									  size,
									  size,
									  &info,
									  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * LocatorExternalCatalogHashCode
 *
 * Compute the hash code associated with a LocatorExternalCatalogTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do this way because the callers need to know the hash code in
 * order to determine which buffer partition to lock, and we don't want to
 * do the hash computation twice (hash_any is a bit slow).
 */
uint32
LocatorExternalCatalogHashCode(const LocatorExternalCatalogTag *tagPtr)
{
	return get_hash_value(LocatorSharedExternalCatalogHash, (void *) tagPtr);
}

/*
 * LocatorExternalCatalogHashLookup
 * 
 * Lookup the given LocatorExternalCatalogTag; return locator external catalog
 * index, or -1 if not found. Caller must hold at least shared lock on partition
 * lock for tag's partition.
 */
int
LocatorExternalCatalogHashLookup(const LocatorExternalCatalogTag *tagPtr, 
								 uint32 hashcode)
{
	LocatorExternalCatalogLookupEntry *result;

	result = (LocatorExternalCatalogLookupEntry *) hash_search_with_hash_value(
		LocatorSharedExternalCatalogHash, 
		(void *) tagPtr, hashcode, HASH_FIND, NULL);

	if (!result) 
		return -1;

	return result->id;
}

/*
 * LocatorExternalCatalogHashInsert
 *
 * Insert a hashtable entry for given tag and index,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on tag's partition
 */
int
LocatorExternalCatalogHashInsert(const LocatorExternalCatalogTag *tagPtr, 
													uint32 hashcode, int index)
{
	LocatorExternalCatalogLookupEntry *result;
	bool found;

	Assert(index >= 0); /* -1 is reserved for not-in-table */

	result = (LocatorExternalCatalogLookupEntry *) hash_search_with_hash_value(
						LocatorSharedExternalCatalogHash,
						(void *) tagPtr, hashcode, HASH_ENTER, &found);

	if (found) /* found something already in the hash table */
		return result->id;

	result->id = index;

	return -1;
}

/*
 * LocatorExternalCatalogHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on tag's partition
 */
void
LocatorExternalCatalogHashDelete(const LocatorExternalCatalogTag *tagPtr, 
								 uint32 hashcode)
{
	LocatorExternalCatalogLookupEntry *result;

	result = (LocatorExternalCatalogLookupEntry *) hash_search_with_hash_value(
		LocatorSharedExternalCatalogHash, 
		(void *) tagPtr, hashcode, HASH_REMOVE, NULL);

	if (!result) 
		elog(ERROR, "shared external catalog hash table corrupted");
}

#endif /* LOCATOR */
