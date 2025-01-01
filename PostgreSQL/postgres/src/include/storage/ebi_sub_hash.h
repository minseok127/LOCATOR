/*-------------------------------------------------------------------------
 *
 * ebi_tree_hash.h
 *    Hash table definitions for mapping EbiSubPage to EbiSubBuffer indexes.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ebi_tree_hash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_SUB_HASH_H
#define EBI_SUB_HASH_H

#define EbiSubHashPartition(hashcode) ((hashcode) % NUM_EBI_SUB_PARTITIONS)

#define EbiSubMappingPartitionLock(hashcode)          \
	(&MainLWLockArray[EBI_SUB_MAPPING_LWLOCK_OFFSET + \
					  EbiSubHashPartition(hashcode)]  \
		  .lock)

extern Size EbiSubHashShmemSize(int size);
extern void EbiSubHashInit(int size);
extern uint32 EbiSubHashCode(const EbiSubBufTag* tagPtr);

extern int EbiSubHashLookup(const EbiSubBufTag* tagPtr, uint32 hashcode);
extern int EbiSubHashInsert(const EbiSubBufTag* tagPtr,
							 uint32 hashcode,
							 int page_id);
extern void EbiSubHashDelete(const EbiSubBufTag* tagPtr, uint32 hashcode);

#endif /* EBI_SUB_HASH_H */
