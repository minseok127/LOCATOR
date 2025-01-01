/*-------------------------------------------------------------------------
 *
 * pleaf.h
 * 		PLeaf API Declarations 
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLEAF_H
#define PLEAF_H

#include "storage/lwlock.h"

/* PLeafIsLeftUpdate return status */
#define PLEAF_NORMAL	(0)
#define PLEAF_SWITCH	(1)
#define PLEAF_RESET		(2)

extern int
PLeafLookupTuple(Oid rel_id,
				 bool fast_memory_lookup,
				 uint64* ret_vers_offset,
				 uint64 offset,
				 Snapshot snapshot, 
				 uint32* tuple_size,
				 void** ret_value);

extern bool
PLeafIsLeftLookup(
		uint64 left_offset,
		uint64 right_offset,
		TransactionId xid_bound,
		Snapshot snapshot);

#ifdef LOCATOR
extern bool
#else /* !LOCATOR */
extern void
#endif /* LOCATOR */
PLeafAppendTuple(Oid rel_id,
				 uint64 offset,
				 uint64* ret_offset,
				 TransactionId xmin,
				 TransactionId xmax,
				 uint32 tuple_size,
				 const void* tuple,
				 LWLock* rwlock);

extern bool
PLeafIsLeftUpdate(uint64 left_offset, uint64 right_offset, int* ret_status);

extern void
PLeafInit(void);

extern Size
PLeafShmemSize(void);

#ifdef LOCATOR
extern void PLeafReleaseHoldingResources(uint32 pd_gen, uint32 *p_pd_gen);
#endif /* LOCATOR */

#endif /* PLEAF_H */
