/*-------------------------------------------------------------------------
 *
 * resowner_private.h
 *	  POSTGRES resource owner private definitions.
 *
 * See utils/resowner/README for more info.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/resowner_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESOWNER_PRIVATE_H
#define RESOWNER_PRIVATE_H

#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/catcache.h"
#include "utils/plancache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"

#ifdef LOCATOR
#include <liburing.h>
#include "locator/hap/hap_reduction.h"
#endif /* LOCATOR */

/* support for buffer refcount management */
extern void ResourceOwnerEnlargeBuffers(ResourceOwner owner);
extern void ResourceOwnerRememberBuffer(ResourceOwner owner, Buffer buffer);
extern void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer);

/* support for local lock management */
extern void ResourceOwnerRememberLock(ResourceOwner owner, LOCALLOCK *locallock);
extern void ResourceOwnerForgetLock(ResourceOwner owner, LOCALLOCK *locallock);

/* support for catcache refcount management */
extern void ResourceOwnerEnlargeCatCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheRef(ResourceOwner owner,
											 HeapTuple tuple);
extern void ResourceOwnerForgetCatCacheRef(ResourceOwner owner,
										   HeapTuple tuple);
extern void ResourceOwnerEnlargeCatCacheListRefs(ResourceOwner owner);
extern void ResourceOwnerRememberCatCacheListRef(ResourceOwner owner,
												 CatCList *list);
extern void ResourceOwnerForgetCatCacheListRef(ResourceOwner owner,
											   CatCList *list);

/* support for relcache refcount management */
extern void ResourceOwnerEnlargeRelationRefs(ResourceOwner owner);
extern void ResourceOwnerRememberRelationRef(ResourceOwner owner,
											 Relation rel);
extern void ResourceOwnerForgetRelationRef(ResourceOwner owner,
										   Relation rel);

/* support for plancache refcount management */
extern void ResourceOwnerEnlargePlanCacheRefs(ResourceOwner owner);
extern void ResourceOwnerRememberPlanCacheRef(ResourceOwner owner,
											  CachedPlan *plan);
extern void ResourceOwnerForgetPlanCacheRef(ResourceOwner owner,
											CachedPlan *plan);

/* support for tupledesc refcount management */
extern void ResourceOwnerEnlargeTupleDescs(ResourceOwner owner);
extern void ResourceOwnerRememberTupleDesc(ResourceOwner owner,
										   TupleDesc tupdesc);
extern void ResourceOwnerForgetTupleDesc(ResourceOwner owner,
										 TupleDesc tupdesc);

/* support for snapshot refcount management */
extern void ResourceOwnerEnlargeSnapshots(ResourceOwner owner);
extern void ResourceOwnerRememberSnapshot(ResourceOwner owner,
										  Snapshot snapshot);
extern void ResourceOwnerForgetSnapshot(ResourceOwner owner,
										Snapshot snapshot);

/* support for temporary file management */
extern void ResourceOwnerEnlargeFiles(ResourceOwner owner);
extern void ResourceOwnerRememberFile(ResourceOwner owner,
									  File file);
extern void ResourceOwnerForgetFile(ResourceOwner owner,
									File file);

/* support for dynamic shared memory management */
extern void ResourceOwnerEnlargeDSMs(ResourceOwner owner);
extern void ResourceOwnerRememberDSM(ResourceOwner owner,
									 dsm_segment *);
extern void ResourceOwnerForgetDSM(ResourceOwner owner,
								   dsm_segment *);

/* support for JITContext management */
extern void ResourceOwnerEnlargeJIT(ResourceOwner owner);
extern void ResourceOwnerRememberJIT(ResourceOwner owner,
									 Datum handle);
extern void ResourceOwnerForgetJIT(ResourceOwner owner,
								   Datum handle);

/* support for cryptohash context management */
extern void ResourceOwnerEnlargeCryptoHash(ResourceOwner owner);
extern void ResourceOwnerRememberCryptoHash(ResourceOwner owner,
											Datum handle);
extern void ResourceOwnerForgetCryptoHash(ResourceOwner owner,
										  Datum handle);

/* support for HMAC context management */
extern void ResourceOwnerEnlargeHMAC(ResourceOwner owner);
extern void ResourceOwnerRememberHMAC(ResourceOwner owner,
									  Datum handle);
extern void ResourceOwnerForgetHMAC(ResourceOwner owner,
									Datum handle);

#ifdef DIVA
/* support for ebi buffer management */
extern void ResourceOwnerEnlargeEbiBuffers(ResourceOwner owner);
extern void ResourceOwnerRememberEbiBuffer(ResourceOwner owner, 
										int32_t ebiBuffer);
extern void ResourceOwnerForgetEbiBuffer(ResourceOwner owner, 
										int32_t ebiBuffer);

#ifdef LOCATOR
/* support for read buffer desc management */
extern void ResourceOwnerEnlargeReadBufferDescs(ResourceOwner owner);
extern void ResourceOwnerRememberReadBufferDesc(ResourceOwner owner, 
										ReadBufDesc readBufDesc);
extern void ResourceOwnerForgetReadBufferDesc(ResourceOwner owner, 
										ReadBufDesc readBufDesc);

/* support for io uring management */
extern void ResourceOwnerEnlargeIoUrings(ResourceOwner owner);
extern void ResourceOwnerRememberIoUring(ResourceOwner owner, struct io_uring *ring);
extern void ResourceOwnerForgetIoUring(ResourceOwner owner, struct io_uring *ring);
#endif /* LOCATOR */
#endif /* DIVA */
#ifdef LOCATOR
extern void ResourceOwnerEnlargeSubstitutionCache(ResourceOwner owner);
extern void ResourceOwnerRememberSubstitutionCache(ResourceOwner owner, 
												HapSubstitutionCache *cache);
extern void ResourceOwnerForgetSubstitutionCache(ResourceOwner owner, 
												HapSubstitutionCache *cache);
#endif /* LOCATOR */

#endif							/* RESOWNER_PRIVATE_H */
