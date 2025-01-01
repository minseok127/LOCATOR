/*-------------------------------------------------------------------------
 *
 * ebi_sub_buf.h
 *    EBI Sub Buffer
 *
 *
 *
 * src/include/storage/ebi_sub_buf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_SUB_BUF_H
#define EBI_SUB_BUF_H

#include "port/atomics.h"
#include "storage/ebi_tree.h"

#define EBI_SUB_SEG_PAGESZ ((Size)BLCKSZ)

typedef struct EbiSubBufTag
{
	EbiSubSegmentId seg_id;
	Oid rel_id;
	EbiSubSegmentPageId page_id;
} EbiSubBufTag;

typedef struct EbiSubBufDesc
{
	/* ID of the cached page. seg_id 0 means this entry is unused */
	EbiSubBufTag tag;

	/* Whether the page is not yet synced */
	bool is_dirty;

	/*
	 * Buffer entry with refcnt > 0 cannot be evicted.
	 * We use refcnt as a pin. The refcnt of an appending page should be
	 * kept 1 or higher, and the transaction which filled up the page
	 * should decrease it to unpin it.
	 */
	pg_atomic_uint32 refcnt;

	/* Simple clock algorithm */
	pg_atomic_uint32 usagecnt;
} EbiSubBufDesc;

#define EBI_BUF_USAGECOUNT_ONE (1)
#define EBI_BUF_MAX_USAGECOUNT (((1ULL << 4) - 1))
#define EBI_BUF_OVERFLOW_USAGECOUNT ((1ULL << 31) - 1)

/* Get usagecount from ebi buffer desc */
#define EBI_BUF_DESC_GET_USAGECOUNT(desc) \
			(pg_atomic_read_u32(&((EbiSubBufDesc *) desc)->usagecnt))

#define EBI_SUB_BUF_DESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union EbiSubBufDescPadded {
	EbiSubBufDesc desc;
	char pad[EBI_SUB_BUF_DESC_PAD_TO_SIZE];
} EbiSubBufDescPadded;

/* Metadata for EBI sub buffer */
typedef struct EbiSubBufMeta
{
	/*
	 * Indicate the cache entry which might be a victim for allocating
	 * a new page. Need to use fetch-and-add on this so that multiple
	 * transactions can allocate/evict cache entries concurrently.
	 */
	pg_atomic_uint64 eviction_rr_idx;
} EbiSubBufMeta;

/* Macros used as helper functions */
#define GetEbiSubBufDescriptor(id) (&EbiSubBufDescriptors[(id)].desc)

#define InvalidEbiSubBuf (INT_MAX)

/* Public functions */
extern Size EbiSubBufShmemSize(void);
extern void EbiSubBufInit(void);

extern void EbiSubAppendVersion(EbiSubSegmentId seg_id,
								Oid rel_id,
								EbiSubSegmentOffset seg_offset,
								uint32 tuple_size,
								const void* tuple,
								LWLock* rwlock);
extern int EbiSubReadVersionRef(EbiSubSegmentId seg_id,
								Oid rel_id,
								EbiSubSegmentOffset seg_offset,
								uint32* tuple_size,
								void** ret_value,
								bool fast_memory_lookup);

extern int EbiSubBufGetBufRef(EbiSubSegmentId seg_id,
							  Oid rel_id,
							  EbiSubSegmentOffset seg_offset,
							  bool fast_memory_lookup);

extern void EbiSubCreateSegmentFile(EbiSubSegmentId seg_id, Oid rel_id);
extern void EbiSubRemoveSegmentFile(EbiSubSegmentId seg_id, Oid rel_id);

extern bool EbiSubBufIsValid(int buf_id);

/* Pin count control */
extern void PinEbiSubBuffer(int buf_id);
extern void UnpinEbiSubBuffer(int buf_id);

extern void EbiSubCreateSegmentFile(EbiSubSegmentId seg_id, Oid rel_id);
extern void EbiSubRemoveSegmentFile(EbiSubSegmentId seg_id, Oid rel_id);

extern char *EbiSubGetPage(int buf_id);

extern int EbiSubOpenSegmentFile(EbiSubSegmentId seg_id, Oid rel_id);
extern void EbiSubCloseSegmentFile(int fd);

extern int ConditionalEbiSubBufGetBufRef(EbiSubSegmentId seg_id,
				   						 Oid rel_id, EbiSubSegmentOffset seg_offset);
#ifdef LOCATOR
extern void EbiReleasePrefetchedBuffers(PrefetchDesc prefetchDesc);
extern void EbiSubRemoveOldPartitionFile(Oid rel_id, OldPartitions old_partitions);
#endif /* LOCATOR */


#endif /* EBI_SUB_BUF_H */
