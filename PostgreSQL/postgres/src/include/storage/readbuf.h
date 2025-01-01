/*-------------------------------------------------------------------------
 *
 * readbuf.h
 *    Read Buffer 
 *
 *
 *
 * src/include/storage/readbuf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef READBUF_H
#define READBUF_H

#include "port/atomics.h"
#include "storage/ebi_tree.h"
#include "access/heapam.h"
#include "storage/lwlock.h"

#define INVALID_READBUF (-1)

#define PREFETCH_PAGE_COUNT (256)
#define READBUF_DESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union ReadBufDescPadded {
	ReadBufDescData desc;
	char pad[READBUF_DESC_PAD_TO_SIZE];
} ReadBufDescPadded;

/* Metadata for read buffer */
typedef struct ReadBufMetaData
{
	/*
	 * Indicate the free list used to find empty buffer chunk.
	 * We use compare and swap to push and pop elements.
	 * We use ReadBufferDescLock for concurrency.
	 */
	ReadBufDesc freelist_head;
	int			cnt;
} ReadBufMetaData;

/* Public functions */
extern ReadBufDesc PopReadBufDesc(void);
extern void PushReadBufDesc(ReadBufDesc ring_buf_desc);

extern Size ReadBufShmemSize(void);
extern void ReadBufInit(void);

extern int ReadEbiBufferPrefetch(Relation reln, PrefetchDesc prefetchDesc, 
																char** buffer);

extern PGDLLIMPORT bool enable_prefetch;
#endif /* READBUF_H */
