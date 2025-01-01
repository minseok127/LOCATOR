/*-------------------------------------------------------------------------
 *
 * buf.h
 *	  Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUF_H
#define BUF_H

#ifdef DIVA
#include "storage/block.h"
#ifdef LOCATOR
#include <liburing.h>
#include "nodes/pg_list.h"
#include "locator/locator.h"
#endif /* LOCATOR */
#endif /* DIVA */

/*
 * Buffer identifiers.
 *
 * Zero is invalid, positive is the index of a shared buffer (1..NBuffers),
 * negative is the index of a local buffer (-1 .. -NLocBuffer).
 */
typedef int Buffer;

#define InvalidBuffer	0

/*
 * BufferIsInvalid
 *		True iff the buffer is invalid.
 */
#define BufferIsInvalid(buffer) ((buffer) == InvalidBuffer)

/*
 * BufferIsLocal
 *		True iff the buffer is local (not visible to other backends).
 */
#define BufferIsLocal(buffer)	((buffer) < 0)

#ifdef LOCATOR
/* Invalid traditional buffer id. */
#define InvalidTraditionalBufferID (-1)

/* We divide into chunk to address I/O */
#define READBUF_CHUNK_SIZE ((Size) BLCKSZ * 256)

/* maximum number of page inside one chunk */
#define MAX_PAGE_IN_CHUNK ((Size) READBUF_CHUNK_SIZE / BLCKSZ)

StaticAssertDecl((READBUF_CHUNK_SIZE % BLCKSZ) == 0, 
						"read buffer chunk size is invalid");

typedef struct IoInfoData
{
	/* ebi sub buffer id, -1 means it's not in buffer*/
	//int			ebi_sub_buf_id;	
	int			buf_id;	
	int			req_page_id;			/* request page id */
	uint32		page_io_num;			/* # of page we request async i/o */
	bool		is_complete; 			/* can we use i/o info now? */
} IoInfoData;

typedef struct IoInfoData *IoInfo;


typedef struct ReadBufDescData
{
	int			buf_id; 				/* unique buffer id in the range 0 to (n - 1) */
	int 		cur_info_idx;			/* cur info idx used for infos array */
	int 		cur_page_idx;			/* cur page idx used for an info */
	int 		cur_buf_idx;			/* cur buffer idx used for io vector */
	size_t		page_num;				/* # of page we can read */
	struct iovec	iovs[MAX_PAGE_IN_CHUNK];
	IoInfoData	infos[MAX_PAGE_IN_CHUNK];

	/*
	 * We use ring buffer description as linked list to recycle.
	 */
	struct ReadBufDescData *next;
} ReadBufDescData;

typedef struct ReadBufDescData *ReadBufDesc; 

typedef struct PrefetchDescData
{
	struct io_uring	*ring;						/* io_uring's ring */

	BlockNumber		next_reqblock;				/* next request block */
	bool 			prefetch_end;				/* check prefetch is over */
	BlockNumber		requested_io_num;			/* # of requested i/o */
	BlockNumber		complete_io_num;			/* # of complete i/o */

	/* ebi prefetch state */
	int 			ebi_next_reqblock;			/* next ebi request block */
	int 			ebi_max_page_id;			/* ebi node's max page id */
	int 			ebi_seg_index;				/* next ebi segment index */
	int 			ebi_seg_num;				/* the # of ebi node segment */
	bool 			ebi_prefetch_end;			/* check prefetch is over */
	/* ebi scan state, it is the same as HeapScanDesc->unobtained_segments's pointer */
	void 			**ebi_unobtained_segments_p;

	/* prefetch desc freelist */
	ReadBufDesc		io_req_free;			/* we use this free list for prefetching */

	ReadBufDesc		io_req_complete_head;	/* completed list head that we can process */
	ReadBufDesc		io_req_complete_tail;	/* completed list tail that we can process */
} PrefetchDescData;

typedef struct PrefetchDescData *PrefetchDesc;

typedef struct PrefetchHeapDescData
{
	struct PrefetchDescData base;			/* default prefetch descriptor */

	/* copied heap scan state */
	BlockNumber rs_nblocks;					/* # of blocks */
	BlockNumber start_block;				/* scan start position */

	/* heappage prefetch state */
	bool 			is_startblock_fetched;	/* check whether first block is fetched or not */
	size_t			prefetch_size;			/* prefetch size */
} PrefetchHeapDescData;

typedef struct PrefetchHeapDescData *PrefetchHeapDesc;

typedef struct LocatorPartInfoData
{
	LocatorPartNumber	partNum;
	LocatorPartGenNo	partGen;
	BlockNumber			block_cnt;
	LocatorSeqNum		last_recCnt;
	LocatorSeqNum		recCnt;
} LocatorPartInfoData;

typedef struct LocatorPartInfoData *LocatorPartInfo;

typedef struct PrefetchPartitionDescData
{
	struct PrefetchDescData base;			/* default prefetch descriptor */
	LocatorPartLevel	level_cnt;

	/* prefetch state */
	int					c_fd;
	LocatorPartLevel	c_level;
	ListCell		   *c_lc;
	BlockNumber			c_block_cnt;

	/* array of lists */
	List **part_infos_to_prefetch;			/* partition infos of each level */

	BlockNumber			locator_last_blocknum;
	BlockNumber			locator_current_block_idx_in_record_zone;
	int					locator_current_record_zone_id;
	void			   *locator_level_desc;	/* LocatorExecutorLevelDesc */
	void			   *locator_group_desc; /* LocatorExecutorColumnGroupDesc */
	uint32				locator_columnar_start_level;
	bool				is_columnar_layout;
} PrefetchPartitionDescData;

typedef struct PrefetchPartitionDescData *PrefetchPartitionDesc;

static inline void
EnqueueReadBufDescToCompleteList(PrefetchDesc prefetchDesc, ReadBufDesc readbufDesc)
{
	/* append to complete list tail */
	if (likely(prefetchDesc->io_req_complete_head != NULL))
	{
		ReadBufDesc old_tail;

		/* There is an element inside complete list */
		Assert(prefetchDesc->io_req_complete_tail != NULL);

		old_tail = prefetchDesc->io_req_complete_tail;

		old_tail->next = readbufDesc;
		prefetchDesc->io_req_complete_tail = readbufDesc;
	}
	else
	{
		/* There is no element inside complete list */
		Assert(prefetchDesc->io_req_complete_tail == NULL);

		prefetchDesc->io_req_complete_head = readbufDesc;
		prefetchDesc->io_req_complete_tail = readbufDesc;
	}
}

static inline ReadBufDesc
DequeueReadBufDescFromCompleteList(PrefetchDesc prefetchDesc)
{
	ReadBufDesc readbufDesc = prefetchDesc->io_req_complete_head;

	if (readbufDesc)
	{
		/* remove from complete list */
		if (prefetchDesc->io_req_complete_head == prefetchDesc->io_req_complete_tail)
		{
			/* there is one */
			prefetchDesc->io_req_complete_head = NULL;
			prefetchDesc->io_req_complete_tail = NULL;
		}
		else
		{
			/* there is two or more*/
			prefetchDesc->io_req_complete_head = readbufDesc->next;
		}
	}

	return readbufDesc;
}

static inline void
PushReadBufDescToFreeList(PrefetchDesc prefetchDesc, ReadBufDesc readbufDesc)
{
	/* append to free list */
	readbufDesc->next = prefetchDesc->io_req_free;
	prefetchDesc->io_req_free = readbufDesc;
}

static inline ReadBufDesc
PopReadBufDescFromFreeList(PrefetchDesc prefetchDesc)
{
	ReadBufDesc readbufDesc;

	/* get and remove from free list */
	readbufDesc = prefetchDesc->io_req_free;
	prefetchDesc->io_req_free = readbufDesc->next;
	readbufDesc->next = NULL;	

	return readbufDesc;
}
#endif /* LOCATOR */

/*
 * Buffer access strategy objects.
 *
 * BufferAccessStrategyData is private to freelist.c
 */
typedef struct BufferAccessStrategyData *BufferAccessStrategy;

#endif							/* BUF_H */
