/*-------------------------------------------------------------------------
 *
 * readbuf.c
 *
 * Read-only Ring Buffer Implementation
 *
 * Copyright (C) 2023 Scalable Computing Systems Lab.
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
 *    src/backend/storage/buffer/readbuf.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef LOCATOR
#include <liburing.h>

#include "postgres.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "postmaster/ebi_tree_process.h"
#include "storage/ebi_sub_buf.h"
#include "storage/readbuf.h"
#include "utils/resowner_private.h"
#include "miscadmin.h"

/* Check both globals.c and miscadmin.h */
extern PGDLLIMPORT int NReadBuffers;

/* GUC parameter */
bool enable_prefetch; /* default: true */

/* Ring buffer descriptors */
ReadBufDescPadded *ReadBufDescriptors;

/* Ring buffer blocks */
char *ReadBufBlocks;

/* Ring buffer metadata */
ReadBufMetaData *ReadBufMeta;

/*
 * TODO: do optimization later
 * 
 * Currently, we employ the free list in the local domain to handle abort 
 * situations. However, moving forward, for the purpose of reduced memory usage, 
 * we must optimize this logic into a more efficient solution.  
 */
ReadBufDesc LocalReadBufDescListHead;

/* Macros used as helper functions */
#define GetReadBufDescriptor(id) (&ReadBufDescriptors[(id)].desc)

#define GetReadBuffer(ring_desc_id, buf_id) \
				(&ReadBufBlocks[((uint64)ring_desc_id) * READBUF_CHUNK_SIZE + \
				((uint64)buf_id) * EBI_SUB_SEG_PAGESZ])

#define PrefetchDescGetNeedEbiPrefetch(prefetchDesc) \
			(((PrefetchDesc) prefetchDesc)->ebi_prefetch_end == false && \
			((PrefetchDesc) prefetchDesc)->io_req_free != NULL)

/* Previate functions */
static void PopulateEbiBuffersFromReadBuffer(Relation reln,
												PrefetchDesc prefetchDesc);
static bool GetEbiBufferFromIoCompletion(PrefetchDesc prefetchDesc, 
								char **readBufferPage, int *ebiBuffer);
static void range_request(struct io_uring* ring, Oid relationId, 
			EbiSubSegmentId segmentId, struct iovec *iovs, IoInfo io_info);


Size
ReadBufShmemSize(void)
{
	Size size = 0;

	/* EBI ring buffer descriptors */
	size =
		add_size(size, mul_size(NReadBuffers, sizeof(ReadBufDescPadded)));

	/* To allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Data pages */
	size = 
		add_size(size, mul_size(NReadBuffers, READBUF_CHUNK_SIZE) +
			EBI_SUB_SEG_PAGESZ);

	/* EBI ring buffer metadata */
	size = add_size(size, sizeof(ReadBufMetaData));

	return size;
}

void
ReadBufInit(void)
{
	bool foundDescs, foundBufs, foundMeta;
	ReadBufDesc buf;
	struct iovec *iov;

	/* Align descriptors to a cacheline boundary */
	ReadBufDescriptors = (ReadBufDescPadded *) ShmemInitStruct(
			"Read Buffer Descriptors",
			NReadBuffers * sizeof(ReadBufDescPadded),
			&foundDescs);

	/* Buffer blocks */
	ReadBufBlocks =
		(char *) ShmemInitStruct("EBI-ring Buffer Blocks",
				NReadBuffers * ((Size) READBUF_CHUNK_SIZE) + EBI_SUB_SEG_PAGESZ,
				&foundBufs);

	/* Align memory address for O_DIRECT */
	ReadBufBlocks += 
		(EBI_SUB_SEG_PAGESZ - (uint64) ReadBufBlocks % (EBI_SUB_SEG_PAGESZ));

	/* Initialize descriptors */
	if (!foundDescs)
	{
		for (int i = 0; i < NReadBuffers; i++)
		{
    		buf = GetReadBufDescriptor(i);
    		buf->buf_id = i;

			/* Initialize i/o vector */
			for (int j = 0; j < MAX_PAGE_IN_CHUNK; ++j)
			{
				iov = &buf->iovs[j];

				iov->iov_base = GetReadBuffer(i, j);
				iov->iov_len = EBI_SUB_SEG_PAGESZ; 
			}

			if (i < (NReadBuffers - 1))
				buf->next = GetReadBufDescriptor(i + 1);
			else
				buf->next = NULL;

			/* Initialize I/O informations */
			memset(buf->infos, 0, MAX_PAGE_IN_CHUNK * sizeof(IoInfoData));
		}
	}

	/* Initialize ring buffer metadata */
	ReadBufMeta = (ReadBufMetaData *) ShmemInitStruct(
			"Read Buffer Metadata", sizeof(ReadBufMetaData), &foundMeta);
	ReadBufMeta->cnt = NReadBuffers;

	if (!foundMeta)
	{
		/* Initialize read buffer description freelist */
		ReadBufMeta->freelist_head = GetReadBufDescriptor(0);
	}
}


/*
 * ReadEbiBufferPrefetch: read the page and prefetch others
 * 
 * We return sub buffer id if that page is in ebi sub buffer. We prefetch required
 * pages on future. 
 */
int
ReadEbiBufferPrefetch(Relation reln, PrefetchDesc prefetchDesc, 
														char** readBufferPage)
{
	int 	ebiSubBuffer;
	int 	completionNum = 0;
	bool	wait = false;
	
recheck_io_request:
	/*
	 * We do I/O request using io_uring.
	 */
	PopulateEbiBuffersFromReadBuffer(reln, prefetchDesc);

	/*
	 * We retrieve completed I/O from io_uring's completion queue.
	 */
	completionNum = RetrieveIoCompletionFromIoUring(prefetchDesc, wait);

	if (!GetEbiBufferFromIoCompletion(prefetchDesc, readBufferPage, 
													&ebiSubBuffer))
	{
		/*
		 * We fail to get next page.
		 * 
		 * When 'wait' is true, we wait to verify the completion of the I/O 
		 * operation. 
		 */
		if (completionNum == 0)
			wait = true;

		goto recheck_io_request;
	}

	/* We get a page succesfully. */
	return ebiSubBuffer;
}

/*
 * PopulateEbiBuffersFromReadBuffer
 *		We load pages into a read buffer chunk utilizing io_uring. If a buffer 
 *		is found in memory, it is accessed directly. However, if it isn't 
 *		available, we initiate an I/O request using io_uring. 		 
 */
static void
PopulateEbiBuffersFromReadBuffer(Relation reln, PrefetchDesc prefetchDesc)
{
	BlockNumber reqPage;
	Oid 		relOid = RelationGetRelid(reln);

	/* If we have element on io_req_free, we need to do prefetch */
	while (PrefetchDescGetNeedEbiPrefetch(prefetchDesc))
	{
		int 			bufId;
		ReadBufDesc		readbufDesc;
		IoInfo 			ioInfo;
		IoInfo 			reqIoInfo;
		EbiSubSegmentId	segId;
		int 			idx; 
		bool			found;
		int				reqIdx = 0;
		uint32			pageIoNum = 0;
		EbiUnobtainedSegment *unobtained_segments =
			((EbiUnobtainedSegment *) prefetchDesc->ebi_unobtained_segments_p);

		/* Get and remove from free list */
		readbufDesc = PopReadBufDescFromFreeList(prefetchDesc);

		/* Set segment id */
		segId = unobtained_segments[prefetchDesc->ebi_seg_index]->seg_id;

		/* Set max page id */
		prefetchDesc->ebi_max_page_id = 
			unobtained_segments[prefetchDesc->ebi_seg_index]->max_page_id + 1;

		/*
		 * Store I/O information on read buffer description
		 */
		for (idx = 0; idx < MAX_PAGE_IN_CHUNK; idx++)
		{
			/* Calculate block id to request async i/o */
			reqPage = prefetchDesc->ebi_next_reqblock;

			if (reqPage == prefetchDesc->ebi_max_page_id)
			{
				/* 
				 * We exhausted all page in a segment. 
				 * Need to move next segment.
				 */
				
				if (pageIoNum > 0)
				{
					reqIdx = idx - pageIoNum;

					Assert(reqIdx >= 0);

					/* Need to request async I/O before we move forward */
					reqIoInfo = &readbufDesc->infos[reqIdx];

					reqIoInfo->is_complete = false;
					reqIoInfo->page_io_num = pageIoNum;
					reqIoInfo->req_page_id = reqPage - pageIoNum;

					/* do range i/o request */
					range_request(prefetchDesc->ring, relOid, segId,
										&readbufDesc->iovs[reqIdx], reqIoInfo);

					prefetchDesc->requested_io_num += 1;
					pageIoNum = 0;
				}

				prefetchDesc->ebi_seg_index += 1;
				reqPage = 0;

				if (prefetchDesc->ebi_seg_index == prefetchDesc->ebi_seg_num)
				{
					/* Stop to prefetch	*/
					prefetchDesc->ebi_prefetch_end = true;
					break;
				}

				/* Change segment id */
				segId = unobtained_segments[prefetchDesc->ebi_seg_index]->seg_id;

				/* Set max page id */
				prefetchDesc->ebi_max_page_id = 
					unobtained_segments[prefetchDesc->ebi_seg_index]->max_page_id + 1;
			}


			/*
			 * lookup the buffer. Let ref cnt increase if the requested block is
			 * in memory. If not exist in ebi sub buffer pool, just return -1.
			 */
			bufId = ConditionalEbiSubBufGetBufRef(segId,
											relOid, reqPage * EBI_SUB_SEG_PAGESZ);

			if (bufId == InvalidEbiSubBuf)
				found = false;
			else
				found = true;

			/* Get current I/O info */
			ioInfo = &readbufDesc->infos[idx];

			/* Set i/o info */
			ioInfo->buf_id = bufId;
			
			if (found == true)
				/* I/O is not required */
				ioInfo->is_complete = true;
			else
				pageIoNum += 1;

			/*
			 * We initiate an I/O request when progressing to the next index 
			 * necessitates such an operation.
			 */
			if ((found || idx + 1 == MAX_PAGE_IN_CHUNK) && pageIoNum > 0)
			{
				if (found == false && idx + 1 == MAX_PAGE_IN_CHUNK)
					reqIdx = idx - pageIoNum + 1;
				else
					reqIdx = idx - pageIoNum;

				Assert(reqIdx >= 0);

				/* Need to request async I/O before we move forward */
				reqIoInfo = &readbufDesc->infos[reqIdx];

				reqIoInfo->is_complete = false;
				reqIoInfo->page_io_num = pageIoNum;

				if (found == false && idx + 1 == MAX_PAGE_IN_CHUNK)
					reqIoInfo->req_page_id = reqPage - pageIoNum + 1;
				else
					reqIoInfo->req_page_id = reqPage - pageIoNum;
			
				/* Do range i/o request */
				range_request(prefetchDesc->ring, relOid, segId,
										&readbufDesc->iovs[reqIdx], reqIoInfo);

				prefetchDesc->requested_io_num += 1;
				pageIoNum = 0;
			}

			/* calculate next request page id */
			prefetchDesc->ebi_next_reqblock = reqPage + 1;
		}

		/* Set read buffer description */
		readbufDesc->page_num = idx;
		readbufDesc->cur_info_idx = 0;
		readbufDesc->cur_page_idx = 0;
		readbufDesc->cur_buf_idx = 0;
		readbufDesc->next = NULL;

		/* Enqueue read buf desc to complete list */
		EnqueueReadBufDescToCompleteList(prefetchDesc, readbufDesc);
	}
}

/*
 * GetEbiBufferFromIoCompletion
 * 		We take buffer from Ebi buffer or read buffer. We distinguish 
 *		it from mark of 'IoInfo'.
 */
static bool
GetEbiBufferFromIoCompletion(PrefetchDesc prefetchDesc, char **readBufferPage, 
													int *ebiBuffer)
{
	ReadBufDesc 	readbufDesc;
	IoInfo			ioInfo;

	/* There is no completion. */
	if (prefetchDesc->io_req_complete_head == NULL)
		return false;

	Assert(prefetchDesc->io_req_complete_tail != NULL);

	/* Get read buffer descriptort from I/O complete list. */
	readbufDesc = prefetchDesc->io_req_complete_head;

	/* Get current processing point from read bufer descriptor. */
	ioInfo = &readbufDesc->infos[readbufDesc->cur_info_idx];

	/*
	 * If the requested page is not yet ready, we should perform a recheck of 
	 * the I/O request. In here, we just return false to recheck.
	 */
	if (ioInfo->is_complete == false)
		return false;

	if (ioInfo->buf_id != InvalidEbiSubBuf)
	{
		/*
		 * We get next page from ebi buffer pool. 
		 */

		/* Move to next info */
		readbufDesc->cur_info_idx += 1;

		/* Move to next buffer */
		readbufDesc->cur_buf_idx += 1;
			
		*readBufferPage = EbiSubGetPage(ioInfo->buf_id);
			
		*ebiBuffer = ioInfo->buf_id;
	}
	else
	{
		/*
		 * We get next page from read buffer pool. 
		 */

		int retBufIdx = readbufDesc->cur_buf_idx;

		/* Move to next buffer */
		readbufDesc->cur_buf_idx += 1;
		readbufDesc->cur_page_idx += 1;

		if (readbufDesc->cur_page_idx == ioInfo->page_io_num)
		{
			/* Move to next info */
			readbufDesc->cur_info_idx += ioInfo->page_io_num;
			readbufDesc->cur_page_idx = 0;
		}

		if (readbufDesc->cur_buf_idx == MAX_PAGE_IN_CHUNK || 
				readbufDesc->cur_buf_idx == readbufDesc->page_num)
		{
			/* Move to next info */
			readbufDesc->cur_info_idx += ioInfo->page_io_num;
		}
			
		*readBufferPage = GetReadBuffer(readbufDesc->buf_id, retBufIdx);
		
		*ebiBuffer = InvalidEbiSubBuf;
	}

	/*
	 * We continuously recycle the ReadBufDesc. To denote that it has been fully 
	 * used, we simply return the ReadBufDesc to the PrefetchDesc's free list for 
	 * future utilization.	 
	 */
	if (readbufDesc->cur_buf_idx == MAX_PAGE_IN_CHUNK || 
			readbufDesc->cur_buf_idx == readbufDesc->page_num)
	{
		/* Remove from complete list */
		readbufDesc = DequeueReadBufDescFromCompleteList(prefetchDesc);

		/* Append to free list */
		PushReadBufDescToFreeList(prefetchDesc, readbufDesc);
	}

	return true;
}

/*
 * PopReadBufDesc
 */
ReadBufDesc
PopReadBufDesc(void)
{
	ReadBufDesc readBufDesc;

	/* TODO: let it use cas */
	LWLockAcquire(ReadBufferDescLock, LW_EXCLUSIVE);

	readBufDesc = ReadBufMeta->freelist_head;

	if (readBufDesc == NULL)
	{
		LWLockRelease(ReadBufferDescLock);
		return NULL;
	}

	ReadBufMeta->freelist_head = readBufDesc->next;
	(ReadBufMeta->cnt)--;

	LWLockRelease(ReadBufferDescLock);

	/* enlarge resource owner array and add read buffer desc to that array */
	ResourceOwnerEnlargeReadBufferDescs(CurrentResourceOwner);
	ResourceOwnerRememberReadBufferDesc(CurrentResourceOwner, readBufDesc);

	return readBufDesc;
}

/*
 * PushReadBufDesc
 */
void
PushReadBufDesc(ReadBufDesc readBufDesc) 
{
	ReadBufDesc head;

	/* TODO: let it use cas */
	LWLockAcquire(ReadBufferDescLock, LW_EXCLUSIVE);

	head = ReadBufMeta->freelist_head;

	readBufDesc->next = head;

	ReadBufMeta->freelist_head = readBufDesc;
	(ReadBufMeta->cnt)++;

	LWLockRelease(ReadBufferDescLock);

	/* Forget read buffer desc from resource owner array */
	ResourceOwnerForgetReadBufferDesc(CurrentResourceOwner, readBufDesc);
}

/*
 *	range_request() -- Request the specified blocks from a relation using async i/o.
 */
static void
range_request(struct io_uring* ring, 
		Oid relationId, EbiSubSegmentId segmentId,
		struct iovec *iovs, IoInfo io_info)
{
	struct io_uring_sqe *sqe;
	uint64				seekpos;
	int    				fd;

	/* get submission queue for io_uring */
	sqe = io_uring_get_sqe(ring);

	/* open segment file */
	fd = EbiSubOpenSegmentFile(segmentId, relationId);

	/* offset in file */
	seekpos = (uint64) EBI_SUB_SEG_PAGESZ * (io_info->req_page_id);

	/* set read request */
	io_uring_prep_readv(sqe, fd, iovs, io_info->page_io_num, seekpos);

	/* set user data */
	io_uring_sqe_set_data(sqe, (void *) io_info);

	/* submit request */
	if (io_uring_submit(ring) < 0)
		elog(ERROR, "io_uring_submit error");

	/* close segment file */
	EbiSubCloseSegmentFile(fd);
}

#endif /* LOCATOR */
