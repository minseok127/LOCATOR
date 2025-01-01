/*-------------------------------------------------------------------------
 *
 * ebi_sub_buf.c
 *
 * EBI Sub Buffer Implementation
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
 *    src/backend/storage/ebi_sub/ebi_sub_buf.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef DIVA
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "miscadmin.h"

#include "postmaster/ebi_tree_process.h"
#include "storage/ebi_sub_buf.h"
#include "storage/ebi_sub_hash.h"
#include "utils/dynahash.h"
#include "utils/resowner_private.h"

/* Check both globals.c and miscadmin.h */
extern PGDLLIMPORT int NEbiSubBuffers;

EbiSubBufDescPadded *EbiSubBufDescriptors;
char *EbiSubBufBlocks;
EbiSubBufMeta *EbiSubBuf;

#define EBI_SUB_SEG_OFFSET_TO_PAGE_ID(off) (uint32)((off) / (EBI_SUB_SEG_PAGESZ))

/* Private functions */

/* Segment control */
static void EbiSubReadSegmentPage(const EbiSubBufTag *tag, int buf_id);
static void EbiSubWriteSegmentPage(const EbiSubBufTag *tag, int buf_id);

/* Pin count control */
static void PinEbiSubBufferInternal(EbiSubBufDesc *buf);
static void UnpinEbiSubBufferInternal(EbiSubBufDesc *buf);

Size
EbiSubBufShmemSize(void)
{
	Size size = 0;

	/* EBI sub buffer descriptors */
	size =
		add_size(size, mul_size(NEbiSubBuffers, sizeof(EbiSubBufDescPadded)));

	/* To allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Data pages */
	size = 
		add_size(size, mul_size(NEbiSubBuffers, EBI_SUB_SEG_PAGESZ) +
			EBI_SUB_SEG_PAGESZ);

	/* EBI sub buffer hash */
	size = add_size(size,
			EbiSubHashShmemSize(NEbiSubBuffers + NUM_EBI_SUB_PARTITIONS));

	/* EBI sub buffer metadata */
	size = add_size(size, sizeof(EbiSubBufMeta));

	return size;
}

void
EbiSubBufInit(void)
{
	bool foundDescs, foundBufs, foundMeta;
	EbiSubBufDesc *buf;

	/* Align descriptors to a cacheline boundary */
	EbiSubBufDescriptors = (EbiSubBufDescPadded *)ShmemInitStruct(
			"EBI-sub Buffer Descriptors",
			NEbiSubBuffers * sizeof(EbiSubBufDescPadded),
			&foundDescs);

	/* Buffer blocks */
	EbiSubBufBlocks =
		(char *)ShmemInitStruct("EBI-sub Buffer Blocks",
				NEbiSubBuffers * ((Size)(EBI_SUB_SEG_PAGESZ)) + EBI_SUB_SEG_PAGESZ,
				&foundBufs);

	/* Align memory address for O_DIRECT */
	EbiSubBufBlocks += 
		(EBI_SUB_SEG_PAGESZ - (uint64) EbiSubBufBlocks % (EBI_SUB_SEG_PAGESZ));

	EbiSubHashInit(NEbiSubBuffers + NUM_EBI_SUB_PARTITIONS);

	/* Initialize descriptors */
	if (!foundDescs)
	{
		for (int i = 0; i < NEbiSubBuffers; i++)
		{
			buf = GetEbiSubBufDescriptor(i);

			buf->tag.seg_id = 0;
			buf->tag.rel_id = 0;
			buf->tag.page_id = 0;
			buf->is_dirty = false;
			pg_atomic_init_u32(&buf->refcnt, 0);
		}
	}

	/* Initialize metadata */
	EbiSubBuf = (EbiSubBufMeta *)ShmemInitStruct(
			"EBI-sub Buffer Metadata", sizeof(EbiSubBufMeta), &foundMeta);
}

void
EbiSubAppendVersion(EbiSubSegmentId seg_id,
					Oid rel_id,
					EbiSubSegmentOffset seg_offset,
					uint32 tuple_size,
					const void *tuple,
					LWLock *rwlock)
{
	uint64_t buf_id;
	EbiSubBufDesc *buf;
	uint64_t page_offset;
	uint64_t dst_offset;

	buf_id = EbiSubBufGetBufRef(seg_id, rel_id, seg_offset, false);
	buf = GetEbiSubBufDescriptor(buf_id);

	page_offset = seg_offset % EBI_SUB_SEG_PAGESZ;
	dst_offset = buf_id * EBI_SUB_SEG_PAGESZ + page_offset;

	/* Pitfall!: We should consider overflow error to calculate offset. */

	/* Copy the tuple into the cache */
	/*
	 * We have to write real tuple data first and tuple length later to handle
	 * conflict between updater and scanner.
	 * The scanner knows whether this version is written successfully or not
	 * by reading tuple length. Updater copy the real tuple data first and
	 * tuple length later, so if the scanner read the tuple length with
	 * seg_offset and the tuple length is 0, it means that tuple is being
	 * written. (memcpy() is thread-safe)
	 * http://www.ssaurel.com/manpages/htmlman3/memcpy.3.html
	 */
	memcpy(&EbiSubBufBlocks[dst_offset + EBI_SUB_VERSION_METADATA], tuple, tuple_size);
	memcpy(&EbiSubBufBlocks[dst_offset], &tuple_size, EBI_SUB_VERSION_METADATA);

	Assert(page_offset + (1 << my_log2(tuple_size + EBI_SUB_VERSION_METADATA)) <=
			EBI_SUB_SEG_PAGESZ);

	/*
	 * Mark it as dirty so that it could be flushed when evicted.
	 */
	buf->is_dirty = true;

	pg_memory_barrier();

	/* Whether or not the page has been full, we should unref the page */
	UnpinEbiSubBuffer(buf_id);
}

int
EbiSubReadVersionRef(EbiSubSegmentId seg_id,
					 Oid rel_id,
					 EbiSubSegmentOffset seg_offset,
					 uint32* tuple_size,
					 void **ret_value,
					 bool fast_memory_lookup)
{
	int buf_id;
	int page_offset;

	buf_id = EbiSubBufGetBufRef(seg_id, rel_id, seg_offset, fast_memory_lookup);

	/* buf_id can be -2 only if fast_memory_lookup is true */
	if (buf_id == -2)
		return -2;

	page_offset = seg_offset % EBI_SUB_SEG_PAGESZ;

  /*
   * Version structure inside ebi subnode file.
   *
   * -----------------------------------------------------
   * |  metadata (4 byte)  | tuple data (variable size)  |
   * -----------------------------------------------------
   *
   * Currently, metadata field is used for representing tuple size.
   */

	/* Set ret_value ans tuple_size to the pointer of the tuple in the cache */
	*ret_value = &EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ + 
                                  page_offset + EBI_SUB_VERSION_METADATA];
	*tuple_size = *((uint32*)&EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ +
											  page_offset]);

	/* The caller must unpin the buffer entry for buf_id */
	return buf_id;
}

/*
 * EbiSubBufGetBufRef
 *
 * Increase refcount of the requested segment page, and returns the cache_id.
 * If the page is not cached, read it from the segment file. If cache is full,
 * evict one page following the eviction policy (currently round-robin..)
 * Caller must decrease refcount after using it. If caller makes the page full
 * by appending more tuple, it has to decrease one more count for unpinning it.
 */
int
EbiSubBufGetBufRef(EbiSubSegmentId seg_id,
				   Oid rel_id,
				   EbiSubSegmentOffset seg_offset,
				   bool fast_memory_lookup)
{
	EbiSubBufTag tag;          /* identity of requested block */
	int buf_id;                 /* buf index of target segment page */
	int candidate_id;           /* buf index of victim segment page */
	LWLock *new_partition_lock; /* buf partition lock for it */
	LWLock *old_partition_lock; /* buf partition lock for it */
	uint32 hashcode;
	uint32 hashcode_vict;
	EbiSubBufDesc *buf;
	int ret;
	EbiSubBufTag victim_tag;
	uint32 my_slot;
	uint32 usagecnt;

	tag.seg_id = seg_id;
	tag.rel_id = rel_id;
	tag.page_id = EBI_SUB_SEG_OFFSET_TO_PAGE_ID(seg_offset);

	/* Get hash code for the segment id & page id */
	hashcode = EbiSubHashCode(&tag);
	new_partition_lock = EbiSubMappingPartitionLock(hashcode);

	LWLockAcquire(new_partition_lock, LW_SHARED);
	buf_id = EbiSubHashLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		/* Target page is already in cache */

		/* Increase refcount by 1, so this page couldn't be evicted */
		PinEbiSubBuffer(buf_id);
		LWLockRelease(new_partition_lock);

		return buf_id;
	}
	LWLockRelease(new_partition_lock);

	/* 
	 * If this is fast_memory_lookup and the ebi page is not in memory, don't do
	 * I/O, just return -2.
	 */
	if (fast_memory_lookup)
		return -2;

	LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
	buf_id = EbiSubHashLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		/* Target page is already in cache */

		/* Increase refcount by 1, so this page couldn't be evicted */
		PinEbiSubBuffer(buf_id);
		LWLockRelease(new_partition_lock);

		return buf_id;
	}

find_cand:
	/* Pick up a candidate cache entry for a new allocation */
	candidate_id = pg_atomic_fetch_add_u64(&EbiSubBuf->eviction_rr_idx, 1) %
		NEbiSubBuffers;
	buf = GetEbiSubBufDescriptor(candidate_id);
	if (pg_atomic_read_u32(&buf->refcnt) != 0)
	{
		/* Someone is accessing this entry, find another candidate */
		goto find_cand;
	}

	if ((usagecnt = EBI_BUF_DESC_GET_USAGECOUNT(buf)) != 0 && 
										usagecnt < EBI_BUF_OVERFLOW_USAGECOUNT)
	{
		/* This buffer is not a suitable block for eviction. */
		pg_atomic_fetch_sub_u32(&buf->usagecnt, EBI_BUF_USAGECOUNT_ONE);

		goto find_cand;
	}

	victim_tag = buf->tag;

	/*
	 * It seems that this entry is unused now. But we need to check it
	 * again after holding the partition lock, because another transaction
	 * might trying to access and increase this refcount just right now.
	 */
	if (victim_tag.seg_id > 0)
	{
		/*
		 * This entry is using now so that we need to remove vcache_hash
		 * entry for it. We also need to flush it if the cache entry is dirty.
		 */
		hashcode_vict = EbiSubHashCode(&victim_tag);
		old_partition_lock = EbiSubMappingPartitionLock(hashcode_vict);
		if (LWLockHeldByMe(old_partition_lock))
		{
			/* Partition lock collision occured by myself */
			/*
			 * TODO: Actually, the transaction can use this entry as a victim
			 * by marking lock collision instead of acquiring nested lock.
			 * It will perform better, but now I just simply find another.
			 */
			goto find_cand;
		}

		if (!LWLockConditionalAcquire(old_partition_lock, LW_EXCLUSIVE))
		{
			/* Partition lock is already held by someone. */
			goto find_cand;
		}

		/* Try to hold refcount for the eviction */
		ret = pg_atomic_fetch_add_u32(&buf->refcnt, 1);
		if (ret > 0)
		{
			/*
			 * Race occured. Another read transaction might get this page,
			 * or possibly another evicting tranasaction might get this page
			 * if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&buf->refcnt, 1);
			LWLockRelease(old_partition_lock);
			goto find_cand;
		}

		if (buf->tag.seg_id != victim_tag.seg_id ||
			buf->tag.rel_id != victim_tag.rel_id ||
			buf->tag.page_id != victim_tag.page_id)
		{
			/*
			 * This exception might very rare, but the possible scenario is,
			 * 1. txn A processed up to just before holding the
			 *    old_partition_lock
			 * 2. round robin cycle is too short, so txn B acquired the
			 *    old_partition_lock, and evicted this page, and mapped it
			 *    to another vcache_hash entry
			 * 3. Txn B unreffed this page after using it so that refcount
			 *    becomes 0, but seg_id and(or) page_id of this entry have
			 *    changed
			 * In this case, just find another victim for simplicity now.
			 */
			pg_atomic_fetch_sub_u32(&buf->refcnt, 1);
			LWLockRelease(old_partition_lock);
			goto find_cand;
		}

		/*
		 * Now we are ready to use this entry as a new cache.
		 * First, check whether this victim should be flushed to segment file.
		 * Appending page shouldn't be picked as a victim because of the
		 * refcount.
		 */
		if (buf->is_dirty)
		{
			Assert(buf->tag.seg_id != 0);

			/* Check if the page related segment file has been removed */
			my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
			pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
			pg_memory_barrier();

			if (EbiSegIsAlive(EbiTreeShmem->ebitree, buf->tag.seg_id))
			{
				EbiSubWriteSegmentPage(&buf->tag, candidate_id);
			}

			pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

			/*
			 * We do not zero the page so that the page could be overwritten
			 * with a new tuple as a new segment page.
			 */
			buf->is_dirty = false;
		}
		/*
		 * Now we can safely evict this entry.
		 * Remove corresponding hash entry for it so that we can release
		 * the partition lock.
		 */
		EbiSubHashDelete(&buf->tag, hashcode_vict);
		LWLockRelease(old_partition_lock);
	}
	else
	{
		/*
		 * This cache entry is unused. Just increase the refcount and use it.
		 */
		ret = pg_atomic_fetch_add_u32(&buf->refcnt, 1);
		if (ret > 0 || buf->tag.seg_id != 0)
		{
			/*
			 * Race occured. Possibly another evicting tranasaction might get
			 * this page if round robin cycle is too short.
			 */
			pg_atomic_fetch_sub_u32(&buf->refcnt, 1);
			goto find_cand;
		} 
	}

	/* Initialize the descriptor for a new cache */
	buf->tag = tag;
	pg_atomic_init_u32(&buf->usagecnt, EBI_BUF_USAGECOUNT_ONE);

	/* Read target segment page into the cache */
	EbiSubReadSegmentPage(&buf->tag, candidate_id);

	/* Next, insert new hash entry for it */
	ret = EbiSubHashInsert(&tag, hashcode, candidate_id);
	Assert(ret == -1);

	LWLockRelease(new_partition_lock);

	ResourceOwnerEnlargeEbiBuffers(CurrentResourceOwner);
	ResourceOwnerRememberEbiBuffer(CurrentResourceOwner, candidate_id);

	/* Return the index of cache entry, holding refcount 1 */
	return candidate_id;
}


/* Segment open & close */

/*
 * EbiSubCreateSegmentFile
 *
 * Make a new file for corresponding seg_id
 */
void
EbiSubCreateSegmentFile(EbiSubSegmentId seg_id, Oid rel_id)
{
	int fd;
	char filename[32];
	int flags = enableFsync ? O_SYNC : 0;

	snprintf(filename, 32, "ebitree.%09d.%05d", seg_id, rel_id);
	fd = open(filename, O_RDWR | O_CREAT | flags, (mode_t)0600);

	if (fd < 0)
	{
		fprintf(stderr, "ebi sub segment file create error: %s\n", strerror(errno));
		elog(ERROR, "ebi sub segment file create error");
	}

	close(fd);
}

/*
 * EbiSubRemoveSegmentFile
 *
 * Remove file for corresponding seg_id
 */
void
EbiSubRemoveSegmentFile(EbiSubSegmentId seg_id, Oid rel_id)
{
	char filename[32];

	snprintf(filename, 32, "ebitree.%09d.%05d", seg_id, rel_id);

	if (remove(filename) != 0)
	{
		int err = errno;

		if (err != ENOENT)
		{
			fprintf(stderr, "EbiSubRemoveSegmentFile error: %s\n", strerror(err));
			elog(ERROR, "EbiSubRemoveSegmentFile error");
		}
	}
}

#ifdef LOCATOR
/*
 * EbiSubRemoveOldPartitionFile
 *
 * Remove old partition file
 */
void
EbiSubRemoveOldPartitionFile(Oid rel_id, OldPartitions old_partitions)
{
	char filename[32];

	for (LocatorPartGenNo part_gen = old_partitions.min_part_gen;
		 part_gen <= old_partitions.max_part_gen; ++part_gen)
	{
		snprintf(filename, 32, "base/%u/%u.%u.%u.%u",
				 old_partitions.db_id, rel_id, 
				 old_partitions.part_lvl, old_partitions.part_num, part_gen);

		if (unlink(filename) != 0)
		{
			int err = errno;

			if (err != ENOENT)
			{
				fprintf(stderr, "EbiSubRemoveOldPartitionFile unlink error: %s\n", strerror(err));
				elog(ERROR, "EbiSubRemoveOldPartitionFile unlink error");
			}
		}

#ifdef LOCATOR_DEBUG
		fprintf(stderr, "[LOCATOR] GC old partition file, name: %s\n", filename);
#endif /* LOCATOR_DEBUG */
	}
}
#endif /* LOCATOR */

/*
 * EbiSubOpenSegmentFile
 *
 * Open Segment file.
 * Caller have to call EbiSubCloseSegmentFile(seg_id) after file io is done.
 */
int
EbiSubOpenSegmentFile(EbiSubSegmentId seg_id, Oid rel_id)
{
	int fd;
	char filename[32];
	int flags = enableFsync ? O_SYNC : 0;

	Assert(seg_id != 0);

	snprintf(filename, 32, "ebitree.%09d.%05d", seg_id, rel_id);

	fd = open(filename, O_RDWR | flags, (mode_t) 0600);

	Assert(fd >= 0);

	return fd;
}

/*
 * EbiSubCloseSegmentFile
 *
 * Close Segment file.
 */
void
EbiSubCloseSegmentFile(int fd)
{
	Assert(fd >= 0);

	if(close(fd) == -1)
		fprintf(stderr, "close error, %s\n", strerror(errno));
}

static void
EbiSubReadSegmentPage(const EbiSubBufTag *tag, int buf_id)
{
	ssize_t read;
	int fd;

	fd = EbiSubOpenSegmentFile(tag->seg_id, tag->rel_id);

	Assert(fd >= 0);

read_retry:
	read = pg_pread(fd,
			&EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ],
			EBI_SUB_SEG_PAGESZ,
			tag->page_id * EBI_SUB_SEG_PAGESZ);

	if (read < 0 && errno == EINTR)
		goto read_retry;

	/* New page */
	if (read == 0)
	{
		memset(&EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ],
				0,
				EBI_SUB_SEG_PAGESZ);
	}

	EbiSubCloseSegmentFile(fd);
}

static void
EbiSubWriteSegmentPage(const EbiSubBufTag *tag, int buf_id)
{
	int fd;
	ssize_t written;

	fd = EbiSubOpenSegmentFile(tag->seg_id, tag->rel_id);

write_retry:
	written = pg_pwrite(fd,
			&EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ],
			EBI_SUB_SEG_PAGESZ,
			tag->page_id * EBI_SUB_SEG_PAGESZ);

	if (written < 0 && errno == EINTR)
		goto write_retry;

	Assert(written == EBI_SUB_SEG_PAGESZ);

	EbiSubCloseSegmentFile(fd);
}

bool
EbiSubBufIsValid(int buf_id)
{
	return buf_id < NEbiSubBuffers;
}

/*
 * PinEbiSubBuffer
 *
 * A public version of PinEbiSubBufferInternal
 */
void
PinEbiSubBuffer(int buf_id)
{
	EbiSubBufDesc *buf;
	if (!EbiSubBufIsValid(buf_id))
	{
		elog(ERROR, "Ebi Sub Buffer id is not valid");
		return;
	}
	ResourceOwnerEnlargeEbiBuffers(CurrentResourceOwner);

	buf = GetEbiSubBufDescriptor(buf_id);
	PinEbiSubBufferInternal(buf);

	/* We increment usage counter for clock algorithm */
	if (EBI_BUF_DESC_GET_USAGECOUNT(buf) < EBI_BUF_MAX_USAGECOUNT)
		pg_atomic_fetch_add_u32(&buf->usagecnt, EBI_BUF_USAGECOUNT_ONE);

	/* Remember ebi buffer within resource owner */
	ResourceOwnerRememberEbiBuffer(CurrentResourceOwner, buf_id);
}

/*
 * PinEbiSubBufferInternal
 *
 * Increment the refcount of the given buf entry
 */
static void
PinEbiSubBufferInternal(EbiSubBufDesc *buf)
{
	pg_atomic_fetch_add_u32(&buf->refcnt, 1);
}

/*
 * UnpinEbiSubBuffer
 *
 * A public version of UnpinEbiSubBufferInternal
 */
void
UnpinEbiSubBuffer(int buf_id)
{
	EbiSubBufDesc *buf;
	if (!EbiSubBufIsValid(buf_id))
	{
		elog(ERROR, "Ebi Sub Buffer id is not valid");
		return;
	}
	buf = GetEbiSubBufDescriptor(buf_id);
	UnpinEbiSubBufferInternal(buf);

	/* Forget ebi buffer within resource owner */
	ResourceOwnerForgetEbiBuffer(CurrentResourceOwner, buf_id);
}

/*
 * UnpinEbiSubBufferInternal
 *
 * Decrement the refcount of the given buf entry
 */
static void
UnpinEbiSubBufferInternal(EbiSubBufDesc *buf)
{
	if (pg_atomic_read_u32(&buf->refcnt) == 0)
		elog(ERROR, "ebi sub buffer refcnt is zero");

	pg_atomic_fetch_sub_u32(&buf->refcnt, 1);
}

char *
EbiSubGetPage(int buf_id)
{
	return &EbiSubBufBlocks[buf_id * EBI_SUB_SEG_PAGESZ];
}

#ifdef LOCATOR
/*
 * EbiReleasePrefetchedBuffers
 *		Release all prefetched buffers that have remained untouched.
 *
 * While scanning ReadBufDesc within PrefetchDesc, we verify whether pages 
 * originate from the ebi buffer pool. If they are sourced from the 
 * traditional buffer pool and have not been released, we proceed to release 
 * all of them.
 */
void
EbiReleasePrefetchedBuffers(PrefetchDesc prefetchDesc)
{
	ReadBufDesc readBufDesc;
	IoInfo		ioInfo;
	int 		infoIdx;

	readBufDesc = prefetchDesc->io_req_complete_head;

	/* Release prefetched buffers, scanning ReadBufDescs */
	while (readBufDesc != NULL)
	{
		Assert(readBufDesc->page_num <= MAX_PAGE_IN_CHUNK);

		infoIdx = readBufDesc->cur_info_idx;

		/* Traverse through ReadBufDesc to locate the page intended for unpinning. */
		while (infoIdx < readBufDesc->page_num)
		{
			ioInfo = &readBufDesc->infos[infoIdx]; 
	
			if (ioInfo->buf_id != -1)
			{
				/* 
				 * It is from traditional buffer pool. 
				 */

				/* Unpin a EBI sub page */
				UnpinEbiSubBuffer(ioInfo->buf_id);
				infoIdx += 1;
			}
			else
			{
				/* It is from read buffer pool */
				infoIdx += ioInfo->page_io_num;
			}
		}

		/* Move to next ReadBufDesc */
		readBufDesc = readBufDesc->next;
	}
}
#endif /* LOCATOR */

/*
 * ConditionalEbiSubBufGetBufRef
 *
 * Increase refcount of the requested segment page, and returns the cache_id.
 * If the page is not cached, just skip.
 * Caller must decrease refcount after using it. If caller makes the page full
 * by appending more tuple, it has to decrease one more count for unpinning it.
 */
int
ConditionalEbiSubBufGetBufRef(EbiSubSegmentId seg_id,
				   Oid rel_id,
				   EbiSubSegmentOffset seg_offset)
{
	EbiSubBufTag tag;          /* identity of requested block */
	int buf_id;                 /* buf index of target segment page */
	LWLock *new_partition_lock; /* buf partition lock for it */
	uint32 hashcode;

	tag.seg_id = seg_id;
	tag.rel_id = rel_id;
	tag.page_id = EBI_SUB_SEG_OFFSET_TO_PAGE_ID(seg_offset);

	/* Get hash code for the segment id & page id */
	hashcode = EbiSubHashCode(&tag);
	new_partition_lock = EbiSubMappingPartitionLock(hashcode);

	LWLockAcquire(new_partition_lock, LW_SHARED);
	buf_id = EbiSubHashLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		/* Target page is already in cache */

		/* Increase refcount by 1, so this page couldn't be evicted */
		PinEbiSubBuffer(buf_id);
		LWLockRelease(new_partition_lock);

		return buf_id;
	}

	LWLockRelease(new_partition_lock);

	/* We don't get page from cache */
	return InvalidEbiSubBuf;
}

#endif /* DIVA */
