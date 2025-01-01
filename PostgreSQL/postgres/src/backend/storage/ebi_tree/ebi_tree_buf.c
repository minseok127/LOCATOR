/*-------------------------------------------------------------------------
 *
 * ebi_tree_buf.c
 *
 * EBI Tree Buffer Implementation
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
 *    src/backend/storage/ebi_tree/ebi_tree_buf.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef DIVA
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
#include "storage/ebi_tree_buf.h"
#include "storage/ebi_tree_hash.h"
#include "utils/dynahash.h"

/* Check both globals.c and miscadmin.h */
extern PGDLLIMPORT int NEbiTreeBuffers;

EbiTreeBufDescPadded *EbiTreeBufDescriptors;
char *EbiTreeBufBlocks;
EbiTreeBufMeta *EbiTreeBuf;

#define EBI_TREE_SEG_OFFSET_TO_PAGE_ID(off) (uint32)((off) / (EBI_TREE_SEG_PAGESZ))

/* Private functions */
static int EbiTreeBufGetBufRef(EbiTreeSegmentId seg_id,
		EbiTreeSegmentOffset seg_offset);

/* Segment control */
static int EbiTreeOpenSegmentFile(EbiTreeSegmentId seg_id);
static void EbiTreeCloseSegmentFile(int fd);
static void EbiTreeReadSegmentPage(const EbiTreeBufTag *tag, int buf_id);
static void EbiTreeWriteSegmentPage(const EbiTreeBufTag *tag, int buf_id);

static void EbiTreeBufUnrefInternal(EbiTreeBufDesc *buf);

Size
EbiTreeBufShmemSize(void)
{
	Size size = 0;

	/* EBI tree buffer descriptors */
	size =
		add_size(size, mul_size(NEbiTreeBuffers, sizeof(EbiTreeBufDescPadded)));

	/* To allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* Data pages */
	size = add_size(size, mul_size(NEbiTreeBuffers, EBI_TREE_SEG_PAGESZ));

	/* EBI tree buffer hash */
	size = add_size(size,
			EbiTreeHashShmemSize(NEbiTreeBuffers + NUM_EBI_TREE_PARTITIONS));

	/* EBI tree buffer metadata */
	size = add_size(size, sizeof(EbiTreeBufMeta));

	return size;
}

void
EbiTreeBufInit(void)
{
	bool foundDescs, foundBufs, foundMeta;
	EbiTreeBufDesc *buf;

	/* Align descriptors to a cacheline boundary */
	EbiTreeBufDescriptors = (EbiTreeBufDescPadded *)ShmemInitStruct(
			"EBI-tree Buffer Descriptors",
			NEbiTreeBuffers * sizeof(EbiTreeBufDescPadded),
			&foundDescs);

	/* Buffer blocks */
	EbiTreeBufBlocks =
		(char *)ShmemInitStruct("EBI-tree Buffer Blocks",
				NEbiTreeBuffers * ((Size)(EBI_TREE_SEG_PAGESZ)),
				&foundBufs);

	EbiTreeHashInit(NEbiTreeBuffers + NUM_EBI_TREE_PARTITIONS);

	/* Initialize descriptors */
	if (!foundDescs) {
		for (int i = 0; i < NEbiTreeBuffers; i++)
		{
			buf = GetEbiTreeBufDescriptor(i);
			buf->tag.seg_id = 0;
			buf->tag.page_id = 0;
			buf->is_dirty = false;
			pg_atomic_init_u32(&buf->refcnt, 0);
		}
	}

	/* Initialize metadata */
	EbiTreeBuf = (EbiTreeBufMeta *)ShmemInitStruct(
			"EBI-tree Buffer Metadata", sizeof(EbiTreeBufMeta), &foundMeta);
}

void
EbiTreeAppendVersion(EbiTreeSegmentId seg_id,
		EbiTreeSegmentOffset seg_offset,
		uint32 tuple_size,
		const void *tuple,
		LWLock *rwlock)
{
	int buf_id;
	EbiTreeBufDesc *buf;
	int page_offset;

	buf_id = EbiTreeBufGetBufRef(seg_id, seg_offset);
	buf = GetEbiTreeBufDescriptor(buf_id);

	page_offset = seg_offset % EBI_TREE_SEG_PAGESZ;

	/* Copy the tuple into the cache */
	memcpy(&EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ + page_offset],
			tuple,
			tuple_size);

	Assert(page_offset + (1 << my_log2(tuple_size)) <= EBI_TREE_SEG_PAGESZ);

	/*
	 * Mark it as dirty so that it could be flushed when evicted.
	 */
	buf->is_dirty = true;

	/* Whether or not the page has been full, we should unref the page */
	EbiTreeBufUnrefInternal(buf);
}

int
EbiTreeReadVersionRef(EbiTreeSegmentId seg_id,
		EbiTreeSegmentOffset seg_offset,
		uint32* tuple_size,
		void **ret_value)
{
	int buf_id;
	int page_offset;

	buf_id = EbiTreeBufGetBufRef(seg_id, seg_offset);

	page_offset = seg_offset % EBI_TREE_SEG_PAGESZ;

	/* Set ret_value to the pointer of the tuple in the cache */
	*ret_value = &EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ + page_offset];

	/* The caller must unpin the buffer entry for buf_id */
	return buf_id;
}

/*
 * EbiTreeBufGetBufRef
 *
 * Increase refcount of the requested segment page, and returns the cache_id.
 * If the page is not cached, read it from the segment file. If cache is full,
 * evict one page following the eviction policy (currently round-robin..)
 * Caller must decrease refcount after using it. If caller makes the page full
 * by appending more tuple, it has to decrease one more count for unpinning it.
 */
static int
EbiTreeBufGetBufRef(EbiTreeSegmentId seg_id, EbiTreeSegmentOffset seg_offset)
{
	EbiTreeBufTag tag;          /* identity of requested block */
	int buf_id;                 /* buf index of target segment page */
	int candidate_id;           /* buf index of victim segment page */
	LWLock *new_partition_lock; /* buf partition lock for it */
	LWLock *old_partition_lock; /* buf partition lock for it */
	uint32 hashcode;
	uint32 hashcode_vict;
	EbiTreeBufDesc *buf;
	int ret;
	EbiTreeBufTag victim_tag;

	tag.seg_id = seg_id;
	tag.page_id = EBI_TREE_SEG_OFFSET_TO_PAGE_ID(seg_offset);

	/* Get hash code for the segment id & page id */
	hashcode = EbiTreeHashCode(&tag);
	new_partition_lock = EbiTreeMappingPartitionLock(hashcode);

	LWLockAcquire(new_partition_lock, LW_SHARED);
	buf_id = EbiTreeHashLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		/* Target page is already in cache */
		buf = GetEbiTreeBufDescriptor(buf_id);

		/* Increase refcount by 1, so this page couldn't be evicted */
		pg_atomic_fetch_add_u32(&buf->refcnt, 1);
		LWLockRelease(new_partition_lock);

		return buf_id;
	}

	LWLockRelease(new_partition_lock);
	LWLockAcquire(new_partition_lock, LW_EXCLUSIVE);
	buf_id = EbiTreeHashLookup(&tag, hashcode);
	if (buf_id >= 0)
	{
		/* Target page is already in cache */
		buf = GetEbiTreeBufDescriptor(buf_id);

		/* Increase refcount by 1, so this page couldn't be evicted */
		pg_atomic_fetch_add_u32(&buf->refcnt, 1);
		LWLockRelease(new_partition_lock);

		return buf_id;
	}

find_cand:
	/* Pick up a candidate cache entry for a new allocation */
	candidate_id = pg_atomic_fetch_add_u64(&EbiTreeBuf->eviction_rr_idx, 1) %
		NEbiTreeBuffers;
	buf = GetEbiTreeBufDescriptor(candidate_id);
	if (pg_atomic_read_u32(&buf->refcnt) != 0)
	{
		/* Someone is accessing this entry, find another candidate */
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
		hashcode_vict = EbiTreeHashCode(&victim_tag);
		old_partition_lock = EbiTreeMappingPartitionLock(hashcode_vict);
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
      uint32 my_slot;
			//seg_id = buf->tag.seg_id;

			Assert(buf->tag.seg_id != 0);

			/* Check if the page related segment file has been removed */
			my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
			pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);
			pg_memory_barrier();

			if (EbiSegIsAlive(EbiTreeShmem->ebitree, buf->tag.seg_id))
			{
				EbiTreeWriteSegmentPage(&buf->tag, candidate_id);
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
		EbiTreeHashDelete(&buf->tag, hashcode_vict);
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

	/* Read target segment page into the cache */
	EbiTreeReadSegmentPage(&buf->tag, candidate_id);

	/* Next, insert new hash entry for it */
	ret = EbiTreeHashInsert(&tag, hashcode, candidate_id);
	Assert(ret == -1);

	LWLockRelease(new_partition_lock);

	/* Return the index of cache entry, holding refcount 1 */
	return candidate_id;
}

/* Segment open & close */

/*
 * EbiTreeCreateSegmentFile
 *
 * Make a new file for corresponding seg_id
 */
void
EbiTreeCreateSegmentFile(EbiTreeSegmentId seg_id)
{
	int fd;
	char filename[128];

	sprintf(filename, "ebitree.%09d", seg_id);
	fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);

	Assert(fd >= 0);

	close(fd);
}

/*
 * EbiTreeRemoveSegmentFile
 *
 * Remove file for corresponding seg_id
 */
	void
EbiTreeRemoveSegmentFile(EbiTreeSegmentId seg_id)
{
	char filename[128];

	sprintf(filename, "ebitree.%09d", seg_id);

	if (remove(filename) != 0)
		elog(ERROR, "EbiTreeRemoveSegmentFile error");
}

/*
 * EbiTreeOpenSegmentFile
 *
 * Open Segment file.
 * Caller have to call EbiTreeCloseSegmentFile(seg_id) after file io is done.
 */
static int
EbiTreeOpenSegmentFile(EbiTreeSegmentId seg_id)
{
	int fd;
	char filename[128];

	Assert(seg_id != 0);

	sprintf(filename, "ebitree.%09d", seg_id);
	fd = open(filename, O_RDWR, (mode_t)0600);

	Assert(fd >= 0);

	return fd;
}

/*
 * EbiTreeCloseSegmentFile
 *
 * Close Segment file.
 */
static void
EbiTreeCloseSegmentFile(int fd)
{
	Assert(fd >= 0);

	close(fd);
}

static void
EbiTreeReadSegmentPage(const EbiTreeBufTag *tag, int buf_id)
{
	ssize_t read;
	int fd;

	fd = EbiTreeOpenSegmentFile(tag->seg_id);

	Assert(fd >= 0);

	read = pg_pread(fd,
			&EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
			EBI_TREE_SEG_PAGESZ,
			tag->page_id * EBI_TREE_SEG_PAGESZ);

	/* New page */
	if (read == 0)
	{
		memset(&EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
				0,
				EBI_TREE_SEG_PAGESZ);
	}

	EbiTreeCloseSegmentFile(fd);
}

static void
EbiTreeWriteSegmentPage(const EbiTreeBufTag *tag, int buf_id)
{
	int fd;
	ssize_t written;

	fd = EbiTreeOpenSegmentFile(tag->seg_id);

	written = pg_pwrite(fd,
			&EbiTreeBufBlocks[buf_id * EBI_TREE_SEG_PAGESZ],
			EBI_TREE_SEG_PAGESZ,
			tag->page_id * EBI_TREE_SEG_PAGESZ);

	if (written != EBI_TREE_SEG_PAGESZ)
		elog(ERROR, "EbiTreeWriteSegmentPage error");

	EbiTreeCloseSegmentFile(fd);
}

/*
 * EbiTreeBufUnref
 *
 * A public version of EbiTreeBufUnrefInternal
 */
void
EbiTreeBufUnref(int buf_id)
{
	EbiTreeBufDesc *buf;
	if (!EbiTreeBufIsValid(buf_id))
	{
		elog(ERROR, "Buffer id is not valid");
		return;
	}
	buf = GetEbiTreeBufDescriptor(buf_id);
	EbiTreeBufUnrefInternal(buf);
}

bool
EbiTreeBufIsValid(int buf_id)
{
	return buf_id < NEbiTreeBuffers;
}

/*
 * EbiTreeBufUnrefInternal
 *
 * Decrease the refcount of the given buf entry
 */
static void
EbiTreeBufUnrefInternal(EbiTreeBufDesc *buf)
{
	if (pg_atomic_read_u32(&buf->refcnt) == 0)
		elog(ERROR, "EbiTreeBufUnrefInternal refcnt == 0");

	pg_atomic_fetch_sub_u32(&buf->refcnt, 1);
}

#endif /* DIVA */
