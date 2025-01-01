/*-------------------------------------------------------------------------
 *
 * locator_md.c
 *
 * Locator Magnetic Disk Implementation
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
 *    src/backend/locator/partitioning/locator_md.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef LOCATOR
#include "postgres.h"

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "utils/wait_event.h"
#include "port/pg_iovec.h"
#include "miscadmin.h"

#include "locator/locator_md.h"
#include "locator/locator_mempart_buf.h"

static File read_file_cache = -1;

/*
 * LocatorNblocks
 *		Get the number of blocks of the file.
 *
 * Called when the partitioning worker does partitioning.
 */
BlockNumber
LocatorNblocks(RelFileNode node, LocatorPartLevel partLvl,
			   LocatorPartNumber partNum, LocatorPartGenNo partGen)
{
	char		   *path;
	int				fd;
	BlockNumber		blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	if (access(path, F_OK) == -1)
	{
		pfree(path);
		return 0;
	}

	/* Open the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDONLY)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was not created yet */
			pfree(path);
			return 0;
		}
		else
		{
#ifdef ABORT_AT_FAIL
			Abort("LocatorNblocks, failed to open locator partition file");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
								  path));
#endif
		}
	}

	/* Get file size and compute new blockNum */
	blockNum = LocatorLseekBlocks(fd);

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorNblocks, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	if (unlikely(blockNum == 0))
	{
#ifdef LOCATOR_DEBUG
		fprintf(stderr, "[LOCATOR] LocatorNblocks, path: %s, nblocks: %u -> %u\n",
						path, blockNum, LocatorMempartPageCount);
#endif /* LOCATOR_DEBUG */
		/*
		 * If blockNum is 0, it means that this file is a level 0 mempartition
		 * and has not yet been flushed after being filled in-memory.
		 * Therefore, let blockNum be the maximum number of blocks in the
		 * mempartition. 
		 */
		Assert(partLvl == 0);
		blockNum = LocatorMempartPageCount;
	}
#ifdef LOCATOR_DEBUG
	else
	{
		fprintf(stderr, "[LOCATOR] LocatorNblocks, path: %s, nblocks: %u\n",
						path, blockNum);
	}
#endif /* LOCATOR_DEBUG */

	pfree(path);
	return blockNum;
}

/*
 * LocatorCreatePartitionFile
 */
int
LocatorCreatePartitionFile(RelFileNode node, LocatorPartLevel partLvl,
			  			   LocatorPartNumber partNum, LocatorPartGenNo partGen,
						   int flags)
{
	char		   *path;
	int				fd;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Check existing partition file. */
	if (unlikely(access(path, F_OK) == 0))
	{
		/* The file is already exists. */
		elog(WARNING, "locator partition file is already created, name: %s", 
					  path);

		/* Open the partition file */
		if ((fd = LocatorSimpleOpen(path, O_RDWR | flags)) == -1)
		{
#ifdef ABORT_AT_FAIL
			Abort("LocatorCreatePartitionFile, failed to open locator partition file");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
								  path));
#endif
		}

		pfree(path);

		return fd;
	}

	/* Open(or create) the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR | O_CREAT | flags)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorCreatePartitionFile, failed to create locator partition file");
#else
		ereport(ERROR, errmsg("Failed to create locator partition file, name: %s",
							  path));
#endif
	}

	/* Resource cleanup. */
	pfree(path);

	return fd;
}

/*
 * LocatorOpenPartitionFile
 */
int
LocatorOpenPartitionFile(RelFileNode node, LocatorPartLevel partLvl,
			  			 LocatorPartNumber partNum, LocatorPartGenNo partGen,
						 int flags)
{
	char		   *path;
	int				fd;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open(or create) the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR | flags)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorOpenPartitionFile, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}

	/* Resource cleanup. */
	pfree(path);

	return fd;
}


/*
 * LocatorExtend
 *		Extend the partition file.
 *
 * Called when the partitioning worker does partitioning.
 */
void
LocatorExtend(RelFileNode node, LocatorPartLevel partLvl,
			  LocatorPartNumber partNum, LocatorPartGenNo partGen,
			  BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;
	int				flags = enableFsync ? O_SYNC : 0;

	/* Get position to write zero page */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open(or create) the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR | O_CREAT | flags)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorExtend, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}

retry:
	/* Write and sync zero page */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
	nbytes = pg_pwrite(fd, buffer, BLCKSZ, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != BLCKSZ && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorExtend, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend locator partition file \"%s\": %m",
							path),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend locator partition file \"%s\": wrote only %d of %d bytes at block %u",
						path, nbytes, BLCKSZ, blockNum),
				 errhint("Check free disk space.")));
	}

	pfree(path);
}

/*
 * LocatorMempartExtend
 *		Extend the partition file.
 *
 * Called when the partitioning worker does partitioning.
 */
void
LocatorMempartExtend(RelFileNode node, LocatorPartLevel partLvl,
					 LocatorPartNumber partNum, LocatorPartGenNo partGen,
					 BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	int				flags = enableFsync ? O_SYNC : 0;

	Assert(blockNum == 0);

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open(or create) the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR | O_CREAT | flags)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartExtend, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}

	/* Mempart does not have to change EOF early */
#if 0
retry:
	/* Write and sync zero page */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_EXTEND);
	nbytes = pg_pwrite(fd, buffer, wsize, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != wsize && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}
#endif

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartExtend, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

#if 0
	if (nbytes != wsize)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend file \"%s\": %m",
							path),
					 errhint("Check free disk space.")));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not extend file \"%s\": wrote only %d of %d bytes at block %u",
						path, nbytes, wsize, blockNum),
				 errhint("Check free disk space.")));
	}
#endif

	pfree(path);
}


static int
LocatorWriteInternal(int fd, char *buffer, off_t seekpos, int writeSize)
{
	int				nbytes;

retry:
	/* Write data */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	nbytes = pg_pwrite(fd, buffer, writeSize, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != writeSize && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	return nbytes;
}

/*
 * LocatorWrite
 *		Write single partition page.
 *
 * Called when evicting a partition data block from PostgreSQL's traditional
 * buffer pool.
 */
bool
LocatorWrite(RelFileNode node, LocatorPartLevel partLvl,
			 LocatorPartNumber partNum, LocatorPartGenNo partGen,
			 BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;

	/* A relation using locator must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get position to write */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	if (access(path, F_OK) == -1)
	{
		pfree(path);
		return false;
	}

	/* Open the partition file */
	if ((fd = LocatorSimpleOpen(path, O_WRONLY)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return false;
		}
		else
		{
#ifdef ABORT_AT_FAIL
			Abort("LocatorWrite, failed to open locator partition file");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
								  path));
#endif
		}
	}

	/* Write the buffer */
	nbytes = LocatorWriteInternal(fd, buffer, seekpos, BLCKSZ);

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorWrite, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	if (nbytes != BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write block %u in locator partition file \"%s\": %m",
							blockNum, path)));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write block %u in locator partition file \"%s\": wrote only %d of %d bytes",
						blockNum,
						path,
						nbytes, BLCKSZ),
				 errhint("Check free disk space.")));
	}

	pfree(path);
	return true;
}

/*
 * LocatorWritePartialColumnZone
 *		Write the part of column group.
 *
 * Called when partitioning worker writes the partial record zone.
 */
int
LocatorWritePartialColumnZone(int fd, BlockNumber blockNum,
							   char *buffer, int nblocks)
{
	off_t			seekpos;
	int				nbytes;
	int				writeSize = BLCKSZ * nblocks;

	Assert(fd > 0);

	/* Get position to write */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Write the buffer */
	nbytes = LocatorWriteInternal(fd, buffer, seekpos, writeSize);

	if (unlikely(nbytes != writeSize))
	{
		/* file access error */
		if (nbytes < 0)
			return -1;
		
		/* disk full error */
		return 1;
	}

	return 0;
}

/*
 * LocatorWriteBatch
 *		Write multiple partition page.
 */
int
LocatorWriteBatch(int fd, BlockNumber blockNum, struct iovec *iovs,
				  int64_t iovcnt)
{
	uint64_t	offset = ((uint64)blockNum) * BLCKSZ;
	struct iovec *curIovs = iovs;
	int64_t		totalNbytes = 0;
	int64_t		tryNbytes = 0;
	int64_t 	iovsum = 0;
	int64_t		curIovcnt;
	int64_t		curOffset;
	int64_t		nbytes;

	for (;;)
	{
		/* Write is done. */
		if (iovcnt == iovsum)
			break;

		/* 
		 * Set iovcnt and iovector address, considering limited i/o vector's 
		 * maximum.
		 */
		curIovs = (struct iovec *) ((char *) iovs + sizeof(struct iovec) * iovsum);
		curOffset = offset + totalNbytes;
		curIovcnt = iovcnt - iovsum;
		if (curIovcnt > IOV_MAX)
			curIovcnt = IOV_MAX;

		/* Calculate the number of bytes for writing. */
		tryNbytes = 0;
		for (int i = 0; i < curIovcnt; ++i)
			tryNbytes += curIovs[i].iov_len;

retry:
		/* Write data */
		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
		nbytes = pwritev2(fd, curIovs, curIovcnt, curOffset, RWF_HIPRI);
		pgstat_report_wait_end();

		if (nbytes != tryNbytes)
		{
			if (errno == 0)
			{
				/* if write didn't set errno, assume problem is no disk space */
				errno = ENOSPC;
				elog(WARNING, "no disk space to write pages");
				return 1;
			}
			else
				return -1;
		}

		if (errno == EINTR)
		{
			/* OK to retry if interrupted */
			goto retry;
		}
		
		iovsum += curIovcnt;
		totalNbytes += nbytes;
	}

	return 0;
}

/*
 * LocatorWriteback
 *		Write single partition page back to storage. (sync)
 *
 * Flushes partition pages to storage from OS cache.
 */
void
LocatorWriteback(RelFileNode node, LocatorPartLevel partLvl,
				 LocatorPartNumber partNum, LocatorPartGenNo partGen,
				 BlockNumber blockNum, BlockNumber nblocks)
{
	char		   *path;
	int				fd;
	off_t			seekpos;

	/* Get position to flush */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	if (access(path, F_OK) == -1)
	{
		pfree(path);
		return;
	}

	/* Open the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return;
		}
		else
		{
#ifdef ABORT_AT_FAIL
			Abort("LocatorWriteback, failed to open locator partition file");
#else
			ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
								  path));
#endif
		}
	}

	/* Flush data */
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_FLUSH);
	pg_flush_data(fd, seekpos, (off_t) BLCKSZ * nblocks);
	pgstat_report_wait_end();

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorWriteback, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	pfree(path);
}

/*
 * LocatorRead
 *		Read single partition page.
 *
 * Called when reading a partition data block to PostgreSQL's traditional buffer
 * pool.
 */

void
LocatorRead(RelFileNode node, LocatorPartLevel partLvl,
			LocatorPartNumber partNum, LocatorPartGenNo partGen,
			BlockNumber blockNum, char *buffer)
{
	char		   *path;
	int				fd = -1;
	off_t			seekpos;
	int				nbytes;

	/* A relation using locator must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get position to read */
	seekpos = (off_t) BLCKSZ * blockNum;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if (read_file_cache != -1 && strcmp(FilePathName(read_file_cache), path) == 0)
	{
		fd = FileGetRawDesc(read_file_cache);
	}
	else
	{
		if (read_file_cache != -1)
			FileClose(read_file_cache);

		read_file_cache = PathNameOpenFile(path, O_RDONLY);
		fd = FileGetRawDesc(read_file_cache);
	}

#if 0
	if (fd != -1 && (fd = LocatorSimpleOpen(path, O_RDONLY)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorRead, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}
#endif

retry:
	if (partLvl == 0)
		pgstat_report_wait_start(WAIT_EVENT_LEVEL_ZERO_DATA_FILE_READ);
	else if (partLvl == 1)
		pgstat_report_wait_start(WAIT_EVENT_LEVEL_ONE_DATA_FILE_READ);
	else if (partLvl == 2)
		pgstat_report_wait_start(WAIT_EVENT_LEVEL_TWO_DATA_FILE_READ);
	else
		pgstat_report_wait_start(WAIT_EVENT_LEVEL_THREE_DATA_FILE_READ);

	/* Read data */
	nbytes = pg_pread(fd, buffer, BLCKSZ, seekpos);
	pgstat_report_wait_end();

	if (nbytes < 0)
	{
		/*
		 * Windows may run out of kernel buffers and return "Insufficient
		 * system resources" error.  Wait a bit and retry to solve it.
		 *
		 * It is rumored that EINTR is also possible on some Unix filesystems,
		 * in which case immediate retry is indicated.
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

#if 0
	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorRead, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}
#endif

	if (nbytes != BLCKSZ)
	{
		int error = errno;

		if (nbytes < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read block %u in locator partition file \"%s\", error: %d: %m",
							blockNum, path, error)));
		}

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read block %u in locator partition file \"%s\", error: %d: read only %d of %d bytes",
							blockNum, path, error,
							nbytes, BLCKSZ)));
	}

	pfree(path);
}

/*
 * LocatorPrepareRequest
 *		Prepare a single request for multi pages using io_uring.
 *
 * The file must have been opened by the caller.
 */
void
LocatorPrepareRequest(struct io_uring* ring, int fd, struct iovec *iovs, 
					  BlockNumber blockNum, int iovcnt, void *io_info,
					  bool is_read)
{
	off_t					seekpos;
	struct io_uring_sqe	   *sqe;

	seekpos = (off_t) BLCKSZ * blockNum;

	sqe = io_uring_get_sqe(ring);

	/* set request */
	if (is_read)
		io_uring_prep_readv(sqe, fd, iovs, iovcnt, seekpos);
	else
		io_uring_prep_writev(sqe, fd, iovs, iovcnt, seekpos);

	/* set user data */
	io_uring_sqe_set_data(sqe, io_info);
}

/*
 * LocatorSubmitRequest
 *		Submit multiple requests using io_uring.
 *
 * The file must have been opened by the caller.
 */
int
LocatorSubmitRequest(struct io_uring* ring)
{
	int						returnCode;

	/* submit requests */
	returnCode = io_uring_submit(ring);

	if (returnCode < 0)
#ifdef ABORT_AT_FAIL
		Abort("LocatorSubmitRequest, FileRequese error");
#else
		elog(ERROR, "FileRequest error");
#endif

	return returnCode;
}

bool
LocatorMempartWrite(RelFileNode node, LocatorPartLevel partLvl,
					LocatorPartNumber partNum, LocatorPartGenNo partGen,
					char *buffer, int nblock)
{
	char		   *path;
	int				fd;
	off_t			seekpos;
	int				nbytes;
	int				flags = enableFsync ? O_SYNC : 0;

	/* A relation using locator must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);
	Assert(nblock < (INT_MAX / BLCKSZ));

	/* Get position to write */
	seekpos = 0;

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	if (access(path, F_OK) == -1)
	{
		pfree(path);
		return false;
	}

	/* Open the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR | flags)) == -1)
	{
		if (likely(errno == ENOENT))
		{
			/* This file was GC'd, so just return */
			pfree(path);
			return false;
		}

#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartWrite, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}

retry:
	/* Write data */
	errno = 0;
	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	nbytes = pg_pwrite(fd, buffer, BLCKSZ * nblock, seekpos);
	pgstat_report_wait_end();

	/* if write didn't set errno, assume problem is no disk space */
	if (nbytes != BLCKSZ * nblock && errno == 0)
		errno = ENOSPC;

	if (nbytes < -1)
	{
		/*
		 * See comments in FileRead()
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartWrite, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	if (nbytes != BLCKSZ * nblock)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write in locator partition file \"%s\": %m",
							path)));
		/* short write: complain appropriately */
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("could not write in locator partition file \"%s\": wrote only %d of %d bytes",
						path,
						nbytes, BLCKSZ * nblock),
				 errhint("Check free disk space.")));
	}

	pfree(path);
	return true;
}

/*
 * LocatorMempartRead
 *		Read mempartition page into mempartition buffer.
 *
 * Called when the target mempartion doesn't exist in mempartition buffer pool.
 * 
 * In almost all cases, all transactions except INSERT that try to read a page
 * from a mempartition will read the page by calling LocatorRead(), not
 * LocatorMempartRead().
 * The only case where LocatorMempartRead() will be called is if the INSERT
 * transaction tries to insert a new record into the most recent mempartition
 * and it is not in the buffer, but in storage, and this will only happen once,
 * the first time PostgreSQL starts.
 */
void
LocatorMempartRead(RelFileNode node, LocatorPartLevel partLvl,
				   LocatorPartNumber partNum, LocatorPartGenNo partGen,
				   char *buffer)
{
	char		   *path;
	int				fd;
	int				nblock;
	int				nbytes;

	elog(LOG, "LocatorMempartRead");
	// sleep(15);

	/* A relation using locator must have a normal space oid */
	Assert(node.spcNode == DEFAULTTABLESPACE_OID);

	/* Get file name */
	path = psprintf("base/%u/%u.%u.%u.%u",
					node.dbNode, node.relNode, partLvl, partNum, partGen);

	/* Open the partition file */
	if ((fd = LocatorSimpleOpen(path, O_RDWR)) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartRead, failed to open locator partition file");
#else
		ereport(ERROR, errmsg("Failed to open locator partition file, name: %s",
							  path));
#endif
	}

retry:
	nblock = LocatorLseekBlocks(fd);
	Assert(nblock < (INT_MAX / BLCKSZ));
	Assert(nblock != 0);

	/* Read data */
	pgstat_report_wait_start(WAIT_EVENT_LEVEL_ZERO_DATA_FILE_READ);
	nbytes = pg_pread(fd, buffer, nblock * BLCKSZ, 0);
	pgstat_report_wait_end();

	if (nbytes < 0)
	{
		/*
		 * Windows may run out of kernel buffers and return "Insufficient
		 * system resources" error.  Wait a bit and retry to solve it.
		 *
		 * It is rumored that EINTR is also possible on some Unix filesystems,
		 * in which case immediate retry is indicated.
		 */
#ifdef WIN32
		DWORD		error = GetLastError();

		switch (error)
		{
			case ERROR_NO_SYSTEM_RESOURCES:
				pg_usleep(1000L);
				errno = EINTR;
				break;
			default:
				_dosmaperr(error);
				break;
		}
#endif
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
	}

	/* Close file */
	if (LocatorSimpleClose(fd) == -1)
	{
#ifdef ABORT_AT_FAIL
		Abort("LocatorMempartRead, failed to close locator partition file");
#else
		ereport(ERROR, errmsg("Failed to close locator partition file, name: %s",
							  path));
#endif
	}

	if (nbytes != nblock * BLCKSZ)
	{
		if (nbytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read locator mempartition file \"%s\": %m",
							path)));

		/*
		 * Short read: we are at or past EOF, or we read a partial block at
		 * EOF.  Normally this is an error; upper levels should never try to
		 * read a nonexistent block.  However, if zero_damaged_pages is ON or
		 * we are InRecovery, we should instead return zeroes without
		 * complaining.  This allows, for example, the case of trying to
		 * update a block that was later truncated away.
		 */
		if (zero_damaged_pages || InRecovery)
			MemSet(buffer, 0, BLCKSZ);
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read locator mempartition file \"%s\": read only %d of %d bytes",
							path,
							nbytes, nblock * BLCKSZ)));
	}

	pfree(path);
}
#endif /* LOCATOR */
