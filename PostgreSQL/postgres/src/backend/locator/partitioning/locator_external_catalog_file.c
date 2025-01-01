/*-------------------------------------------------------------------------
 *
 * locator_external_catalog_file.c
 *
 * Locator External Catalog Implementation
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
 *    src/backend/locator/partitioning/locator_external_catalog_file.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef LOCATOR
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
#include "utils/dynahash.h"
#include "utils/resowner_private.h"

#include "locator/locator_external_catalog.h"

static void
LocatorGetExternalCatalogFilename(const Oid relationOid, char *filename)
{
	snprintf(filename, 32, "external_catalog/%d", relationOid);
}

/*
 * LocatorCreateExternalCatalogFile
 *
 * Make a new file for corresponding relation oid.
 */
void
LocatorCreateExternalCatalogFile(const Oid relationOid)
{
	int fd;
	char filename[32];

	LocatorGetExternalCatalogFilename(relationOid, filename);
	fd = open(filename, O_RDWR | O_CREAT | O_SYNC, (mode_t)0600);

	if (fd < 0)
	{
		fprintf(stderr, 
			"locator external catalog file create error: %s", strerror(errno));
		Assert(false);
	}

	close(fd);
}

/*
 * LocatorOpenExternalCatalogFile
 *
 * Open external catalog file.
 * Caller have to call 'LocatorCloseExternalCatalogFile' after file I/O is done.
 */
static int
LocatorOpenExternalCatalogFile(Oid relationOid)
{
	int fd;
	char filename[32];

	LocatorGetExternalCatalogFilename(relationOid, filename);

	fd = open(filename, O_RDWR | O_SYNC, (mode_t) 0600);
	Assert(fd >= 0);

	return fd;
}

/*
 * LocatorCloseExternalCatalogFile
 *
 * Close external catalog file.
 */
static void
LocatorCloseExternalCatalogFile(int fd)
{
	Assert(fd >= 0);

	if (close(fd) == -1)
	{
		fprintf(stderr, "external catalog file close error, %s\n", strerror(errno));
		Assert(false);
	}
}


/*
 * LocatorReadExternalCatalogFile
 */
bool
LocatorExternalCatalogFileExists(const Oid relationOid)
{
	char filename[32];
	
	LocatorGetExternalCatalogFilename(relationOid, filename);

	if (access(filename, F_OK) != -1)
		return true;
	else
		return false;
}


/*
 * LocatorReadExternalCatalogFile
 */
void
LocatorReadExternalCatalogFile(const Oid relationOid, char *block)
{
	ssize_t read;
	int fd;

	fd = LocatorOpenExternalCatalogFile(relationOid);

read_retry:
	read = pg_pread(fd, block, EXTERNAL_CATALOG_BLOCKSIZE, 0);
	if (read < 0 && errno == EINTR)
		goto read_retry;

	Assert(read == EXTERNAL_CATALOG_BLOCKSIZE);

	LocatorCloseExternalCatalogFile(fd);
}

/*
 * LocatorWriteExternalCatalogFile
 */
void
LocatorWriteExternalCatalogFile(const Oid relationOid, char *block)
{
	int fd;
	ssize_t written;

	fd = LocatorOpenExternalCatalogFile(relationOid);

write_retry:
	written = pg_pwrite(fd, block, EXTERNAL_CATALOG_BLOCKSIZE, 0);
	if (written < 0 && errno == EINTR)
		goto write_retry;

	Assert(written == EXTERNAL_CATALOG_BLOCKSIZE);

	LocatorCloseExternalCatalogFile(fd);
}

#endif /* LOCATOR */
