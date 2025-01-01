/*-------------------------------------------------------------------------
 *
 * locator_mempart_buf.h
 *	  LOCATOR's memaprt buffer.
 *
 *
 * src/include/locator/locator_mempart_buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_MEMPART_BUF_H
#define LOCATOR_MEMPART_BUF_H

#include "storage/lwlock.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "locator/locator_md.h"

#define LOCATOR_MEMPART_COUNT (128)

/* 1024 means 8MB (Page size 8KB * 1024) */
#define LOCATOR_MEMPART_PAGE_COUNT (1024 * 2) 

#define LOCATOR_MEMPART_BUFFER_TOTAL_PAGE_COUNT \
	(LOCATOR_MEMPART_COUNT * LOCATOR_MEMPART_PAGE_COUNT)

extern int LocatorGetMempartIdFromBufId(int buf_id);

extern LWLock *LocatorGetMempartLock(int mempart_id);

extern int LocatorGetMempartBufId(int buf_id);

extern bool LocatorCheckMempartBuf(int buf_id);

extern BlockNumber LocatorGetMempartBlockCount(RelFileNode node,
											   LocatorPartNumber partNum);

#endif /* LOCATOR_MEMPART_BUF_H */
