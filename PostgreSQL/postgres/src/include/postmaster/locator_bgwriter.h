/*-------------------------------------------------------------------------
 *
 * locator_bgwriter.h
 *	  postmaster/locator_bgwriter.c
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 *
 * src/include/postmaster/locator_bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _LOCATOR_BGWRITER_H
#define _LOCATOR_BGWRITER_H

#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/sync.h"

/* GUC options */
extern PGDLLIMPORT int LocatorBgWriterDelay;

extern void LocatorBackgroundWriterMain(void) pg_attribute_noreturn();

#endif /* _LOCATOR_BGWRITER_H */
