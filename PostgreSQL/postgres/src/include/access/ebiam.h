/*-------------------------------------------------------------------------
 *
 * heapam.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA

#ifndef EBIAM_H
#define EBIAM_H

#ifdef LOCATOR
#include <liburing.h>
#endif /* LOCATOR */
#include "nodes/nodes.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "storage/block.h"
#include "access/sdir.h"
#include "executor/tuptable.h"

/* Tricky variable for passing the cmd type from ExecutePlan to heap code */
extern CmdType curr_cmdtype;

extern void ebigetpage(HeapScanDesc scan);
extern bool ebi_getnextslot(TableScanDesc sscan, ScanDirection direction, 
								struct TupleTableSlot *slot);
extern void InsertUnobtainedVersionOffset(HeapScanDesc scan, 
										  EbiSubVersionOffset versionOffset);

#endif							/* EBIAM_H */
#endif							/* DIVA */
