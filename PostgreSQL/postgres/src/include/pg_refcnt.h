#ifndef PG_REFCNT_H
#define PG_REFCNT_H

#ifdef WAIT_YIELD
#include <pthread.h>
#endif /* WAIT_YIELD */

#include "c.h"
#include "port/atomics.h"
#include "storage/s_lock.h"

#ifdef LOCATOR

typedef struct DualRefDescData
{
	pg_atomic_uint64   *dual_ref;
	uint64				page_ref_unit;
	uint64				prev_toggle_bit;
} DualRefDescData;

typedef struct DualRefDescData *DualRefDesc;

/*
 *  dual_ref (64 bits)
 *  - Two reference counters are combined in one single 8byte atomic variable.
 * ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
 * | 1-bit  |    2-bits    | 1-bit |      1-bit       |     11-bits     |      1-bit       |      11-bits     |     12-bits     |     12-bits     |       12-bits      |
 * | toggle | modification | free  | modification bit | reference count | modification bit | reference count  | toggle-on  side | toggle-off side |   target record    |
 * |  bit   |  state bits  | space | of left version  | of left version | of right version | of right version | reference count | reference count | offnum (p-locator) |
 * ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
 *                                 |------------------- reference counter of target record -------------------|---- reference counter of page ----|
 * 
 * - meanings -
 * 
 * target record:
 * - The record target of modification(UPDATE/DELETE).
 * 
 * modification bit of each version:
 * - A bit indicating whether the version in the target record is being modified.
 * - 0: This version is not currently being modified.
 * - 1: This version is currently being modified.
 * 
 * toggle bit:
 * - A bit indicating where the reference count of the page should be shown.
 * - 0: Reference count of this page should be shown in left area.
 * - 1: Reference count of this page should be shown in right area.
 * 
 * modification state bits:
 * - Bits indicating what kind of write is currently occurring on the page.
 * - 11: INSERT
 * - 10: UPDATE
 * - 01: DELETE
 * - 00: NONE (not being written)
 */
#define DRefGetVal(dref) (pg_atomic_read_u64(dref))

/* MASKs */
#define DRefToggleBit				(0x8000000000000000ULL)						/* |1|00|0| |0|000 0000 0000| |0|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefStateBits				(0x6000000000000000ULL)						/* |0|11|0| |0|000 0000 0000| |0|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefTargetBits				(0x0800800000000000ULL)						/* |0|00|0| |1|000 0000 0000| |1|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefTargetLeftBit			(0x0800000000000000ULL)						/* |0|00|0| |1|000 0000 0000| |0|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefTargetLeftRefCnt		(0x07FF000000000000ULL)						/* |0|00|0| |0|111 1111 1111| |0|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefTargetRightBit			(0x0000800000000000ULL)						/* |0|00|0| |0|000 0000 0000| |1|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefTargetRightRefCnt		(0x00007FF000000000ULL)						/* |0|00|0| |0|000 0000 0000| |0|111 1111 1111| |0000 0000 0000| |0000 0000 0000| |0000 0000 0000| */
#define DRefToggleOnRefCnt			(0x0000000FFF000000ULL)						/* |0|00|0| |0|000 0000 0000| |0|000 0000 0000| |1111 1111 1111| |0000 0000 0000| |0000 0000 0000| */
#define DRefToggleOffRefCnt			(0x0000000000FFF000ULL)						/* |0|00|0| |0|000 0000 0000| |0|000 0000 0000| |0000 0000 0000| |1111 1111 1111| |0000 0000 0000| */
#define DRefPageRefCnt				(0x0000000FFFFFF000ULL)						/* |0|00|0| |0|000 0000 0000| |0|000 0000 0000| |1111 1111 1111| |1111 1111 1111| |0000 0000 0000| */
#define DRefTargetOffnum			(0x0000000000000FFFULL)						/* |0|00|0| |0|000 0000 0000| |0|000 0000 0000| |0000 0000 0000| |0000 0000 0000| |1111 1111 1111| */
#define DRefClearMask				(0x97FF7FFFFFFFF000ULL)						/* |1|00|1| |0|111 1111 1111| |0|111 1111 1111| |1111 1111 1111| |1111 1111 1111| |0000 0000 0000| */

/* reference count unit */
#define DRefTargetLeftUnit			(0x0001000000000000ULL)
#define DRefTargetRightUnit			(0x0000001000000000ULL)
#define DRefTargetBothUnit			(0x0001001000000000ULL)
#define DRefToggleOnUnit			(0x0000000001000000ULL)
#define DRefToggleOffUnit			(0x0000000000001000ULL)
#define DRefSwitchUnit				(0x0000000000FFF000ULL)

/* modification states */
#define DRefStateINSERT				(0x6000000000000000ULL) /* 11 */
#define DRefStateUPDATE				(0x4000000000000000ULL) /* 10 */
#define DRefStateDELETE				(0x2000000000000000ULL) /* 01 */
#define DRefStateNONE				(0x0000000000000000ULL) /* 00 */

#define DRefGetToggleBit(drefval) \
	(drefval & DRefToggleBit)
#define DRefGetState(drefval) \
	(drefval & DRefStateBits)
#define DRefGetTargetBits(drefval) \
	(drefval & DRefTargetBits)
#define DRefGetTargetOffnum(drefval) \
	(drefval & DRefTargetOffnum)

#define DRefIsToggled(drefval) \
	(DRefGetToggleBit(drefval) != 0)

#define DRefIncrRefCnt(dref, ref_unit) \
	(pg_atomic_fetch_add_u64(dref, ref_unit))
#define DRefDecrRefCnt(dref, ref_unit) \
	(pg_atomic_fetch_sub_u64(dref, ref_unit))

#define DRefSetINSERT(dref) \
	(pg_atomic_fetch_add_u64(dref, DRefStateINSERT) & DRefPageRefCnt)
#define DRefSetModicationInfo(dref, state, offnum) \
	(pg_atomic_fetch_add_u64(dref, (DRefToggleBit | state | offnum)) & DRefToggleBit)
#define DRefClearModicationInfo(dref) \
	(pg_atomic_fetch_and_u64(dref, DRefClearMask))

#ifdef WAIT_WITH_FUNCTION
extern void DRefWaitForReaders(pg_atomic_uint64 *dref, uint64 mask);
extern void DRefSetAndWaitForLeft(pg_atomic_uint64 *dref);
extern void DRefSetAndWaitForRight(pg_atomic_uint64 *dref);
extern void DRefWaitForINSERT(pg_atomic_uint64 *dref, uint64 oldval);
#else
#pragma GCC push_options
#pragma GCC optimize ("O0")

#ifdef WAIT_YIELD
#define DRefWaitForReaders(dref, mask) \
	do { \
		while ((DRefGetVal(dref) & mask) != 0) \
			{pthread_yield();} \
	} while (0)

#define DRefSetAndWaitForLeft(dref) \
	do { \
		if ((pg_atomic_fetch_or_u64(dref, DRefTargetLeftBit) & DRefTargetLeftRefCnt) != 0) \
			while ((DRefGetVal(dref) & DRefTargetLeftRefCnt) != 0) {pthread_yield();} \
	} while (0)

#define DRefSetAndWaitForRight(dref) \
	do { \
		if ((pg_atomic_fetch_or_u64(dref, DRefTargetRightBit) & DRefTargetRightRefCnt) != 0) \
			while ((DRefGetVal(dref) & DRefTargetRightRefCnt) != 0) {pthread_yield();} \
	} while (0)

#define DRefWaitForINSERT(dref, oldval) \
	do { \
		while (DRefGetState(oldval = DRefGetVal(dref)) == DRefStateINSERT) \
			{pthread_yield();} \
	} while (0)
#else
#define DRefWaitForReaders(dref, mask) \
	do { \
		while ((DRefGetVal(dref) & mask) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#define DRefSetAndWaitForLeft(dref) \
	do { \
		if ((pg_atomic_fetch_or_u64(dref, DRefTargetLeftBit) & DRefTargetLeftRefCnt) != 0) \
			while ((DRefGetVal(dref) & DRefTargetLeftRefCnt) != 0) {SPIN_DELAY();} \
	} while (0)

#define DRefSetAndWaitForRight(dref) \
	do { \
		if ((pg_atomic_fetch_or_u64(dref, DRefTargetRightBit) & DRefTargetRightRefCnt) != 0) \
			while ((DRefGetVal(dref) & DRefTargetRightRefCnt) != 0) {SPIN_DELAY();} \
	} while (0)

#define DRefWaitForINSERT(dref, oldval) \
	do { \
		while (DRefGetState(oldval = DRefGetVal(dref)) == DRefStateINSERT) \
			{SPIN_DELAY();} \
	} while (0)
#endif /* WAIT_YIELD */

#pragma GCC pop_options
#endif /* WAIT_WITH_FUNCTION */

#endif /* LOCATOR */

#endif /* PG_REFCNT_H */
