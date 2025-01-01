/*-------------------------------------------------------------------------
 *
 * itemid.h
 *	  Standard POSTGRES buffer page item identifier/line pointer definitions.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/itemid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ITEMID_H
#define ITEMID_H

/*
 * A line pointer on a buffer page.  See buffer page definitions and comments
 * for an explanation of how line pointers are used.
 *
 * In some cases a line pointer is "in use" but does not have any associated
 * storage on the page.  By convention, lp_len == 0 in every line pointer
 * that does not have storage, independently of its lp_flags state.
 */

#ifdef DIVA
typedef struct ItemIdData
{
	unsigned	lp_off:14,		/* offset to tuple (from start of page) */
				lp_flags:2,		/* state of line pointer, see below */
				lp_siro:2,		/* representing SIRO, see below */
				lp_pflag:1,		/* representing PLEAF line pointer */
				lp_len:13;		/* byte length of tuple */
} ItemIdData;

#ifdef LOCATOR
#define LP_SET_ATOMIC(itemId, itemIdData)	(__sync_lock_test_and_set((unsigned*)itemId, *((unsigned*)(&itemIdData))));
#endif /* LOCATOR */
/* 
 * 0x: OVR-USING
 * 1x : OVR-UNUSED
 * 
 * These two are specific to p-locators only
 * 10: 1st oldest version is left
 * 11: 1st oldest version is right
 */
#define LP_OVR_IS_LEFT(itemId)		(!((itemId)->lp_siro & 0x1))
#define LP_OVR_IS_RIGHT(itemId)		((itemId)->lp_siro & 0x1)
#define LP_OVR_IS_USING(itemId)		(!((itemId)->lp_siro & 0x2))
#define LP_OVR_IS_UNUSED(itemId)	((itemId)->lp_siro & 0x2)

#define LP_OVR_SET_LEFT(itemId)		((itemId)->lp_siro &= 0x2) 
#define LP_OVR_SET_RIGHT(itemId)	((itemId)->lp_siro |= 0x1) 
#define LP_OVR_SET_USING(itemId)	((itemId)->lp_siro &= 0x1) 
#define LP_OVR_SET_UNUSED(itemId)	((itemId)->lp_siro |= 0x2)

#define LP_SET_PLEAF_FLAG(itemId)	((itemId)->lp_pflag = 1)
#define LP_IS_PLEAF_FLAG(itemId)	((itemId)->lp_pflag)

/* check_var for readers */
#define CHECK_NONE	(0x0)
#define CHECK_LEFT	(0x1)
#define CHECK_RIGHT	(0x2)
#define CHECK_BOTH	(0x3)
#else
typedef struct ItemIdData
{
	unsigned	lp_off:15,		/* offset to tuple (from start of page) */
				lp_flags:2,		/* state of line pointer, see below */
				lp_len:15;		/* byte length of tuple */
} ItemIdData;
#endif
typedef ItemIdData *ItemId;

/*
 * lp_flags has these possible states.  An UNUSED line pointer is available
 * for immediate re-use, the other states are not.
 */
#define LP_UNUSED		0		/* unused (should always have lp_len=0) */
#define LP_NORMAL		1		/* used (should always have lp_len>0) */
#define LP_REDIRECT		2		/* HOT redirect (should have lp_len=0) */
#define LP_DEAD			3		/* dead, may or may not have storage */

/*
 * Item offsets and lengths are represented by these types when
 * they're not actually stored in an ItemIdData.
 */
typedef uint16 ItemOffset;
typedef uint16 ItemLength;


/* ----------------
 *		support macros
 * ----------------
 */

/*
 *		ItemIdGetLength
 */
#define ItemIdGetLength(itemId) \
   ((itemId)->lp_len)

/*
 *		ItemIdGetOffset
 */
#define ItemIdGetOffset(itemId) \
   ((itemId)->lp_off)

/*
 *		ItemIdGetFlags
 */
#define ItemIdGetFlags(itemId) \
   ((itemId)->lp_flags)

/*
 *		ItemIdGetRedirect
 * In a REDIRECT pointer, lp_off holds offset number for next line pointer
 */
#define ItemIdGetRedirect(itemId) \
   ((itemId)->lp_off)

/*
 * ItemIdIsValid
 *		True iff item identifier is valid.
 *		This is a pretty weak test, probably useful only in Asserts.
 */
#define ItemIdIsValid(itemId)	PointerIsValid(itemId)

/*
 * ItemIdIsUsed
 *		True iff item identifier is in use.
 */
#define ItemIdIsUsed(itemId) \
	((itemId)->lp_flags != LP_UNUSED)

/*
 * ItemIdIsNormal
 *		True iff item identifier is in state NORMAL.
 */
#define ItemIdIsNormal(itemId) \
	((itemId)->lp_flags == LP_NORMAL)

/*
 * ItemIdIsRedirected
 *		True iff item identifier is in state REDIRECT.
 */
#define ItemIdIsRedirected(itemId) \
	((itemId)->lp_flags == LP_REDIRECT)

/*
 * ItemIdIsDead
 *		True iff item identifier is in state DEAD.
 */
#define ItemIdIsDead(itemId) \
	((itemId)->lp_flags == LP_DEAD)

/*
 * ItemIdHasStorage
 *		True iff item identifier has associated storage.
 */
#define ItemIdHasStorage(itemId) \
	((itemId)->lp_len != 0)

/*
 * ItemIdSetUnused
 *		Set the item identifier to be UNUSED, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetUnused(itemId) \
( \
	(itemId)->lp_flags = LP_UNUSED, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetNormal
 *		Set the item identifier to be NORMAL, with the specified storage.
 *		Beware of multiple evaluations of itemId!
 */
#ifdef DIVA
#define ItemIdSetNormal(itemId, off, len) \
( \
	(itemId)->lp_flags = LP_NORMAL,	\
	(itemId)->lp_off = (off),	\
	(itemId)->lp_len = (len),	\
	(itemId)->lp_siro = (0), \
	(itemId)->lp_pflag = (0) \
)
#else
#define ItemIdSetNormal(itemId, off, len) \
( \
	(itemId)->lp_flags = LP_NORMAL, \
	(itemId)->lp_off = (off), \
	(itemId)->lp_len = (len) \
)
#endif
/*
 * ItemIdSetRedirect
 *		Set the item identifier to be REDIRECT, with the specified link.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetRedirect(itemId, link) \
( \
	(itemId)->lp_flags = LP_REDIRECT, \
	(itemId)->lp_off = (link), \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdSetDead
 *		Set the item identifier to be DEAD, with no storage.
 *		Beware of multiple evaluations of itemId!
 */
#define ItemIdSetDead(itemId) \
( \
	(itemId)->lp_flags = LP_DEAD, \
	(itemId)->lp_off = 0, \
	(itemId)->lp_len = 0 \
)

/*
 * ItemIdMarkDead
 *		Set the item identifier to be DEAD, keeping its existing storage.
 *
 * Note: in indexes, this is used as if it were a hint-bit mechanism;
 * we trust that multiple processors can do this in parallel and get
 * the same result.
 */
#define ItemIdMarkDead(itemId) \
( \
	(itemId)->lp_flags = LP_DEAD \
)

#endif							/* ITEMID_H */
