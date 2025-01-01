/*-------------------------------------------------------------------------
 *
 * locator_mempart.c
 *
 * Locator Mempart Implementation
 *
 * IDENTIFICATION
 *    src/backend/locator/mempart/locator_mempart.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "locator/locator_mempart_buf.h"

#ifdef LOCATOR
/*
 * LocatorGetMempartIdFromBufId
 *		Get mempartition id from the given buffer id.
 *
 * LOCATOR's mempartition buffer is located in the end of traditional buffer
 * pool and it start from LocatorMempartStart buffer id.
 *
 * All mempartitions have same size, LocatorMempartPageCount. Using these
 * informations, change the given buffer id to the mempartition id.
 *
 * Note that if the given buf_id is not in the mempartition buffers, return -1.
 */
inline int
LocatorGetMempartIdFromBufId(int buf_id)
{
	if (!LocatorCheckMempartBuf(buf_id))
		return -1;

	return (buf_id - LocatorMempartStart) / LocatorMempartPageCount;
}

/*
 * Return the mempartition's LWLock pointer.
 */
inline LWLock *
LocatorGetMempartLock(int mempart_id)
{
	LWLockPadded *lock = LocatorMempartLocks + mempart_id;
	return &lock->lock;
}

/*
 * LocatorGetMempartBufId
 *		Get mempartition's buffer id.
 *
 * LOCATOR's mempartitions are located in the traditional buffer pool. So
 * mempartitions has buffer id too.
 *
 * Using the given buf_id, calculate mempartition's start buffer id. When the
 * given buf_id is not located in mempartition buffers, return -1.
 */
inline int
LocatorGetMempartBufId(int buf_id)
{
	if (!LocatorCheckMempartBuf(buf_id))
		return -1;

	return LocatorMempartStart +
		(LocatorGetMempartIdFromBufId(buf_id) * LocatorMempartPageCount);
}

/*
 * Return true when the given buffer is located in mempart buffer.
 */
inline bool
LocatorCheckMempartBuf(int buf_id)
{
	return buf_id >= LocatorMempartStart;
}

BlockNumber
LocatorGetMempartBlockCount(RelFileNode node, LocatorPartNumber partNum)
{
	return LocatorNblocks(node, 0, partNum, 0);
}

#else /* !LOCATOR */

inline int
LocatorGetMempartIdFromBufId(int buf_id)
{
	return -1;
}

inline LWLock *
LocatorGetMempartLock(int mempart_id)
{
	return NULL;
}

inline int
LocatorGetMempartBufId(int buf_id)
{
	return -1;
}

/*
 * Return true when the given buffer is located in mempart buffer.
 */
inline bool
LocatorCheckMempartBuf(int buf_id)
{
	return false;
}
#endif /* LOCATOR */
