
#include "postgres.h"
#include "pg_refcnt.h"

#ifdef LOCATOR

#ifdef WAIT_WITH_FUNCTION
void DRefWaitForReaders(pg_atomic_uint64 *dref, uint64 mask)
{
	int limit = 10000000;
	int i = 0;

	for (; i < limit; i++)
	{
		if (!(DRefGetVal(dref) & mask))
			break;
		SPIN_DELAY();
	}
	
	if (i == limit)
	{
		elog(LOG, "DRefWaitForReaders hit limit, dual_ref: %p, val: 0x%16x",
			 dref, dref->value);
		while (true) {sleep(10000);}
	}
}
void DRefSetAndWaitForLeft(pg_atomic_uint64 *dref)
{
	int limit = 10000000;
	int i = 0;

	if ((pg_atomic_fetch_or_u64(dref, DRefTargetLeftBit) & DRefTargetLeftRefCnt) != 0)
	{
		for (; i < limit; i++)
		{
			if ((DRefGetVal(dref) & DRefTargetLeftRefCnt) == 0)
				break;
			SPIN_DELAY();
		}
	}
	
	if (i == limit)
	{
		elog(LOG, "DRefSetAndWaitForLeft hit limit, dual_ref: %p, val: 0x%16x",
			 dref, dref->value);
		while (true) {sleep(10000);}
	}
}
void DRefSetAndWaitForRight(pg_atomic_uint64 *dref)
{
	int limit = 10000000;
	int i = 0;

	if ((pg_atomic_fetch_or_u64(dref, DRefTargetRightBit) & DRefTargetRightRefCnt) != 0)
	{
		for (; i < limit; i++)
		{
			if ((DRefGetVal(dref) & DRefTargetRightRefCnt) == 0)
				break;
			SPIN_DELAY();
		}
	}
	
	if (i == limit)
	{
		elog(LOG, "DRefSetAndWaitForRight hit limit, dual_ref: %p, val: 0x%16x",
			 dref, dref->value);
		while (true) {sleep(10000);}
	}
}
void DRefWaitForINSERT(pg_atomic_uint64 *dref, uint64 oldval)
{
	int limit = 1000000;
	int i = 0;

	for (; i < limit; i++)
	{
		if (DRefGetState(oldval = DRefGetVal(dref)) != DRefStateINSERT)
			break;
		SPIN_DELAY();
	}
	
	if (i == limit)
	{
		elog(LOG, "DRefWaitForINSERT hit limit, dual_ref: %p, val: 0x%16x",
			 dref, dref->value);
		while (true) {sleep(10000);}
	}
}
#endif /* WAIT_WITH_FUNCTION */

#endif /* LOCATOR */