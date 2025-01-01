/*-------------------------------------------------------------------------
 *
 * pg_locator.c
 *	  routines to support manipulation of the pg_locator catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/hap/pg_hap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_locator_hap.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "locator/locator_catalog.h"

/*
 * LocatorInsertRelTuple
 *		Create a single pg_locator entry for the new relation.
 *
 * When a table uses LOCATOR as partitioning method, pg_locator entry is
 * created.
 */
void
LocatorInsertRelTuple(Oid relid, int32 part_count,
					  int16 level_count, int16 spread_factor)
{
	Datum		values[Natts_pg_locator];
	bool		nulls[Natts_pg_locator];
	HeapTuple	htup;
	Relation	pglocatorRel;

	pglocatorRel = table_open(LocatorRelationId, RowExclusiveLock);

	/* Make a pg_locator entry */
	values[Anum_pg_locator_locatorrelid - 1]
		= ObjectIdGetDatum(relid);
	values[Anum_pg_locator_locatorpartcount - 1]
		= Int32GetDatum(part_count);
	values[Anum_pg_locator_locatorlevelcount - 1]
		= Int16GetDatum(level_count);
	values[Anum_pg_locator_locatorspreadfactor - 1]
		= Int16GetDatum(spread_factor);

	/* There is no nullable column */
	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(pglocatorRel), values, nulls);

	CatalogTupleInsert(pglocatorRel, htup);

	heap_freetuple(htup);

	table_close(pglocatorRel, RowExclusiveLock);
}

/*
 * Delete a single pg_locator entry for the dropped relation.
 */
void
LocatorDeleteRelTuple(Oid relId)
{
	Relation	pglocatorRel;
	ScanKeyData	scanKey;
	SysScanDesc	scanDesc;
	HeapTuple	htup;

	/* Find and delete the pg_hap entry */
	pglocatorRel = table_open(LocatorRelationId, RowExclusiveLock);
	ScanKeyInit(&scanKey,
				Anum_pg_locator_locatorrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));
	scanDesc = systable_beginscan(pglocatorRel, LocatorRelIdIndexId,
								  true, NULL, 1, &scanKey);

	/* relId is uniqure on the pg_hap, so loop is unnecessary */
	if (HeapTupleIsValid(htup = systable_getnext(scanDesc)))
		CatalogTupleDelete(pglocatorRel, &htup->t_self);

	/* Done */
	systable_endscan(scanDesc);
	table_close(pglocatorRel, RowExclusiveLock);
}

/*
 * LocatorGetTransformationInfo
 *		Get metadata of transformation
 *
 * Lookup the pg_locator_locatorrelid_index using relation oid.
 * Save metadatas to the arguments.
 */
void
LocatorGetTransformationInfo(Oid relid, int32 *part_count,
						 int16 *level_count, int16 *spread_factor)
{
	Form_pg_locator locatortup;
	HeapTuple tuple;

	tuple = SearchSysCache1(LOCATORRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
	{
		*part_count = 0;
		*level_count = 0;
		*spread_factor = 0;
		return;
	}

	locatortup = (Form_pg_locator) GETSTRUCT(tuple);
	*part_count = locatortup->locatorpartcount;
	*level_count = locatortup->locatorlevelcount;
	*spread_factor = locatortup->locatorspreadfactor;

	ReleaseSysCache(tuple);
}

/*
 * Return spread factor.
 */
int16
LocatorGetSpreadFactor(Oid relid)
{
	Form_pg_locator locator;
	HeapTuple tuple;
	int16 ret;

	tuple = SearchSysCache1(LOCATORRELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for locator spread factor");

	locator = (Form_pg_locator) GETSTRUCT(tuple);
	ret = locator->locatorspreadfactor;
	ReleaseSysCache(tuple);

	return ret;
}

/*
 * Return level count.
 */
int16
LocatorGetLevelCount(Oid relid)
{
	Form_pg_locator locator;
	HeapTuple tuple;
	int16 ret;

	tuple = SearchSysCache1(LOCATORRELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for locator spread factor");

	locator = (Form_pg_locator) GETSTRUCT(tuple);
	ret = locator->locatorlevelcount;
	ReleaseSysCache(tuple);

	return ret;
}

/*
 * Check whether the given relation uses LOCATOR or not.
 */
bool
IsLocatorRelId(Oid relid)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(LOCATORRELID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);

	return true;
}

/*
 * Return LOCATOR relation oid list.
 */
List *
LocatorGetRelOidList(void)
{
	List *ret = NIL;
	HeapTuple tuple;
	SysScanDesc scan;
	Relation locatorRel;

	locatorRel = table_open(LocatorRelationId, AccessShareLock);

	scan = systable_beginscan(locatorRel, InvalidOid, false,
							  NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_locator locator = (Form_pg_locator) GETSTRUCT(tuple);

		ret = lappend_oid(ret, locator->locatorrelid);
	}

	systable_endscan(scan);
	table_close(locatorRel, AccessShareLock);

	return ret;
}
