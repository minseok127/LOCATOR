/*-------------------------------------------------------------------------
 *
 * pg_locator_hap_partition_map.c
 *	  routines to support manipulation of the pg_hap_partition_map catalog
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/hap/catalog/pg_locator_hap_partition_map.c
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

#include "locator/hap/hap_catalog.h"

/*
 * Create a single pg_hap_partition_map entry.
 */
void
HapInsertPartMapTuple(Oid relid, Oid partmapid,
					  Oid hashid, Oid ginid)
{
	Datum		values[Natts_pg_locator_hap_partition_map];
	bool		nulls[Natts_pg_locator_hap_partition_map];
	HeapTuple	htup;
	Relation	happartmapRel;

	happartmapRel = table_open(HapPartitionMapRelationId, RowExclusiveLock);

	/* Make a pg_hap entry */
	values[Anum_pg_locator_hap_partition_map_haprelid - 1]
		= ObjectIdGetDatum(relid);
	values[Anum_pg_locator_hap_partition_map_happartmapid - 1]
		= ObjectIdGetDatum(partmapid);
	values[Anum_pg_locator_hap_partition_map_happartmaphashid - 1]
		= ObjectIdGetDatum(hashid);
	values[Anum_pg_locator_hap_partition_map_happartmapginid - 1]
		= ObjectIdGetDatum(ginid);

	/* There is no nullable column */
	memset(nulls, 0, sizeof(nulls));

	htup = heap_form_tuple(RelationGetDescr(happartmapRel), values, nulls);

	CatalogTupleInsert(happartmapRel, htup);

	heap_freetuple(htup);

	table_close(happartmapRel, RowExclusiveLock);
}

/*
 * Delete a single pg_hap_partition_map entry for the dropped relation.
 */
void
HapDeletePartMapTuple(Oid relid)
{
	Relation	happartmapRel;
	ScanKeyData	scanKey;
	SysScanDesc	scanDesc;
	HeapTuple	htup;

	/* Find and delete the pg_hap entry */
	happartmapRel = table_open(HapPartitionMapRelationId, RowExclusiveLock);
	ScanKeyInit(&scanKey,
				Anum_pg_locator_hap_partition_map_haprelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	scanDesc = systable_beginscan(happartmapRel,
								  HapPartitionMapRelIdIndexId,
								  true, NULL, 1, &scanKey);

	/* relId is uniqure on the pg_hap, so loop is unnecessary */
	if (HeapTupleIsValid(htup = systable_getnext(scanDesc)))
		CatalogTupleDelete(happartmapRel, &htup->t_self);

	/* Done */
	systable_endscan(scanDesc);
	table_close(happartmapRel, RowExclusiveLock);
}

/*
 * HapGetPartMapInfo
 *		Using the given relation oid, look up the pg_hap_partition_map entry and
 *		give partition map informations to the caller.
 */
void
HapGetPartMapInfo(Oid relid, Oid *partmapid, Oid *hashid, Oid *ginid)
{
	Form_pg_hap_partition_map happartmap;
	HeapTuple tuple;

	tuple = SearchSysCache1(HAPPARTMAP, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for HAP's partition map");

	happartmap = (Form_pg_hap_partition_map) GETSTRUCT(tuple);
	*partmapid = happartmap->happartmapid;
	*hashid = happartmap->happartmaphashid;
	*ginid = happartmap->happartmapginid;
	ReleaseSysCache(tuple);
}
