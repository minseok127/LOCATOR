/*-------------------------------------------------------------------------
 *
 * locator_catalog.c
 *	  routines to support LOCATOR related catalogs.
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/locator_catalog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/array.h"

#include "locator/locator_catalog.h"

/*
 * LocatorDeleteCatalogTuples
 *		Remove LOCATOR related catalog tuples.
 *
 * When a table is dropped, all related HAP catalog entries are deleted by this
 * function.
 */
void
LocatorDeleteCatalogTuples(Oid relid)
{
	/* pg_locator */
	LocatorDeleteRelTuple(relid);
}

/*
 * LocatorUpdateCatalogPartitioningFlag
 *		Update pg_class to record locator's online-partitioing flag.
 *
 * Record this table uses locator's online-partitioning flag.
 */
static void
LocatorUpdateCatalogPartitioningFlag(Oid relid)
{
	Form_pg_class rd_rel;
	Relation pg_class;
	HeapTuple tuple;

	/* Get a modifiable copy of the relation's pg_class row */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	rd_rel = (Form_pg_class) GETSTRUCT(tuple);

	/* Update the pg_class row */
	rd_rel->rellocatorpartitioning = true;
	rd_rel->relam = get_table_am_oid("locator", false);
	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	/* Done */
	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * LocatorRegisterRel
 * 		Initialize locator catalog to use online-partitioning.
 *
 * If the HAP module wants to use online-partitioning, it must register metadata
 * to the locator's catalog.
 *
 * Before registering, translate DefElem and get the level info too.
 */
void
LocatorRegisterRel(Oid relid, int part_count, DefElem *spread_factor_def)
{
	int16 level_count = 1, spread_factor = LOCATOR_DEFAULT_SPREAD_FACTOR;
	int tmp_part_count;

	if (spread_factor_def)
	{
		if (!IsA(spread_factor_def->arg, Integer))
			elog(ERROR, "Locator's spread factor must be a int");

		spread_factor = ((Integer *) spread_factor_def->arg)->ival;
	}

	if (spread_factor > part_count)
		spread_factor = part_count;

	/* The partition # of level zero should be equal to spread factor. */
	tmp_part_count = spread_factor;

	/* Calculate level of LOCATOR relation */
	while (tmp_part_count < part_count)
	{
		level_count++;
		tmp_part_count *= spread_factor;
	}

	LocatorInsertRelTuple(relid, part_count, level_count, spread_factor);

	LocatorUpdateCatalogPartitioningFlag(relid);
}
