/*-------------------------------------------------------------------------
 *
 * pg_locator_hap_partition.h
 *	  definition of the "Hidden Attribute Partitioning" system catalog
 *	  (pg_locator_hap_partition)
 *
 *
 * src/include/catalog/pg_locator_hap_partition_map.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOCATOR_HAP_PARTITION_MAP_H
#define PG_LOCATOR_HAP_PARTITION_MAP_H

#include "catalog/genbki.h"
#include "catalog/pg_locator_hap_partition_map_d.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_locator_hap_partition_map definition.	cpp turns this into
 *		typedef struct FormData_pg_hap_partition_map
 * ----------------
 */
CATALOG(pg_locator_hap_partition_map,9984,HapPartitionMapRelationId)
{
	Oid			haprelid			BKI_LOOKUP(pg_class);
	Oid			happartmapid		BKI_DEFAULT(0);
	Oid			happartmaphashid	BKI_DEFAULT(0);
	Oid			happartmapginid		BKI_DEFAULT(0);
} FormData_pg_locator_hap_partition_map;

/* ----------------
 *		Form_pg_hap_partition_map corresponds to a pointer to a tuple with
 *		the format of pg_hap_partition_map relation.
 * ----------------
 */
typedef FormData_pg_locator_hap_partition_map *Form_pg_hap_partition_map;

DECLARE_UNIQUE_INDEX_PKEY(pg_locator_hap_partition_map_haprelid_index,9983,HapPartitionMapRelIdIndexId,on pg_locator_hap_partition_map using btree(haprelid oid_ops));

#endif	/* PG_LOCATOR_HAP_PARTITION_MAP_H */
