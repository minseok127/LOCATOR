/*-------------------------------------------------------------------------
 *
 * locator_catalog.h
 *	  POSTGRES LOCATOR catalogs.
 *
 *
 * src/include/locator/locator_catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_CATALOG_H
#define LOCATOR_CATALOG_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

#include "catalog/pg_locator.h"

/* common */

#define LOCATOR_DEFAULT_SPREAD_FACTOR (16)

extern void LocatorDeleteCatalogTuples(Oid relid);

extern void LocatorRegisterRel(Oid relid,
							   int part_count,
							   DefElem *locator_spread_factor);


/* pg_locator */

extern void LocatorInsertRelTuple(Oid relid,
								  int32 part_count,
								  int16 level_count,
								  int16 spread_factor);

extern void LocatorDeleteRelTuple(Oid relid);

extern void LocatorGetTransformationInfo(Oid relid,
									 int32 *part_count,
									 int16 *level_count,
									 int16 *spread_factor);

extern int16 LocatorGetSpreadFactor(Oid relid);

extern int16 LocatorGetLevelCount(Oid relid);

extern bool IsLocatorRelId(Oid relid);

extern List *LocatorGetRelOidList(void);

#endif	/* LOCATOR_CATALOG_H */
