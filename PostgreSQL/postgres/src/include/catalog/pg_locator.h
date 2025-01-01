/*-------------------------------------------------------------------------
 *
 * pg_locator.h
 *	  definition of the "LOCATOR" system catalog (pg_locator)
 *
 *
 * src/include/catalog/pg_locator.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOCATOR_H
#define PG_LOCATOR_H

#include "catalog/genbki.h"
#include "catalog/pg_locator_d.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------
 *		pg_locator definition.	cpp turns this into
 *		typedef struct FormData_pg_locator
 * ----------------
 */
CATALOG(pg_locator,9986,LocatorRelationId)
{
	Oid			locatorrelid		BKI_LOOKUP(pg_class);
	int32		locatorpartcount	BKI_DEFAULT(0);
	int16		locatorlevelcount	BKI_DEFAULT(0);
	int16		locatorspreadfactor	BKI_DEFAULT(0);
} FormData_pg_locator;

/* ----------------
 *		Form_pg_locator corresponds to a pointer to a tuple with
 *		the format of pg_locator relation.
 * ----------------
 */
typedef FormData_pg_locator *Form_pg_locator;

DECLARE_UNIQUE_INDEX_PKEY(pg_locator_locatorrelid_index,9985,LocatorRelIdIndexId,on pg_locator using btree(locatorrelid oid_ops));

#endif	/* PG_LOCATOR_H */
