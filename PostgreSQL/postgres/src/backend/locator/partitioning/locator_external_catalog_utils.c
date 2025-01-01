/*-------------------------------------------------------------------------
 *
 * locator_external_catalog_utils.c
 *
 * Locator External Catalog Util Implementation
 *
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * IDENTIFICATION
 *    src/backend/locator/partitioning/locator_external_catalog_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifdef LOCATOR
#include "postgres.h"

#include <math.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/resowner_private.h"

#include "locator/locator.h"
#include "locator/locator_external_catalog.h"

#define MAX_EXTERNAL_CATALOG (32)

/*
 * LocatorGetPartitionNumsInLevel
 */
int 
LocatorGetPartitionNumsForLevel(LocatorExternalCatalog *exCatalog,
								int partitionLevel)
{
	Assert(partitionLevel <= exCatalog->lastPartitionLevel);

	return (int) pow(exCatalog->spreadFactor, partitionLevel + 1);
}

/*
 * LocatorGetPartGenNo
 */
LocatorPartGenNo
LocatorGetPartGenerationNumber(LocatorExternalCatalog *exCatalog, 
							   LocatorPartLevel partitionLevel, 
							   LocatorPartNumber partitionNumber)
{
	LocatorPartGenNo generationNumber;

	/* Partition level zero uses generation number specially. */
	Assert(partitionLevel != 0);

	if (partitionLevel == exCatalog->lastPartitionLevel)
		generationNumber = 0;
	else
		generationNumber = exCatalog->generationNums[partitionLevel][partitionNumber];

	return generationNumber;
}

/*
 * LocatorGetNumRows
 *
 * Return total number of tuples in the relation.
 */
uint64
LocatorGetNumRows(Relation relation)
{
	LocatorExternalCatalog *exCatalog = 
		LocatorGetExternalCatalog(RelationGetRelid(relation));
	uint64 tupleNums = 0;

	for (int i = 0; i < exCatalog->spreadFactor; ++i)
	{
		tupleNums += exCatalog->sequenceNumberCounter[i].val;
	}

	return tupleNums;
}
#endif /* LOCATOR */
