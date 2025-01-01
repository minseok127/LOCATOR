/*-------------------------------------------------------------------------
 *
 * locator_external_catalog.c
 *
 * Locator External Catalog Implementation
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
 *    src/backend/locator/partitioning/locator_external_catalog.c
 *
 *-------------------------------------------------------------------------
 */
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
#include "miscadmin.h"
#include "common/file_perm.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "access/table.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/rel.h"
#include "utils/resowner_private.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"

#ifdef LOCATOR
#include "locator/locator.h"
#include "locator/locator_external_catalog.h"
#include "locator/locator_catalog.h"
#include "locator/locator_mempart_buf.h"
#include "locator/locator_partitioning.h"

#include "access/heaptoast.h" /* for maxlen of column group */
#include "access/htup_details.h"

#define MAX_EXTERNAL_CATALOG (32)

/* External catalog structures. */
LocatorExternalCatalogPadded *LocatorExternalCatalogs;
LocatorExternalCatalogGlobalMetaData *LocatorExternalCatalogGlobalMeta;

/* src/include/locator/locator_external_catalog.h */
dsa_area* external_catalog_dsa_area;

static inline void
InitColumnarLayout(LocatorExternalCatalog *exCatalog, Relation relation)
{
	exCatalog->layoutArray[exCatalog->lastPartitionLevel] = 0;
	*(exCatalog->columnarLayoutArray[0].recordsPerRecordZone) =
		relation->records_per_block;
	exCatalog->columnarLayoutArray[0].blocksPerRecordZone[0] = 1;
	exCatalog->columnarLayoutArray[0].blocksPerRecordZone[1] = 1;
	*(exCatalog->columnarLayoutArray[0].columnGroupCount) = 1;
	exCatalog->columnarLayoutArray[0].columnGroupMetadataArray[0].tupleSize =
		relation->rd_att->maxlen;
	exCatalog->columnarLayoutArray[0].columnGroupMetadataArray[0].tuplesPerBlock =
		relation->records_per_block;
}

/*
 * LocatorExternalCatalogShmemSize 
 *
 * This function returns share memoey size for locator external catalog. 
 */
Size
LocatorExternalCatalogShmemSize(void)
{
	Size size = 0;

	/* locator external catalog */
	size = add_size(size, mul_size(MAX_EXTERNAL_CATALOG, 
									sizeof(LocatorExternalCatalogPadded)));

	/* To allow aligning external catalog descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* locator external catalog hash */
	size = add_size(size,
		LocatorExternalCatalogHashShmemSize(MAX_EXTERNAL_CATALOG + 
										NUM_LOCATOR_EXTERNAL_CATALOG_PARTITIONS));

	/* locator external catalog global meta data */
	size = add_size(size, sizeof(LocatorExternalCatalogGlobalMetaData));

	return size;
}


/*
 * LocatorExternalCatalogInit
 *
 * Locator initializes the external catalog structure at the start of the 
 * database.
 */
void
LocatorExternalCatalogInit(void)
{
	bool foundExCatalog, foundGlobalMetaData;
	LocatorExternalCatalog *exCatalog;
	char external_catalog_dir[32];

	/* Align descriptors to a cacheline boundary */
	LocatorExternalCatalogs = (LocatorExternalCatalogPadded *) ShmemInitStruct(
			"locator external catalogs",
			MAX_EXTERNAL_CATALOG * sizeof(LocatorExternalCatalogPadded),
			&foundExCatalog);

	LocatorExternalCatalogHashInit(MAX_EXTERNAL_CATALOG + 
									NUM_LOCATOR_EXTERNAL_CATALOG_PARTITIONS);

	/* Initialize external catalogs */
	if (!foundExCatalog)
	{
		for (int i = 0; i < MAX_EXTERNAL_CATALOG; i++)
		{
			exCatalog = ExCatalogIndexGetLocatorExternalCatalog(i);

			/* Init values. */
			exCatalog->tag.relationOid = 0;
			exCatalog->exCatalogId = i;
			exCatalog->inited = false;
			exCatalog->partitioningLevel = InvalidLocatorPartLevel;
			exCatalog->partitioningNumber = InvalidLocatorPartNumber;
			exCatalog->partitioningMinTieredNumber = InvalidLocatorTieredNumber;
			exCatalog->partitioningMaxTieredNumber = InvalidLocatorTieredNumber;
		}
	}

	LocatorExternalCatalogGlobalMeta = (LocatorExternalCatalogGlobalMetaData *) 
										ShmemInitStruct(
											"locator external catalog global meta data", 
											sizeof(LocatorExternalCatalogGlobalMetaData), 
											&foundGlobalMetaData);

	/* Initialize external catalog global meta data */
	if (!foundGlobalMetaData)
	{
		LocatorExternalCatalogGlobalMeta->externalCatalogNums = 0;
		LWLockInitialize(
			(LWLock*)(&LocatorExternalCatalogGlobalMeta->globalMetaLock),
			LWTRANCHE_LOCATOR_PARTITION);
	}

	/* Create external catalog directory */
	snprintf(external_catalog_dir, 32, "external_catalog");
	if (pg_mkdir_p(external_catalog_dir, pg_dir_create_mode) != 0 &&
		errno != EEXIST)
		elog(ERROR, "Error creating external_catalog directory");
}


/*
 * LocatorGetExternalCatalog
 *
 * Locator maintains external catalog per relation for utilization in the buffer
 * cache. If external catalog has been created prior to calling this function,
 * the caller receives the external catalog of the relation. 
 */
LocatorExternalCatalog *
LocatorGetExternalCatalog(Oid relationOid)
{
	LocatorExternalCatalog *exCatalog;
	LocatorExternalCatalogTag tag;		/* identity of requested relation */
	LWLock *partitionLock;			/* partition lock for target */
	uint32 hashcode;
	int exCatalogId;

	if (unlikely(relationOid == InvalidOid))
		return NULL;

	tag.relationOid = relationOid;

	/* Calculate hash code */
	hashcode = LocatorExternalCatalogHashCode(&tag);
	partitionLock = LocatorExternalCatalogMappingPartitionLock(hashcode);

	LWLockAcquire(partitionLock, LW_SHARED);
	exCatalogId = LocatorExternalCatalogHashLookup(&tag, hashcode);
	if (exCatalogId >= 0)
	{
		LWLockRelease(partitionLock);

		exCatalog = ExCatalogIndexGetLocatorExternalCatalog(exCatalogId);
		Assert(exCatalog->tag.relationOid == relationOid);
		return exCatalog;
	}

	LWLockRelease(partitionLock);

	/* There is no hash table entry yet, create one */
	if (IsLocatorRelId(relationOid))
		return LocatorCreateExternalCatalog(relationOid);

	/* The given relation does not use LOCATOR */
	return NULL;
}

/*
 * LocatorPopulateExternalCatalog
 *
 * We read the external catalog file and set pointers to reference it. If the 
 * external catalog file doesn't exist, the external catalog is initialized 
 * with zero values.
 */
static void
LocatorPopulateExternalCatalog(LocatorExternalCatalog *exCatalog)
{
	Oid relationOid = exCatalog->tag.relationOid;
	int64_t currentOffset = 0;
	Relation relation;
	int level;
	bool needInit;

	/* For logging */
	uint32 mega;
	uint32 kilo;
	uint32 bytes;

	Assert(exCatalog->inited == false);

	exCatalog->spreadFactor = LocatorGetSpreadFactor(relationOid);
	exCatalog->lastPartitionLevel = LocatorGetLevelCount(relationOid) - 1;
	pg_atomic_init_u32(&(exCatalog->partitioningWorkerCounter), BlockCheckpointerBit);

	relation = table_open(relationOid, AccessShareLock);

	/* For checkpointer */
	exCatalog->smgr.smgr_rnode.node = RelationGetSmgr(relation)->smgr_rnode.node;
	exCatalog->smgr.smgr_rnode.backend = InvalidBackendId;

	memcpy(&(exCatalog->tupDesc), relation->rd_att,
		   offsetof(struct TupleDescData, attrs));

	exCatalog->relation.is_siro = relation->is_siro;
	exCatalog->relation.rd_locator_partitioning = relation->rd_locator_partitioning;
	exCatalog->relation.rd_node = relation->rd_node;

	exCatalog->relation.rd_smgr = &(exCatalog->smgr);
	exCatalog->relation.rd_att = &(exCatalog->tupDesc);

	/* Check existing external catalog file */
	if (LocatorExternalCatalogFileExists(relationOid))
	{
		/* Stored file exists. */
		LocatorReadExternalCatalogFile(relationOid, exCatalog->block);	
		needInit = false;
	}
	else
	{
		/* Stored file doesn't exists. */
		memset(exCatalog->block, 0, EXTERNAL_CATALOG_BLOCKSIZE);
		needInit = true;

		/* Create new external catalog file and fill zeros. */
		LocatorCreateExternalCatalogFile(relationOid);
	}

	/* Set sequence number counter pointer. */
	exCatalog->sequenceNumberCounter = 
							(LocatorUint64Padded *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorUint64Padded) * exCatalog->spreadFactor);

	/* Set block counter pointer. */
	exCatalog->nblocks = (pg_atomic_uint64 *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(pg_atomic_uint64) * 1);

	/* Set last repartitioning's max partition number in level zero. */
	exCatalog->PreviousTieredNumberLevelZero = 
							(LocatorTieredNumber *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorTieredNumber) * exCatalog->spreadFactor);

	/* Set current max partition number in level zero. */
	exCatalog->CurrentTieredNumberLevelZero = 
							(LocatorTieredNumber *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorTieredNumber) * exCatalog->spreadFactor);

	/* Set current record # in level zero. */
	exCatalog->CurrentRecordCountsLevelZero = 
							(LocatorUint64Padded *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorUint64Padded) * exCatalog->spreadFactor);

	/* Set tiered record # in level zero. */
	exCatalog->TieredRecordCountsLevelZero = 
							(LocatorUint64Padded *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorUint64Padded) * exCatalog->spreadFactor);

	/* Set reorganized record # in level zero. */
	exCatalog->ReorganizedRecordCountsLevelZero = 
							(LocatorUint64Padded *) (exCatalog->block + currentOffset);
	currentOffset += (int) (sizeof(LocatorUint64Padded) * exCatalog->spreadFactor);

	/* Set generation number's pointer to stored block. */
	for (level = 0; level <= exCatalog->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (level == 0 || level == exCatalog->lastPartitionLevel)
		{
			/* We don't maintain last levels. */
 			currentPartitionNums = 0;

			exCatalog->generationNums[level] = NULL;
		}
		else
		{
			/* Get partition numbers in the level. */
			currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

			exCatalog->generationNums[level] = 
						(LocatorPartGenNo *) (exCatalog->block + currentOffset);

			if (needInit)
			{
				memset(exCatalog->generationNums[level], -1, 
							sizeof(LocatorPartGenNo) * currentPartitionNums);
			}
		}

		currentOffset += (int) (sizeof(LocatorPartGenNo) * currentPartitionNums);
	}

	/* Set sequence number's pointer to stored block. */
	for (level = 0; level <= exCatalog->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (level == 0 || level == exCatalog->lastPartitionLevel)
		{
			/* We don't maintain last levels. */
 			currentPartitionNums = 0;

			exCatalog->locationInfo[level] = NULL;
		}
		else
		{
			/* Get partition numbers in the level. */
			currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

			exCatalog->locationInfo[level] = 
						(LocationInfo *) (exCatalog->block + currentOffset);

			if (needInit)
			{
				for (int part = 0; part < currentPartitionNums; ++part)
				{
					exCatalog->locationInfo[level][part].firstSequenceNumber = 0xFFFFFFFFFF;
					exCatalog->locationInfo[level][part].reorganizedRecordsCount = 0;
				}
			}
		}

		currentOffset += (int) (sizeof(uint64_t) * currentPartitionNums);
	}

	/* Set real record count's pointer to stored block. */
	for (level = 0; level <= exCatalog->lastPartitionLevel; ++level)
	{
		int currentPartitionNums;

		if (unlikely(level == 0))
		{
			/* We don't maintain first levels. */
 			currentPartitionNums = 0;

			exCatalog->realRecordsCount[level] = NULL;
		}
		else
		{
			/* Get partition numbers in the level. */
			currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

			exCatalog->realRecordsCount[level] = 
						(uint64_t *) (exCatalog->block + currentOffset);
		}

		currentOffset += (int) (sizeof(uint64_t) * currentPartitionNums);
	}

	/* Set cumulative record count's pointer to stored block. */
	{
		int currentPartitionNums;

		/* Cumulative count doesn't be used at level 0 */
		exCatalog->internalCumulativeCount[0] = NULL;

		/*
		 * Cumulative counts of internal levels allow wrap-around, so the size
		 * of variable can be 4-bytes.
		 */
		for (level = 1; level < exCatalog->lastPartitionLevel; ++level)
		{
			currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

			exCatalog->internalCumulativeCount[level] = 
						(LocatorInPartCnt *) (exCatalog->block + currentOffset);

			currentOffset += (int) (sizeof(LocatorInPartCnt) * currentPartitionNums);
		}

		currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

		/*
		 * Cumulative count of last level don't allow wrap-around, so the size
		 * of variable must be larger than 4-bytes.
		 */
		exCatalog->lastCumulativeCount =
			(LocatorSeqNum *) (exCatalog->block + currentOffset);

		currentOffset += (int) (sizeof(LocatorSeqNum) * currentPartitionNums);
	}

	/* Set the columnar layouts */
	{
		int columnarLayoutMaxIndex;
		int natts = relation->rd_att->natts;

		exCatalog->layoutArray = (uint8*) (exCatalog->block + currentOffset);
		columnarLayoutMaxIndex = exCatalog->layoutArray[exCatalog->lastPartitionLevel];
		currentOffset += (int) (sizeof(uint8) * exCatalog->lastPartitionLevel + 1);

		/* Set default layout (ROW-ORIENTED) */
		{
			exCatalog->columnarLayoutArray[0].recordsPerRecordZone =
				(uint32*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(uint32));

			exCatalog->columnarLayoutArray[0].blocksPerRecordZone =
				(uint32*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(uint32) * 2);

			exCatalog->columnarLayoutArray[0].columnGroupCount =
				(uint8*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(uint8));

			exCatalog->columnarLayoutArray[0].columnGroupArray =
				(uint8*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(uint8) * natts);

			exCatalog->columnarLayoutArray[0].columnGroupMetadataArray =
				(ColumnGroupMetadata*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(ColumnGroupMetadata));
		}

		if (needInit)
		{
			/* Init the default layout */
			InitColumnarLayout(exCatalog, relation);
		}

		/* If the columnar layout is already set, read it in */
		if (columnarLayoutMaxIndex != 0)
		{
			int columnGroupCount;

			for (int i = 1; i <= columnarLayoutMaxIndex; i++)
			{
				exCatalog->columnarLayoutArray[i].recordsPerRecordZone =
					(uint32*) (exCatalog->block + currentOffset);
				currentOffset += (int) (sizeof(uint32));

				exCatalog->columnarLayoutArray[i].blocksPerRecordZone =
					(uint32*) (exCatalog->block + currentOffset);
				currentOffset += (int) (sizeof(uint32) * 2);

				exCatalog->columnarLayoutArray[i].columnGroupCount =
					(uint8*) (exCatalog->block + currentOffset);
				currentOffset += (int) (sizeof(uint8));

				exCatalog->columnarLayoutArray[i].columnGroupArray =
					(uint8*) (exCatalog->block + currentOffset);
				currentOffset += (int) (sizeof(uint8) * natts);

				exCatalog->columnarLayoutArray[i].columnGroupMetadataArray =
					(ColumnGroupMetadata*) (exCatalog->block + currentOffset);
				columnGroupCount =
					*(exCatalog->columnarLayoutArray[i].columnGroupCount);
				currentOffset += (int) (sizeof(ColumnGroupMetadata) * columnGroupCount);
			}
		}
		else
		{
			/* For the future columnar layout setting */
			exCatalog->columnarLayoutArray[1].recordsPerRecordZone =
				(uint32*) (exCatalog->block + currentOffset);
			currentOffset += (int) (sizeof(uint32));
		}
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	ereport(LOG, (errmsg("external catalog was populated, rel id: %u, size: %u MiB %u KiB %u B",
						 relationOid, mega, kilo, bytes)));

	Assert(currentOffset <= EXTERNAL_CATALOG_BLOCKSIZE);

	/* Init locks */
    for (int i = 0; i < MAX_SPREAD_FACTOR; ++i)
    {
	    LWLockInitialize((LWLock *) (&exCatalog->mempartitionLock[i]), LWTRANCHE_LOCATOR_PARTITION);
    }

	/* Set ref counters' pointer. */
	currentOffset = 0;
	for (level = 0; level <= exCatalog->lastPartitionLevel; ++level)
	{
		int currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

		exCatalog->refcounter[level] = 
			(LocatorAtomicUint64Padded *) (exCatalog->refcounter_space + currentOffset);

		currentOffset += (int) (sizeof(LocatorAtomicUint64Padded) * currentPartitionNums);
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	ereport(LOG, (errmsg("refcounter, rel id: %u, size: %u MiB %u KiB %u B",
						 relationOid, mega, kilo, bytes)));

	Assert(currentOffset <= EXTERNAL_CATALOG_REFCOUNTERSIZE);

	/* Set partition xmax' pointer. */
	currentOffset = 0;
	for (level = 0; level <= exCatalog->lastPartitionLevel; ++level)
	{
		int currentPartitionNums = LocatorGetPartitionNumsForLevel(exCatalog, level);

		exCatalog->partitionXmax[level] = 
			(TransactionId *) (exCatalog->partition_xmax_space + currentOffset);

		for (int part = 0; part < currentPartitionNums; ++part)
			exCatalog->partitionXmax[level][part] = FirstNormalTransactionId;

		currentOffset += (int) (sizeof(TransactionId) * currentPartitionNums);
	}

	mega = currentOffset;
	
	bytes = mega % 1024;
	mega /= 1024;

	kilo = mega % 1024;
	mega /= 1024;

	ereport(LOG, (errmsg("xmax, rel id: %u, size: %u MiB %u KiB %u B",
						 relationOid, mega, kilo, bytes)));

	Assert(currentOffset <= EXTERNAL_CATALOG_PARTITION_XMAXSIZE);

	/* Get values from 'Relation'. */
	exCatalog->recordSize = relation->record_size;
	exCatalog->recordNumsPerBlock = relation->records_per_block;
	table_close(relation, AccessShareLock);

	if (needInit)
		LocatorWriteExternalCatalogFile(relationOid, exCatalog->block);

	exCatalog->inited = true;
}

/*
 * LocatorCreateExternalCatalog
 * 
 * Locator creates external catalog for relations during the creation or
 * alteration of tables. While the creation logic does not need to account for
 * concurrency issues, we employ locks for accessing it.
 */
LocatorExternalCatalog *
LocatorCreateExternalCatalog(Oid relationOid)
{
	LocatorExternalCatalog *exCatalog;
	LocatorExternalCatalogTag tag;
	Index candidateExCatalogId;
	LWLock *partitionLock;			/* partition lock for target */
	uint32 hashcode;

	tag.relationOid = relationOid;

	/* Calcuate hash code */
	hashcode = LocatorExternalCatalogHashCode(&tag);
	partitionLock = LocatorExternalCatalogMappingPartitionLock(hashcode);

	LWLockAcquire((LWLock*)(&LocatorExternalCatalogGlobalMeta->globalMetaLock), LW_EXCLUSIVE);
	LWLockAcquire(partitionLock, LW_EXCLUSIVE);

	for (int i = 0; i < LocatorExternalCatalogGlobalMeta->externalCatalogNums; ++i)
	{
		exCatalog = ExCatalogIndexGetLocatorExternalCatalog(i);

		if (exCatalog->tag.relationOid == relationOid)
		{
			/* Exists arleady. */
			LWLockRelease(partitionLock);
			LWLockRelease((LWLock*)(&LocatorExternalCatalogGlobalMeta->globalMetaLock));
	
			return exCatalog;
		}
	}

	/* 
	 * Pick up a empty external catalog for a new allocation 
	 */
	candidateExCatalogId = LocatorExternalCatalogGlobalMeta->externalCatalogNums;

	if (MAX_EXTERNAL_CATALOG <= LocatorExternalCatalogGlobalMeta->externalCatalogNums)
	{
		LWLockRelease(partitionLock);

		elog(WARNING, 
			"we cannot create a new external catalog due to limitations.");

		return NULL;
	}

	exCatalog = ExCatalogIndexGetLocatorExternalCatalog(candidateExCatalogId);
	Assert(candidateExCatalogId == exCatalog->exCatalogId);

	/* Init external catalog. */
	exCatalog->tag = tag;

	/* 
	 * If an existing file exists, we read the external catalog. If not, we 
	 * populate new external catalog.
	 */
	LocatorPopulateExternalCatalog(exCatalog);

	/* Insert hash element to hashtable. */
	LocatorExternalCatalogHashInsert(&tag, hashcode, candidateExCatalogId);
	LocatorExternalCatalogGlobalMeta->externalCatalogNums += 1;

	LWLockRelease(partitionLock);
	LWLockRelease((LWLock*)(&LocatorExternalCatalogGlobalMeta->globalMetaLock));

	return exCatalog;
}

/*
 * LocatorFlushExternalCatalog
 *
 * Locator records the external catalog block in the external catalog file of
 * the relation.
 */
void
LocatorFlushExternalCatalog(LocatorExternalCatalog *exCatalog)
{
	LWLock *exCatalogMempartitionLock;

	if (unlikely(exCatalog->tag.relationOid == InvalidOid))
		return;

	/* We lock all external catalog mempartition lock here. */
	for (int partNumLevelZero = 0;
				partNumLevelZero < exCatalog->spreadFactor; ++partNumLevelZero)
	{
		exCatalogMempartitionLock = 
			LocatorExternalCatalogMemPartitionLock(exCatalog, partNumLevelZero);

		LWLockAcquire(exCatalogMempartitionLock, LW_SHARED);
	}

	LocatorWriteExternalCatalogFile(exCatalog->tag.relationOid, exCatalog->block);

	for (int partNumLevelZero = 0;
				partNumLevelZero < exCatalog->spreadFactor; ++partNumLevelZero)
	{
		exCatalogMempartitionLock = 
			LocatorExternalCatalogMemPartitionLock(exCatalog, partNumLevelZero);

		LWLockRelease(exCatalogMempartitionLock);
	}
}

/*
 * LocatorFlushAllExternalCatalogs
 *
 * Flush all external catalog currently opened.
 */
void
LocatorFlushAllExternalCatalogs(void)
{
	for (int i = 0; i < LocatorExternalCatalogGlobalMeta->externalCatalogNums; i++)
	{
		LocatorFlushExternalCatalog(ExCatalogIndexGetLocatorExternalCatalog(i));
	}
}

/*
 * LocatorCheckAllExternalCatalogRecordsCount
 * 
 * Print the sequence number and records count of all external catalogs.
 */
void
LocatorCheckAllExternalCatalogRecordsCount(void)
{
	LocatorExternalCatalog *exCatalog;
	LWLock *exCatalogMempartitionLock[32];
	int lastLevel;
	int spreadFactor;
	ColumnarLayout *columnarLayout;
	uint32 recordsPerRecordZone;
	uint32 blocksPerRecordZone;
	uint64 recCntSum;
	uint64 recCnt_lvl0_real[32] = { 0 };
	uint64 recCnt_lvl0_tiered[32] = { 0 };
	uint64 recCnt_lvl0_reorganized[32] = { 0 };
	uint64 recCnt_lvl0_real_sum = 0;
	uint64 recCnt_lvl0_tiered_sum = 0;
	uint64 recCnt_lvl0_reorganized_sum = 0;
	uint64 recCntLvlSum[32] = { 0 };
	uint64 seqNum[32] = { 0 };
	uint64 seqNum_sum;
	uint64 *levelRealRecCnt;
	uint64 nblocks;
	uint64 nblocksLvl[32] = { 0 };
	uint64 tmpCnt;

	/* 
	 * TODO: This function is deprecated, we need to modify this function 
	 * to support new mempartition policy.
	 */

	pg_atomic_uint64 *refcounter;
	uint32 ref_oldval;

	for (int m = 0; m < LocatorExternalCatalogGlobalMeta->externalCatalogNums; m++)
	{
		exCatalog = ExCatalogIndexGetLocatorExternalCatalog(m);
		lastLevel = exCatalog->lastPartitionLevel;
		spreadFactor = exCatalog->spreadFactor;

		for (int tmp = 0; tmp < spreadFactor; ++tmp)
		{
			exCatalogMempartitionLock[tmp] =
				LocatorExternalCatalogMemPartitionLock(exCatalog, tmp);
		}

		for (int tmp = 0; tmp < 32; ++tmp)
			recCntLvlSum[tmp] = nblocksLvl[tmp] = 0;

		tmpCnt = 0;
		seqNum_sum = 0;
		recCntSum = 0;
		recCnt_lvl0_real_sum = 0;
		recCnt_lvl0_tiered_sum = 0;
		recCnt_lvl0_reorganized_sum = 0;

		fprintf(stderr, "[LOCATOR] relation oid: %u\n", exCatalog->tag.relationOid);

		/* Acquire lock, and increment refcounts */
		for (int tmp = 0; tmp < spreadFactor; ++tmp)
			LWLockAcquire(exCatalogMempartitionLock[tmp], LW_SHARED);

		for (int level = 0; level <= lastLevel; ++level)
		{
			for (LocatorPartNumber part_num = 0;
				 part_num < (LocatorPartNumber) pow(spreadFactor, level+1);
				 ++part_num)
			{
				/* Don't use MACRO for log */
				refcounter = LocatorExternalCatalogRefCounter(exCatalog, level, part_num);
retry:
				WaitForMetaDataBit(refcounter);
				ref_oldval = pg_atomic_fetch_add_u64(refcounter, MetaDataCount);
				if (unlikely((ref_oldval & MetaDataBit) == 1))
				{
					DecrMetaDataCount(refcounter);
					goto retry;
				}

				if (ref_oldval)
					fprintf(stderr, "[LOCATOR] level: %u, partNum: %u, refcount: 0x%08x\n", level, part_num, ref_oldval);
			}
		}

		nblocks = pg_atomic_read_u64(exCatalog->nblocks);
		columnarLayout = exCatalog->columnarLayoutArray;
		recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
		for (int i = 0; i < spreadFactor; i++)
		{
			seqNum[i] = exCatalog->sequenceNumberCounter[i].val;
			seqNum_sum += seqNum[i];

			recCnt_lvl0_real[i] = exCatalog->CurrentRecordCountsLevelZero[i].val;
			recCnt_lvl0_tiered[i] = exCatalog->TieredRecordCountsLevelZero[i].val;
			recCnt_lvl0_reorganized[i] = exCatalog->ReorganizedRecordCountsLevelZero[i].val;
			recCnt_lvl0_real_sum += recCnt_lvl0_real[i];
			recCnt_lvl0_tiered_sum += recCnt_lvl0_tiered[i];
			recCnt_lvl0_reorganized_sum += recCnt_lvl0_reorganized[i];
			tmpCnt = (recCnt_lvl0_real[i] + recCnt_lvl0_tiered[i]);
			recCntSum += tmpCnt;
			if (tmpCnt != 0) /* nblocks == nrecordzones in level 0 */
			{
				nblocksLvl[0] += 
					NRecordZonesUsingRecordCount(tmpCnt, recordsPerRecordZone);
			}
		}
		recCntLvlSum[0] = recCntSum;
		
		for (int level = 1; level <= lastLevel; ++level)
		{
			levelRealRecCnt = exCatalog->realRecordsCount[level];
			columnarLayout = exCatalog->columnarLayoutArray + exCatalog->layoutArray[level];
			recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
			blocksPerRecordZone = columnarLayout->blocksPerRecordZone[level == lastLevel ? 1 : 0];

			for (LocatorPartNumber part_num = 0;
				 part_num < (LocatorPartNumber) pow(spreadFactor, level+1);
				 ++part_num)
			{
				tmpCnt = levelRealRecCnt[part_num];
				recCntSum += tmpCnt;
				recCntLvlSum[level] += tmpCnt;

				if (tmpCnt != 0)
					nblocksLvl[level] += NRecordZonesUsingRecordCount(tmpCnt, recordsPerRecordZone) * blocksPerRecordZone;
			}
		}

		/* Release lock, and decrement refcounts */
		for (int level = lastLevel; level >= 0; --level)
		{
			for (LocatorPartNumber part_num = 0;
				 part_num < (LocatorPartNumber) pow(spreadFactor, level+1);
				 ++part_num)
			{
				DecrMetaDataCount(LocatorExternalCatalogRefCounter(exCatalog, level, part_num));
			}
		}

		for (int tmp = 0; tmp < spreadFactor; ++tmp)
			LWLockRelease(exCatalogMempartitionLock[tmp]);

		fprintf(stderr, "[LOCATOR] seqNum: %lu, sum: %lu (nblocks: %lu)\n", seqNum_sum, recCntSum, nblocks);
		fprintf(stderr, "[LOCATOR] level 0, real: %lu, tiered: %lu, reorganized: %lu, sum: %lu\n", recCnt_lvl0_real_sum, recCnt_lvl0_tiered_sum, recCnt_lvl0_reorganized_sum, recCnt_lvl0_real_sum+recCnt_lvl0_tiered_sum+recCnt_lvl0_reorganized_sum);
		for (int i = 0; i < spreadFactor; i++)
			fprintf(stderr, "[LOCATOR] level 0, part: %d, seqNum: %lu, real: %lu, tiered: %lu, reorganized: %lu, sum: %lu\n", i, seqNum[i], recCnt_lvl0_real[i], recCnt_lvl0_tiered[i], recCnt_lvl0_reorganized[i], recCnt_lvl0_real[i]+recCnt_lvl0_tiered[i]+recCnt_lvl0_reorganized[i]);
		for (int level = 0; level <= lastLevel; ++level)
			fprintf(stderr, "[LOCATOR] level: %d, sum: %lu (nblocks: %lu)\n", level, recCntLvlSum[level], nblocksLvl[level]);
	}
}

/*
 * SetRepartitioningStatusInprogress
 */
void
SetRepartitioningStatusInprogress(pg_atomic_uint64 *refcounter)
{
	/* This partition should not be in repartitioning. */
	if (pg_atomic_fetch_or_u64(refcounter, REPARTITIONING_INPROGRESS) &
			 REPARTITIONING_FLAG_MASK)
		elog(ERROR, "SetRepartitioningStatusInprogress error");

	/* Wait normal modifications. */
	WaitForNormalModificationDone(refcounter);
}

/*
 * SetRepartitioningStatusModificationBlocking
 */
void
SetRepartitioningStatusModificationBlocking(pg_atomic_uint64 *refcounter)
{
	/* This partition should already have the repartitioning inprogress flag. */
	if ((pg_atomic_fetch_or_u64(refcounter, REPARTITIONING_MODIFICATION_BLOCKING) &
			REPARTITIONING_FLAG_MASK) != REPARTITIONING_INPROGRESS)
		elog(ERROR, "SetRepartitioningStatusModificationBlocking error");

	/* Wait modifications with delta. */
	WaitForModificationWithDeltaDone(refcounter);
}

/*
 * SetRepartitioningStatusDone
 */
void
SetRepartitioningStatusDone(pg_atomic_uint64 *refcounter)
{
	Assert(GetModificationWithDeltaCount(refcounter) == 0);	

	/* Unset all repartitioning status flags. */
	pg_atomic_fetch_and_u64(refcounter, 
			~(REPARTITIONING_INPROGRESS | 
				REPARTITIONING_MODIFICATION_BLOCKING | MetaDataBit));
}

/*
 * GetModificationMethod
 * 
 * Modification workers need to get their method to modify tuples. So, this 
 * function gets target partition's refcounter and returns appropriate 
 * modification method.
 */
LocatorModificationMethod
GetModificationMethod(LocatorExternalCatalog *exCatalog, pg_atomic_uint64 *refcounter,
					  LocatorPartLevel targetPartitionLevel, 
					  LocatorPartNumber targetPartitionNumber)
{
	uint64 refcntForChecking = 0;
	uint64 refcnt;

	/* The caller increments external catalog data counter. */
	Assert(GetMetaDataCount(refcounter) > 0);

retry_get_modification_method:
	/*
	 * Corner case 
	 * The repartitioning worker does not perform repartitioning on the 
	 * update worker's target in level zero.
	 */
	if (targetPartitionLevel == 0 && 
			!CheckPartitioningInLevelZero(exCatalog, 
				targetPartitionLevel, targetPartitionNumber))
	{
		/* Increment normal modification count. */
		refcntForChecking = IncrNormalModificationCount(refcounter);

		return NORMAL_MODIFICATION;
	}

	/* Read reference counter. */
	refcnt = pg_atomic_read_u64(refcounter);
	pg_memory_barrier();

	if ((refcnt & (REPARTITIONING_INPROGRESS | 
					REPARTITIONING_MODIFICATION_BLOCKING)) == 0ULL) 
	{
		/* 
		 * Repartitioning is not inprogress.
		 */
		
		/* Increment normal modification count. */
		refcntForChecking = IncrNormalModificationCount(refcounter);

		if ((refcntForChecking & (REPARTITIONING_INPROGRESS |
							REPARTITIONING_MODIFICATION_BLOCKING)) != 0ULL)
		{
			/* Repartitioning worker intervene here. Just retry it. */
			DecrNormalModificationCount(refcounter);
			goto retry_get_modification_method;
		}

		return NORMAL_MODIFICATION;
	}
	else if (refcnt & REPARTITIONING_MODIFICATION_BLOCKING)
	{
		/* 
		 * The worker cannot proceed with modification for the partition.
		 * Caller needs to go downside partition to find the tuple.
		 */
		Assert(refcnt & REPARTITIONING_INPROGRESS);

		return MODIFICATION_BLOCKING;
	}
	else
	{
		/* 
		 * Repartitioning is inprogress. But, the worker can do modification 
		 * with delta. 
		 */
		Assert(refcnt & REPARTITIONING_INPROGRESS);

		/* Increment normal modification count. */
		refcntForChecking = IncrModificationWithDeltaCount(refcounter);
		Assert(refcntForChecking & REPARTITIONING_INPROGRESS);

		if (refcntForChecking & REPARTITIONING_MODIFICATION_BLOCKING)
		{
			/* 
			 * At this point, the repartitioning worker intervenes. If 
			 * repartitioning enters the blocking phase, we won't be able to 
			 * find the tuple in this partition. 
			 */
			DecrModificationWithDeltaCount(refcounter);
	
			return MODIFICATION_BLOCKING;
		}

		return MODIFICATION_WITH_DELTA;
	}

	/* quiet compiler */
	Assert(false);
	return MODIFICATION_BLOCKING;
}

/*
 * DecrModificationCount
 */
void
DecrModificationCount(pg_atomic_uint64 *refcounter, 
				LocatorModificationMethod modificationMethod)
{
	/* 
	 * If modification method is not 'normal' or 'with delta', that is an 
	 * error.
	 */
	Assert((modificationMethod == NORMAL_MODIFICATION) || 
				(modificationMethod == MODIFICATION_WITH_DELTA));

	if (modificationMethod == NORMAL_MODIFICATION)
		DecrNormalModificationCount(refcounter);
	else if (modificationMethod == MODIFICATION_WITH_DELTA)
		DecrModificationWithDeltaCount(refcounter);
}

/*
 * UnsetAllBlockCheckpointerBits
 * 
 * Unset BlockCheckpointerBits of all external catalog to notify checkpointer
 * that repartitioning newest mempartition is safe from now.
 */
void
UnsetAllBlockCheckpointerBits(void)
{
	LocatorExternalCatalog *exCatalog;

	for (int i = 0; i < LocatorExternalCatalogGlobalMeta->externalCatalogNums; i++)
	{
		exCatalog = ExCatalogIndexGetLocatorExternalCatalog(i);

		if (IsNoWorker(exCatalog))
			UnsetBlockCheckpointerBit(exCatalog);
	}
}

/*
 * Append level 0 stat into the given string. There is no partition metadata for
 * each tiers. But each of them has same size. So estimate the page count using
 * that knowledge.
 */
static inline void
locator_append_level_0_stat(LocatorExternalCatalog *exCatalog, StringInfo line)
{
	appendStringInfo(line, "%u ",
		(exCatalog->CurrentTieredNumberLevelZero[0] - exCatalog->PreviousTieredNumberLevelZero[0])
			* LOCATOR_MEMPART_PAGE_COUNT);
}

/*
 * Append non-zero level stat into the given string.
 */
static inline void
locator_append_non_0_level_stat(LocatorExternalCatalog *exCatalog,
								Relation rel, StringInfo line)
{
	for (LocatorPartLevel level = 1;
		 level <= exCatalog->lastPartitionLevel; level++)
	{
		LocatorPartNumber max_partnum = pow(rel->rd_locator_spread_factor,
											level);
		uint64_t page_cnt = 0;

		for (LocatorPartNumber partnum = 0;
			 partnum < max_partnum; partnum++)
		{
			uint64_t recordcnt = exCatalog->realRecordsCount[level][partnum];
	
			Assert(exCatalog->recordNumsPerBlock != 0);
			page_cnt += recordcnt % exCatalog->recordNumsPerBlock == 0 ?
						recordcnt / exCatalog->recordNumsPerBlock :
						recordcnt / exCatalog->recordNumsPerBlock + 1;
		}
	
		appendStringInfo(line, "%lu ", page_cnt);
	}
}

/*
 * Write level stat string into the specified directory.
 */
static inline void
locator_write_level_stat_str(LocatorExternalCatalog *exCatalog,
							 Oid relid, char *dir)
{
	Relation rel = table_open(relid, AccessShareLock);
	struct timespec curr;
	StringInfo line;
	char path[256];
	FILE *fp;

	/* Initialize line */
	line = palloc0(sizeof(StringInfoData));
	initStringInfo(line);

	/* First column is current time (second) */
	clock_gettime(CLOCK_MONOTONIC, &curr);
	appendStringInfo(line, "%lu ", curr.tv_sec);

	/* Append level 0's stat */
	locator_append_level_0_stat(exCatalog, line);

	/* Append other level's stats */
	locator_append_non_0_level_stat(exCatalog, rel, line);

	/* Write the line */
	sprintf(path, "%s/%s", dir, RelationGetRelationName(rel));
	fp = fopen(path, "a");
	fprintf(fp, "%s\n", line->data);
	fclose(fp);

	/* Done */
	pfree(line);
	table_close(rel, AccessShareLock);
}

/*
 * Function for stat.
 */
Datum
locator_get_level_stat(PG_FUNCTION_ARGS)
{
	char *dir = text_to_cstring(PG_GETARG_TEXT_P(0));
	List *relid_list = LocatorGetRelOidList();
	ListCell *lc;

	/* Make a given directory automatically */
	if (mkdir(dir, 0777) == -1 && errno != EEXIST)
		elog(ERROR, "mkdir error");

	/* Write level stat string for locator tables */
	foreach(lc, relid_list)
	{
		Oid relid = lfirst_oid(lc);
		LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(relid);

		if (exCatalog == NULL)
			continue;

		locator_write_level_stat_str(exCatalog, relid, dir);
	}

	PG_RETURN_VOID();
}

static uint32
GetExpOf2(uint32 n)
{
	uint32 a = 1;

	while (a < n)
		a <<= 1;

	return a;
}

static uint64
ATTRALIGN(uint64 len)
{
	if (len <= 2)
		return SHORTALIGN(len);

	if (len <= 4)
		return INTALIGN(len);

	return MAXALIGN(len);
}

#define PRINT_COLUMNAR_LAYOUT 1
/*
 * Columnar layout setter
 *
 * Parse the raw string given as input and set the layout of a record zone, the
 * size of a record zone, and the metadata of each column group using those
 * values.
 * 
 * To facilitate data management, we equalize the tuple cardinality of each
 * column group within a record zone.
 * 
 * It is impractical to predict and calculate these accurately, so we align the
 * values to achieve a practical record zone layout with some fragmentation.
 * 
 * 	1. 	Find the limit of the number of records that a record zone can contain.
 * 		a.	For each column group, align the number of tuples a page can have
 * 			with a power of 2.
 * 		b.	Find the maximum value of the aligned values and set it as the
 * 			limit.
 * 	2.	Calculate the maximum tuple cardinality for each column group under the
 * 		record zone's record count limit.
 * 	3.	Equalize the tuple cardinality of the column groups using the minimum
 * 		of them.
 * 
 * 
 * 	record zone's record
 * 	count limit		------------------------------------------------------------
 * 
 * 					|-------------|
 * 					|  group 0's  |
 * 					| cardinality |             |-------------|
 * 	record zone's	|             |             |  group 2's  |-------------|
 * 	record count  <-------------- |-------------| cardinality |  group 3's  |
 * 					|             |  group 1's  |             | cardinality |
 * 					|             | cardinality |             |             |
 */
void
LocatorSetColumnarLayout(Oid relationOid, char *raw_string)
{
	char	*ptr;
	char	*exCatalogPtr;
	int64	currentOffset;

	uint8	targetLevelCount;
	uint8	targetLevel;
	uint16	columnGroupCount;
	uint16	groupLen;
	uint16	attrIndex;
	int		natts;
	int		nattsInserted;
	int		layoutIndex;
	ColumnarLayout *columnarLayoutArray;

	Relation relation;
	LocatorExternalCatalog *exCatalog;

	uint32 blockFreeSpace;
	uint32 recordsPerRecordZone;
	uint32 recordsPerRecordZoneLimit;
	uint16 maxlen;

	uint16 tupleSize;
	uint16 tuplesPerBlock;
	uint32 tuplesPerGroup;
	uint32 blockOffset;
	uint32 blockCount = 0;
	uint32 exp;

	if (unlikely(!IsLocatorRelId(relationOid)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid relationOid")));
	}

	/* Get target level count */
	ptr = strtok(raw_string, "|");
	targetLevelCount = atoi(ptr);

	/* The target level count must be 1 */
	/* TODO: COLUMNAR layout (targetLevelCount 2) */
	if (unlikely(targetLevelCount != 1))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target level count must be 1 or 2")));
	}

	/* Init variables */
	relation = table_open(relationOid, AccessShareLock);
	exCatalog = LocatorGetExternalCatalog(relationOid);

	natts = RelationGetNumberOfAttributes(relation);
	blockFreeSpace = (BLCKSZ - (RelationGetTargetPageFreeSpace(relation,
					  HEAP_DEFAULT_FILLFACTOR) + SizeOfPageHeaderData));

	/* Overwrite the columnar layout if it already exists. */
	if (exCatalog->layoutArray[exCatalog->lastPartitionLevel] != 0)
		ereport(WARNING, (errmsg("columnar layout is already set")));

	{ /* Fill the layout array */
		int levelIndex = 1;
		layoutIndex = ROW_ORIENTED;

		for (; layoutIndex < targetLevelCount; layoutIndex++)
		{
			/* Get the target level */
			ptr = strtok(NULL, "|");
			targetLevel = atoi(ptr);

			/* To prevent the checkpointer from reformatting */
			Assert(targetLevel > 1);

			for (; levelIndex < targetLevel; levelIndex++)
				exCatalog->layoutArray[levelIndex] = layoutIndex;
		}

		for (; levelIndex < relation->rd_locator_level_count; levelIndex++)
			exCatalog->layoutArray[levelIndex] = layoutIndex;
	}

	exCatalogPtr = (char*) (exCatalog->columnarLayoutArray[MIXED].recordsPerRecordZone);
	currentOffset = 0;

	/* Set columnar layouts */
	for (layoutIndex = MIXED; layoutIndex <= targetLevelCount; layoutIndex++)
	{
		/* Get the number of column group */
		ptr = strtok(NULL, "|");
		columnGroupCount = atoi(ptr);

		if (columnGroupCount > MAX_COLUMN_GROUP_COUNT)
		{
			InitColumnarLayout(exCatalog, relation);
			table_close(relation, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("exceed MAX_COLUMN_GROUP_COUNT")));
		}

		/* For sanity checking */
		nattsInserted = 0;

		/* Set positions */
		{
			columnarLayoutArray = exCatalog->columnarLayoutArray + layoutIndex;

			columnarLayoutArray->recordsPerRecordZone =
				(uint32*) (exCatalogPtr + currentOffset);
			currentOffset += (int) (sizeof(uint32));

			columnarLayoutArray->blocksPerRecordZone =
				(uint32*) (exCatalogPtr + currentOffset);
			currentOffset += (int) (sizeof(uint32) * 2);

			columnarLayoutArray->columnGroupCount =
				(uint8*) (exCatalogPtr + currentOffset);
			*(columnarLayoutArray->columnGroupCount) = columnGroupCount;
			currentOffset += (int) (sizeof(uint8));

			columnarLayoutArray->columnGroupArray =
				(uint8*) (exCatalogPtr + currentOffset);
			currentOffset += (int) (sizeof(uint8) * natts);

			columnarLayoutArray->columnGroupMetadataArray =
				(ColumnGroupMetadata*) (exCatalogPtr + currentOffset);
			currentOffset += (int) (sizeof(ColumnGroupMetadata) * columnGroupCount);
		}

		/* Init the record count limit */
		recordsPerRecordZoneLimit = 1;
		for (uint16 groupNum = 0; groupNum < columnGroupCount; groupNum++)
		{
			/* Get the number of attributes of this column group */
			ptr = strtok(NULL, "|");
			groupLen = atoi(ptr);

			if (groupLen > MAX_ATTR_COUNT_OF_COLUMN_GROUP)
			{
				InitColumnarLayout(exCatalog, relation);
				table_close(relation, AccessShareLock);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("exceed MAX_ATTR_COUNT_OF_COLUMN_GROUP")));
			}

			/* For sanity checking */
			nattsInserted += groupLen;

			maxlen = 0;
			for (uint16 k = 0; k < groupLen; k++)
			{
				ptr = strtok(NULL, "|");
				attrIndex = atoi(ptr);

				/* Wrong attribute */
				if (attrIndex >= natts)
				{
					InitColumnarLayout(exCatalog, relation);
					table_close(relation, AccessShareLock);
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("exceed available attribute index")));
				}

				/* Map the attribute and this column group */
				columnarLayoutArray->columnGroupArray[attrIndex] = groupNum;

				/* Get the max length of the tuple of this column group */
				maxlen += relation->rd_locator_attrs_maxlen[attrIndex];
			}

			/* The group 0 of MIXED layout is SIRO-versioned */
			if (unlikely(layoutIndex == MIXED && groupNum == 0))
			{
				uint16 headerLen, alignedMaxLen, alignedMaxDataLen;

				headerLen =	MAXALIGN(offsetof(HeapTupleHeaderData, t_bits) +
									 BITMAPLEN(natts));
				alignedMaxLen = MAXALIGN(headerLen + maxlen);
				alignedMaxDataLen =
					MAXALIGN(RelationGetToastTupleTarget(relation, TOAST_TUPLE_TARGET) -
							 headerLen);

				/* SIRO-versioned tuple */
				tupleSize = alignedMaxLen < alignedMaxDataLen ?
							alignedMaxLen : alignedMaxDataLen;
				tuplesPerBlock = blockFreeSpace /
								 (3 * sizeof(ItemIdData) + MAXALIGN(PITEM_SZ) +
								  2 * tupleSize);
			}
			else
			{
				/*
				 * A tuple in a column group has an attribute and a
				 * corresponding null bit. The calculation of the maximum number
				 * of tuples that one page can have is as follows:
				 * 
				 * TC: The maximum count of tuples of one column group
				 * AC: The count of attributes (length of the column group)
				 * TS: The size of a tuple
				 * FS: The size of free space
				 * 
				 * 		                               AC * TC
				 * 		       TS * TC       +       -----------    =  FS
				 * 		                                  8
				 * 		  (size of tuples)      (size of null bitmap)
				 * 
				 * 
				 * 		=>   8 * TS * TC     +         AC * TC      =  8 * FS
				 * 
				 * 		                          8 * FS
				 *      =>       TC     =     ---------------
				 * 		                        8 * TS + AC
				 */
				tupleSize = ATTRALIGN(sizeof(TransactionId) + maxlen); /* xmin + tuple */
				tuplesPerBlock = (8 * blockFreeSpace) /
								 (8 * tupleSize + groupLen);
			}

			/* Set the metadata of this column group */
			columnarLayoutArray->columnGroupMetadataArray[groupNum].tupleSize = tupleSize;
			columnarLayoutArray->columnGroupMetadataArray[groupNum].tuplesPerBlock = tuplesPerBlock;

			/* Align the value and get maximum limit */
			exp = GetExpOf2(tuplesPerBlock);
			if (recordsPerRecordZoneLimit < exp)
				recordsPerRecordZoneLimit = exp;
		}

		if (nattsInserted != natts)
		{
			InitColumnarLayout(exCatalog, relation);
			table_close(relation, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("number of inserted attrs is wrong, nattsInserted: %hu, natts: %d", nattsInserted, natts)));
		}

		/* record keys for partitioning worker */
		if (layoutIndex == COLUMNAR)
		{
			tupleSize = MAXALIGN(sizeof(LocatorRecordKey)); /* record key is 8 bytes */
			tuplesPerBlock = (blockFreeSpace / tupleSize);

			/* Set the metadata of this column group */
			columnarLayoutArray->columnGroupMetadataArray[columnGroupCount].tupleSize = tupleSize;
			columnarLayoutArray->columnGroupMetadataArray[columnGroupCount].tuplesPerBlock = tuplesPerBlock;

			/* Align the value and get maximum limit */
			exp = GetExpOf2(tuplesPerBlock);
			if (recordsPerRecordZoneLimit < exp)
				recordsPerRecordZoneLimit = exp;

			/* record keys for partitioning worker */
			columnGroupCount += 1;
		}

		recordsPerRecordZoneLimit *= RecordZoneSizeAmplifier;

		recordsPerRecordZone = recordsPerRecordZoneLimit;
		for (uint16 groupNum = 0; groupNum < columnGroupCount; groupNum++)
		{
			/* Get the tuple cardinality of this column group (tuplesPerGroup) */
			tuplesPerBlock = columnarLayoutArray->columnGroupMetadataArray[groupNum].tuplesPerBlock;
			blockCount = recordsPerRecordZoneLimit / tuplesPerBlock;
			tuplesPerGroup = tuplesPerBlock * blockCount;

			/* Calculate the minimum value */
			if (recordsPerRecordZone > tuplesPerGroup)
				recordsPerRecordZone = tuplesPerGroup;
		}

		/* Set values */
		*(columnarLayoutArray->recordsPerRecordZone) = recordsPerRecordZone;
		blockOffset = 0;
		for (uint16 groupNum = 0; groupNum < columnGroupCount; groupNum++)
		{
			tuplesPerBlock = columnarLayoutArray->columnGroupMetadataArray[groupNum].tuplesPerBlock;
			blockCount = ((recordsPerRecordZone - 1) / tuplesPerBlock) + 1;
			columnarLayoutArray->columnGroupMetadataArray[groupNum].blockOffset = blockOffset;
			blockOffset += blockCount;
		}

		/* record keys for partitioning worker */
		if (layoutIndex == COLUMNAR)
		{
			columnarLayoutArray->blocksPerRecordZone[0] = blockOffset;
			columnarLayoutArray->blocksPerRecordZone[1] = blockOffset - blockCount;
		}
		else
			columnarLayoutArray->blocksPerRecordZone[0] = blockOffset;
	}

	if (targetLevelCount == 1)
		columnarLayoutArray->blocksPerRecordZone[1] = columnarLayoutArray->blocksPerRecordZone[0];

	if (unlikely(strtok(NULL, "|") != NULL))
	{
		InitColumnarLayout(exCatalog, relation);
		table_close(relation, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("wrong format")));
	}

#if PRINT_COLUMNAR_LAYOUT == 1
	fprintf(stderr, "[LOCATOR] relation: %d\n", relationOid);
	fprintf(stderr, "[LOCATOR] maxlen: ");
	for (int i = 0; i < natts; i++)
		fprintf(stderr, "%hu ", relation->rd_locator_attrs_maxlen[i]);
	fprintf(stderr, "\n");

	fprintf(stderr, "[LOCATOR] layout array: ");
	for (int i = 0; i < relation->rd_locator_level_count; i++)
		fprintf(stderr, "%hu ", exCatalog->layoutArray[i]);
	fprintf(stderr, "\n");

	for (layoutIndex = ROW_ORIENTED; layoutIndex <= targetLevelCount; layoutIndex++)
	{
		columnarLayoutArray = exCatalog->columnarLayoutArray + layoutIndex;

		fprintf(stderr, "[LOCATOR] layout: %d\n", layoutIndex);
		fprintf(stderr, "[LOCATOR] \trecords per record zone: %u\n", *(columnarLayoutArray->recordsPerRecordZone));
		fprintf(stderr, "[LOCATOR] \tblocks per record zone: [%u, %u]\n", columnarLayoutArray->blocksPerRecordZone[0], columnarLayoutArray->blocksPerRecordZone[1]);
		fprintf(stderr, "[LOCATOR] \tcount of column groups: %hhu\n", *(columnarLayoutArray->columnGroupCount));

		fprintf(stderr, "[LOCATOR] \tcolumn group array:");
		for (int i = 0; i < natts; i++)
			fprintf(stderr, "%hhu ", columnarLayoutArray->columnGroupArray[i]);
		fprintf(stderr, "\n");

		columnGroupCount = *(columnarLayoutArray->columnGroupCount);
		if (layoutIndex == COLUMNAR)
			columnGroupCount += 1;
		fprintf(stderr, "[LOCATOR] \tcolumn group metadata array:\n");
		for (int i = 0; i < columnGroupCount; i++)
		{
			fprintf(stderr, "[LOCATOR] \t\tmetadata of group %d:\n", i);
			fprintf(stderr, "[LOCATOR] \t\t\ttuple size: %hu\n", columnarLayoutArray->columnGroupMetadataArray[i].tupleSize);
			fprintf(stderr, "[LOCATOR] \t\t\ttuples per block: %hu\n", columnarLayoutArray->columnGroupMetadataArray[i].tuplesPerBlock);
			fprintf(stderr, "[LOCATOR] \t\t\tblock offset: %u\n", columnarLayoutArray->columnGroupMetadataArray[i].blockOffset);
		}
	}
#endif

	table_close(relation, AccessShareLock);
}
#else /* !LOCATOR */
/*
 * Dummy function
 */
Datum
locator_get_level_stat(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}
#endif /* LOCATOR */
