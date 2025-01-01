/*
 * locator_partitioning.c
 *
 * Locator Partitioning Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/partitioning/locator_partitioning.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef LOCATOR

#include "postgres.h"

#include <time.h>
#include <math.h>
#include <unistd.h>
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "access/heapam.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/itemptr.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "miscadmin.h"

#include "pg_refcnt.h"
#include "locator/locator.h"
#include "locator/locator_external_catalog.h"
#include "locator/locator_partitioning.h"
#include "locator/locator_mempart_buf.h"
#include "locator/locator_md.h"

#include "storage/ebi_tree.h"

/* src/include/postmaster/partition_mgr.h */
dsa_area* locatorPartitionDsaArea;

/* GUC parameter */
bool enable_uring_partitioning; /* default: true */

void
LocatorReformatSiroTuple(LocatorRepartitioningDesc *desc, SiroTuple siroTuple)
{
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	LocatorReformattingDesc *reformattingDesc = &(upperScan->reformattingDesc);
	TupleDesc tupleDesc = desc->relation->rd_att;
	uint16 tupleSize = lowerInsert->tupleSize[0];
	int attrIdx;

	HeapTupleData tuple;
	HeapTupleHeader newTuple;
	Size data_len;
	Size off = MAXALIGN(offsetof(HeapTupleHeaderData, t_bits) +
						BITMAPLEN(tupleDesc->natts));

	Assert(upperScan->layoutType != COLUMNAR);

	/* Deform left version. */
	Assert(siroTuple->leftVersionLen != 0);
	{
		tuple.t_data = siroTuple->leftVersionData;
		heap_deform_tuple(&tuple, tupleDesc, reformattingDesc->leftValues,
						  reformattingDesc->leftIsnull);
	}

	/* Deform right version. */
	if (siroTuple->rightVersionLen != 0)
	{
		tuple.t_data = siroTuple->rightVersionData;
		heap_deform_tuple(&tuple, tupleDesc, reformattingDesc->rightValues,
						  reformattingDesc->rightIsnull);
	}

	/* Reform the left version. */
	{
		Assert(reformattingDesc->tupleAttrCounts[0] < tupleDesc->natts);

		/* Set xmin for columnar tuples. */
		/* TODO: implement COLUMNAR layout */
		reformattingDesc->xmin =
			HeapTupleHeaderGetRawXmin(siroTuple->leftVersionData);

		/*
		 * Set all null bits of siroIsnull except for attributes of SIRO
		 * tuple.
		 */
		for (int i = 0; i < reformattingDesc->tupleAttrCounts[0]; i++)
		{
			attrIdx = reformattingDesc->tupleAttrs[0][i];
			reformattingDesc->siroIsnull[attrIdx] =
				reformattingDesc->leftIsnull[attrIdx];
		}

		/*
		 * Aborted case:
		 * 		Set all null bits of leftIsnull except for attributes of SIRO
		 * 		tuple too.
		 */
		if (siroTuple->isAborted)
		{
			for (int i = 0; i < tupleDesc->natts; i++)
			{
				reformattingDesc->leftIsnull[i] =
					reformattingDesc->siroIsnull[i];
			}
		}

		/* Zero the new tuple first. */
		newTuple = (HeapTupleHeader)(reformattingDesc->tuples[0]);
		memset(newTuple, 0, tupleSize);

		/* HeapTupleHeaderData without null bitmap. */
		memcpy(newTuple, siroTuple->leftVersionData,
			   offsetof(HeapTupleHeaderData, t_bits));

		/* Compute the size of a new SIRO tuple. */
		data_len = heap_compute_data_size(tupleDesc,
										  reformattingDesc->leftValues,
										  reformattingDesc->siroIsnull);

		/* Fill with new values. */
		newTuple->t_hoff = off;
		heap_fill_tuple(tupleDesc, reformattingDesc->leftValues,
						reformattingDesc->siroIsnull,
						(char*)(newTuple) + off, data_len,
						&(newTuple->t_infomask), newTuple->t_bits);

		Assert(off + data_len <= tupleSize);

		/* Map the new tuple. */
		siroTuple->leftVersionData = newTuple;
		siroTuple->leftVersionLen = off + data_len;
	}

	/* Reform the right version. */
	if (siroTuple->rightVersionLen != 0)
	{
		/* Set the null bitmap. */
		for (int i = 0; i < reformattingDesc->tupleAttrCounts[0]; i++)
		{
			attrIdx = reformattingDesc->tupleAttrs[0][i];
			reformattingDesc->siroIsnull[attrIdx] =
				reformattingDesc->rightIsnull[attrIdx];
		}

		/* Zero the new tuple first. */
		newTuple = (HeapTupleHeader)(reformattingDesc->tuples[0] + tupleSize);
		memset(newTuple, 0, tupleSize);

		/* HeapTupleHeaderData without null bitmap. */
		memcpy(newTuple, siroTuple->rightVersionData,
			   offsetof(HeapTupleHeaderData, t_bits));

		/* Compute the size of a new SIRO tuple. */
		data_len = heap_compute_data_size(tupleDesc,
										  reformattingDesc->rightValues,
										  reformattingDesc->siroIsnull);

		/* Fill with new values. */
		newTuple->t_hoff = off;
		heap_fill_tuple(tupleDesc, reformattingDesc->rightValues,
						reformattingDesc->siroIsnull,
						(char*)(newTuple) + off, data_len,
						&(newTuple->t_infomask), newTuple->t_bits);

		Assert(off + data_len <= tupleSize);

		/* Map the new tuple. */
		siroTuple->rightVersionData = newTuple;
		siroTuple->rightVersionLen = off + data_len;
	}
}

/*
 * LocatorReformatLocatorRecord
 */
static void
LocatorReformatLocatorRecord(LocatorRepartitioningDesc *desc,
							 LocatorRecord locatorRecord)
{
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	LocatorReformattingDesc *reformattingDesc = &(upperScan->reformattingDesc);
	TupleDesc tupleDesc = desc->relation->rd_att;
	int attrIdx;

	uint16 *attrMaxlens = desc->relation->rd_locator_attrs_maxlen;
	Form_pg_attribute attr;
	bool isAborted = false;
	int columnGroup = 0;

	/* Deform the upper record */
	columnGroup = 0;

	/* If the SIRO tuple exists in the previous layout, reform it first. */
	if (likely(upperScan->layoutType != COLUMNAR))
	{
		LocatorReformatSiroTuple(desc, &(locatorRecord->siroTuple));
		isAborted = locatorRecord->siroTuple.isAborted;
		columnGroup = 1;
	}
	else
	{
#if 0
		/* TODO: Set isAborted. */
#endif
	}

	/* And deform the rest columnar tuples to the most recent version. */
#if 0 /* TODO: Implement COLUMNAR layout. */
	for (; columnGroup < upperScan->columnGroupCount; columnGroup++)
	{
		/* TODO: Deform each column tuple to values, isnull of the most recent version. */
	}
#endif

	/* Then reform the rest columnar tuples. */
	for (; columnGroup < lowerInsert->columnGroupCount; columnGroup++)
	{
		char *data = reformattingDesc->tuples[columnGroup];
		locatorRecord->columnarTuple[columnGroup].columnarTupleData = (Item)data;

		/* Zero the new tuple first. */
		memset(data, 0, lowerInsert->tupleSize[columnGroup]);

		/* Copy the xmin. */
		memcpy(data, &(reformattingDesc->xmin), sizeof(TransactionId));
		data += sizeof(TransactionId);

		/* Fill with new values. */
		for (int i = 0; i < reformattingDesc->tupleAttrCounts[columnGroup]; i++)
		{
			attrIdx = reformattingDesc->tupleAttrs[columnGroup][i];
			attr = TupleDescAttr(tupleDesc, attrIdx);
			locatorRecord->columnarTuple[columnGroup].isnull[i] =
				upperScan->reformattingDesc.leftIsnull[attrIdx];

			fill_columnar_val(attr, data, reformattingDesc->leftValues[attrIdx],
							  reformattingDesc->leftIsnull[attrIdx], isAborted);

			data += attrMaxlens[attrIdx];
		}

		Assert((uint64)data - (uint64)reformattingDesc->tuples[columnGroup] <= lowerInsert->tupleSize[columnGroup]);
	}
}

/*
 * LocatorBeginRepartitioning
 *
 * When starting repartitioning, this function is called by repartitioning 
 * worker. In here, we do allocate and set values for repartitioning.
 */
static void
LocatorBeginRepartitioning(LocatorRepartitioningDesc *desc,	Oid relationOid,
						   LocatorPartLevel partitionLevel,
						   LocatorPartNumber partitionNumber,
						   LocatorTieredNumber minTieredNumber,					
						   LocatorTieredNumber maxTieredNumber,					
						   LocatorPartLevel targetPartitionLevel,
						   bool isPartitioningWorker)
{
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	MemoryContext oldContext;
	int natts;
	int initCapacity;
	uint16 alignment;

	/* For upperScan */
	ColumnarLayout *columnarLayout = exCatalog->columnarLayoutArray +
									 exCatalog->layoutArray[partitionLevel];
	uint32 recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
	uint32 blocksPerRecordZone = columnarLayout->blocksPerRecordZone[0];
	uint16 columnGroupCount = *(columnarLayout->columnGroupCount);

	Assert(partitionLevel < targetPartitionLevel);

	/*
	 * Normal partitioning worker can open the relation, but the checkpointer
	 * can't. So if this repartitioning is being executed by checkpointer, use
	 * backed up data rather than opening the relation.
	 * 
	 * TODO: If the data is backed up for checkpointer, shouldn't the
	 * partitioning worker also not have to open the relation?
	 */
	if (likely(isPartitioningWorker))
	{
		StartTransactionCommand();
		desc->relation = table_open(relationOid, AccessShareLock);
		desc->descTopMemoryContext = TopTransactionContext;
	}
	else
	{
		desc->relation = &(exCatalog->relation);
		desc->descTopMemoryContext = TopMemoryContext;
	}

	natts = desc->relation->rd_att->natts;

	/* Init partition scan domain. */
	upperScan->exCatalog = exCatalog;
	upperScan->relation = desc->relation;
	upperScan->partitionLevel = partitionLevel;
	upperScan->partitionNumber = partitionNumber;
	upperScan->minTieredNumber = minTieredNumber;
	upperScan->maxTieredNumber = maxTieredNumber;
	upperScan->firstScan = true;
	upperScan->currentRecord = NULL;
	upperScan->isPartitioningWorker = isPartitioningWorker;

	if (partitionLevel == 0)
	{
		upperScan->currentPartitionGenerationNumber = minTieredNumber;
	}
	else
	{
		upperScan->currentPartitionGenerationNumber = 
						LocatorGetPartGenerationNumber(exCatalog, 
								upperScan->partitionLevel, upperScan->partitionNumber);
	}

	/*
	 * Ready for the first partition
	 *
	 * There are two types of objects that repartition: partitioning worker and
	 * checkpointer.
	 * 
	 * Normally, only the partitioning worker does the repartitioning, but when
	 * the PostgreSQL server shuts down, the checkpointer does the last
	 * repartitioning, and it only repartitions level 0, including the newest
	 * mempartition.
	 * 
	 * By default, a mempartition at level 0 is not evicted unless the threshold
	 * is fully filled, so the number of pages in a tiered mempartition has a
	 * constant value.
	 * Therefore, in a normal case, when the partitioning worker repartitions
	 * level 0, only tiered mempartitions are repartitioned, so the constant
	 * value mentioned above can be used.
	 * 
	 * However, the repartitioning of level 0 executed by the checkpointer when
	 * the server is shut down will repartition including the newest
	 * mempartition that has not fully filled the threshold.
	 * Therefore, in this case, the number of pages of the newest mempartition
	 * does not have a constant value, so the number of pages must be calculated
	 * using the number of records recorded in the external catalog.
	 */
	if (likely(isPartitioningWorker))
	{
		/* Normal case: partitioning worker */
		upperScan->nrecordzones = partitionLevel == 0 ?
			LocatorMempartPageCount : /* nblocks == nrecordzones in level 0 */
			NRecordZonesUsingRecordCount(exCatalog->realRecordsCount[partitionLevel][partitionNumber],
									recordsPerRecordZone);
	}
	else
	{
		/* PostgreSQL server is shutting down: checkpointer */
		Assert(partitionLevel == 0);

		upperScan->nrecordzones = 
			(minTieredNumber == exCatalog->CurrentTieredNumberLevelZero[partitionNumber]) ?
				NRecordZonesUsingRecordCount( /* nblocks == nrecordzones in level 0 */
					exCatalog->CurrentRecordCountsLevelZero[partitionNumber].val,
					recordsPerRecordZone) :
				LocatorMempartPageCount;
	}

	/* Init for layout. */
	upperScan->layoutType = exCatalog->layoutArray[partitionLevel];
	upperScan->columnarLayout = columnarLayout;
	upperScan->blocksPerRecordZone = blocksPerRecordZone;

	/* Init indicators.*/
	for (int i = 0; i < columnGroupCount; i++)
	{
		upperScan->columnGroups[i].currentBuffer = InvalidBuffer;
		upperScan->columnGroups[i].currentBlock = InvalidBlockNumber;
		upperScan->attrCount[i] = 0;
	}

	/* Set the number of attributes for each column group. */
	for (int i = 0; i < natts; i++)
		++(upperScan->attrCount[columnarLayout->columnGroupArray[i]]);

	/* Get the appropriate number of column groups and init the related variables. */
	if (upperScan->layoutType == COLUMNAR)
	{
		/* Init for the recordKey group. */
		upperScan->columnGroups[columnGroupCount].currentBuffer = InvalidBuffer;
		upperScan->columnGroups[columnGroupCount].currentBlock = InvalidBlockNumber;
		upperScan->attrCount[columnGroupCount] = 1;
		upperScan->recordKeyGroupIndex = columnGroupCount;
		++columnGroupCount;
	}
	else
		upperScan->recordKeyGroupIndex = MAX_COLUMN_GROUP_COUNT + 1; /* unreachable */

	/* Set the number of column groups. */
	upperScan->columnGroupCount = columnGroupCount;

#ifdef LOCATOR_DEBUG
	upperScan->unstableRecords = 0;
	upperScan->repartitionedNrecord_partition = 0;
#endif /* LOCATOR_DEBUG */

	desc->deltaNums = 0;
	desc->parentRecordZones = upperScan->nrecordzones;
	desc->childRecordZones = 0;
	desc->parentRecordsPerRecordZone = recordsPerRecordZone;
	desc->parentBlocksPerRecordZone = blocksPerRecordZone;

	/* For lowerInsert */
	columnarLayout = exCatalog->columnarLayoutArray +
					 exCatalog->layoutArray[targetPartitionLevel];
	recordsPerRecordZone = *(columnarLayout->recordsPerRecordZone);
	blocksPerRecordZone = columnarLayout->blocksPerRecordZone[targetPartitionLevel == exCatalog->lastPartitionLevel ? 1 : 0];
	columnGroupCount = *(columnarLayout->columnGroupCount);

	desc->childRecordsPerRecordZone = recordsPerRecordZone;
	desc->childBlocksPerRecordZone = blocksPerRecordZone;

#ifdef LOCATOR_DEBUG
	fprintf(stderr, "[LOCATOR] LocatorBeginRepartitioning, level: %u, number: %u, nrecordzones: %u\n",
					partitionLevel, partitionNumber, upperScan->nrecordzones);
#endif /* LOCATOR_DEBUG */

	/* Init insert description. */
	lowerInsert->upperPartitionLevel = partitionLevel;
	lowerInsert->targetPartitionLevel = targetPartitionLevel;

	/* Init for layout. */
	lowerInsert->columnarLayout = columnarLayout;
	lowerInsert->layoutType = exCatalog->layoutArray[targetPartitionLevel];
	lowerInsert->blocksPerRecordZone = blocksPerRecordZone;

	/* Check if the layout type changes. */
	if (upperScan->layoutType != lowerInsert->layoutType)
		upperScan->layoutNeedsToBeChanged = true;
	else
		upperScan->layoutNeedsToBeChanged = false;

	/* Get the info of column groups.*/
	for (int i = 0; i < columnGroupCount; i++)
	{
		lowerInsert->attrCount[i] = 0;
		lowerInsert->tupleSize[i] =
			columnarLayout->columnGroupMetadataArray[i].tupleSize;
		lowerInsert->tuplesPerBlock[i] =
			columnarLayout->columnGroupMetadataArray[i].tuplesPerBlock;
		lowerInsert->blockOffset[i] =
			columnarLayout->columnGroupMetadataArray[i].blockOffset;
	}

	/* Set the number of attributes for each column group. */
	for (int i = 0; i < natts; i++)
		++(lowerInsert->attrCount[columnarLayout->columnGroupArray[i]]);

	/* Get the appropriate number of column groups and init the related variables. */
	if (lowerInsert->layoutType == COLUMNAR && targetPartitionLevel != exCatalog->lastPartitionLevel)
	{
		/* Init for the recordKey group. */
		lowerInsert->attrCount[columnGroupCount] = 1;
		lowerInsert->tupleSize[columnGroupCount] =
			columnarLayout->columnGroupMetadataArray[columnGroupCount].tupleSize;
		lowerInsert->tuplesPerBlock[columnGroupCount] =
			columnarLayout->columnGroupMetadataArray[columnGroupCount].tuplesPerBlock;
		lowerInsert->blockOffset[columnGroupCount] =
			columnarLayout->columnGroupMetadataArray[columnGroupCount].blockOffset;
		++columnGroupCount;
	}
	else
		lowerInsert->recordKeyGroupIndex = MAX_COLUMN_GROUP_COUNT + 1; /* unreachable */

	/* Set the number of column groups. */
	lowerInsert->columnGroupCount = columnGroupCount;

	oldContext = MemoryContextSwitchTo(desc->descTopMemoryContext);

	upperScan->strategy = GetAccessStrategy(BAS_BULKREAD);
	upperScan->locatorRecords =
		(LocatorRecord)palloc0(sizeof(LocatorRecordData) *
							   desc->parentRecordsPerRecordZone);

	/* If the layout type changes, allocate space for reformatting. */
	if (upperScan->layoutNeedsToBeChanged)
	{
		int i = 0;
		int tmpGroup;

		/* isnull, values array */
		upperScan->reformattingDesc.leftIsnull = (bool*)palloc0(natts);
		upperScan->reformattingDesc.rightIsnull = (bool*)palloc0(natts);
		upperScan->reformattingDesc.leftValues = (Datum*)palloc0(natts * sizeof(Datum));
		upperScan->reformattingDesc.rightValues = (Datum*)palloc0(natts * sizeof(Datum));

		/* tuples of each column group */
		upperScan->reformattingDesc.tuples = (char**)palloc(columnGroupCount *
		 													sizeof(char*));

		/* Allocate space for SIRO tuple. */
		if (lowerInsert->layoutType != COLUMNAR)
		{
			/* only for making SIRO tuple */
			upperScan->reformattingDesc.siroIsnull = (bool*)palloc0(natts);
			for (i = 0; i < natts; i++)
				upperScan->reformattingDesc.siroIsnull[i] = true;

			upperScan->reformattingDesc.tuples[0] =
				(char*)palloc0(2 * lowerInsert->tupleSize[i]);

			upperScan->reformattingDesc.tupleAttrCounts[0] = 0;

			i = 1;
		}
		else
			upperScan->reformattingDesc.siroIsnull = NULL;

		/* Allocate space for columnar tuples and init the attr count. */
		for (; i < columnGroupCount; i++)
		{
			upperScan->reformattingDesc.tuples[i] =
				(char*)palloc0(lowerInsert->tupleSize[i]);
			upperScan->reformattingDesc.tupleAttrCounts[i] = 0;
		}

		/* Set the attr count. */
		for (i = 0; i < natts; i++)
		{
			tmpGroup = columnarLayout->columnGroupArray[i];
			upperScan->reformattingDesc.tupleAttrs[tmpGroup][upperScan->reformattingDesc.tupleAttrCounts[tmpGroup]++] = i;
		}

		/* Set the blocking time. */
		upperScan->blockingTime = 100; /* us */

#ifdef LOCATOR_DEBUG
		fprintf(stderr, "[LOCATOR] upperScan->layoutNeedsToBeChanged == true;, pid: %d\n", getpid());
#endif /* LOCATOR_DEBUG */
	}

	lowerInsert->nprivatebuffer = (int) 
		pow(exCatalog->spreadFactor, targetPartitionLevel - partitionLevel);

	lowerInsert->startTargetPartitionNumber = 
		partitionNumber * lowerInsert->nprivatebuffer;

	/* Allocate private buffers. */
	lowerInsert->privateBuffers = (LocatorPartitionPrivateBuffer **)
		palloc0(sizeof(LocatorPartitionPrivateBuffer *) * 
								lowerInsert->nprivatebuffer);

	for (int i = 0; i < lowerInsert->nprivatebuffer; ++i)
	{
		Page initPageRecordZones;
		LocatorPartitionPrivateBuffer *privateBuffer = 
			(LocatorPartitionPrivateBuffer *)
				palloc0(sizeof(LocatorPartitionPrivateBuffer));

		/* 
		 * Set partition number and partition generation number to append 
		 * tuples. 
		 */
		privateBuffer->partitionNumber = 
			lowerInsert->startTargetPartitionNumber + i;
		privateBuffer->partitionGenerationNumber = 
			LocatorGetPartGenerationNumber(exCatalog, 
				lowerInsert->targetPartitionLevel, 
				privateBuffer->partitionNumber);

		privateBuffer->firstRecCnt = 
			exCatalog->realRecordsCount[targetPartitionLevel][privateBuffer->partitionNumber];
		privateBuffer->currentRecCnt = privateBuffer->firstRecCnt;

		/* Check partials. */
		for (int j = 0; j < lowerInsert->columnGroupCount; j++)
			privateBuffer->partials[j] = false;
		privateBuffer->partial = false;

		/* If a new file should be created, set the next generation number. */
		if (privateBuffer->firstRecCnt == 0 &&
				lowerInsert->targetPartitionLevel != exCatalog->lastPartitionLevel)
		{
			/* Move to next generation number. */
			if (privateBuffer->partitionGenerationNumber == 
													InvalidLocatorPartGenNo)
				privateBuffer->partitionGenerationNumber = 0;
			else
				privateBuffer->partitionGenerationNumber += 1;
		}

		/* Init page record zone array */
		initCapacity = (PRIVATEBUFFER_INIT_CAPACITY - 1) / blocksPerRecordZone + 1;
		privateBuffer->pageRecordZones =
			(Page *) palloc0(sizeof(Page) * initCapacity);
		privateBuffer->npageRecordZones = 0;
		privateBuffer->capacity = initCapacity;
		privateBuffer->alignments =
			(uint16 *) palloc0(sizeof(uint16) * PRIVATEBUFFER_ENLARGE_CAPACITY);
		privateBuffer->enlargeCount = 1;

		/* Allocate page record zones. (1 is for alignment(io_uring)) */
		initPageRecordZones =
			(Page) palloc(((int64_t) blocksPerRecordZone * initCapacity + 1) * BLCKSZ);
		alignment = (uint16)(BLCKSZ - ((uint64)initPageRecordZones % BLCKSZ));
		privateBuffer->alignments[0] = alignment;
		initPageRecordZones += alignment;

		/* Set page record zone pointer in new allocated page array. */
		for (int j = 0; j < initCapacity; ++j)
		{
			privateBuffer->pageRecordZones[j] = ((char *) initPageRecordZones) +
				blocksPerRecordZone * BLCKSZ * j;
		}

		lowerInsert->privateBuffers[i] = privateBuffer;
	}

	MemoryContextSwitchTo(oldContext);
}

/*
 * LocatorWritePartialRecordZone
 * 
 * At the end of repartitioning, we write partial pages to the shared memory.
 * We write only modified space, because there are existing tuples. Some workers
 * can access to existing tuples, so we get the exclusive lock on the page. 
 */
static int
LocatorWritePartialRecordZone(Relation relation, 
						 LocatorPartitionInsertDesc *lowerInsert,
						 LocatorPartitionPrivateBuffer *privateBuffer,
						 LocatorPartLevel targetPartitionLevel,
						 BufferAccessStrategy strategy, struct io_uring *ring,
						 LocatorLowerIOVec lowerIoVec)
{
	uint32 recordsPerRecordZone = 
				*(lowerInsert->columnarLayout->recordsPerRecordZone);
	BlockNumber recordZoneCount = privateBuffer->firstRecCnt / recordsPerRecordZone;
	uint32 lastRecordCount = privateBuffer->firstRecCnt % recordsPerRecordZone;

	BlockNumber	fulledBlockCount = recordZoneCount * lowerInsert->blocksPerRecordZone;
	BlockNumber	blockOffsetEnd = lowerInsert->blocksPerRecordZone;
	BlockNumber	blockOffsetStart;
	BlockNumber	blockOffset;
	BlockNumber	lastBlockNumber;

	Buffer 		buffer;
	Page		page;
	PageHeader 	phdr;
	PageHeader	dp;
	uint16		tupleSize;

	int flushCount = 0;

	Assert(privateBuffer->firstRecCnt != 0);

	for (int i = lowerInsert->columnGroupCount - 1; i >= 0 ; i--)
	{
		blockOffsetStart = lowerInsert->blockOffset[i];
		blockOffset = blockOffsetStart +
			lastRecordCount / (uint32)(lowerInsert->tuplesPerBlock[i]);
		lastBlockNumber = fulledBlockCount + blockOffset;
		page = (privateBuffer->pageRecordZones[0] + blockOffset * BLCKSZ);

		if (privateBuffer->partials[i])
		{
			/* Get the partition's last page before appending tuples. */
			buffer =
				ReadPartitionBufferForRepartitioning(relation, targetPartitionLevel, 
													 privateBuffer->partitionNumber,
													 privateBuffer->partitionGenerationNumber,
													 lastBlockNumber, RBM_NORMAL, strategy);

			dp = (PageHeader) BufferGetPage(buffer);
			phdr = (PageHeader)page;

			/* Lock the buffer. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			if (i == 0 && lowerInsert->layoutType != COLUMNAR)
			{
				LocationIndex prevLower = dp->pd_lower;
				LocationIndex prevUpper = dp->pd_upper;

				Assert(SizeOfPageHeaderData && prevLower <= phdr->pd_lower);
				Assert(phdr->pd_upper <= prevUpper && prevUpper <= BLCKSZ);

				/* Copy line items to the shared memory's buffer. */
				memcpy(((char*)dp) + prevLower,
					   ((char*)phdr) + prevLower,
					   phdr->pd_lower - prevLower);

				/* Copy tuples to the shared memory's buffer. */
				memcpy(((char*)dp) + phdr->pd_upper,
					   ((char*)phdr) + phdr->pd_upper,
					   prevUpper - phdr->pd_upper);

				pg_memory_barrier();

				/* Adjust lower and upper bound of the page. */
				dp->pd_lower = phdr->pd_lower;
				dp->pd_upper = phdr->pd_upper;

#ifdef LOCATOR_DEBUG
				fprintf(stderr, "[LOCATOR] partial, rel: %u, level: %u, partNum: %u, partGen: %u, blockNum: %u, prevLower: %hu, prevUpper: %hu, pid: %d\n",
						relation->rd_id, targetPartitionLevel, privateBuffer->partitionNumber, privateBuffer->partitionGenerationNumber, lastBlockNumber, prevLower, prevUpper, getpid());
#endif /* LOCATOR_DEBUG */
			}
			else
			{
				/* Get tupleSize. */
				tupleSize = lowerInsert->tupleSize[i];

				/* Sanity check. */
				Assert(dp->pd_lower == phdr->pd_lower &&
					   phdr->pd_lower == SizeOfPageHeaderData);
				Assert(dp->pd_upper == phdr->pd_upper &&
					   phdr->pd_upper == (BLCKSZ - (lowerInsert->tuplesPerBlock[i] * tupleSize)));
				Assert(dp->pd_special == lastRecordCount % lowerInsert->tuplesPerBlock[i]);
				Assert(phdr->pd_upper + (dp->pd_special * tupleSize) +
					   (phdr->pd_special - dp->pd_special) * tupleSize <= BLCKSZ);

				/* Copy the null bit array. */
				if (likely(i != lowerInsert->recordKeyGroupIndex))
				{
					uint8 attrCount = lowerInsert->attrCount[i];
					uint16 dp_nullBitCount = attrCount * dp->pd_special;
					uint16 phdr_nullBitCount = attrCount * phdr->pd_special;
					bits8 *dp_bp = (bits8*)((char*)dp + dp->pd_lower +
											(dp_nullBitCount >> 3));
					bits8 *phdr_bp = (bits8*)((char*)phdr + phdr->pd_lower +
											(dp_nullBitCount >> 3));
					uint32 bytes;

					Assert((*dp_bp & *phdr_bp) == 0);

					/* Copy the partial null byte. */
					*dp_bp |= *phdr_bp;

					/* Copy the rest of null bit array. */
					bytes = (TYPEALIGN(8, phdr_nullBitCount) -
							 TYPEALIGN(8, dp_nullBitCount)) >> 3;
					memcpy((void*)(dp_bp + 1), (void*)(phdr_bp + 1), bytes);
				}

				/* Copy the data. */
				memcpy(((char*)dp) + dp->pd_upper + (dp->pd_special * tupleSize),
					   ((char*)phdr) + phdr->pd_upper + (dp->pd_special * tupleSize),
					   (phdr->pd_special - dp->pd_special) * tupleSize);

				/* Adjust the number of tuples of the page. */
				dp->pd_special = phdr->pd_special;
			}

			MarkBufferDirty(buffer);

			/* Unlock and unpin the buffer. */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);

			/* Skip partial page. */
			lastBlockNumber++;
			blockOffset++;
			page += BLCKSZ;
		}

		Assert(blockOffsetEnd >= blockOffset);

		/* Write the remaining column group pages. */
		if (likely(blockOffsetEnd > blockOffset))
		{
			if (likely(ring != NULL))
			{
				IoVecs ioVecs = &(lowerIoVec->iovecsArray[i]);
				struct iovec *iov = (struct iovec *) palloc(sizeof(struct iovec));

				iov->iov_base = (void *)page,
				iov->iov_len = (blockOffsetEnd - blockOffset) * BLCKSZ;

				ioVecs->iovs = iov;
				ioVecs->iovcnt = 1;

				LocatorPrepareRequest(ring, lowerIoVec->fd, ioVecs->iovs,
									  lastBlockNumber, 1, ioVecs, false);
			}
			else
			{
				LocatorWritePartialColumnZone(lowerIoVec->fd, lastBlockNumber,
											   (char*)page,
											   blockOffsetEnd - blockOffset);
			}

			++flushCount;
		}

		blockOffsetEnd = blockOffsetStart;
	}

	return flushCount;
}

/*
 * LocatorWriteRemainRecordZones
 *
 * The repartitioning worker writes the remaining record zones stored in private
 * memory to the partition file. To minimize the number of system calls, we
 * utilize the 'pwritev' system call.
 */
static int
LocatorWriteRemainRecordZones(Relation relation, LocatorPartitionInsertDesc *lowerInsert,
						 LocatorPartitionPrivateBuffer *privateBuffer,
						 LocatorPartLevel targetPartitionLevel,
						 struct io_uring *ring, LocatorLowerIOVec lowerIoVec)
{
	int startRecordZoneIdx = privateBuffer->partial ? 1 : 0;
	BlockNumber recordZoneCount = 
					privateBuffer->npageRecordZones - startRecordZoneIdx;
	int iovsIdx;
	struct iovec *iovs;
	BlockNumber firstBlockNum;
	int64 blocksPerRecordZone = (int64)(lowerInsert->blocksPerRecordZone);
	int64 recordZoneSize = blocksPerRecordZone * BLCKSZ;
	int capacityUnit = (PRIVATEBUFFER_INIT_CAPACITY - 1) / blocksPerRecordZone + 1;
	int iovcnt;

	/* If we don't have any remain page to write, just return. */
	if (privateBuffer->npageRecordZones == 0 || recordZoneCount == 0)
		return 0;

	/* Allocate i/o vector. */
	iovcnt = (privateBuffer->npageRecordZones - 1) / capacityUnit + 1;
	iovs = (struct iovec *) palloc0(sizeof(struct iovec) * iovcnt);

	/* Set values within i/o vector. */
	for (iovsIdx = 0; iovsIdx < iovcnt; ++iovsIdx)
	{
		iovs[iovsIdx].iov_base = 
			(void *) privateBuffer->pageRecordZones[capacityUnit * iovsIdx];
		iovs[iovsIdx].iov_len = capacityUnit * recordZoneSize;

		if (iovsIdx == 0)
		{
			/* Set first iovec. */
			iovs[iovsIdx].iov_base = 
				((char *) iovs[iovsIdx].iov_base) + startRecordZoneIdx * recordZoneSize; 
			iovs[iovsIdx].iov_len -= startRecordZoneIdx * recordZoneSize;
		}

		if (iovsIdx == iovcnt - 1)
		{
			/* Set last iovec. */
			int64_t lastEmptyRecordZoneCnt = capacityUnit -
										((privateBuffer->npageRecordZones - 1) % 
											capacityUnit + 1);

			iovs[iovsIdx].iov_len -= lastEmptyRecordZoneCnt * recordZoneSize;
		}

		Assert((((uint64) iovs[iovsIdx].iov_base) % BLCKSZ) == 0 && 
								iovs[iovsIdx].iov_len >= 0);
	}

	/* Get block number to start appending tuples. */
	if (privateBuffer->firstRecCnt == 0)
	{
		Assert(privateBuffer->partial == false);
		firstBlockNum = 0;
	}
	else
	{
		firstBlockNum = (((privateBuffer->firstRecCnt - 1) /
						  *(lowerInsert->columnarLayout->recordsPerRecordZone)) + 1) *
								lowerInsert->blocksPerRecordZone;
	}

	if (likely(ring != NULL))
	{
		IoVecs ioVecs = &(lowerIoVec->iovecsArray[lowerInsert->columnGroupCount]);

		ioVecs->iovs = iovs;
		ioVecs->iovcnt = iovcnt;

		LocatorPrepareRequest(ring, lowerIoVec->fd, iovs,
							  firstBlockNum, iovcnt, ioVecs, false);
	}
	else
	{
		/* Write remain pages. */
		LocatorWriteBatch(lowerIoVec->fd, firstBlockNum, iovs, iovcnt);

		/* Resource cleanup. */
		pfree(iovs);
	}

	return 1;
}

/*
 * LocatorEndRepartitioning
 *
 * At the end of repartitioning, we write the pages stored in private memory 
 * either to shared memory or directly to a file. Additionally, we perform 
 * resource cleanup tasks, such as 'pfree'. 
 */
static void
LocatorEndRepartitioning(LocatorRepartitioningDesc *desc)
{
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	Relation relation = desc->relation;
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	pg_atomic_uint64 *refcounter;
	uint32 recordsPerRecordZone = *(lowerInsert->columnarLayout->recordsPerRecordZone);
	MemoryContext oldContext;
	struct io_uring *ring = desc->ring;
	LocatorLowerIOVec lowerIoVecs;
	int nprivatebuffer = lowerInsert->nprivatebuffer;
	uint16 columnGroupCount = lowerInsert->columnGroupCount;
	int ret, flushCount = 0;
	int flags = enableFsync ? O_SYNC : 0;

	oldContext = MemoryContextSwitchTo(desc->descTopMemoryContext);

	/* Allocate memory space to flush lower partitions. */
	lowerIoVecs = (LocatorLowerIOVec) palloc0(sizeof(LocatorLowerIOVecData) *
											  nprivatebuffer);

	/* Check if io_uring is available. */
	if (unlikely(enable_uring_partitioning && ring == NULL))
		elog(WARNING, "io_uring_queue_init failed, (partitioning worker will use pwritev())");

	/* Iterate all private buffer to write. */
	for (int idx = 0; idx < nprivatebuffer; ++idx)
	{
		LocatorPartitionPrivateBuffer *privateBuffer = 
											lowerInsert->privateBuffers[idx];
		LocatorLowerIOVec lowerIoVec = lowerIoVecs + idx;
		int prevRecordZoneNum, curRecordZoneNum;

		if (unlikely(privateBuffer->currentRecCnt == privateBuffer->firstRecCnt))
		{
			lowerIoVec->fd = -1;
			continue;
		}

		Assert(privateBuffer->currentRecCnt > privateBuffer->firstRecCnt);

		/*
		 * columnGroupCount is for partial record zone. (each column group)
		 * 1 is for remain record zones.
		 */
		lowerIoVec->iovecsArray =
			(IoVecs) palloc0(sizeof(IoVecsData) * (columnGroupCount + 1));

		if (privateBuffer->firstRecCnt == 0)
		{
			/* Create partition file if not exist. */
			lowerIoVec->fd =
				LocatorCreatePartitionFile(relation->rd_node, 
										   lowerInsert->targetPartitionLevel,
										   privateBuffer->partitionNumber,
										   privateBuffer->partitionGenerationNumber,
										   flags);

			/* Get prev block number. */
			prevRecordZoneNum = -1;
		}
		else
		{
			lowerIoVec->fd = 
				LocatorOpenPartitionFile(relation->rd_node, 
										 lowerInsert->targetPartitionLevel,
										 privateBuffer->partitionNumber,
										 privateBuffer->partitionGenerationNumber,
										 flags);

			/* If there are partial pages to write, we write those pages first. */
			if (privateBuffer->partial)
			{
				flushCount +=
					LocatorWritePartialRecordZone(relation, lowerInsert,
											 privateBuffer,
											 lowerInsert->targetPartitionLevel,
											 upperScan->strategy, ring,
											 lowerIoVec);
			}

			/* Get prev block number. */
			prevRecordZoneNum = 
				(privateBuffer->firstRecCnt - 1) / recordsPerRecordZone;
		}

		Assert(lowerIoVec->fd >= 0);
		pg_memory_barrier();

		/* Write remain pages calling one pwritev. */
		flushCount +=
			LocatorWriteRemainRecordZones(relation, lowerInsert, privateBuffer,
									 lowerInsert->targetPartitionLevel, ring,
									 lowerIoVec);

		/* Get current block number. */
		if (privateBuffer->currentRecCnt == 0)
			curRecordZoneNum = -1;
		else
			curRecordZoneNum = 
				(privateBuffer->currentRecCnt - 1) / recordsPerRecordZone;

		/* 
		 * Increment the number of blocks that we created for child partitions. 
		 */
		desc->childRecordZones += (curRecordZoneNum - prevRecordZoneNum);

#ifdef LOCATOR_DEBUG
		fprintf(stderr, "[LOCATOR] LocatorEndRepartitioning, idx: %d, prev: %d, curr: %d, added block amount: %u (relid: %d, level: %d, number: %d)\n",
						idx, prevRecordZoneNum, curRecordZoneNum, (curRecordZoneNum - prevRecordZoneNum) * lowerInsert->blocksPerRecordZone, 
						relation->rd_node.relNode, lowerInsert->targetPartitionLevel, privateBuffer->partitionNumber);
		fprintf(stderr, "[LOCATOR] LocatorEndRepartitioning, unstable records: %d\n", upperScan->unstableRecords);
#endif /* LOCATOR_DEBUG */
	}

	/* Start flush and wait for it. */
	if (likely(ring != NULL))
	{
		struct io_uring_cqe *cqe;
		int reqIdx = 0;

#ifdef LOCATOR_DEBUG
		IoVecs ioVecs;
#endif /* LOCATOR_DEBUG */

		/* Submit prepared requests. */
		LocatorSubmitRequest(ring);

		/* Wait for the io_uring completion. */
		while (reqIdx < flushCount)
		{
#ifdef LOCATOR_DEBUG
			size_t nbytes = 0;
#endif /* LOCATOR_DEBUG */

			CHECK_FOR_INTERRUPTS();

			ret = io_uring_wait_cqe(ring, &cqe);
		
			if (ret)
			{
				if (ret == -EINTR)
				{
					/* 
					 * We receive an interrupt during the execution of the 
					 * io_uring_wait_cqe function. 
					 */
					CHECK_FOR_INTERRUPTS();
					continue;
				}
				else
				{
#ifdef ABORT_AT_FAIL
					Abort("LocatorEndRepartitioning, get invalid return from io_uring");
#else
					elog(PANIC, "partitioning worker: get invalid return from io_uring, %d", 
								ret);
#endif
				}
			}

			Assert(cqe != NULL);

#ifdef LOCATOR_DEBUG
			ioVecs = (IoVecs)io_uring_cqe_get_data(cqe);
			for (int i = 0; i < ioVecs->iovcnt; ++i)
				nbytes += ioVecs->iovs[i].iov_len;
			Assert(cqe->res == nbytes);
#endif /* LOCATOR_DEBUG */

			/* Clean up this entry. */
			io_uring_cqe_seen(ring, cqe);

			++reqIdx;
		}

		Assert(io_uring_peek_cqe(ring, &cqe) == -EAGAIN && !cqe);
	}

	/*
	 * Repartitioning at checkpoint is executed when the PostgreSQL server shuts
	 * down, so no delta is created.
	 */
	if (likely(upperScan->isPartitioningWorker))
	{
		/* Get upper partition's reference counter pointer. */
		refcounter = 
			LocatorExternalCatalogRefCounter(exCatalog, 
				upperScan->partitionLevel, upperScan->partitionNumber);

		/* Prepare buffers for deltas before blocking. */
		PreallocSharedBuffersForDeltas(locatorPartitionDsaArea, desc);

		/* 
		 * The simple memory copy phase is done. Now, we need to fix 
		 * uncorrected records within private buffers. 
		 */
		SetRepartitioningStatusModificationBlocking(refcounter);
		
		/* Prepare left buffers for deltas after blocking. */
		PreallocSharedBuffersForDeltas(locatorPartitionDsaArea, desc);

		/* Apply delta to uncorrected records. */
		ApplyLocatorDeltas(locatorPartitionDsaArea, desc);
	}

	/*
	 * Checkpointer uses backed up relation data, so it didn't open any
	 * relation.
	 */
	if (likely(upperScan->isPartitioningWorker))
	{
		/* Files close. */
		for (int idx = 0; idx < nprivatebuffer; ++idx)
		{
			LocatorLowerIOVec lowerIoVec = lowerIoVecs + idx;

			/* IO vectors cleanup. */
			if (lowerIoVec->fd != -1)
			{
				Assert(lowerIoVec->iovecsArray != NULL);
				LocatorSimpleClose(lowerIoVec->fd);
			}
		}

		MemoryContextSwitchTo(oldContext);

		/* Close relation. */
		table_close(relation, AccessShareLock);
		CommitTransactionCommand();
	}
	else
	{
		for (int idx = 0; idx < nprivatebuffer; ++idx)
		{
			LocatorLowerIOVec lowerIoVec = lowerIoVecs + idx;

			/* IO vectors cleanup. */
			if (lowerIoVec->fd != -1)
			{
				Assert(lowerIoVec->iovecsArray != NULL);

				LocatorSimpleClose(lowerIoVec->fd);
			}
		}
	}
}

/*
 * LocatorGetRecordFromPartitions
 */
static LocatorRecord
LocatorGetRecordFromPartitions(LocatorRepartitioningDesc *desc)
{
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorRecord record;

	for (;;)
	{
		/*
		 * Get a record to do repartitioning into downside partition.
		 */
		record = LocatorPartitionGetRecord(upperScan);

		if (unlikely(record == NULL))
		{
#ifdef LOCATOR_DEBUG
			fprintf(stderr, "[LOCATOR] one partition was repartitioned, level: %u, number: %lu, ntuple: %lu\n",
							upperScan->partitionLevel, upperScan->partitionNumber, upperScan->repartitionedNrecord_partition);
			if (upperScan->partitionLevel == 0 && upperScan->isPartitioningWorker &&
				upperScan->repartitionedNrecord_partition < desc->exCatalog->recordNumsPerBlock * 2048)
			{
				char errmsg[32];
				sprintf(errmsg, "cur record zone: %u, nrecordzones: %u", upperScan->currentRecordZone, upperScan->nrecordzones);
				Abort(errmsg);
			}
			upperScan->repartitionedNrecord_partition = 0;
#endif /* LOCATOR_DEBUG */

			if ((upperScan->partitionLevel == 0) && 
				(upperScan->currentPartitionGenerationNumber < upperScan->maxTieredNumber))
			{
				/* Set the next partition scan descriptor. */
				upperScan->currentPartitionGenerationNumber += 1; 

				/*
				 * Set the number of pages of current partition.
				 * (See LocatorBeginRepartitioning() for details)
				 */
				if (likely(upperScan->isPartitioningWorker))
				{
					/* Normal case: partitioning worker */
					upperScan->nrecordzones = LocatorMempartPageCount;
				}
				else
				{
					/* PostgreSQL server is shutting down: checkpointer */
					upperScan->nrecordzones = 
						(upperScan->currentPartitionGenerationNumber == 
							desc->exCatalog->CurrentTieredNumberLevelZero[upperScan->partitionNumber]) ?
						NRecordZonesUsingRecordCount(
							desc->exCatalog->CurrentRecordCountsLevelZero[upperScan->partitionNumber].val,
							desc->parentRecordsPerRecordZone) : /* nblocks == nrecordzones in level 0 */
						LocatorMempartPageCount;
				}

				upperScan->firstScan = true;

#ifdef LOCATOR_DEBUG
				fprintf(stderr, "[LOCATOR] LocatorGetRecordFromPartitions, level: %u, number: %u, nrecordzones: %u\n",
								upperScan->partitionLevel, upperScan->partitionNumber, upperScan->nrecordzones);
#endif /* LOCATOR_DEBUG */

				desc->parentRecordZones += upperScan->nrecordzones;
			}
			else
			{
				/* End scan */
				return NULL;
			}
		}
		else
		{
			/* Reformat the record if necessary. */
			if (upperScan->layoutNeedsToBeChanged)
				LocatorReformatLocatorRecord(desc, record);

			return record;
		}
	}

	Assert(false);

	/* quiet compiler */
	return NULL;
}


/*
 * LocatorRepartitioning - repartitioning main entry
 * 
 * We redistribute upper partition's records to lower partitions. When we get 
 * records from upper partition, we access to shared buffer. When we move 
 * records to lower partitions, we save tuples to private memory.
 *
 * (level)
 *    n     ▢▢▢                 <-- upper partition
 *           ↓---↘----↘----↘
 *   n+1    ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢  <-- lower partitions
 */
void
LocatorRepartitioning(LocatorRepartitioningDesc *desc, Oid relationOid,
					  uint64 recordCount,
					  LocatorPartLevel upperPartitionLevel,
					  LocatorPartNumber upperPartitionNumber,
					  LocatorTieredNumber upperMinTieredNumber,
					  LocatorTieredNumber upperMaxTieredNumber, 
					  LocatorPartLevel targetPartitionLevel,
					  uint32 *reorganizedRecCnt,
					  LocatorSeqNum *firstSeqNums, bool isPartitioningWorker)
{
	uint32 repartitionedNrecord = 0;
	LocatorRecord upperRecord;
	uint32 childIndex;

	/* Init repartitioning descriptor. */
	LocatorBeginRepartitioning(desc, relationOid, upperPartitionLevel,
							   upperPartitionNumber,
							   upperMinTieredNumber, upperMaxTieredNumber,
							   targetPartitionLevel, isPartitioningWorker);
	
	for (;;)
	{
		/* Retrieve a record from the target partitions. */
		upperRecord = LocatorGetRecordFromPartitions(desc);

		if (unlikely(upperRecord == NULL))
		{
#ifdef LOCATOR_DEBUG
			if (repartitionedNrecord != recordCount)
			{
				char errmsg[32];
				sprintf(errmsg, "repartitionedNrecord != recordCount (%u, %lu)",
						repartitionedNrecord, recordCount);
				Abort(errmsg);
			}
#else /* !LOCATOR_DEBUG */
			Assert(repartitionedNrecord == recordCount);
#endif /* LOCATOR_DEBUG */

			/* When the return value is NULL, the scan is complete. */
			LocatorEndRepartitioning(desc);

			return;
		}

		repartitionedNrecord += 1;

		/* 
		 * The retrieved record is added to the lower partitions at the target 
		 * level. 
		 */
		childIndex = LocatorPartitionAppend(desc, upperRecord, firstSeqNums);
		pg_memory_barrier();

		/* Memoize the number of repartitioned records */
		reorganizedRecCnt[childIndex]++;
	}
}

/* ----------------------------------------------------------------
 *			 locator partition access method interface
 * ----------------------------------------------------------------
 */

/*
 * LocatorGetSiroPage - subroutine for LocatorPartitionGetRecord()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
static void
LocatorGetSiroPage(LocatorPartitionScanDesc *upperScan, BlockNumber page,
				   int currentIndex)
{
	Relation	relation = upperScan->relation;
	Buffer		buffer;
	Page		dp;
	PageHeader	phdr;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	Item		pLocator;
	HeapTupleData loctup;

	/* release previous scan buffer, if any */
	if (BufferIsValid(upperScan->columnGroups[0].currentBuffer))
	{
		UnlockReleaseBuffer(upperScan->columnGroups[0].currentBuffer);
		upperScan->columnGroups[0].currentBuffer = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	buffer =
		ReadPartitionBufferForRepartitioning(upperScan->relation, 
											 upperScan->partitionLevel, 
											 upperScan->partitionNumber, 
											 upperScan->currentPartitionGenerationNumber,
											 page, RBM_NORMAL, upperScan->strategy);

	upperScan->columnGroups[0].currentBlock = page;
	upperScan->columnGroups[0].currentBuffer = buffer;

	/*
	 * This prevents repartitioning a mempartition where no actual record has
	 * been inserted yet.
	 * Repartitioning immediately after tiering while records have not yet been
	 * inserted will cause problems, so ensure that all records in the tiered
	 * mempartition are inserted before proceeding with repartitioning.
	 * In practice, this is very unlikely to happen, and in fact, the above
	 * problem has never occurred even when we didn't add this code.
	 * However, it is theoretically possible, so we add this code to ensure
	 * stability.
	 */
	if (unlikely(LocatorCheckMempartBuf(buffer - 1) && page == 0 &&
				 upperScan->isPartitioningWorker))
	{
#ifdef LOCATOR_DEBUG
		fprintf(stderr, "[LOCATOR] mempart, rel: %u, cnt: %u, buf: %p, pid: %d\n",
						upperScan->relation->rd_id, GetInsertedRecordsCount(buffer),
						GetBufferDescriptor(LocatorGetMempartBufId(buffer - 1)),
						(int)getpid());
#endif /* LOCATOR_DEBUG */
		WaitForInsert(buffer, upperScan);
	}

	loctup.t_tableOid = relation->rd_node.relNode;

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	lines = PageGetMaxOffsetNumber(dp);
	ntup = 0;
	phdr = (PageHeader)dp;

	Assert(!PageIsCOLUMNAR(dp));

	for (lineoff = FirstOffsetNumber, lpp = PageGetItemId(dp, lineoff);
		 lineoff <= lines;
		 lineoff++, lpp++)
	{
		SiroTuple	versionLocTup;
		uint32	   *p_pd_gen;
		HeapTupleHeader tuple;
		VersionVisibilityStatus visibilityStatus[2] = { VERSION_INSERTED };

#ifdef LOCATOR_DEBUG
		int test_sum = 0;
#endif /* LOCATOR_DEBUG */

		/* Don't read version directly */
		if (!LP_IS_PLEAF_FLAG(lpp))
			continue;

		/* Get p-locator of this record */
		pLocator = PageGetItem(dp, lpp);
		p_pd_gen = (uint32*)((char *) pLocator + PITEM_GEN_OFF);

		versionLocTup = &(upperScan->locatorRecords[currentIndex + ntup].siroTuple);

		versionLocTup->plocatorData = pLocator;
		versionLocTup->plocatorLen = ItemIdGetLength(lpp);

recheck:
		versionLocTup->leftVersionData = NULL;
		versionLocTup->leftVersionLen = 0;
		versionLocTup->rightVersionData = NULL;
		versionLocTup->rightVersionLen = 0;

		Assert(versionLocTup->plocatorLen == PITEM_SZ);

		/* Check pd_gen */
		if (*p_pd_gen == phdr->pd_gen)
			versionLocTup->isUpdated = true;
		else
			versionLocTup->isUpdated = false;
		
		if (LP_OVR_IS_LEFT(lpp))
			versionLocTup->isLeftMostRecent = true;
		else
			versionLocTup->isLeftMostRecent = false;

		versionLocTup->isAborted = false;

		for (int veroff = CHECK_LEFT; veroff <= CHECK_RIGHT; ++veroff)
		{
			OffsetNumber	versionOffset = InvalidOffsetNumber;
			BlockNumber		versionBlock;
			Page			versionDp;
			ItemId			versionLpp;

			/* Set block number and offset */
			versionBlock = page;
			versionOffset = lineoff + veroff;
			Assert(versionBlock == page && versionOffset != 0);

			/* Set self Pointer of tuple */
			ItemPointerSet(&(loctup.t_self), versionBlock, versionOffset);

			/*
			 * If the version is in the same page with p-locator, just get it.
			 * Or not, read the buffer that it is in.
			 */
			if (likely(versionBlock == page))
			{
				//versionBuffer = buffer;
				versionDp = dp;
			}
			else
			{
				/* XXX: We are not serving appended update yet. */
				Assert(false);
				// versionBuffer = ReadBuffer(upperScan->relation, versionBlock);
				// Assert(BufferIsValid(versionBuffer));
				// versionDp = BufferGetPage(versionBuffer);

				// upperScan->recordBuffers[upperScan->pinOtherBufferNums] = versionBuffer;
				// upperScan->pinOtherBufferNums += 1;
			}

			versionLpp = PageGetItemId(versionDp, versionOffset);

			/* The target has never been updated after INSERT */
			if (unlikely(LP_OVR_IS_UNUSED(versionLpp)))
				continue;

			tuple = (HeapTupleHeader) PageGetItem(versionDp, versionLpp);

			if (veroff == CHECK_LEFT)
			{
				versionLocTup->leftVersionData = tuple;
				versionLocTup->leftVersionLen = ItemIdGetLength(versionLpp);
				Assert(versionLocTup->leftVersionData->t_choice.t_heap.t_xmin != 
														InvalidTransactionId);
	
				if (versionLocTup->leftVersionLen != 0)
				{
					loctup.t_data = versionLocTup->leftVersionData; 
					loctup.t_len = versionLocTup->leftVersionLen;

					if (upperScan->partitionLevel != 0)
					{
						visibilityStatus[0] =
							HeapTupleGetVisibilityStatus(&loctup, buffer);
					}
				}
			}
			else if (veroff == CHECK_RIGHT)
			{
				versionLocTup->rightVersionData = tuple;
				versionLocTup->rightVersionLen = ItemIdGetLength(versionLpp);
				Assert(versionLocTup->rightVersionData->t_choice.t_heap.t_xmin != 
															InvalidTransactionId);

				if (versionLocTup->rightVersionLen != 0)
				{
					loctup.t_data = versionLocTup->rightVersionData; 
					loctup.t_len = versionLocTup->rightVersionLen;

					if (upperScan->partitionLevel != 0)
					{
						visibilityStatus[1] =
							HeapTupleGetVisibilityStatus(&loctup, buffer);
					}
				}
			}
		}

		/*
		 * Rare case: If INSERT transaction is still in progess during
		 * reformatting, wait for the transaction.
		 */
		if (unlikely(upperScan->layoutNeedsToBeChanged &&
					 versionLocTup->rightVersionData == NULL &&
					 visibilityStatus[0] == VERSION_INPROGRESS))
		{
			elog(WARNING, "INSERT trx is still in progress during reformatting, sleep %lu micro seconds.", upperScan->blockingTime);
			pg_usleep(upperScan->blockingTime);
			elog(WARNING, "partitioning worker wakes up.");
			upperScan->blockingTime <<= 1; /* doubling */
			goto recheck;
		}

		Assert(versionLocTup->leftVersionData != NULL);
		Assert(visibilityStatus[0] != VERSION_ABORTED ||
			   visibilityStatus[1] != VERSION_ABORTED);

		ntup += 1;

		if (visibilityStatus[0] == VERSION_ABORTED &&
			versionLocTup->rightVersionData == NULL)
			versionLocTup->isAborted = true;

#ifdef LOCATOR_DEBUG
		if (test_sum != 3)
			upperScan->unstableRecords++;
#endif /* LOCATOR_DEBUG */
	}

	Assert(ntup == lines/3);
	Assert(ntup <= MaxHeapTuplesPerPage);
	upperScan->columnGroups[0].currentIndex = 0;
	upperScan->columnGroups[0].nrecords = ntup;

#ifdef LOCATOR_DEBUG
	if (ntup == 0 || ntup != lines/3)
	{
		char errmsg[32];
		sprintf(errmsg, "ntup: %d, lines/3: %d", ntup, lines/3);
		Abort(errmsg);
	}

	upperScan->repartitionedNrecord_partition += ntup;
#endif /* LOCATOR_DEBUG */
}

static void
LocatorGetColumnarPage(LocatorPartitionScanDesc *upperScan, BlockNumber page,
					   int currentIndex, int columnGroupIndex)
{
	Buffer			buffer;
	Page			dp;
	PageHeader		phdr;
	int				lines;
	int				ntup;

	bits8			*bp;
	bits8			nullBit;
	Item			item;
	uint16			tupleSize;
	ColumnarTuple	columnarTuple;
	uint8			attrCount = upperScan->attrCount[columnGroupIndex];

	/* release previous scan buffer, if any */
	if (BufferIsValid(upperScan->columnGroups[columnGroupIndex].currentBuffer))
	{
		UnlockReleaseBuffer(upperScan->columnGroups[columnGroupIndex].currentBuffer);
		upperScan->columnGroups[columnGroupIndex].currentBuffer = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	buffer =
		ReadPartitionBufferForRepartitioning(upperScan->relation, 
											 upperScan->partitionLevel, 
											 upperScan->partitionNumber, 
											 upperScan->currentPartitionGenerationNumber,
											 page, RBM_NORMAL, NULL);

	upperScan->columnGroups[columnGroupIndex].currentBlock = page;
	upperScan->columnGroups[columnGroupIndex].currentBuffer = buffer;

	/* Column group can't be in mempart */
	Assert(!LocatorCheckMempartBuf(buffer - 1));

	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	phdr = (PageHeader)dp;
	lines = phdr->pd_special; /* pd_special is tuple count at columnar page */

	Assert(PageIsCOLUMNAR(dp));

	bp = (bits8*)(dp + phdr->pd_lower); /* null-bit array */
	nullBit = 0x01;
	tupleSize = upperScan->columnarLayout->columnGroupMetadataArray[columnGroupIndex].tupleSize;

	for (ntup = 0, item = dp + phdr->pd_upper; ntup < lines;
		 ntup++, item += tupleSize)
	{
		columnarTuple =
			&(upperScan->locatorRecords[currentIndex + ntup].columnarTuple[columnGroupIndex]);

		/* Set data pointer */
		columnarTuple->columnarTupleData = item;

		for (int j = 0; j < attrCount; j++)
		{
			/* Check if is it non-null */
			if (likely(*bp & nullBit))
				columnarTuple->isnull[j] = false;
			else
				columnarTuple->isnull[j] = true;

			nullBit <<= 1;
			if (nullBit == 0)
			{
				bp += 1;
				nullBit = 0x01;
			}
		}
	}

	Assert(ntup == lines);
	Assert(ntup <= MaxColumnarTuplesPerPage);
	upperScan->columnGroups[columnGroupIndex].currentIndex = 0;
	upperScan->columnGroups[columnGroupIndex].nrecords = ntup;
}

static void
LocatorGetRecordKeyPage(LocatorPartitionScanDesc *upperScan, BlockNumber page,
						int currentIndex, int columnGroupIndex)
{
	Buffer		buffer;
	Page		dp;
	PageHeader	phdr;
	int			lines;
	int			ntup;

	Item		item;
	uint16		tupleSize;

	Abort("COLUMNAR layout is TODO");

	/* release previous scan buffer, if any */
	if (BufferIsValid(upperScan->columnGroups[columnGroupIndex].currentBuffer))
	{
		UnlockReleaseBuffer(upperScan->columnGroups[columnGroupIndex].currentBuffer);
		upperScan->columnGroups[columnGroupIndex].currentBuffer = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.  Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	buffer =
		ReadPartitionBufferForRepartitioning(upperScan->relation, 
											 upperScan->partitionLevel, 
											 upperScan->partitionNumber, 
											 upperScan->currentPartitionGenerationNumber,
											 page, RBM_NORMAL, NULL);

	upperScan->columnGroups[columnGroupIndex].currentBlock = page;
	upperScan->columnGroups[columnGroupIndex].currentBuffer = buffer;

	/* Column group can't be in mempart */
	Assert(!LocatorCheckMempartBuf(buffer - 1));

	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	dp = BufferGetPage(buffer);
	phdr = (PageHeader)dp;
	lines = phdr->pd_special; /* pd_special is tuple count at columnar page */

	Assert(PageIsCOLUMNAR(dp));

	tupleSize = upperScan->columnarLayout->columnGroupMetadataArray[columnGroupIndex].tupleSize;

	/* Copy the partition id */
	for (ntup = 0, item = (Item)(dp + phdr->pd_upper);
		 ntup < lines; ntup++, item += tupleSize)
		upperScan->locatorRecords[currentIndex + ntup].recordKey = *((LocatorRecordKey*)item);

	Assert(ntup == lines);
	Assert(ntup <= MaxColumnarTuplesPerPage);
	upperScan->columnGroups[columnGroupIndex].currentIndex = 0;
	upperScan->columnGroups[columnGroupIndex].nrecords = ntup;
}

/*
 * LocatorPartitionGetRecord
 */
LocatorRecord
LocatorPartitionGetRecord(LocatorPartitionScanDesc *upperScan)
{
	BlockNumber	page, firstPageofRecordZone;
	bool		finished;
	bool		recordZoneFinished = false;
	int			pagesToGet[MAX_COLUMN_GROUP_COUNT + 1];
	int			currentIndex;
	int			columnGroupIndex, newPageCount;
	uint16		columnGroupCount = upperScan->columnGroupCount;

	/*
	 * Calculate next starting currentIndex.
	 */
	if (unlikely(upperScan->firstScan))
	{
		/*
		 * return null immediately if relation is empty
		 */
		if (upperScan->nrecordzones == 0)
		{
			upperScan->currentRecord = NULL;
			return NULL;
		}

		/* Init variables. */
		currentIndex = 0; /* index of records in the record zone */
		newPageCount = 0;

		/* Get new pages. */
		for (columnGroupIndex = 0; columnGroupIndex < columnGroupCount; columnGroupIndex++)
		{
			page = upperScan->columnarLayout->columnGroupMetadataArray[columnGroupIndex].blockOffset; /* first page */
			upperScan->columnGroups[columnGroupIndex].currentIndex = 0; /* index of tuples in the column group */

			if (columnGroupIndex == 0 && upperScan->layoutType != COLUMNAR)
				LocatorGetSiroPage(upperScan, page, currentIndex);
			else if (unlikely(columnGroupIndex == upperScan->recordKeyGroupIndex))
				LocatorGetRecordKeyPage(upperScan, page, currentIndex, columnGroupIndex);
			else
				LocatorGetColumnarPage(upperScan, page, currentIndex, columnGroupIndex);
		}

		upperScan->currentRecordZone = 0;
		upperScan->firstScan = false;
	}
	else
	{
		/* next record */
		currentIndex = upperScan->currentIndex + 1;

		/* continue from previously returned page/record */
		for (columnGroupIndex = 0, newPageCount = 0;
			 columnGroupIndex < columnGroupCount; columnGroupIndex++)
		{
			/* Check which column groups need to get a new page. */
			if (unlikely(++(upperScan->columnGroups[columnGroupIndex].currentIndex) ==
				upperScan->columnGroups[columnGroupIndex].nrecords))
			{
				pagesToGet[newPageCount] = columnGroupIndex;
				newPageCount++;
			}
		}

		/*
		 * If all column groups need to get a new page at the same time, it
		 * means that this record zone is finished.
		 */
		if (unlikely(newPageCount == columnGroupCount))
			recordZoneFinished = true;
	}

	/* Get new pages. */
	if (!recordZoneFinished)
	{
		for (int i = 0; i < newPageCount; i++)
		{
			columnGroupIndex = pagesToGet[i];
			page = upperScan->columnGroups[columnGroupIndex].currentBlock + 1;

			if (columnGroupIndex == 0 && upperScan->layoutType != COLUMNAR)
				LocatorGetSiroPage(upperScan, page, currentIndex);
			else if (unlikely(columnGroupIndex == upperScan->recordKeyGroupIndex))
				LocatorGetRecordKeyPage(upperScan, page, currentIndex, columnGroupIndex);
			else
				LocatorGetColumnarPage(upperScan, page, currentIndex, columnGroupIndex);
		}
	}

	/*
	 * advance the scan until we find a qualifying record or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		if (!recordZoneFinished)
		{
			upperScan->currentRecord = &(upperScan->locatorRecords[currentIndex]);
			upperScan->currentIndex = currentIndex;

			Assert(upperScan->currentRecord->siroTuple.plocatorLen == PITEM_SZ);
			Assert(upperScan->currentRecord->siroTuple.leftVersionData->t_choice.t_heap.t_xmin != InvalidTransactionId);
			Assert(upperScan->currentRecord->siroTuple.rightVersionData == NULL ||
				   upperScan->currentRecord->siroTuple.rightVersionData->t_choice.t_heap.t_xmin != InvalidTransactionId);

			return upperScan->currentRecord;
		}

		finished = (++(upperScan->currentRecordZone) == upperScan->nrecordzones);

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (unlikely(finished))
		{
			for (columnGroupIndex = 0; columnGroupIndex < columnGroupCount; columnGroupIndex++)
			{
				/* Release all buffers. */
				if (BufferIsValid(upperScan->columnGroups[columnGroupIndex].currentBuffer))
					UnlockReleaseBuffer(upperScan->columnGroups[columnGroupIndex].currentBuffer);

				upperScan->columnGroups[columnGroupIndex].currentBuffer = InvalidBuffer;
				upperScan->columnGroups[columnGroupIndex].currentBlock = InvalidBlockNumber;
			}

			upperScan->currentRecord = NULL;
			upperScan->firstScan = false;
			return NULL;
		}

		currentIndex = 0;

		/* Get new pages. */
		firstPageofRecordZone = upperScan->currentRecordZone * upperScan->blocksPerRecordZone;
		for (columnGroupIndex = 0; columnGroupIndex < columnGroupCount; columnGroupIndex++)
		{
			page = firstPageofRecordZone + upperScan->columnarLayout->columnGroupMetadataArray[columnGroupIndex].blockOffset;
			upperScan->columnGroups[columnGroupIndex].currentIndex = 0;

			if (columnGroupIndex == 0 && upperScan->layoutType != COLUMNAR)
				LocatorGetSiroPage(upperScan, page, currentIndex);
			else if (unlikely(columnGroupIndex == upperScan->recordKeyGroupIndex))
				LocatorGetRecordKeyPage(upperScan, page, currentIndex, columnGroupIndex);
			else
				LocatorGetColumnarPage(upperScan, page, currentIndex, columnGroupIndex);
		}

		recordZoneFinished = false;
	}
}

/*
 * EnlargePrivateBufferPageArray
 */
static void
EnlargePrivateBufferPageArray(LocatorRepartitioningDesc *desc,
							  LocatorPartitionPrivateBuffer *privateBuffer)
{
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	int64_t	blocksPerRecordZone = lowerInsert->blocksPerRecordZone;
	Page	*oldPageArray = privateBuffer->pageRecordZones;
	Page	*newPageArray;
	int64_t	newCapacity;
	Page	newPageRecordZones;
	Size	newPageRecordZonesSize =
			(privateBuffer->capacity * blocksPerRecordZone + 1) * BLCKSZ;
	MemoryContext oldContext;
	uint16	alignment;
	int64_t	increasedCapacity;
	int		enlargeCount = privateBuffer->enlargeCount;
	int i, j;

	if (privateBuffer->npageRecordZones < privateBuffer->capacity)
		return;

	oldContext = MemoryContextSwitchTo(desc->descTopMemoryContext);

	/* Check capacity. */
	if (AllocSizeIsValid(newPageRecordZonesSize))
		increasedCapacity = privateBuffer->capacity;
	else
	{
		increasedCapacity = ((MaxAllocSize / BLCKSZ) - 1) / blocksPerRecordZone;
		newPageRecordZonesSize = (increasedCapacity * blocksPerRecordZone + 1) * BLCKSZ;
	}

	pg_memory_barrier();

	newCapacity = privateBuffer->capacity + increasedCapacity;
	newPageArray = (Page *) palloc0(sizeof(Page) * newCapacity);

	/* Copy elements of old array. */
	memcpy(newPageArray, oldPageArray, sizeof(Page) * privateBuffer->npageRecordZones);
	pfree(oldPageArray);

	/* Increase capacity of alignment value array if needed. */
	if (unlikely(enlargeCount % PRIVATEBUFFER_ENLARGE_CAPACITY == 0))
	{
		/* Alloc new array. */
		uint16 *newAlignments = (uint16 *) palloc0(sizeof(uint16) *
				(enlargeCount + PRIVATEBUFFER_ENLARGE_CAPACITY));

		/* Copy old values. */
		memcpy(newAlignments, privateBuffer->alignments,
			   sizeof(uint16) * enlargeCount);

		/* Free old array. */
		pfree(privateBuffer->alignments);

		/* Set new array. */
		privateBuffer->alignments = newAlignments;
	}

	/* Allocate new empty contiguous page record zones. (1 is for alignment(io_uring)) */
	newPageRecordZones = (Page) palloc(newPageRecordZonesSize);
	alignment = (uint16)(BLCKSZ - ((uint64)newPageRecordZones % BLCKSZ));
	privateBuffer->alignments[enlargeCount] = alignment;
	newPageRecordZones += alignment;

	/* Set page record zone pointer in new allocated page array. */
	for (i = privateBuffer->capacity, j = 0; i < newCapacity; ++i, ++j)
	{
		newPageArray[i] = ((char *) newPageRecordZones) +
						  blocksPerRecordZone * BLCKSZ * j;
	}

	/* Set new array. */
	privateBuffer->pageRecordZones = newPageArray;
	privateBuffer->capacity = newCapacity;

	privateBuffer->enlargeCount = enlargeCount + 1;

	MemoryContextSwitchTo(oldContext);
}

/*
 * LocatorPartitionGetPrivateBufferForTuple
 *
 * We find target page to append tuple.
 */
static LocatorPartitionPrivateBuffer *
LocatorPartitionGetPrivateBufferForTuple(LocatorRepartitioningDesc *desc)
{
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	LocatorPartitionPrivateBuffer *privateBuffer;
	Index		privateBufferIdx;
	Page		pageRecordZone;
	uint32		blocksPerRecordZone = lowerInsert->blocksPerRecordZone;

	/* Get index on private buffers to append the tuple. */
	privateBufferIdx = 
		lowerInsert->targetPartitionNumber % lowerInsert->nprivatebuffer;
	privateBuffer = lowerInsert->privateBuffers[privateBufferIdx];
	Assert(lowerInsert->targetPartitionNumber == privateBuffer->partitionNumber);

	/* Enlarge page array. */
	EnlargePrivateBufferPageArray(desc, privateBuffer);
	Assert(privateBuffer->npageRecordZones < privateBuffer->capacity);

	/* Get page from private buffers. */
	if (unlikely(privateBuffer->npageRecordZones == 0 ||
				 (privateBuffer->currentRecCnt %
				  desc->childRecordsPerRecordZone == 0)))
	{
		Size len;
		int tuplesPerBlock;

		/* Initially appending to the partition. */
	
		/* Get new page record zone, using local memory. */
		pageRecordZone = privateBuffer->pageRecordZones[privateBuffer->npageRecordZones];

		/* Init new pages in descending order. */
		for (int i = lowerInsert->columnGroupCount - 1, j = blocksPerRecordZone - 1;
			 i >= 0; i--)
		{
			if (i == 0 && lowerInsert->layoutType != COLUMNAR)
			{
				/* simple trick */
				len = 0;
				tuplesPerBlock = 0;
			}
			else
			{
				len = lowerInsert->tupleSize[i];
				tuplesPerBlock = lowerInsert->tuplesPerBlock[i];
			}

			for (; j >= (int)(lowerInsert->blockOffset[i]); j--)
			{
				LocatorPartitionPageInit(pageRecordZone + j * BLCKSZ, BLCKSZ, len,
										 tuplesPerBlock);
			}
		}

		/* Check that the last page was being filled. (partial) */
		if (unlikely(privateBuffer->npageRecordZones == 0 &&
					 (privateBuffer->firstRecCnt % 
					  desc->childRecordsPerRecordZone != 0)))
		{
			PageHeader pageHeader;
			int pageOffset, tupleCount;
			int remainingRecords = privateBuffer->firstRecCnt % 
								   desc->childRecordsPerRecordZone;

			for (int i = 0; i < lowerInsert->columnGroupCount; i++)
			{
				tupleCount = remainingRecords % (int)(lowerInsert->tuplesPerBlock[i]);

				/* Skip if the last page of this column group is full */
				if (tupleCount == 0)
					continue;

				pageOffset = lowerInsert->blockOffset[i] +
							 remainingRecords / (int)(lowerInsert->tuplesPerBlock[i]);
				pageHeader = (PageHeader)(pageRecordZone + pageOffset * BLCKSZ);

				/* Case: SIRO tuple */
				if (i == 0 && lowerInsert->layoutType != COLUMNAR)
				{
					len = lowerInsert->tupleSize[i];

					pageHeader->pd_lower += 3 * sizeof(ItemIdData) *
											tupleCount;
					pageHeader->pd_upper -= (MAXALIGN(PITEM_SZ) + 2 * len) *
											tupleCount;
				}
				/* Case: columnar tuple */
				else
				{
					/*
					 * The columnar page uses pd_lower, pd_upper, and pd_special
					 * differently. (See LocatorPartitionPageInit())
					 */
					pageHeader->pd_special = tupleCount;
				}

				privateBuffer->partials[i] = true;
			}

			privateBuffer->partial = true;
		}

		privateBuffer->npageRecordZones += 1;
	}

	return privateBuffer;
}

/*
 * LocatorPartitionPageAddSiroTuple
 * 
 * We add SIRO tuple to the page.
 */
static void
LocatorPartitionPageAddSiroTuple(Page page, BlockNumber blockNum,
								 Size tupleSize, SiroTuple siroTuple)
{
	PageHeader	phdr = (PageHeader) page;
	ItemId		plocatorItemId, leftVersionItemId, rightVersionItemId;
	char	   *p_pointer;
	Size		p_alignedSize;
	int			lower;
	int			upper;
	OffsetNumber insertionOffsetNumber;
	uint32	   *p_pd_gen;

	Assert(!PageIsCOLUMNAR(page));

	/*
	 * Be wary about corrupted page pointers
	 */
	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper ||
		phdr->pd_upper > phdr->pd_special ||
		phdr->pd_special > BLCKSZ)
	{
		ereport(PANIC,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("corrupted page pointers at AddSiroTuple: lower = %u, upper = %u, special = %u",
				 phdr->pd_lower, phdr->pd_upper, phdr->pd_special)));
	}

	/*
	 * Select offsetNumber to place the new item at
	 */
	insertionOffsetNumber = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	p_alignedSize = MAXALIGN(PITEM_SZ);

	lower = phdr->pd_lower + sizeof(ItemIdData) * 3;
	upper = (int) phdr->pd_upper - (int) tupleSize * 2 - (int) p_alignedSize;

	if (lower > upper)
		elog(PANIC, "lower > upper, failed to add tuples to page");

	/*
	 * OK to append the item.
	 */
	
	/* Set the line pointer for p-locator */
	plocatorItemId = PageGetItemId(phdr, insertionOffsetNumber);
	ItemIdSetNormal(plocatorItemId, upper, PITEM_SZ);
	LP_OVR_SET_UNUSED(plocatorItemId);
	LP_SET_PLEAF_FLAG(plocatorItemId);

	if (siroTuple->isLeftMostRecent)
		LP_OVR_SET_LEFT(plocatorItemId);
	else
		LP_OVR_SET_RIGHT(plocatorItemId);

	/* set the line pointer of left version*/
	leftVersionItemId = PageGetItemId(phdr, insertionOffsetNumber + 1);
	ItemIdSetNormal(leftVersionItemId,
					upper + (int) p_alignedSize, siroTuple->leftVersionLen);
	if (siroTuple->leftVersionLen == 0)
		LP_OVR_SET_UNUSED(leftVersionItemId);
	else
		LP_OVR_SET_USING(leftVersionItemId);

	/* Set the line pointer of right version */
	rightVersionItemId = PageGetItemId(phdr, insertionOffsetNumber + 2);
	ItemIdSetNormal(rightVersionItemId, 
					upper + ((int) p_alignedSize + (int) tupleSize),
					siroTuple->rightVersionLen);
	if (siroTuple->rightVersionLen > 0)
		LP_OVR_SET_USING(rightVersionItemId);
	else
		LP_OVR_SET_UNUSED(rightVersionItemId);

	/* Zero the p-locator */
	p_pointer = (char *) page + upper;
	Assert(PITEM_SZ <= p_alignedSize);
	memcpy(p_pointer, siroTuple->plocatorData, (Size) PITEM_SZ);

	/* Copy the item's data onto the page (not onto dummy) */
	p_pointer += (int) p_alignedSize;
	Assert(siroTuple->leftVersionLen <= tupleSize);
	if (siroTuple->leftVersionLen > 0)
		memcpy(p_pointer, siroTuple->leftVersionData, siroTuple->leftVersionLen);

	/* Zero the right version */
	p_pointer += (int) tupleSize;
	Assert(siroTuple->rightVersionLen <= tupleSize);
	if (siroTuple->rightVersionLen > 0)
		memcpy(p_pointer, siroTuple->rightVersionData, siroTuple->rightVersionLen);

	/* Set info of p-locator (see architecture in bufpage.h) */
	p_pointer = (char *) page + upper + PITEM_GEN_OFF;
	p_pd_gen = (uint32*)p_pointer;

	/* Set pd_gen */
	if (siroTuple->isUpdated)
		*p_pd_gen = phdr->pd_gen;
	else
		*p_pd_gen = phdr->pd_gen - 1;

	/* adjust page header */
	phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;
}

/*
 * LocatorPartitionPageAddColumnarTuple
 *
 * We add columnar tuple to the page.
 */
static void
LocatorPartitionPageAddColumnarTuple(Page page, bool isRecordKey,
									 Size tupleSize, ColumnarTuple columnarTuple,
									 int attrCount)
{
	PageHeader	phdr = (PageHeader) page;
	int			nullBitCount = attrCount * phdr->pd_special;
	bits8		*bp = (bits8*)((char*)page + phdr->pd_lower + (nullBitCount >> 3));
	bits8		nullBit = 0x01 << (nullBitCount & 0x07);
	char		*data = (char*)page + phdr->pd_upper +
						(phdr->pd_special * tupleSize);

	Assert(PageIsCOLUMNAR(page));

	/* Fill the null bit array. */
	if (likely(!isRecordKey))
	{
		for (int i = 0; i < attrCount; i++)
		{
			if (!columnarTuple->isnull[i])
				*bp |= nullBit;

			nullBit <<= 1;
			if (nullBit == 0)
			{
				bp += 1;
				nullBit = 0x01;
			}
		}
	}

	/* Copy the tuple data. */
	memcpy(data, columnarTuple->columnarTupleData, tupleSize);

	/* Increment the number of tuples. */
	phdr->pd_special += 1;

	Assert(phdr->pd_lower + (TYPEALIGN(8, attrCount * phdr->pd_special) >> 3) <= phdr->pd_upper);
}

/*
 * GetPartitionNumberFromPartitionKey
 */
LocatorPartNumber
GetPartitionNumberFromPartitionKey(int spreadFactor, LocatorPartLevel destLevel, 
								   LocatorPartLevel lastLevel, int partitionId)
{
	int coverage;

	Assert(destLevel <= lastLevel);
	coverage = (int) pow(spreadFactor, lastLevel - destLevel);

	return (LocatorPartNumber) (partitionId / coverage);
}

/*
 * LocatorPartitionAppend
 *
 * In the repartitioning process, the record will be appended to the lower 
 * partitions at the target level.
 */
uint32
LocatorPartitionAppend(LocatorRepartitioningDesc *desc,
					   LocatorRecord locatorRecord, LocatorSeqNum *firstSeqNums)
{
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	BlockNumber			blockOffset;
	LocatorPartitionPrivateBuffer *privateBuffer;
	LocatorRecordKey	recordKey;
	Page				page;
	uint32				childIndex;
	uint16				tupleSize;
	uint8				attrCount;
	bool				isRecordKey = false;
	uint32				recCntOfLastRecordZone;

	/* Get a partition ID using the record key from the p-locator. */
#ifdef LOCATOR_DEBUG
	if (locatorRecord->siroTuple.plocatorLen != PITEM_SZ)
	{
		char errmsg[32];
		sprintf(errmsg, "record->plocatorLen != PITEM_SZ (%u)\n",
						locatorRecord->siroTuple.plocatorLen);
		Abort(errmsg);
	}
#else /* !LOCATOR_DEBUG */
	Assert(locatorRecord->siroTuple.plocatorLen == PITEM_SZ);
#endif /* LOCATOR_DEBUG */

	/* Get target partition Id. */
	if (desc->upperScan.layoutType != COLUMNAR)
	{
		recordKey =
			*(PGetLocatorRecordKey(locatorRecord->siroTuple.plocatorData));
	}
	else
		recordKey = locatorRecord->recordKey;

	/* Set partition number to append record. */
	lowerInsert->targetPartitionNumber = 
		GetPartitionNumberFromPartitionKey(exCatalog->spreadFactor, 
										   lowerInsert->targetPartitionLevel, 
										   exCatalog->lastPartitionLevel,
										   recordKey.partKey);
	
	childIndex = lowerInsert->targetPartitionNumber % exCatalog->spreadFactor;

	pg_memory_barrier();

	/* Memoize first record sequence number. */
	if (unlikely(firstSeqNums[childIndex] == InvalidLocatorSeqNum))
		firstSeqNums[childIndex] = recordKey.seqnum;

	/* Get private memory space to append the record. */
	privateBuffer =
		LocatorPartitionGetPrivateBufferForTuple(desc);

	recCntOfLastRecordZone = privateBuffer->currentRecCnt % desc->childRecordsPerRecordZone;

	/* Add tuples to the each column group. */
	for (int i = 0; i < lowerInsert->columnGroupCount; i++)
	{
		/* Calculate current block offset. */
		blockOffset = lowerInsert->blockOffset[i] +
					  recCntOfLastRecordZone / (uint32)(lowerInsert->tuplesPerBlock[i]);

		/* Get private memory space to append the tuple. */
		page = privateBuffer->pageRecordZones[privateBuffer->npageRecordZones - 1] +
			   blockOffset * BLCKSZ;

		/* Get the size of columnar tuple. */
		tupleSize = lowerInsert->tupleSize[i];

		/* NO EREPORT(ERROR) from here till changes are logged */
		START_CRIT_SECTION();

		/* Append the tuple to the page of the private buffer. */
		if (i == 0 && lowerInsert->layoutType != COLUMNAR)
		{
			BlockNumber	blockNum = privateBuffer->currentRecCnt /
								   desc->childRecordsPerRecordZone *
								   lowerInsert->blocksPerRecordZone +
								   blockOffset;

			LocatorPartitionPageAddSiroTuple(page, blockNum, tupleSize,
											 &(locatorRecord->siroTuple));

			Assert(lowerInsert->columnGroupCount != 1 ||
				   blockNum == privateBuffer->currentRecCnt / lowerInsert->tuplesPerBlock[i]);
		}
		else
		{
			/* Check if this column group is record key. */
			if (unlikely(i == lowerInsert->recordKeyGroupIndex))
				isRecordKey = true;

			attrCount = lowerInsert->attrCount[i];
			LocatorPartitionPageAddColumnarTuple(page, isRecordKey, tupleSize,
												 &(locatorRecord->columnarTuple[i]),
												 attrCount);
		}

		END_CRIT_SECTION();
	}

	privateBuffer->currentRecCnt += 1;

	return childIndex;
}

/*
 * RepartitionNewestMempartition
 *
 * When the PostgreSQL server shuts down, checkpointer repartitions the newest
 * mempartition remaining in the buffer.
 */
void
RepartitionNewestMempartition(int flags)
{
	Oid	relationOid;
	LocatorExternalCatalog *exCatalog;
	LocatorRepartitioningDesc desc;
	LocatorTieredNumber	minTieredNumber;
	LocatorTieredNumber	maxTieredNumber;

	uint64				level0RecordCount;
	uint32				reorganizedRecCnt[MAX_SPREAD_FACTOR];
	LocatorSeqNum		firstSeqNums[MAX_SPREAD_FACTOR];
	char				oldFileName[32];

	struct io_uring		ring;
	int					ring_ret = -1;

	/*
	 * Repartitioning for the newest mempartition should only be executed at
	 * PostgreSQL server shutdown.
	 */
	if (likely(!(flags & CHECKPOINT_IS_SHUTDOWN)))
		return;

	for (int exCatId = 0; exCatId < LocatorExternalCatalogGlobalMeta->externalCatalogNums; exCatId++)
	{
		exCatalog = ExCatalogIndexGetLocatorExternalCatalog(exCatId);

		/* Wait for remained partitioning workers */
		WaitForPartitioningWorkers(exCatalog);

		/* Init io_uring queue */
		if (likely(enable_uring_partitioning))
		{
			ring_ret = io_uring_queue_init(exCatalog->spreadFactor * MAX_COLUMN_GROUP_COUNT,
										   &ring, IORING_SETUP_COOP_TASKRUN);
		}

		if (ring_ret >= 0)
			desc.ring = &ring;
		else
			desc.ring = NULL;

		/*
		 * Since this is executed after all partitioning workers have finished,
		 * there is no need for concurrency control in the external catalog.
		 */

		relationOid = exCatalog->tag.relationOid;

		for (int partNum = 0; partNum < exCatalog->spreadFactor; ++partNum)
		{
			level0RecordCount = exCatalog->CurrentRecordCountsLevelZero[partNum].val +
								exCatalog->TieredRecordCountsLevelZero[partNum].val;

			/* If no records exist at level 0, skip */
			if (level0RecordCount == 0)
				continue;

			desc.exCatalog = exCatalog;

			if (ring_ret >= 0)
				desc.ring = &ring;
			else
				desc.ring = NULL;

			/* Set repartitioning range, including newest mempartition */
			minTieredNumber = exCatalog->PreviousTieredNumberLevelZero[partNum];
			maxTieredNumber = exCatalog->CurrentTieredNumberLevelZero[partNum];

			MemSet(reorganizedRecCnt, 0, sizeof(uint32) * MAX_SPREAD_FACTOR);
			MemSet(firstSeqNums, -1, sizeof(LocatorSeqNum) * MAX_SPREAD_FACTOR);

			pg_memory_barrier();

			/* locator repartitioning main entry. */
			LocatorRepartitioning(&desc, relationOid,
								  level0RecordCount,
								  0,
								  partNum, 
								  minTieredNumber, 
								  maxTieredNumber,
								  1,
								  reorganizedRecCnt,
								  firstSeqNums,
								  false);

			pg_memory_barrier();

			/* level 0 */
			exCatalog->PreviousTieredNumberLevelZero[partNum] =
				++(exCatalog->CurrentTieredNumberLevelZero[partNum]);
			exCatalog->CurrentRecordCountsLevelZero[partNum].val = 0;
			exCatalog->TieredRecordCountsLevelZero[partNum].val = 0;
			exCatalog->ReorganizedRecordCountsLevelZero[partNum].val += level0RecordCount;

			/* level 1 */
			for (int i = 0; i < exCatalog->spreadFactor; i++)
			{
				LocatorPartNumber childPartNum =
									partNum * exCatalog->spreadFactor + i;

				/*
				 * If there are new records that have been repartitioned, apply that
				 * information to the external catalog.
				 */
				if (reorganizedRecCnt[i] != 0)
				{
					/*
					 * If these repartitioned records are the first of this
					 * partition
					 */
					if (exCatalog->realRecordsCount[1][childPartNum] == 0)
					{
						if (likely(exCatalog->lastPartitionLevel != 1))
						{
							/* Increment generation number if needed */
							if (unlikely(++(exCatalog->generationNums[1][childPartNum]) == InvalidLocatorPartGenNo))
								exCatalog->generationNums[1][childPartNum] = 0;

							/* Set first sequence number of partition */
							exCatalog->locationInfo[1][childPartNum].firstSequenceNumber = firstSeqNums[i];
						}
					}

					/* Adds memoized reorganized records count */
					exCatalog->realRecordsCount[1][childPartNum] += reorganizedRecCnt[i];
				}
			}

			/* Increment block count for analyze */
			IncrBlockCount(exCatalog, (desc.childRecordZones * desc.childBlocksPerRecordZone) -
						   (desc.parentRecordZones * desc.parentBlocksPerRecordZone));

			pg_memory_barrier();

			/* GC the repartitioned old file immediately */
			for (LocatorTieredNumber tieredNum = minTieredNumber;
				 				tieredNum <= maxTieredNumber; ++tieredNum)
			{
				snprintf(oldFileName, 32, "base/%u/%u.%u.%u.%u",
									 exCatalog->relation.rd_node.dbNode,
									 relationOid, 0, partNum, tieredNum);
				unlink(oldFileName);
			}
		}

		/* Exit io_uring. */
		if (ring_ret >= 0)
			io_uring_queue_exit(&ring);
	}
}

#endif /* LOCATOR */
