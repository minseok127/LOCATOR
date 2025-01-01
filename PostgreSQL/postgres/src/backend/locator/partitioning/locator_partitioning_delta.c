/*
 * locator_partitioning_delta.c
 *
 * Locator Partitioning Delta Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/locator/partitioning/locator_partitioning_delta.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef LOCATOR

#include "postgres.h"
#include "utils/dsa.h"
#include "storage/bufmgr.h"
#include "locator/locator_partitioning.h"
#include "locator/locator_external_catalog.h"
#include "postmaster/locator_partition_mgr.h"
#ifdef LOCATOR_DEBUG
#include "access/heapam.h"
#endif /* LOCATOR_DEBUG */

static LocatorGarbageReservedDeltaSpace *
LocatorConvertToGarbageReservedDeltaSpace(dsa_area* area, dsa_pointer ptr)
{
	return (LocatorGarbageReservedDeltaSpace *) dsa_get_address(area, ptr);
}
 
static LocatorDeltaSpace *
LocatorConvertToDeltaSpace(dsa_area* area, dsa_pointer ptr)
{
	return (LocatorDeltaSpace *) dsa_get_address(area, ptr);
}

static LocatorDeltaSpaceEntry *
LocatorConvertToDeltaSpaceEntry(dsa_area* area, dsa_pointer ptr)
{
	return (LocatorDeltaSpaceEntry *) dsa_get_address(area, ptr);
}

static Page
LocatorConvertToHugepage(dsa_area *area, dsa_pointer ptr)
{
	return (Page) dsa_get_address(area, ptr);
}

/*
 * CreateLocatorDeltaSpace
 */
static dsa_pointer
CreateLocatorDeltaSpace(dsa_area *area, int nhugepage)
{
	dsa_pointer dsaDeltaSpace;
	LocatorDeltaSpace *deltaSpace;
 
	dsaDeltaSpace = 
		dsa_allocate_extended(area, 
			sizeof(LocatorDeltaSpace) + sizeof(dsa_pointer) * nhugepage, 
														DSA_ALLOC_ZERO);

	deltaSpace = LocatorConvertToDeltaSpace(area, dsaDeltaSpace);
	deltaSpace->nhugepage = nhugepage;

	return dsaDeltaSpace;
}

/*
 * CreateLocatorDeltaSpaceEntry
 */
dsa_pointer
CreateLocatorDeltaSpaceEntry(dsa_area *area)
{
	int nhugepage = 1; /* init value */
	dsa_pointer dsaDeltaSpaceEntry;
	LocatorDeltaSpaceEntry *deltaSpaceEntry;
	LocatorDeltaSpace *deltaSpace;

	/* dsa allocation. */
	dsaDeltaSpaceEntry =
		dsa_allocate_extended(area, sizeof(LocatorDeltaSpaceEntry), DSA_ALLOC_ZERO);

	/* Change dsa address. */
	deltaSpaceEntry = LocatorConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);

	/* Init values */
	deltaSpaceEntry->SizePerHugepage = LOCATOR_DELTA_PAGE_SIZE;
	deltaSpaceEntry->deltaNumsPerHugepage = 
		(deltaSpaceEntry->SizePerHugepage / sizeof(LocatorDelta));

	pg_atomic_init_u64(&deltaSpaceEntry->deltaNums, 0);
	pg_atomic_init_u64(&deltaSpaceEntry->deltaCapacity, 
									deltaSpaceEntry->deltaNumsPerHugepage);
	
	deltaSpaceEntry->deltaSpace = CreateLocatorDeltaSpace(area, nhugepage); 
	deltaSpaceEntry->reservedForGarbageCollection = InvalidDsaPointer;

	/* Add one hugepage to delta space. */
	deltaSpace = LocatorConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);
	deltaSpace->hugepages[0] = dsa_allocate(area, LOCATOR_DELTA_PAGE_SIZE);

	return dsaDeltaSpaceEntry;
}

/*
 * DeleteLocatorDeltaSpaceEntry
 */
void
DeleteLocatorDeltaSpaceEntry(dsa_area *area, dsa_pointer dsaDeltaSpaceEntry)
{
	dsa_pointer garbageTarget;
	LocatorDeltaSpace *deltaSpace;
	LocatorDeltaSpaceEntry *deltaSpaceEntry;

	if (!DsaPointerIsValid(dsaDeltaSpaceEntry))
	{
		elog(WARNING, "try to free dangling dsa pointer.");
		return;
	}

	deltaSpaceEntry = LocatorConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);
	deltaSpace = LocatorConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Delete delta space's hugepages. */
	for (int i = 0; i < deltaSpace->nhugepage; ++i)
		dsa_free(area, deltaSpace->hugepages[i]);

	dsa_free(area, deltaSpaceEntry->deltaSpace);

	/* Do garbage collection */
	garbageTarget = deltaSpaceEntry->reservedForGarbageCollection;

	while (DsaPointerIsValid(garbageTarget))
	{
		LocatorGarbageReservedDeltaSpace *garbageReservedDeltaSpace;
		dsa_pointer nextGarbageTarget;

		/* Get structure. */
		garbageReservedDeltaSpace = 
			LocatorConvertToGarbageReservedDeltaSpace(area, garbageTarget);

		/* Get next garbage target's dsa pointer. */
		nextGarbageTarget = garbageReservedDeltaSpace->next;

		/* Resource cleanup. */
		dsa_free(area, garbageReservedDeltaSpace->currentItem);
		dsa_free(area, garbageTarget);

		/* Move to next item for garbage collection. */
		garbageTarget = nextGarbageTarget;
	}

	/* Resource cleanup. */
	dsa_free(area, dsaDeltaSpaceEntry);
}

/*
 * GetLocatorDelta
 */
static LocatorDelta *
GetLocatorDelta(dsa_area *area, LocatorDeltaSpace *deltaSpace, 
				Index deltaIdx, int deltaNumsPerHugepage)
{
	Page hugepage;
	Index hugepageIdx;
	off_t offset;
	LocatorDelta *delta;

	/* Calculate target hugepage and index. */
	hugepageIdx = deltaIdx / deltaNumsPerHugepage;
	offset = deltaIdx % deltaNumsPerHugepage;
	Assert(hugepageIdx < deltaSpace->nhugepage);

	/* Get hugepage. */
	hugepage = LocatorConvertToHugepage(area, deltaSpace->hugepages[hugepageIdx]);

	/* Get delta from hugepage. */
	delta = (LocatorDelta *) ((char *) hugepage + sizeof(LocatorDelta) * offset); 

	return delta;
}

/*
 * LocatorDeltaGetTuple
 */
static void
LocatorDeltaGetTuple(LocatorRepartitioningDesc *desc,
					 LocatorPartLevel partitionLevel, 
					 LocatorPartNumber partitionNumber,
					 LocatorPartGenNo partitionGenerationNumber,
					 Buffer *buffer, int upperTuplePosition, SiroTuple siroTuple)
{
	Relation 	relation = desc->relation;
	uint32		recordsPerRecordZone = 
					*(desc->upperScan.columnarLayout->recordsPerRecordZone);
	uint32		blocksPerRecordZone = desc->upperScan.blocksPerRecordZone;
	uint16		tuplesPerBlock = desc->upperScan.columnarLayout->columnGroupMetadataArray[0].tuplesPerBlock;

	uint32		recordCountInRecordZone;
	BlockNumber recordZoneNumber;
	BlockNumber blockNumber;

	OffsetNumber offsetNumber;
	ItemId plocatorLpp, leftLpp, rightLpp;
	Page page;

	Item		pLocator;

#ifdef LOCATOR_DEBUG
	HeapTupleData leftLoctup;
	HeapTupleData rightLoctup;
#endif /* LOCATOR_DEBUG */

	Assert(*buffer == InvalidBuffer);

#ifdef LOCATOR_DEBUG
	leftLoctup.t_tableOid = relation->rd_node.relNode;
	rightLoctup.t_tableOid = relation->rd_node.relNode;
#endif /* LOCATOR_DEBUG */

	/* Updated record cannot be aborted. */
	siroTuple->isAborted = false;

	/* Calculate target block number. */
	recordZoneNumber = upperTuplePosition / recordsPerRecordZone;
	recordCountInRecordZone = upperTuplePosition % recordsPerRecordZone;
	blockNumber = (recordZoneNumber * blocksPerRecordZone) + // blockOffset of columnGroup[0] is 0
				  (recordCountInRecordZone / tuplesPerBlock);
	offsetNumber = (recordCountInRecordZone % tuplesPerBlock) * 3 + 1;

	/* Read target page. */
	*buffer = ReadPartitionBufferExtended(relation, 
										  partitionLevel, 
										  partitionNumber, 
										  partitionGenerationNumber,
										  blockNumber, RBM_NORMAL,
										  desc->upperScan.strategy);
	page = BufferGetPage(*buffer);

	Assert(!PageIsCOLUMNAR(page));

	/* 
	 * XXX: In the blocking phase, there are no updates to this page. However, 
	 * we acquire a lock to prevent modifications, such as those performed by 
	 * vacuum processes.. 
 	 */
	LockBuffer(*buffer, BUFFER_LOCK_SHARE);

	/* Set plocator info. */
	plocatorLpp = PageGetItemId(page, offsetNumber);
	pLocator = PageGetItem(page, plocatorLpp);
	siroTuple->plocatorData = pLocator;
	siroTuple->plocatorLen = ItemIdGetLength(plocatorLpp);

	/* Check pd_gen. */
	if (*((uint32*)((char *) pLocator + PITEM_GEN_OFF)) ==
		((PageHeader)page)->pd_gen)
		siroTuple->isUpdated = true;
	else
		siroTuple->isUpdated = false;
		
	if (LP_OVR_IS_LEFT(plocatorLpp))
		siroTuple->isLeftMostRecent = true;
	else
		siroTuple->isLeftMostRecent = false;

	/* Set left version info. */
	leftLpp = PageGetItemId(page, offsetNumber + 1);
	siroTuple->leftVersionData = (HeapTupleHeader) PageGetItem(page, leftLpp);
	siroTuple->leftVersionLen = ItemIdGetLength(leftLpp);

	/* Set right version info. */
	rightLpp = PageGetItemId(page, offsetNumber + 2);
	siroTuple->rightVersionData = (HeapTupleHeader) PageGetItem(page, rightLpp);
	siroTuple->rightVersionLen = ItemIdGetLength(rightLpp);

#ifdef LOCATOR_DEBUG
	leftLoctup.t_data = siroTuple->leftVersionData;
	rightLoctup.t_data = siroTuple->rightVersionData;
	ItemPointerSet(&(leftLoctup.t_self), blockNumber, offsetNumber + 1);
	ItemPointerSet(&(rightLoctup.t_self), blockNumber, offsetNumber + 2);

	/* Updated record cannot be aborted. */
	Assert(HeapTupleGetVisibilityStatus(&leftLoctup, *buffer) != VERSION_ABORTED ||
		   HeapTupleGetVisibilityStatus(&rightLoctup, *buffer) != VERSION_ABORTED);
#endif /* LOCATOR_DEBUG */

	LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * LocatorWriteDeltaToSharedBuffer
 */
static void
LocatorWriteDeltaToSharedBuffer(LocatorRepartitioningDesc *desc, 
								int64_t lowerTuplePosition, SiroTuple siroTuple,
								Buffer buffer)
{
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	uint32			recordsPerRecordZone =
		*(lowerInsert->columnarLayout->recordsPerRecordZone);
	uint16			tuplesPerBlock =
		lowerInsert->columnarLayout->columnGroupMetadataArray[0].tuplesPerBlock;
#ifdef LOCATOR_DEBUG
	Index 			pageIdx;
	BlockNumber 	targetRecordZoneNumber;
	BlockNumber 	targetBlockNumber;
#endif
	uint32			targetTupleNumber;
	OffsetNumber 	targetOffsetNumber;
	ItemId 			plocatorLpp, leftLpp, rightLpp;
	Page			page;
	PageHeader		phdr;
	uint32		   *p_pd_gen;
	Item			plocator;

	/* Calculate record zone number and offset that the tuple should be modified. */
	targetTupleNumber = lowerTuplePosition % recordsPerRecordZone;
	targetOffsetNumber = (targetTupleNumber % tuplesPerBlock) * 3 + 1;

#ifdef LOCATOR_DEBUG
	targetRecordZoneNumber = lowerTuplePosition / recordsPerRecordZone;
	pageIdx = targetTupleNumber / tuplesPerBlock;
	targetBlockNumber = targetRecordZoneNumber * lowerInsert->blocksPerRecordZone + pageIdx;
	Assert(BufferGetBlockNumber(buffer) == targetBlockNumber);
#endif

	/* 
	 * Get page in the private buffer. The repartitioning worker needs to apply 
	 * delta to this page.
	 */
	page = BufferGetPage(buffer);
	phdr = (PageHeader) page;

	Assert(!PageIsNew(page));

	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	/* Get lineitems. */
	plocatorLpp = PageGetItemId(page, targetOffsetNumber);
	leftLpp = PageGetItemId(page, targetOffsetNumber + 1);
	rightLpp = PageGetItemId(page, targetOffsetNumber + 2);
	
	/* Fix lineitems (= slot array). */
	plocatorLpp->lp_len = siroTuple->plocatorLen;
	leftLpp->lp_len = siroTuple->leftVersionLen;
	rightLpp->lp_len = siroTuple->rightVersionLen;
	LP_OVR_SET_USING(rightLpp);

	if (siroTuple->isLeftMostRecent)
		LP_OVR_SET_LEFT(plocatorLpp);
	else
		LP_OVR_SET_RIGHT(plocatorLpp);

	/* Get p-locator item. */
	plocator = PageGetItem(page, plocatorLpp);
	Assert(PGetLocatorRecordKey(plocator)->seqnum == 
				PGetLocatorRecordKey(siroTuple->plocatorData)->seqnum);
	
	/* Fix tuple. */
	memcpy(plocator, siroTuple->plocatorData, siroTuple->plocatorLen);
	memcpy(PageGetItem(page, leftLpp), siroTuple->leftVersionData, 
									   siroTuple->leftVersionLen);
	memcpy(PageGetItem(page, rightLpp), siroTuple->rightVersionData, 
										siroTuple->rightVersionLen);

	pg_memory_barrier();

	/* We need to change pd gen for new partition. */
	p_pd_gen = (uint32 *) (plocator + PITEM_GEN_OFF);
	*p_pd_gen = phdr->pd_gen;

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * ApplyLocatorDeltas
 * 
 * The partition worker applies deltas.
 */
void
ApplyLocatorDeltas(dsa_area *area, LocatorRepartitioningDesc *desc)
{
	dsa_pointer dsaLocatorPartitionDeltaSpaceEntry;
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	LocatorPartLevel upperPartitionLevel;
	LocatorDeltaSpaceEntry *deltaSpaceEntry;
	LocatorDeltaSpace *deltaSpace;
	uint64 deltaNums;
	uint64 deltaIdx;

	/* Get upper partition level and lower patition level. */
	upperPartitionLevel = upperScan->partitionLevel;

	/* Get correct delta space entry. */
	if (upperPartitionLevel == 0)
	{
		dsaLocatorPartitionDeltaSpaceEntry = 
					exCatalog->dsaPartitionDeltaSpaceEntryForLevelZero;
	}
	else
	{
		dsaLocatorPartitionDeltaSpaceEntry = 
					exCatalog->dsaPartitionDeltaSpaceEntryForOthers;
	}

	/* Get delta space structure. */
	deltaSpaceEntry =
		LocatorConvertToDeltaSpaceEntry(area, dsaLocatorPartitionDeltaSpaceEntry);
	deltaSpace =
		LocatorConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Get the total number of delta. */
	deltaNums = pg_atomic_read_u64(&deltaSpaceEntry->deltaNums);
	if (deltaNums == 0)
	{
		/* If there are no deltas that need to be applied, simply return. */
		return;
	}

	/* for debugging. */
	desc->deltaNums = deltaNums;

	/* Iterate all deltas. */
	for (deltaIdx = 0; deltaIdx < deltaNums; ++deltaIdx)
	{
		Buffer buffer = InvalidBuffer;
		LocatorDelta *delta;
		SiroTupleData siroTupleData;

		/* Get a delta using delta idx. */
		delta = GetLocatorDelta(area, deltaSpace, deltaIdx, 
								deltaSpaceEntry->deltaNumsPerHugepage);

		/* We skip duplicate delta. */
		if (delta->skip)
		{
			Assert(delta->preparedBuffer == InvalidBuffer);
			continue;
		}

		Assert(delta->active && delta->preparedBuffer != InvalidBuffer);

		/* Get a modified siro tuple from shared buffer. */
		LocatorDeltaGetTuple(desc, upperPartitionLevel,
							 delta->partitionNumber,
							 delta->partitionGenerationNumber,
							 &buffer, delta->upperTuplePosition, &siroTupleData);
		Assert(PGetLocatorRecordKey(siroTupleData.plocatorData)->partKey == 
															delta->partitionId);
		Assert(BufferIsValid(buffer));

		if (upperScan->layoutNeedsToBeChanged)
		{
			/* Reformat only stable partition. */
			Assert(desc->lowerInsert.layoutType != COLUMNAR);
			LocatorReformatSiroTuple(desc, &siroTupleData);
		}

		/* Write modified siro tuple to shared buffer. */
		LocatorWriteDeltaToSharedBuffer(desc, delta->lowerTuplePosition,
								&siroTupleData, delta->preparedBuffer);

		/* Release pins. */
		ReleaseBuffer(delta->preparedBuffer);
		ReleaseBuffer(buffer);
	}
}


/*
 * CreateLocatorGarbageReservedDeltaSpace
 */
static dsa_pointer
CreateLocatorGarbageReservedDeltaSpace(dsa_area *area, 
									   dsa_pointer garbagedeltaSpace)
{
	dsa_pointer dsaGarbageReservedDeltaSpace;
	LocatorGarbageReservedDeltaSpace *garbageReservedDeltaSpace;

	/* dsa allocation. */
	dsaGarbageReservedDeltaSpace =
		dsa_allocate(area, sizeof(LocatorGarbageReservedDeltaSpace));

	/* Change dsa address. */
	garbageReservedDeltaSpace = 
		LocatorConvertToGarbageReservedDeltaSpace(area, 
												dsaGarbageReservedDeltaSpace);

	/* Init values. */
	garbageReservedDeltaSpace->currentItem = garbagedeltaSpace;
	garbageReservedDeltaSpace->next = InvalidDsaPointer;

	return dsaGarbageReservedDeltaSpace;
}

/*
 * CopyToDeltaSpace
 */
static void
CopyToDeltaSpace(dsa_area *area, dsa_pointer dsaFromDeltaSpace, 
				 dsa_pointer dsaToDeltaSpace)
{
	LocatorDeltaSpace *fromDeltaSpace =
		LocatorConvertToDeltaSpace(area, dsaFromDeltaSpace);
	LocatorDeltaSpace *toDeltaSpace =
		LocatorConvertToDeltaSpace(area, dsaToDeltaSpace);

	/* We assume that copying will be performed into a larger array. */
	Assert(fromDeltaSpace->nhugepage < toDeltaSpace->nhugepage);
	
	memcpy(&toDeltaSpace->hugepages, &fromDeltaSpace->hugepages, 
					sizeof(dsa_pointer) * fromDeltaSpace->nhugepage);
}

/*
 * ExpandLocatorDeltaSpace
 */
static void
ExpandLocatorDeltaSpace(dsa_area *area, LocatorDeltaSpaceEntry *deltaSpaceEntry)
{
	uint64 curDeltaCapacity = pg_atomic_read_u64(&deltaSpaceEntry->deltaCapacity);
	dsa_pointer dsaGarbageReservedDeltaSpace;
	dsa_pointer dsaNewDeltaSpace;
	LocatorDeltaSpace *newDeltaSpace;
	LocatorGarbageReservedDeltaSpace *garbageReservedDeltaSpace;
	int curHugepageNums, nextHugepageNums;

	/* Create wrapper for garbage collection later. */
 	dsaGarbageReservedDeltaSpace =
		CreateLocatorGarbageReservedDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Get structure. */
	garbageReservedDeltaSpace = 
		LocatorConvertToGarbageReservedDeltaSpace(area, 
							dsaGarbageReservedDeltaSpace);

	/* Set next garbage wrapper. */
	garbageReservedDeltaSpace->next = 
		deltaSpaceEntry->reservedForGarbageCollection;

	/*
	 * Only one worker is responsible for expanding the delta space, and we 
	 * ensure that there is no need for concurrency control in this process.
	 */
	deltaSpaceEntry->reservedForGarbageCollection = dsaGarbageReservedDeltaSpace;

	/* Get current hugepage number. */
	curHugepageNums = curDeltaCapacity / deltaSpaceEntry->deltaNumsPerHugepage;

	/* Get next hugepage number. */
	nextHugepageNums = curHugepageNums + 1;
	Assert((curDeltaCapacity % deltaSpaceEntry->deltaNumsPerHugepage) == 0);

	/* Create expanded delta space. */
	dsaNewDeltaSpace = CreateLocatorDeltaSpace(area, nextHugepageNums);

	/* Copy existing hugepage's dsa pointers to new one. */
	CopyToDeltaSpace(area, deltaSpaceEntry->deltaSpace, dsaNewDeltaSpace);

	/* Add one hugepage to delta space. */
	newDeltaSpace = LocatorConvertToDeltaSpace(area, dsaNewDeltaSpace);
	Assert(newDeltaSpace->hugepages[nextHugepageNums - 1] == 0);
	newDeltaSpace->hugepages[nextHugepageNums - 1] = 
			dsa_allocate(area, LOCATOR_DELTA_PAGE_SIZE);

	/* Change current delta space to new one. */
	deltaSpaceEntry->deltaSpace = dsaNewDeltaSpace;

	pg_memory_barrier();
	pg_atomic_fetch_add_u64(&deltaSpaceEntry->deltaCapacity, 
								deltaSpaceEntry->deltaNumsPerHugepage);
}

/*
 * AppendLocatorDeltaInternal
 */
static void
AppendLocatorDeltaInternal(dsa_area *area, 
						   LocatorDeltaSpaceEntry *deltaSpaceEntry, 
						   uint64_t deltaPosition, LocatorDelta *delta) 
{
	LocatorDeltaSpace *deltaSpace;
	Index hugepageIdx;
	off_t offset;
	Page hugepage;
	LocatorDelta *deltaData;

	/* Get delta space structure.  */
	deltaSpace = LocatorConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Calculate delta position to write. */
	hugepageIdx = deltaPosition / deltaSpaceEntry->deltaNumsPerHugepage;
	offset = deltaPosition % deltaSpaceEntry->deltaNumsPerHugepage;
	Assert(hugepageIdx < deltaSpace->nhugepage);

	/* Get hugepage pointer. */
	hugepage = 
		LocatorConvertToHugepage(area, deltaSpace->hugepages[hugepageIdx]);

	/* Get delta data by using offset. */
	deltaData = 
		(LocatorDelta *) ((char *) hugepage + sizeof(LocatorDelta) * offset);

	/* Write delta. */
	memcpy(deltaData, delta, sizeof(LocatorDelta));

	pg_memory_barrier();

	/* Set the delta as an active (=enabled) delta. */
	deltaData->active = true;
}

/*
 * AppendLocatorDelta
 *
 * Update workers append deltas to delta space in a blocking time.
 */
void
AppendLocatorDelta(dsa_area *area, LocatorExternalCatalog *exCatalog, 
				   LocatorTuplePosition tuplePosition, bool isLevelZero)
{
	dsa_pointer dsaDeltaSpaceEntry;
	LocatorDeltaSpaceEntry *deltaSpaceEntry;
	LocatorDelta modificationDelta;
	uint64_t deltaPosition;

	/* Get correct delta space entry. */
	if (isLevelZero)
 		dsaDeltaSpaceEntry = exCatalog->dsaPartitionDeltaSpaceEntryForLevelZero;
	else
		dsaDeltaSpaceEntry = exCatalog->dsaPartitionDeltaSpaceEntryForOthers;
		
	/* Get delta space entry from the external catalog. */
	deltaSpaceEntry = LocatorConvertToDeltaSpaceEntry(area, dsaDeltaSpaceEntry);

	/* Increment and get delta position atomically. */
	deltaPosition = pg_atomic_fetch_add_u64(&deltaSpaceEntry->deltaNums, 1);

	/*
	 * If the delta position exceeds the capacity, we must wait for 
	 * someone to expand the delta space.
	 */
	for (;;)
	{
		uint64_t deltaCapacity = 
			pg_atomic_read_u64(&deltaSpaceEntry->deltaCapacity);

		if (deltaCapacity < deltaPosition)
		{
			/* Wait for expanding. */
			SPIN_DELAY();
		}
		else if (deltaPosition == deltaCapacity)
		{
			/* The worker has responsibility to expand delta space. */
			ExpandLocatorDeltaSpace(area, deltaSpaceEntry);
			break;
		}
		else
		{
			/* We can store delta safely. */
			break;
		}
	}

	pg_memory_barrier();

	/* 
	 * Append delta directly. In here, we can store delta to delta space 
	 * safely. 
	 */

	/* Init values. */
	modificationDelta.upperTuplePosition = 
		tuplePosition->partitionTuplePosition;
	modificationDelta.partitionNumber = tuplePosition->partitionNumber;
	modificationDelta.partitionGenerationNumber =
		tuplePosition->partitionGenerationNumber;
	modificationDelta.partitionId = tuplePosition->partitionId;
	modificationDelta.lowerTuplePosition = 
		tuplePosition->lowerPartitionTuplePosition;
	modificationDelta.skip = false;
	modificationDelta.preparedBuffer = InvalidBuffer;
	modificationDelta.active = false;

	/* Write delta to delta space. */
	AppendLocatorDeltaInternal(area, deltaSpaceEntry, 
							   deltaPosition, &modificationDelta);
}

/*
 * CheckDuplicationForDelta
 * 
 * Delta duplication is checked and skipped before applying the delta.
 */
static bool
CheckDuplicationForDelta(dsa_area *area, LocatorDeltaSpaceEntry *deltaSpaceEntry, 
						 LocatorDeltaSpace *deltaSpace, uint64 deltaNums, 
						 LocatorDelta *targetDelta)
{
	LocatorDelta *delta;
	uint64 deltaIdx;

	Assert(targetDelta->preparedBuffer == InvalidBuffer);

	/* Iterate all deltas. */
	for (deltaIdx = 0; deltaIdx < deltaNums; ++deltaIdx)
	{
		/* Get a delta using delta idx. */
		delta = GetLocatorDelta(area, deltaSpace, deltaIdx, 
								deltaSpaceEntry->deltaNumsPerHugepage);
		
		/* Skip redundant deltas. */
		if (delta->skip || !delta->active || delta == targetDelta)
			continue;

		/* This delta has same key value. */
		if (targetDelta->upperTuplePosition == delta->upperTuplePosition)
			return true;
	}

	return false;
}

/*
 * LocatorDeltaGetPreparedBuffer
 * 
 * We prepare buffer per delta.
 */
static Buffer
LocatorDeltaAllocPreparedBuffer(LocatorRepartitioningDesc *desc,
								LocatorDelta *delta)
{
	LocatorPartitionInsertDesc *lowerInsert = &(desc->lowerInsert);
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	uint32			recordsPerRecordZone =
		*(lowerInsert->columnarLayout->recordsPerRecordZone);
	uint16				tuplesPerBlock =
		lowerInsert->columnarLayout->columnGroupMetadataArray[0].tuplesPerBlock;
	bool				partial = false;
	Buffer				preparedBuffer;
	LocatorPartLevel	lowerPartitionLevel;
	LocatorPartNumber 	lowerPartitionNumber; 
	LocatorPartitionPrivateBuffer *privateBuffer;
	BlockNumber 		targetRecordZoneNumber;
	BlockNumber 		targetBlockNumber;
	uint32				targetTupleNumber;
	Index 				privateBufferIdx;
	Index 				pageIdx;
	int		fulledRecordZoneCnt;
	Index	recordZoneIdx;

	/* Get upper partition level and lower patition level. */
	lowerPartitionLevel = lowerInsert->targetPartitionLevel;

	/* Get target partition number. */
	lowerPartitionNumber =
		GetPartitionNumberFromPartitionKey(exCatalog->spreadFactor, 
			lowerPartitionLevel, exCatalog->lastPartitionLevel, delta->partitionId);

	/* Get index on private buffers. */
	privateBufferIdx = lowerPartitionNumber % lowerInsert->nprivatebuffer;
	privateBuffer = lowerInsert->privateBuffers[privateBufferIdx];

	/* Calculate record zone number and offset. */
	targetRecordZoneNumber = delta->lowerTuplePosition / recordsPerRecordZone;
	targetTupleNumber = delta->lowerTuplePosition % recordsPerRecordZone;

	/* Calculate target block number and record zone index within private buffer. */
	fulledRecordZoneCnt = privateBuffer->firstRecCnt / recordsPerRecordZone;
	recordZoneIdx = targetRecordZoneNumber - fulledRecordZoneCnt;
	pageIdx = targetTupleNumber / tuplesPerBlock;
	targetBlockNumber = targetRecordZoneNumber * lowerInsert->blocksPerRecordZone + pageIdx;

	/* Check this delta should be written in partial page. */
	if (privateBuffer->partial && recordZoneIdx == 0)
		partial = true;

	/* 
	 * If the delta should be written in partial page and that page is not in 
	 * buffer, we get a page from disk. If the delta should be written in 
	 * non-partial page, we get a page from private buffer.
	 */
	preparedBuffer = ReadPartitionBufferExtended(desc->relation, 
							lowerPartitionLevel, lowerPartitionNumber, 
							privateBuffer->partitionGenerationNumber,
							targetBlockNumber, 
							partial ? RBM_NORMAL : RBM_NO_READ_NO_LOCK, NULL);

	if (!partial)
	{
		Page	sharedBufferPage = BufferGetPage(preparedBuffer);
		Page	privateBufferPage;
		
		/* 
		 * Get page in the private buffer. The repartitioning worker needs to apply 
		 * delta to this page.
		 */
		Assert(recordZoneIdx < privateBuffer->npageRecordZones);
		privateBufferPage = 
			((char *) privateBuffer->pageRecordZones[recordZoneIdx]) + (pageIdx * BLCKSZ);

		/* Write page using private buffer page. */
		memcpy(sharedBufferPage, privateBufferPage, BLCKSZ);
	}

	Assert(!PageIsNew(BufferGetPage(preparedBuffer)));

	/* Return prepared buffer. We get a pin here. */
	return preparedBuffer;
}

/*
 * PreallocSharedBuffersForDeltas
 * 
 * We preallocate shared buffer for fast writing of deltas.
 */
void
PreallocSharedBuffersForDeltas(dsa_area *area, LocatorRepartitioningDesc *desc)
{
	dsa_pointer dsaLocatorPartitionDeltaSpaceEntry;
	LocatorPartitionScanDesc *upperScan = &(desc->upperScan);
	LocatorExternalCatalog *exCatalog = desc->exCatalog;
	LocatorPartLevel upperPartitionLevel;
	LocatorDeltaSpaceEntry *deltaSpaceEntry;
	LocatorDeltaSpace *deltaSpace;
	uint64 deltaNums, deltaCapacity;
	uint64 deltaIdx;

	/* Get upper partition level and lower patition level. */
	upperPartitionLevel = upperScan->partitionLevel;

	/* Get correct delta space entry. */
	if (upperPartitionLevel == 0)
	{
		dsaLocatorPartitionDeltaSpaceEntry = 
					exCatalog->dsaPartitionDeltaSpaceEntryForLevelZero;
	}
	else
	{
		dsaLocatorPartitionDeltaSpaceEntry = 
					exCatalog->dsaPartitionDeltaSpaceEntryForOthers;
	}

	/* Get delta space structure. */
	deltaSpaceEntry =
		LocatorConvertToDeltaSpaceEntry(area, dsaLocatorPartitionDeltaSpaceEntry);
	deltaSpace =
		LocatorConvertToDeltaSpace(area, deltaSpaceEntry->deltaSpace);

	/* Get the total number of delta. */
	deltaNums = pg_atomic_read_u64(&deltaSpaceEntry->deltaNums);
	deltaCapacity = pg_atomic_read_u64(&deltaSpaceEntry->deltaCapacity);

	/* We only preallocate deltas that are smaller than delta capacity. */
	if (deltaCapacity < deltaNums)
		deltaNums = deltaCapacity;

	/* If there are no deltas that need to be applied, simply return. */
	if (deltaNums == 0)
		return;

	/* Iterate all deltas. */
	for (deltaIdx = 0; deltaIdx < deltaNums; ++deltaIdx)
	{
		LocatorDelta *delta;

		/* Get a delta using delta idx. */
		delta = GetLocatorDelta(area, deltaSpace, deltaIdx, 
								deltaSpaceEntry->deltaNumsPerHugepage);

		/* Skip inactive or duplicate or already prepared deltas. */
		if (!delta->active || delta->skip || 
				delta->preparedBuffer != InvalidBuffer)
			continue;

		/*  
		 * Check duplicate deltas. If we meet duplicate deltas, we set 
		 * skip flag as true.
		 */
		if (CheckDuplicationForDelta(area, deltaSpaceEntry, deltaSpace, 
														deltaNums, delta))
		{
			delta->skip = true;
			continue;
		}

		/* Prepare a buffer for optimistic writing. */
		delta->preparedBuffer = LocatorDeltaAllocPreparedBuffer(desc, delta);
	}
}
#endif /* LOCATOR */
