/*-------------------------------------------------------------------------
 *
 * locator_external_catalog.h
 *    locator external catalog
 *
 *
 * src/include/locator/locator_external_catalog.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCATOR_EXTERNAL_CATALOG_H
#define LOCATOR_EXTERNAL_CATALOG_H

#include "utils/dsa.h"
#include "utils/rel.h"
#include "port/atomics.h"
#include "storage/s_lock.h"
#include "storage/lwlock.h"
#include "locator/locator.h"

#ifdef WAIT_YIELD
#include <pthread.h>
#endif /* WAIT_YIELD */

typedef union
{
	uint64 val;
	char pad[PG_CACHE_LINE_SIZE];
} LocatorUint64Padded;

typedef union
{
	pg_atomic_uint64 val;
	char pad[PG_CACHE_LINE_SIZE];
} LocatorAtomicUint64Padded;

typedef struct
{
	Oid relationOid; 	/* target relation oid */
} LocatorExternalCatalogTag;

typedef struct
{
	LocatorExternalCatalogTag tag;
	int id;				/* index */
} LocatorExternalCatalogLookupEntry;

#define LocatorExternalCatalogHashPartition(hashcode) \
							((hashcode) % NUM_LOCATOR_EXTERNAL_CATALOG_PARTITIONS)
#define LocatorExternalCatalogMappingPartitionLock(hashcode)          \
	(&MainLWLockArray[LOCATOR_EXTERNAL_CATALOG_MAPPING_LWLOCK_OFFSET + \
					  LocatorExternalCatalogHashPartition(hashcode)]  \
		  				.lock)
#define LocatorExternalCatalogMemPartitionLock(exCatalog, partNumLevelZero) \
	((LWLock*) (&((exCatalog)->mempartitionLock[partNumLevelZero])))
#define LocatorExternalCatalogRefCounter(exCatalog, level, partNum) \
	(&((exCatalog)->refcounter[level][partNum].val))

#define EXTERNAL_CATALOG_MAX_LEVEL (8)
#define EXTERNAL_CATALOG_BLOCKSIZE (1024 * 1024 * 8) /* 8 MiB */
#define EXTERNAL_CATALOG_REFCOUNTERSIZE (1024 * 1024 * 16) /* 16 MiB */
#define EXTERNAL_CATALOG_PARTITION_XMAXSIZE (1024 * 1024 * 8) /* 8 MiB */

#define MAX_ATTR_COUNT_OF_COLUMN_GROUP	(16)
#define MAX_COLUMN_GROUP_COUNT	(7) /* without recordKey group */

#define IncrBlockCount(exCatalog, n) \
	(pg_atomic_fetch_add_u64((exCatalog)->nblocks, (n)))
#define LocatorGetNBlocks(rel) \
	(pg_atomic_read_u64((LocatorGetExternalCatalog(RelationGetRelid(rel)))->nblocks))

/*
 * reference counter of external catalog
 * (for avoiding conflict with cleaning worker) 
 */
#define BlockCheckpointerBit						(0x80000000)
#define UnsetBlockCheckpointerBit(exCatalog) \
	(pg_atomic_fetch_and_u32(&(exCatalog->partitioningWorkerCounter), ~BlockCheckpointerBit))

#define IncrPartitioningWorkerCount(exCatalog) \
	(pg_atomic_fetch_add_u32(&(exCatalog->partitioningWorkerCounter), 1))
#define DecrPartitioningWorkerCount(exCatalog) \
	(pg_atomic_fetch_sub_u32(&(exCatalog->partitioningWorkerCounter), 1))
#define IsNoWorker(exCatalog) \
	((pg_atomic_read_u32(&(exCatalog->partitioningWorkerCounter)) & ~BlockCheckpointerBit) == 0)

/*
 * reference counter of partitions:
 * 
 * ---------------------------------------------------------------------
 * |     2-bits     |   15-bits    |    15-bits   | 1-bit  |  15-bits  |
 * |      flag      |  reference   |   reference  | toggle | reference |
 * |      bits      |    count     |     count    |  bit   |   count   |
 * ---------------------------------------------------------------------
 * | repartitioning |    normal    | modification |     meta data      |
 * |     status     | modification |   w/ delta   |                    |
 * ---------------------------------------------------------------------
 */

#define MetaDataBit									(0x0000000000008000ULL)
#define MetaDataCount								(0x0000000000000001ULL)
#define MetaDataMask								(0x0000000000007FFFULL)
#define MODIFICATION_WITH_DELTA_COUNT_ONE			(1ULL << 16)
#define MODIFICATION_WITH_DELTA_COUNT_MASK			(0x7FFFULL << 16)
#define NORMAL_MODIFICATION_COUNT_ONE				(1ULL << 31)
#define NORMAL_MODIFICATION_COUNT_MASK				(0x7FFFULL << 31)
#define REPARTITIONING_FLAG_MASK					(0x3ULL << 46)

/* repartitioning status flag */
#define REPARTITIONING_MODIFICATION_BLOCKING		(1ULL << 46)
#define REPARTITIONING_INPROGRESS					(1ULL << 47)

#define SetMetaDataBitInternal(refcounter) \
	(pg_atomic_fetch_or_u64(refcounter, MetaDataBit))
#define UnsetMetaDataBit(refcounter) \
	(pg_atomic_fetch_and_u64(refcounter, ~MetaDataBit))
#define GetMetaDataBit(refcounter) \
	(pg_atomic_read_u64(refcounter) & MetaDataBit)

#define IncrMetaDataCountInternal(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, MetaDataCount) & MetaDataBit)
#define DecrMetaDataCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, MetaDataCount))
#define GetMetaDataCount(refcounter) \
	(pg_atomic_read_u64(refcounter) & MetaDataMask)

#define GetNormalModificationCount(refcounter) \
	((pg_atomic_read_u64(refcounter) & NORMAL_MODIFICATION_COUNT_MASK) >> 31)
#define IncrNormalModificationCount(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, NORMAL_MODIFICATION_COUNT_ONE))
#define DecrNormalModificationCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, NORMAL_MODIFICATION_COUNT_ONE))

#define GetModificationWithDeltaCount(refcounter) \
	((pg_atomic_read_u64(refcounter) & MODIFICATION_WITH_DELTA_COUNT_MASK) >> 16)
#define IncrModificationWithDeltaCount(refcounter) \
	(pg_atomic_fetch_add_u64(refcounter, MODIFICATION_WITH_DELTA_COUNT_ONE))
#define DecrModificationWithDeltaCount(refcounter) \
	(pg_atomic_fetch_sub_u64(refcounter, MODIFICATION_WITH_DELTA_COUNT_ONE))

#pragma GCC push_options
#pragma GCC optimize ("O0")

#ifdef WAIT_YIELD
#define WaitForMetaDataBit(refcounter) \
	do { \
		while (GetMetaDataBit(refcounter) != 0) \
			{pthread_yield();} \
	} while (0)

#define WaitForMetaDataCount(refcounter) \
	do { \
		while (GetMetaDataCount(refcounter) != 0) \
			{pthread_yield();} \
	} while (0)

#define WaitForNormalModificationDone(refcounter) \
	do { \
		while (GetNormalModificationCount(refcounter) != 0) \
		{pthread_yield();} \
	} while (0)

#define WaitForModificationWithDeltaDone(refcounter) \
	do { \
		while (GetModificationWithDeltaCount(refcounter) != 0) \
		{pthread_yield();} \
	} while (0)

#define WaitForPartitioningWorkers(exCatalog) \
	do { \
		while (pg_atomic_read_u32(&(exCatalog->partitioningWorkerCounter)) != 0) \
			{pthread_yield();} \
	} while (0)

#else /* !WAIT_YIELD */

#define WaitForMetaDataBit(refcounter) \
	do { \
		while (GetMetaDataBit(refcounter) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#define WaitForMetaDataCount(refcounter) \
	do { \
		while (GetMetaDataCount(refcounter) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#define WaitForNormalModificationDone(refcounter) \
	do { \
		while (GetNormalModificationCount(refcounter) != 0) \
		{SPIN_DELAY();} \
	} while (0)

#define WaitForModificationWithDeltaDone(refcounter) \
	do { \
		while (GetModificationWithDeltaCount(refcounter) != 0) \
		{SPIN_DELAY();} \
	} while (0)

#define WaitForPartitioningWorkers(exCatalog) \
	do { \
		while (pg_atomic_read_u32(&(exCatalog->partitioningWorkerCounter)) != 0) \
			{SPIN_DELAY();} \
	} while (0)

#endif /* WAIT_YIELD */
#pragma GCC pop_options

#define SetMetaDataBit(refcounter) \
	do { \
		if ((SetMetaDataBitInternal(refcounter) & MetaDataMask) != 0) \
			WaitForMetaDataCount(refcounter); \
	} while (0)

#define IncrMetaDataCount(refcounter) \
	do { \
		while (true) { \
			WaitForMetaDataBit(refcounter); \
			if (likely(IncrMetaDataCountInternal(refcounter) == 0)) \
				break; \
			DecrMetaDataCount(refcounter); \
		} \
	} while (0)

#define CheckPartitioningInLevelZero(exCatalog, partNum, partGen) \
	(exCatalog->partitioningLevel == 0 && \
	 exCatalog->partitioningNumber == partNum && \
	 exCatalog->partitioningMinTieredNumber <= partGen && \
	 exCatalog->partitioningMaxTieredNumber >= partGen)

#define RecordZoneSizeAmplifier	(4) /* heuristic */

typedef struct LocationInfo
{
	uint64 reorganizedRecordsCount: 24,
		   firstSequenceNumber: 40;
} LocationInfo;

/*
 * A row-oriented record is divided into multiple column groups by columnar
 * reformatting on frozen line. A record zone consists of column groups, and each
 * column group consists of 8 KiB pages(blocks).
 *
 *
 * |-----------------------------------------------------------------------|
 * |                             record zone                               |
 * |-----------------|-----------------|-----------------|-----------------|
 * |  columne zone 0 |  column zone 1  |  column zone 2  |  column zone 3  |
 * |--------|--------|--------|--------|--------|--------|--------|--------|
 * | page 0 | page 1 | page 0 | page 1 | page 0 | page 1 | page 0 | page 1 |
 * |--------|--------|--------|--------|--------|--------|--------|--------|
 * | page 2 | page 3 | page 2 | page 3 | page 2 | page 3 | page 2 | page 3 |
 * |--------|--------|--------|--------|--------|--------|--------|--------|
 * | page 4 | page 5 | page 4 | page 5 | page 4 | page 5 | page 4 | page 5 |
 * |--------|--------|--------|--------|--------|--------|--------|--------|
 * |                                   :                                   |
 * |                                   :                                   |
 * |--------|--------|--------|--------|--------|--------|--------|--------|
 *
 *
 * The columnar layout is stored as several arrays in the external catalog.
 * Here's an explanation using an example. (CH-benCHmark)
 * 
 * relation id: 16639 (order_line)
 * 
 * size of original record: 268
 *  = (3 * linepointer + p-locator + 2 * max length of tuple)
 *  = (3 * sizeof(ItemIdData) + MAXALIGN(PITEM_SZ) + 2 * maxlen)
 * 
 * the number of original records per block: 30
 * 
 * the number of attributes in the relation: 11 (normal 10 + hidden 1)
 * 
 * information about the columnar layout as input:
 * {
 * 		target level count: 1	(0: This relation will not be reformatted.
 * 								 1: Only columns that will not be updated will
 * 									be reformatted once.
 * 								 2: Then, when all records are stable(frozen
 * 									and no loner changing), they will be
 * 									reformatted again. (TODO))
 * 
 * 		target levels: 2		([0]: The first target of reformatting
 * 								 [1]: The second target of reformatting)
 * 
 * 		reformatting rules:
 * 			1: Group columns modified by UPDATE
 * 			2: Group columns used for a primary key and RETURNING
 * 			3: Group columns defined as foreign keys
 * 			4: Group columns trivial attributes, including hidden attributes
 * 			5: Group unused columns for both OLTP and OLAP workloads
 * 
 * 		columnar groups:
 * 		level 2:
 * 			group 0: 0		   (group 0 of first target level is SIRO-versioned)
 * 			group 1: 1 2 5 6 8
 * 			group 2: 3 4
 * 			group 3: 7 10
 * 			group 4: 9
 * 		level 5:
 * 			TODO: Implement COLUMNAR layout
 * }
 * 
 * In this case, this information is passed through the following SQL query:
 * {
 * 		SELECT locator_set_columnar_layout(to_regclass('order_line')::oid,'1|2|5|1|0|5|1|2|5|6|8|2|3|4|2|7|10|1|9');
 * 
 * 		1: level count
 * 		2: target level
 * 
 * 		5: the number of column groups at first layout
 * 			1: length of group 0
 * 				0: attributes of group 0
 * 			5: length of group 1
 * 				1|2|5|6|8: attributes of group 1
 * 			2: length of group 2
 * 				3|4: attributes of group 2
 * 			2: length of group 3
 * 				7|10: attributes of group 3
 * 			1: length of group 3
 * 				9: attributes of group 3
 * }
 * 
 * Then this information is stored in the external catalog as follows:
 * (How the values are calculated is described in the LocatorSetColumnarLayout().)
 * {
 *		layoutArray:					| ROW_ORIENTED | ROW_ORIENTED | MIXED | MIXED | MIXED |
 *
 * 		columnarLayoutArray[ROW_ORIENTED]:		   (default row-oriented layout)
 * 			recordsPerRecordZone:		32
 * 			blocksPerRecordZone:		[1, 1]		(internal level, last level) (last level is for COLUMNAR layout, TODO)
 * 			columnGroupCount:			1
 * 			columnGroupArray:			|0|0|0|0|0|0|0|0|0|0|0|
 * 			columnGroupMetadataArray:
 * 				metadata of group 0:
 * 					tupleSize:	 		104
 * 					tuplesPerBlock:		32
 * 					blockOffset:		0
 * 
 * 		columnarLayoutArray[MIXED]:
 * 			recordsPerRecordZone:		1986
 * 			blocksPerRecordZone:		[53, 53]	(internal level, last level)
 * 			columnGroupCount:			5
 * 			columnGroupArray:			|0|1|1|2|2|1|1|3|1|4|3|
 * 			columnGroupMetadataArray:
 * 				metadata of group 0:
 * 					tupleSize:	 		40
 * 					tuplesPerBlock:		65
 * 					blockOffset:		0
 * 				metadata of group 1:
 * 					tupleSize:	 		24
 * 					tuplesPerBlock:		331
 * 					blockOffset:		31
 * 				metadata of group 2:
 * 					tupleSize:	 		16
 * 					tuplesPerBlock:		502
 * 					blockOffset:		37
 * 				metadata of group 3:
 * 					tupleSize:	 		16
 * 					tuplesPerBlock:		502
 * 					blockOffset:		41
 * 				metadata of group 4:
 * 					tupleSize:	 		32
 * 					tuplesPerBlock:		254
 * 					blockOffset:		45
 * }
 * 
 * columnGroupArray[attribute index] = column group number
 * columnGroupMetadataArray[column group number] = metadata of the column group
 * 
 * -----------------------------------------------------------------------------
 * Q:	| At level 2, what column group does the 7th attribute belong to?
 * 		| (The order is counted from 0.)
 * -----|-----------------------------------------------------------------------
 * A:	| layoutArray[2] = 1 (MIXED)
 * 		| columnarLayoutArray[1].columnGroupArray[7] = 3
 * 		|
 *		| - The 7th attribute belongs to the 3rd column group.
 * -----------------------------------------------------------------------------
 * 
 * -----------------------------------------------------------------------------
 * Q:	| In any partition at level 2, where is the 9th attribute of the 65842th
 * 		| record of the partition?
 * 		| (The cumulative count of the 65842th record is 65842.)
 * 		| (The order is counted from 0.)
 * -----|-----------------------------------------------------------------------
 * A:	| layoutArray[2] = 1 (MIXED)
 * 		|
 * 		| columnarLayoutArray[1].recordsPerRecordZone = 1986
 * 		| 65842 / 1986 = 33
 * 		| 65842 % 1986 = 304
 * 		| - The 65842th record of the partition is the 304th record of the 33th
 * 		|	record zone of the partition.
 * 		|
 * 		| columnarGroupArray[1].colomnGroupArray[9] = 4
 * 		| - The 9th attribute belongs to the 4th column group.
 * 		|
 * 		| columnarGroupArray[1].columnGroupMetadataArray[4].blockOffset = 45
 * 		| - The tuples of the 4th column group are started from the 45th block
 * 		|	of the record zone.
 * 		|
 * 		| columnarGroupArray[1].columnGroupMetadataArray[4].recordsPerBlock =
 * 		| 254
 * 		| 304 / 254 = 1
 * 		| 304 % 254 = 50
 * 		| - The 9th attribute of the 304th record of the record zone is in the
 * 		|	50th tuple of the 46th(45 + 1) block of the record zone.
 * 		|
 *		| - The 9th attribute of the 65842th record of the partition is in the
 *		|	33th record zone, 46th block, 50th tuple.
 *		|	(block number: 33 * 53(blocks per record zone) + 46)
 * -----------------------------------------------------------------------------
 */

typedef enum LocatorLayoutType
{
	ROW_ORIENTED,
	MIXED,
	COLUMNAR /* TODO */
} LocatorLayoutType;

typedef struct ColumnGroupMetadata
{
	uint16	tupleSize;		/* size of a tuple */
	uint16	tuplesPerBlock;	/* the number of tuples per block */
	uint32	blockOffset;	/* offset in huge page where the first tuple is stored */
} ColumnGroupMetadata;

typedef struct ColumnarLayout
{
	uint32					*recordsPerRecordZone;
	uint32					*blocksPerRecordZone; /* [0]: for internal level */
											 /* [1]: for last level (for COLUMNAR layout) */
	uint8					*columnGroupCount;
	uint8					*columnGroupArray;
	ColumnGroupMetadata		*columnGroupMetadataArray;
} ColumnarLayout;

/*
 * LocatorExternalCatalog
 */
typedef struct LocatorExternalCatalog
{
	Index exCatalogId;				/* identity */
	LocatorExternalCatalogTag tag;	/* tag for hashtable logic */
	bool inited;					/* is init? */

	/*
	 * msb: blocking checkpointer, left bits: worker counter
	 * concurrency control between partitioning worker and checkpointing worker
	 */
	pg_atomic_uint32 partitioningWorkerCounter;
	
	/*
	 * When the value of 'spreadFactor' is two, our perspective is as follows:
	 * 
	 * [Example] (level count: n)
	 *
	 * (level)
	 *    0     ▢▢▢
	 *           ↓--------↘
	 * 	  1     ▢▢▢       ▢▢▢
	 *           ↓---↘     ↓---↘
	 *    2     ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢
	 *           ↓↘   ↓↘   ↓↘   ↓↘
	 *   ...
	 *           ↓    ↓    ↓    ↓
	 *   n-1    ▢▢▢  ▢▢▢  ▢▢▢  ▢▢▢  ...
	 *
	 * We utilize the first index of generationNum as the partition level and 
	 * the second index as the partition number.						
	 */

	int spreadFactor;							/* fanout number */
	int lastPartitionLevel;						/* highest partition level */

	uint16 recordSize;
	uint16 recordNumsPerBlock;

	LocatorPartLevel partitioningLevel;
	LocatorPartNumber partitioningNumber;
	LocatorTieredNumber partitioningMinTieredNumber;
	LocatorTieredNumber partitioningMaxTieredNumber;

	LocatorUint64Padded *sequenceNumberCounter;
	pg_atomic_uint64 *nblocks;

	/* 
	 * We have two workers for partitioning, so we control the concurrency 
	 * between them using locks. 
	 */
	LWLockPadded levelZeroPartitioningLock[MAX_SPREAD_FACTOR];

	/*
	 * - protection -
	 * external catalog lvl0 lock:
	 *  - PreviousTieredNumberLevelZero[]
	 *  - CurrentTieredNumberLevelZero[]
	 *  - CurrentRecordCountsLevelZero[]
	 *  - TieredRecordCountsLevelZero[]
	 * ------------------------------------
	 * external catalog lvl0 ref counter:
	 *  - ReorganizedRecordCountsLevelZero[]
	 */
	LWLockPadded	mempartitionLock[MAX_SPREAD_FACTOR];    /* [partition number] */

	/* (Last repartitioning's max partition number) + 1 */
	LocatorTieredNumber *PreviousTieredNumberLevelZero;		/* [partition number] */

	/* Current partition number */
	LocatorTieredNumber *CurrentTieredNumberLevelZero;		/* [partition number] */

	LocatorUint64Padded *CurrentRecordCountsLevelZero;		/* [partition number] */
	LocatorUint64Padded *TieredRecordCountsLevelZero;		/* [partition number] */
	LocatorUint64Padded *ReorganizedRecordCountsLevelZero;	/* [partition number] */

	/* locator delta space - LocatorDeltaSpaceEntry */

	/* for level 0 */
	volatile dsa_pointer dsaPartitionDeltaSpaceEntryForLevelZero;	
	/* for level 1 and above */
	volatile dsa_pointer dsaPartitionDeltaSpaceEntryForOthers;	

	/* 
	 * The generation number for partition levels 0 and N (highest) is both set 
	 * to 0, so this variable does not reference the first and last levels.
	 * This implies that 'generationNums[0]' and 'generationNums[n]' have NULL 
	 * values.
	 */
	LocatorPartGenNo *generationNums[MAX_SPREAD_FACTOR];			/* [level][number] */
	uint64 *realRecordsCount[MAX_SPREAD_FACTOR];					/* [level][number] */
	LocatorAtomicUint64Padded *refcounter[MAX_SPREAD_FACTOR];		/* [level][number] */
	TransactionId *partitionXmax[MAX_SPREAD_FACTOR];				/* [level][number] */
	LocationInfo *locationInfo[MAX_SPREAD_FACTOR];					/* [level][number] */
	LocatorInPartCnt *internalCumulativeCount[MAX_SPREAD_FACTOR];	/* [internal-level][number] */
	LocatorSeqNum *lastCumulativeCount;								/* [  last-level  ][number] */

	uint8			*layoutArray;
	ColumnarLayout	columnarLayoutArray[EXTERNAL_CATALOG_MAX_LEVEL];

	char refcounter_space[EXTERNAL_CATALOG_REFCOUNTERSIZE];
	char partition_xmax_space[EXTERNAL_CATALOG_PARTITION_XMAXSIZE];
	char block[EXTERNAL_CATALOG_BLOCKSIZE];	/* meta block */

	/*
	 * These are only for repartitioning of checkpointer.
	 * Checkpointer can't open relation during repartitioning, so used backed up
	 * data.
	 */
	RelationData relation;
	SMgrRelationData smgr;
	TupleDescData tupDesc;
} LocatorExternalCatalog;

#define LOCATOR_EXTERNAL_CATALOG_PAD_TO_SIZE \
							(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union LocatorExternalCatalogPadded {
	LocatorExternalCatalog exCatalog;
	char pad[LOCATOR_EXTERNAL_CATALOG_PAD_TO_SIZE];
} LocatorExternalCatalogPadded;

/* Metadata per locator's external catalog */
typedef struct LocatorExternalCatalogGlobalMetaData
{
	dsa_handle handle;		/* dsa handle */
	int externalCatalogNums;
	LWLockPadded globalMetaLock;
} LocatorExternalCatalogGlobalMetaData;

extern LocatorExternalCatalogPadded *LocatorExternalCatalogs;
extern LocatorExternalCatalogGlobalMetaData *LocatorExternalCatalogGlobalMeta;

/* Macros used as helper functions */
#define ExCatalogIndexGetLocatorExternalCatalog(id) \
							(&LocatorExternalCatalogs[(id)].exCatalog)

/* backend/locator/partitioning/locator_external_catalog.c */
extern dsa_area* external_catalog_dsa_area;

/* Public functions */

/* backend/locator/partitioning/locator_external_catalog.c */
extern Size LocatorExternalCatalogShmemSize(void);
extern void LocatorExternalCatalogInit(void);
extern LocatorExternalCatalog *LocatorGetExternalCatalog(Oid relationOid);
extern LocatorExternalCatalog *LocatorCreateExternalCatalog(Oid relationOid);
extern void LocatorFlushExternalCatalog(LocatorExternalCatalog *exCatalog);
extern void SetRepartitioningStatusInprogress(pg_atomic_uint64 *refcounter);
extern void SetRepartitioningStatusModificationBlocking(pg_atomic_uint64 *refcounter);
extern void SetRepartitioningStatusDone(pg_atomic_uint64 *refcounter);
extern LocatorModificationMethod GetModificationMethod(LocatorExternalCatalog *exCatalog, 
													   pg_atomic_uint64 *refcounter,
													   LocatorPartLevel targetPartitionLevel, 
													   LocatorPartNumber targetPartitionNumber);
extern void DecrModificationCount(pg_atomic_uint64 *refcounter, 
								  LocatorModificationMethod modificationMethod);
extern void LocatorFlushAllExternalCatalogs(void);

/* backend/locator/partitioning/locator_external_catalog_hash.c */
extern Size LocatorExternalCatalogHashShmemSize(int size);
extern void LocatorExternalCatalogHashInit(int size);
extern uint32 LocatorExternalCatalogHashCode(const LocatorExternalCatalogTag* tagPtr);
extern int LocatorExternalCatalogHashLookup(const LocatorExternalCatalogTag* tagPtr, 
											uint32 hashcode);
extern int LocatorExternalCatalogHashInsert(const LocatorExternalCatalogTag* tagPtr,
											uint32 hashcode, int page_id);
extern void LocatorExternalCatalogHashDelete(const LocatorExternalCatalogTag* tagPtr, 
											 uint32 hashcode);

/* backend/locator/partitioning/locator_external_catalog_file.c */
extern void LocatorCreateExternalCatalogFile(const Oid relationOid);
extern bool LocatorExternalCatalogFileExists(const Oid relationOid);
extern void LocatorReadExternalCatalogFile(const Oid relationOid, char *block);
extern void LocatorWriteExternalCatalogFile(const Oid relationOid, char *block);

/* backend/locator/partitioning/locator_external_catalog_utils.c */
extern int LocatorGetPartitionNumsForLevel(LocatorExternalCatalog *exCatalog,
										   int partitionLevel);
extern LocatorPartGenNo LocatorGetPartGenerationNumber(LocatorExternalCatalog *exCatalog, 
													   LocatorPartLevel partitionLevel, 
													   LocatorPartNumber partitionNumber);
extern uint64 LocatorGetNumRows(Relation relation);
extern void LocatorCheckAllExternalCatalogRecordsCount(void);
extern void UnsetAllBlockCheckpointerBits(void);

extern void LocatorSetColumnarLayout(Oid relationOid, char *raw_string);

#endif /* LOCATOR_EXTERNAL_CATALOG_H */
