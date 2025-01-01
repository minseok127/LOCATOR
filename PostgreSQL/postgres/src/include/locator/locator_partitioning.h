/*-------------------------------------------------------------------------
 *
 * locator_partitioning.h
 *	  repartitioning implementation of LOCATOR.
 *
 *
 * src/include/locator/locator_partitioning.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_PARTITIONING_H
#define LOCATOR_PARTITIONING_H

#include <liburing.h>
#include "utils/dsa.h"
#include "pg_config.h"
#include "utils/relcache.h"
#include "access/htup_details.h"
#include "locator/locator.h"
#include "locator/locator_external_catalog.h"
#include "postmaster/locator_partition_mgr.h"

/*
 * GetTimeIntervalMillisecond - Get time difference in millisecond.
 */
#define GetTimeIntervalMillisecond(start_time, end_time) \
    (((double) (end_time)->tv_sec - (start_time)->tv_sec) * 1e3 + \
					((double) (end_time)->tv_nsec - (start_time)->tv_nsec) / 1e6)

/*
 * XXX: PRIVATEBUFFER_INIT_CAPACITY must be a power of 2.
 */
#define PRIVATEBUFFER_INIT_CAPACITY \
			((LOCATOR_REPARTITIONING_THRESHOLD / BLCKSZ) / 8)
#define PRIVATEBUFFER_ENLARGE_CAPACITY (4)

#define LocatorRecordsPerBlock(relation) \
			((relation)->records_per_block)

/*
 * We use this structure to store siro tuple's address and size. We access the
 * tuple using stored address and size.
 */
typedef struct SiroTupleData
{
	/* Is p_pd_gen is same with pd_gen */
	bool			isUpdated;

	/* Is the record aborted? */
	bool			isAborted;

	/* Is the left version most recent? */
	bool			isLeftMostRecent;

	/* Length */
	uint32			plocatorLen;		/* length of *plocatorData */
	uint32			leftVersionLen;		/* length of *leftVersionData */
	uint32			rightVersionLen;	/* length of *rightVersionData */

	/* Data */
	Item			plocatorData;		/* tuple header and data */
	HeapTupleHeader leftVersionData;	/* tuple header and data */
	HeapTupleHeader rightVersionData;	/* tuple header and data */
} SiroTupleData;

typedef struct ColumnarTupleData
{
	bool			isnull[MAX_ATTR_COUNT_OF_COLUMN_GROUP];
	Item			columnarTupleData;
} ColumnarTupleData;

StaticAssertDecl((sizeof(LocatorRecordKey) + sizeof(ColumnarTupleData)) <= sizeof(SiroTupleData),
				 "ColumnarTupleData size setting");

typedef struct LocatorRecordData
{
	/*
	 * -----------------------------------------
	 * |                   | recordKey 8 bytes |
	 * |                   |-------------------|
	 * |     siroTuple     |      padded       |
	 * |     40  bytes     |      8 bytes      |
	 * |                   |-------------------|
	 * |                   | columnarTuple[0]  |
	 * |                   |      24 bytes     |
	 * --------------------|-------------------|
	 *                     | columnarTuple[1]  |
	 *                     |      24 bytes     |
	 *                     |-------------------|
	 *                     | columnarTuple[2]  |
	 *                     |      24 bytes     |
	 *                     |-------------------|
	 *                     | columnarTuple[3]  |
	 *                     |      24 bytes     |
	 *                     |-------------------|
	 *                     |         :         |
	 *                     |         :         |
	 */
	union
	{
		SiroTupleData			siroTuple;	/* for ROW_ORIENTED layout and
											   column group 0 of MIXED layout */
		struct
		{
			LocatorRecordKey	recordKey;	/* for COLUMNAR layout */
			char				padded[sizeof(SiroTupleData) -
									   (sizeof(LocatorRecordKey) +
									    sizeof(ColumnarTupleData))];
			ColumnarTupleData	columnarTuple[MAX_COLUMN_GROUP_COUNT];
		};
	};
} LocatorRecordData;

typedef SiroTupleData *SiroTuple;
typedef ColumnarTupleData *ColumnarTuple;
typedef LocatorRecordData *LocatorRecord;

typedef struct LocatorColumnGroupScanDesc
{
	/* State set up at the beginning of repartitioning. */
	BlockNumber			currentBlock;
	Buffer				currentBuffer;
	int					currentIndex;
	int					nrecords;
} LocatorColumnGroupScanDesc;

typedef struct LocatorReformattingDesc
{
	TransactionId xmin;

	/* only for making SIRO tuple */
	bool	*siroIsnull;

	bool	*leftIsnull;
	bool	*rightIsnull;
	Datum	*leftValues;
	Datum	*rightValues;

	uint8	tupleAttrCounts[MAX_COLUMN_GROUP_COUNT];
	uint8	tupleAttrs[MAX_COLUMN_GROUP_COUNT][MAX_COLUMN_GROUP_COUNT];

	char	**tuples;
} LocatorReformattingDesc;

/*
 * When scanning partition files for repartitioning, repartitioning worker saves
 * scan status to this structure.
 */
typedef struct LocatorPartitionScanDesc 
{
	LocatorExternalCatalog *exCatalog;

	/* State set up at the beginning of repartitioning. */
	Relation					relation;
	LocatorPartLevel			partitionLevel;
	LocatorPartNumber			partitionNumber;
	LocatorTieredNumber			minTieredNumber;
	LocatorTieredNumber			maxTieredNumber;
	ColumnarLayout				*columnarLayout;
	LocatorLayoutType			layoutType;
	uint16						columnGroupCount;
	uint16						recordKeyGroupIndex;
	uint32						blocksPerRecordZone;
	uint8						attrCount[MAX_COLUMN_GROUP_COUNT + 1];
	BufferAccessStrategy		strategy;
	bool						layoutNeedsToBeChanged;
	LocatorReformattingDesc		reformattingDesc;
	uint64						blockingTime;		/* us */

	/* state set up at the beginning of each partition scan. */
	LocatorPartGenNo			currentPartitionGenerationNumber;
	BlockNumber					nrecordzones;		/* total number of record zones in partition */
	bool						firstScan;

	/* scan current state */
	BlockNumber					currentRecordZone;
	LocatorColumnGroupScanDesc	columnGroups[MAX_COLUMN_GROUP_COUNT + 1];
	LocatorRecord				currentRecord;	/* current tuple in scan, if any */

	int							currentIndex;	/* current tuple's index in vistuples */
	int							nrecords;		/* number of visible tuples on page */
	LocatorRecord				locatorRecords;

	/*
	 * There are two types of objects that repartition: partitioning worker and
	 * checkpointer.
	 * 
	 * Normally, only the partitioning worker does the repartitioning, but when
	 * the PostgreSQL server shuts down, the checkpointer does the last
	 * repartitioning, and it only repartitions level 0, including the newest
	 * mempartition.
	 * 
	 * The type of object being repartitioned determines whether the newest
	 * mempartition is repartitioned or not, so a flag is needed to distinguish
	 * between the two.
	 */
	bool				isPartitioningWorker;

#ifdef LOCATOR_DEBUG
	/* for debug */
	uint64				repartitionedNrecord_partition;
	int					unstableRecords;
#endif /* LOCATOR_DEBUG */
} LocatorPartitionScanDesc;

/*
 * Private buffer is a local memory. Repartitioning worker appends tuples to
 * private buffer's 'pages' area. Also, the status of private buffer is stored 
 * here.
 */
typedef struct LocatorPartitionPrivateBuffer
{
	Page		*pageRecordZones;	/* record zone array */
	int			npageRecordZones;	/* current # of record zone */
	int			capacity;		/* page array capacity */
    uint16		*alignments;	/* alignment for io_uring */
	int			enlargeCount;

	/* private buffer current state */
	LocatorPartNumber	partitionNumber;					  /* target partition number */
	LocatorPartGenNo	partitionGenerationNumber;			  /* target generation number */
	int					firstRecCnt;						  /* the # of tuple before partitioning. */
	int					currentRecCnt;						  /* the # of tuple. */
	bool				partials[MAX_COLUMN_GROUP_COUNT + 1]; /* does the record zone have partial pages? */
	bool				partial;							  /* does it have partial record zone? */
} LocatorPartitionPrivateBuffer;

/*
 * When moving upper partition's tuples to downside, repartitioning uses this 
 * insertion description.
 */
typedef struct LocatorPartitionInsertDesc
{
	/* state set up at the beginning of repartitioning. */
	ColumnarLayout		*columnarLayout;
	LocatorLayoutType	layoutType;
	uint16				columnGroupCount;
	uint16				recordKeyGroupIndex;
	uint32				blocksPerRecordZone;
	uint16				tupleSize[MAX_COLUMN_GROUP_COUNT + 1];
	uint16				tuplesPerBlock[MAX_COLUMN_GROUP_COUNT + 1];
	uint32				blockOffset[MAX_COLUMN_GROUP_COUNT + 1];
	uint8				attrCount[MAX_COLUMN_GROUP_COUNT + 1];

	/* insert current state */
	LocatorPartNumber	targetPartitionNumber; /* partition number for insertion */
	LocatorPartLevel	upperPartitionLevel; /* source partition level */
	LocatorPartLevel	targetPartitionLevel; /* destination partition level */

	/* private memory space */
	LocatorPartitionPrivateBuffer **privateBuffers;
	int 	nprivatebuffer;		/* the # of private buffer space */
	int		startTargetPartitionNumber;
} LocatorPartitionInsertDesc;

/*
 * Locator's repartitioning description
 */
typedef struct LocatorRepartitioningDesc 
{
	/* state set up at the beginning of repartitioning. */
	LocatorExternalCatalog 	*exCatalog;
	Relation				relation;

	/* upper partition info */
	LocatorPartitionScanDesc upperScan;

	/* insert info */
	LocatorPartitionInsertDesc lowerInsert;

	MemoryContext			descTopMemoryContext;

	struct io_uring			*ring;

	/* for debugging. */
	uint64 deltaNums;
	uint64 parentRecordZones;
	uint64 childRecordZones;

	uint32 parentRecordsPerRecordZone;
	uint32 parentBlocksPerRecordZone;
	uint32 childRecordsPerRecordZone;
	uint32 childBlocksPerRecordZone;
} LocatorRepartitioningDesc;

#define LOCATOR_DELTA_PAGE_SIZE (64 * 1024) /* 64 KiB */

/*
 * LocatorDelta
 *
 * Delta data structure stored in a delta space.
 */
typedef struct LocatorDelta
{
	/* member variables to optimize repartitioning. */
	bool active;			/* delta creation is complete. */
	bool skip;				/* delta overlaps and is skipped. */
	Buffer preparedBuffer;	/* prepared buffer. */

	/* member variables to search tuple from upper partition. */
	int upperTuplePosition; /* used for finding offset in partition file. */
	LocatorPartNumber partitionNumber; 
	LocatorPartGenNo partitionGenerationNumber; 

	/* member variables to copy tuple to lower target partition. */
	int partitionId;		/* used for finding target partition. */
	int64_t lowerTuplePosition; /* used for finding offset in partition file. */
} LocatorDelta;

/*
 * LocatorGarbageReservedDeltaSpace
 */
typedef struct LocatorGarbageReservedDeltaSpace
{
	dsa_pointer currentItem; /* delta space to do garbage collection. */
	dsa_pointer next; 		 /* next 'LocatorGarbageReservedDeltaSpace'. */
} LocatorGarbageReservedDeltaSpace;

/*
 * LocatorDeltaSpace
 */
typedef struct LocatorDeltaSpace
{
	int nhugepage;

	/* Page size is the same 2 MiB. */
	dsa_pointer hugepages[FLEXIBLE_ARRAY_MEMBER]; /* should be last member. */
} LocatorDeltaSpace;

/*
 * LocatorDeltaSpaceEntry
 * 
 * We make deltas on update workers for concurrent updates. This delta space 
 * entry is used to access and save deltas on both a update worker and 
 * a repartitioning worker.
 */
typedef struct LocatorDeltaSpaceEntry
{
	int SizePerHugepage;			/* bytes unit */
	int deltaNumsPerHugepage;

	pg_atomic_uint64 deltaNums;		/* delta nums */
	pg_atomic_uint64 deltaCapacity;	/* delta space's capacity */

	/* LocatorDeltaSpace */
	dsa_pointer 	 deltaSpace;

	/* LocatorGarbageReservedDeltaSpace */
	dsa_pointer		 reservedForGarbageCollection;
} LocatorDeltaSpaceEntry;

typedef struct IoVecsData
{
	struct iovec	*iovs;
	int				iovcnt;
} IoVecsData;
typedef IoVecsData *IoVecs;

typedef struct LocatorLowerIOVecData
{
	int				fd;
	IoVecs			iovecsArray;
} LocatorLowerIOVecData;
typedef LocatorLowerIOVecData *LocatorLowerIOVec;

/* Public functions. */

/* backend/locator/partitioning/locator_partitioning.c */
extern void LocatorReformatSiroTuple(LocatorRepartitioningDesc *desc,
								     SiroTuple siroTuple);
extern LocatorRecord LocatorPartitionGetRecord(LocatorPartitionScanDesc *lscan);
extern void LocatorRepartitioning(LocatorRepartitioningDesc *desc,
								  Oid relationOid,
								  uint64 recordCount,
								  LocatorPartLevel upperPartitionLevel,
								  LocatorPartNumber upperPartitionNumber,
								  LocatorTieredNumber upperMinTieredNumber,
								  LocatorTieredNumber upperMaxTieredNumber,
								  LocatorPartLevel targetPartitionLevel,
								  uint32 *reorganizedRecCnt,
								  LocatorSeqNum *firstSeqNums,
								  bool isPartitioningWorker);
extern uint32 LocatorPartitionAppend(LocatorRepartitioningDesc *desc, 
									 LocatorRecord locatorRecord,
					   				 LocatorSeqNum *firstSeqNums);
extern LocatorPartNumber GetPartitionNumberFromPartitionKey(int spreadFactor, 
													LocatorPartLevel destLevel, 
													LocatorPartLevel lastLevel, 
													int partitionId);

/* backend/locator/partitioning/locator_partitioning_delta.c */
extern dsa_pointer CreateLocatorDeltaSpaceEntry(dsa_area *area);
extern void DeleteLocatorDeltaSpaceEntry(dsa_area *area, 
										 dsa_pointer dsaDeltaSpaceEntry);
extern void AppendLocatorDelta(dsa_area *area, LocatorExternalCatalog *exCatalog, 
							   LocatorTuplePosition tuplePosition, 
							   bool isLevelZero);
extern void ApplyLocatorDeltas(dsa_area *area, LocatorRepartitioningDesc *desc);
extern void RepartitionNewestMempartition(int flags);
extern void PreallocSharedBuffersForDeltas(dsa_area *area, 
										   LocatorRepartitioningDesc *desc);

extern PGDLLIMPORT bool enable_uring_partitioning;
#endif /* LOCATOR_PARTITIONING_H */
