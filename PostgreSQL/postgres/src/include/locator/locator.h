/*-------------------------------------------------------------------------
 *
 * locator.h
 *	  Fundamental definitions for LOCATOR.
 *
 *
 * src/include/locator/locator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#include "c.h"

typedef uint32 LocatorPartLevel;
typedef uint32 LocatorPartNumber;
typedef uint32 LocatorPartGenNo;
typedef uint32 LocatorTieredNumber;
typedef uint64 LocatorSeqNum;

#define InvalidLocatorPartLevel		((LocatorPartLevel)		0xFFFFFFFF)
#define InvalidLocatorPartNumber	((LocatorPartNumber)	0xFFFFFFFF)
#define InvalidLocatorPartGenNo		((LocatorPartGenNo)		0xFFFFFFFF)
#define InvalidLocatorTieredNumber	((LocatorPartGenNo)		0xFFFFFFFF)
#define InvalidLocatorSeqNum		((LocatorSeqNum)		0xFFFFFFFFFFFFFFFF)

#define CUMULATIVECOUNT_MASK		(0x0000000000FFFFFF) /* 3-bytes */

#define MAX_SPREAD_FACTOR (100)

typedef struct LocatorRecordKey
{
	uint64 partKey: 24,
		   seqnum: 40;
} LocatorRecordKey;

typedef struct LocatorInPartCnt
{
	uint32 cumulative_count:24,
		   padding: 8; /* */
} LocatorInPartCnt;

typedef struct LocatorRecordPositions
{
	LocatorTieredNumber first_lvl_tierednum;
	uint32	first_lvl_pos;
	uint64	last_lvl_pos;
	LocatorInPartCnt in_partition_counts[FLEXIBLE_ARRAY_MEMBER];
} LocatorRecordPositions;

typedef struct LocatorRouteSynopsisData
{
	LocatorRecordKey		rec_key;
	LocatorRecordPositions	rec_pos;
} LocatorRouteSynopsisData;

typedef struct LocatorRouteSynopsisData *LocatorRouteSynopsis;


/* modification method 
 *
 * ====================== Repartitioning Worker Flow =========================
 *
 * (1) repartitioning                   (2)  set            (3) repartitioning
 *         start                           blocking                  end
 *           ↓                                ↓                       ↓
 * Time -------------------------------------------------------------------->
 *               ↑           ↑           ↑             ↑            ↑
 *                MODIFICATION_WITH_DELTA           MODIFICATION_BLOCKING 
 *                           
 * ====================== Transactions for Modification ======================
 */
typedef enum {
	NORMAL_MODIFICATION = 1, 	/* repartitioning is not inprogress. */
	MODIFICATION_WITH_DELTA,
	MODIFICATION_BLOCKING
} LocatorModificationMethod;

/*
 * LocatorTuplePosition
 * 		We save the position of tuple within this struct. Partition level, 
 *		partition number, partition generation number, and real tuple position
 *		are saved. In this context, partitionTuplePosition represents a tuple 
 *		count, not a byte unit.
 */
typedef struct LocatorTuplePositionData
{
	LocatorPartLevel partitionLevel;	/* partition level to access */
	LocatorPartNumber partitionNumber;	/* partition number to access */
	LocatorPartGenNo partitionGenerationNumber; /* partition generation number */

	/* the selected position in the target partition */
	LocatorSeqNum partitionTuplePosition;

	LocatorRecordKey rec_key; /* For assertion */

	/* status for modification delta */
	bool needDecrRefCount;
	LocatorModificationMethod modificationMethod;
	int partitionId;
	LocatorSeqNum lowerPartitionTuplePosition;
} LocatorTuplePositionData;

typedef struct LocatorTuplePositionData *LocatorTuplePosition;

#define SizeOfLocatorRouteSynopsisData(lvl_cnt) \
	(sizeof(LocatorRecordKey) + \
	 offsetof(LocatorRecordPositions, in_partition_counts) + \
	 ((lvl_cnt - 2) * sizeof(LocatorInPartCnt)))

#define NRecordZonesUsingRecordCount(rec_cnt, rec_per_recordzone) \
	(((rec_cnt - 1) / rec_per_recordzone) + 1)
#define LastRecordCountUsingRecordCount(rec_cnt, rec_per_block) \
	(((rec_cnt - 1) % rec_per_block) + 1)

#define IndexTupleGetRouteSynopsisOffset(itup) \
	(*((unsigned short *) &itup->t_tid))
#define IndexTupleSetRouteSynopsisOffset(itup, offset) \
	(*((unsigned short *) &itup->t_tid) = offset)

#define GetLocatorRouteSynopsis(itup) \
	((LocatorRouteSynopsis)(((char *)itup) + IndexTupleGetRouteSynopsisOffset(itup)))

#endif /* LOCATOR_H */
