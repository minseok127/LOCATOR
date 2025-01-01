/*-------------------------------------------------------------------------
 *
 * ebi_tree.h
 *    EBI Tree
 *
 *
 *
 * src/include/storage/ebi_tree.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_H
#define EBI_TREE_H

#include <time.h>

#include "c.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"
#ifdef LOCATOR
#include "locator/locator.h"
#endif /* LOCATOR */

/*
 * VersionOffset(64) = SegmentId(32) + SegmentOffset (32)
 * +---------------------+---------------------+
 * |   segment id(32)    | segment offset (32) |
 * +---------------------+---------------------+
 */
typedef uint64 EbiTreeVersionOffset;
typedef uint32 EbiTreeSegmentId;
typedef uint32 EbiTreeSegmentOffset;

typedef uint32 EbiTreeSegmentPageId;

#define EbiGetCurrentTime(timespec) clock_gettime(CLOCK_MONOTONIC, timespec)
#define EbiGetTimeElapsedInSeconds(now, base) \
	(1000LL * (now.tv_sec - base.tv_sec) + (now.tv_nsec - base.tv_nsec) / 1000000LL)
	
#define MAX_NODE_HEIGHT (32)

#define EBI_MAX_SUBNODES (20)
#define EBI_SUB_VERSION_METADATA (4)
#define MAX_SEGMENTS_PER_EBI_SCAN (1024)
#ifdef LOCATOR
#define LOCATOR_MAX_OLD_FILES (128)
#endif /* LOCATOR */

typedef uint64 EbiSubVersionOffset;
typedef uint32 EbiSubSegmentId;
typedef uint32 EbiSubSegmentOffset;
typedef uint32 EbiSubSegmentPageId;

typedef struct EbiNodeData
{
	dsa_pointer parent;
	dsa_pointer left;
	dsa_pointer right;
	dsa_pointer proxy_target;
	dsa_pointer proxy_list_tail; /* tail pointer used for appending */

#ifdef DIVA_PRINT
	uint32 left_most;
#endif
	uint32 height;
	pg_atomic_uint32 refcnt;

	dsa_pointer left_boundary;
	dsa_pointer right_boundary;

	EbiTreeSegmentId seg_id;       /* file segment */
	pg_atomic_uint32 seg_offset;   /* aligned version offset */
	pg_atomic_uint64 num_versions; /* number of versions */

	dsa_pointer subnodes[EBI_MAX_SUBNODES]; /* EbiSubNode[EBI_MAX_SUBNODES] */
	Oid rel_ids[EBI_MAX_SUBNODES];
	uint8 num_subnodes;
	LWLock lock; /* Protects the subnode list from concurrent insertion */
} EbiNodeData;

typedef struct EbiNodeData* EbiNode;

#ifdef LOCATOR
typedef struct OldPartitions
{
	Oid db_id;
	LocatorPartLevel part_lvl;
	LocatorPartNumber part_num;
	LocatorPartGenNo min_part_gen;
	LocatorPartGenNo max_part_gen;
} OldPartitions;
#endif /* LOCATOR */

typedef struct EbiSubNodeData
{
	Oid rel_id;
	EbiSubSegmentId seg_id;
	int max_page_id;
	pg_atomic_uint32 seg_offset; /* aligned version offset in subnode */
	pg_atomic_uint64 num_versions; /* number of versions */
#ifdef LOCATOR
	pg_atomic_uint32 old_partitions_counter;
	OldPartitions old_partitions[LOCATOR_MAX_OLD_FILES];
#endif /* LOCATOR */
} EbiSubNodeData;

typedef struct EbiSubNodeData* EbiSubNode;

#if 0
typedef struct EbiPageInfo
{
	EbiSubSegmentId 	seg_id;
	int 				max_page_id;
	int					min_page_id;
} EbiPageInfo;
#endif

typedef struct EbiUnobtainedSegmentData
{
	EbiSubSegmentId		seg_id;			/* Unobtained ebi segment id */
	uint32 				max_page_id;	/* Max page id among versions */
	uint32				min_page_id;	/* Min page id among versions */
	uint64				match_tuple_cnt;	/* matched tuple # */
	uint64				match_visible_cnt;	/* matched tuple # */

	EbiTreeVersionOffset *head;			/* Head of sorted versions */
	EbiTreeVersionOffset *tail;			/* Tail of sorted versions */
} EbiUnobtainedSegmentData;

typedef struct EbiUnobtainedSegmentData* EbiUnobtainedSegment;

typedef struct EbiTreeData
{
	dsa_pointer root;        /* EbiNode */
	dsa_pointer recent_node; /* EbiNode */

} EbiTreeData;

typedef struct EbiTreeData* EbiTree;

typedef struct EbiMpscQueueNodeData
{
	dsa_pointer dsa_node;    /* EbiNode */
	dsa_pointer_atomic next; /* EbiMpscQueueNode */
} EbiMpscQueueNodeData;

typedef struct EbiMpscQueueNodeData* EbiMpscQueueNode;

typedef struct EbiMpscQueueStruct
{
	dsa_pointer front; /* EbiMpscQueueNode */
	dsa_pointer rear;  /* EbiMpscQueueNode */
	pg_atomic_uint32 cnt;
} EbiMpscQueueStruct;

typedef struct EbiMpscQueueStruct* EbiMpscQueue;

typedef struct EbiSpscQueueNodeData
{
	EbiNode node;
	dsa_pointer dsa_ptr; /* dsa_pointer to the EbiNode (optimization) */
	struct EbiSpscQueueNodeData* next;
} EbiSpscQueueNodeData;

typedef struct EbiSpscQueueNodeData* EbiSpscQueueNode;

typedef struct EbiSpscQueueData
{
	EbiSpscQueueNode front;
	EbiSpscQueueNode rear;
} EbiSpscQueueData;

typedef struct EbiSpscQueueData* EbiSpscQueue;

/* Public functions */
EbiTree EbiConvertToTree(dsa_area* area, dsa_pointer ptr);
EbiNode EbiConvertToNode(dsa_area* area, dsa_pointer ptr);

EbiSubNode EbiConvertToSubNode(dsa_area* area, dsa_pointer ptr);

extern dsa_pointer EbiIncreaseRefCount(Snapshot snapshot);
extern void EbiDecreaseRefCount(dsa_pointer node);

extern dsa_pointer EbiInitTree(dsa_area* area);
extern void EbiDeleteTree(dsa_pointer dsa_ebitree);

extern void EbiInsertNode(dsa_pointer dsa_ebitree);
extern void EbiUnlinkNodes(dsa_pointer dsa_ebitree,
						   dsa_pointer unlink_queue,
						   EbiSpscQueue delete_queue);

extern void PrintAllTree(dsa_pointer dsa_curr, int level, FILE* fp);
#ifdef PRINT_EBI_INFO
extern void PrintTreeToFile(int time);
#endif
extern void EbiDeleteNodes(EbiSpscQueue delete_queue);
extern void EbiDeleteNode(EbiNode node, dsa_pointer dsa_ptr);
extern EbiNode EbiSift(TransactionId xmin, TransactionId xmax);
extern EbiTreeVersionOffset EbiSiftAndBind(TransactionId xmin,
										   TransactionId xmax,
										   Size tuple_size,
										   const void* tuple,
										   LWLock* rwlock);
extern bool EbiIsLeaf(EbiNode node);
extern bool IsEbiExist(Snapshot snapshot);

extern EbiTreeVersionOffset EbiSiftAndBindWithSub(Oid rel_id,
                      					  		  TransactionId xmin,
										  		  TransactionId xmax,
										  		  Size tuple_size,
										  		  const void* tuple,
										  		  LWLock* rwlock);
extern EbiSubNode EbiSiftSubNode(EbiNode node, Oid rel_id);
extern bool EbiSubOverlaps(EbiNode node, Snapshot snapshot);
extern int EbiTreeVersionCompare(const void *a, const void *b);
#ifdef LOCATOR
extern void EbiSiftAndBindPartitionOldFile(Oid db_id,
										   Oid rel_id,
										   TransactionId xmin,
										   TransactionId xmax,
										   LocatorPartLevel part_lvl,
										   LocatorPartNumber part_num,
										   LocatorPartGenNo min_part_gen,
										   LocatorPartGenNo max_part_gen);
#endif /* LOCATOR */

extern int EbiLookupVersion(EbiTreeVersionOffset version_offset,
                    		Oid rel_id,
							bool fast_memory_lookup,
							uint32* tuple_size,
							void** ret_value);

extern bool EbiSegIsAlive(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id);
extern bool EbiRecentNodeIsAlive(dsa_pointer dsa_ebitree);

/* Statistics */
extern void EbiMarkTupleSize(Size tuple_size);

/* Debug */
void EbiPrintTree(dsa_pointer dsa_ebitree);
void EbiPrintTreeRecursive(EbiNode node);

#endif /* EBI_TREE_H */
