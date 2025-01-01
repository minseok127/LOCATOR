#ifndef LOCATOR_EXECUTOR_H
#define LOCATOR_EXECUTOR_H

#include "c.h"

#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "locator/locator.h"
#include "utils/relcache.h"

/*
 * Data structure for mapping the attributes of a column oriented tuple and the
 * attributes of a row oriented tuple.
 */
typedef struct LocatorExecutorAttMap
{
	/* offset in column oriented tuple */
	int offset;

	/* attribute index in row oriented tuple */
	uint16 row_oriented_attidx;

	/* attribute index in column oriented tuple */
	uint16 column_oriented_attidx;
} LocatorExecutorAttMap;

/*
 * Descriptor about the column group.
 *
 * This data structure is an element of
 * LocatorExecutorLevelDesc->required_column_group_desc_list.
 *
 * locator_init_column_group_desc() creates and initializes this data.
 */
typedef struct LocatorExecutorColumnGroupDesc
{
	/*
	 * The list of LocatorExecutorAttMap used by this scan.
	 */
	List *required_attmap_list;

	/* Corresponds to ColumnGroupMetatdata->tuplesPerBlock */
	uint16 tuple_count_per_block;

	/* Corresponds to ColumnGroupMetatdata->blockOffset */
	uint32 start_block_offset_in_record_zone;

	/* Corresponds to ColumnGroupMetadata->tupleSize */
	uint16 tuple_size;

	/* The number of attributes in this group */
	uint16 natts;

	/* Current buffer for this group */
	Buffer c_buf;

	/* Current block number */
	BlockNumber c_blocknum;

	/* Current data page corresponding to c_buf */
	Page c_dp;

	/* Prefetch descriptor for the asynchronous I/O */
	PrefetchPartitionDesc prefetch_partition_desc;

	/* Current tuple position in block */
	uint16 c_rec_pos_in_block;

	/* Is the current page from local read buffer? */
	bool c_buf_is_readbuf;

	/* Whether it uses SIRO or not */
	bool isSiro;
} LocatorExecutorColumnGroupDesc;

/*
 * Descriptor about the columnar layout used by each partition level.
 *
 * This data structure is an element of LocatorExecutor->level_desc_list.
 *
 * locator_init_level_desc() creates and initializes this data at the beginning
 * of the scan for the corresponding level.
 */
typedef struct LocatorExecutorLevelDesc
{
	/*
	 * The list of LocatorExecutorColumnGroupDesc used by this scan.
	 */
	List *required_column_group_desc_list;

	 /*
	  * SIRO group descriptor. It is used for update.
	  */
	LocatorExecutorColumnGroupDesc *siro_group_desc;

	/* Corresponds to ColumnarLayout->recordPerRecordZone */
	uint32 record_count_per_record_zone;

	/*
	 * Corresponds to ColumnarLayout->blocksPerRecordZone.
	 * Note that internal level and last level has different value.
	 */
	uint32 block_count_per_record_zone;

	/* Locator partition level */
	int level;

	/* Is prefetch descriptor initialized? */
	bool prefetch_inited;
} LocatorExecutorLevelDesc;

/*
 * LOCATOR's column group scan is used in locator_getnextslot(),
 * locatoram_index_fetch_tuple(), and locatoram_search_row_version().
 *
 * The information required for column group scan must be passed inside these
 * functions, and the common argument taken by all functions is TupleTableSlot.
 *
 * So, we create a variable called tts_locator_executor in TupleTableSlot and
 * point it to this data structure.
 */
typedef struct LocatorExecutor
{
	/* Target Relatoin for the columnar scan */
	Relation rel;

	/* Bitmap that stores the indexes of attributes required for scanning */
	Bitmapset *required_attidx_bitmap;

	/* SET attributes */
	Bitmapset *update_attidx_bitmap;

	/* RETURNING attributes */
	Bitmapset *returning_attidx_bitmap;

	/* Bitmap to restore NULL state for RETURNING */
	Bitmapset *restoring_null_attidx_bitmap;

	/* 
	 * The list of LocatorExecutorLevelDesc corresponding to each level.
	 * NULL is placed at levels that do not use column layout.
	 */
	List *level_desc_list;

	/* First partition level that uses columnar layout */
	int first_columnar_level;

	/* Is null set to the attributes except SIRO? */
	bool isnull_enforced_except_siro;
} LocatorExecutor;

extern LocatorExecutorLevelDesc *locator_init_level_desc(
										LocatorExecutor *executor, int level);

extern void locator_release_all_buffer(LocatorExecutor *executor);

/*
 * Does the given level require a columnar scan?
 */
static inline bool
locator_is_columnar_layout(LocatorExecutor *executor, int level)
{
	return (executor != NULL && executor->first_columnar_level <= level);
}

/*
 * Get columnar layout information for the given level.
 */
static inline LocatorExecutorLevelDesc *
locator_get_level_columnar_desc(LocatorExecutor *executor,
								int level)
{
	LocatorExecutorLevelDesc *level_desc = list_nth(executor->level_desc_list,
													level);

	if (level_desc == NULL)
		level_desc = locator_init_level_desc(executor, level);

	return level_desc;
}

/*
 * Calculate record zone id for the given tuple position.
 */
static inline uint32
locator_calculate_record_zone_id(LocatorExecutorLevelDesc *level_desc,
								 LocatorSeqNum position)
{
	return (position / level_desc->record_count_per_record_zone);
}

/*
 * Calculate tuple position in the record zone.
 */
static inline uint32
locator_calculate_tuple_position_in_record_zone(
	LocatorExecutorLevelDesc *level_desc, LocatorSeqNum position)
{
	return (position % level_desc->record_count_per_record_zone);
}

/*
 * Calculate start block number for the given record zone.
 */
static inline uint32
locator_calculate_record_zone_start_blocknum(
	LocatorExecutorLevelDesc* level_desc, uint32 record_zone_id)
{
	return (record_zone_id * level_desc->block_count_per_record_zone);
}

/*
 * Calculate target block for the given tuple position.
 */
static inline BlockNumber
locator_calculate_columnar_tuple_blocknum(LocatorExecutorLevelDesc *level_desc,
									LocatorExecutorColumnGroupDesc* group_desc,
									LocatorSeqNum position)
{
	uint32 record_zone_id
		= locator_calculate_record_zone_id(level_desc, position);
	uint32 pos_in_record_zone
		= locator_calculate_tuple_position_in_record_zone(level_desc, position);
	
	/*
	 * 	| start of record zone (rec_zone_id * block count per record zone)
	 * 	|
	 *	-------------------------------------
	 *	|			record zone 			|
	 *	-------------------------------------
	 *		|
	 *		| start of target column group (group_desc->start_block_off)
	 *		|
	 *		---------------
	 *		| column zone |
	 *		---------------
	 *			|
	 *			| target block number (pos_in_rec_zone / group->tup_cnt_per_blk)
	 *			|
	 *			---------
	 *			| block |
	 *			---------
	 *
	 * Note that the nth tuple in the record zone also means the nth tuple in
	 * the column zone.
	 */
	return (record_zone_id * level_desc->block_count_per_record_zone)
			+ group_desc->start_block_offset_in_record_zone
			+ (pos_in_record_zone / group_desc->tuple_count_per_block);
}

/*
 * Calculate tuple position in a block.
 */
static inline uint16
locator_calculate_tuple_position_in_block(
	LocatorExecutorLevelDesc *level_desc, 
	LocatorExecutorColumnGroupDesc *group_desc,
	LocatorSeqNum part_tup_pos)
{
	/* position in record zone also means position in column zone */
	uint32 pos_in_column_zone
		= locator_calculate_tuple_position_in_record_zone(level_desc,
														  part_tup_pos);

	return (pos_in_column_zone % group_desc->tuple_count_per_block);
}

#endif /* LOCATOR_EXECUTOR_H */
