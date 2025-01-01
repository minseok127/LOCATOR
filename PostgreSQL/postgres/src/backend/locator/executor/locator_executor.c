/*-------------------------------------------------------------------------
 *
 * locator_executor.c
 *
 * LOCATOR executor Implementation
 *
 * IDENTIFICATION
 *    src/backend/locator/executor/locator_executor.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "executor/tuptable.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "locator/locator_external_catalog.h"
#include "storage/buf.h"

#ifdef LOCATOR
/*
 * Make SIRO attribute index list.
 *
 * ROW_ORIENTED layout is not considered in this function.
 */
static List *
locator_get_siro_attidx_list(LocatorExternalCatalog *exCatalog, TupleDesc tupleDesc)
{
	List *siro_attidx_list = NIL;

	for (int level = 0; level <= exCatalog->lastPartitionLevel; level++)
	{
		uint8 layout_id = exCatalog->layoutArray[level];
		ColumnarLayout *layout_info = exCatalog->columnarLayoutArray + layout_id;

#ifdef LOCATOR_DEBUG
		Assert(layout_id >= 0);
#endif /* LOCATOR_DEBUG */

		/* layout id 0 means row-oriented layout */
		if (layout_id == ROW_ORIENTED)
			continue;

		for (int attidx = 0; attidx < tupleDesc->natts; attidx++)
		{
			int groupidx = layout_info->columnGroupArray[attidx];

#ifdef LOCATOR_DEBUG
			Assert(groupidx >= 0);
#endif /* LOCATOR_DEBUG */

			/* Group 0 means SIRO column group */
			if (groupidx == 0)
				siro_attidx_list = lappend_int(siro_attidx_list, attidx);
		}

		/*
		 * We assume that group 0 uses SIRO at all levels and that in columnar
		 * layout levels, group 0's attribute composition does not change.
		 *
		 * Therefore, we don't need to go to the next level. Break the loop.
		 */
		break;
	}

	return siro_attidx_list;
}

/*
 * Init function for LocatorExecutor.
 */
void
locator_init_columnar_slot(TupleTableSlot *slot, Relation rel,
						   List *required_attidx_list)
{
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(rel->rd_id);
	List *siro_attidx_list = NIL;
	bool is_columnar = false;
	LocatorExecutor *executor;
	ListCell *lc, *lc2;

	if (exCatalog == NULL)
	{
		slot->tts_locator_executor = NULL;
		return;
	}
	
	executor = palloc0(sizeof(LocatorExecutor));

#ifdef LOCATOR_DEBUG
	Assert(executor != NULL);
#endif /* LOCATOR_DEBUG */
	
	/* Find the first column oriented partition level */
	for (int level = 0; level <= exCatalog->lastPartitionLevel; level++)
	{
		uint8 layout_id = exCatalog->layoutArray[level];

#ifdef LOCATOR_DEBUG
		Assert(layout_id >= ROW_ORIENTED);
#endif /* LOCATOR_DEBUG */

		/* layout id 0 means row-oriented layout */
		if (layout_id != ROW_ORIENTED && !is_columnar)
		{
			executor->first_columnar_level = level;
			is_columnar = true;
		}

		/*
		 * Placeholders for each level.
		 * It will be initialized at the beginning of the scan.
		 */
		executor->level_desc_list = lappend(executor->level_desc_list, NULL);
	}

	if (!is_columnar)
	{
		if (executor->level_desc_list)
			pfree(executor->level_desc_list);
		pfree(executor);
		slot->tts_locator_executor = NULL;
		return;
	}

	executor->rel = rel;
	executor->required_attidx_bitmap = NULL;
	executor->update_attidx_bitmap = NULL;
	executor->returning_attidx_bitmap = NULL;
	executor->restoring_null_attidx_bitmap = NULL;
	executor->isnull_enforced_except_siro = false;

	/* Remember required indexes */
	foreach(lc, required_attidx_list)
	{
		int attidx = lfirst_int(lc);

		/*
		 * If it is a system attribute, it will request the information
		 * contained in the tuple header.
		 *
		 * For this we need to read a SIRO tuple (non-SIRO tuples do not have a
		 * tuple header).
		 */
		if (attidx < 0)
		{
			siro_attidx_list = locator_get_siro_attidx_list(exCatalog,
															slot->tts_tupleDescriptor);

			foreach(lc2, siro_attidx_list)
			{
				int siro_attidx = lfirst_int(lc2);

				executor->required_attidx_bitmap
					= bms_add_member(executor->required_attidx_bitmap,
									 siro_attidx);
			}

			list_free(siro_attidx_list);

			continue;
		}

		executor->required_attidx_bitmap
			= bms_add_member(executor->required_attidx_bitmap, attidx);
	}

#ifdef LOCATOR_DEBUG
	Assert(executor->required_attidx_bitmap != NULL);
#endif /* LOCATOR_DEBUG */

	slot->tts_locator_executor = executor;
}

/*
 * Initialize columnar slot for update.
 */
void
locator_init_columnar_slot_for_update(TupleTableSlot *slot,
									  Relation rel,
									  List *update_attidx_list,
									  List *returning_attidx_list)
{
	LocatorExecutor *executor;
	ListCell *lc;

	locator_init_columnar_slot(slot, rel,
							   list_concat_copy(update_attidx_list,
												returning_attidx_list));

	executor = slot->tts_locator_executor;
	if (executor == NULL)
		return;

	/* Remember SET attributes */
	foreach(lc, update_attidx_list)
	{
		int attidx = lfirst_int(lc);

#ifdef LOCATOR_DEBUG
		Assert(attidx >= 0);
#endif /* LOCATOR_DEBUG */

		executor->update_attidx_bitmap
			= bms_add_member(executor->update_attidx_bitmap, attidx);
	}

	/* Remember RETURNING attributes */
	foreach(lc, returning_attidx_list)
	{
		int attidx = lfirst_int(lc);

#ifdef LOCATOR_DEBUG
		Assert(attidx >= 0);
#endif /* LOCATOR_DEBUG */

		executor->returning_attidx_bitmap
			= bms_add_member(executor->returning_attidx_bitmap, attidx);
	}
}

/*
 * Initialize columnar slot for tuple check.
 *
 * This function is used for table_index_fetch_tuple_check() and
 * ExecGetTriggerOldSlot(). These functions just require the sysattr information
 * of the tuple.
 *
 * A tuple header exists only in SIRO group. So we require that group.
 */
void
locator_init_columnar_slot_for_tuple_check(TupleTableSlot *slot,
										   Relation rel)
{
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(rel->rd_id);
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;
	List *siro_attidx_list = NIL;

	if (exCatalog == NULL)
	{
		slot->tts_locator_executor = NULL;
		return;
	}

	siro_attidx_list = locator_get_siro_attidx_list(exCatalog, tupleDesc);

	locator_init_columnar_slot(slot, rel, siro_attidx_list);

	list_free(siro_attidx_list);
}

/* 
 * Make a list of attribute indexes required for the scan from the ScanState
 * used in SeqNext and IndexNext.
 *
 * Note that attribute index 0 means attribute number 1.
 */
List *
locator_get_required_attidx_list(ScanState *ss)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;
	int natts = tupleDesc->natts;
	Plan *plan = ss->ps.plan;
	int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS |	
				PVC_RECURSE_PLACEHOLDERS;
	List *vars = list_concat(pull_var_clause((Node *) plan->targetlist, flags),
							 pull_var_clause((Node *) plan->qual, flags));
	Oid relid = RelationGetRelid(ss->ss_currentRelation);
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(relid);
	ListCell *lc;
	List *attidx_list = NIL;

	/* Quick exit */
	if (exCatalog == NULL)
		return NULL;

	foreach(lc, vars)
	{
		Var *var = lfirst(lc);

		/*
		 * Minimum attribute number is 1.
		 * If the number is 0, it means that all attributes are required.
		 */
		if (var->varattno == 0)
		{
			/* previous list is meaningless, free it */
			if (attidx_list != NIL)
			{
				pfree(attidx_list);
				attidx_list = NIL;
			}

			for (int i = 0; i < natts; i++)
			{
				attidx_list = lappend_int(attidx_list, i);
			}

			break;
		}

		attidx_list = lappend_int(attidx_list, var->varattno - 1);
	}

	/*
	 * In the case of foreign key check, constant can be entered in the
	 * target list, such as (SELECT 1 FROM ...).
	 *
	 * In this case, vars becomes NIL. Then we use SIRO column group instead.
	 */
	if (vars == NIL)
		attidx_list = locator_get_siro_attidx_list(exCatalog, tupleDesc);

	return attidx_list;
}

/*
 * Create and initialize a LocatorExecutorColumnGroupDesc
 */
static LocatorExecutorColumnGroupDesc *
locator_init_column_group_desc(Relation rel,
							   ColumnarLayout *layout_info,
							   int groupidx,
							   int natts,
							   Bitmapset *required_attidx_bitmap)
{
	LocatorExecutorColumnGroupDesc *group_desc
		= palloc0(sizeof(LocatorExecutorColumnGroupDesc));
	ColumnGroupMetadata *group_meta
		= layout_info->columnGroupMetadataArray + groupidx;
	int offset = 0, column_oriented_attidx = 0, row_oriented_attidx = 0;

	group_desc->isSiro = (groupidx == 0); /* group index 0 is SIRO */
	group_desc->tuple_count_per_block = group_meta->tuplesPerBlock;
	group_desc->start_block_offset_in_record_zone = group_meta->blockOffset;
	group_desc->tuple_size = group_meta->tupleSize;

	group_desc->prefetch_partition_desc = NULL;
	group_desc->c_buf = InvalidBuffer;
	group_desc->c_blocknum = InvalidBlockNumber;
	group_desc->c_dp = NULL;
	group_desc->c_rec_pos_in_block = 0;
	group_desc->c_buf_is_readbuf = false;

	for (row_oriented_attidx = 0; row_oriented_attidx < natts;
		 row_oriented_attidx++)
	{
		int current_groupidx = layout_info->columnGroupArray[row_oriented_attidx];
		LocatorExecutorAttMap *attmap;
		uint16 maxlen;

		/* This attribute does not belong to the target group, skip it */
		if (current_groupidx != groupidx)
			continue;

		maxlen = rel->rd_locator_attrs_maxlen[row_oriented_attidx];

		/*
		 * If this attribute is required for this scan, we have to remember
		 * mapping information.
		 */
		if (bms_is_member(row_oriented_attidx, required_attidx_bitmap))
		{
			attmap = palloc0(sizeof(LocatorExecutorAttMap));

			attmap->offset = offset;
			attmap->row_oriented_attidx = row_oriented_attidx;
			attmap->column_oriented_attidx = column_oriented_attidx;

			group_desc->required_attmap_list
				= lappend(group_desc->required_attmap_list, attmap);
		}

		/* Move to the next attribute in this group */
		offset += maxlen;
		column_oriented_attidx++;
	}

#ifdef LOCATOR_DEBUG
	Assert(column_oriented_attidx > 0);
#endif /* LOCATOR_DEBUG */

	group_desc->natts = column_oriented_attidx;

	return group_desc;
}

/*
 * Create and initialize a LocatorExecutorLevelDesc.
 */
LocatorExecutorLevelDesc *
locator_init_level_desc(LocatorExecutor *executor, int level)
{
	LocatorExecutorLevelDesc *level_desc;
	Relation rel = executor->rel;
	LocatorExternalCatalog *exCatalog = LocatorGetExternalCatalog(rel->rd_id);
	uint8 layout_id = exCatalog->layoutArray[level];
	ColumnarLayout *layout_info
		= exCatalog->columnarLayoutArray + layout_id;
	TupleDesc tupdesc = RelationGetDescr(rel);
	int required_attidx = -1, natts = tupdesc->natts;
	Bitmapset *bms_column_group_init = NULL;

	if (layout_id == ROW_ORIENTED)
		return NULL;

	level_desc = palloc0(sizeof(LocatorExecutorLevelDesc));

#ifdef LOCATOR_DEBUG
	Assert(level_desc != NULL);

	/* XXX currently columnar layout only uses id 1 */
	Assert(layout_id == MIXED);
#endif /* LOCATOR_DEBUG */

	level_desc->level = level;
	level_desc->siro_group_desc = NULL;
	level_desc->prefetch_inited = false;

	/*
	 * ColumnarLayout manages recordsPerRecordZone as a pointer.
	 *
	 * Get the value.
	 */
	level_desc->record_count_per_record_zone = *(layout_info->recordsPerRecordZone);

	/*
	 * The number of blocks per record zone for the internal level and the last
	 * level may be different.
	 *
	 * This is because the last level does not require any information needed
	 * for partitioning (internal level has part_id in a tuple).
	 */
	if (level != exCatalog->lastPartitionLevel)
	{
		level_desc->block_count_per_record_zone
			= layout_info->blocksPerRecordZone[0];
	}
	else
	{
		level_desc->block_count_per_record_zone
			= layout_info->blocksPerRecordZone[1];
	}

	/* Make LocatorExecutorColumnGroupDesc required by this scan */
	while ((required_attidx = bms_next_member(executor->required_attidx_bitmap,
											  required_attidx)) >= 0)
	{
		int groupidx = layout_info->columnGroupArray[required_attidx];

#ifdef LOCATOR_DEBUG
		Assert(groupidx >= 0);
#endif /* LOCATOR_DEBUG */
		
		/* If this group is not initialized yet, create it */
		if (!bms_is_member(groupidx, bms_column_group_init))
		{
			LocatorExecutorColumnGroupDesc *group_desc
				= locator_init_column_group_desc(rel,
												 layout_info,
												 groupidx,
												 natts,
												 executor->required_attidx_bitmap);

#ifdef LOCATOR_DEBUG
			Assert(group_desc != NULL);
#endif /* LOCATOR_DEBUG */

			level_desc->required_column_group_desc_list
				= lappend(level_desc->required_column_group_desc_list,
						  group_desc);

			/* Group index 0 is SIRO */
			if (groupidx == 0)
				level_desc->siro_group_desc = group_desc;

			bms_column_group_init = bms_add_member(bms_column_group_init, groupidx);
		}
	}

	/* Replace NULL into the level descriptor */
#ifdef LOCATOR_DEBUG
	Assert(list_nth(executor->level_desc_list, level) == NULL);
#endif /* LOCATOR_DEBUG */
	executor->level_desc_list = list_insert_nth(executor->level_desc_list,
												level, level_desc);

	bms_free(bms_column_group_init);

	return level_desc;
}

/*
 * Deform SIRO tuple.
 *
 * This function assumes that the visibility check has already been performed.
 */
static pg_attribute_hot void
locator_deform_siro_tuple(LocatorExecutorColumnGroupDesc *group_desc,
						  TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;
	HeapTuple tuple = bslot->base.copied_tuple;
	bool hasnulls = HeapTupleHasNulls(tuple);
	HeapTupleHeader tup = tuple->t_data;
	Datum *values = slot->tts_values;
	bool *isnull = slot->tts_isnull;
	char *tp = (char *) tup + tup->t_hoff;
	bits8 *bp = tup->t_bits;
	ListCell *lc;

#ifdef LOCATOR_DEBUG
	Assert(tuple != NULL);
#endif /* LOCATOR_DEBUG */

	foreach(lc, group_desc->required_attmap_list)
	{
		LocatorExecutorAttMap *attmap = lfirst(lc);
		int attidx = attmap->row_oriented_attidx;
		Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attidx);

		/* The default is set to NULL, so just skip it */
		if (hasnulls && att_isnull(attidx, bp))
		{
#ifdef LOCATOR_DEBUG
			Assert(isnull[attidx] == true);
#endif /* LOCATOR_DEBUG */
			continue;
		}

		isnull[attidx] = false;
		values[attidx] = fetchatt(thisatt, tp + attmap->offset);
	}
}

/*
 * Check whether the given attribute is null or not.
 *
 * This function targets LOCATOR's columnar page layout.
 *
 * Note that a 0 in the null bitmap indicates a null, while 1 indicates
 * non-null.
 */
static pg_attribute_always_inline int
locator_att_isnull(bits8 *columnar_page_bitmap,
				   uint16 tuple_position_in_block,
				   int natts, int attidx)
{
	int bitnum = tuple_position_in_block * natts + attidx;
	return !(columnar_page_bitmap[bitnum >> 3] & (1 << (bitnum & 0x07)));
}

/*
 * Get the start pointer of the columnar layout bitmap.
 *
 * LOCATOR's columnar page layout uses pd_lower as a start point of the bitmap
 * of the page.
 *
 * Note that while the existing postgres has a null bitmap at the end of the
 * tuple header for each tuple, in columnar layout, the null bitmaps of all
 * tuples are gathered consecutively at the end of the page header.
 */
static pg_attribute_always_inline bits8 *
locator_get_columnar_layout_page_bitmap(Page dp)
{
	PageHeader header = (PageHeader) dp;
	return (bits8 *) (dp + header->pd_lower);
}

/*
 * Get the start pointer of the columnar layout tuple.
 *
 * LOCATOR's columnar page layout uses pd_upper as a start point of the first
 * tuple data in the page.
 */
static pg_attribute_always_inline char *
locator_get_columnar_tuple_pointer(LocatorExecutorColumnGroupDesc *group_desc,
								 Page dp, uint16 tuple_position_in_block)
{
	PageHeader header = (PageHeader) dp;
	return (char *) (dp + header->pd_upper +
							(tuple_position_in_block * group_desc->tuple_size));
}

/*
 * Access to columnar tuple and deform it.
 */
static pg_attribute_hot bool
locator_deform_non_siro_tuple(LocatorExecutorColumnGroupDesc *group_desc,
							  uint16 tuple_position_in_block,
							  TupleTableSlot *slot,
							  Snapshot snapshot)
{
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;
	Page dp = group_desc->c_dp;
	Datum *values = slot->tts_values;
	bool *isnull = slot->tts_isnull;
	bits8 *columnar_page_bitmap;
	char *tuple_pointer;
	TransactionId xmin;
	ListCell *lc;

	columnar_page_bitmap = locator_get_columnar_layout_page_bitmap(dp);
	tuple_pointer = locator_get_columnar_tuple_pointer(group_desc,
													   dp,
													   tuple_position_in_block);

	/*
	 * Check visibility.
	 *
	 * Non-siro tuples are reformatted only after they are committed or aborted.
	 *
	 * However, if non-siro tuples have not been reformatted when the current
	 * transaction starts, the transactions that created these tuples may still be
	 * in-progress.
	 *
	 * In this case, we cannot see these tuples. Filter these out.
	 */
	xmin = *((TransactionId *) tuple_pointer);
	if (XidInMVCCSnapshot(xmin, snapshot))
		return false;

	tuple_pointer += sizeof(TransactionId);

	foreach(lc, group_desc->required_attmap_list)
	{
		LocatorExecutorAttMap *attmap = lfirst(lc);
		int row_oriented_attidx = attmap->row_oriented_attidx;
		int column_oriented_attidx = attmap->column_oriented_attidx;
		char *attribute_pointer = tuple_pointer + attmap->offset;
		Form_pg_attribute thisatt
			= TupleDescAttr(tupleDesc, row_oriented_attidx);

		/* The default is set to NULL, so just skip it */
		if (locator_att_isnull(columnar_page_bitmap, tuple_position_in_block,
							   group_desc->natts, column_oriented_attidx))
		{
#ifdef LOCATOR_DEBUG
			Assert(isnull[row_oriented_attidx] == true);
#endif /* LOCATOR_DEBUG */

			/*
			 * In the process of changing the layout, the data of any attribute
			 * that is NULL is unconditionally initialized to 0.
			 *
			 * If this is not satisfied, it means that the tuple has been
			 * aborted, so deformation ends immediately.
			 */
			if (*attribute_pointer)
				return false;

			continue;
		}

		isnull[row_oriented_attidx] = false;
		values[row_oriented_attidx] = fetchatt(thisatt, attribute_pointer);
	}

	return true;
}

/*
 * Read new page for the given column group.
 */
static void
locator_read_buffer_for_column_group(Relation rel,
									 LocatorExecutorLevelDesc *level_desc,
									 LocatorExecutorColumnGroupDesc *group_desc,
									 LocatorPartLevel level,
									 LocatorPartNumber partnum,
									 LocatorPartGenNo gennum,
									 LocatorSeqNum parttuppos)
{
	BlockNumber target_blocknum
		= locator_calculate_columnar_tuple_blocknum(level_desc,
													group_desc,
													parttuppos);
	Page readbuf_dp;

	if (group_desc->prefetch_partition_desc != NULL)
	{
		PrefetchDesc prefetch_desc
			= (PrefetchDesc) group_desc->prefetch_partition_desc;

		/*
		 * If the buffer is valid, it means that we are using postgres's
		 * traditional buffer pool. Otherwise, the buffer is located LOCATOR's
		 * local read buffer.
		 *
		 * In the case of a local read buffer, we don't need to release the
		 * buffer here because the prefetch descriptor recycles the same buffer
		 * and frees it at the end of scan.
		 */
		if (BufferIsValid(group_desc->c_buf))
		{
			ReleaseBuffer(group_desc->c_buf);
			group_desc->c_buf = InvalidBuffer;
		}

		group_desc->c_buf
			= ReadPartitionBufferPrefetch(rel,
										  target_blocknum,
										  prefetch_desc,
										  NULL,
										  &readbuf_dp);
	}
	else
	{
		group_desc->c_buf = LocatorReleaseAndReadBuffer(group_desc->c_buf,
														rel,
														level,
														partnum,
														gennum,
														target_blocknum);
	}

	if (BufferIsValid(group_desc->c_buf))
	{
		group_desc->c_dp = BufferGetPage(group_desc->c_buf);
		group_desc->c_buf_is_readbuf = false;
	}
	else
	{
		group_desc->c_dp = readbuf_dp;
		group_desc->c_buf_is_readbuf = true;
	}

	group_desc->c_blocknum = target_blocknum;
}

/*
 * Make the given slot to NULL.
 */
static inline void
locator_init_slot_as_null(TupleTableSlot *slot)
{
	MemSet(slot->tts_values, 0,
		   slot->tts_tupleDescriptor->natts * sizeof(Datum));
	memset(slot->tts_isnull, true,
		   slot->tts_tupleDescriptor->natts * sizeof(bool));
	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
}

/*
 * Make the given slot valid.
 *
 * We set tts_nvalid to the maximum value to avoid additional deforming
 * such as slot_getsomeattrs().
 */
static inline void
locator_make_slot_valid(TupleTableSlot *slot, Relation rel)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;

	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_tableOid = RelationGetRelid(rel);

	if (bslot->base.copied_tuple != NULL)
	{
		bslot->base.tuple = bslot->base.copied_tuple;
		slot->tts_tid = bslot->base.copied_tuple->t_self;
	}
	bslot->base.off = 0;
}

/*
 * Get required column group tuples, and deform them.
 *
 * This function is used for the locator_index_fetch_tuple() and
 * locator_search_row_version().
 *
 * In this function, pages are acquired from the buffer pool. However, there is
 * no release for these buffers. This is up to the caller.
 */
bool
locator_search_and_deform_version(Relation rel,
								  LocatorExecutorLevelDesc *level_desc,
								  TupleTableSlot *slot,
								  Snapshot snapshot,
								  LocatorTuplePosition tuplepos,
								  IndexFetchHeapData *hscan,
								  bool is_modification)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	DualRefDescData drefDesc;
	bool found = false;
	ListCell *lc;

	/* Initialize the slot as a NULL */
	locator_init_slot_as_null(slot);

	/* Access to the each columnar tuple and deform them */
	foreach(lc, level_desc->required_column_group_desc_list)
	{
		LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc);
		uint16 tuple_position_in_block
			= locator_calculate_tuple_position_in_block(
											level_desc,
											group_desc,
											tuplepos->partitionTuplePosition);

		/* Read buffer */
		locator_read_buffer_for_column_group(rel,
											 level_desc,
											 group_desc,
											 tuplepos->partitionLevel,
											 tuplepos->partitionNumber,
											 tuplepos->partitionGenerationNumber,
											 tuplepos->partitionTuplePosition);

#ifdef LOCATOR_DEBUG
		Assert(BufferIsValid(group_desc->c_buf));
#endif /* LOCATOR_DEBUG */

#ifdef USING_LOCK
		drefDesc.dual_ref = NULL;
		LockBuffer(group_desc->c_buf, BUFFER_LOCK_SHARE);
#else /* !USING_LOCK */
		drefDesc.dual_ref
			= (pg_atomic_uint64 *) GetBufferDualRef(group_desc->c_buf);
		SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK */

		if (group_desc->isSiro)
		{
			found = locator_search_version(NULL,
										   rel,
										   group_desc->c_buf,
										   snapshot,
										   &bslot->base.tupdata,
										   &bslot->base.copied_tuple,
										   &drefDesc,
										   NULL,
										   true,
										   hscan,
										   group_desc->c_blocknum,
										   tuple_position_in_block);
			if (found)
				locator_deform_siro_tuple(group_desc, slot);
		}
		else
		{
			/*
			 * Non-SIRO tuples do not support updates.
			 * In other words, there is no need to do version searching.
			 *
			 * Just access the tuple in the data page and deform it immediately.
			 *
			 * Note that the reason the @found variable is used is to indicate
			 * an aborted tuple.
			 */
			found = locator_deform_non_siro_tuple(group_desc,
												  tuple_position_in_block,
												  slot,
												  snapshot);
		}

#ifdef USING_LOCK
		LockBuffer(group_desc->c_buf, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
		DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */

		/*
		 * If there is no valid version, just end this searching.
		 */
		if (!found)
			return false;

		/*
		 * Buffer releasing is up to the caller.
		 */
	}

	/*  Do what ExecStoreBufferHeapTuple() does */
	locator_make_slot_valid(slot, rel);

	return true;
}

/*
 * A columnar layout gathers only the attributes used in SET and organizes
 * them into a SIRO group.
 *
 * Therefore, when constructing a tuple to be written to the heap page,
 * only these attributes (SIRO group) should be materialized (for UPDATE).
 *
 * Make other attributes to NULL so that postgres can't see the non-SIRO
 * attributes in the materialize function.
 */
void
locator_prepare_materialize_for_update(TupleTableSlot *slot)
{
	LocatorExecutor *executor = slot->tts_locator_executor;
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;

	/* Quick exit */
	if (executor->isnull_enforced_except_siro)
		return;

	/* Make NULL except SET attributes (SIRO group)  */
	for (int attidx = 0; attidx < tupleDesc->natts; attidx++)
	{
		if (slot->tts_isnull[attidx] == false &&
			!bms_is_member(attidx, executor->update_attidx_bitmap))
		{
			executor->restoring_null_attidx_bitmap
					= bms_add_member(executor->restoring_null_attidx_bitmap,
									 attidx);

			slot->tts_isnull[attidx] = true;
		}
	}

	executor->isnull_enforced_except_siro = true;
}

/* 
 * When UPDATE, the attributes requried for RETURNING may be set as NULL by
 * locator_prepare_materialize_for_update().
 *
 * Restore them.
 */
void
locator_restore_isnull(TupleTableSlot *slot)
{
	LocatorExecutor *executor = slot->tts_locator_executor;
	TupleDesc tupleDesc = slot->tts_tupleDescriptor;
	int restore_attidx = -1;

	while ((restore_attidx
			= bms_next_member(executor->restoring_null_attidx_bitmap,
							  restore_attidx)) >= 0)
	{
		slot->tts_isnull[restore_attidx] = false;
	}

	bms_free(executor->restoring_null_attidx_bitmap);
	executor->restoring_null_attidx_bitmap = NULL;
	executor->isnull_enforced_except_siro = false;

	/*
	 * Materialize function sets slot->tts_nvalid to 0.
	 *
	 * Restore it to avoid additional deforming such as slot_getsomeattrs().
	 */
	slot->tts_nvalid = tupleDesc->natts;
}

/*
 * Release all buffers.
 */
void
locator_release_all_buffer(LocatorExecutor *executor)
{
	ListCell *lc, *lc2;

	if (executor == NULL)
		return;

	foreach(lc, executor->level_desc_list)
	{
		LocatorExecutorLevelDesc *level_desc = lfirst(lc);

		if (level_desc == NULL)
			continue;

		foreach(lc2, level_desc->required_column_group_desc_list)
		{
			LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc2);

			if (BufferIsValid(group_desc->c_buf))
			{
				ReleaseBuffer(group_desc->c_buf);
				group_desc->c_buf = InvalidBuffer;
			}
		}
	}
}

/*
 * Whether we need a new page or not.
 */
static inline bool
locator_next_page_required(LocatorScanDesc lscan,
						   LocatorExecutorLevelDesc *level_desc,
						   LocatorExecutorColumnGroupDesc *group_desc)
{
	BlockNumber required_blocknum
		= locator_calculate_columnar_tuple_blocknum(level_desc,
													group_desc,
													lscan->c_rec_position);

	return (group_desc->c_blocknum != required_blocknum);
}

/*
 * Access to the SIRO tuple.
 *
 * If we have to get a new page, call locatorgetapge().
 *
 * It will copy SIRO tuples into the rs_vistuples_copied[] when we read them
 * from the postgres's traditional buffer pool.
 *
 * If we read them from the LOCATOR's local read buffer, it does not copy the
 * tuples. Just access to the page and get the pointer.
 */
static bool
locator_get_siro_tuple(TableScanDesc sscan,
					   LocatorExecutorLevelDesc *level_desc,
					   LocatorExecutorColumnGroupDesc *group_desc,
					   HeapTuple *copied_tuple)
{
	HeapScanDesc hscan = (HeapScanDesc) sscan;
	int vistuple_pos_in_block;
	HeapTuple tup;

#ifdef LOCATOR_DEBUG
	Assert(sscan->counter_in_ebi == 0);
#endif /* LOCATOR_DEBUG */

	/*
	 * SIRO pages are allowed to be updated. During this process, the
	 * version can be moved out of the heap page and into the EBI page.
	 *
	 * So, when we first read a SIRO page, we copy all valid versions to
	 * memory (rs_vistuples_copied[]).
	 *
	 * But if the page has been uploaded to LOCATOR's local read buffer, we
	 * don't have to worry about updates. In this case, rs_vistuples[] contains
	 * the OffsetNumber within the page. This allows us to access tuples withint
	 * the page.
	 */
	if (group_desc->c_rec_pos_in_block == 0)
	{
		locatorgetpage(sscan, true, level_desc, group_desc);
		hscan->rs_cindex = 0;
	}

	/*
	 * All visible tuple are consumed.
	 */
	if (hscan->rs_ntuples == hscan->rs_cindex)
		return false;

	/*
	 * In locatorgetpage(), if a specific tuple does not have a version visible
	 * to the current transaction, it is not stored in rs_vistuples_copied[].
	 *
	 * For example, assume that the tuple created by the aborted transaction is
	 * the first tuple on the page.
	 *
	 * In this case, rs_vistuples_copied[0] will not represent that tuple, but
	 * will be the visible version of the next tuple (of course, if the next
	 * tuple has a visible version).
	 *
	 * Therefore, we need to know where the tuples in rs_vistuples_copied[] were
	 * located within the page. This is expressed as rs_vistuples_idx[].
	 */
	vistuple_pos_in_block = hscan->rs_vistuples_pos_in_block[hscan->rs_cindex];

	/*
	 * Current record position is not visible to us.
	 */
	if (vistuple_pos_in_block > group_desc->c_rec_pos_in_block)
		return false;

	tup = hscan->rs_vistuples_copied[hscan->rs_cindex];

	/*
	 * If this is NULL, it means that the current page is a local read buffer.
	 * At this time, other transactions cannot update our local buffer. So the
	 * page is accessed directly without copying the tuples.
	 */
	if (tup == NULL)
	{
		OffsetNumber lineoff = hscan->rs_vistuples[hscan->rs_cindex];
		ItemId lpp = PageGetItemId(group_desc->c_dp, lineoff);
		HeapTuple tuple = &(hscan->rs_ctup);

#ifdef LOCATOR_DEBUG
		Assert(ItemIdIsNormal(lpp));
#endif /* LOCATOR_DEBUG */

		tuple->t_data = (HeapTupleHeader) PageGetItem(group_desc->c_dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);

		*copied_tuple = tuple;
	}
	else
		*copied_tuple = tup;

	/* Move to the next visible tuple */
	hscan->rs_cindex++;

	return true;
}

/*
 * Function for columnar layout corresponding to locatorgettup_pagemode().
 *
 * The caller must have specified the target partition.
 */
bool
locator_gettup_and_deform(TableScanDesc sscan,
						  LocatorExecutorLevelDesc *level_desc,
						  TupleTableSlot *slot)
{
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	LocatorScanDesc lscan = (LocatorScanDesc) sscan;
	HeapScanDesc hscan = (HeapScanDesc) sscan;
	LocatorPartInfo part_info = lscan->c_part_info;
	Relation rel = hscan->rs_base.rs_rd;
	DualRefDescData drefDesc;
	bool found = false, aborted = false;
	ListCell *lc;

#ifdef USING_LOCK
	drefDesc.dual_ref = NULL;
#endif /* USING_LOCK */

	/* Initialize the slot as a NULL */
	locator_init_slot_as_null(slot);

	/* 
	 * Loop until a valid record is found.
	 * 
	 * If there are no tuples remaining inside the partition, false is returned.
	 * Then the caller will move the partition.
	 */
	for (;;)
	{
		/* Access to the each columnar tuple and deform them */
		foreach(lc, level_desc->required_column_group_desc_list)
		{
			LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc);

			if (locator_next_page_required(lscan, level_desc, group_desc))
			{
				CHECK_FOR_INTERRUPTS();

				locator_read_buffer_for_column_group(rel,
													 level_desc,
													 group_desc,
													 lscan->c_level,
													 part_info->partNum,
													 part_info->partGen,
													 lscan->c_rec_position);
				group_desc->c_rec_pos_in_block = 0;
			}

			/*
			 * If we encounter an aborted record in the previous column group,
			 * we don't need to deform other column groups.
			 *
			 * Just move to the next record.
			 */
			if (aborted)
			{
				group_desc->c_rec_pos_in_block++;
				continue;
			}

			/*
			 * XXX 
			 *
			 * For a SIRO tuples, locatorgetpage() is called and locking is
			 * performed within that function.
			 *
			 * Therefore, we only need to perform locking in non-SIRO case.
			 */

			if (group_desc->isSiro)
			{
				found = locator_get_siro_tuple(sscan,
											   level_desc,
											   group_desc,
											   &bslot->base.copied_tuple);

				if (found)
					locator_deform_siro_tuple(group_desc, slot);
			}
			else
			{
				if (group_desc->c_buf_is_readbuf)
				{
					/*
					 * Read buffer is a local buffer.
					 * So we don't need to perform locking to the page.
					 */
#ifdef LOCATOR_DEBUG
					Assert(BufferIsInvalid(group_desc->c_buf));
#endif /* LOCATOR_DEBUG */
				}
				else
				{
#ifdef LOCATOR_DEBUG
					Assert(BufferIsValid(group_desc->c_buf));
#endif /* LOCATOR_DEBUG */
#ifdef USING_LOCK
					LockBuffer(group_desc->c_buf, BUFFER_LOCK_SHARE);
#else /* !USING_LOCK */
					drefDesc.dual_ref
						= (pg_atomic_uint64 *) GetBufferDualRef(group_desc->c_buf);
					SetPageRefUnit(&drefDesc);
#endif /* USING_LOCK */
				}

				/*
				 * Non-SIRO tuples do not support updates.
				 * In other words, we don't need to do version searching.
				 *
				 * Just access the tuple in the heap page and deform it immediately.
				 *
				 * Note that the reason the @found variable is used is to indicate
				 * an aborted tuple.
				 */
				found = locator_deform_non_siro_tuple(group_desc,
													  group_desc->c_rec_pos_in_block,
													  slot,
													  hscan->rs_base.rs_snapshot);

				if (group_desc->c_buf_is_readbuf)
				{
					/*
					 * Read buffer is a local buffer.
					 * So we don't need to perform locking to the page.
					 */
#ifdef LOCATOR_DEBUG
					Assert(BufferIsInvalid(group_desc->c_buf));
#endif /* LOCATOR_DEBUG */
				}
				else
				{
#ifdef LOCATOR_DEBUG
					Assert(BufferIsValid(group_desc->c_buf));
#endif /* LOCATOR_DEBUG */
#ifdef USING_LOCK
					LockBuffer(group_desc->c_buf, BUFFER_LOCK_UNLOCK);
#else /* !USING_LOCK */
					DRefDecrRefCnt(drefDesc.dual_ref, drefDesc.page_ref_unit);
#endif /* USING_LOCK */
				}
			}

			group_desc->c_rec_pos_in_block++;

			/*
			 * Set the flag to avoid unnecessary deforming in the next group.
			 */
			if (!found)
				aborted = true;

			/*
		 	 * Buffer releasing is up to the caller.
		 	 */
		}

		/* Move to the next record */
		lscan->c_rec_position++;
		aborted = false;

		/*
		 * If we found valid tuple or we reached the end of the current
		 * partition, break the loop and go back to the caller.
		 *
		 * Otherwise, move to the next tuple.
	 	 */
		if (found || (lscan->c_rec_position == lscan->c_part_info->recCnt))
			break;
	}

	/* Do what ExecStoreBufferHeapTuple() does */
	if (found)
		locator_make_slot_valid(slot, rel);

	return found;
}

/*
 * Clear locator's resources.
 *
 * Memory deallocation will be performed when the memory context is freed,
 * so free only other system resources.
 */
void
locator_clear_executor(TupleTableSlot *slot)
{
	LocatorExecutor *executor = slot->tts_locator_executor;
	ListCell *lc, *lc2;

	if (executor == NULL)
		return;

	locator_release_all_buffer(executor);

	/* Release prefetched resources */
	foreach(lc, executor->level_desc_list)
	{
		LocatorExecutorLevelDesc *level_desc = lfirst(lc);

		if (level_desc == NULL)
			continue;

		foreach(lc2, level_desc->required_column_group_desc_list)
		{
			LocatorExecutorColumnGroupDesc *group_desc = lfirst(lc2);
			PrefetchDesc prefetch_desc
				= (PrefetchDesc) group_desc->prefetch_partition_desc;

			if (prefetch_desc != NULL)
			{
				/* 
				 * TODO LOCATOR's columnar heapscan does not support EBI scan yet
				 */
				ReleasePrefetchDesc(prefetch_desc, true);
				group_desc->prefetch_partition_desc = NULL;
			}
		}

		level_desc->prefetch_inited = false;
	}
}
#endif /* LOCATOR */
