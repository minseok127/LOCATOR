/*-------------------------------------------------------------------------
 *
 * hap_partition.h
 *	  POSTGRES hidden attribute partitioning (HAP).
 *
 *
 * src/include/locator/hap/hap_partition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HAP_PARTITION_H
#define HAP_PARTITION_H

#include "access/amapi.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

#define HAP_MAX_PARTKEY_COUNT	(16)
#define HAP_PARTMAP_DESC_SHIFT	(16)

typedef struct {
	Datum partkey[HAP_MAX_PARTKEY_COUNT];
	Oid relid;
} HapPartMapCacheTag;

typedef struct {
	HapPartMapCacheTag tag;
	uint32_t partid;
} HapPartMapCacheLookupEntry;

#define HAP_PARTMAP_ENTRY_NUM	(1024*128)
#define NUM_HAP_PARTMAP_CACHE_PARTITIONS (256)

/*
 * HapPartKey
 *
 */
typedef struct HapPartKey {
	List *propagate_oid_list;
	Oid dimension_relid;
	AttrNumber dimension_attrnum;
	int16 dimension_descid;
	int16 target_descid;
	int set_idx;
	int key_idx;
} HapPartKey;

/*
 * HapPartKeySet
 *
 */
typedef struct HapPartKeySet {
	List *partkey_list;
	List *value_set_list;
	Oid scan_dimension_relid;
	int value_set_count;
	int value_set_idx;
} HapPartKeySet;

/*
 * HapPartCreateStmt
 *
 */
typedef struct HapPartCreateStmt {
	List *partkey_list;
	List *partkey_set_list;
	Oid target_relid;
	uint32 part_count;
	uint64 total_value_set_count;
	uint64 value_set_count_per_part;
	Oid partmap_id;
	Oid gin_idx_id;
	Oid hash_idx_id;
} HapPartCreateStmt;

extern void HapBuildPartCreateStmt(HapPartCreateStmt *stmt, Oid relid,
								   DefElem *hidden_partition_key,
								   DefElem *hidden_partition_num);

extern void HapParsePartKeyStr(HapPartCreateStmt *stmt, char *keystr);

extern void HapFindPartKeyValues(HapPartCreateStmt *stmt);

extern void HapGetNextPartKeyValues(HapPartCreateStmt *stmt, Datum *arr);

extern void HapInsertPartKeyToPartMap(HapPartCreateStmt *stmt, Oid partmapid,
									  Datum *keys, int32 partid);

extern uint32_t HapGetPartIdHash(Relation rel, Datum *keys, int len);

extern List *HapGetPartIdListGin(Oid relid, List *propagated_exprs);

extern Size HapSharedPartMapCacheSize(void);

extern void HapSharedPartMapCacheInit(void);

#endif	/* HAP_PARTITION_H */
