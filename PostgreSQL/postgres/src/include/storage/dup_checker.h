#ifndef DUP_CHECKER_H
#define DUP_CHECKER_H

#include "postgres.h"
#include "fmgr.h"
#include "executor/tuptable.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/relcache.h"

typedef struct DupChecker
{
	HTAB *hashtable;
	int16 key_size;

	int max_attnum;
	int attrs_cnt;
	Oid indexOid;
	List *attrs;
	int16 attnums[INDEX_MAX_KEYS];
	Oid typeOids[INDEX_MAX_KEYS];
	Oid opclasses[INDEX_MAX_KEYS];
	int16 att_sizes[INDEX_MAX_KEYS];

	char hashKeyBuffer[INDEX_MAX_KEYS * 8];
} DupChecker;

extern void InitDupChecker(DupChecker *dup_checker, Relation rel, int htab_size);
extern void InsertOriginalToDupChecker(DupChecker *dup_checker, TupleTableSlot *slot);
extern bool IsDuplicate(DupChecker *dup_checker, TupleTableSlot *slot);
extern void DestroyDupChecker(DupChecker *dup_checker);

#endif /* DUP_CHECKER_H */