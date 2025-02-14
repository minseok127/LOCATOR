/*
 * This file represents API between client working with Postgres API and C++ server working with RocksDB API.
 * To avoid collision between C/C++ headers they are sharing just this one header file.
 */
#ifndef __LSM_API_H__
#define __LSM_API_H__

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Maximal size of record which can be transfered through client-server protocol and read batch size as well
 */
#define LSM_MAX_RECORD_SIZE (64*1024)

/*
 * Name of the directory in $PGDATA
 */
#define LSM_FDW_NAME        "lsm"

extern int  LsmQueueSize;
extern bool LsmSync;
extern bool LsmUpsert;

typedef int     LsmRelationId;
typedef int     LsmQueueId;
typedef int64_t LsmCursorId;

typedef enum
{
#ifdef LASER
	LsmOpStart,
	LsmOpCommit,
	LsmOpAbort,
#endif /* LASER */
	LsmOpTerminate,
	LsmOpInsert,
#ifdef LASER
	LsmOpUpdate,
#endif /* LASER */
	LsmOpDelete,
	LsmOpCount,
	LsmOpCloseCursor,
#ifdef LASER
	LsmOpSetFilter,
#endif /* LASER */
	LsmOpFetch,
	LsmOpLookup
} LsmOperation;

#ifdef LSM_TXN
extern void		LsmStart(LsmQueueId qid, LsmRelationId rid);
extern void		LsmCommit(LsmQueueId qid, LsmRelationId rid);
extern void		LsmAbort(LsmQueueId qid, LsmRelationId rid);
#endif /* LSM_TXN */

extern void		LsmError(char const* message);
extern size_t	LsmShmemSize(int maxClients);
extern void		LsmInitialize(void* ctl, int maxClients);
extern void		LsmAttach(void* ctl);
extern bool		LsmInsert(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen, char *val, size_t valLen);
#ifdef LASER
extern bool		LsmUpdate(LsmQueueId qid, LsmRelationId rid, char *key, size_t keyLen, char *val, size_t valLen);
#endif /* LASER */
extern uint64_t	LsmCount(LsmQueueId qid, LsmRelationId rid);
extern void		LsmCloseCursor(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid);
#ifdef LASER
extern void		LsmSetFilter(LsmQueueId qid, size_t natts, bool *read_filter);
#endif /* LASER */
extern bool		LsmReadNext(LsmQueueId qid, LsmRelationId rid, LsmCursorId cid, char *buf, size_t *size);
extern bool		LsmLookup(LsmQueueId qid, LsmRelationId rid,  char *key, size_t keyLen, char *val, size_t *valLen);
extern bool		LsmDelete(LsmQueueId qid, LsmRelationId rid,  char *key, size_t keyLen);
extern void		LsmRunWorkers(int maxClients);
extern void		LsmStopWorkers(void);
extern void		LsmMemoryBarrier(void);

#ifdef __cplusplus
}
#endif

#endif

