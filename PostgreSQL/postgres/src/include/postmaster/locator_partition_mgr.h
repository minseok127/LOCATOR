/*-------------------------------------------------------------------------
 *
 * locator_partition_process.h
 *	  partitioning process of LOCATOR
 *
 * src/include/postmaster/locator_partition_process.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCATOR_PARTITION_PROCESS_H
#define LOCATOR_PARTITION_PROCESS_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"

#define LOCATOR_PARTITION_JOB_MAX (32)
#define LOCATOR_PARTITIONING_LAUNCHER_SLEEP_MILLI_SECOND (100)
#define LOCATOR_PARTITIONING_WORKER_SLEEP_MILLI_SECOND (10)
#define LOCATOR_REPARTITIONING_THRESHOLD (16 * 1024 * 1024) /* 16 MiB */

typedef struct LocatorPartitionJob
{
	Oid dbOid;						/* target db oid */
	Oid relOid;						/* target relation oid */
	bool levelZeroDedicated;		/* is dedicated from level 0 to level 1? */
	bool inited;

	/* state of partition job */
	volatile pid_t partitioningWorkerPid;	/* partitioning worker pid */
	LWLock partitionJobLock;
} LocatorPartitionJob;

typedef struct LocatorPartitioningShmemStruct
{
	/* launcher PID and worker PID. */
	pid_t launcherpid;
	bool isLauncherAlive;			/* To handle conflict with checkpointer */

	volatile bool initializing;
	volatile Oid initDbOid;

	LocatorPartitionJob partitionJobs[LOCATOR_PARTITION_JOB_MAX];
	pg_atomic_uint64 currentWorkerNums;
	pg_atomic_uint64 neededWorkerNums;

	dsa_handle locatorPartitionHandle;		/* dsa handle */
} LocatorPartitioningShmemStruct;

extern LocatorPartitioningShmemStruct* PartitioningShmem;

/* src/backend/locator/paritioning/locator_paritioning.c */
extern dsa_area* locatorPartitionDsaArea;
 
extern int StartLocatorPartitionWorker(void);
extern void LocatorPartitionWorkerMain(int argc, char *argv[]);
extern bool IsLocatorPartitionManagerProcess(void);
extern void LocatorPartitioningDsaInit(void);
extern Size LocatorPartitioningShmemSize(void);
extern void LocatorPartitioningShmemInit(void);

extern int StartLocatorPartitionLauncher(void);
extern void LocatorPartitionLauncherMain(int argc, char *argv[]);
extern bool IsLocatorPartitionLauncherProcess(void);
extern void LocatorPartitionDsaInit(void);

#endif /* LOCATOR_PARTITION_PROCESS_H */
