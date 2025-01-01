#ifndef __LSM_DB_H__
#define __LSM_DB_H__

#include "lsm_api.h"
#include "lsm_posix.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#ifdef LSM_TXN
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#endif /* LSM_TXN */
#ifdef LASER
#include <inttypes.h>
#include <vector>
#endif /* LASER */

using namespace rocksdb;

#ifdef LSM_TXN
struct LsmTransaction
{
	Transaction *txn;
	const Snapshot *snapshot;
};
#endif /* LSM_TXN */

/*
 * Wrapper for RocksSB
 */
struct LsmConnection
{
#ifdef LSM_TXN
	TransactionDB* txn_db;
#else /* !LSM_TXN */
	DB* db;
#endif /* LSM_TXN */

#ifdef LSM_TXN
	LsmTransaction startTx();
	void commitTx(LsmTransaction *txn);
	void abortTx(LsmTransaction *txn);
#endif /* LSM_TXN */

	void      open(char const* path);
	uint64_t  count();
	void      close();

#ifdef LASER
#ifdef LSM_TXN
	Iterator* getIterator(std::vector<uint32_t> *read_filter,
						  LsmTransaction *txn);
#else /* !LSM_TXN */
	Iterator* getIterator(std::vector<uint32_t> *read_filter);
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
	Iterator* getIterator(LsmTransaction *txn);
#else /* !LSM_TXN */
	Iterator* getIterator();
#endif /* LSM_TXN */
#endif /* LASER */

	void      releaseIterator(Iterator* iter);

#ifdef LASER
#ifdef LSM_TXN
	size_t    lookup(char const* key, size_t keySize, char* buf,
					 std::vector<uint32_t> *read_filter,
					 LsmTransaction *txn);
#else /* !LSM_TXN */
	size_t    lookup(char const* key, size_t keySize, char* buf,
					 std::vector<uint32_t> *read_filter);
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
	size_t    lookup(char const* key, size_t keySize, char* buf,
					 LsmTransaction *txn);
#else /* !LSM_TXN */
	size_t    lookup(char const* key, size_t keySize, char* buf);
#endif /* LSM_TXN */
#endif /* LASER */

	size_t    next(Iterator* iter, char* buf);

#ifdef LASER
#ifdef LSM_TXN
	bool      insert(char* key, size_t keyLen, char* val, size_t valLen,
					 LsmTransaction *txn);
	bool      update(char* key, size_t keyLen, char* val, size_t valLen,
					 LsmTransaction *txn);
	bool      remove(char* key, size_t keyLen, LsmTransaction *txn);
#else /* !LSM_TXN */
	bool      insert(char* key, size_t keyLen, char* val, size_t valLen);
	bool      update(char* key, size_t keyLen, char* val, size_t valLen);
	bool      remove(char* key, size_t keyLen);
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
	bool      insert(char* key, size_t keyLen, char* val, size_t valLen,
					 LsmTransaction *txn);
	bool      remove(char* key, size_t keyLen, LsmTransaction *txn);
#else /* !LSM_TXN */
	bool      insert(char* key, size_t keyLen, char* val, size_t valLen);
	bool      remove(char* key, size_t keyLen);
#endif /* LSM_TXN */
#endif /* LASER */

#ifdef LASER
	uint32_t  column_num;
#endif /* LASER */

#ifdef LSM_TXN
	LsmConnection() : txn_db(NULL) {}
#else /* !LSM_TXN */
	LsmConnection() : db(NULL) {}
#endif /* LSM_TXN */

	~LsmConnection() { close(); }
};

/*
 * Client-server protocol message header, followed by optional key and value bodies
 */
struct LsmMessageHeader
{
	LsmOperation  op;
	uint32_t      keySize;
	uint32_t      valueSize;
	LsmRelationId rid;
	LsmCursorId   cid;
};

/*
 * Protocol message
 */
struct LsmMessage
{
	LsmMessageHeader hdr;
	char*            key;
	char*            value;
};

/*
 * Queue for tranferring data between backend and LSM worker thread.
 */
struct LsmQueue
{
	volatile int   getPos;   // get position in ring buffer (updated only by consumer)
	volatile int   putPos;   // put position in ring buffer (updated only by producer)
	volatile int   respSize; // response size
	volatile int   writerBlocked; // producer is blocked because queue is full
	volatile int   terminate;// worker receives termination request
	sem_t          empty;    // semaphore to wait until queue is not empty
	sem_t          full;     // semaphore to wait until queue is not full
	sem_t          ready;    // semaphore to wait response from server
	char           resp[LSM_MAX_RECORD_SIZE]; // response data
	char           req[1];   // ring buffer (LsmQueueSize long)

	void put(LsmMessage const& msg);
	void get(char* buf, LsmMessage& msg);
	void next(LsmMessage const& msg);

	LsmQueue() : getPos(0), putPos(0), respSize(0), writerBlocked(false) {}
};

struct LsmCursor
{
	LsmConnection* con;
	Iterator* iter;

	LsmCursor() : con(NULL), iter(NULL) {}
};

struct LsmServer;

struct LsmWorker
{
	std::map<LsmCursorId, LsmCursor> cursors;
	LsmServer* server;
	LsmQueue*  queue;
	pthread_t  thread;

#ifdef LSM_TXN
	LsmWorker(LsmServer* s, LsmQueue* q) : server(s), queue(q) { txn.txn = NULL; }
#else /* !LSM_TXN */
	LsmWorker(LsmServer* s, LsmQueue* q) : server(s), queue(q) {}
#endif /* LSM_TXN */

	void start();
	void stop();
	void run();
	void wait();

private:
	LsmConnection& open(LsmMessage const& msg);

#ifdef LSM_TXN
	void startTx(LsmMessage const& msg);
	void commitTx(LsmMessage const& msg);
	void abortTx(LsmMessage const& msg);
#endif /* LSM_TXN */

	void insert(LsmMessage const& msg);
#ifdef LASER
	void update(LsmMessage const& msg);
#endif /* LASER */
	void remove(LsmMessage const& msg);
	void closeCursor(LsmMessage const& msg);
#ifdef LASER
	void setFilter(LsmMessage const& msg);
#endif /* LASER */
	void fetch(LsmMessage const& msg);
	void count(LsmMessage const& msg);
	void lookup(LsmMessage const& msg);

	static void* main(void* arg);

#ifdef LASER
	std::vector<uint32_t> read_filter;
#endif /* LASER */
#ifdef LSM_TXN
	LsmTransaction txn;
#endif /* LSM_TXN */
};

class Mutex
{
	pthread_mutex_t mutex;
public:
	Mutex()
	{
		PthreadMutexInit(&mutex);
	}

	~Mutex()
	{
		PthreadMutexDestroy(&mutex);
	}

	void lock()
	{
		PthreadMutexLock(&mutex);
	}

	void unlock()
	{
		PthreadMutexUnlock(&mutex);
	}
};

class CriticalSection
{
	Mutex& mutex;
public:
	CriticalSection(Mutex& m) : mutex(m)
	{
		mutex.lock();
	}
	~CriticalSection()
	{
		mutex.unlock();
	}
};

struct LsmServer
{
	LsmWorker** workers;
	size_t nWorkers;
	Mutex mutex;
	std::map<LsmRelationId, LsmConnection> connections;

	void start();
	void wait();
	void stop();

	LsmConnection& open(LsmMessage const& msg);
	LsmServer(size_t maxClients);
	~LsmServer();
};

extern LsmQueue** queues;

#endif
