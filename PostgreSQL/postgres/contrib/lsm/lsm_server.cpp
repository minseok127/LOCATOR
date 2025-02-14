//
// Worker's part of LSM queue
//
#include "lsm_api.h"
#include "lsm_db.h"

static LsmServer* server;;

// LSM GUCs
int  LsmQueueSize;
bool LsmSync;
bool LsmUpsert;

/*
 * Enqueue message
 */
void LsmQueue::put(LsmMessage const& msg)
{
	int size = sizeof(LsmMessageHeader) + msg.hdr.keySize + msg.hdr.valueSize;

	if (size > LsmQueueSize)
		LsmError("Message is too long");

	while (true)
	{
		int getPos = this->getPos;
		int putPos = this->putPos;
		int available = putPos >= getPos ? LsmQueueSize - putPos + getPos : getPos - putPos;

		if (size >= available) /* queue overflow? */
		{
			if (!writerBlocked)
			{
				writerBlocked = true;
				LsmMemoryBarrier();
				// Enforce "writeBlocked" flag to be visible by consumer and retry availability check
			}
			else
			{
				SemWait(&full);
			}
			continue;
		}
		size_t tail = LsmQueueSize - putPos;

		// Copy header
		if (tail <= sizeof(LsmMessageHeader))
		{
			memcpy(&req[putPos], &msg, tail);
			memcpy(&req[0], (char*)&msg + tail, sizeof(LsmMessageHeader) - tail);
			putPos = sizeof(LsmMessageHeader) - tail;
		}
		else
		{
			memcpy(&req[putPos], &msg, sizeof(LsmMessageHeader));
			putPos += sizeof(LsmMessageHeader);
		}
		tail = LsmQueueSize - putPos;

		// Copy key
		if (tail <= msg.hdr.keySize)
		{
			memcpy(&req[putPos], msg.key, tail);
			memcpy(&req[0], msg.key + tail, msg.hdr.keySize - tail);
			putPos = msg.hdr.keySize - tail;
		}
		else
		{
			memcpy(&req[putPos], msg.key, msg.hdr.keySize);
			putPos += msg.hdr.keySize;
		}
		tail = LsmQueueSize - putPos;

		// Copy value
		if (tail <= msg.hdr.valueSize)
		{
			memcpy(&req[putPos], msg.value, tail);
			memcpy(&req[0], msg.value + tail, msg.hdr.valueSize - tail);
			putPos = msg.hdr.valueSize - tail;
		}
		else
		{
			memcpy(&req[putPos], msg.value, msg.hdr.valueSize);
			putPos += msg.hdr.valueSize;
		}
		this->putPos = putPos;
		SemPost(&empty); // Enforce write barrier and notify consumer
		return;
	}
}

/*
 * Dequeue message.
 * This method is not advancing getPos to make it possible to point data directly in ring buffer.
 * getPost will be addvance by next() method after th end of message processing
 */
void LsmQueue::get(char* buf, LsmMessage& msg)
{
	// Wait until queue is not empty.
	// We are not comparing getPos with putPos before waiting semaphore to make sure that writer barrier enforced by SemPost
	// makes all data written by producer visible for consumer.
	SemWait(&empty);

	if (terminate)
	{
		msg.hdr.op = LsmOpTerminate;
		return;
	}

	int getPos = this->getPos;
	int putPos = this->putPos;

	if (putPos == getPos)
		LsmError("Queue race condition!");

	size_t tail = LsmQueueSize - getPos;

	//  Copy header
	if (tail <= sizeof(LsmMessageHeader))
	{
		memcpy(&msg, &req[getPos], tail);
		memcpy((char*)&msg + tail, &req[0], sizeof(LsmMessageHeader) - tail);
		getPos = sizeof(LsmMessageHeader) - tail;
	}
	else
	{
		memcpy(&msg, &req[getPos], sizeof(LsmMessageHeader));
		getPos += sizeof(LsmMessageHeader);
	}
	tail = LsmQueueSize - getPos;

	// Copy key
	if (tail < msg.hdr.keySize)
	{
		memcpy(buf, &req[getPos], tail);
		memcpy(buf + tail, &req[0], msg.hdr.keySize - tail);
		getPos = msg.hdr.keySize - tail;
		msg.key = buf;
		buf += msg.hdr.keySize;
	}
	else
	{
		msg.key = &req[getPos];
		getPos += msg.hdr.keySize;
		if (getPos == LsmQueueSize)
		{
			getPos = 0;
		}
	}
	tail = LsmQueueSize - getPos;

	// Copy value
	if (tail < msg.hdr.valueSize)
	{
		memcpy(buf, &req[getPos], tail);
		memcpy(buf + tail, &req[0], msg.hdr.valueSize - tail);
		msg.value = buf;
	}
	else
	{
		msg.value = &req[getPos];
	}
}

#ifdef LSM_TXN
/*
 * Start transaction
 */
void
LsmWorker::startTx(LsmMessage const& msg)
{
	if (txn.txn == NULL)
	{
		LsmConnection& con(open(msg));
		txn = con.startTx();
	}
	SemPost(&queue->ready);
}

/*
 * Commit transaction
 */
void
LsmWorker::commitTx(LsmMessage const& msg)
{
	if (txn.txn != NULL)
	{
		LsmConnection& con(open(msg));
		con.commitTx(&txn);
	}
	SemPost(&queue->ready);
}

/*
 * Abort transaction
 */
void
LsmWorker::abortTx(LsmMessage const& msg)
{
	if (txn.txn != NULL)
	{
		LsmConnection& con(open(msg));
		con.abortTx(&txn);
	}
	SemPost(&queue->ready);
}
#endif /* LSM_TXN */

/*
 * Advance getPos in queue (see comment to get() method
 */
void LsmQueue::next(LsmMessage const& msg)
{
	int getPos = this->getPos;
	bool writerBlocked = this->writerBlocked;
	size_t size = sizeof(LsmMessageHeader) + msg.hdr.keySize + msg.hdr.valueSize;
	size_t tail = LsmQueueSize - getPos;
	this->getPos = (tail <= size) ? size - tail : getPos + size;
	if (writerBlocked)
	{
		// Notify consumer that some more free space is avaialble in ring buffer
		this->writerBlocked = false;
		SemPost(&full);
	}
}

inline LsmConnection&
LsmWorker::open(LsmMessage const& msg)
{
	return server->open(msg);
}

/*
 * Insert or update record in the storage
 */
void
LsmWorker::insert(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));

#ifdef LSM_TXN
	queue->resp[0] = (char)con.insert(msg.key, msg.hdr.keySize, msg.value, msg.hdr.valueSize, &txn);
#else /* !LSM_TXN */
	queue->resp[0] = (char)con.insert(msg.key, msg.hdr.keySize, msg.value, msg.hdr.valueSize);
#endif /* LSM_TXN */
#ifdef LASER
	SemPost(&queue->ready);
#else /* !LASER */
	if (LsmSync)
		SemPost(&queue->ready);
#endif /* LASER */
}
#ifdef LASER
/*
 * Update record in the storage
 */
void
LsmWorker::update(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
#ifdef LSM_TXN
	queue->resp[0] = (char)con.update(msg.key, msg.hdr.keySize, msg.value, msg.hdr.valueSize, &txn);
#else /* !LSM_TXN */
	queue->resp[0] = (char)con.update(msg.key, msg.hdr.keySize, msg.value, msg.hdr.valueSize);
#endif /* LSM_TXN */
	SemPost(&queue->ready);
}
#endif /* LASER */
/*
 * Delete record
 */
void
LsmWorker::remove(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
#ifdef LSM_TXN
	queue->resp[0] = (char)con.remove(msg.key, msg.hdr.keySize, &txn);
#else /* !LSM_TXN */
	queue->resp[0] = (char)con.remove(msg.key, msg.hdr.keySize);
#endif /* LSM_TXN */
#ifdef LASER
	SemPost(&queue->ready);
#else /* !LASER */
	if (LsmSync)
		SemPost(&queue->ready);
#endif /* LASER */
}

/*
 * Get estimation for number of records in the relation
 */
void
LsmWorker::count(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
	uint64_t count = con.count();
	memcpy(queue->resp, &count, sizeof(count));
	SemPost(&queue->ready);
}

/*
 * Close cursor implicitly openned by fetch() method
 */
void
LsmWorker::closeCursor(LsmMessage const& msg)
{
	LsmCursor& csr(cursors[msg.hdr.cid]);
	csr.con->releaseIterator(csr.iter);
	cursors.erase(msg.hdr.cid);
}
#ifdef LASER
/*
 * Set read_filter for LASER
 */
void
LsmWorker::setFilter(LsmMessage const& msg)
{
	bool *filter = (bool *)(msg.value);
	size_t filterSize = msg.hdr.valueSize;
	int natts = filterSize / sizeof(bool);
	read_filter.clear();
	for (int i = 1; i < natts; i++)
	{
		if (filter[i])
		{
			read_filter.push_back(i);
		}
	}
	SemPost(&queue->ready);
}
#endif /* LASER */
/*
 * Locate record by key
 */
void
LsmWorker::lookup(LsmMessage const& msg)
{
	LsmConnection& con(open(msg));
#ifdef LASER
#ifdef LSM_TXN
	queue->respSize = con.lookup(msg.key, msg.hdr.keySize, queue->resp,
								 &read_filter, &txn);
#else /* !LSM_TXN */
	queue->respSize = con.lookup(msg.key, msg.hdr.keySize, queue->resp,
								 &read_filter);
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
	queue->respSize = con.lookup(msg.key, msg.hdr.keySize, queue->resp, &txn);
#else /* !LSM_TXN */
	queue->respSize = con.lookup(msg.key, msg.hdr.keySize, queue->resp);
#endif /* LSM_TXN */
#endif /* LASER */
	SemPost(&queue->ready);
}

/*
 * Fetch serveral records from iterator
 */
void
LsmWorker::fetch(LsmMessage const& msg)
{
	LsmCursor& csr(cursors[msg.hdr.cid]);
	if (!csr.con)
	{
		csr.con = &open(msg);
#ifdef LASER
#ifdef LSM_TXN
		csr.iter = csr.con->getIterator(&read_filter, &txn);
#else /* !LSM_TXN */
		csr.iter = csr.con->getIterator(&read_filter);
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
		csr.iter = csr.con->getIterator(&txn);
#else /* !LSM_TXN */
		csr.iter = csr.con->getIterator();
#endif /* LSM_TXN */
#endif /* LASER */
	}
    queue->respSize = csr.con->next(csr.iter, queue->resp);
	SemPost(&queue->ready);
}

/*
 * Worker main loop
 */
void
LsmWorker::run()
{
	while (true)
	{
		LsmMessage msg;
		char buf[LSM_MAX_RECORD_SIZE];
		queue->get(buf, msg);

		switch (msg.hdr.op) {
#ifdef LSM_TXN
		  case LsmOpStart:
			startTx(msg);
			break;
		  case LsmOpCommit:
			commitTx(msg);
			break;
		  case LsmOpAbort:
			abortTx(msg);
			break;
#endif /* LSM_TXN */
		  case LsmOpTerminate:
			return;
		  case LsmOpCount:
			count(msg);
			break;
		  case LsmOpCloseCursor:
			closeCursor(msg);
			break;
#ifdef LASER
		  case LsmOpSetFilter:
			setFilter(msg);
			break;
#endif /* LASER */
		  case LsmOpFetch:
			fetch(msg);
			break;
		  case LsmOpLookup:
			lookup(msg);
			break;
		  case LsmOpInsert:
			insert(msg);
			break;
#ifdef LASER
		  case LsmOpUpdate:
			update(msg);
			break;
#endif /* LASER */
		  case LsmOpDelete:
			remove(msg);
			break;
		  default:
			assert(false);
		}
		queue->next(msg);
	}
}

void
LsmWorker::start()
{
	PthreadCreate(&thread, NULL, LsmWorker::main, this);
}

void
LsmWorker::stop()
{
	queue->terminate = true;
	SemPost(&queue->empty);
}

void
LsmWorker::wait()
{
	void* status;
	PthreadJoin(thread, &status);
}

void*
LsmWorker::main(void* arg)
{
	((LsmWorker*)arg)->run();
	return NULL;
}

/*
 * Start LSM worker threads for all backends and wait their completion.
 * TODO: threads can be started on demand, but it complicates client-server protocol.
 */
void
LsmRunWorkers(int maxClients)
{
	server = new LsmServer(maxClients);
	server->start();
	server->wait();
	delete server;
}

/*
 * Wait terination of LSM threads
 */
void
LsmStopWorkers(void)
{
	server->stop();
}

LsmServer::LsmServer(size_t maxClients) : nWorkers(maxClients)
{
	workers = new LsmWorker*[nWorkers];
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i] = new LsmWorker(this, queues[i]);
	}
}

void
LsmServer::start()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->start();
	}
}

void
LsmServer::wait()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->wait();
	}
}


LsmServer::~LsmServer()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		delete workers[i];
	}
	delete[] workers;
}

void
LsmServer::stop()
{
	for (size_t i = 0; i < nWorkers; i++)
	{
		workers[i]->stop();
	}
}

LsmConnection&
LsmServer::open(LsmMessage const& msg)
{
	CriticalSection cs(mutex);
	LsmConnection& con = connections[msg.hdr.rid];
#ifdef LSM_TXN
	if (con.txn_db == NULL)
#else /* !LSM_TXN */
	if (con.db == NULL)
#endif /* LSM_TXN */
	{
		char path[64];
		sprintf(path, "%s/%d", LSM_FDW_NAME, msg.hdr.rid);
		con.open(path);
	}
	return con;
}
