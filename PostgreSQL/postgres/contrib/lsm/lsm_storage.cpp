//
// RocksDB wrapper implementation
//
#include "lsm_db.h"
#ifdef LASER
#include <inttypes.h>
#include <vector>
#include <tuple>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

using namespace std;

vector<vector<tuple<uint32_t, uint32_t>>> createCGMatrix4(int num_levels, uint32_t column_num) {
	vector<vector<tuple<uint32_t, uint32_t>>> matrix(num_levels);

	vector<tuple<uint32_t, uint32_t>> cg_j1{make_tuple(0,3)};
	vector<tuple<uint32_t, uint32_t>> cg_j2{make_tuple(0,1), make_tuple(2,3)};
	vector<tuple<uint32_t, uint32_t>> cg_j3{make_tuple(0,0), make_tuple(1,1), make_tuple(2,2), make_tuple(3,3)};

	matrix[0] = cg_j1;
	matrix[1] = cg_j1;
	matrix[2] = cg_j2;
	matrix[3] = cg_j3;
	matrix[4] = cg_j3;
	matrix[5] = cg_j3;
	matrix[6] = cg_j3;
	return matrix;
}

vector<vector<tuple<uint32_t, uint32_t>>> createCGMatrix19(int num_levels, uint32_t column_num) {
	vector<vector<tuple<uint32_t, uint32_t>>> matrix(num_levels);

	vector<tuple<uint32_t, uint32_t>> cg_j1{make_tuple(0,29)};
	vector<tuple<uint32_t, uint32_t>> cg_j2{make_tuple(0,14), make_tuple(15,29)};
	vector<tuple<uint32_t, uint32_t>> cg_j3{make_tuple(0,14), make_tuple(15,19), make_tuple(20,29)};
	vector<tuple<uint32_t, uint32_t>> cg_j4{make_tuple(0,14), make_tuple(15,19), make_tuple(20,26), make_tuple(27,29)};

	matrix[0] = cg_j1;
	matrix[1] = cg_j1;
	matrix[2] = cg_j2;
	matrix[3] = cg_j2;
	matrix[4] = cg_j3;
	matrix[5] = cg_j3;
	matrix[6] = cg_j4;
	matrix[7] = cg_j4;
	return matrix;
}
#endif /* LASER */

#ifdef LSM_TXN
LsmTransaction
LsmConnection::startTx()
{
	LsmTransaction result;
	WriteOptions opts;
	TransactionOptions txn_options;
	txn_options.set_snapshot = true;
	opts.sync = LsmSync;
	Transaction *txn = txn_db->BeginTransaction(opts, txn_options);
	txn->SetSnapshot();

	result.txn = txn;
	result.snapshot = txn->GetSnapshot();

	return result;
}

void
LsmConnection::commitTx(LsmTransaction *txn)
{
	txn->txn->Commit();
	txn->txn = NULL;
}

void
LsmConnection::abortTx(LsmTransaction *txn)
{
	txn->txn->Rollback();
	txn->txn = NULL;
}
#endif /* LSM_TXN */

#ifdef LASER
static void
readCGMatrixFile(Options *options, ifstream &cg_matrix_file)
{
	vector<vector<tuple<uint32_t, uint32_t>>> matrix;
	string line;
	int num_levels = 0, num_columns = 0;

	/*
	 * CG matrix file's example:
	 *
	 * 0 19
	 * 0 4,5 19
	 * 0 4,5 19
	 * 0 4,5 19
	 * 0 4,5 14,15 19
	 * 0 4,5 14,15 19
	 *
	 * which means:
	 *
	 * level 0: tuple(0,19) => row oriented format
	 * level 1,2,3: tuple(0,4) | tuple(5,19) => two column groups
	 * level 4,5: tuple(0,4) | tuple(5,14) | tuple(15,19) => three column groups
	 */
	while (getline(cg_matrix_file, line)) {
		uint32_t cg_count = 0;
		stringstream ss(line);
		vector<tuple<uint32_t, uint32_t>> sublist;
		string token;

		while (getline(ss, token, ',')) {
			stringstream tuple_stream(token);
			uint32_t min_val, max_val;
			tuple_stream >> min_val >> max_val;
			sublist.emplace_back(min_val, max_val);
			cg_count++;
			
			/*
			 * level 0 is row-oriented. So max attribute number represents the
			 * last attribute number.
			 */
			if (num_levels == 0)
				num_columns = max_val + 1;
		}

		matrix.push_back(sublist);
		options->levels_cg_count.push_back(cg_count);
		num_levels++;
	}

	options->num_levels = num_levels;
	options->cg_range_matrix = matrix;
	options->column_from_k = 2;
	options->column_num = num_columns;
}
#endif /* LASER */

void
LsmConnection::open(char const* path)
{
	Options options;
#ifdef LSM_TXN
	TransactionDBOptions txn_db_options;
#endif /* LSM_TXN */
#ifdef LASER
	const string cg_matrix_filename = string(LSM_FDW_NAME) + "/cg_matrix";
	ifstream cg_matrix_file(cg_matrix_filename);
#endif /* LASER */

	options.create_if_missing = true;
	options.write_buffer_size = 64 * 1024 * 1024;	// default 64MB
	options.max_write_buffer_number = 2; // default 2
	options.min_write_buffer_number_to_merge = 1; // We can change this if merging slows down writes default 1
	options.max_bytes_for_level_base = 1 * 128 * 1024 * 1024ul; // default 256MB
	options.target_file_size_base = 64 * 1024 * 1024;
	options.target_file_size_multiplier = 1;	// we can reduce this given that target_file_size_base=256MB now default 1
	options.max_bytes_for_level_multiplier = 10; // default 10
	options.max_background_jobs = 2;
	options.max_subcompactions = 4;
	options.num_levels = 7; // default 7
	options.disable_auto_compactions = false; // default false
	options.compaction_pri = rocksdb::CompactionPri::kOldestLargestSeqFirst;
#ifdef LASER	
	if (!cg_matrix_file.is_open())
	{
		options.column_num = 4;
		options.column_from_k = 2;

		options.cg_range_matrix = createCGMatrix4(options.num_levels,
												  options.column_num);
		
		options.levels_cg_count = {1, 1, 2, 4, 4, 4, 4};
	}
	else
	{
		readCGMatrixFile(&options, cg_matrix_file);

		cg_matrix_file.close();

#ifdef EVAL_LASER
		// We set the write buffer size to 2GB.
		options.max_write_buffer_number = 32;
#endif
	}

	column_num = options.column_num;
#endif /* LASER */

#ifdef LSM_TXN
	Status s = TransactionDB::Open(options, txn_db_options, std::string(path), &txn_db);
#else /* !LSM_TXN */
	Status s = DB::Open(options, std::string(path), &db);
#endif /* LSM_TXN */
	if (!s.ok())
		LsmError(s.getState());
}

void
LsmConnection::close()
{
#ifdef LSM_TXN
	delete txn_db;
	txn_db = NULL;
#else /* !LSM_TXN */
	delete db;
	db = NULL;
#endif /* LSM_TXN */
}

uint64_t
LsmConnection::count()
{
	std::string count;
#ifdef LSM_TXN
	txn_db->GetProperty("rocksdb.estimate-num-keys", &count);
#else /* !LSM_TXN */
	db->GetProperty("rocksdb.estimate-num-keys", &count);
#endif /* LSM_TXN */
	return stoull(count);
}

Iterator*
#ifdef LASER
#ifdef LSM_TXN
LsmConnection::getIterator(std::vector<uint32_t> *read_filter,
						   LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::getIterator(std::vector<uint32_t> *read_filter)
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
LsmConnection::getIterator(LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::getIterator()
#endif /* LSM_TXN */
#endif /* LASER */
{
	ReadOptions ro = ReadOptions();
#ifdef LASER
	ro.filter_columns = *read_filter;
#endif /* LASER */
#ifdef LSM_TXN
	ro.snapshot = txn->snapshot;
	Iterator* it =txn->txn->GetIterator(ro);
#else /* !LSM_TXN */
	Iterator* it = db->NewIterator(ro);
#endif /* LSM_TXN */
	it->SeekToFirst();
	return it;
}

void
LsmConnection::releaseIterator(Iterator* it)
{
	delete it;
}

size_t
LsmConnection::next(Iterator* it, char* buf)
{
	size_t size;
	// Fetch as much records asfits in response buffer
	for (size = 0; it->Valid(); it->Next())
	{
		int keyLen = it->key().size();
		int valLen = it->value().size();
		int pairSize = sizeof(int)*2 + keyLen + valLen;

		if (size + pairSize > LSM_MAX_RECORD_SIZE)
			break;

		memcpy(&buf[size], &keyLen, sizeof keyLen);
		size += sizeof keyLen;
		memcpy(&buf[size], it->key().data(), keyLen);
		size += keyLen;

		memcpy(&buf[size], &valLen, sizeof valLen);
		size += sizeof valLen;
		memcpy(&buf[size], it->value().data(), valLen);
		size += valLen;
	}
	return size;
}

size_t
#ifdef LASER
#ifdef LSM_TXN
LsmConnection::lookup(char const* key, size_t keyLen, char* buf,
					  std::vector<uint32_t> *read_filter,
					  LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::lookup(char const* key, size_t keyLen, char* buf,
					  std::vector<uint32_t> *read_filter)
#endif /* LSM_TXN */
#else /* !LASER */
#ifdef LSM_TXN
LsmConnection::lookup(char const* key, size_t keyLen, char* buf,
					  LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::lookup(char const* key, size_t keyLen, char* buf)
#endif /* LSM_TXN */
#endif /* LASER */
{
	std::string sval;
	ReadOptions ro;
#ifdef LASER
	ro.filter_columns = *read_filter;
#endif /* LASER */
#ifdef LSM_TXN
	ro.snapshot = txn->snapshot;
	Status s = txn->txn->GetForUpdate(ro, Slice(key, keyLen), &sval);
#else /* !LSM_TXN */
	Status s = db->Get(ro, Slice(key, keyLen), &sval);
#endif /* LSM_TXN */
	if (!s.ok())
		return 0;
	size_t valLen = sval.length();
	memcpy(buf, sval.c_str(), valLen);
	return valLen;
}

bool
#ifdef LSM_TXN
LsmConnection::insert(char* key, size_t keyLen, char* val, size_t valLen,
					  LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::insert(char* key, size_t keyLen, char* val, size_t valLen)
#endif /* LSM_TXN */
{
	Status s;
#ifndef LSM_TXN
	WriteOptions opts;
#endif /* !LSM_TXN */
	if (!LsmUpsert)
	{
		std::string sval;
		ReadOptions ro;
#ifdef LSM_TXN
		ro.snapshot = txn->snapshot;
		s = txn->txn->GetForUpdate(ro, Slice(key, keyLen), &sval);
#else /* !LSM_TXN */
		s = db->Get(ro, Slice(key, keyLen), &sval);
#endif /* LSM_TXN */
		if (s.ok()) // key already exists 
			return false;
	}
#ifdef LSM_TXN
	s = txn->txn->Put(Slice(key, keyLen), Slice(val, valLen));
#else /* !LSM_TXN */
	opts.sync = LsmSync;
	s = db->Put(opts, Slice(key, keyLen), Slice(val, valLen));
#endif /* LSM_TXN */
	return s.ok();
}
#ifdef LASER
bool
#ifdef LSM_TXN
LsmConnection::update(char* key, size_t keyLen, char* val, size_t valLen,
					  LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::update(char* key, size_t keyLen, char* val, size_t valLen)
#endif /* LSM_TXN */
{
	Status s;
#ifdef LSM_TXN
	s = txn->txn->Merge(Slice(key, keyLen), Slice(val, valLen));
#else /* !LSM_TXN */
	WriteOptions opts;
	opts.sync = LsmSync;
	s = db->Merge(opts, Slice(key, keyLen), Slice(val, valLen));
#endif /* LSM_TXN */
	return s.ok();
}
#endif /* LASER */
bool
#ifdef LSM_TXN
LsmConnection::remove(char* key, size_t keyLen, LsmTransaction *txn)
#else /* !LSM_TXN */
LsmConnection::remove(char* key, size_t keyLen)
#endif /* LSM_TXN */
{
#ifdef LSM_TXN
	Status s = txn->txn->Delete(Slice(key, keyLen));
#else /* !LSM_TXN */
	WriteOptions opts;
	opts.sync = LsmSync;
	Status s = db->Delete(opts, Slice(key, keyLen));
#endif /* LSM_TXN */
	return s.ok();
}

