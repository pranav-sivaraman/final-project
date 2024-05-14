#include "mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000; i++) {
    Write(i, 0, 0);
    std::mutex *key_mutex = new std::mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (auto it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it) {
    delete it->second;
  }

  mvcc_data_.clear();

  for (auto it = mutexs_.begin(); it != mutexs_.end(); ++it) {
    delete it->second;
  }

  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you
// read/update the version_list
void MVCCStorage::Lock(Key key) { mutexs_[key]->lock(); }

// Unlock the key.
void MVCCStorage::Unlock(Key key) { mutexs_[key]->unlock(); }

// MVCC Read
// If there exists a record for the specified key, sets '*result' equal to
// the value associated with the key and returns true, else returns false;
// The third parameter is the txn_unique_id(txn timestamp), which is used for
// MVCC.
bool MVCCStorage::Read(Key key, Value *result, int txn_unique_id) {
  //
  // Implement this method!

  // Hint: Iterate the version_lists and return the version whose write
  // timestamp (version_id) is the largest write timestamp less than or equal to
  // txn_unique_id.

  // Check if the key exists in mvcc_data_
  if (mvcc_data_.count(key) == 0) {
    return false;
  }

  for (auto version : *mvcc_data_[key]) {
    // Return the first version whose version_id is less than or equal to
    // txn_unique_id This assumes that the version list is sorted in descending
    // order
    if (version->version_id_ <= txn_unique_id) {
      *result = version->value_;
      version->max_read_id_ = txn_unique_id;
      return true;
    }
  }

  return false;
}

// Check whether the txn executed on the latest version of the key.
bool MVCCStorage::CheckKey(Key key, int txn_unique_id) {
  //
  // Implement this method!

  // Hint: Before all writes are applied (and SSI reads are validated), we need
  // to make sure that each key was accessed safely based on MVCC timestamp
  // ordering protocol. This method only checks one key, so you should call this
  // method for each key (as necessary). Return true if this key passes the
  // check, return false if not. Note that you don't have to call Lock(key) in
  // this method, just call Lock(key) before you call this method and call
  // Unlock(key) afterward.

  // If key doesn't exist, or if deque is empty for some reason, return false
  if (mvcc_data_.count(key) == 0 || mvcc_data_[key]->empty()) {
    return false;
  }

  // Assuming that the version list is sorted in descending order
  return txn_unique_id >= mvcc_data_[key]->front()->version_id_;
}

// MVCC Write, call this method only if CheckWrite return true.
// Inserts a new version with key and value
// The third parameter is the txn_unique_id(txn timestamp), which is used for
// MVCC.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  //
  // Implement this method!

  // Hint: Insert a new version (malloc a Version and specify its
  // value/version_id/max_read_id) into the version_lists. Note that
  // InitStorage() also calls this method to init storage. Note that you don't
  // have to call Lock(key) in this method, just call Lock(key) before you call
  // this method and call Unlock(key) afterward. Note that the performance would
  // be much better if you organize the versions in decreasing order.

  auto version = new Version();
  version->value_ = value;
  version->version_id_ = txn_unique_id;
  version->max_read_id_ = 0;

  if (mvcc_data_.count(key) == 0) {
    mvcc_data_[key] = new std::deque<Version *>();
  }

  // Insert the new version in descending order
  auto it = mvcc_data_[key]->begin();
  while (it != mvcc_data_[key]->end() && (*it)->version_id_ > txn_unique_id) {
    ++it;
  }
  mvcc_data_[key]->insert(it, version);
}

