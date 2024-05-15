#include <atomic>
#include <mutex>
#include <set>
#include <stdio.h>
#include <unordered_set>

#include "lock_manager.h"
#include "txn_processor.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

using std::set;

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Create the storage
  if (mode_ == MVCC || mode_ == MVCC_SSI) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }

  storage_->InitStorage();
  stopped_ = false;
  scheduler_thread_ = std::thread{&TxnProcessor::RunScheduler, this};
}

TxnProcessor::~TxnProcessor() {
  // Wait for the scheduler thread to join back before destroying the object and
  // its thread pool.
  if (mode_ == CALVIN_EPOCH) {
    pthread_join(calvin_sequencer_thread, NULL);
  }
  stopped_ = true;
  scheduler_thread_.join();

  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn *txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  txn->unique_id_ = next_unique_id_++;
  txn_requests_.Push(txn);
}

Txn *TxnProcessor::GetTxnResult() {
  Txn *txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    usleep(1);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
  case SERIAL:
    RunSerialScheduler();
    break;
  case LOCKING:
    RunLockingScheduler();
    break;
  case LOCKING_EXCLUSIVE_ONLY:
    RunLockingScheduler();
    break;
  case OCC:
    RunOCCScheduler();
    break;
  case P_OCC:
    RunOCCParallelScheduler();
    break;
  case MVCC:
    RunMVCCScheduler();
    break;
  case CALVIN:
    RunCalvinContScheduler();
    break;
  case CALVIN_I:
    RunCalvinContIndivScheduler();
    break;
  case CALVIN_EPOCH:
    RunCalvinEpochScheduler();
    break;
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn *txn;
  while (!stopped_) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        committed_txns_.Push(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn *txn;
  while (!stopped_) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (std::set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
        }
      }

      // Request write locks.
      for (std::set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        if (!lm_->WriteLock(txn, *it)) {
          blocked = true;
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed.
      if (blocked == false) {
        ready_txns_.push_back(txn);
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        committed_txns_.Push(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Release read locks.
      for (std::set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (std::set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.AddTask([this, txn]() { this->ExecuteTxn(txn); });
    }
  }
}

void TxnProcessor::ExecuteTxn(Txn *txn) {
  // Get the current commited transaction index for the further validation.
  txn->occ_start_idx_ = committed_txns_.Size();

  // Read everything in from readset.
  for (std::set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (std::set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn *txn) {
  // Write buffered writes out to storage.
  for (std::map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  Txn *txn;
  while (!stopped_) {
    // Get the next new txn request (if one is pending)
    if (txn_requests_.Pop(&txn)) {
      // Pass it to an execution thread
      tp_.AddTask([this, txn]() { this->ExecuteTxn(txn); });
    }

    // Dealing with a finished transaction
    while (completed_txns_.Pop(&txn)) {
      // Validation phase
      // Use the data structure in `txn_processor` class to check overlap with
      // each record whose key appears in the txn's read and write sets
      bool valid = true;

      // Check for overlap with newly committed transactions
      // after the txn's occ_start_idx_
      for (int i = txn->occ_start_idx_ + 1; i < committed_txns_.Size(); i++) {
        Txn *t = committed_txns_[i];

        // check if write_set of t intersects with read_set of txn
        for (auto key : txn->readset_) {
          if (t->writeset_.find(key) != t->writeset_.end()) {
            valid = false;
            break;
          }
        }
      }

      // If validation failed, cleanup txn and completely restart it
      if (!valid) {
        // Cleanup txn
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;

        // Restart txn
        std::scoped_lock lock{mutex_};
        txn->unique_id_ = next_unique_id_++;
        txn_requests_.Push(txn);
      } else {
        // Apply all writes
        ApplyWrites(txn);

        // Mark transaction as committed
        committed_txns_.Push(txn);
        txn->status_ = COMMITTED;

        // Update relevant data structure
        txn_results_.Push(txn);
      }
    }
  }
}

void TxnProcessor::ExecuteTxnParallel(Txn *txn) {
  txn->occ_start_idx_ = committed_txns_.Size();

  // Perform "read phase" of transaction
  // Read everything in from readset.
  for (std::set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (std::set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Start of critical section
  // Make a copy of the active set
  std::unique_lock lock{active_set_mutex_};
  auto finish_active = active_set_.GetSet();
  // Add this txn to the active set
  active_set_.Insert(txn);
  // End of critical section
  lock.unlock();

  // Validation phase
  // Use the data structure in `txn_processor` class to check overlap with
  // each record whose key appears in the txn's read and write sets
  bool valid = true;

  // NOTE: This is not in the pseudocode in the project description
  // Check for overlap with newly committed transactions
  // after the txn's occ_start_idx_
  for (int i = txn->occ_start_idx_ + 1; i < committed_txns_.Size(); i++) {
    Txn *t = committed_txns_[i];

    // check if write_set of t intersects with read_set of txn
    for (auto key : txn->readset_) {
      if (t->writeset_.find(key) != t->writeset_.end()) {
        valid = false;
        break;
      }
    }
  }

  // Check overlap with each record whose key appears in the txn's read and
  // write sets NOTE: we only run this if the txn hasn't been invalidated by the
  // previous check NOTE: this is the only validation implemented in the
  // pseudocode in the project description
  if (valid) {
    for (auto t : finish_active) {
      // if txn's write set intersects with t's write sets
      for (auto key : txn->writeset_) {
        if (t->writeset_.find(key) != t->writeset_.end()) {
          valid = false;
          break;
        }
      }

      // if txn's read set intersects with t's write sets
      for (auto key : txn->readset_) {
        if (t->writeset_.find(key) != t->writeset_.end()) {
          valid = false;
          break;
        }
      }
    }
  }

  // If validation failed, cleanup txn and completely restart it
  if (!valid) {
    // Remove this txn from the active set
    std::unique_lock active_set_lock{active_set_mutex_};
    active_set_.Erase(txn);
    active_set_lock.unlock();

    // Cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Restart txn
    std::scoped_lock lock{mutex_};
    txn->unique_id_ = next_unique_id_++;
    txn_requests_.Push(txn);
  } else {
    // Apply all writes
    ApplyWrites(txn);

    // Remove this txn from the active set
    std::unique_lock active_set_lock{active_set_mutex_};
    active_set_.Erase(txn);
    active_set_lock.unlock();

    // Mark transaction as committed
    committed_txns_.Push(txn);
    txn->status_ = COMMITTED;

    // Update relevant data structure
    txn_results_.Push(txn);
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  Txn *txn;
  while (!stopped_) {
    // Get the next new transaction request (if one is pending) and pass it to
    // an execution thread that executes the txn logic *and also* does the
    // validation and write phases.
    if (txn_requests_.Pop(&txn)) {
      tp_.AddTask([this, txn]() { this->ExecuteTxnParallel(txn); });
    }
  }
}

set<Key> set_union(const std::set<Key> &s1, const set<Key> &s2) {
  std::set<Key> result = s1;
  result.insert(s2.begin(), s2.end());
  return result;
}

void TxnProcessor::MVCCExecuteTxn(Txn *txn) {
  // Read all necessary data for this transaction from storage
  // (Note that unlike the version of MVCC from class, you should lock the key
  // before each read)

  // Read everything in from readset and writeset.
  for (auto key : set_union(txn->readset_, txn->writeset_)) {
    // Lock the key
    storage_->Lock(key);

    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(key, &result, txn->unique_id_))
      txn->reads_[key] = result;

    // Unlock the key
    storage_->Unlock(key);
  }

  // Execute txn's program logic.
  txn->Run();

  // Acquire all locks for keys in the write_set_
  for (auto key : txn->writeset_) {
    storage_->Lock(key);
  }

  // Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  bool checkPassed = true;
  for (auto key : txn->writeset_) {
    if (!((MVCCStorage *)storage_)->CheckKey(key, txn->unique_id_)) {
      checkPassed = false;
      break;
    }
  }

  // If each key passed the check
  if (checkPassed) {
    // Apply the writes
    ApplyWrites(txn);

    // Release all locks for keys in the write_set_
    for (auto key : txn->writeset_) {
      storage_->Unlock(key);
    }

    // Mark transaction as committed
    committed_txns_.Push(txn);
    txn->status_ = COMMITTED;

    // Update relevant data structure
    txn_results_.Push(txn);
  } else { // At least one key failed the check
    // Release all locks for keys in the write_set_
    for (auto key : txn->writeset_) {
      storage_->Unlock(key);
    }

    // Cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Restart txn -- same as OCC
    std::scoped_lock lock{mutex_};
    txn->unique_id_ = next_unique_id_++;
    txn_requests_.Push(txn);
  }
}

void TxnProcessor::RunMVCCScheduler() {
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like
  // TxnProcessor::MVCCExecuteTxn.

  Txn *txn;
  while (!stopped_) {
    // Get the next new transaction request (if one is pending) and pass it to
    // an execution thread that executes the txn logic *and also* does the
    // validation and write phases.
    if (txn_requests_.Pop(&txn)) {
      tp_.AddTask([this, txn]() { this->MVCCExecuteTxn(txn); });
    }
  }
}

void TxnProcessor::MVCCSSIExecuteTxn(Txn *txn) {
  // Read all necessary data for this transaction from storage
  // (Note that unlike the version of MVCC from class, you should lock the key
  // before each read)

  // Read everything in from readset and writeset.
  for (auto key : set_union(txn->readset_, txn->writeset_)) {
    // Lock the key
    storage_->Lock(key);

    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(key, &result, txn->unique_id_))
      txn->reads_[key] = result;

    // Unlock the key
    storage_->Unlock(key);
  }

  // Execute txn's program logic.
  txn->Run();

  // THIS IS DIFFERENT FROM MVCCExecuteTxn: we lock write_set AND read_set
  // Acquire all locks for keys in the read_set_ and write_set_
  // (Lock any overlapping key only once.)
  for (auto key : set_union(txn->writeset_, txn->readset_)) {
    storage_->Lock(key);
  }

  // Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  bool checkPassed = true;
  for (auto key : txn->writeset_) {
    if (!((MVCCStorage *)storage_)->CheckKey(key, txn->unique_id_)) {
      checkPassed = false;
      break;
    }
  }

  // If each key passed the check
  if (checkPassed) {
    // Apply the writes
    ApplyWrites(txn);

    // Release all locks for ALL keys (read_set_ and write_set_)
    for (auto key : set_union(txn->writeset_, txn->readset_)) {
      storage_->Unlock(key);
    }

    // Mark transaction as committed
    committed_txns_.Push(txn);
    txn->status_ = COMMITTED;

    // Update relevant data structure
    txn_results_.Push(txn);
  } else { // At least one key failed the check
    // Release all locks for ALL keys (read_set_ and write_set_)
    for (auto key : set_union(txn->writeset_, txn->readset_)) {
      storage_->Unlock(key);
    }

    // Cleanup txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Restart txn -- same as OCC
    std::scoped_lock lock{mutex_};
    txn->unique_id_ = next_unique_id_++;
    txn_requests_.Push(txn);
  }
}

void TxnProcessor::RunMVCCSSIScheduler() {
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like
  // TxnProcessor::MVCCSSIExecuteTxn.

  Txn *txn;
  while (!stopped_) {
    // Get the next new transaction request (if one is pending) and pass it to
    // an execution thread that executes the txn logic *and also* does the
    // validation and write phases.
    if (txn_requests_.Pop(&txn)) {
      tp_.AddTask([this, txn]() { this->MVCCSSIExecuteTxn(txn); });
    }
  }
}
