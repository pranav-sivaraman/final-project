#include <atomic>
#include <set>
#include <stdio.h>
#include <unordered_set>

#include "lock_manager.h"
#include "txn_processor.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

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
    RunCalvinScheduler();
  }
}

void TxnProcessor::ExecuteTxnCalvin(Txn *txn) {
  ExecuteTxn(txn);

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

  std::shared_lock lock1{adj_list_mutex, std::defer_lock};
  std::unique_lock lock2{indegrees_mutex, std::defer_lock};
  std::lock(lock1, lock2);

  auto neighbors = adj_list[txn];
  for (const auto &neighbor : neighbors) {
    if (--indegrees[neighbor] == 0) {
      tp_.AddTask([this, neighbor]() { this->ExecuteTxnCalvin(neighbor); });
    }
  }

  txn_results_.Push(txn);
}

void TxnProcessor::RunCalvinScheduler() {
  Txn *txn;

  std::unordered_map<Key, std::unordered_set<Txn *>> shared_holders;
  std::unordered_map<Key, Txn *> last_exclusive;

  while (!stopped_.load(std::memory_order_relaxed)) {
    // Get next txn request
    if (txn_requests_.Pop(&txn)) {
      std::scoped_lock lock{adj_list_mutex, indegrees_mutex};
      adj_list[txn] = {};

      // Loop through readset
      for (const Key &key : txn->readset_) {
        // Add to shared holders
        if (!shared_holders.contains(key)) {
          shared_holders[key] = {};
        }
        shared_holders[key].insert(txn);

        // If the last_excl txn is not the current txn, add an edge
        if (last_exclusive.contains(key) && last_exclusive[key] != txn &&
            last_exclusive[key]->Status() == INCOMPLETE &&
            !adj_list[last_exclusive[key]].contains(txn)) {
          adj_list[last_exclusive[key]].insert(txn);
          indegrees[txn]++;
        }
      }

      for (const Key &key : txn->writeset_) {
        // Add an edge between the current txn and all shared holders
        if (shared_holders.contains(key)) {
          for (auto conflicting_txn : shared_holders[key]) {
            if (conflicting_txn != txn &&
                conflicting_txn->Status() == INCOMPLETE &&
                !adj_list[conflicting_txn].contains(txn)) {
              adj_list[conflicting_txn].insert(txn);
              indegrees[txn]++;
            }
          }
          shared_holders[key].clear();
        }

        last_exclusive[key] = txn;
      }

      if (indegrees[txn] == 0) {
        tp_.AddTask([this, txn]() { this->ExecuteTxnCalvin(txn); });
      }
    }
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
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunOCCParallelScheduler() {
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler() {
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like
  // TxnProcessor::MVCCExecuteTxn.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCSSIScheduler() {
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like
  // TxnProcessor::MVCCSSIExecuteTxn.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}
