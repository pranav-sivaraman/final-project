#include "lock_manager.h"

LockManagerA::LockManagerA(std::deque<Txn *> *ready_txns) {
  // // printf("creating new lock manager?\n");
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key) {
  //
  // Implement this method!

  // get the queue for the key
  if (lock_table_.find(key) == lock_table_.end()) {
    // key isn't ins the table yet
    lock_table_[key] = new std::deque<LockRequest>();
  }
  std::deque<LockRequest> *lr_queue = lock_table_[key];

  // calculate how many transactions were already present in queue
  int num_before = lr_queue->size();

  // create and add lock request for this transaction
  lr_queue->push_back(LockRequest(LockMode::EXCLUSIVE, txn));

  // APPARENTLY WE DONT ADD TO THE READY LIST?????
  if (num_before > 0) {
    // add transaction to waiting list

    if (txn_waits_.find(txn) == txn_waits_.end()) {
      // txn was not in the map
      // add transaction to map
      txn_waits_[txn] = num_before;
    } else {
      // add transaction to map
      txn_waits_[txn] += num_before;
    }
  }
  return num_before == 0;
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn *txn, const Key &key) {
  //
  // Implement this method!

  if (lock_table_.find(key) == lock_table_.end()) {
    // key not in lock / can't actually release
    return;
  }
  // get queue for the key
  std::deque<LockRequest> *lr_queue = lock_table_[key];

  // find location of this transaction in the queue
  size_t txn_index = lr_queue->size();
  for (size_t i = 0; i < lr_queue->size(); i++) {
    if (lr_queue->at(i).txn_ == txn) {
      txn_index = i;
      break;
    }
  }
  if (txn_index == lr_queue->size()) {
    // this transaction doesn't have a lock on this key
    return;
  }

  // remove this transaction from the queue
  lr_queue->erase(lr_queue->begin() + txn_index);

  // update following elements
  for (size_t i = txn_index; i < lr_queue->size(); i++) {
    // get transaction waiting for us to release a lock
    Txn *update_txn = lr_queue->at(i).txn_;
    // update number of transactions before it
    txn_waits_[update_txn]--;
    // if the transaction is no longer waiting for a lock
    // remove it from txn_waits_ and add to read_txns_
    if (txn_waits_[update_txn] == 0) {
      ready_txns_->push_back(update_txn);
      txn_waits_.erase(update_txn);
    }
  }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key &key, std::vector<Txn *> *owners) {
  //
  // Implement this method!
  owners->clear();

  if (lock_table_.find(key) == lock_table_.end() ||
      lock_table_[key]->size() == 0) {
    // the key isn't in the lock table or empty
    return LockMode::UNLOCKED;
  } else {
    owners->push_back(lock_table_[key]->at(0).txn_);
    return LockMode::EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(std::deque<Txn *> *ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn *txn, const Key &key) {
  // printf("write locking key: %d\n", key);

  //
  // Implement this method!

  // first let's get the correct Lock Request Queue
  if (lock_table_.find(key) == lock_table_.end()) {
    // queue doesn't exist yet
    lock_table_[key] = new std::deque<LockRequest>();
  }

  std::deque<LockRequest> *lr_queue = lock_table_[key];

  size_t num_before = lr_queue->size();

  // add lock request to queue
  lr_queue->push_back(LockRequest(LockMode::EXCLUSIVE, txn));

  if (num_before > 0) {
    // can't get lock yet
    // add txn to waitlist
    if (txn_waits_.find(txn) == txn_waits_.end()) {
      txn_waits_[txn] = 0;
    }
    txn_waits_[txn] += num_before;
  }

  return num_before == 0;
}

bool LockManagerB::ReadLock(Txn *txn, const Key &key) {
  //
  // Implement this method!
  // printf("read locking key: %d\n", key);

  if (lock_table_.find(key) == lock_table_.end()) {
    // queue doesn't exist yet
    lock_table_[key] = new std::deque<LockRequest>();
  }

  // first let's get the correct Lock Request Queue
  std::deque<LockRequest> *lr_queue = lock_table_[key];

  int num_exclusive_before = 0;
  // check for exclusive transactions before this one
  for (size_t i = 0; i < lr_queue->size(); i++) {
    if (lr_queue->at(i).mode_ == LockMode::EXCLUSIVE) {
      num_exclusive_before++;
    }
  }

  // add lock request to queue
  lr_queue->push_back(LockRequest(LockMode::SHARED, txn));

  if (num_exclusive_before > 0) {
    // can't get lock yet
    // add txn to waitlist
    if (txn_waits_.find(txn) == txn_waits_.end()) {
      txn_waits_[txn] = 0;
    }
    txn_waits_[txn] += num_exclusive_before;
  }

  return num_exclusive_before == 0;
}

void LockManagerB::Release(Txn *txn, const Key &key) {
  // Implement this method!

  if (lock_table_.find(key) == lock_table_.end()) {
    // don't need to do anything if the lock doesn't exist on the key
    return;
  }

  std::deque<LockRequest> *lr_queue = lock_table_[key];
  LockMode released_type;
  // find location of this transaction in the queue
  size_t txn_index = lr_queue->size();
  for (size_t i = 0; i < lr_queue->size(); i++) {
    if (lr_queue->at(i).txn_ == txn) {
      txn_index = i;
      released_type = lr_queue->at(i).mode_;
      break;
    }
  }

  if (txn_index == lr_queue->size()) {
    // this transaction doesn't have a lock on the key
    return;
  }

  // remove this transaction from the queue
  lr_queue->erase(lr_queue->begin() + txn_index);

  // update following elements
  for (size_t i = txn_index; i < lr_queue->size(); i++) {

    // get transaction waiting for us to release a lock
    LockMode update_type = lr_queue->at(i).mode_;
    Txn *update_txn = lr_queue->at(i).txn_;

    // update number of transactions before it if necessary
    // shared locks only count exclusives before them
    // exlusive lock count all before them
    if (released_type == LockMode::EXCLUSIVE ||
        update_type == LockMode::EXCLUSIVE) {
      if (txn_waits_.find(update_txn) == txn_waits_.end()) {
        printf("Shouldn't be here in release lock :(\n");
        //
      }
      txn_waits_[update_txn]--;
      // if the transaction is no longer waiting for a lock
      // remove it from txn_waits_ and add to read_txns_
      if (txn_waits_[update_txn] == 0) {
        ready_txns_->push_back(update_txn);
        txn_waits_.erase(update_txn);
      }
    }
  }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key &key, std::vector<Txn *> *owners) {
  // Implement this method!
  if (lock_table_.find(key) == lock_table_.end()) {
    return UNLOCKED;
  }

  owners->clear();

  std::deque<LockRequest> *lr_queue = lock_table_[key];

  LockMode mode;

  if (lr_queue->size() == 0) {
    // empty queue
    mode = LockMode::UNLOCKED;
  } else if (lr_queue->at(0).mode_ == LockMode::EXCLUSIVE) {
    mode = LockMode::EXCLUSIVE;
    owners->push_back(lr_queue->at(0).txn_);
  } else {
    mode = LockMode::SHARED;
    for (size_t i = 0; i < lr_queue->size(); i++) {
      LockRequest lr = lr_queue->at(i);
      if (lr.mode_ == LockMode::EXCLUSIVE) {
        break;
      } else {
        owners->push_back(lr.txn_);
      }
    }
  }
  return mode;
}
