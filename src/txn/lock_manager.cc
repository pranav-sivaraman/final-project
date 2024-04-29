#include "lock_manager.h"

LockManagerA::LockManagerA(std::deque<Txn *> *ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn *txn, const Key &key) {
  bool granted_immediately = false;
  bool contains_key = lock_table_.contains(key);
  if (!contains_key || lock_table_[key]->empty()) {
    granted_immediately = true;
  } else {
    if (!contains_key) {
      txn_waits_[txn] = 0;
    }
    txn_waits_[txn]++;
  }

  if (!contains_key) {
    lock_table_[key] = new std::deque<LockRequest>();
  }

  LockRequest new_req{EXCLUSIVE, txn};
  lock_table_[key]->push_back(new_req);

  return granted_immediately;
}

bool LockManagerA::ReadLock(Txn *txn, const Key &key) {
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn *txn, const Key &key) {
  std::deque<LockRequest> *locks_deque = lock_table_[key];

  for (auto it = locks_deque->begin(); it != locks_deque->end();) {
    if (it->txn_ == txn) {
      it = locks_deque->erase(it);
    } else {
      it++;
    }
  }

  if (!locks_deque->empty() && txn != locks_deque->front().txn_) {
    Txn *next_txn = locks_deque->front().txn_;
    if (txn_waits_.find(next_txn) != txn_waits_.end() &&
        --txn_waits_[next_txn] == 0) {
      txn_waits_.erase(next_txn);
      ready_txns_->push_back(next_txn);
    }
  } else {
    txn_waits_.erase(txn);
  }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key &key, std::vector<Txn *> *owners) {
  if (!lock_table_.contains(key) || lock_table_[key]->empty()) {
    return UNLOCKED;
  }

  owners->clear();
  owners->push_back(lock_table_[key]->front().txn_);

  return EXCLUSIVE;
}

LockManagerB::LockManagerB(std::deque<Txn *> *ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn *txn, const Key &key) {
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn *txn, const Key &key) {
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn *txn, const Key &key) {
  //
  // Implement this method!
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key &key, std::vector<Txn *> *owners) {
  //
  // Implement this method!
  return UNLOCKED;
}
