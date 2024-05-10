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
  bool granted_immediately = false;
  if (lock_table_.find(key) == lock_table_.end() || lock_table_[key]->empty()) {
    granted_immediately = true;
  } else {
    if (txn_waits_.find(txn) == txn_waits_.end()) {
      txn_waits_[txn] = 0;
    }
    txn_waits_[txn]++;
  }

  if (lock_table_.find(key) == lock_table_.end()) {
    lock_table_[key] = new std::deque<LockRequest>();
  }
  LockRequest new_request(EXCLUSIVE, txn);
  lock_table_[key]->push_back(new_request);

  return granted_immediately;
}

bool LockManagerB::ReadLock(Txn *txn, const Key &key) {
  bool granted_immediately = false;
  if (lock_table_.find(key) == lock_table_.end() || lock_table_[key]->empty()) {
    granted_immediately = true;
  } else {
    std::deque<LockRequest> *locks_deque = lock_table_[key];
    bool found_exclusive = false;
    for (auto request : *locks_deque) {
      if (request.mode_ == EXCLUSIVE) {
        found_exclusive = true;
        break;
      }
    }
    if (found_exclusive) {
      if (txn_waits_.find(txn) == txn_waits_.end()) {
        txn_waits_[txn] = 0;
      }
      txn_waits_[txn]++;
    } else {
      granted_immediately = true;
    }
  }

  if (lock_table_.find(key) == lock_table_.end()) {
    lock_table_[key] = new std::deque<LockRequest>();
  }
  LockRequest new_request(SHARED, txn);
  lock_table_[key]->push_back(new_request);

  return granted_immediately;
}

void LockManagerB::Release(Txn *txn, const Key &key) {
  std::deque<LockRequest> *locks_deque = lock_table_[key];
  std::vector<Txn *> owners;
  LockMode curr_mode = Status(key, &owners);
  bool has_lock = false;

  for (auto owner : owners) {
    if (owner == txn) {
      has_lock = true;
      break;
    }
  }

  for (auto it = locks_deque->begin(); it != locks_deque->end();) {
    if (it->txn_ == txn) {
      it = locks_deque->erase(it);
    } else {
      it++;
    }
  }

  if (has_lock && !locks_deque->empty()) {
    LockRequest next_request = locks_deque->front();
    if (next_request.mode_ == EXCLUSIVE) {
      if (txn_waits_.find(next_request.txn_) != txn_waits_.end() &&
          --txn_waits_[next_request.txn_] == 0) {
        txn_waits_.erase(next_request.txn_);
        ready_txns_->push_back(next_request.txn_);
      }
    } else {
      Status(key, &owners);
      for (auto txn : owners) {
        if (txn_waits_.find(txn) != txn_waits_.end() &&
            --txn_waits_[txn] == 0) {
          txn_waits_.erase(txn);
          ready_txns_->push_back(txn);
        }
      }
    }
  } else {
    if (curr_mode == SHARED) {
      // Checking if the curr shared prefix has gotten longer due to cancel
      Status(key, &owners);
      for (auto txn : owners) {
        if (txn_waits_.find(txn) != txn_waits_.end() &&
            --txn_waits_[txn] == 0) {
          txn_waits_.erase(txn);
          ready_txns_->push_back(txn);
        }
      }
    }
  }
}

// NOTE: The owners input std::vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key &key, std::vector<Txn *> *owners) {
  if (lock_table_.find(key) == lock_table_.end() || lock_table_[key]->empty()) {
    return UNLOCKED;
  } else {
    std::deque<LockRequest> *locks_deque = lock_table_[key];
    owners->clear();

    if (locks_deque->front().mode_ == EXCLUSIVE) {
      owners->push_back(locks_deque->front().txn_);
      return EXCLUSIVE;
    } else {
      for (auto request : *locks_deque) {
        if (request.mode_ == SHARED) {
          owners->push_back(request.txn_);
        } else {
          break;
        }
      }
      return SHARED;
    }
  }
}
