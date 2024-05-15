#include "txn_processor.h"
#include "utils/common.h"
#include <chrono>
#include <set>
#include <stdio.h>
#include <unordered_set>

#include "lock_manager.h"

/***********************************************
 *  Calvin Continuous Execution -- Global Locks *
 ***********************************************/
void TxnProcessor::RunCalvinContScheduler() {
  Txn *txn;

  std::unordered_map<Key, std::unordered_set<Txn *>> shared_holders;
  std::unordered_map<Key, Txn *> last_excl;

  while (!stopped_) {
    if (txn_requests_.Pop(&txn)) {

      adj_list_lock.lock();
      indegree_lock.lock();

      adj_list[txn] = {};

      // Print the adj_list in one go so the lines aren't interleaved

      // Don't add to indegree hashmap right away because if indegree == 0,
      // we want to add it to the threadpool right away
      int ind = 0;

      // Loop through writeset
      for (const Key &key : txn->writeset_) {
        // Add an edge between the current txn and all shared holders
        if (shared_holders.contains(key)) {
          for (auto conflicting_txn : shared_holders[key]) {
            if (conflicting_txn != txn &&
                conflicting_txn->Status() == INCOMPLETE &&
                !adj_list[conflicting_txn].contains(txn)) {
              adj_list[conflicting_txn].insert(txn);
              ind++;
            }
          }
          shared_holders[key].clear();
        }

        if (last_excl.contains(key) && last_excl[key] != txn) {
          last_excl[key]->neighbors_mutex.lock();

          if (last_excl[key]->Status() != COMMITTED &&
              last_excl[key]->Status() != ABORTED) {
            // We came in before CalvinExecutorFunc took "snapshot" of
            // neighbors
            txn->indegree++;
            last_excl[key]->neighbors.insert(txn);
          }

          last_excl[key]->neighbors_mutex.unlock();
        }

        last_excl[key] = txn;
      }

      // Loop through readset
      // auto merged_sets = txn->readset_;
      // merged_sets.insert(txn->writeset_.begin(), txn->writeset_.end());
      for (const Key &key : txn->readset_) {
        // Add to shared holders
        if (!shared_holders.contains(key)) {
          shared_holders[key] = {};
        }
        shared_holders[key].insert(txn);

        // If the last_excl txn is not the current txn, add an edge
        if (last_excl.contains(key) && last_excl[key] != txn &&
            last_excl[key]->Status() == INCOMPLETE &&
            !adj_list[last_excl[key]].contains(txn)) {
          adj_list[last_excl[key]].insert(txn);
          ind++;
        }
      }

      // If current transaction's indegree is 0, add it to the threadpool
      if (ind == 0) {
        tp_.AddTask([this, txn]() { this->CalvinContExecutorFunc(txn); });
      } else {
        // Otherwise, add it to the indegree hashmap
        indegree[txn] = ind;
      }
      indegree_lock.unlock();
      adj_list_lock.unlock();
    }
  }
}

void TxnProcessor::CalvinContExecutorFunc(Txn *txn) {
  // Execute txn.
  ExecuteTxn(txn);

  // Commit/abort txn according to program logic's commit/abort decision.
  // Note: we do this within the worker thread instead of returning
  // back to the scheduler thread.

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

  // Update indegrees of neighbors
  // If any has indegree 0, add them back to the queue
  // if (adj_list.find(txn) != adj_list.end()) {
  // std::shared_lock<std::shared_mutex>
  // adj_list_shared_lock(adj_list_lock);
  // std::shared_lock<std::shared_mutex>
  // indegree_shared_lock(indegree_lock);

  adj_list_lock.lock();
  indegree_lock.lock();
  auto neighbors = adj_list[txn];
  for (auto nei : neighbors) {
    indegree[nei]--;
    if (indegree[nei] == 0) {
      tp_.AddTask([this, nei]() { this->CalvinContExecutorFunc(nei); });
    }
  }

  adj_list_lock.unlock();
  indegree_lock.unlock();

  // Return result to client.
  txn_results_.Push(txn);
}

/***********************************************
 *  Calvin Continuous Execution -- Indiv Locks  *
 ***********************************************/
void TxnProcessor::RunCalvinContIndivScheduler() {
  Txn *txn;

  std::unordered_map<Key, std::unordered_set<Txn *>> shared_holders;
  std::unordered_map<Key, Txn *> last_excl;

  while (!stopped_) {
    while (txn_requests_.Pop(&txn)) {
      std::vector<Key> sorted_keys;
      sorted_keys.reserve(txn->readset_.size() + txn->writeset_.size());
      sorted_keys.insert(sorted_keys.end(), txn->readset_.begin(),
                         txn->readset_.end());
      sorted_keys.insert(sorted_keys.end(), txn->writeset_.begin(),
                         txn->writeset_.end());
      std::sort(sorted_keys.begin(), sorted_keys.end());

      txn->indegree_mutex.lock();
      txn->indegree = 0;
      txn->neighbors.clear();

      // Handle writeset
      for (const Key &key : txn->writeset_) {
        if (shared_holders.contains(key)) {
          for (auto conflicting_txn : shared_holders[key]) {
            if (conflicting_txn != txn) {
              if (conflicting_txn->neighbors_mutex.try_lock()) {

                if (conflicting_txn->Status() != COMMITTED &&
                    conflicting_txn->Status() != ABORTED &&
                    !conflicting_txn->neighbors.contains(txn)) {
                  // We came in before CalvinExecutorFunc took "snapshot" of
                  // neighbors
                  txn->indegree++;
                  conflicting_txn->neighbors.insert(txn);
                }

                conflicting_txn->neighbors_mutex.unlock();
              }
            }
          }
          shared_holders[key].clear();

        }

        else if (last_excl.contains(key) && last_excl[key] != txn) {
          // if (last_excl[key]->neighbors_mutex.try_lock()) {
          if (last_excl[key]->neighbors_mutex.try_lock()) {

            if (last_excl[key]->Status() != COMMITTED &&
                last_excl[key]->Status() != ABORTED &&
                !last_excl[key]->neighbors.contains(txn)) {
              // We came in before CalvinExecutorFunc took "snapshot" of
              // neighbors
              txn->indegree++;
              last_excl[key]->neighbors.insert(txn);
            }

            last_excl[key]->neighbors_mutex.unlock();
          }
        }

        last_excl[key] = txn;
      }

      // Handle readset
      for (const Key &key : txn->readset_) {
        if (!shared_holders.contains(key)) {
          shared_holders[key] = {};
        }
        shared_holders[key].insert(txn);

        if (last_excl.contains(key) && last_excl[key] != txn) {
          if (last_excl[key]->neighbors_mutex.try_lock()) {

            if (last_excl[key]->Status() != COMMITTED &&
                last_excl[key]->Status() != ABORTED &&
                !last_excl[key]->neighbors.contains(txn)) {
              // We came in before CalvinExecutorFunc took "snapshot" of
              // neighbors
              txn->indegree++;
              last_excl[key]->neighbors.insert(txn);
            }

            last_excl[key]->neighbors_mutex.unlock();
          }
        }
      }

      // If current transaction's indegree is 0, add it to the threadpool
      if (txn->indegree == 0) {
        tp_.AddTask([this, txn]() { this->CalvinContIndivExecutorFunc(txn); });
      }
      txn->indegree_mutex.unlock();
    }
  }
}

void TxnProcessor::CalvinContIndivExecutorFunc(Txn *txn) {
  // Execute txn.
  // printf("Starting to execute txn %d\n", txn->unique_id_);
  ExecuteTxn(txn);

  // Commit/abort txn according to program logic's commit/abort decision.
  // Note: we do this within the worker thread instead of returning
  // back to the scheduler thread.
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

  // Update indegrees of neighbors
  // if (txn->neighbors_mutex.try_lock()) {
  txn->neighbors_mutex.lock();
  std::vector<Txn *> sorted_neighbors(txn->neighbors.begin(),
                                      txn->neighbors.end());
  // std::sort(sorted_neighbors.begin(), sorted_neighbors.end());

  for (Txn *nei : sorted_neighbors) {
    // test with trylock here
    nei->indegree_mutex.lock();
    nei->indegree--;
    if (nei->indegree == 0) {
      tp_.AddTask([this, nei]() { this->CalvinContIndivExecutorFunc(nei); });
    }
    nei->indegree_mutex.unlock();
  }

  // Return result to client.
  txn_results_.Push(txn);
  txn->neighbors_mutex.unlock();
}

/***********************************************
 *            Calvin Epoch Execution            *
 ***********************************************/

void TxnProcessor::RunCalvinEpochSequencer() {
  Txn *txn;
  // save time of last epoch for calvin sequencer
  auto last_epoch_time = std::chrono::high_resolution_clock::now();
  // set up current epoch
  Epoch *current_epoch = new Epoch();
  while (!stopped_) {
    // Add the txn to the epoch.
    if (txn_requests_.Pop(&txn)) {
      current_epoch->push(txn);
    }

    // check if we need to close the epoch
    auto curr_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        curr_time - last_epoch_time);
    if (duration.count() > 5) {
      // new epoch is out of scope
      last_epoch_time = curr_time;

      // make new epoch if last epoch has anything in it
      if (!current_epoch->empty()) {
        epoch_queue.Push(current_epoch);
        current_epoch = new Epoch();
      }
    }
  }
}

void *TxnProcessor::calvin_sequencer_helper(void *arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunCalvinEpochSequencer();
  return NULL;
}

void TxnProcessor::RunCalvinEpochScheduler() {

  // Start Calvin Sequencer
  pthread_create(&calvin_sequencer_thread, NULL, calvin_sequencer_helper,
                 reinterpret_cast<void *>(this));
  //   Start Calvin Epoch Executor
  // pthread_create(&calvin_epoch_executor_thread, NULL,
  //                calvin_epoch_executor_helper, reinterpret_cast<void
  //                *>(this));
  // for(int i = 0; i < THREAD_COUNT; i++) {
  //   tp_.AddTask([this]() { this->CalvinEpochExecutorLMAO(); });
  // }

  Epoch *curr_epoch;
  EpochDag *dag;
  while (!stopped_) {
    // Get the next epoch
    if (epoch_queue.Pop(&curr_epoch)) {
      // Create new DAG for this epoch
      std::unordered_map<Key, std::unordered_set<Txn *>> shared_holders;
      std::unordered_map<Key, Txn *> last_excl;

      dag = (EpochDag *)malloc(sizeof(EpochDag));

      std::unordered_map<Txn *, std::unordered_set<Txn *>> *adj_list =
          new std::unordered_map<Txn *, std::unordered_set<Txn *>>();
      std::unordered_map<Txn *, std::atomic<int>> *indegree =
          new std::unordered_map<Txn *, std::atomic<int>>();
      std::queue<Txn *> *root_txns = new std::queue<Txn *>();

      Txn *txn;
      while (!curr_epoch->empty()) {
        txn = curr_epoch->front();
        curr_epoch->pop();
        adj_list->emplace(txn, std::unordered_set<Txn *>());
        indegree->emplace(txn, 0);

        // Loop through readset
        for (const Key &key : txn->readset_) {
          // Add to shared holders
          if (!shared_holders.contains(key)) {
            shared_holders[key] = std::unordered_set<Txn *>();
          }
          shared_holders[key].insert(txn);

          // If the last_excl txn is not the current txn, add an edge

          // add an edge between the last_excl txn and the current txn
          // if we read a key that was last written by last_excl txn
          if (last_excl.contains(key) &&
              !adj_list->at(last_excl[key]).contains(txn)) {
            adj_list->at(last_excl[key]).insert(txn);
            indegree->at(txn)++;
          }
        }
        // Loop through writeset
        for (const Key &key : txn->writeset_) {
          // Add an edge between the current txn and all shared holders
          if (shared_holders.contains(key)) {
            for (auto conflicting_txn : shared_holders[key]) {
              if (!adj_list->at(conflicting_txn).contains(txn)) {
                adj_list->at(conflicting_txn).insert(txn);
                indegree->at(txn)++;
              }
            }
            shared_holders[key].clear();
          }
          last_excl[key] = txn;
        }

        // set as root if indegree of 0
        if (indegree->at(txn) == 0) {
          root_txns->push(txn);
        }
      }
      // finalize new epoch dag
      dag->adj_list = adj_list;
      dag->indegree = indegree;
      dag->root_txns = root_txns;
      //      dag->indegree_locks = indegree_locks;

      // push dag to queue for executor to read
      // epoch_dag_queue.Push(dag);
      CalvinExecuteSingleEpoch(dag);
    }
  }
}

void TxnProcessor::CalvinEpochExecutor() {
  num_txns_left_in_epoch = 0;
  while (!stopped_) {
    if (epoch_dag_queue.Pop(&current_epoch_dag)) {
      if (num_txns_left_in_epoch != 0) {
        std::cout << "Num transactions in epoch: " << num_txns_left_in_epoch
                  << std::endl;
        std::cout << "UH OH--------------------------------UH OH" << std::endl;
      }
      num_txns_left_in_epoch = current_epoch_dag->adj_list->size();
      Txn *txn;
      std::queue<Txn *> *root_txns = current_epoch_dag->root_txns;

      // add all root txns to threadpool
      while (!root_txns->empty()) {
        txn = root_txns->front();
        root_txns->pop();
        tp_.AddTask([this, txn]() { this->CalvinEpochExecutorFunc(txn); });
      }

      // wait for epoch to end executing
      int sleep_duration = 1; // in microseconds
      while (num_txns_left_in_epoch > 0) {
        usleep(sleep_duration);
        // Back off exponentially.
        if (sleep_duration < 32)
          sleep_duration *= 2;
      }
      delete current_epoch_dag->adj_list;
      delete current_epoch_dag->indegree;
      delete current_epoch_dag->root_txns;
      free(current_epoch_dag);
      current_epoch_dag = NULL;
    }
  }
}

void TxnProcessor::CalvinExecuteSingleEpoch(EpochDag *epoch_dag) {
  num_txns_left_in_epoch = 0;
  // if (num_txns_left_in_epoch != 0) {
  //   std::cout << "Num transactions in epoch: " << num_txns_left_in_epoch
  //             << std::endl;
  //   std::cout << "UH OH--------------------------------UH OH" << std::endl;
  // }
  current_epoch_dag = epoch_dag;
  num_txns_left_in_epoch = current_epoch_dag->adj_list->size();
  Txn *txn;
  std::queue<Txn *> *root_txns = current_epoch_dag->root_txns;

  // add all root txns to threadpool
  while (!root_txns->empty()) {
    txn = root_txns->front();
    root_txns->pop();
    tp_.AddTask([this, txn]() { this->CalvinEpochExecutorFunc(txn); });
  }

  // wait for epoch to end executing
  int sleep_duration = 1; // in microseconds
  while (num_txns_left_in_epoch > 0) {
    usleep(sleep_duration);
    // Back off exponentially.
    if (sleep_duration < 32)
      sleep_duration *= 2;
  }
  delete current_epoch_dag->adj_list;
  delete current_epoch_dag->indegree;
  delete current_epoch_dag->root_txns;
  free(current_epoch_dag);
  current_epoch_dag = NULL;
}

void *TxnProcessor::calvin_epoch_executor_helper(void *arg) {
  reinterpret_cast<TxnProcessor *>(arg)->CalvinEpochExecutor();
  return NULL;
}

void TxnProcessor::CalvinEpochExecutorFunc(Txn *txn) {
  ExecuteTxn(txn);

  // Commit/abort txn according to program logic's commit/abort decision.
  // Note: we do this within the worker thread instead of returning
  // back to the scheduler thread.
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

  // Update indegrees of neighbors
  // If any has indegree 0, add them back to the queue
  auto neighbors = current_epoch_dag->adj_list->at(txn);
  for (Txn *blocked_txn : neighbors) {
    if (current_epoch_dag->indegree->at(blocked_txn)-- == 1) {
      tp_.AddTask([this, blocked_txn]() {
        this->CalvinEpochExecutorFunc(blocked_txn);
      });
    }
  }
  num_txns_left_in_epoch--;

  // Return result to client.
  txn_results_.Push(txn);
}
