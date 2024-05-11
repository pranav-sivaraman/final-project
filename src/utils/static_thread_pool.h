#ifndef _DB_UTILS_STATIC_THREAD_POOL_H_
#define _DB_UTILS_STATIC_THREAD_POOL_H_

#include <atomic>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "assert.h"
#include "stdlib.h"
#include "utils/atomic.h"
#include "utils/thread_pool.h"

class StaticThreadPool : public ThreadPool {
public:
  StaticThreadPool(int nthreads) : thread_count_(nthreads), stopped_(false) {
    threads_.resize(nthreads);
    queues_.resize(nthreads);

    for (int i = 0; i < nthreads; i++) {
      threads_[i] = std::thread{&StaticThreadPool::RunThread, this, i};
    }
  }
  ~StaticThreadPool() {
    stopped_ = true;
    for (int i = 0; i < thread_count_; i++) {
      threads_[i].join();
    }
  }

  bool Active() { return !stopped_; }

  virtual void AddTask(Task &&task) {
    assert(!stopped_);
    while (!queues_[rand() % thread_count_].PushNonBlocking(
        std::forward<Task>(task))) {
    }
  }

  virtual void AddTask(const Task &task) {
    assert(!stopped_);
    while (!queues_[rand() % thread_count_].PushNonBlocking(task)) {
    }
  }

  virtual int ThreadCount() { return thread_count_; }

private:
  // Function executed by each pthread.
  void RunThread(int queue_id) {
    Task task;
    int sleep_duration = 1; // in microseconds
    while (true) {
      if (this->queues_[queue_id].PopNonBlocking(&task)) {
        task();
        // Reset backoff.
        sleep_duration = 1;
      } else {
        usleep(sleep_duration);
        // Back off exponentially.
        if (sleep_duration < 32)
          sleep_duration *= 2;
      }

      if (this->stopped_.load(std::memory_order_relaxed)) {
        // Go through ALL queues looking for a remaining task.
        while (this->queues_[queue_id].Pop(&task)) {
          task();
        }

        break;
      }
    }
  }

  int thread_count_;
  std::vector<std::thread> threads_;

  // Task queues.
  std::vector<AtomicQueue<Task>> queues_;

  std::atomic<bool> stopped_;
};

#endif // _DB_UTILS_STATIC_THREAD_POOL_H_
