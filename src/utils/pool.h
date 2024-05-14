#include <functional>
#include <thread>
#include <vector>

#include "atomic_queue.h"

using task = std::function<void()>;

#define TRY_FACTOR 10

class thread_pool {
private:
  std::atomic<unsigned> counter_{};
  std::atomic<bool> stopped_ = false;
  unsigned num_threads_{};
  std::vector<atomic_queue<task>> queues_;
  std::vector<std::thread> threads_;

  void run(unsigned tid) {
    while (!stopped_) {
      task f;

      // Check Our Queue
      if (!queues_[tid].pop(f)) {
        for (unsigned i = 0; i < num_threads_; i++) {
          // Try and Steal Other work
          if (queues_[i].try_pop(f)) {
            break;
          }
        }
      }

      if (f)
        f();
    }
  }

public:
  thread_pool(unsigned num_threads)
      : num_threads_(num_threads), queues_(num_threads), threads_(num_threads) {
    for (unsigned i = 0; i < num_threads; i++) {
      threads_[i] = std::thread{&thread_pool::run, this, i};
    }
  }

  ~thread_pool() {
    stopped_ = true;
    for (unsigned i = 0; i < num_threads_; i++) {
      threads_[i].join();
    }
  }

  void AddTask(task &&f) {
    unsigned tid = counter_++;

    // Try to Assign Round Robin
    if (!queues_[tid % num_threads_].try_push(std::forward<task>(f))) {
      for (unsigned i = 0; i < num_threads_; i++) {
        // Try and Push to another queue
        if (queues_[i % num_threads_].try_push(std::forward<task>(f))) {
          return;
        }
      }

      // Otherwise push in order
      queues_[tid % num_threads_].push(std::forward<task>(f));
    }
  }
};
