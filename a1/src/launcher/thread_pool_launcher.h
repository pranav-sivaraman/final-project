#ifndef THREAD_POOL_LAUNCHER_H_
#define THREAD_POOL_LAUNCHER_H_

#include <pthread.h>
#include <semaphore.h>

#include "launcher.h"

struct thread_state
{
    Request *req_; /* request to process */

    pthread_mutex_t thread_ready_mutex_; /* thread mutex exclusive lock */
    pthread_cond_t thread_ready_cond_;   /* thread conditional variable */
    bool thread_ready_;                  /* notify thread of a request */

    uint32_t *num_idle_threads_;              /* # idle threads */
    pthread_mutex_t *num_idle_threads_mutex_; /* mutex exclusive lock for idle threads */
    pthread_cond_t *num_idle_threads_cond_;   /* conditional variable for idle threads */

    pthread_t *thread_id_;        /* identifier of current thread */
    pthread_mutex_t *pool_mutex_; /* lock thread pool */
    thread_state **pool_;         /* ptr to launcher's pool */
    thread_state *next_;          /* next thread_state in the pool */

    volatile uint64_t *txns_executed_; /* ptr to txn executed counter */
};

class ThreadPoolLauncher : public Launcher
{
   private:
    uint32_t pool_sz_; /* pool size */

    uint32_t num_idle_threads_;              /* # idle threads */
    pthread_mutex_t num_idle_threads_mutex_; /* mutex exclusive lock for idle threads */
    pthread_cond_t num_idle_threads_cond_;   /* conditional variable for idle threads */

    pthread_mutex_t pool_mutex_; /* pool lock */
    thread_state *pool_;         /* pool of idle threads */

    /* Executed by threads in the thread pool */
    static void *ExecutorFunc(void *arg);

   public:
    ThreadPoolLauncher(int pool_sz);
    ~ThreadPoolLauncher();
    void ExecuteRequest(Request *req);
};

#endif  // THREAD_POOL_LAUNCHER_H_
