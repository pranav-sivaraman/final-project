#ifndef PROCESS_POOL_LAUNCHER_H_
#define PROCESS_POOL_LAUNCHER_H_

#include <semaphore.h>

#include "launcher.h"
#include "txn/request.h"

#define RQST_BUF_SZ (1 << 10) /* 1K per request */

struct proc_state;

struct proc_mgr
{
    volatile uint32_t *num_idle_procs_;     /* # idle processes */
    pthread_mutex_t *num_idle_procs_mutex_; /* mutex exclusive lock for idle processes */
    pthread_cond_t *num_idle_procs_cond_;   /* conditional variable for idle processes */

    pthread_mutex_t *pool_mutex_; /* locks the pool below */
    proc_state *pool_;            /* process pool */
};

struct proc_state
{
    Request *request_; /* request location */

    pthread_mutex_t *proc_mutex_; /* proc mutex exclusive lock */
    pthread_cond_t *proc_cond_;   /* proc conditional variable */
    bool *proc_ready_;             /* notify proc of a request */
    uint32_t *proc_id_;              /* id to identify the process */

    proc_mgr *launcher_state_;         /* global pool mgmt state */
    volatile uint64_t *txns_executed_; /* ptr to txn executed counter */
    volatile uint64_t *high_priority_txns_executed_; /* ptr to high-priority txn executed counter */
    proc_state *next_;             /* links proc states */
};

class ProcessPoolLauncher : public Launcher
{
   private:
    uint32_t pool_sz_;
    proc_mgr *launcher_state_;

    static void ExecutorFunc(proc_state *st); /* executed by pooled procs */

   public:
    ProcessPoolLauncher(uint32_t pool_sz);
    ~ProcessPoolLauncher();
    void ExecuteRequest(Request *req);
};

#endif  // PROCESS_POOL_LAUNCHER_H_
