#include "process_pool_launcher.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <utils.h>
#include <cassert>
#include <iostream>

ProcessPoolLauncher::ProcessPoolLauncher(uint32_t nprocs) : Launcher()
{
    uint32_t i;
    char *req_bufs;
    proc_state *pstates;

    /*
     * Setup launcher state. Initialize the launcher's proc_mgr struct.
     */
    launcher_state_ = (proc_mgr *)mmap(NULL, sizeof(proc_mgr), PROT_FLAGS, MAP_FLAGS, 0, 0);

    /*
     * YOUR CODE HERE
     *
     * Initialize launcher_state_->num_idle_procs_, num_idle_procs_mutex_ and
     * num_idle_procs_cond_ with an appropriate starting value/attributes.
     * num_idle_procs_ is used to track the number of idle processes.
     * ProcessPoolLauncher's use of num_idle_procs_ is similar to
     * ProcessLauncher's use of max_outstanding_ to control the number
     * of outstanding processes.
     *
     * Hint: Since they are shared among multiple processes, their
     * argument must be set appropriately.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);

    /*
     * YOUR CODE HERE
     *
     * pool_mutex_ is used as a mutex lock. It protects the pool of
     * idle processes from concurrent modifications.
     * Initialize pool_mutex_ with an appropriate starting value.
     *
     * Hint: Since pool_mutex_ is shared among multiple processes, its
     * second argument must be set appropriately.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);

    /*
     * Setup request buffers. Each process in the pool has its own private
     * request buffer. In order to assign a request to a process the
     * launcher process must copy the request into the process' request
     * buffer.
     */
    req_bufs = (char *)mmap((NULL), nprocs * RQST_BUF_SZ, PROT_FLAGS, MAP_FLAGS, 0, 0);

    /*
     * YOUR CODE HERE
     *
     * Each process in the pool has a corresponding mutex, conditional variable
     * and value (procs_ready) which are used to signal the process to begin executing
     * a new request.
     *
     * Initialize procs_ready, proc_id, procs_mutex and procs_cond
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    bool *procs_ready            = NULL;
    uint32_t *procs_id           = NULL;
    pthread_mutex_t *procs_mutex = NULL;
    pthread_cond_t *procs_cond   = NULL;
    assert(false);

    /*
     * Setup proc_states for each process in the pool. proc_states are
     * linked via the next_ field. The free-list of proc_states is
     * stored in launcher_state_->pool_.
     */
    pstates = (proc_state *)mmap(NULL, sizeof(proc_state) * nprocs, PROT_FLAGS, MAP_FLAGS, 0, 0);
    for (i = 0; i < nprocs; ++i)
    {
        pstates[i].request_                     = (Request *)&req_bufs[i * RQST_BUF_SZ];
        pstates[i].proc_ready_                  = &procs_ready[i];
        pstates[i].proc_id_                     = &procs_id[i];
        pstates[i].proc_mutex_                  = &procs_mutex[i];
        pstates[i].proc_cond_                   = &procs_cond[i];
        pstates[i].launcher_state_              = launcher_state_;
        pstates[i].txns_executed_               = txns_executed_;
        pstates[i].high_priority_txns_executed_ = high_priority_txns_executed_;
        pstates[i].next_                        = &pstates[i + 1];
    }
    pstates[i - 1].next_    = NULL;
    launcher_state_->pool_ = pstates;

    /*
     * YOUR CODE HERE
     *
     * Launch the processes in the process pool.
     * Also, remember to update pool_sz_
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);
}

ProcessPoolLauncher::~ProcessPoolLauncher()
{
    int err;
    err = munmap((void *)launcher_state_->num_idle_procs_, sizeof(uint32_t));
    assert(err == 0);

    pthread_mutex_destroy(launcher_state_->num_idle_procs_mutex_);
    err = munmap((void *)launcher_state_->num_idle_procs_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    pthread_cond_destroy(launcher_state_->num_idle_procs_cond_);
    err = munmap((void *)launcher_state_->num_idle_procs_cond_, sizeof(pthread_cond_t));
    assert(err == 0);

    pthread_mutex_destroy(launcher_state_->pool_mutex_);
    err = munmap((void *)launcher_state_->pool_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    proc_state* pstate = launcher_state_->pool_;
    while (pstate != NULL)
    {
        err = munmap((void *)pstate->proc_ready_, sizeof(bool));
        assert(err == 0);

        err = munmap((void *)pstate->proc_id_, sizeof(uint32_t));
        assert(err == 0);

        pthread_mutex_destroy(pstate->proc_mutex_);
        err = munmap((void *)pstate->proc_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_cond_destroy(pstate->proc_cond_);
        err = munmap((void *)pstate->proc_cond_, sizeof(pthread_cond_t));
        assert(err == 0);

        err = munmap((void *)pstate->request_, RQST_BUF_SZ);
        assert(err == 0);

        proc_state* next = pstate->next_;
        err = munmap((void *)pstate, sizeof(proc_state));
        assert(err == 0);
        pstate = next;
    }

    err = munmap((void *)launcher_state_, sizeof(proc_mgr));
    assert(err == 0);
}

void ProcessPoolLauncher::ExecutorFunc(proc_state *st)
{
    while (true)
    {
        /*
         * YOUR CODE HERE
         *
         * Wait for a new request, and execute it. After executing the
         * request return proc_state to the launcher's proc_state
         * pool. Remember to signal/wait the appropriate proc_ready_,
         * and num_idle_procs_.
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */
        assert(false);

        st->request_->Execute();
        fetch_and_increment(st->txns_executed_);
        if (st->request_->IsHighPriority()) fetch_and_increment(st->high_priority_txns_executed_);

        /*
         * YOUR CODE HERE
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */
        assert(false);
    }
    exit(0);
}

void ProcessPoolLauncher::ExecuteRequest(Request *req)
{
    proc_state *st;
    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    st = NULL;
    /*
     * YOUR CODE HERE
     *
     * Find an idle process from pool_, **copy** the request into the
     * process' request buffer, and execute the request on the idle process.
     *
     * Hint: Use the process' proc_ready_, and the launcher's num_idle_procs_
     * to initiate a new request on the idle process, and ensure that there
     * exist idle processes.
     * 
     * Hint: Use the API from txn/request.h to copy the request.
     * 
     * Hint: Remeber to use the process with proc_id_ = 0 for high priority requests.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);
}
