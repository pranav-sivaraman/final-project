#include "thread_pool_launcher.h"

#include <stdlib.h>
#include <unistd.h>
#include <utils.h>
#include <cassert>
#include <iostream>

ThreadPoolLauncher::ThreadPoolLauncher(int pool_sz) : Launcher()
{
    assert(pool_sz > 0);

    int i;
    pthread_t *thread;
    thread_state *states;

    /* Initialize ThreadPoolLauncher fields */
    pool_sz_ = pool_sz;

    /*
     * YOUR CODE HERE
     *
     * Initialize num_idle_threads_ (num_idle_threads_mutex_ and num_idle_threads_cond_)
     * and pool_mutex_.
     *
     * pool_mutex_ protects the pool of thread_states, which
     * corresponds to the list of idle threads.
     *
     * num_idle_threads_ is used to track the number of idle/busy processes.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);

    /*
     * Initialize thread_state structs. Each thread in the thread pool is
     * assigned a thread_state struct.
     */
    states = (thread_state *)malloc(sizeof(thread_state) * pool_sz);
    for (i = 0; i < pool_sz; ++i)
    {
        thread = (pthread_t *)malloc(sizeof(pthread_t));

        /*
         * Each thread in the pool has a corresponding semaphore which
         * is used to signal the thread to begin executing a new
         * request. We initialize the value of this semaphore to 0.
         */
        states[i].thread_ready_cond_      = PTHREAD_COND_INITIALIZER;
        states[i].thread_ready_mutex_     = PTHREAD_MUTEX_INITIALIZER;
        states[i].thread_ready_           = false;
        states[i].req_                    = NULL;
        states[i].num_idle_threads_       = &num_idle_threads_;
        states[i].num_idle_threads_mutex_ = &num_idle_threads_mutex_;
        states[i].num_idle_threads_cond_  = &num_idle_threads_cond_;
        states[i].thread_id_              = thread;
        states[i].pool_mutex_             = &pool_mutex_;
        states[i].txns_executed_          = txns_executed_;
        states[i].next_                   = &states[i + 1];
        states[i].pool_                   = &pool_;
    }
    states[i - 1].next_ = NULL;
    pool_          = states;

    /*
     * YOUR CODE HERE
     *
     * Create threads in the thread pool.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);
}

ThreadPoolLauncher::~ThreadPoolLauncher()
{
    int err;

    pthread_mutex_destroy(&num_idle_threads_mutex_);
    err = munmap((void *)&num_idle_threads_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    pthread_cond_destroy(&num_idle_threads_cond_);
    err = munmap((void *)&num_idle_threads_cond_, sizeof(pthread_cond_t));
    assert(err == 0);

    pthread_mutex_destroy(&pool_mutex_);
    err = munmap((void *)&pool_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    while (pool_ != NULL)
    {
        pthread_mutex_destroy(&num_idle_threads_mutex_);
        err = munmap((void *)&num_idle_threads_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_cond_destroy(&num_idle_threads_cond_);
        err = munmap((void *)&num_idle_threads_cond_, sizeof(pthread_cond_t));
        assert(err == 0);

        pthread_mutex_destroy(&pool_mutex_);
        err = munmap((void *)&pool_mutex_, sizeof(pthread_mutex_t));
        assert(err == 0);

        pthread_join(*pool_->thread_id_, NULL);
        pool_ = pool_->next_;
    }
    free(pool_);
}

void ThreadPoolLauncher::ExecuteRequest(Request *req)
{
    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    /*
     * YOUR CODE HERE
     *
     * Find an idle thread from the pool, and execute the request on
     * the idle thread.
     *
     * Hint:
     * 1. Use num_idle_threads_, num_idle_threads_mutex_, and num_idle_threads_cond_
     * 2. Use thread_ready_, thread_ready_mutex_, thread_ready_cond_
     * to coordinate the execution of threads in the pool and the launcher.
     *
     * When your code is ready, remove the assert(false) statement
     * below.
     */
    assert(false);
}

void *ThreadPoolLauncher::ExecutorFunc(void *arg)
{
    thread_state *st;

    st = (thread_state *)arg;
    while (true)
    {
        /*
         * YOUR CODE HERE
         *
         * Wait for a new request, and execute it. After executing the
         * request return thread_state to the launcher's thread_state
         * pool.
         *
         * When your code is ready, remove the assert(false) statement
         * below.
         */
        assert(false);

        /* exec request */
        st->req_->Execute();
        fetch_and_increment(st->txns_executed_);

        /*
         * YOUR CODE HERE
         */
        assert(false);
    }
    return NULL;
}
