#include "thread_launcher.h"

#include <stdlib.h>
#include <utils.h>
#include <cassert>

ThreadLauncher::ThreadLauncher(int max_outstanding) : Launcher()
{
    /*
     * max_outstanding_ ensures that the number of outstanding requests never
     * exceeds max_outstanding. The ProcessLauncher class' max_outstanding_ field
     * serves the exact same purpose.
     */
    max_outstanding_       = max_outstanding;
    max_outstanding_mutex_ = PTHREAD_MUTEX_INITIALIZER;
    max_outstanding_cond_  = PTHREAD_COND_INITIALIZER;

    /*
     * targ_list_mutex_ is used as a mutex lock. It is used to protect the
     * value of targ_list_, which is a linked-list of thread_args
     * corresponding to threads (or requests) that have finished executing.
     *
     * targ_list_ is initialized to NULL.
     */
    targ_list_mutex_ = PTHREAD_MUTEX_INITIALIZER;
    targ_list_       = NULL;
}

void ThreadLauncher::ExecuteRequest(Request *req)
{
    pthread_t *thread;
    thread_arg *arg, *temp;
    int err;

    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    /*
     * Create a new thread to assign the request to.
     */
    thread = (pthread_t *)malloc(sizeof(pthread_t));

    /* Setup the thread's thread_arg struct. */
    arg = GenThreadArg(req, thread, &max_outstanding_, &max_outstanding_cond_, &max_outstanding_mutex_,
                       &targ_list_mutex_, &targ_list_, txns_executed_);

    /* Create a thread to execute the request */
    err = pthread_create(thread, NULL, ThreadLauncher::ExecutorFunc, arg);
    assert(err == 0);

    /*
     * Wait until the value of max_outstanding_ exceeds 0.
     */
    pthread_mutex_lock(&max_outstanding_mutex_);
    while (max_outstanding_ <= 0)
    {
        pthread_cond_wait(&max_outstanding_cond_, &max_outstanding_mutex_);
    }
    --max_outstanding_;
    pthread_mutex_unlock(&max_outstanding_mutex_);

    /*
     * targ_list_ is a linked-list of thread_args corresponding to requests
     * that have finished executing. Free up the memory we allocated for
     * these args.
     *
     * Note that targ_list_mutex_ performs a very different function from that
     * of max_outstanding_mutex_. We use targ_list_mutex_ as a mutual exclusion
     * lock to protect targ_list_, which guarantees that only a single thread can ever
     * execute code between calls to pthread_mutex_lock and pthread_mutex_unlock.
     */
    pthread_mutex_lock(&targ_list_mutex_);
    arg        = targ_list_;
    targ_list_ = NULL;
    pthread_mutex_unlock(&targ_list_mutex_);
    while (arg != NULL)
    {
        temp = arg;
        arg  = arg->next_;
        pthread_join(*temp->thread_id_, NULL);
        free(temp->thread_id_);
        free(temp);
    }
}

ThreadLauncher::~ThreadLauncher()
{
    pthread_mutex_destroy(&max_outstanding_mutex_);
    pthread_cond_destroy(&max_outstanding_cond_);
    pthread_mutex_destroy(&targ_list_mutex_);

    while (targ_list_ != NULL)
    {
        pthread_join(*targ_list_->thread_id_, NULL);
        targ_list_ = targ_list_->next_;
    }
}

/*
 * ExecutorFunc is executed by each thread created to execute a request.
 */
void *ThreadLauncher::ExecutorFunc(void *arg)
{
    thread_arg *targ;
    Request *req;

    targ = (thread_arg *)arg;
    req  = targ->request_;

    /* Execute the request */
    req->Execute();

    /*
     * Return the thread_arg back to the launcher thread by linking it into
     * targ_list_. As in ThreadLauncher::ExecuteRequest, targ_list_mutex_
     * is used as a mutex lock to protect targ_list_ from concurrent
     * modifications.
     */
    pthread_mutex_lock(targ->targ_list_mutex_);
    targ->next_       = *targ->targ_list_;
    *targ->targ_list_ = targ;
    pthread_mutex_unlock(targ->targ_list_mutex_);

    /*
     * Notify the launcher thread that the request has finished executing.
     * Recall that max_outstanding_ is used to enforce the requirement that the
     * number of outstanding requests does not exceed max_outstanding_.
     */
    pthread_mutex_lock(targ->max_outstanding_mutex_);
    ++*targ->max_outstanding_;
    pthread_cond_signal(targ->max_outstanding_cond_);
    pthread_mutex_unlock(targ->max_outstanding_mutex_);

    /* Atomically increment the number of executed transactions */
    fetch_and_increment(targ->txns_executed_);
    return NULL;
}

/*
 * Allocate and intialize a thread_arg struct. This function is simply allows
 * the caller to succinctly initialize a thread_arg.
 */
thread_arg *ThreadLauncher::GenThreadArg(Request *req, pthread_t *thread_id, uint32_t *outstanding,
                                         pthread_cond_t *outstanding_cond, pthread_mutex_t *outstanding_mutex,
                                         pthread_mutex_t *targ_list_mutex, thread_arg **targ_list,
                                         volatile uint64_t *txns_executed)
{
    thread_arg *arg;

    arg                         = (thread_arg *)malloc(sizeof(thread_arg));
    arg->request_               = req;
    arg->thread_id_             = thread_id;
    arg->max_outstanding_       = outstanding;
    arg->max_outstanding_cond_  = outstanding_cond;
    arg->max_outstanding_mutex_ = outstanding_mutex;
    arg->targ_list_mutex_       = targ_list_mutex;
    arg->targ_list_             = targ_list;
    arg->next_                  = NULL;
    arg->txns_executed_         = txns_executed;

    return arg;
}
