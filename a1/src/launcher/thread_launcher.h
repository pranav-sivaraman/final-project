#ifndef THREAD_LAUNCHER_H_
#define THREAD_LAUNCHER_H_

#include <pthread.h>
#include <deque>

#include "launcher.h"

/*
 * thread_args are used to communicate requests and coordination info between
 * the launcher thread and request execution threads.
 */
struct thread_arg
{
    Request *request_;     /* ptr to request to execute */
    pthread_t *thread_id_; /* thread id of the execution thread */

    uint32_t *max_outstanding_;              /* max outstanding requests */
    pthread_mutex_t *max_outstanding_mutex_; /* mutex exclusive lock for max outstanding */
    pthread_cond_t *max_outstanding_cond_;   /* conditional variable for max outstanding */

    pthread_mutex_t *targ_list_mutex_; /* ptr to launcher's targ_list_sem_ semaphore */
    thread_arg *next_;                 /* links together entries in targ_list_ */
    thread_arg **targ_list_;           /* points to launcher's list of executed requests */

    volatile uint64_t *txns_executed_; /* ptr to txns executed counter */
};

class ThreadLauncher : public Launcher
{
   private:
    uint32_t max_outstanding_;              /* Ensures that the max outstanding requests stays bounded */
    pthread_mutex_t max_outstanding_mutex_; /* mutex exclusive lock for max outstanding */
    pthread_cond_t max_outstanding_cond_;   /* conditional variable for max outstanding */
    pthread_mutex_t targ_list_mutex_;       /* Protects the list of executed thread_args below */
    thread_arg *targ_list_;                 /* Linked-list of executed thread_args */

    /* Function executed by a spawned thread */
    static void *ExecutorFunc(void *arg);

    /* Convenience function to intialize a thread_arg */
    static thread_arg *GenThreadArg(Request *req, pthread_t *thread_id, uint32_t *outstanding,
                                    pthread_cond_t *outstanding_cond, pthread_mutex_t *outstanding_mutex,
                                    pthread_mutex_t *targ_list_mutex, thread_arg **targ_list,
                                    volatile uint64_t *txns_executed);

   public:
    ThreadLauncher(int num_outstanding);

    ~ThreadLauncher();

    /* Execute a new request */
    void ExecuteRequest(Request *req);
};

#endif  // THREAD_LAUNCHER_H_
