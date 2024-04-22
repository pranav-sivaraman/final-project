#include "process_launcher.h"

#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <utils.h>
#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <iostream>

ProcessLauncher::ProcessLauncher(int max_outstanding) : Launcher()
{
    assert(max_outstanding < INT_MAX && max_outstanding > 0);

    int err;
    struct sigaction sigchild_action;

    /*
     * Prevent children turning into zombies. When a child exits, we want
     * the operating system to immediately free any resources allocated to
     * the child. Doing this allows us to avoid waiting on a child to free
     * resources.
     */
    sigchild_action.sa_handler = SIG_IGN;
    sigemptyset(&sigchild_action.sa_mask);
    sigchild_action.sa_flags = 0;
    err                      = sigaction(SIGCHLD, &sigchild_action, 0);
    assert(err == 0);

    /* Initialize fields */
    max_outstanding_ = (volatile int *)mmap(NULL, sizeof(int), PROT_FLAGS, MAP_FLAGS, 0, 0);
    memset((void *)max_outstanding_, 0x0, sizeof(int));
    *max_outstanding_ = max_outstanding;

    /*
     * Map max_outstanding_mutex_ and max_outstanding_cond_ into a memory segment that is shared
     * by this process, and any subsequent forked processes. They must be
     * shared between this process and its children. We use them to control
     * the maximum number of in-flight processes (as specified in max_outstanding_).
     * Note that the call to mmap only _allocates_ memory for max_outstanding_mutex_
     * and max_outstanding_cond_. We still need to intialize them appropriately.
     */
    max_outstanding_mutex_ = (pthread_mutex_t *)mmap(NULL, sizeof(pthread_mutex_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    pthread_mutexattr_t mutexattr;
    pthread_mutexattr_init(&mutexattr);
    pthread_mutexattr_setpshared(&mutexattr, PTHREAD_PROCESS_SHARED);
    pthread_mutex_init(max_outstanding_mutex_, &mutexattr);

    max_outstanding_cond_ = (pthread_cond_t *)mmap(NULL, sizeof(pthread_cond_t), PROT_FLAGS, MAP_FLAGS, 0, 0);
    pthread_condattr_t condattr;
    pthread_condattr_init(&condattr);
    pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);
    pthread_cond_init(max_outstanding_cond_, &condattr);
}

ProcessLauncher::~ProcessLauncher()
{
    int err;
    err = munmap((void *)max_outstanding_, sizeof(int));
    assert(err == 0);

    pthread_mutex_destroy(max_outstanding_mutex_);
    err = munmap((void *)max_outstanding_mutex_, sizeof(pthread_mutex_t));
    assert(err == 0);

    pthread_cond_destroy(max_outstanding_cond_);
    err = munmap((void *)max_outstanding_cond_, sizeof(pthread_cond_t));
    assert(err == 0);
}

void ProcessLauncher::ExecuteRequest(Request *req)
{
    /* Child process identifier */
    pid_t pid;

    /*
     * Track the number of requests issued.
     */
    num_requests_++;

    /* fork() creates a new child process.
     *
     * fork() returns 0 to the child process (the code-path corresponding to
     * the "if" branch).
     *
     * fork() returns a non-zero pid to the parent process (the code-path
     * corresponding to the "else" branch).
     */
    if ((pid = fork()) != 0)
    { /* This code is executed in the parent */

        /*
         * Assert that the fork() call did not fail. fork() returns a
         * negative pid on an error.
         */
        assert(pid != -1);

        /*
         * YOUR CODE HERE
         *
         * Wait until the value of max_outstanding_ is greater than
         * 0, and then decrement its value by 1. This guarantees that
         * the number of outstanding requests is always at most
         * max_outstanding_.
         */
    }
    else
    { /* This code is executed in the child */

        /*
         * Execute the request. We treat requests' "execute" function
         * as a black-box. Just make sure it's called at the appropriate
         * place.
         */
        req->Execute();

        /* Atomically increment the number of executed transactions */
        fetch_and_increment(this->txns_executed_);

        /*
         * YOUR CODE HERE
         *
         * Increment the value of max_outstanding_, which tells the
         * parent process that an outstanding request has finished
         * executing.
         */
        
        
        exit(0); /* Why need to exit? */
    }
}
