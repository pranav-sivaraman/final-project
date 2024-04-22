#include "launcher.h"

#include <stddef.h>
#include <string.h>
#include <utils.h>
#include <cassert>

Launcher::Launcher()
{
    txns_executed_ = (volatile uint64_t *)mmap(NULL, CACHE_LINE_SZ, PROT_FLAGS, MAP_FLAGS, 0, 0);
    assert((void *)txns_executed_ != MAP_FAILED);
    memset((void *)txns_executed_, 0x0, CACHE_LINE_SZ);
    assert(*txns_executed_ == 0);

    high_priority_txns_executed_ = (volatile uint64_t *)mmap(NULL, CACHE_LINE_SZ, PROT_FLAGS, MAP_FLAGS, 0, 0);
    assert((void *)high_priority_txns_executed_ != MAP_FAILED);
    memset((void *)high_priority_txns_executed_, 0x0, CACHE_LINE_SZ);
    assert(*high_priority_txns_executed_ == 0);

    num_requests_ = 0;
}

Launcher::~Launcher()
{
    int err;
    err = munmap((void *)txns_executed_, CACHE_LINE_SZ);
    assert(err == 0);
    err = munmap((void *)high_priority_txns_executed_, CACHE_LINE_SZ);
    assert(err == 0);
}

uint64_t Launcher::ReadTxnsExecuted()
{
    uint64_t num_executed;
    barrier();
    num_executed = *txns_executed_;
    barrier();
    return num_executed;
}

uint64_t Launcher::ReadHighPriorityTxnsExecuted()
{
    uint64_t num_high_priority_executed;
    barrier();
    num_high_priority_executed = *high_priority_txns_executed_;
    barrier();
    return num_high_priority_executed;
}

void Launcher::IncrDoneTxns(volatile uint64_t *done_ptr) { fetch_and_increment(done_ptr); }
void Launcher::ExecuteRequest(__attribute__((unused)) Request *req) { num_requests_ += 1; }
void Launcher::WaitOutstanding()
{
    while (true)
    {
        barrier();
        if (*txns_executed_ == num_requests_) break;
        barrier();
        asm volatile("pause;" :::);
    }
}
