#include "perf_monitor.h"

#include <unistd.h>
#include <utils.h>
#include <cassert>

PerfMonitor::PerfMonitor(double *results, double *high_priority_results, volatile uint64_t *done, Launcher *lnchr)
{
    results_ = results;
    high_priority_results_ = high_priority_results;
    done_    = done;
    lnchr_   = lnchr;
}

timespec PerfMonitor::DiffTime(timespec end, timespec start)
{
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0)
    {
        temp.tv_sec  = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    }
    else
    {
        temp.tv_sec  = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}

void *PerfMonitor::ExecuteThread(void *arg)
{
    PerfMonitor *monitor;
    monitor = (PerfMonitor *)arg;
    monitor->Run();
    return NULL;
}

double PerfMonitor::TimespecSeconds(timespec t)
{
    double elapsed_sec;
    elapsed_sec = t.tv_sec + t.tv_nsec / 1000000000.0;
    return elapsed_sec;
}

void PerfMonitor::Run()
{
    assert(*done_ == 0);
    uint64_t total_executed, interval_executed;
    uint64_t total_high_priority_executed, interval_high_priority_executed;
    timespec now, interval;
    uint32_t i;
    double elapsed_sec;

    prev_txns_elapsed_ = lnchr_->ReadTxnsExecuted();
    prev_high_priority_txns_elapsed_ = lnchr_->ReadHighPriorityTxnsExecuted();
    clock_gettime(CLOCK_REALTIME, &prev_time_elapsed_);
    barrier();
    for (i = 0; i < EXPT_LEN; ++i)
    {
        sleep(1);
        total_executed               = lnchr_->ReadTxnsExecuted();
        total_high_priority_executed = lnchr_->ReadHighPriorityTxnsExecuted();
        clock_gettime(CLOCK_REALTIME, &now);
        barrier();

        interval                        = DiffTime(now, prev_time_elapsed_);
        interval_executed               = total_executed - prev_txns_elapsed_;
        interval_high_priority_executed = total_high_priority_executed - prev_high_priority_txns_elapsed_;


        elapsed_sec               = TimespecSeconds(interval);
        results_[i]               = interval_executed / elapsed_sec;
        high_priority_results_[i] = interval_high_priority_executed / elapsed_sec;

        prev_txns_elapsed_               = total_executed;
        prev_high_priority_txns_elapsed_ = total_high_priority_executed;
        prev_time_elapsed_               = now;
    }
    assert(*done_ == 0);
    fetch_and_increment(done_);
}
