#ifndef PERF_MONITOR_H_
#define PERF_MONITOR_H_

#include <time.h>

#include "launcher/launcher.h"

#define EXPT_LEN 60

class PerfMonitor
{
   private:
    volatile uint64_t *done_;
    Launcher *lnchr_;
    double *results_;
    double *high_priority_results_;
    uint64_t prev_txns_elapsed_;
    uint64_t prev_high_priority_txns_elapsed_;
    timespec prev_time_elapsed_;

    static timespec DiffTime(timespec end, timespec begin);
    static double TimespecSeconds(timespec t);
    void Run();

   public:
    PerfMonitor(double *results, double *high_priority_results, volatile uint64_t *done, Launcher *lnchr);
    static void *ExecuteThread(void *arg);
};

#endif  // PERF_MONITOR_H_
