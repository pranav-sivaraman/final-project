#include <unistd.h>
#include <utils.h>

#include <fstream>
#include <iostream>
#include <set>

#include "db/config.h"

#define TXN_SZ 50
#define LOW_DATABASE_SZ 5000000
#define HIGH_DATABASE_SZ 500
#define DRY_RUN_SZ 1000
#define NUM_REQS 2000000
#define HIGH_PRIORITY_FREQ 1000

const uint32_t rand_seed = 0xdeadbeef;
const char *output_file  = "results.txt";

uint64_t gen_unique(uint64_t max, std::set<uint64_t> *seen)
{
    uint64_t gen;

    while (true)
    {
        gen = ((uint64_t)rand()) % max;
        if (seen->count(gen) == 0)
        {
            seen->insert(gen);
            break;
        }
    }
    assert(seen->count(gen) > 0);
    assert(gen < max);
    return gen;
}

Request *generate_single_request(Database *db, bool high_priority)
{
    std::set<uint64_t> seen_keys;
    uint64_t *writeset, *updates;
    uint32_t i, nfields;
    Request *rqst;

    seen_keys.clear();
    /* Generate writeset */
    writeset = (uint64_t *)malloc(sizeof(uint64_t) * TXN_SZ);
    for (i = 0; i < TXN_SZ; ++i)
    {
        writeset[i] = gen_unique(db->DBSize(), &seen_keys);
    }

    /* Generate updates */
    nfields = RECORD_SIZE / FIELD_SIZE;
    updates = (uint64_t *)malloc(sizeof(uint64_t) * nfields);
    for (i = 0; i < nfields; ++i) updates[i] = (uint64_t)rand();

    /* Generate request */
    rqst = new Request(db, high_priority, TXN_SZ, writeset, updates);
    return rqst;
}

Request **generate_requests(Database *db, bool high_priority, uint32_t num_requests)
{
    Request **ret;
    uint32_t i;

    ret = (Request **)malloc(sizeof(Request *) * num_requests);
    for (i = 0; i < num_requests; ++i)
        ret[i] = generate_single_request(db, (high_priority && (i % HIGH_PRIORITY_FREQ == 0)));
    return ret;
}

pthread_t *run_experiment(PerfMonitor *monitor, Launcher *lnchr, Request ***requests, volatile uint64_t *done_flag)
{
    int err;
    uint32_t i;
    pthread_t *ret;

    ret = (pthread_t *)malloc(sizeof(pthread_t));

    /* Do dry run */
    for (i = 0; i < DRY_RUN_SZ; ++i) lnchr->ExecuteRequest(requests[0][i]);
    lnchr->WaitOutstanding();
    std::cerr << "Done dry run\n";

    /*
     * Run the "real" experiment. We create a new thread to monitor the
     * progress of requests. See include/perf_monitor.h and
     * src/perf_monitor.cc. The PerfMonitor thread samples the number of
     * txns_executed_ counter in the launcher class in one second
     * intervals, and sets the done_flag below after 60 seconds.
     */
    err = pthread_create(ret, NULL, PerfMonitor::ExecuteThread, monitor);
    assert(err == 0);
    barrier();
    i = 0;
    while (true)
    {
        barrier();
        if (*done_flag != 0) break;
        barrier();
        lnchr->ExecuteRequest(requests[1][i % NUM_REQS]);
        i += 1;
    }
    return ret;
}

void run_test(expt_config conf)
{
    uint32_t test_db_sz, num_requests, i;
    Database *db_test, *db_simple;
    bool high_priority;
    bool multiProcess;
    Request **reqs;
    Launcher *test;

    /* Use a small db to guarantee conflicts */
    test_db_sz   = 500;
    multiProcess = (conf._type == PROCESS_POOL || conf._type == PROCESS);

    /* Create database */
    db_test   = Database::Create(test_db_sz, multiProcess);
    db_simple = Database::Create(test_db_sz, multiProcess);
    Database::Copy(db_simple, db_test);

    /* Gen requests */
    num_requests  = 10000;
    high_priority = false;
    reqs          = generate_requests(db_test, high_priority, num_requests);

    /* Create launcher */
    switch (conf._type)
    {
        case PROCESS_POOL:
            test = new ProcessPoolLauncher(conf._pool_size);
            break;
        case PROCESS:
            test = new ProcessLauncher(conf.max_outstanding_);
            break;
        case THREAD:
            test = new ThreadLauncher(conf.max_outstanding_);
            break;
        case THREAD_POOL:
            test = new ThreadPoolLauncher(conf._pool_size);
            break;
        default:
            assert(false); /* Shouldn't get here */
    }

    sleep(1);

    /* Run test */
    for (i = 0; i < num_requests; ++i) test->ExecuteRequest(reqs[i]);
    test->WaitOutstanding();

    /* Switch database */
    for (i = 0; i < num_requests; ++i) reqs[i]->SetDatabase(db_simple);

    /* Run sequential test */
    for (i = 0; i < num_requests; ++i) reqs[i]->Execute();

    /* Compare databases to ensure they're the same */
    Database::Compare(db_test, db_simple);
    std::cerr << "Test passed!\n";
}

void write_results(expt_config conf, double *results, double *high_priority_results)
{
    double throughput;
    double high_priority_throughput;
    std::ofstream result_file;
    uint32_t i;

    throughput = 0;
    for (i = 0; i < EXPT_LEN; ++i) throughput += results[i];

    high_priority_throughput = 0;
    for (i = 0; i < EXPT_LEN; ++i) high_priority_throughput += high_priority_results[i];

    throughput               = throughput / (EXPT_LEN * 1.0);
    high_priority_throughput = high_priority_throughput / (EXPT_LEN * 1.0);

    std::cerr << "Throughput: " << throughput << "\n";
    std::cerr << "High priority throughput: " << high_priority_throughput << "\n";
    result_file.open(output_file, std::ios::app | std::ios::out);

    switch (conf._type)
    {
        case PROCESS_POOL:
            result_file << "process_pool ";
            result_file << "pool_size:" << conf._pool_size << " ";
            break;
        case PROCESS:
            result_file << "process ";
            result_file << "max_oustanding:" << conf.max_outstanding_ << " ";
            break;
        case THREAD_POOL:
            result_file << "thread_pool ";
            result_file << "pool_size:" << conf._pool_size << " ";
            break;
        case THREAD:
            result_file << "thread ";
            result_file << "max_outstanding:" << conf.max_outstanding_ << " ";
            break;
        default:
            assert(false);
    }

    result_file << "throughput:" << throughput << " ";
    result_file << "high_priority_throughput:" << high_priority_throughput << " ";
    if (conf._contention == false)
        result_file << "low_contention ";
    else
        result_file << "high_contention ";
    result_file << "\n";
    result_file.close();
}

int main(int argc, char **argv)
{
    expt_config conf(argc, argv);

    Database *db;
    uint64_t dbSize;

    Request **txns[2];
    Launcher *lnchr = nullptr;

    PerfMonitor *monitor;
    pthread_t *monitor_thread;

    bool multiProcess;
    bool high_priority;
    double *results;
    double *high_priority_results;

    volatile uint64_t done;

    srand(rand_seed);

    if (conf._test == true)
    {
        run_test(conf);
        return 0;
    }

    /* Initialize database */
    multiProcess = (conf._type == PROCESS || conf._type == PROCESS_POOL);
    if (conf._contention == true)
        dbSize = HIGH_DATABASE_SZ;
    else
        dbSize = LOW_DATABASE_SZ;
    db = Database::Create(dbSize, multiProcess);
    std::cerr << "Database created\n";

    /* Process pool implmentations support high priority txns. */
    high_priority = conf._type == PROCESS_POOL;
    /* Generate requests to process */
    txns[0] = generate_requests(db, high_priority, DRY_RUN_SZ);
    txns[1] = generate_requests(db, high_priority, NUM_REQS);

    /* Initialize the appropriate launcher */
    if (conf._type == PROCESS)
        lnchr = new ProcessLauncher(conf.max_outstanding_);
    else if (conf._type == PROCESS_POOL)
        lnchr = new ProcessPoolLauncher(conf._pool_size);
    else if (conf._type == THREAD)
        lnchr = new ThreadLauncher(conf.max_outstanding_);
    else if (conf._type == THREAD_POOL)
        lnchr = new ThreadPoolLauncher(conf._pool_size);
    else
        assert(false);

    sleep(1);
    std::cerr << "Launcher created\n";

    /* Measure throughput, and report results */
    results               = (double *)malloc(sizeof(double) * EXPT_LEN);
    high_priority_results = (double *)malloc(sizeof(double) * EXPT_LEN);
    done                  = 0;
    std::cerr << "Running experiment\n";
    barrier();
    monitor        = new PerfMonitor(results, high_priority_results, &done, lnchr);
    monitor_thread = run_experiment(monitor, lnchr, txns, &done);
    free(monitor_thread);
    std::cerr << "Experiment done\n";
    write_results(conf, results, high_priority_results);
}
