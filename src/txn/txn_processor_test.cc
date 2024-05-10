#include <fstream>
#include <memory>
#include <vector>

#include <perfetto.h>

#include "trace_categories.h"
#include "txn/txn_types.h"
#include "txn_processor.h"

void initialize_perfetto() {
  perfetto::TracingInitArgs args;
  args.backends |= perfetto::kInProcessBackend;
  perfetto::Tracing::Initialize(args);
  perfetto::TrackEvent::Register();
}

std::unique_ptr<perfetto::TracingSession> start_tracing() {
  perfetto::TraceConfig cfg;
  cfg.add_buffers()->set_size_kb(1024);
  auto *ds_cfg = cfg.add_data_sources()->mutable_config();
  ds_cfg->set_name("track_event");

  auto tracing_session = perfetto::Tracing::NewTrace();
  tracing_session->Setup(cfg);
  tracing_session->StartBlocking();
  return tracing_session;
}

void stop_tracing(std::unique_ptr<perfetto::TracingSession> tracing_session) {
  // Make sure the last event is closed for this example.
  perfetto::TrackEvent::Flush();

  // Stop tracing and read the trace data.
  tracing_session->StopBlocking();
  std::vector<char> trace_data(tracing_session->ReadTraceBlocking());

  // Write the result into a file.
  // Note: To save memory with longer traces, you can tell Perfetto to write
  // directly into a file by passing a file descriptor into Setup() above.
  std::ofstream output;
  output.open("db.pftrace", std::ios::out | std::ios::binary);
  output.write(&trace_data[0], std::streamsize(trace_data.size()));
  output.close();
  PERFETTO_LOG("Trace written in example.pftrace file. To read this trace in "
               "text form, run `./tools/traceconv text example.pftrace`");
}

// Returns a human-readable string naming of the providing mode.
std::string ModeToString(CCMode mode) {
  switch (mode) {
  case SERIAL:
    return " Serial   ";
  case LOCKING_EXCLUSIVE_ONLY:
    return " Locking A";
  case LOCKING:
    return " Locking B";
  case OCC:
    return " OCC      ";
  case P_OCC:
    return " OCC-P    ";
  case MVCC:
    return " MVCC     ";
  default:
    return "INVALID MODE";
  }
}

class LoadGen {
public:
  virtual ~LoadGen() {}
  virtual Txn *NewTxn() = 0;
};

class RMWLoadGen : public LoadGen {
public:
  RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
      : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize),
        wait_time_(wait_time) {}

  virtual Txn *NewTxn() {
    return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
  }

private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWLoadGen2 : public LoadGen {
public:
  RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
      : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize),
        wait_time_(wait_time) {}

  virtual Txn *NewTxn() {
    // 80% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    if (rand() % 100 < 80)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }

private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWDynLoadGen : public LoadGen {
public:
  RMWDynLoadGen(int dbsize, int rsetsize, int wsetsize,
                std::vector<double> wait_times)
      : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize) {
    wait_times_ = wait_times;
  }

  virtual Txn *NewTxn() {
    // Mix transactions with different time durations (wait_times_)
    if (rand() % 100 < 30)
      return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[0]);
    else if (rand() % 100 < 60)
      return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[1]);
    else
      return new RMW(dbsize_, rsetsize_, wsetsize_, wait_times_[2]);
  }

private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  std::vector<double> wait_times_;
};

class RMWDynLoadGen2 : public LoadGen {
public:
  RMWDynLoadGen2(int dbsize, int rsetsize, int wsetsize,
                 std::vector<double> wait_times)
      : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize) {
    wait_times_ = wait_times;
  }

  virtual Txn *NewTxn() {
    // 80% of transactions are READ only transactions and run for the different
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    if (rand() % 100 < 80) {
      // Mix transactions with different time durations (wait_times_)
      if (rand() % 100 < 30)
        return new RMW(dbsize_, rsetsize_, 0, wait_times_[0]);
      else if (rand() % 100 < 60)
        return new RMW(dbsize_, rsetsize_, 0, wait_times_[1]);
      else
        return new RMW(dbsize_, rsetsize_, 0, wait_times_[2]);
    } else {
      return new RMW(dbsize_, 0, wsetsize_, 0);
    }
  }

private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  std::vector<double> wait_times_;
};

void Benchmark(const std::vector<LoadGen *> &lg) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = 100;
  std::deque<Txn *> doneTxns;

  // For each MODE...
  for (CCMode mode = SERIAL; mode <= LOCKING;
       mode = static_cast<CCMode>(mode + 1)) {
    TRACE_EVENT("rendering", "Benchmark", "CCMode", ModeToString(mode));
    // Print out mode name.
    std::cout << ModeToString(mode) << std::flush;

    // For each experiment, run 2 times and get the average.
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      double throughput[2];
      for (uint32 round = 0; round < 2; round++) {
        int txn_count = 0;

        // Create TxnProcessor in next mode.
        TxnProcessor *p = new TxnProcessor(mode);

        // Record start time.
        double start = GetTime();

        // Start specified number of txns running.
        for (int i = 0; i < active_txns; i++)
          p->NewTxnRequest(lg[exp]->NewTxn());

        // Keep 100 active txns at all times for the first full second.
        while (GetTime() < start + 0.5) {
          Txn *txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
          p->NewTxnRequest(lg[exp]->NewTxn());
        }

        // Wait for all of them to finish.
        for (int i = 0; i < active_txns; i++) {
          Txn *txn = p->GetTxnResult();
          doneTxns.push_back(txn);
          txn_count++;
        }

        // Record end time.
        double end = GetTime();

        throughput[round] = txn_count / (end - start);

        for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it) {
          delete *it;
        }

        doneTxns.clear();
        delete p;
      }

      // Print throughput
      std::cout << "\t" << (throughput[0] + throughput[1]) / 2 << "\t"
                << std::flush;
    }

    std::cout << std::endl;
  }
}

int main(int argc, char **argv) {
  initialize_perfetto();
  auto tracing_session = start_tracing();

  perfetto::ProcessTrack process_track = perfetto::ProcessTrack::Current();
  perfetto::protos::gen::TrackDescriptor desc = process_track.Serialize();
  desc.mutable_process()->set_process_name("Txn Processor Test");
  perfetto::TrackEvent::SetTrackDescriptor(process_track, desc);

  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  std::cout << "\t\t                Average Transaction Duration" << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  std::cout << "\t\t0.1ms\t\t1ms\t\t10ms\t\t(0.1ms, 1ms, 10ms)" << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;

  std::vector<LoadGen *> lg;

  TRACE_EVENT_BEGIN("rendering", "Low contention Read only (5 records)");

  std::cout << "\t\t            Low contention Read only (5 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));
  lg.push_back(new RMWDynLoadGen(1000000, 5, 0, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "Low contention Read only (30 records)");

  std::cout << "\t\t            Low contention Read only (30 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.01));
  lg.push_back(new RMWDynLoadGen(1000000, 30, 0, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "High contention Read only (5 records)");

  std::cout << "\t\t            High contention Read only (5 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));
  lg.push_back(new RMWDynLoadGen(100, 5, 0, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "High contention Read only (30 records)");

  std::cout << "\t\t            High contention Read only (30 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(100, 30, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 30, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 30, 0, 0.01));
  lg.push_back(new RMWDynLoadGen(100, 30, 0, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "Low contention Read only (5 records)");

  std::cout << "\t\t            Low contention read-write (5 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));
  lg.push_back(new RMWDynLoadGen(1000000, 0, 5, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "Low contention read-write (10 records)");

  std::cout << "\t\t            Low contention read-write (10 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));
  lg.push_back(new RMWDynLoadGen(1000000, 0, 10, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "High contention read-write (5 records)");

  std::cout << "\t\t            High contention read-write (5 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));
  lg.push_back(new RMWDynLoadGen(100, 0, 5, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "High contention read-write (10 records)");

  std::cout << "\t\t            High contention read-write (10 records)"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));
  lg.push_back(new RMWDynLoadGen(100, 0, 10, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  TRACE_EVENT_BEGIN("rendering", "High contention mixed read only/read-write");

  // 80% of transactions are READ only transactions and run for the full
  // transaction duration. The rest are very fast (< 0.1ms), high-contention
  // updates.
  std::cout << "\t\t            High contention mixed read only/read-write"
            << std::endl;
  std::cout
      << "\t\t----------------------------------------------------------------"
         "---"
      << std::endl;
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.0001));
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.001));
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.01));
  lg.push_back(new RMWDynLoadGen2(50, 30, 10, {0.0001, 0.001, 0.01}));

  Benchmark(lg);
  std::cout << std::endl;

  TRACE_EVENT_END("rendering");

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  stop_tracing(std::move(tracing_session));

  return 0;
}
