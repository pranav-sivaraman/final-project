add_library(launcher STATIC
    launcher/launcher.cc
    launcher/thread_launcher.cc
    launcher/thread_pool_launcher.cc
    launcher/process_launcher.cc
    launcher/process_pool_launcher.cc
)
add_library(monitor STATIC monitor/perf_monitor.cc)
add_library(transaction STATIC txn/request.cc)
add_library(database STATIC db/database.cc)

add_executable(db db_start.cc)
target_link_libraries(db PUBLIC Threads::Threads launcher monitor transaction database)
install(TARGETS db DESTINATION ${DB_BINARY_DIR})
