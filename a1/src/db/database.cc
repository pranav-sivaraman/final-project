#include "database.h"

#include <string.h>
#include <utils.h>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <iostream>

size_t Database::DBSize() { return (size_t)num_records_; }
void Database::Compare(Database *db1, Database *db2)
{
    assert(db1->DBSize() == db2->DBSize());

    uint32_t i;
    Record *record1, *record2;
    bool cmp;

    for (i = 0; i < db1->DBSize(); ++i)
    {
        record1 = db1->GetRecord(i);
        record2 = db2->GetRecord(i);
        cmp     = memcmp(record1->bytes_, record2->bytes_, RECORD_SIZE);
        if (cmp != 0)
        {
            std::cerr << "Database records do not match!\n";
            exit(0);
        }
    }
}

void Database::Copy(Database *dst, Database *src)
{
    assert(dst->DBSize() == src->DBSize());

    uint32_t i;
    Record *to_rec, *from_rec;
    for (i = 0; i < dst->DBSize(); ++i)
    {
        to_rec   = dst->GetRecord(i);
        from_rec = src->GetRecord(i);
        memcpy(to_rec->bytes_, from_rec->bytes_, RECORD_SIZE);
    }
}

Database *Database::Create(uint32_t recordNum, bool multiProcess)
{
    assert(RECORD_SIZE % FIELD_SIZE == 0);

    Database *db_mem;
    Record *record_mem;
    pthread_mutex_t *mutex_mem;
    uint32_t i;
    int sem_flag;

    /*
     * Allocate memory to Database class, records, and their semaphores.
     *
     * Memory is allocated as anonymous mmap'ed buffers. Allocating as anon
     * mmap'ed buffers allows the allocator (this process) to share memory
     * with child processes.
     */
    db_mem = (Database *)mmap(NULL, sizeof(Database), PROT_FLAGS, MAP_FLAGS, 0, 0);
    assert(db_mem != MAP_FAILED);
    record_mem = (Record *)mmap(NULL, sizeof(Record) * recordNum, PROT_FLAGS, MAP_FLAGS, 0, 0);
    assert(record_mem != MAP_FAILED);
    mutex_mem = (pthread_mutex_t *)mmap(NULL, sizeof(pthread_mutex_t) * recordNum, PROT_FLAGS, MAP_FLAGS, 0, 0);
    assert(mutex_mem != MAP_FAILED);

    /*
     * If semaphores are used for inter-process synchronization, sem_flag is 1.
     * If semaphores are used for inter-thread synchronization, sem_flag is 0.
     */
    if (multiProcess)
        sem_flag = 1;
    else
        sem_flag = 0;

    /*
     * Initialize records and their associated semaphores.
     *
     * Each semaphore is initailized to 1. Setting its value to 1 allows it
     * to function as a mutex lock.
     */
    for (i = 0; i < recordNum; ++i)
    {
        if (sem_flag == 0)
            mutex_mem[i] = PTHREAD_MUTEX_INITIALIZER;
        else
        {
            pthread_mutexattr_t mutexattr;
            pthread_mutexattr_init(&mutexattr);
            pthread_mutexattr_setpshared(&mutexattr, PTHREAD_PROCESS_SHARED);
            pthread_mutex_init(&mutex_mem[i], &mutexattr);
        }
        InitRecord((char *)&record_mem[i]);
    }

    /* Initialize db class state */
    db_mem->num_records_ = recordNum;
    db_mem->records_     = record_mem;
    db_mem->locks_       = mutex_mem;

    return db_mem;
}

/*
 * Dispose of the memory associated with the Database.
 */
void Database::Destroy(Database *db)
{
    uint32_t i;
    int err;

    /* OS needs to be explicitly notified of semaphores */
    for (i = 0; i < db->num_records_; ++i)
    {
        pthread_mutex_destroy(&db->locks_[i]);
    }

    err = munmap(db->locks_, sizeof(sem_t) * db->num_records_);
    assert(err == 0);
    err = munmap(db->records_, sizeof(Record) * db->num_records_);
    assert(err == 0);
    err = munmap(db, sizeof(Database));
    assert(err == 0);
}

/*
 * Initialize a buffer corresponding to a single Record.
 *
 * The buffer's size is RECORD_SIZE. Each field is of size FIELD_SIZE.
 */
void Database::InitRecord(char *buf)
{
    uint32_t i, nfields;
    uint64_t *field_ptr;

    nfields = RECORD_SIZE / FIELD_SIZE;
    for (i = 0; i < nfields; ++i)
    {
        field_ptr  = (uint64_t *)(&buf[FIELD_SIZE * i]);
        *field_ptr = (uint64_t)rand();
    }
}

/*
 * Obtain mutually exclusive access to a Record. Recall that each Record's
 * semaphore is initialized to 1. Thus, only a single process/thread at a time
 * is allowed to obtain the semaphore.
 */
void Database::LockRecord(uint64_t key)
{
    assert(key < (uint64_t)num_records_);
    pthread_mutex_lock(&locks_[key]);
}

/* Relinquish mutually exclusive access to a Record. */
void Database::UnlockRecord(uint64_t key)
{
    assert(key < (uint64_t)num_records_);
    pthread_mutex_unlock(&locks_[key]);
}

/* Return a reference to a record */
Record *Database::GetRecord(uint64_t key) { return &records_[key]; }
