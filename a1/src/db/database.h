#ifndef DATABASE_H_
#define DATABASE_H_

#define RECORD_SIZE 1000
#define FIELD_SIZE 100

#include <semaphore.h>
#include <stdint.h>

struct Record
{
    char bytes_[RECORD_SIZE];
};

class Database
{
   private:
    /* Disable constructors */
    Database();
    Database(const Database &);
    Database &operator=(const Database &);

   protected:
    uint32_t num_records_;
    Record *records_;
    pthread_mutex_t *locks_;

    static void InitRecord(char *buf);

   public:
    static Database *Create(uint32_t recordNum, bool multiProcess);
    static void Destroy(Database *db);
    static void Compare(Database *db1, Database *db2);
    static void Copy(Database *dst, Database *src);

    Record *GetRecord(uint64_t key);
    void LockRecord(uint64_t key);
    void UnlockRecord(uint64_t key);
    size_t DBSize();
};

#endif  // DATABASE_H_
