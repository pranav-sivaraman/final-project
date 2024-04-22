#ifndef REQUEST_H_
#define REQUEST_H_

#include <cstdint>
#include <vector>

#include "db/database.h"

class Request
{
   private:
    Request();
    Request(const Request &);
    Request &operator=(const Request &);

   protected:
    Database *db_;
    bool is_high_priority_;
    uint32_t num_writes_;
    uint64_t *writeset_;
    uint64_t *updates_;

    static void DoWrite(char *Record, uint64_t *updates);

    void Lock();
    void Txn();
    void Unlock();

   public:
    Request(Database *db, bool is_high_priority, 
        uint32_t nwrites, uint64_t *writeset, uint64_t *updates);

    static void CopyRequest(char *buf, Request *req);
    static size_t CopySize(Request *req);
    void Execute();
    void SetDatabase(Database *db);
    bool IsHighPriority();
};

#endif  // REQUEST_H_
