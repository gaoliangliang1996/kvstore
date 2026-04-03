#pragma once
#include "common.h"
#include "skiplist.h"
#include "wal.h"
#include "sstable.h"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>

namespace kvstore {

class KVStore {
private:
    Config config;
    SkipList<string, string> memtable;
    std::unique_ptr<WAL> wal;
    std::vector<std::unique_ptr<SSTable>> sstables;
    
    std::atomic<size_t> memtable_size;
    std::thread flush_thread;
    std::atomic<bool> running;

    void flushMemtable();
    void backgroundFlush();
    bool getFromSSTables(const string& key, string& value);

public:
    KVStore(const Config& cfg);
    ~KVStore();

    Status put(const string& key, const string& value);
    Status get(const string& key, string& value);
    Status del(const string& key);

    void sync();
};

}