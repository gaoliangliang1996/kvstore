#pragma once
#include "common.h"
#include "skiplist.h"
#include "wal.h"
#include "sstable.h"
#include "compaction.h"
#include "memtable_manager.h"
#include "lru_cache.h"
#include "bloom_filter.h"

#include <memory>
#include <vector>
#include <thread>
#include <atomic>

namespace kvstore {

class KVStore {
private:
    Config config;
    // SkipList<string, string> memtable;
    std::unique_ptr<MemTableManager> memtable_manager;

    std::unique_ptr<WAL> wal;
    std::vector<std::shared_ptr<SSTable>> sstables;
    std::unique_ptr<LRUCache> cache;
    
    std::thread flush_thread;
    std::atomic<bool> running;

    std::unique_ptr<Compaction> compaction;
    void flushMemtable(SkipList<string, string>* memtable);
    void backgroundFlush();
    bool getFromSSTables(const string& key, string& value);

public:
    KVStore(const Config& cfg);
    ~KVStore();

    Status put(const string& key, const string& value);
    Status get(const string& key, string& value);
    Status del(const string& key);

    void sync();

    struct Stats {
        size_t cache_hits;
        size_t cache_misses;
        size_t bloom_filter_saves;
    };
    Stats get_stats();
};

}