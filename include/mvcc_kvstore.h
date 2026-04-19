// include/mvcc_kvstore.h
#pragma once
#include "mvcc.h"
#include "mvcc_sstable.h"
#include "wal.h"
#include "logger.h"
#include <memory>
#include <vector>
#include <atomic>
#include <condition_variable>

namespace kvstore {

class MVCCKVStore {
private:
    std::unique_ptr<MVCCMemTable> memtable;
    std::vector<std::shared_ptr<MVCCSSTable>> sstables;
    std::unique_ptr<WAL> wal;
    std::unique_ptr<Logger> logger;
    
    std::mutex flush_mutex;
    std::atomic<bool> running;
    std::atomic<bool> is_flushing;
    
    // 快照管理
    mutable std::mutex snapshot_mutex;
    std::vector<std::weak_ptr<Snapshot>> active_snapshots;

    struct KeyLock {
        uint64_t owner_txn;         // 锁的拥有者事务 ID
        std::mutex mutex;           // 锁保护 owner_txn 和 locked
        std::condition_variable cv; // 等待锁释放的条件变量
        bool locked;                // 是否被锁定

        KeyLock() : owner_txn(0), locked(false) {}
    };

    std::map<string, KeyLock> key_locks; // key -> KeyLock
    std::mutex lock_manager_mutex;       // 保护 key_locks 的互斥锁
    
    Config config;
    
    void flushImmutableMemtable();
    bool getFromSSTables(const string& key, string& value, Version snap_ver);
    void cleanupOldSnapshots();
    Version getMinActiveSnapshotVersion();
    
public:
    MVCCKVStore(const Config& cfg);
    ~MVCCKVStore();

    // 禁止拷贝
    MVCCKVStore(const MVCCKVStore&) = delete;
    MVCCKVStore& operator=(const MVCCKVStore&) = delete;
    
    // 基本操作
    Version put(const string& key, const string& value);
    Version del(const string& key);
    bool get(const string& key, string& value, Version snap_ver = 0);
    
    // 快照管理
    std::shared_ptr<Snapshot> create_snapshot();
    void release_snapshot(std::shared_ptr<Snapshot> snapshot);
    
    // 手动 flush 接口
    void flush();
    void flush_async();
    bool get_is_flushing() const { return is_flushing.load(); }
    
    // 垃圾回收
    void garbage_collect();
    
    // 同步
    void sync();
    
    // 统计信息
    struct Stats {
        size_t active_memtable_size;
        size_t active_memtable_bytes;
        bool has_immutable;
        size_t sstable_count;
        size_t active_snapshots;
        Version current_version;
        bool flushing;
    };
    Stats get_stats() const;
    
    // 辅助方法（用于测试）
    //    std::vector<std::shared_ptr<MVCCSSTable>> sstables;
    const std::vector<std::shared_ptr<MVCCSSTable>>& get_sstables() const { 
        return sstables; // 为什么要把私有数据成员暴露出来？这是为了测试方便，实际设计中应该提供更安全的访问接口。
    }

    const MVCCMemTable* get_memtable() const {
        return memtable.get();
    }
    
    bool has_immutable() const { return memtable->get_has_immutable(); }

    // 事务支持（锁管理）
    bool try_lock_key(const string& key, uint64_t txn_id);
    void unlock_key(const string& key, uint64_t txn_id);
    bool is_key_modified_after(const string& key, Version version);
    Version get_current_version() const { return memtable->get_current_version(); }
};

} // namespace kvstore