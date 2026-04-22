// include/mvcc_kvstore.h
#pragma once
#include "mvcc.h"
#include "mvcc_sstable.h"
#include "wal.h"
#include "write_batch.h"
#include "logger.h"
#include <memory>
#include <vector>
#include <atomic>
#include <condition_variable>

namespace kvstore {

class RangeIterator {
private:
    struct KeyValue {
        string key;     ///< 键
        string value;   ///< 值
        Version version; ///< 版本号
    };
    
    std::vector<KeyValue> results; ///< 查询结果列表
    size_t current_pos;            ///< 当前迭代位置
    
public:
    RangeIterator() : current_pos(0) {}
    
    void add_result(const string& key, const string& value, Version ver) {
        results.push_back({key, value, ver});
    }
    
    void sort_results() {
        std::sort(results.begin(), results.end(),
                  [](const KeyValue& a, const KeyValue& b) {
                      return NaturalLess()(a.key, b.key);
                  });
    }
    

    bool valid() const { return current_pos < results.size(); }
    void next() { current_pos++; }
    string key() const { return results[current_pos].key; }
    string value() const { return results[current_pos].value; }
    Version version() const { return results[current_pos].version; }
    size_t size() const { return results.size(); }
    void clear() { results.clear(); current_pos = 0; }
};

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

    struct PageResult {
        std::vector<std::pair<string, string>> data; ///< 当前页的数据
        string next_token;                            ///< 下一页的起始 token
        bool has_more;                                ///< 是否还有更多数据

        PageResult() : has_more(false), next_token("") {}
    };

    // 内部范围查询实现
    void collect_from_memtable(const string& start_key, const string& end_key, Version snap_ver, std::map<string, std::pair<string, Version>>& merged);
    void collect_from_sstables(const string& start_key, const string& end_key, std::map<string, std::pair<string, Version>>& merged);
    // 辅助方法
    bool is_key_in_range(const string& key, const string& start, const string& end) const;
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

    // 范围查询
    RangeIterator range_scan(const string& start_key, const string& end_key, Version snap_ver = 0);
    RangeIterator prefix_scan(const string& prefix, Version snap_ver = 0);
    PageResult paginated_scan(const string& start_key, const string& end_key, size_t page_size, const string& page_token, Version snap_ver = 0);
    std::vector<string> get_all_keys(Version snap_ver = 0);

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
    
    bool has_immutable() const { return memtable->get_has_immutable(); }

    // 事务支持（锁管理）
    bool try_lock_key(const string& key, uint64_t txn_id);
    void unlock_key(const string& key, uint64_t txn_id);
    bool is_key_modified_after(const string& key, Version version);
    Version get_current_version() const { return memtable->get_current_version(); }

    // 批量写入
    struct BatchWriteResult {
        bool success;
        std::vector<Version> versions;
        std::vector<string> failed_keys;
        string error;
    };
    BatchWriteResult BatchWrite(const WriteBatch& batch);

    // 批量读取
    struct BatchReadResult {
        struct Item {
            string key;
            string value;
            bool found;
            Version version;
        };
        bool success;
        std::vector<Item> items;
        uint32_t found_count;
        string error;
    };
    BatchReadResult BatchRead(const std::vector<string>& keys, Version snap_ver = 0);

    // 批量删除
    BatchWriteResult BatchDelete(const std::vector<string>& keys);
};

} // namespace kvstore