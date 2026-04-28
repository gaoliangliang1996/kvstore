// include/transaction.h
#pragma once
#include "common.h"
#include "mvcc_kvstore.h"
#include <atomic>
#include <map>
#include <vector>
#include <set>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace kvstore {

// 前向声明
class MVCCKVStore;

// 锁类型
enum class LockType {
    SHARED,     // 共享锁（读锁）
    EXCLUSIVE,  // 排他锁（写锁）
    GAP,        // 间隙锁（防止幻读）
    NEXT_KEY    // 临键锁（Record Lock + Gap Lock）
};

// 间隙锁范围
struct GapLockRange {
    std::string start_key;
    std::string end_key;
    bool include_start;
    bool include_end;
    
    GapLockRange(const std::string& start, const std::string& end, 
                 bool inc_start = false, bool inc_end = false)
        : start_key(start), end_key(end), 
          include_start(inc_start), include_end(inc_end) {}
    
    bool contains(const std::string& key) const {
        bool after_start = include_start ? key >= start_key : key > start_key;
        bool before_end = include_end ? key <= end_key : key < end_key;
        return after_start && before_end;
    }
    
    // 判断两个间隙锁范围是否重叠
    bool overlaps(const GapLockRange& other) const {
        return !(end_key < other.start_key || other.end_key < start_key); // 返回 true 表示有重叠
    }
    
    std::string to_string() const {
        return "[" + start_key + ", " + end_key + "]";
    }
};

// 锁管理器
class LockManager {
private:
    struct Lock {
        LockType type;
        uint64_t txn_id;
        std::chrono::steady_clock::time_point acquire_time;
        GapLockRange gap_range;
        std::string key;
    };
    
    std::map<std::string, std::vector<Lock>> record_locks_; // key -> list of locks
    // record_locks_ 结构示例：
    // {
    //     "key1": [{type: SHARED,    txn_id: 1, acquire_time: t1, gap_range: {}, key: "key1"},
    //              {type: EXCLUSIVE, txn_id: 2, acquire_time: t2, gap_range: {}, key: "key1"}],
    //     "key2": [{type: SHARED,    txn_id: 3, acquire_time: t3, gap_range: {}, key: "key2"}]
    // }
    // 对于 record_locks_，每个 key 可能有多个锁（共享锁可以共存，排他锁只能独占）。每个锁记录了锁的类型、持有者事务 ID、获取时间等信息。
    
    std::vector<Lock> gap_locks_; // list of gap locks
    // gap_locks_ 结构示例：
    // [{type: GAP, txn_id: 4, acquire_time: t4, gap_range: [keyA, keyB], key: ""}]
    // gap_locks_ 中的每个元素表示一个间隙锁，记录了锁的类型（GAP）、持有者事务 ID、获取时间以及锁定的键范围（gap_range）。key 字段在间隙锁中不使用，保留为 ""。

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    
    bool CheckGapLockConflict(const Lock& existing, const Lock& requested);
    bool CheckRecordLockConflict(const Lock& existing, const Lock& requested);
    
public:
    LockManager() = default;
    
    bool AcquireRecordLock(const std::string& key, LockType type, uint64_t txn_id, int timeout_ms = 1000);
    bool AcquireGapLock(const GapLockRange& range, uint64_t txn_id, int timeout_ms = 1000);
    bool AcquireNextKeyLock(const std::string& key, uint64_t txn_id, int timeout_ms = 1000);
    
    void ReleaseLocks(uint64_t txn_id);
    void ReleaseLock(const std::string& key, uint64_t txn_id);
    void ReleaseGapLocks(uint64_t txn_id);
    
    bool IsLocked(const std::string& key) const;
    bool IsGapLocked(const GapLockRange& range) const;
    bool CheckPhantomProtection(const std::string& start_key, const std::string& end_key, uint64_t txn_id);
    
    std::string GetLockInfo() const;
    std::string lock_type_str(LockType t) const;
    void print_locks(const std::string& key) const;
};

// 事务类
class Transaction {
    friend class MVCCKVStore;
    friend class TransactionManager;
    
public:
    Transaction(MVCCKVStore* store, IsolationLevel level, uint64_t txn_id);
    ~Transaction();
    
    // 基本操作
    bool Get(const std::string& key, std::string& value);
    void Put(const std::string& key, const std::string& value);
    void Delete(const std::string& key);
    
    // 范围查询
    bool RangeQuery(const std::string& start_key, const std::string& end_key,
                    std::vector<std::pair<std::string, std::string>>& results, 
                    bool for_update = false);
    
    // 显式锁
    void LockForUpdate(const std::string& key);
    void LockRange(const std::string& start_key, const std::string& end_key);
    
    // 事务控制
    bool Commit();
    void Rollback();
    
    // 状态查询
    bool IsActive() const { return state_ == State::ACTIVE; }
    uint64_t GetTxnId() const { return txn_id_; }
    IsolationLevel GetIsolationLevel() const { return isolation_level_; }
    std::string GetIsolationLevelName() const;
    
    // 写集合查询（供 MVCCKVStore 使用）
    bool GetWriteSetValue(const std::string& key, std::string& value) const;
    
    // 调试
    void PrintState() const;
    
private:
    enum class State {
        ACTIVE,
        COMMITTED,
        ABORTED
    };
    
    // 读操作实现
    bool ReadUncommitted(const std::string& key, std::string& value);
    bool ReadCommitted(const std::string& key, std::string& value);
    bool RepeatableRead(const std::string& key, std::string& value);
    bool SnapshotIsolation(const std::string& key, std::string& value);
    bool Serializable(const std::string& key, std::string& value);
    
    // 范围查询实现
    bool RangeQueryRepeatableRead(const std::string& start_key, const std::string& end_key,
                                  std::vector<std::pair<std::string, std::string>>& results,
                                  bool for_update);
    bool RangeQuerySerializable(const std::string& start_key, const std::string& end_key,
                                std::vector<std::pair<std::string, std::string>>& results,
                                bool for_update);
    
    // 冲突检测
    bool ValidateReadSet();
    bool ValidateWriteSet();
    bool DetectLostUpdate();
    
    // 锁管理
    void ReleaseLocks();
    void RegisterWithStore();
    void UnregisterFromStore();
    
    // 日志
    void Log(const std::string& op, const std::string& key = "", const std::string& value = "");
    
    // 成员变量
    MVCCKVStore* store_;
    IsolationLevel isolation_level_;
    uint64_t txn_id_;
    State state_;
    
    // 读写集合
    struct ReadItem {
        std::string key;
        Version version;
        std::string value;
    };
    struct WriteItem {
        std::string key;
        std::string value;
        bool is_delete;
    };
    
    std::vector<ReadItem> read_set_;
    std::vector<WriteItem> write_set_;
    
    // 快照版本
    Version snapshot_version_;
    std::shared_ptr<Snapshot> snapshot_;
    
    // 锁管理
    std::set<std::string> locked_keys_;
    std::vector<GapLockRange> acquired_gap_locks_;
    
    // 全局锁管理器
    static LockManager lock_manager_; // 所有事务共享一个全局锁管理器
};

// 事务管理器
class TransactionManager {
private:
    MVCCKVStore* store_;
    std::map<uint64_t, std::unique_ptr<Transaction>> active_transactions_; // txn_id -> Transaction*
    mutable std::mutex mutex_;
    std::atomic<uint64_t> next_txn_id_{1};
    std::atomic<uint64_t> total_committed_{0};
    std::atomic<uint64_t> total_aborted_{0};
    
public:
    TransactionManager(MVCCKVStore* store);
    ~TransactionManager();
    
    Transaction* Begin(IsolationLevel level = IsolationLevel::SNAPSHOT_ISOLATION);
    bool Commit(Transaction* txn);
    void Rollback(Transaction* txn);
    
    void GetStats(uint64_t& committed, uint64_t& aborted) const;
    void Cleanup();
    
    std::string GetActiveTransactionsInfo() const;
};

} // namespace kvstore