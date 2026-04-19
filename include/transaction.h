#pragma once
#include "common.h"
#include "mvcc_kvstore.h"
#include <map>
#include <vector>
#include <mutex>
#include <atomic>

namespace kvstore {

// 隔离级别
enum class IsolationLevel {
    READ_UNCOMMITTED,   // 读未提交
    READ_COMMITTED,     // 读已提交
    REPEATABLE_READ,    // 可重复读
    SNAPSHOT_ISOLATION, // 快照隔离（默认）
    SERIALIZABLE        // 可串行化
};

// 操作类型
enum class OpType {
    PUT,
    DELETE
};

// 写操作
struct WriteOp {
    OpType type;
    string key;
    string value;
    
    WriteOp(OpType t, const string& k, const string& v = "")
        : type(t), key(k), value(v) {}
};

// 读操作（用于冲突检测）
struct ReadOp {
    string key;
    Version version;  // 读取时的版本
    string value;     // 读取的值
    
    ReadOp(const string& k, Version ver, const string& val)
        : key(k), version(ver), value(val) {}
};

// 事务类
class Transaction {
private:
    MVCCKVStore* store;
    uint64_t txn_id; // 事务 ID
    IsolationLevel isolation;
    
    // 事务数据
    std::vector<WriteOp> write_buffer;

    // 写集合：充当本地缓冲区，事务内的修改先写到这里，commit时才真正写入存储
    // 读集合：记录读取了哪些数据以及读取时的版本，用于冲突检测
    std::map<string, ReadOp> read_set;      // 读集合，记录读过的所有 key（用于冲突检测） key -> ReadOp
    std::map<string, WriteOp> write_set;    // 写集合，记录本事务的写操作（用于冲突检测）  key -> WriteOp
    
    // 快照信息
    Version snapshot_version;               // 快照版本，事务开始时的数据版本
    std::shared_ptr<Snapshot> snapshot;
    
    // 状态
    enum class State {
        ACTIVE,     // 活跃状态，可进行读写
        COMMITTED,  // 已提交
        ABORTED     // 已回滚
    };
    State state;
    
    // 锁
    std::mutex txn_mutex;
    
    // 静态计数器
    static std::atomic<uint64_t> next_txn_id;
    
    // 内部方法
    bool validate_read_set();
    bool validate_write_set();
    void apply_writes();
    void release_locks();
    bool acquire_locks();
    
public:
    Transaction(MVCCKVStore* kvstore, 
                IsolationLevel level = IsolationLevel::SNAPSHOT_ISOLATION);
    ~Transaction();
    
    // 读写操作
    bool get(const string& key, string& value);
    void put(const string& key, const string& value);
    void del(const string& key);
    
    // 事务控制
    bool commit();
    void rollback();
    
    // 状态查询
    bool is_active() const { return state == State::ACTIVE; }
    uint64_t get_txn_id() const { return txn_id; }
    IsolationLevel get_isolation_level() const { return isolation; }
};

// 事务管理器
class TransactionManager {
private:
    MVCCKVStore* store;
    std::map<uint64_t, std::unique_ptr<Transaction>> active_transactions; // <txn_id , Transaction *>
    std::mutex txn_mutex;
    std::atomic<uint64_t> total_committed;
    std::atomic<uint64_t> total_aborted;
    
public:
    TransactionManager(MVCCKVStore* kvstore);
    ~TransactionManager();
    
    // 创建事务
    Transaction* begin(IsolationLevel level = IsolationLevel::SNAPSHOT_ISOLATION);
    
    // 提交/回滚
    bool commit(Transaction* txn);
    void rollback(Transaction* txn);
    
    // 获取统计信息
    void get_stats(uint64_t& committed, uint64_t& aborted) const {
        committed = total_committed.load();
        aborted = total_aborted.load();
    }
    
    // 清理已提交的事务
    void cleanup_completed();
};

} // namespace kvstore