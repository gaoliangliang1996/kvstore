// src/transaction.cpp
#include "transaction.h"
#include <iostream>
#include <sstream>
#include <algorithm>

namespace kvstore {

// 静态成员初始化
LockManager Transaction::lock_manager_;

// ============== LockManager 实现 ==============

bool LockManager::AcquireRecordLock(const std::string& key, LockType type, 
                                     uint64_t txn_id, int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto start = std::chrono::steady_clock::now();
    
    while (true) {
        std::vector<Lock>& locks = record_locks_[key];
        bool can_acquire = true;
        bool has_self_lock = false;
        
        for (const auto& l : locks) {
            if (l.txn_id == txn_id) {
                has_self_lock = true;
                continue;  // ✅ 跳过自己，继续检查其他人的锁
            }
            
            // 别人的锁：检查冲突
            if (type == LockType::SHARED && l.type == LockType::EXCLUSIVE) {
                can_acquire = false;
                break;
            }
            if (type == LockType::EXCLUSIVE) {
                // 请求 X 锁，别人的任何锁（S 或 X）都冲突
                can_acquire = false;
                break;
            }
        }
        
        if (can_acquire) {
            if (has_self_lock) {
                // 升级
                for (auto& l : locks) {
                    if (l.txn_id == txn_id) {
                        l.type = type;
                        l.acquire_time = std::chrono::steady_clock::now();
                        break;  // ← 这里用 break 是对的，因为找到自己就改
                    }
                }
            } else {
                locks.push_back({type, txn_id, std::chrono::steady_clock::now(), 
                                GapLockRange("", ""), key});
            }
            return true;
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed >= timeout_ms) return false;
        cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms - elapsed));
    }
}

bool LockManager::AcquireGapLock(const GapLockRange& range, uint64_t txn_id, int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    auto start = std::chrono::steady_clock::now();
    
    while (true) {
        bool conflict = false;
        
        for (const auto& gap_lock : gap_locks_) {
            if (gap_lock.txn_id != txn_id && gap_lock.gap_range.overlaps(range)) { // 其他事务持有的间隙锁与请求的范围重叠，存在冲突
                conflict = true;
                break;
            }
        }
        
        if (!conflict) { // 没有冲突，可以获取间隙锁
            gap_locks_.push_back({LockType::GAP, txn_id, std::chrono::steady_clock::now(), range, ""});
            return true;
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        
        if (elapsed >= timeout_ms) {
            return false;
        }
        
        cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms - elapsed));
    }
}

// 临键锁（Next-Key Lock）
bool LockManager::AcquireNextKeyLock(const std::string& key, uint64_t txn_id, int timeout_ms) {
    // 1. 先获取记录上的排他锁
    if (!AcquireRecordLock(key, LockType::EXCLUSIVE, txn_id, timeout_ms)) {
        return false;
    }
    
    // 2. 再获取 key 的下一个 key 的间隙锁
    std::string next_key = key;
    next_key.back()++;
    // 构造间隙范围：[key, next_key), 锁的是 (key 之后, next_key 之前) 的开区间间隙
    GapLockRange gap_range(key, next_key, true, false);
    return AcquireGapLock(gap_range, txn_id, timeout_ms);
}

// 一次性释放该事务所持有的所有记录锁和间隙锁
void LockManager::ReleaseLocks(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& [key, locks] : record_locks_) { // std::vector<Lock>& locks = record_locks_[key];
        locks.erase(
            std::remove_if(locks.begin(), locks.end(), [txn_id](const Lock& l) { return l.txn_id == txn_id; }),
            locks.end()
        );
    }
    
    gap_locks_.erase(
        std::remove_if(gap_locks_.begin(), gap_locks_.end(), [txn_id](const Lock& l) { return l.txn_id == txn_id; }),
        gap_locks_.end()
    );
    
    cv_.notify_all();
}

// 该 Key 下 属于 该事务的所有记录
void LockManager::ReleaseLock(const std::string& key, uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // std::map<std::string, std::vector<Lock>> record_locks_; // key -> list of locks
    auto it = record_locks_.find(key);
    if (it != record_locks_.end()) {
        std::vector<Lock>& locks = it->second;
        locks.erase(
            std::remove_if(locks.begin(), locks.end(), [txn_id](const Lock& l) { return l.txn_id == txn_id; }),
            locks.end()
        ); 
        
        if (locks.empty()) {
            record_locks_.erase(it);
        }
    }
    
    cv_.notify_all();
}

void LockManager::ReleaseGapLocks(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    gap_locks_.erase(
        std::remove_if(gap_locks_.begin(), gap_locks_.end(), [txn_id](const Lock& l) { return l.txn_id == txn_id; }),
        gap_locks_.end()
    );
    
    cv_.notify_all();
}

bool LockManager::IsLocked(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = record_locks_.find(key);
    return it != record_locks_.end() && !it->second.empty();
}

bool LockManager::IsGapLocked(const GapLockRange& range) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& gap_lock : gap_locks_) {
        if (gap_lock.gap_range.overlaps(range)) {
            return true;
        }
    }
    return false;
}

// 检查查询范围内 是否被其他事务插入了间隙锁（用于幻读保护）
bool LockManager::CheckPhantomProtection(const std::string& start_key, const std::string& end_key, uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    GapLockRange query_range(start_key, end_key, true, true);
    
    for (const auto& gap_lock : gap_locks_) {
        if (gap_lock.txn_id != txn_id && gap_lock.gap_range.overlaps(query_range)) { // 其他事务持有的间隙锁与查询范围重叠，说明可能有幻读
            return false;
        }
    }
    return true;
}

std::string LockManager::GetLockInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::stringstream ss;
    ss << "Record locks: " << record_locks_.size() << ", Gap locks: " << gap_locks_.size();
    return ss.str();
}

std::string LockManager::lock_type_str(LockType t) const {
    return t == LockType::SHARED ? "S" : "X";
}

void LockManager::print_locks(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = record_locks_.find(key);
        if (it == record_locks_.end() || it->second.empty()) {
            std::cout << "    locks[" << key << "] = (空)" << std::endl;
        } else {
            std::cout << "    locks[" << key << "] = [";
            for (size_t i = 0; i < it->second.size(); i++) {
                if (i > 0) std::cout << ", ";
                std::cout << "{" << lock_type_str(it->second[i].type) << ", txn=" << it->second[i].txn_id << "}";
            }
            std::cout << "]" << std::endl;
        }
    }

// ============== Transaction 实现 ==============

Transaction::Transaction(MVCCKVStore* store, IsolationLevel level, uint64_t txn_id)
    : store_(store), isolation_level_(level), txn_id_(txn_id), state_(State::ACTIVE),
      snapshot_version_(0), snapshot_(nullptr) {
    
    RegisterWithStore();
    
    // 对于 REPEATABLE_READ 及以上隔离级别，在事务开始时创建快照，后续读取都基于这个快照版本，保证同一事务内多次读取同一 key 的结果一致
    if (isolation_level_ >= IsolationLevel::REPEATABLE_READ) {
        snapshot_ = store_->create_snapshot();
        snapshot_version_ = snapshot_->get_version();
    }
    
    Log("BEGIN", "", GetIsolationLevelName());
}

Transaction::~Transaction() {
    if (state_ == State::ACTIVE) {
        Rollback();
    }
    UnregisterFromStore();
    if (snapshot_) {
        store_->release_snapshot(snapshot_);
    }
}

std::string Transaction::GetIsolationLevelName() const {
    switch (isolation_level_) {
        case IsolationLevel::READ_UNCOMMITTED: return "READ_UNCOMMITTED";
        case IsolationLevel::READ_COMMITTED:   return "READ_COMMITTED";
        case IsolationLevel::REPEATABLE_READ:  return "REPEATABLE_READ";
        case IsolationLevel::SNAPSHOT_ISOLATION: return "SNAPSHOT_ISOLATION";
        case IsolationLevel::SERIALIZABLE:     return "SERIALIZABLE";
        default: return "UNKNOWN";
    }
}

void Transaction::Log(const std::string& op, const std::string& key, const std::string& value) {
    std::stringstream ss;
    ss << "[Txn " << txn_id_ << "][" << GetIsolationLevelName() << "] " << op;
    if (!key.empty()) ss << " " << key;
    if (!value.empty()) ss << " = " << value;
    std::cout << ss.str() << std::endl;
}

bool Transaction::Get(const std::string& key, std::string& value) {
    if (state_ != State::ACTIVE) {
        Log("GET FAILED - transaction not active", key);
        return false;
    }
    
    // 1. 先检查本事务的写集合（未提交的修改）
    for (const auto& item : write_set_) {
        if (item.key == key) {
            if (item.is_delete) {
                Log("GET (from write set - DELETED)", key);
                return false;
            } else {
                value = item.value;
                Log("GET (from write set)", key, value);
                return true;
            }
        }
    }
    
    // 2. 若未找到，则根据隔离级别从 store 读取
    bool found = false;
    switch (isolation_level_) {
        case IsolationLevel::READ_UNCOMMITTED:
            found = ReadUncommitted(key, value);
            break;
        case IsolationLevel::READ_COMMITTED:
            found = ReadCommitted(key, value);
            break;
        case IsolationLevel::REPEATABLE_READ:
            found = RepeatableRead(key, value);
            break;
        case IsolationLevel::SNAPSHOT_ISOLATION:
            found = SnapshotIsolation(key, value);
            break;
        case IsolationLevel::SERIALIZABLE:
            found = Serializable(key, value);
            break;
    }
    
    if (found) {
        Log("GET", key, value);
    } else {
        Log("GET NOT FOUND", key);
    }
    
    return found;
}

// 无锁，无快照; 会出现 脏读
bool Transaction::ReadUncommitted(const std::string& key, std::string& value) {
    // 直接读取最新版本（包括未提交的）
    return store_->get_uncommitted(key, value); // 读取每个事务的 write_set_
}

// 无锁，无快照; 只能读取已提交的版本，可能出现 不可重复读 和 幻读
bool Transaction::ReadCommitted(const std::string& key, std::string& value) {
    // 读取已提交的最新版本
    return store_->get(key, value, 0); // 从 store 读取最新的已提交版本
}

// 使用快照; 保证可重复读
bool Transaction::RepeatableRead(const std::string& key, std::string& value) {
    // 事务开始时获取快照版本，后续所有读取都基于这个版本，保证同一事务内多次读取同一 key 的结果一致
    if (snapshot_version_ == 0) {
        snapshot_version_ = store_->get_current_version();
    }
    
    // 获取共享锁，防止其他事务修改这个 key
    if (!lock_manager_.AcquireRecordLock(key, LockType::SHARED, txn_id_)) {
        return false;
    }
    locked_keys_.insert(key); // 记录已锁定的 key，方便事务结束时释放
    
    // 从快照版本读取数据
    bool found = store_->get(key, value, snapshot_version_);
    if (found) {
        read_set_.push_back({key, snapshot_version_, value});
    }
    return found;
}

// 快照隔离直接从快照版本读取数据，不需要加锁
// 通过提交时验证 来检测冲突
bool Transaction::SnapshotIsolation(const std::string& key, std::string& value) {
    return store_->get(key, value, snapshot_version_);
}

// 可串行化需要获取共享锁，防止其他事务修改这个 key，同时从快照版本读取数据
bool Transaction::Serializable(const std::string& key, std::string& value) {
    if (!lock_manager_.AcquireRecordLock(key, LockType::SHARED, txn_id_)) {
        return false;
    }
    locked_keys_.insert(key);
    return store_->get(key, value, 0);
}

// Put 和 Delete 操作在 SERIALIZABLE 隔离级别下需要获取排他锁
void Transaction::Put(const std::string& key, const std::string& value) {
    if (state_ != State::ACTIVE) {
        Log("PUT FAILED - transaction not active", key, value);
        return;
    }
    
    Log("PUT", key, value);
    // 将修改记录到写集合中，实际写入在 Commit 时进行
    write_set_.push_back({key, value, false});
    
    if (isolation_level_ == IsolationLevel::SERIALIZABLE) {
        lock_manager_.AcquireRecordLock(key, LockType::EXCLUSIVE, txn_id_);
        locked_keys_.insert(key);
    }
}

void Transaction::Delete(const std::string& key) {
    if (state_ != State::ACTIVE) {
        Log("DELETE FAILED - transaction not active", key);
        return;
    }
    
    Log("DELETE", key);
    // 将删除记录到写集合中，实际写入在 Commit 时进行
    write_set_.push_back({key, "", true});
    
    if (isolation_level_ == IsolationLevel::SERIALIZABLE) {
        lock_manager_.AcquireRecordLock(key, LockType::EXCLUSIVE, txn_id_);
        locked_keys_.insert(key);
    }
}

bool Transaction::RangeQuery(const std::string& start_key, const std::string& end_key,
                             std::vector<std::pair<std::string, std::string>>& results,
                             bool for_update) {
    if (state_ != State::ACTIVE) {
        Log("RANGE QUERY FAILED - transaction not active", start_key + " to " + end_key);
        return false;
    }
    
    Log("RANGE QUERY", start_key + " to " + end_key);

    switch (isolation_level_) {
        case IsolationLevel::REPEATABLE_READ:
            return RangeQueryRepeatableRead(start_key, end_key, results, for_update);
        case IsolationLevel::SERIALIZABLE:
            return RangeQuerySerializable(start_key, end_key, results, for_update);
        default:
            // 其他级别直接查询
            auto iter = store_->range_scan(start_key, end_key, snapshot_version_);
            while (iter.valid()) {
                results.push_back({iter.key(), iter.value()});
                iter.next();
            }
            return true;
    }
}

// for_update == false, 只获取间隙锁，不加记录锁; 效果: 防止插入(幻读)，但不防止修改和删除已有的行
// for_update == true,    获取间隙锁  +  记录锁; 效果: 防止插入(幻读)，也防止修改和删除已有的行
bool Transaction::RangeQueryRepeatableRead(const std::string& start_key, const std::string& end_key,
                                           std::vector<std::pair<std::string, std::string>>& results,
                                           bool for_update) {
    // 获取间隙锁防止幻读
    GapLockRange range(start_key, end_key, true, true);
    if (!lock_manager_.AcquireGapLock(range, txn_id_)) {
        Log("RANGE QUERY - Failed to acquire gap lock", start_key + " to " + end_key);
        return false;
    }
    acquired_gap_locks_.push_back(range);
    
    // 查询数据
    for (char c = start_key.back(); c <= end_key.back(); c++) {
        std::string key = start_key.substr(0, start_key.length() - 1) + c;
        std::string value;
        if (store_->get(key, value, snapshot_version_)) { // 锁住存在的 key; 不存在的 key 不加锁
            results.emplace_back(key, value);
            if (for_update) {
                lock_manager_.AcquireRecordLock(key, LockType::EXCLUSIVE, txn_id_);
                locked_keys_.insert(key);
            }
        }
    }
    
    Log("RANGE QUERY", "found " + std::to_string(results.size()) + " rows, gap lock acquired");
    return true;
}

// 每个位置都加锁，不论数据是否存在
bool Transaction::RangeQuerySerializable(const std::string& start_key, const std::string& end_key,
                                         std::vector<std::pair<std::string, std::string>>& results,
                                         bool for_update) {
    // 使用临键锁
    for (char c = start_key.back(); c <= end_key.back(); c++) {
        std::string key = start_key.substr(0, start_key.length() - 1) + c;
        lock_manager_.AcquireNextKeyLock(key, txn_id_);
        locked_keys_.insert(key);
        
        std::string value;
        if (store_->get(key, value, snapshot_version_)) {
            results.emplace_back(key, value); // 存在才加入结果
        }
    }
    
    Log("RANGE QUERY (SERIALIZABLE)", "found " + std::to_string(results.size()) + " rows, next-key locks acquired");
    return true;
}

void Transaction::LockForUpdate(const std::string& key) {
    if (lock_manager_.AcquireRecordLock(key, LockType::EXCLUSIVE, txn_id_)) {
        locked_keys_.insert(key);
        Log("LOCK FOR UPDATE", key);
    }
}

void Transaction::LockRange(const std::string& start_key, const std::string& end_key) {
    GapLockRange range(start_key, end_key, true, true);
    if (lock_manager_.AcquireGapLock(range, txn_id_)) {
        acquired_gap_locks_.push_back(range);
        Log("LOCK RANGE", start_key + " to " + end_key);
    }
}

bool Transaction::ValidateReadSet() {
    for (const ReadItem& item : read_set_) {
        std::string current_value;
        if (store_->get(item.key, current_value, 0)) { // 找到了
            if (current_value != item.value) { // 读取的值与当前值不一致，说明读集中的数据被其他事务修改了
                Log("VALIDATION FAILED - read set changed", item.key);
                return false;
            }
            // 找到了，且 值相同，通过
        } 
        // 没找到 但 item.value 不为空，说明数据被删除了
        else if (!item.value.empty()) {
            Log("VALIDATION FAILED - key deleted", item.key);
            return false;
        }
        // 没找到 且 item.value 为空，通过
    }
    return true;
}

bool Transaction::ValidateWriteSet() {
    for (const auto& item :  write_set_) {
        // 检查写集中的每个 key 是否在事务开始后被其他事务修改了（版本号更大）
        if (store_->is_key_modified_after(item.key, snapshot_version_)) {
            Log("VALIDATION FAILED - write set conflict", item.key);
            return false;
        }
    }
    return true;
}

// 三个同时满足才算丢失更新：
// 1. write_set_ 中有这个 key（本事务要写）
// 2. read_set_  中也有这个 key（本事务读过）
// 3. 现在这个 key 的值 ≠ 读时候的值（被其他事务改了）
bool Transaction::DetectLostUpdate() {
    for (const auto& item : write_set_) {
        std::string current_value;
        if (store_->get(item.key, current_value, 0)) { // 读取数据库中该 key 的当前值
            for (const auto& read_item : read_set_) {
                if (read_item.key == item.key && read_item.value != current_value) { // 读过这个 key，且值被修改了，说明发生了丢失更新
                    Log("LOST UPDATE DETECTED", item.key);
                    return true;
                }
            }
        }
    }
    return false;
}

void Transaction::ReleaseLocks() {
    for (const auto& key : locked_keys_) {
        lock_manager_.ReleaseLock(key, txn_id_);
    }
    locked_keys_.clear();
    
    lock_manager_.ReleaseGapLocks(txn_id_);
    acquired_gap_locks_.clear();
}

void Transaction::RegisterWithStore() {
    store_->register_transaction(txn_id_, this);
}

void Transaction::UnregisterFromStore() {
    store_->unregister_transaction(txn_id_);
}

bool Transaction::GetWriteSetValue(const std::string& key, std::string& value) const {
    for (auto it = write_set_.rbegin(); it != write_set_.rend(); ++it) { // 从后往前找，返回最新的修改
        if (it->key == key) {
            if (!it->is_delete) {
                value = it->value;
                return true;
            }
            return false; // 已删除
        }
    }
    return false;
}

bool Transaction::Commit() {
    if (state_ != State::ACTIVE) {
        Log("COMMIT FAILED - transaction not active", "");
        return false;
    }
    
    Log("COMMIT", "");
    
    bool success = true;
    
    switch (isolation_level_) {
        case IsolationLevel::READ_UNCOMMITTED:  // 不验证：允许脏读、不可重复读、幻读和丢失更新
        case IsolationLevel::READ_COMMITTED:    // 不验证：允许不可重复读和幻读，但不允许脏读；丢失更新在 READ COMMITTED 中也不允许，因为写入时会检查版本号
            success = true;
            break;
        case IsolationLevel::REPEATABLE_READ:    
            success = ValidateWriteSet()        // ValidateWriteSet():  我要写的数据，快照后没被别人改过
                   && !DetectLostUpdate();      // !DetectLostUpdate(): 我读过又要写的数据，没被人改过
                                                // 不检查 ValidateReadSet() 因为 REPEATABLE_READ 允许不可重复读，读过的数据被改是允许的。
            break;
        case IsolationLevel::SNAPSHOT_ISOLATION:
            success = ValidateReadSet()         // ValidateReadSet():  我读过的所有数据都没变
                   && ValidateWriteSet();       // ValidateWriteSet(): 我要写的数据快照后没变
                                                // 不检查 DetectLostUpdate(). 因为 ValidateReadSet + ValidateWriteSet 已经覆盖了丢失更新的场景
            break;
        case IsolationLevel::SERIALIZABLE:      // 不是不需要验证，而是验证已经在执行阶段完成了
            success = true; // SERIALIZABLE 通过加锁来保证，不需要在提交时额外验证；如果有冲突，执行阶段就已经失败了，提交时直接成功即可
            break;
    }
    
    if (!success) {
        Log("COMMIT FAILED - conflict detected", "");
        Rollback();
        return false;
    }
    
    for (const auto& item : write_set_) {
        if (item.is_delete) {
            store_->del(item.key);
        } else {
            store_->put(item.key, item.value);
        }
    }
    
    ReleaseLocks();
    state_ = State::COMMITTED;
    Log("COMMIT SUCCESS", "");
    
    return true;
}

void Transaction::Rollback() {
    if (state_ != State::ACTIVE) {
        return;
    }
    
    Log("ROLLBACK", "");
    write_set_.clear();
    read_set_.clear();
    ReleaseLocks();
    state_ = State::ABORTED;
    Log("ROLLBACK COMPLETE", "");
}

void Transaction::PrintState() const {
    std::cout << "Transaction " << txn_id_ << " [" << GetIsolationLevelName() << "] - "
              << (state_ == State::ACTIVE ? "ACTIVE" : 
                  state_ == State::COMMITTED ? "COMMITTED" : "ABORTED")
              << ", writes: " << write_set_.size() 
              << ", reads: " << read_set_.size()
              << ", locks: " << locked_keys_.size()
              << ", gap locks: " << acquired_gap_locks_.size()
              << std::endl;
}

// ============== TransactionManager 实现 ==============

TransactionManager::TransactionManager(MVCCKVStore* store) : store_(store) {}

TransactionManager::~TransactionManager() {
    Cleanup();
}

Transaction* TransactionManager::Begin(IsolationLevel level) {
    uint64_t txn_id = next_txn_id_++;
    auto txn = new Transaction(store_, level, txn_id);
    
    std::lock_guard<std::mutex> lock(mutex_);
    active_transactions_[txn_id] = std::unique_ptr<Transaction>(txn);
    
    return txn;
}

bool TransactionManager::Commit(Transaction* txn) {
    if (!txn || !txn->IsActive()) {
        return false;
    }
    
    
    bool success = txn->Commit();
    if (success) {
        total_committed_++;
    } else {
        total_aborted_++;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    active_transactions_.erase(txn->GetTxnId());
    
    return success;
}

void TransactionManager::Rollback(Transaction* txn) {
    if (!txn || !txn->IsActive()) {
        return;
    }
    
    txn->Rollback();
    total_aborted_++;
    
    std::lock_guard<std::mutex> lock(mutex_);
    active_transactions_.erase(txn->GetTxnId());
}

void TransactionManager::GetStats(uint64_t& committed, uint64_t& aborted) const {
    committed = total_committed_.load();
    aborted = total_aborted_.load();
}

void TransactionManager::Cleanup() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_transactions_.clear();
}

std::string TransactionManager::GetActiveTransactionsInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::stringstream ss;
    ss << "Active transactions: " << active_transactions_.size();
    return ss.str();
}

} // namespace kvstore