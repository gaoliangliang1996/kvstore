#include "transaction.h"
#include <random>
#include <chrono>
#include <iostream>

namespace kvstore {

std::atomic<uint64_t> Transaction::next_txn_id{1};

// ============== Transaction 实现 ==============

Transaction::Transaction(MVCCKVStore* kvstore, IsolationLevel level)
    : store(kvstore), isolation(level), state(State::ACTIVE) {
    txn_id = next_txn_id++;
    
    // 创建快照
    snapshot = store->create_snapshot();
    snapshot_version = snapshot->get_version();
    
    std::cout << "[Transaction] Txn " << txn_id << " started, snapshot version: " << snapshot_version << std::endl;
}

Transaction::~Transaction() {
    if (state == State::ACTIVE) {
        rollback();
    }
    if (snapshot) {
        store->release_snapshot(snapshot);
    }
}

// 步骤：1. 验证读集合 2. 验证写集合 3. 获取锁 4. 应用写入 5. 释放锁
bool Transaction::get(const string& key, string& value) {
    if (state != State::ACTIVE) {
        std::cerr << "[Transaction] Txn " << txn_id 
                  << " is not active" << std::endl;
        return false;
    }
    
    // 检查写集合（本地缓冲区）
    auto write_it = write_set.find(key);
    if (write_it != write_set.end()) { // 如果在写集合中，说明之前写过这个 key
        if (write_it->second.type == OpType::PUT) {
            value = write_it->second.value; // 读到自己写的值
            return true;
        } else {
            return false;  // 已删除
        }
    }

    // 没有在写集合中，再根据隔离级别从存储中读取
    
    // 根据隔离级别读取
    Version read_version;
    switch (isolation) {
        case IsolationLevel::READ_UNCOMMITTED:
            // 直接读取最新版本（可能读到未提交的数据）
            if (!store->get(key, value, 0)) {
                return false;
            }
            read_version = store->get_current_version();
            break;
            
        case IsolationLevel::READ_COMMITTED:
            // 读取最新已提交版本
        case IsolationLevel::REPEATABLE_READ:
        case IsolationLevel::SNAPSHOT_ISOLATION:
        case IsolationLevel::SERIALIZABLE:
            // 使用快照读取
            if (!store->get(key, value, snapshot_version)) {
                return false;
            }
            read_version = snapshot_version;
            break;
            
        default:
            return false;
    }
    
    // 记录读集合（用于冲突检测）
    // 使用 emplace 代替 operator[]，避免需要默认构造函数
    read_set.emplace(key, ReadOp(key, read_version, value)); // 为什么要记录读集合？这是为了在提交时进行冲突检测，确保我们读到的数据 在我们提交之前没有被其他事务修改过。
    
    return true;
}

void Transaction::put(const string& key, const string& value) {
    if (state != State::ACTIVE) {
        std::cerr << "[Transaction] Txn " << txn_id 
                  << " is not active" << std::endl;
        return;
    }
    
    write_set.emplace(key, WriteOp(OpType::PUT, key, value));
    std::cout << "[Transaction] Txn " << txn_id  
              << " PUT: " << key << " = " << value << std::endl;
}

void Transaction::del(const string& key) {
    if (state != State::ACTIVE) {
        std::cerr << "[Transaction] Txn " << txn_id 
                  << " is not active" << std::endl;
        return;
    }
    
    write_set.emplace(key, WriteOp(OpType::DELETE, key));
    std::cout << "[Transaction] Txn " << txn_id 
              << " DELETE: " << key << std::endl;
}

// 验证我读过的数据，在我读取之后没有被别人修改过。
bool Transaction::validate_read_set() {
    // 检查读集合中的值是否被其他事务修改
    for (const auto& [key, read_op] : read_set) {
        string current_value;
        Version current_version;
        
        // 获取当前最新版本
        if (!store->get(key, current_value, 0)) { // A. key 现在已经不存在了
            // key 已被删除
            if (read_op.value.empty()) {
                continue;  // 原来读到的就是不存在，现在还是不存在，没问题
            }

            // 原来有值，但现在被删除了 -> 冲突
            std::cout << "[Transaction] Txn " << txn_id << " validation failed: key " << key << " was deleted" << std::endl;
            return false;
        }
        
        // B. key 现在存在，检查版本是否改变了
        current_version = store->get_current_version();
        if (current_version != read_op.version) { // 版本改变了，说明被修改了 -> 冲突
            std::cout << "[Transaction] Txn " << txn_id << " validation failed: key " << key << " changed from version " << read_op.version << " to " << current_version << std::endl;
            return false;
        }
    }
    
    return true;
}

// 验证我要写的 key，在我拿到快照之后没有被别人修改过
bool Transaction::validate_write_set() {
    // 检查写集合中的键是否被其他事务修改
    for (const auto& [key, write_op] : write_set) {
        // 检查：从快照版本到现在，有没有其他事务修改过这个 key？
        if (store->is_key_modified_after(key, snapshot_version)) {
            std::cout << "[Transaction] Txn " << txn_id << " validation failed: key " << key << " was modified by another transaction" << std::endl;
            return false;
        }
    }
    
    return true;
}

bool Transaction::acquire_locks() {
    // 对于 SERIALIZABLE 级别，需要获取锁
    if (isolation != IsolationLevel::SERIALIZABLE) {
        return true;
    }
    
    // 按顺序获取锁，避免死锁
    std::vector<string> keys;
    for (const auto& [key, _] : write_set) {
        keys.push_back(key);
    }
    for (const auto& [key, _] : read_set) {
        keys.push_back(key);
    }
    std::sort(keys.begin(), keys.end());
    
    // 获取锁 获取所有key的锁吗？是的，为了实现 SERIALIZABLE 隔离级别，我们需要在提交前获取所有读写涉及的 key 的锁，以确保没有其他事务可以修改这些 key，从而避免幻读和不可重复读等问题。
    for (const auto& key : keys) {
        if (!store->try_lock_key(key, txn_id)) { // 获取锁失败，说明有冲突
            // 释放已获取的锁
            for (const auto& locked_key : keys) {
                if (locked_key == key) break;
                store->unlock_key(locked_key, txn_id);
            }
            return false;
        }
        // 成功获取锁，继续获取下一个
    }
    
    return true;
}

void Transaction::release_locks() {
    if (isolation != IsolationLevel::SERIALIZABLE) {
        return;
    }
    
    // 释放所有锁
    for (const auto& [key, _] : write_set) {
        store->unlock_key(key, txn_id);
    }
    for (const auto& [key, _] : read_set) {
        store->unlock_key(key, txn_id);
    }
}

void Transaction::apply_writes() {
    for (const auto& [key, write_op] : write_set) {
        if (write_op.type == OpType::PUT) {
            store->put(key, write_op.value);
            std::cout << "[Transaction] Txn " << txn_id 
                      << " applied PUT: " << key << std::endl;
        } else {
            store->del(key);
            std::cout << "[Transaction] Txn " << txn_id 
                      << " applied DELETE: " << key << std::endl;
        }
    }
}

bool Transaction::commit() {
    if (state != State::ACTIVE) {
        std::cerr << "[Transaction] Txn " << txn_id << " is not active" << std::endl;
        return false;
    }
    
    std::cout << "[Transaction] Txn " << txn_id << " committing, isolation=" << (int)isolation << std::endl;
    
    // 1. 获取锁（用于 SERIALIZABLE）
    if (!acquire_locks()) {
        std::cout << "[Transaction] Txn " << txn_id << " failed to acquire locks" << std::endl;
        rollback();
        return false;
    }
    
    // 2. 根据隔离级别验证
    bool valid = true;
    switch (isolation) {
        case IsolationLevel::READ_UNCOMMITTED:
            // 不需要验证
            break;
            
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::REPEATABLE_READ:
            // 只验证写集合
            valid = validate_write_set();
            break;
            
        case IsolationLevel::SNAPSHOT_ISOLATION:
            // 验证读写集合
            valid = validate_read_set() && validate_write_set();
            break;
            
        case IsolationLevel::SERIALIZABLE:
            // 最严格的验证
            valid = validate_read_set() && validate_write_set();
            break;
    }
    
    if (!valid) {
        std::cout << "[Transaction] Txn " << txn_id << " validation failed" << std::endl;
        release_locks();
        rollback();
        return false;
    }
    
    // 3. 应用写操作
    apply_writes();
    
    // 4. 释放锁
    release_locks();
    
    state = State::COMMITTED;
    std::cout << "[Transaction] Txn " << txn_id << " committed successfully" << std::endl;
    
    return true;
}

void Transaction::rollback() {
    if (state != State::ACTIVE) {
        return;
    }
    
    std::cout << "[Transaction] Txn " << txn_id << " rolling back" << std::endl;
    
    // 清空缓冲区
    write_buffer.clear();
    write_set.clear();
    read_set.clear();
    
    // 释放锁
    release_locks();
    
    state = State::ABORTED;
    std::cout << "[Transaction] Txn " << txn_id << " rolled back" << std::endl;
}

// ============== TransactionManager 实现 ==============

TransactionManager::TransactionManager(MVCCKVStore* kvstore)
    : store(kvstore), total_committed(0), total_aborted(0) {}

TransactionManager::~TransactionManager() {
    cleanup_completed();
}

Transaction* TransactionManager::begin(IsolationLevel level) {
    Transaction* txn = new Transaction(store, level);
    
    std::lock_guard<std::mutex> lock(txn_mutex);
    active_transactions[txn->get_txn_id()] = std::unique_ptr<Transaction>(txn); // active_transactions 管理 Transaction 对象的生命周期
    
    return txn;
}

bool TransactionManager::commit(Transaction* txn) {
    if (!txn || !txn->is_active()) {
        return false;
    }
    
    bool success = txn->commit();
    
    if (success) {
        total_committed++;
    } else {
        total_aborted++;
    }
    
    // 从活动事务中移除
    std::lock_guard<std::mutex> lock(txn_mutex);
    active_transactions.erase(txn->get_txn_id());
    
    return success;
}

void TransactionManager::rollback(Transaction* txn) {
    if (!txn || !txn->is_active()) {
        return;
    }
    
    txn->rollback();
    total_aborted++;
    
    std::lock_guard<std::mutex> lock(txn_mutex);
    active_transactions.erase(txn->get_txn_id());
}

void TransactionManager::cleanup_completed() {
    std::lock_guard<std::mutex> lock(txn_mutex);
    
    auto it = active_transactions.begin();
    while (it != active_transactions.end()) {
        if (!it->second->is_active()) {
            it = active_transactions.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace kvstore