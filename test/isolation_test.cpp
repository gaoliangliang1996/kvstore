// test/isolation_full_test.cpp
#include "../include/transaction.h"
#include "../include/mvcc_kvstore.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

using namespace kvstore;

void wait_ms(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// 测试1: 脏读 (Dirty Read)
void test_dirty_read() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TEST 1: DIRTY READ (READ_UNCOMMITTED only)" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    store.put("balance", "1000");
    std::cout << "Initial balance: 1000\n" << std::endl;
    
    // 事务 A: 更新但不提交
    auto txnA = txn_mgr.Begin(IsolationLevel::READ_UNCOMMITTED);
    txnA->Put("balance", "2000");
    std::cout << "\n[Txn A] UPDATE balance = 2000 (uncommitted)" << std::endl;
    
    wait_ms(100);
    
    // 事务 B: 在不同隔离级别下读取
    std::vector<IsolationLevel> levels = {
        IsolationLevel::READ_UNCOMMITTED,
        IsolationLevel::READ_COMMITTED,
        IsolationLevel::REPEATABLE_READ,
        IsolationLevel::SNAPSHOT_ISOLATION,
        IsolationLevel::SERIALIZABLE
    };
    
    for (auto level : levels) {
        auto txnB = txn_mgr.Begin(level);
        std::string balance;
        std::cout << "\n[Txn B][" << txnB->GetIsolationLevelName() << "] SELECT balance";
        
        if (txnB->Get("balance", balance)) {
            std::cout << " -> " << balance;
            if (balance == "2000") {
                std::cout << " ✅ DIRTY READ!";
            }
        }
        std::cout << std::endl;
        txn_mgr.Rollback(txnB);
    }
    
    txn_mgr.Rollback(txnA);
    std::cout << "\n✅ Dirty Read Test Complete" << std::endl;
}

// 测试2: 不可重复读 (Non-Repeatable Read)
void test_non_repeatable_read() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TEST 2: NON-REPEATABLE READ" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    std::vector<IsolationLevel> levels = {
        IsolationLevel::READ_UNCOMMITTED,
        IsolationLevel::READ_COMMITTED,
        IsolationLevel::REPEATABLE_READ,
        IsolationLevel::SNAPSHOT_ISOLATION,
        IsolationLevel::SERIALIZABLE
    };
    
    for (auto level : levels) {
        
        std::cout << "\n--- Testing " << (int)level << " ---" << std::endl;
        
        store.put("price", "100");
        std::cout << "Initial price: 100\n" << std::endl;

        // 事务 B: 读取两次
        auto txnB = txn_mgr.Begin(level);
        std::string price1, price2;
        
        txnB->Get("price", price1);
        std::cout << "[Txn B] First read: " << price1 << std::endl;
        
                // 事务 A: 修改并提交
                auto txnA = txn_mgr.Begin(IsolationLevel::READ_COMMITTED);
                txnA->Put("price", "80");
                txn_mgr.Commit(txnA);
                std::cout << "[Txn A] Updated price to 80 and committed" << std::endl;
        
                wait_ms(100);
        
        txnB->Get("price", price2);
        std::cout << "[Txn B] Second read: " << price2;
        
        if (price1 != price2) {
            std::cout << " ✅ NON-REPEATABLE READ!";
        }
        std::cout << std::endl;
        
        txn_mgr.Rollback(txnB);
    }
    
    std::cout << "\n✅ Non-Repeatable Read Test Complete" << std::endl;
}

// 测试3: 幻读 (Phantom Read)
void test_phantom_read() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TEST 3: PHANTOM READ" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    // 初始数据
    store.put("user:1", "Alice");
    store.put("user:2", "Bob");
    store.put("user:3", "Charlie");
    std::cout << "Initial users: user:1, user:2, user:3\n" << std::endl;
    
    std::vector<IsolationLevel> levels = {
        IsolationLevel::READ_UNCOMMITTED,
        IsolationLevel::READ_COMMITTED,
        IsolationLevel::REPEATABLE_READ,
        IsolationLevel::SNAPSHOT_ISOLATION,
        IsolationLevel::SERIALIZABLE
    };
    
    // for (auto level : levels) {
        auto level = IsolationLevel::SERIALIZABLE;
        std::cout << "\n--- Testing " << (int)level << " ---" << std::endl;
        
        // 事务 B: 范围查询
        auto txnB = txn_mgr.Begin(level);
        std::vector<std::pair<std::string, std::string>> results1, results2;
        
        txnB->RangeQuery("user:1", "user:5", results1);
        std::cout << "[Txn B] First query: found " << results1.size() << " users" << std::endl;
        
                // 事务 A: 插入新行
                auto txnA = txn_mgr.Begin(IsolationLevel::READ_COMMITTED);
                txnA->Put("user:4", "David");
                txn_mgr.Commit(txnA);
                std::cout << "[Txn A] Inserted user:4 = David and committed" << std::endl;
        
        wait_ms(100);
        
        txnB->RangeQuery("user:1", "user:5", results2);
        std::cout << "[Txn B] Second query: found " << results2.size() << " users";
        
        if (results2.size() > results1.size()) {
            std::cout << " ✅ PHANTOM READ!";
        }
        std::cout << std::endl;
        
        txn_mgr.Rollback(txnB);
    // }
    
    std::cout << "\n✅ Phantom Read Test Complete" << std::endl;
}

// 测试4: Lost Update
void test_lost_update() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TEST 4: LOST UPDATE" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    // store.put("counter", "0");
    // std::cout << "Initial counter: 0\n" << std::endl;
    
    std::vector<std::pair<IsolationLevel, std::string>> levels = {
        {IsolationLevel::READ_UNCOMMITTED, "READ_UNCOMMITTED"},
        {IsolationLevel::READ_COMMITTED, "READ_COMMITTED"},
        {IsolationLevel::REPEATABLE_READ, "REPEATABLE_READ"},
        {IsolationLevel::SNAPSHOT_ISOLATION, "SNAPSHOT_ISOLATION"},
        {IsolationLevel::SERIALIZABLE, "SERIALIZABLE"}
    };
    
    // for (const auto& [level, name] : levels) {
        // 重置计数器
        auto level = IsolationLevel::SERIALIZABLE;
        string name = "SERIALIZABLE";

        store.put("counter", "0");
        
        std::cout << "\n--- Testing " << name << " ---" << std::endl;
        
        // 两个并发事务
        std::atomic<bool> t1_done{false}, t2_done{false};
        
        auto t1 = std::thread([&]() {
            auto txn = txn_mgr.Begin(level);
            std::string counter;
            txn->Get("counter", counter);
            std::cout << "t1's counter: " << counter << std::endl;
            int val = std::stoi(counter);
            txn->Put("counter", std::to_string(val + 1));
            txn_mgr.Commit(txn);
            t1_done = true;
        });
        
        auto t2 = std::thread([&]() {
            auto txn = txn_mgr.Begin(level);
            std::string counter;
            txn->Get("counter", counter);
            std::cout << "t2's counter: " << counter << std::endl;
            int val = std::stoi(counter);
            txn->Put("counter", std::to_string(val + 1));
            txn_mgr.Commit(txn);
            t2_done = true;
        });
        
        t1.join();
        t2.join();
        
        std::string final_counter;
        store.get("counter", final_counter);
        std::cout << "Expected: 2, Actual: " << final_counter;
        
        if (final_counter != "2") {
            std::cout << " ✅ LOST UPDATE!";
        }
        std::cout << std::endl;
    // }
    
    std::cout << "\n✅ Lost Update Test Complete" << std::endl;
}

// 测试5: 间隙锁防止幻读
void test_gap_lock_prevent_phantom() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TEST 5: GAP LOCK PREVENTS PHANTOM READ" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    store.put("user:1", "Alice");
    store.put("user:2", "Bob");
    store.put("user:3", "Charlie");
    std::cout << "Initial users: user:1, user:2, user:3\n" << std::endl;
    
    // 使用 REPEATABLE_READ + 间隙锁
    auto txnB = txn_mgr.Begin(IsolationLevel::REPEATABLE_READ);
    std::cout << "[Txn B] SELECT * FROM users WHERE id BETWEEN 1 AND 10 FOR UPDATE" << std::endl;
    
    std::vector<std::pair<std::string, std::string>> results;
    txnB->RangeQuery("user:1", "user:9", results, true);
    std::cout << "Txn B found " << results.size() << " users" << std::endl;
    
    // 事务 A 尝试插入（应该被间隙锁阻塞）
    std::atomic<bool> insert_done{false};
    auto insert_thread = std::thread([&]() {
        auto txnA = txn_mgr.Begin(IsolationLevel::READ_COMMITTED);
        std::cout << "\n[Txn A] INSERT INTO users (id=4, name='David') - BLOCKED BY GAP LOCK" << std::endl;
        txnA->Put("user:4", "David");
        bool committed = txn_mgr.Commit(txnA);
        insert_done = true;
        std::cout << "[Txn A] Insert " << (committed ? "SUCCEEDED" : "FAILED") << std::endl;
    });
    
    wait_ms(500);
    
    if (!insert_done) {
        std::cout << "\n✅ GAP LOCK: Insert operation is blocked!" << std::endl;
    }
    
    // 再次查询（应该看不到新数据）
    results.clear();
    txnB->RangeQuery("user:1", "user:9", results, false);
    std::cout << "[Txn B] Second query: found " << results.size() << " users (no phantom read)" << std::endl;
    
    txn_mgr.Commit(txnB);
    std::cout << "\n[Txn B] COMMITTED, releasing gap lock" << std::endl;
    
    insert_thread.join();
    
    std::cout << "\n✅ Gap Lock Test Complete" << std::endl;
}

// 测试6: 隔离级别总结
void test_isolation_summary() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "ISOLATION LEVEL SUMMARY" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    std::cout << "\n┌─────────────────────┬────────────┬────────────┬────────────┬─────────────┬─────────────┐\n";
    std::cout << "│ Isolation Level     │ Dirty Read │ Non-Repeat │ Phantom    │ Lost Update │ Gap Lock    │\n";
    std::cout << "│                     │            │ able Read  │ Read       │             │             │\n";
    std::cout << "├─────────────────────┼────────────┼────────────┼────────────┼─────────────┼─────────────┤\n";
    std::cout << "│ READ_UNCOMMITTED    │  可能 ✅   │   可能 ✅  │   可能 ✅  │   可能 ✅   │   不获取    │\n";
    std::cout << "│ READ_COMMITTED      │  不可能    │   可能 ✅  │   可能 ✅  │   可能 ✅   │   不获取    │\n";
    std::cout << "│ REPEATABLE_READ     │  不可能    │   不可能   │   可能 ✅  │   可能 ✅   │   获取      │\n";
    std::cout << "│ SNAPSHOT_ISOLATION  │  不可能    │   不可能   │   不可能   │   可能 ✅   │   不获取    │\n";
    std::cout << "│ SERIALIZABLE        │  不可能    │   不可能   │   不可能   │   不可能    │   获取      │\n";
    std::cout << "└─────────────────────┴────────────┴────────────┴────────────┴─────────────┴─────────────┘\n";
    
    std::cout << "\n✅ 可能: 该隔离级别下可能发生此异常\n";
    std::cout << "✅ 获取: 该隔离级别会使用间隙锁防止幻读\n";
}

// 主函数
int main() {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "TRANSACTION ISOLATION LEVEL COMPLETE TEST SUITE" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    // test_dirty_read();
    // test_non_repeatable_read();
    // test_phantom_read();
    // test_lost_update();
    test_gap_lock_prevent_phantom();
    // test_isolation_summary();
    
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "ALL TESTS COMPLETED" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    return 0;
}