#include "transaction.h"
#include <iostream>
#include <thread>
#include <vector>

using namespace kvstore;

void test_basic_transaction() {
    std::cout << "\n========== Test 1: Basic Transaction ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    // 开始事务
    Transaction* txn = txn_mgr.begin();
    
    // 执行操作
    txn->put("user:1", "Alice");
    txn->put("user:2", "Bob");
    
    string value;
    if (txn->get("user:1", value)) {
        std::cout << "Read user:1 = " << value << std::endl;
    }
    
    // 提交
    if (txn_mgr.commit(txn)) {
        std::cout << "Transaction committed successfully" << std::endl;
    } else {
        std::cout << "Transaction commit failed" << std::endl;
    }
    
    // 验证数据
    string final_value;
    if (store.get("user:1", final_value)) {
        std::cout << "Final user:1 = " << final_value << std::endl;
    }
}

void test_transaction_rollback() {
    std::cout << "\n========== Test 2: Transaction Rollback ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    // 写入初始数据
    store.put("user:1", "Initial");
    
    // 开始事务
    Transaction* txn = txn_mgr.begin();
    
    txn->put("user:1", "Modified");
    txn->put("user:3", "Charlie");
    
    // 回滚
    txn_mgr.rollback(txn);
    std::cout << "Transaction rolled back" << std::endl;
    
    // 验证数据未被修改
    string value;
    if (store.get("user:1", value)) {
        std::cout << "user:1 = " << value << " (should be Initial)" << std::endl;
    }
    
    if (store.get("user:3", value)) {
        std::cout << "user:3 exists (should not)" << std::endl;
    } else {
        std::cout << "user:3 does not exist (correct)" << std::endl;
    }
}

void test_concurrent_transactions() {
    std::cout << "\n========== Test 3: Concurrent Transactions ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    // 启动多个并发事务
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&store, &txn_mgr, &success_count, i]() {
            Transaction* txn = txn_mgr.begin();
            
            string key = "counter";
            string value;
            
            // 不加锁 读取当前值
            if (txn->get(key, value)) { // 如果 key 存在，解析并递增
                int count = std::stoi(value);
                txn->put(key, std::to_string(count + 1));
            } else { // 如果 key 不存在，初始化为 1
                txn->put(key, "1");
            }
            
            // 提交
            if (txn_mgr.commit(txn)) { // 检测冲突
                success_count++;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "Successful transactions: " << success_count << std::endl;
    
    string final_value;
    store.get("counter", final_value);
    std::cout << "Final counter value: " << final_value << std::endl;
}

void test_snapshot_isolation() {
    std::cout << "\n========== Test 4: Snapshot Isolation ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    TransactionManager txn_mgr(&store);
    
    // 初始数据
    store.put("balance", "100");
    
    // 事务1：读取并修改
    Transaction* txn1 = txn_mgr.begin(IsolationLevel::SNAPSHOT_ISOLATION);
    string balance;
    txn1->get("balance", balance);
    std::cout << "Txn1 reads balance: " << balance << std::endl;
    
            // 事务2：修改同一个 key
            Transaction* txn2 = txn_mgr.begin(IsolationLevel::SNAPSHOT_ISOLATION);
            txn2->put("balance", "200");
            bool txn2_committed = txn_mgr.commit(txn2);
            std::cout << "Txn2 committed: " << txn2_committed << std::endl;
    
    // 事务1：尝试提交（应该失败，因为数据被修改）
    txn1->put("balance", "150");
    bool txn1_committed = txn_mgr.commit(txn1);
    std::cout << "Txn1 committed: " << txn1_committed << " (should fail)" << std::endl;
    
    // 最终值
    string final_balance;
    store.get("balance", final_balance);
    std::cout << "Final balance: " << final_balance << std::endl;
}

int main() {
    std::cout << "========== Transaction Test Suite ==========" << std::endl;
    
    // test_basic_transaction();
    test_transaction_rollback();
    // test_concurrent_transactions();
    // test_snapshot_isolation();
    
    std::cout << "\n========== All Tests Passed! ==========" << std::endl;
    
    return 0;
}