#include "transaction.h"
#include <iostream>
#include <thread>
#include <iomanip>

using namespace kvstore;

// ============== 测试辅助函数 ==============
void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "  " << title << std::endl;
    std::cout << std::string(70, '=') << std::endl;
}

void print_result(bool success, const std::string& msg) {
    if (success) {
        std::cout << "  ✅ " << msg << std::endl;
    } else {
        std::cout << "  ❌ " << msg << std::endl;
    }
}

// ============== 场景 A：只有自己持有 S 锁，升级到 X ==============
void test_scenario_a() {
    print_separator("场景 A：只有事务1持有 S 锁，事务1升级到 X 锁");
    
    LockManager lm;
    const std::string key = "counter";
    
    // Txn1 获取 S 锁
    bool ok1 = lm.AcquireRecordLock(key, LockType::SHARED, 1);
    std::cout << "[Txn1] 获取 S 锁: " << (ok1 ? "成功" : "失败") << std::endl;
    lm.print_locks(key);
    
    // Txn1 升级为 X 锁
    bool ok2 = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, 1);
    std::cout << "[Txn1] S → X 升级: " << (ok2 ? "成功" : "失败") << std::endl;
    lm.print_locks(key);
    
    print_result(ok1 && ok2, "预期: 成功 → 成功");
    
    lm.ReleaseLocks(1);
}

// ============== 场景 B：两个事务都持有 S 锁，事务1试图升级 ==============
void test_scenario_b() {
    print_separator("场景 B：事务1和事务2都持有 S 锁，事务1试图升级到 X 锁");
    
    LockManager lm;
    const std::string key = "counter";
    
    // Txn1 获取 S 锁
    lm.AcquireRecordLock(key, LockType::SHARED, 1);
    std::cout << "[Txn1] 获取 S 锁" << std::endl;
    
    // Txn2 获取 S 锁
    lm.AcquireRecordLock(key, LockType::SHARED, 2);
    std::cout << "[Txn2] 获取 S 锁" << std::endl;
    lm.print_locks(key);
    
    // Txn1 试图升级为 X 锁（会阻塞，因为 Txn2 有 S 锁）
    std::atomic<bool> upgrade_result{false};
    std::thread t1([&]() {
        bool ok = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, 1, 500);  // 500ms 超时
        upgrade_result = ok;
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "[Txn1] 尝试 S → X 升级（等待中...）" << std::endl;
    
    // Txn2 释放 S 锁
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::cout << "[Txn2] 释放 S 锁" << std::endl;
    lm.ReleaseLocks(2);
    lm.print_locks(key);
    
    t1.join();
    std::cout << "[Txn1] 升级结果: " << (upgrade_result ? "成功" : "失败（超时）") << std::endl;
    lm.print_locks(key);
    
    print_result(upgrade_result, "预期: 等待 Txn2 释放后升级成功");
    
    lm.ReleaseLocks(1);
}

// ============== 场景 B2：死锁测试 ==============
void test_scenario_b2_deadlock() {
    print_separator("场景 B2：死锁 - 两个事务都持有 S 锁，都试图升级");
    
    LockManager lm;
    const std::string key = "counter";
    
    // Txn1 和 Txn2 都获取 S 锁
    lm.AcquireRecordLock(key, LockType::SHARED, 1);
    lm.AcquireRecordLock(key, LockType::SHARED, 2);
    std::cout << "[Txn1] 和 [Txn2] 都获取了 S 锁" << std::endl;
    lm.print_locks(key);
    
    std::atomic<bool> t1_result{false}, t2_result{false};
    
    // 同时试图升级
    std::thread t1([&]() {
        t1_result = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, 1, 1000);
    });
    std::thread t2([&]() {
        t2_result = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, 2, 1000);
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "[Txn1] 和 [Txn2] 同时尝试 S → X 升级..." << std::endl;
    
    t1.join();
    t2.join();
    
    std::cout << "[Txn1] 结果: " << (t1_result ? "成功" : "超时") << std::endl;
    std::cout << "[Txn2] 结果: " << (t2_result ? "成功" : "超时") << std::endl;
    
    print_result(!t1_result || !t2_result, 
        "预期: 至少一个超时（死锁被超时机制打破）");
    
    lm.ReleaseLocks(1);
    lm.ReleaseLocks(2);
}

// ============== 场景 C：事务1持有 S 锁，事务2试图获取 X 锁 ==============
void test_scenario_c() {
    print_separator("场景 C：事务1持有 S 锁，事务2试图获取 X 锁");
    
    LockManager lm;
    const std::string key = "counter";
    
    // Txn1 获取 S 锁
    lm.AcquireRecordLock(key, LockType::SHARED, 1);
    std::cout << "[Txn1] 获取 S 锁" << std::endl;
    lm.print_locks(key);
    
    // Txn2 试图获取 X 锁（会阻塞）
    std::atomic<bool> t2_result{false};
    std::thread t2([&]() {
        t2_result = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, 2, 1000);
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "[Txn2] 尝试获取 X 锁（等待中...）" << std::endl;
    
    // Txn1 释放 S 锁
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::cout << "[Txn1] 释放 S 锁" << std::endl;
    lm.ReleaseLocks(1);
    lm.print_locks(key);
    
    t2.join();
    std::cout << "[Txn2] 获取 X 锁: " << (t2_result ? "成功" : "失败（超时）") << std::endl;
    lm.print_locks(key);
    
    print_result(t2_result, "预期: 等待 Txn1 释放后成功");
    
    lm.ReleaseLocks(2);
}

// ============== 场景 D：完整矩阵测试 ==============
void test_full_matrix() {
    print_separator("场景 D：完整兼容矩阵测试");
    
    std::cout << "\n  矩阵: 已有锁(left) × 请求锁(top)\n" << std::endl;
    
    struct TestCase {
        std::string name;
        std::vector<std::pair<LockType, uint64_t>> existing;  // (type, txn_id)
        LockType request_type;
        uint64_t request_txn;
        bool expected;
    };
    
    std::vector<TestCase> tests = {
        // D1-D2: 空锁 → 加任何锁都成功
        {"D1. [空] + S",    {},                    LockType::SHARED,    1, true},
        {"D2. [空] + X",    {},                    LockType::EXCLUSIVE, 1, true},
        
        // D3-D6: 一个 S 锁
        {"D3. [S1] + S(1)", {{LockType::SHARED,1}}, LockType::SHARED,    1, true},   // 重入
        {"D4. [S1] + X(1)", {{LockType::SHARED,1}}, LockType::EXCLUSIVE, 1, true},   // 升级
        {"D5. [S1] + S(2)", {{LockType::SHARED,1}}, LockType::SHARED,    2, true},   // S和S兼容
        {"D6. [S1] + X(2)", {{LockType::SHARED,1}}, LockType::EXCLUSIVE, 2, false},  // 冲突
        
        // D7-D10: 一个 X 锁
        {"D7. [X1] + S(1)", {{LockType::EXCLUSIVE,1}}, LockType::SHARED,    1, true},  // 自己
        {"D8. [X1] + X(1)", {{LockType::EXCLUSIVE,1}}, LockType::EXCLUSIVE, 1, true},  // 重入
        {"D9. [X1] + S(2)", {{LockType::EXCLUSIVE,1}}, LockType::SHARED,    2, false}, // 冲突
        {"D10.[X1] + X(2)", {{LockType::EXCLUSIVE,1}}, LockType::EXCLUSIVE, 2, false}, // 冲突
        
        // D11-D13: 两个 S 锁
        {"D11.[S1,S2]+X(1)", {{LockType::SHARED,1},{LockType::SHARED,2}}, LockType::EXCLUSIVE, 1, false},
        {"D12.[S1,S2]+S(3)", {{LockType::SHARED,1},{LockType::SHARED,2}}, LockType::SHARED,    3, true},
        {"D13.[S1,S2]+X(3)", {{LockType::SHARED,1},{LockType::SHARED,2}}, LockType::EXCLUSIVE, 3, false},
    };
    
    std::cout << "  " << std::left << std::setw(25) << "测试名称" 
              << std::setw(20) << "已有锁" 
              << std::setw(15) << "请求锁" 
              << std::setw(10) << "预期" 
              << "实际" << std::endl;
    std::cout << "  " << std::string(75, '-') << std::endl;
    
    int passed = 0;
    int failed = 0;
    
    for (const auto& test : tests) {
        LockManager lm;
        const std::string key = "test_key";
        
        // 添加已有锁
        for (const auto& [type, txn] : test.existing) {
            lm.AcquireRecordLock(key, type, txn, 100);
        }
        
        // 尝试获取新锁（用短超时避免等太久）
        bool result = lm.AcquireRecordLock(key, test.request_type, test.request_txn, 100);
        
        // 构建已有锁字符串
        std::string existing_str;
        for (const auto& [type, txn] : test.existing) {
            if (!existing_str.empty()) existing_str += ",";
            existing_str += lm.lock_type_str(type) + std::to_string(txn);
        }
        
        std::string request_str = lm.lock_type_str(test.request_type) + "(" + std::to_string(test.request_txn) + ")";
        
        bool match = (result == test.expected);
        if (match) passed++;
        else failed++;
        
        std::cout << "  " << std::left << std::setw(25) << test.name
                  << std::setw(20) << existing_str
                  << std::setw(15) << request_str
                  << std::setw(10) << (test.expected ? "成功" : "失败")
                  << (match ? "✅ 通过" : "❌ 失败") << std::endl;
    }
    
    std::cout << "\n  结果: " << passed << " 通过, " << failed << " 失败 (共 " << tests.size() << " 项)" << std::endl;
}

// ============== 并发竞争测试 ==============
void test_concurrent_lock_competition() {
    print_separator("并发竞争测试：10个线程同时读写同一个 key");
    
    LockManager lm;
    const std::string key = "hot_key";
    std::atomic<int> read_success{0};
    std::atomic<int> write_success{0};
    std::atomic<int> read_timeout{0};
    std::atomic<int> write_timeout{0};
    
    auto reader = [&](int id) {
        // 读者
        bool ok = lm.AcquireRecordLock(key, LockType::SHARED, id, 5000);
        if (ok) {
            read_success++;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 持有锁 50ms
            lm.ReleaseLocks(id);
        } else {
            read_timeout++;
        }
    };
    
    auto writer = [&](int id) {
        // 写者
        bool ok = lm.AcquireRecordLock(key, LockType::EXCLUSIVE, id + 100, 5000);
        if (ok) {
            write_success++;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 持有锁 100ms
            lm.ReleaseLocks(id + 100);
        } else {
            write_timeout++;
        }
    };
    
    std::vector<std::thread> threads;
    
    // 5 个读者，5 个写者
    for (int i = 0; i < 5; i++) {
        threads.emplace_back(reader, i);
    }
    for (int i = 0; i < 5; i++) {
        threads.emplace_back(writer, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "  读者: " << read_success << " 成功, " << read_timeout << " 超时" << std::endl;
    std::cout << "  写者: " << write_success << " 成功, " << write_timeout << " 超时" << std::endl;
    
    print_result(read_success + write_success == 10, 
        "预期: 所有 10 个操作都成功（读写正常并发）");
}

// ============== 主函数 ==============
int main() {
    std::cout << "╔══════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║              锁管理器完整测试套件                              ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════════════════════╝" << std::endl;
    
    // test_scenario_a();           // 只有自己，S → X 升级
    // test_scenario_b();           // 两个 S 锁，Txn1 升级等待
    // test_scenario_b2_deadlock(); // 死锁测试
    // test_scenario_c();           // 有 S 锁时，别人加 X 锁
    test_full_matrix();          // 完整兼容矩阵
    // test_concurrent_lock_competition(); // 并发竞争
    
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "  所有测试完成！" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    
    return 0;
}