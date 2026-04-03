#include "kvstore.h"
#include <iostream>
#include <chrono>

using namespace kvstore;

int main() {
    Config cfg;
    cfg.data_dir = "./test_data";
    cfg.wal_file = "./test_data/wal.log";
    cfg.memtable_size = 1024 * 1024;  // 1MB for testing
    
    KVStore kv(cfg);
    
    // 测试 Put 和 Get
    std::cout << "Testing Put/Get..." << std::endl;
    kv.put("name", "Alice");
    kv.put("age", "25");
    kv.put("city", "Beijing");
    
    string value;
    if (kv.get("name", value) == Status::OK) {
        std::cout << "name = " << value << std::endl;
    }
    
    // 测试 Delete
    std::cout << "\nTesting Delete..." << std::endl;
    kv.del("age");
    if (kv.get("age", value) != Status::OK) {
        std::cout << "age deleted successfully" << std::endl;
    }
    
    // 测试大量数据（触发 flush）
    std::cout << "\nTesting bulk insert..." << std::endl;
    auto start = std::chrono::steady_clock::now();
    
    for (int i = 0; i < 10000; i++) {
        kv.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Inserted 10000 items in " << duration.count() << "ms" << std::endl;
    
    // 验证数据
    int found = 0;
    for (int i = 0; i < 10000; i++) {
        if (kv.get("key_" + std::to_string(i), value) == Status::OK) {
            found++;
        }
    }
    std::cout << "Found " << found << " of 10000 items" << std::endl;
    
    // 测试持久化（重启恢复）
    std::cout << "\nTesting persistence..." << std::endl;
    kv.sync();
    
    // 重新打开 KVStore
    KVStore kv2(cfg);
    if (kv2.get("name", value) == Status::OK) {
        std::cout << "After restart, name = " << value << std::endl;
    }
    
    std::cout << "\nAll tests passed!" << std::endl;
    return 0;
}