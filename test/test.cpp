#include "../include/kvstore.h"
#include <iostream>
#include <chrono>

using namespace kvstore;

void test_sstable_iterator() {
    // 创建测试数据
    std::map<string, string> data;
    data["apple"] = "fruit";
    data["banana"] = "fruit";
    data["cat"] = "animal";
    data["dog"] = "animal";
    data["elephant"] = "animal";

    // 创建 SSTable 
    SSTable* sst = SSTable::createFromMemTable("test.sst", data);

    // 正向遍历
    std::cout << "Forward iteration: " << std::endl;
    for (auto it = sst->begin(); it.valid(); it.next()) {
        std::cout << " " << it.key() << " -> " << it.value() << std::endl;
    }

    // 反向遍历
    std::cout << "\nBackward iteration: " << std::endl;
    for (auto it = sst->begin(); it.valid(); it.next()) {
        // 先到末尾
        if (it.key() == sst->get_max_key())
            for (; it.valid(); it.prev()) { // 反向遍历
                std::cout << " " << it.key() << " -> " << it.value() << std::endl;
            }
    }

    // 范围查询
    std::cout << "\nRange query [c, e]: " << std::endl;
    auto it = sst->find("cat");
    while (it.valid() && it.key() <= "elephant") {
        std::cout << " " << it.key() << " -> " << it.value() << std::endl;
        it.next();
    }

    // seek 操作
    std::cout << "\nSeek 'c' (first >= 'c'): " << std::endl;
    it = sst->begin();
    it.seek("c");
    if (it.valid()) {
        std::cout << " Found: " << it.key() << std::endl;
    }

    delete sst;
}

void test_compaction() {
    Compaction compaction("./test_data");
    
    // 模拟添加多个 SSTable
    for (int i = 0; i < 10; i++) {
        std::map<string, string> data;
        data["key_" + std::to_string(i)] = "value_" + std::to_string(i);
        
        SSTable* sst = SSTable::createFromMemTable(
            "./test_data/test_" + std::to_string(i) + ".sst", data
        );
        compaction.add_sstable(0, std::unique_ptr<SSTable>(sst));
    }
    
    // 触发合并
    compaction.trigger_compaction();
    
    // 等待合并完成
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    // 检查结果
    std::cout << "Level 0 files: " << compaction.get_level_count(0) << std::endl;
    std::cout << "Level 1 files: " << compaction.get_level_count(1) << std::endl;
    std::cout << "Level 1 size: " << compaction.get_level_size(1) << " bytes" << std::endl;
}

void test01(){ 
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
}

void test02() {
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1 * 1024;  // 4MB
    
    KVStore kv(cfg);
    
    // 写入数据
    for (int i = 0; i < 100000; i++) {
        kv.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // 读取数据（会使用缓存）
    string value;
    if (kv.get("key_12345", value) == Status::OK) {
        std::cout << "Found: " << value << std::endl;
    }
    
    // 查看统计信息
    auto stats = kv.get_stats();
    std::cout << "Cache hit rate: " 
              << 100.0 * stats.cache_hits / (stats.cache_hits + stats.cache_misses)
              << "%" << std::endl;
}

int main() {
    // test_sstable_iterator();
    // test_compaction();
    test02();
    
    return 0;
}