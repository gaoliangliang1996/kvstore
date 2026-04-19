#include "../include/kvstore.h"
#include <iostream>
#include <chrono>
#include <assert.h>
#include "mvcc_kvstore.h"

using namespace kvstore;
/*
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
    cfg.memtable_size = 100 * 1024;  // 4MB
    
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

void test_bloom_filter_with_sstable() {
    // 创建测试数据
    std::map<string, string> data;
    for (int i = 0; i < 1000; i++) {
        data["key_" + std::to_string(i)] = "value_" + std::to_string(i);
    }

    // 创建 SSTable (会自动创建 Bloom Filter)
    SSTable* sst = SSTable::createFromMemTable("test_bloom.sst", data);

    // 测试存在的 key
    string value;
    for (int i = 0; i < 1000; i++) {
        string key = "key_" + std::to_string(i);
        assert(sst->get(key, value));
        assert(value == "value_" + std::to_string(i));
    }

    // 测试不存在的 key (Bloom Filter 会快速过滤)
    int false_positives = 0;
    int total = 10000;

    for (int i = 1000; i < 11000; i++) {
        string key = "key_" + std::to_string(i);
        if (sst->mayContain(key)) {
            // Bloom Filter 说明可能存在
            if (sst->get(key, value))
                false_positives++; // 假阳性
        }
    }

    double false_positive_rate = 100.0 * false_positives / total;
    std::cout << "False positive rate: " << false_positive_rate << std::endl;
}
*/
void test_mvcc() {
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore kv(cfg);
    
    // 1. 写入数据
    Version v1 = kv.put("user:1", "Alice");
    Version v2 = kv.put("user:2", "Bob");
    Version v3 = kv.put("user:1", "Alice Updated");
    
    std::cout << "Write versions: " << v1 << ", " << v2 << ", " << v3 << std::endl;
    
    // 2. 创建快照
    auto snap1 = kv.create_snapshot();
    std::cout << "Snapshot version: " << snap1->get_version() << std::endl;
    
    // 3. 继续写入
    kv.put("user:1", "Alice Again");
    kv.del("user:2");
    
    // 4. 读取不同版本的数据
    string value;
    
    // 最新版本（user:1 应该是 "Alice Again"）
    if (kv.get("user:1", value)) {
        std::cout << "Latest user:1 = " << value << std::endl;
    }
    
    // 快照版本（user:1 应该是 "Alice Updated"）
    if (kv.get("user:1", value, snap1->get_version())) {
        std::cout << "Snapshot user:1 = " << value << std::endl;
    }
    
    // 删除的 key（user:2 在快照中应该存在）
    if (kv.get("user:2", value, snap1->get_version())) {
        std::cout << "Snapshot user:2 = " << value << std::endl;
    } else {
        std::cout << "Snapshot user:2 not found" << std::endl;
    }
    
    // 5. 并发测试
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([&kv, i]() {
            std::shared_ptr<Snapshot> snap = kv.create_snapshot();
            kv.put("thread_key_" + std::to_string(i), "value_" + std::to_string(i));
            
            string value;
            if (kv.get("thread_key_" + std::to_string(i), value, snap->get_version())) {
                // 应该读不到刚写入的数据（因为在快照之后）
                std::cout << "Snapshot isolation works!" << std::endl;
            }
            
            kv.release_snapshot(snap);
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // 当子线程结束后，它们创建的快照会被自动释放，active_snapshots 中的弱指针也会失效，垃圾回收机制可以清理掉过旧的版本数据。

    // 6. 统计信息
    auto stats = kv.get_stats();
    std::cout << "\n=== Stats ===" << std::endl;
    std::cout << "MemTable size: " << stats.active_memtable_size << std::endl;
    std::cout << "SSTable count: " << stats.sstable_count << std::endl;
    std::cout << "Active snapshots: " << stats.active_snapshots << std::endl;
    std::cout << "Current version: " << stats.current_version << std::endl;
    
    // 7. 垃圾回收
    kv.garbage_collect();
}
/*
void test03() {
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    KVStore kv(cfg);
    
    // 测试写入
    kv.put("name", "Alice");
    kv.put("age", "25");
    
    // 测试读取
    string value;
    if (kv.get("name", value) == Status::OK) {
        std::cout << "name = " << value << std::endl;
    }
    
    // 测试删除
    kv.del("age");
    if (kv.get("age", value) != Status::OK) {
        std::cout << "age deleted successfully" << std::endl;
    }
    
    std::cout << "All tests passed!" << std::endl;
}


void test_manual_flush() {
    std::cout << "========== Manual Flush Test ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024 * 100;  // 100MB，不自动 flush
    
    MVCCKVStore store(cfg);
    
    // 1. 写入一些数据
    std::cout << "\n1. Writing data..." << std::endl;
    for (int i = 0; i < 100; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // 2. 手动同步 flush
    std::cout << "\n2. Manual synchronous flush..." << std::endl;
    store.flush();
    
    // 3. 写入更多数据
    std::cout << "\n3. Writing more data..." << std::endl;
    for (int i = 100; i < 200; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    // 4. 异步 flush
    std::cout << "\n4. Manual asynchronous flush..." << std::endl;
    store.flush_async(); 
    
    // 等待异步 flush 完成
    while (store.get_is_flushing()) {
        std::cout << "   Waiting for async flush to complete..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // 5. 验证数据完整性
    std::cout << "\n5. Verifying data..." << std::endl;
    string value;
    for (int i = 0; i < 200; i++) {
        string key = "key_" + std::to_string(i);
        if (!store.get(key, value)) {
            std::cout << "   Missing key: " << key << std::endl;
            assert(false);
        }
    }
    std::cout << "   All 200 keys verified!" << std::endl;
    
    std::cout << "\n========== Manual Flush Test Passed! ==========" << std::endl;
}


void test_dual_memtable() {
    std::cout << "========== Dual MemTable Test ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 100;  // 100KB，小阈值便于测试
    
    MVCCKVStore store(cfg);
    
    // 1. 写入第一批数据（触发 flush）
    std::cout << "\n1. Writing first batch (0-999)..." << std::endl;
    for (int i = 0; i < 10; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    auto stats = store.get_stats();
    std::cout << "   Active memtable size: " << stats.active_memtable_size << std::endl;
    std::cout << "   Has immutable: " << stats.has_immutable << std::endl;
    std::cout << "   Is flushing: " << stats.flushing << std::endl;
    
    // 等待 flush 完成
    store.flush();
    while (store.get_is_flushing()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    stats = store.get_stats();
    std::cout << "\n2. After flush:" << std::endl;
    std::cout << "   Active memtable size: " << stats.active_memtable_size << std::endl;
    std::cout << "   Has immutable: " << stats.has_immutable << std::endl;
    std::cout << "   SSTable count: " << stats.sstable_count << std::endl;
    
    // 3. 写入第二批数据
    std::cout << "\n3. Writing second batch (1000-1999)..." << std::endl;
    for (int i = 10; i < 20; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    store.flush();
    stats = store.get_stats();
    std::cout << "   Active memtable size: " << stats.active_memtable_size << std::endl;
    std::cout << "   Has immutable: " << stats.has_immutable << std::endl;
    
    // 4. 验证数据完整性
    std::cout << "\n4. Verifying all 2000 keys..." << std::endl;
    string value;
    int found = 0;
    for (int i = 0; i < 20; i++) {
        string key = "key_" + std::to_string(i);
        if (store.get(key, value)) {
            found++;
        }
    }
    std::cout << "   Found " << found << " out of 2000 keys" << std::endl;
    
    // 5. 手动 flush
    std::cout << "\n5. Manual flush..." << std::endl;
    store.flush();
    
    stats = store.get_stats();
    std::cout << "   Active memtable size: " << stats.active_memtable_size << std::endl;
    std::cout << "   SSTable count: " << stats.sstable_count << std::endl;
    
    std::cout << "\n========== Test Passed! ==========" << std::endl;
}
*/

int main() {
    // test_sstable_iterator();
    // test_compaction();
    // test02();
    // test_bloom_filter_with_sstable();
    test_mvcc();
    // test03();
    // test_manual_flush();
    // test_dual_memtable();
    
    
    return 0;
}