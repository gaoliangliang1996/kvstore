// test/range_query_test.cpp
#include "mvcc_kvstore.h"
#include "range_query.h"
#include <iostream>
#include <cassert>
#include <chrono>
#include <thread>

using namespace kvstore;

// 辅助函数：将 MVCCSSTable 列表转换为 SSTable 引用列表
//        std::vector<std::shared_ptr<MVCCSSTable>> sstables;
std::vector<std::shared_ptr<SSTable>> to_sstable_refs(
    const std::vector<std::shared_ptr<MVCCSSTable>>& mvcc_sstables) {
    
    std::vector<std::shared_ptr<SSTable>> result;
    for (const auto& sst : mvcc_sstables) {
        result.push_back(std::static_pointer_cast<SSTable>(sst));
    }
    return result;
}

// 准备测试数据（大数据量）
void prepare_test_data(MVCCKVStore& store, int key_count = 10000) {
    std::cout << "Preparing " << key_count << " test keys..." << std::endl;
    
    for (int i = 0; i < key_count; i++) {
        string key = "key_" + std::to_string(i);
        string value = "value_" + std::to_string(i);
        store.put(key, value);
    }
    
    std::cout << "Data prepared" << std::endl;
}

// 准备水果数据
void prepare_fruit_data(MVCCKVStore& store) {
    std::cout << "Preparing fruit data..." << std::endl;
    
    std::vector<string> fruits = {
        "apple", "apricot", "avocado", "banana", "blueberry",
        "cherry", "coconut", "date", "elderberry", "fig",
        "grape", "grapefruit", "guava", "honeydew", "kiwi",
        "lemon", "mango", "nectarine", "orange", "papaya",
        "quince", "raspberry", "strawberry", "tangerine", "watermelon"
    };
    
    for (const auto& fruit : fruits) {
        store.put(fruit, "fruit_" + fruit);
        store.put(fruit + "_fresh", "fresh_" + fruit);
        store.put(fruit + "_dried", "dried_" + fruit);
    }
    
    std::cout << "Fruit data prepared. Total keys: " << fruits.size() * 3 << std::endl;
}

// 测试1: 基本范围查询
void test_basic_range_scan() {
    std::cout << "\n========== Test 1: Basic Range Scan ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;  // 1MB
    
    MVCCKVStore store(cfg);
    prepare_test_data(store, 100);
    // prepare_fruit_data(store);
    
    // 等待可能的 flush
    store.flush();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    auto sstables = to_sstable_refs(store.get_sstables());
    std::cout << "SSTable count: " << sstables.size() << std::endl;
    
    // 使用 RangeQuerySupport 进行范围查询
    // 注意：RangeQuerySupport 需要访问 SSTable 和 MemTable
    // 由于 MemTable 是私有的，我们只查询 SSTable 中的数据
    // std::cout << "\nRange scan [apple, banana] (from SSTables):" << std::endl;
    // RangeIterator iter = RangeQuerySupport::scan(
    //     sstables, nullptr,  // 不查询 MemTable
    //     "apple", "banana", 0
    // );


    std::cout << "\nRange scan [key_0, key_20] (from SSTables):" << std::endl;
    RangeIterator iter = RangeQuerySupport::scan(
        sstables, nullptr,  // 不查询 MemTable
        "key_0", "key_100", 0
    );
    
    int count = 0;
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        count++;
        iter.next();
    }
    std::cout << "Found " << count << " keys in SSTables" << std::endl;
    
    std::cout << "Test 1 passed!" << std::endl;
}

// 测试2: 范围查询不存在的边界
void test_boundary_scan() {
    std::cout << "\n========== Test 2: Boundary Scan ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    prepare_test_data(store, 100);
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    store.flush();
    
    auto sstables = to_sstable_refs(store.get_sstables());
    
    // 查询不存在的范围
    std::cout << "\nRange scan [aaa, aab] (no data):" << std::endl;
    RangeIterator iter = RangeQuerySupport::scan(
        sstables, nullptr,
        "aaa", "aab", 0
    );
    
    int count = 0;
    while (iter.valid()) {
        count++;
        iter.next();
    }
    assert(count == 0);
    std::cout << "  Empty range OK" << std::endl;
    
    // 查询单个存在的 key
    std::cout << "\nRange scan [key_100, key_100]:" << std::endl;
    iter = RangeQuerySupport::scan(
        sstables, nullptr,
        "key_100", "key_100", 0
    );
    
    if (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        iter.next();
    }
    
    std::cout << "Test 2 passed!" << std::endl;
}

// 测试3: 前缀扫描
void test_prefix_scan() {
    std::cout << "\n========== Test 3: Prefix Scan ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    prepare_fruit_data(store);

    store.flush();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    auto sstables = to_sstable_refs(store.get_sstables());
    
    // 查询 "apple" 前缀
    std::cout << "\nPrefix scan 'apple':" << std::endl;
    auto iter = RangeQuerySupport::prefix_scan(
        sstables, nullptr, "apple", 0
    );
    
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        iter.next();
    }
    
    // 查询 "grape" 前缀
    std::cout << "\nPrefix scan 'grape':" << std::endl;
    iter = RangeQuerySupport::prefix_scan(
        sstables, nullptr, "grape", 0
    );
    
    int count = 0;
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        count++;
        iter.next();
    }
    assert(count >= 3);
    
    std::cout << "Test 3 passed!" << std::endl;
}

// 测试4: 分页查询
void test_paginated_scan() {
    std::cout << "\n========== Test 4: Paginated Scan ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    prepare_test_data(store, 100);
    
    store.flush();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    auto sstables = to_sstable_refs(store.get_sstables());
    
    // 每页10条数据
    size_t page_size = 10; // 每页10条数据
    string next_token;     // 分页 token，初始为空表示从头开始
    bool has_more = true;  // 是否还有更多数据
    int page_num = 1;      // 页码
    int total_keys = 0;    // 统计总扫描的键数
    
    while (has_more && page_num <= 5) {
        RangeQuerySupport::PageResult result = RangeQuerySupport::paginated_scan(
            sstables, nullptr,
            "key_0", "key_z", page_size, next_token, 0
        );
        
        std::cout << "Page " << page_num++ << " (" << result.data.size() << " items):" << std::endl;
        for (const auto& kv : result.data) {
            std::cout << "  " << kv.first << std::endl;
            total_keys++;
        }
        
        next_token = result.next_token;
        has_more = result.has_more;
    }
    
    std::cout << "Total keys scanned: " << total_keys << std::endl;
    assert(total_keys > 0);
    std::cout << "Test 4 passed!" << std::endl;
}

// 测试5: 直接使用 KVStore 的范围查询（如果有实现）
void test_direct_range_scan() {
    std::cout << "\n========== Test 5: Direct Range Scan ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 插入测试数据
    store.put("a1", "value1");
    store.put("a2", "value2");
    store.put("b1", "value3");
    store.put("b2", "value4");
    store.put("c1", "value5");
    
    // 手动 flush 确保数据在 SSTable 中
    store.flush();
    
    // 获取 SSTable 列表
    auto sstables = to_sstable_refs(store.get_sstables());
    
    // 范围查询
    std::cout << "\nRange scan [a1, b2]:" << std::endl;
    auto iter = RangeQuerySupport::scan(
        sstables, nullptr,
        "a1", "b2", 0
    );
    
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        iter.next();
    }
    
    std::cout << "Test 5 passed!" << std::endl;
}

int main() {
    std::cout << "========== Range Query Test Suite ==========" << std::endl;
    
    try {
        test_basic_range_scan();
        test_boundary_scan();
        test_prefix_scan();
        test_paginated_scan();
        test_direct_range_scan();
        
        std::cout << "\n========== All Tests Passed! ==========" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}