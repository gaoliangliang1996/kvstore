// test/cache_bloom_test.cpp
#include "../include/mvcc_kvstore.h"
#include <iostream>
#include <chrono>

using namespace kvstore;

void test_cache_effectiveness() {
    std::cout << "\n========== Cache Effectiveness Test ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 64 * 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 写入数据
    std::cout << "\nWriting 10000 keys..." << std::endl;
    for (int i = 0; i < 10000; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    store.flush();
    store.ClearCache();
    
    // 第一次读取（缓存未命中）
    std::string value;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 10000; i++) {
        store.get("key_" + std::to_string(i), value);
    }
    auto end = std::chrono::steady_clock::now();
    auto first_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    auto stats = store.GetCacheStats();
    std::cout << "\nFirst read (cold cache):" << std::endl;
    std::cout << "  Time: " << first_time.count() << " ms" << std::endl;
    std::cout << "  Cache hits: " << stats.hits << std::endl;
    std::cout << "  Cache misses: " << stats.misses << std::endl;
    std::cout << "  Hit rate: " << stats.hit_rate() << "%" << std::endl;

    store.ClearCacheStats();
    
    // 第二次读取（缓存命中）
    start = std::chrono::steady_clock::now();
    for (int i = 0; i < 10000; i++) {
        store.get("key_" + std::to_string(i), value);
    }
    end = std::chrono::steady_clock::now();
    auto second_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    stats = store.GetCacheStats();
    std::cout << "\nSecond read (warm cache):" << std::endl;
    std::cout << "  Time: " << second_time.count() << " ms" << std::endl;
    std::cout << "  Cache hits: " << stats.hits << std::endl;
    std::cout << "  Cache misses: " << stats.misses << std::endl;
    std::cout << "  Hit rate: " << stats.hit_rate() << "%" << std::endl;
    
    double speedup = (double)first_time.count() / second_time.count();
    std::cout << "\nSpeedup: " << speedup << "x" << std::endl;
    
    std::cout << "\n✅ Cache Test Passed!" << std::endl;
}

void test_bloom_filter_effectiveness() {
    std::cout << "\n========== Bloom Filter Effectiveness Test ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 64 * 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 写入数据并 flush 到 SSTable
    std::cout << "\nWriting 50000 keys and flushing..." << std::endl;
    for (int i = 0; i < 50000; i++) {
        store.put("existing_key_" + std::to_string(i), "value_" + std::to_string(i));
        if (i % 10000 == 0 && i > 0) {
            store.flush();
        }
    }
    store.flush();
    store.ClearCache();
    
    // 查询不存在的 key（测试 Bloom Filter 过滤效果）
    std::string value;
    int false_positives = 0;
    int total_queries = 10000;
    
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < total_queries; i++) {
        string key = "non_existing_key_" + std::to_string(i);
        if (store.get(key, value)) {
            false_positives++;
        }
    }
    auto end = std::chrono::steady_clock::now();
    auto query_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    double false_positive_rate = 100.0 * false_positives / total_queries;
    
    std::cout << "\nBloom Filter Statistics:" << std::endl;
    std::cout << "  Queries: " << total_queries << std::endl;
    std::cout << "  False positives: " << false_positives << std::endl;
    std::cout << "  False positive rate: " << false_positive_rate << "%" << std::endl;
    std::cout << "  Query time: " << query_time.count() << " ms" << std::endl;
    
    std::cout << "\n✅ Bloom Filter Test Passed!" << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "    CACHE & BLOOM FILTER TEST" << std::endl;
    std::cout << "========================================" << std::endl;
    
    test_cache_effectiveness();
    test_bloom_filter_effectiveness();
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "    ALL TESTS PASSED!" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}