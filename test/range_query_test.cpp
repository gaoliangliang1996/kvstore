// test/range_query_test.cpp (简化版)
#include "mvcc_kvstore.h"
#include <iostream>

using namespace kvstore;

void test_range_scan() {
    std::cout << "\n========== Range Scan Test ==========" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 插入测试数据
    for (int i = 1; i <= 20; i++) {
        store.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    store.put("apple", "fruit_apple");
    store.put("apricot", "fruit_apricot");
    store.put("banana", "fruit_banana");
    
    // 刷新到 SSTable
    store.flush();
    
    // 范围查询
    std::cout << "\nRange scan [key_5, key_15]:" << std::endl;
    auto iter = store.range_scan("key_5", "key_15");
    
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        iter.next();
    }
    
    // 前缀查询
    std::cout << "\nPrefix scan 'apple':" << std::endl;
    iter = store.prefix_scan("apple");
    
    while (iter.valid()) {
        std::cout << "  " << iter.key() << " -> " << iter.value() << std::endl;
        iter.next();
    }
    
    // 分页查询
    std::cout << "\nPaginated scan (page_size=5):" << std::endl;
    string token;
    bool has_more = true;
    int page = 1;
    
    while (has_more) {
        auto result = store.paginated_scan("key_0", "key_z", 5, token);
        std::cout << "Page " << page++ << " (" << result.data.size() << " items):" << std::endl;
        for (const auto& kv : result.data) {
            std::cout << "  " << kv.first << std::endl;
        }
        token = result.next_token;
        has_more = result.has_more;
    }
}

int main() {
    test_range_scan();
    return 0;
}