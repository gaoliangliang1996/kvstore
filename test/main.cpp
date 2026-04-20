// test/main.cpp
// #include "kvstore.h"
#include "mvcc_kvstore.h"
#include "server.h"
#include "client.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace kvstore;

void run_server() {
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 64 * 1024 * 1024;
    
    auto store = std::make_shared<MVCCKVStore>(cfg);
    KVServer server("0.0.0.0:50051", store);
    
    server.Start();
    std::cout << "Server started on 0.0.0.0:50051" << std::endl;
    server.Wait();
}

void test_client() {
    // 等待服务器启动
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    KVClient client("localhost:50051");
    
    if (!client.Ping()) {
        std::cerr << "Cannot connect to server" << std::endl;
        return;
    }
    
    std::cout << "\n========== Basic Operations ==========" << std::endl;
    
    // Put
    std::cout << "Put: name = Alice" << std::endl;
    uint64_t version;
    if (client.Put("name", "Alice", &version)) {
        std::cout << "  Success, version: " << version << std::endl;
    }
    
    client.Put("age", "25");
    client.Put("city", "Beijing");
    
    // Get
    std::string value;
    if (client.Get("name", value)) {
        std::cout << "Get name: " << value << std::endl;
    }
    
    // Delete
    std::cout << "Delete: age" << std::endl;
    client.Delete("age");
    
    if (!client.Get("age", value)) {
        std::cout << "age deleted successfully" << std::endl;
    }
    
    std::cout << "\n========== Batch Operations ==========" << std::endl;
    
    // MultiPut
    std::vector<std::pair<std::string, std::string>> batch = {
        {"user:1", "Alice"},
        {"user:2", "Bob"},
        {"user:3", "Charlie"}
    };
    client.MultiPut(batch);
    std::cout << "Batch put 3 users" << std::endl;
    
    // MultiGet
    auto results = client.MultiGet({"user:1", "user:2", "user:3"});
    std::cout << "Batch get:" << std::endl;
    for (const auto& [k, v] : results) {
        std::cout << "  " << k << " = " << v << std::endl;
    }
    
    std::cout << "\n========== Range Scan ==========" << std::endl;
    
    // 插入更多数据用于范围扫描
    for (int i = 1; i <= 20; i++) {
        client.Put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    auto scan_results = client.Scan("key_5", "key_15");
    std::cout << "Scan [key_5, key_15]:" << std::endl;
    for (const auto& [k, v] : scan_results) {
        std::cout << "  " << k << " = " << v << std::endl;
    }
    
    std::cout << "\n========== Transaction Test ==========" << std::endl;
    
    // 开始事务
    uint64_t txn_id = client.BeginTransaction();
    std::cout << "Transaction started, id: " << txn_id << std::endl;
    
    // 事务内操作
    client.TxnPut(txn_id, "txn_key1", "txn_value1");
    client.TxnPut(txn_id, "txn_key2", "txn_value2");
    
    std::string txn_value;
    client.TxnGet(txn_id, "txn_key1", txn_value);
    std::cout << "Inside transaction: txn_key1 = " << txn_value << std::endl;
    
    // 提交事务
    if (client.CommitTransaction(txn_id)) {
        std::cout << "Transaction committed" << std::endl;
    }
    
    // 验证事务外的数据
    client.Get("txn_key1", value);
    std::cout << "After commit: txn_key1 = " << value << std::endl;
    
    // std::cout << "\n========== Stats ==========" << std::endl;
    // std::cout << client.GetStats() << std::endl;
    
    // Flush
    std::cout << "Flushing..." << std::endl;
    client.Flush();
    
    std::cout << "\nAll tests completed!" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc > 1 && std::string(argv[1]) == "--server") {
        run_server();
    } else {
        test_client();
    }
    
    return 0;
}