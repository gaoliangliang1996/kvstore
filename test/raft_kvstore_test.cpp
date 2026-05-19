// test/raft_kvstore_test.cpp
#include "raft_kvstore.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace kvstore;
using namespace raft;

int main() {
    std::cout << "========== RaftKVStore Test ==========" << std::endl;
    
    // Config 在 kvstore 命名空间中
    Config kv_config;
    kv_config.data_dir = "./test_data";
    kv_config.memtable_size = 64 * 1024 * 1024;
    
    // RaftConfig 在 raft 命名空间中
    RaftConfig raft_config;
    raft_config.node_id = "node-1";
    
    // peers 是 vector<NodeAddress>，NodeAddress 需要 id, host, port
    raft_config.peers = {
        NodeAddress("node-1", "localhost", 50051),
        NodeAddress("node-2", "localhost", 50052),
        NodeAddress("node-3", "localhost", 50053)
    };
    
    raft_config.election_timeout_ms = 150;
    raft_config.heartbeat_interval_ms = 50;
    raft_config.data_dir = "./raft_data";
    
    std::cout << "Creating RaftKVStore..." << std::endl;
    
    try {
        RaftKVStore store(kv_config, raft_config);
        
        std::cout << "Node ID: " << raft_config.node_id << std::endl;
        std::cout << "Is Leader: " << store.IsLeader() << std::endl;
        
        // 等待领导者选举
        std::cout << "Waiting for leader election (3 seconds)..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(3));
        
        std::cout << "Leader ID: " << store.GetLeaderId() << std::endl;
        std::cout << "Is Leader: " << store.IsLeader() << std::endl;
        
        // 如果是领导者，测试读写
        if (store.IsLeader()) {
            std::cout << "\n--- Testing Write Operations ---" << std::endl;
            
            // 写入单个 key
            std::cout << "Putting key1=value1..." << std::endl;
            Version ver = store.put("key1", "value1");
            std::cout << "  Version: " << ver << std::endl;
            
            // 批量写入
            std::cout << "\nBatch putting 3 keys..." << std::endl;
            std::vector<std::pair<std::string, std::string>> batch = {
                {"key2", "value2"},
                {"key3", "value3"},
                {"key4", "value4"}
            };
            auto batch_result = store.batch_put(batch);
            std::cout << "  Batch put success: " << batch_result.success << std::endl;
            std::cout << "  Batch put count: " << batch_result.versions.size() << std::endl;
            
            // 读取数据
            std::cout << "\n--- Testing Read Operations ---" << std::endl;
            std::string value;
            if (store.get("key1", value)) {
                std::cout << "  key1 = " << value << std::endl;
            }
            
            // 删除操作
            std::cout << "\n--- Testing Delete Operations ---" << std::endl;
            std::cout << "Deleting key2..." << std::endl;
            Version del_ver = store.del("key2");
            std::cout << "  Delete version: " << del_ver << std::endl;
            
        } else {
            std::cout << "\nNot leader, leader is: " << store.GetLeaderId() << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "\n========== Test Complete ==========" << std::endl;
    return 0;
}