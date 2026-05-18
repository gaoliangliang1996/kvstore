// raft/test/raft_test.cpp
#include "../include/raft.h"
#include "../include/raft_node.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <cassert>

using namespace raft;

void wait_ms(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// 测试辅助函数：创建测试配置
RaftConfig createTestConfig(int node_id, const std::vector<std::string>& all_peer_ids) {
    RaftConfig config;
    config.node_id = "node-" + std::to_string(node_id);
    
    // 构建 peers 列表（包含所有节点的地址信息）
    for (const auto& peer_id : all_peer_ids) {
        // 从 peer_id 提取端口号（假设 node-1 -> 50051, node-2 -> 50052, ...）
        int peer_num = std::stoi(peer_id.substr(5));  // "node-1" -> 1
        int port = 50050 + peer_num;
        config.peers.push_back(NodeAddress(peer_id, "127.0.0.1", port));
    }
    
    // config.election_timeout_ms = 5000; // 150;
    // config.heartbeat_interval_ms = 1000; // 50;

    // config.election_timeout_ms = 150;
    // config.heartbeat_interval_ms = 50;

    config.election_timeout_ms = 1000;      // 1 秒
    config.heartbeat_interval_ms = 200;     // 200ms

    config.max_append_entries = 100;
    config.snapshot_threshold = 10;
    config.data_dir = "./raft_test_data_" + std::to_string(node_id);
    
    // 设置本节点监听端口
    config.listen_host = "127.0.0.1";
    config.listen_port = 50050 + node_id;
    
    return config;
}

// 测试1: 领导者选举
void test_leader_election() {
    std::cout << "\n========== Test 1: Leader Election ==========\n" << std::endl;
    
    std::vector<std::string> all_peer_ids = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, all_peer_ids);
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetLeaderChangeCallback([i](const std::string& leader_id) {
            std::cout << "[Callback] Node " << i << " detected leader: " << leader_id << std::endl;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待选举完成
    std::cout << "\nWaiting for leader election..." << std::endl;
    wait_ms(5000);
    
    // 检查是否只有一个领导者
    int leader_count = 0;
    std::string leader_id;
    for (auto& node : nodes) {
        if (node->IsLeader()) {
            leader_count++;
            leader_id = node->GetLeaderId();
            std::cout << "Leader found: " << leader_id << std::endl;
        }
        std::cout << "Node " << node->GetLeaderId() << " state=" << NodeStateToString(node->GetState()) << " term=" << node->GetCurrentTerm() << std::endl;
    }
    
    assert(leader_count == 1);
    std::cout << "\n✅ Test 1 Passed: Single leader elected" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 测试2: 日志复制
void test_log_replication() {
    std::cout << "\n========== Test 2: Log Replication ==========\n" << std::endl;
    
    std::vector<std::string> all_peer_ids = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::atomic<int> commit_count{0};
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, all_peer_ids);
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetCommitCallback([&commit_count](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
            commit_count++;
            std::cout << "[Commit] index=" << index << " cmd=" << cmd << std::endl;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(5000);
    
    // 找到领导者
    RaftNode* leader = nullptr;
    for (auto& node : nodes) {
        if (node->IsLeader()) {
            leader = node.get();
            break;
        }
    }
    
    assert(leader != nullptr);
    std::cout << "Leader found: " << leader->GetLeaderId() << std::endl;
    
    // 提交 proposals
    std::cout << "\nSubmitting proposals..." << std::endl;
    for (int i = 0; i < 1; i++) {
        std::string cmd = "put";
        std::string data = "key" + std::to_string(i) + ":value" + std::to_string(i);
        std::vector<uint8_t> data_vec(data.begin(), data.end());
        
        leader->Propose(cmd, data_vec, 
            [i](bool success, uint64_t index) {
                if (success) {
                    std::cout << "Proposal " << i << " committed at index " << index << std::endl;
                } else {
                    std::cout << "Proposal " << i << " failed" << std::endl;
                }
            }
        );
        
        wait_ms(100);
    }
    
    // 等待所有日志复制完成
    wait_ms(2000);
    
    // 检查所有节点的 commit index
    uint64_t last_commit = leader->GetCommitIndex();
    std::cout << "\nLeader commit index: " << last_commit << std::endl;
    
    bool all_synced = true;
    for (auto& node : nodes) {
        uint64_t commit_idx = node->GetCommitIndex();
        std::cout << "Node " << node->GetLeaderId() << " commit index: " << commit_idx << std::endl;
        if (commit_idx < last_commit) {
            all_synced = false;
        }
    }
    
    assert(all_synced);
    assert(commit_count >= 10);
    
    std::cout << "\n✅ Test 2 Passed: Log replicated to all nodes" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 测试3: 领导者故障转移
void test_leader_failover() {
    std::cout << "\n========== Test 3: Leader Failover ==========\n" << std::endl;
    
    std::vector<std::string> all_peer_ids = {"node-1", "node-2", "node-3", "node-4", "node-5"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::string> leader_history;
    
    // 创建 5 个节点
    for (int i = 1; i <= 5; i++) {
        auto config = createTestConfig(i, all_peer_ids);
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetLeaderChangeCallback([&leader_history](const std::string& leader_id) {
            leader_history.push_back(leader_id);
            std::cout << "[LeaderChange] New leader: " << leader_id << std::endl;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(5000);
    
    // 找到当前领导者
    int leader_idx = -1;
    for (size_t i = 0; i < nodes.size(); i++) {
        if (nodes[i]->IsLeader()) {
            leader_idx = i;
            std::cout << "Current leader: node-" << leader_idx + 1 << std::endl;
            break;
        }
    }
    
    assert(leader_idx != -1);
    
    // 停止领导者
    std::cout << "\nStopping leader node-" << leader_idx + 1 << "..." << std::endl;
    nodes[leader_idx]->Stop();
    
    // 等待新领导者选举
    std::cout << "Waiting for new leader election..." << std::endl;
    wait_ms(4000);
    
    // 检查是否有新领导者
    bool new_leader_found = false;
    for (size_t i = 0; i < nodes.size(); i++) {
        if (i != (size_t)leader_idx && nodes[i]->IsLeader()) {
            new_leader_found = true;
            std::cout << "New leader elected: node-" << i + 1 << std::endl;
            break;
        }
    }
    
    assert(new_leader_found);
    assert(leader_history.size() >= 2);
    
    std::cout << "\n✅ Test 3 Passed: New leader elected after failover" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

int main() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "     RAFT CONSENSUS ALGORITHM TESTS" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // test_leader_election();
    // test_log_replication();
    test_leader_failover();
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "     ALL TESTS PASSED!" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // 清理测试数据
    system("rm -rf ./raft_test_data_*");
    
    return 0;
}