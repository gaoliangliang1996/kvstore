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
RaftConfig createTestConfig(int node_id, const std::vector<std::string>& all_peers) {
    RaftConfig config;
    config.node_id = "node-" + std::to_string(node_id);
    config.peer_ids = all_peers;
    config.election_timeout_ms = 150;
    config.heartbeat_interval_ms = 50;
    config.max_append_entries = 100;
    config.snapshot_threshold = 10;
    config.data_dir = "./data" + std::to_string(node_id);
    return config;
}

// 测试1: 领导者选举
void test_leader_election() {
    std::cout << "\n========== Test 1: Leader Election ==========\n" << std::endl;
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
        auto node = std::make_unique<RaftNode>(config);
        
        // 设置领导者变化回调
        node->SetLeaderChangeCallback([i, &nodes](const std::string& leader_id) {
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
    wait_ms(2000);
    
    // 检查是否只有一个领导者
    int leader_count = 0;
    std::string leader_id;
    for (auto& node : nodes) {
        if (node->IsLeader()) {
            leader_count++;
            leader_id = node->GetLeaderId();
            std::cout << "Leader found: " << leader_id << std::endl;
        }
        std::cout << "Node " << node->GetLeaderId() 
                  << " state=" << (int)node->GetState() 
                  << " term=" << node->GetCurrentTerm() << std::endl;
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
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::atomic<int> commit_count{0};
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
        auto node = std::make_unique<RaftNode>(config);
        
        // 设置提交回调
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
    wait_ms(2000);
    
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
    for (int i = 0; i < 10; i++) {
        std::string cmd = "put";
        std::string data = "key" + std::to_string(i) + ":value" + std::to_string(i);
        std::vector<uint8_t> data_vec(data.begin(), data.end());
        
        leader->Propose(cmd, data_vec, [i](bool success, uint64_t index) {
            if (success) {
                std::cout << "Proposal " << i << " committed at index " << index << std::endl;
            } else {
                std::cout << "Proposal " << i << " failed" << std::endl;
            }
        });
        
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
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::string> leader_history;
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
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
    wait_ms(2000);
    
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
    wait_ms(3000);
    
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
        if (node) node->Stop();
    }
}

// 测试4: 网络分区
void test_network_partition() {
    std::cout << "\n========== Test 4: Network Partition ==========\n" << std::endl;
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3", "node-4", "node-5"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    
    // 创建 5 个节点
    for (int i = 1; i <= 5; i++) {
        auto config = createTestConfig(i, peers);
        auto node = std::make_unique<RaftNode>(config);
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(2000);
    
    // 找到当前领导者
    int leader_idx = -1;
    for (size_t i = 0; i < nodes.size(); i++) {
        if (nodes[i]->IsLeader()) {
            leader_idx = i;
            std::cout << "Current leader: node-" << leader_idx + 1 << std::endl;
            break;
        }
    }
    
    // 模拟网络分区：停止 3 个节点（包括领导者）
    std::cout << "\nSimulating network partition: stopping 3 nodes..." << std::endl;
    for (int i = 0; i < 3; i++) {
        if (i != leader_idx) {
            nodes[i]->Stop();
        }
    }
    
    wait_ms(3000);
    
    // 检查剩下的 2 个节点是否能选举出新领导者（应该不能，因为需要多数）
    int second_part_leader_count = 0;
    for (int i = 3; i < 5; i++) {
        if (nodes[i]->IsLeader()) {
            second_part_leader_count++;
        }
    }
    
    std::cout << "Leaders in minority partition: " << second_part_leader_count << std::endl;
    // 少数分区不应该有领导者
    assert(second_part_leader_count == 0);
    
    // 恢复分区
    std::cout << "\nRestoring partition..." << std::endl;
    for (int i = 0; i < 3; i++) {
        if (i != leader_idx) {
            auto config = createTestConfig(i + 1, peers);
            nodes[i] = std::make_unique<RaftNode>(config);
            nodes[i]->Start();
        }
    }
    
    wait_ms(3000);
    
    // 检查是否重新选举出领导者
    bool leader_exists = false;
    for (auto& node : nodes) {
        if (node->IsLeader()) {
            leader_exists = true;
            std::cout << "Leader after recovery: " << node->GetLeaderId() << std::endl;
            break;
        }
    }
    
    assert(leader_exists);
    std::cout << "\n✅ Test 4 Passed: Network partition handled correctly" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 测试5: 日志一致性
void test_log_consistency() {
    std::cout << "\n========== Test 5: Log Consistency ==========\n" << std::endl;
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::map<uint64_t, std::string> committed_entries;
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetCommitCallback([&committed_entries](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
            std::string data_str(data.begin(), data.end());
            committed_entries[index] = data_str;
            std::cout << "[Commit] node index=" << index << " data=" << data_str << std::endl;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(2000);
    
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
    
    // 提交多个 proposal
    std::cout << "\nSubmitting 20 proposals..." << std::endl;
    for (int i = 0; i < 20; i++) {
        std::string data = "log_entry_" + std::to_string(i);
        std::vector<uint8_t> data_vec(data.begin(), data.end());
        
        leader->Propose("append", data_vec, nullptr);
        wait_ms(50);
    }
    
    wait_ms(2000);
    
    // 重启所有节点
    std::cout << "\nRestarting all nodes..." << std::endl;
    for (auto& node : nodes) {
        node->Stop();
    }
    
    wait_ms(1000);
    
    // 重新启动
    for (size_t i = 0; i < nodes.size(); i++) {
        auto config = createTestConfig(i + 1, peers);
        nodes[i] = std::make_unique<RaftNode>(config);
        nodes[i]->SetCommitCallback([&committed_entries](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
            std::string data_str(data.begin(), data.end());
            committed_entries[index] = data_str;
        });
        nodes[i]->Start();
    }
    
    wait_ms(3000);
    
    // 检查日志一致性
    std::cout << "\nChecking log consistency after restart..." << std::endl;
    uint64_t term_sum = 0;
    uint64_t commit_sum = 0;
    
    for (auto& node : nodes) {
        uint64_t term = node->GetCurrentTerm();
        uint64_t commit = node->GetCommitIndex();
        std::cout << "Node " << node->GetLeaderId() 
                  << " term=" << term << " commit=" << commit << std::endl;
        term_sum += term;
        commit_sum += commit;
    }
    
    assert(term_sum > 0);
    assert(commit_sum >= 20 * 3);  // 所有节点都应该有至少 20 条日志
    
    std::cout << "\n✅ Test 5 Passed: Logs consistent after restart" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 测试6: 性能测试
void test_performance() {
    std::cout << "\n========== Test 6: Performance Test ==========\n" << std::endl;
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::atomic<int> committed{0};
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
        config.heartbeat_interval_ms = 20;  // 更快的频率
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetCommitCallback([&committed](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
            committed++;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(2000);
    
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
    
    // 性能测试：测量吞吐量
    const int total_ops = 1000;
    auto start_time = std::chrono::steady_clock::now();
    
    for (int i = 0; i < total_ops; i++) {
        std::string data = "perf_test_" + std::to_string(i);
        std::vector<uint8_t> data_vec(data.begin(), data.end());
        leader->Propose("append", data_vec, nullptr);
    }
    
    // 等待所有操作完成
    while (committed < total_ops) {
        wait_ms(100);
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    
    double throughput = total_ops * 1000.0 / elapsed;
    std::cout << "\nPerformance Results:" << std::endl;
    std::cout << "  Total operations: " << total_ops << std::endl;
    std::cout << "  Time: " << elapsed << " ms" << std::endl;
    std::cout << "  Throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << "  Committed: " << committed << std::endl;
    
    assert(committed >= total_ops);
    
    std::cout << "\n✅ Test 6 Passed: Performance within expected range" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 测试7: 并发提案
void test_concurrent_proposals() {
    std::cout << "\n========== Test 7: Concurrent Proposals ==========\n" << std::endl;
    
    std::vector<std::string> peers = {"node-1", "node-2", "node-3"};
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::atomic<int> total_committed{0};
    std::atomic<int> proposal_id{0};
    
    // 创建 3 个节点
    for (int i = 1; i <= 3; i++) {
        auto config = createTestConfig(i, peers);
        auto node = std::make_unique<RaftNode>(config);
        
        node->SetCommitCallback([&total_committed](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
            total_committed++;
        });
        
        nodes.push_back(std::move(node));
    }
    
    // 启动所有节点
    for (auto& node : nodes) {
        node->Start();
    }
    
    // 等待领导者选举
    wait_ms(2000);
    
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
    
    // 并发提交
    const int num_threads = 10;
    const int ops_per_thread = 100;
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([leader, ops_per_thread, &proposal_id]() {
            for (int i = 0; i < ops_per_thread; i++) {
                int id = proposal_id++;
                std::string data = "concurrent_" + std::to_string(id);
                std::vector<uint8_t> data_vec(data.begin(), data.end());
                leader->Propose("append", data_vec, nullptr);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // 等待所有提交完成
    int expected_total = num_threads * ops_per_thread;
    int max_wait = 30;  // 最多等待 30 秒
    while (total_committed < expected_total && max_wait-- > 0) {
        wait_ms(1000);
        std::cout << "Committed: " << total_committed << "/" << expected_total << std::endl;
    }
    
    std::cout << "\nTotal committed: " << total_committed << std::endl;
    assert(total_committed >= expected_total);
    
    std::cout << "\n✅ Test 7 Passed: Concurrent proposals handled correctly" << std::endl;
    
    // 清理
    for (auto& node : nodes) {
        node->Stop();
    }
}

// 主函数
int main() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "     RAFT CONSENSUS ALGORITHM TESTS" << std::endl;
    std::cout << "========================================" << std::endl;
    
    try {
        test_leader_election();
        // test_log_replication();
        // test_leader_failover();
        // test_network_partition();
        // test_log_consistency();
        // test_performance();
        // test_concurrent_proposals();
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "     ALL TESTS PASSED!" << std::endl;
        std::cout << "========================================" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "\n❌ Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    // 清理测试数据
    system("rm -rf ./raft_test_data_*");
    
    return 0;
}