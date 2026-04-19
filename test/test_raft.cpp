/**
 * @file test_raft.cpp
 * @brief Raft 共识算法的测试程序
 *
 * 本文件包含多个测试用例，用于验证和学习 Raft 算法的核心功能：
 * - 单节点基本功能测试
 * - 领导者选举测试
 * - 日志复制测试
 * - 网络分区和故障恢复测试
 * - 持久化和恢复测试
 *
 * 运行方式：
 *   ./test_raft [测试名称]
 *   例如：./test_raft election
 */

#include "raft.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <map>
#include <set>
#include <random>
#include <algorithm>
#include <sstream>
#include <iomanip>

using namespace kvstore;
using namespace std::chrono_literals;

// ============================================================================
// 测试辅助类：模拟 RPC 网络
// ============================================================================

/**
 * @class MockNetwork
 * @brief 模拟 Raft 节点之间的网络通信
 *
 * 支持：
 * - 消息传递（点对点）
 * - 网络延迟模拟
 * - 网络分区模拟
 * - 消息丢失模拟
 */
/**
 * @class MockNetwork
 * @brief 模拟 Raft 节点之间的网络通信
 */
class MockNetwork {
public:
    struct Message {
        int from;
        int to;
        std::string rpc_type;
        std::string data;
        std::chrono::steady_clock::time_point deliver_time;
    };

    MockNetwork() : enabled_(true), loss_rate_(0.0), 
                    delay_min_ms_(0), delay_max_ms_(0),
                    partitioned_(false) {}
    
    // data 格式： term|candidate_id|last_log_index|last_log_term
    void send(int from, int to, const std::string& rpc_type, const std::string& data) {
        if (!enabled_) return;
        
        if (random_double() < loss_rate_) { // 模拟消息丢失
            std::cout << "[Network] Dropped " << rpc_type << " from " << from << " to " << to << std::endl;
            return;
        }
        
        Message msg{from, to, rpc_type, data};
        msg.deliver_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(random_delay()); // 模拟网络延迟
        
        // 4. 加入消息队列
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.push_back(msg);
    }
    
    // 处理所有到达投递时间的消息，调用对应节点的 RPC 处理函数
    void process_messages(std::map<int, RaftNode*>& nodes) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = std::chrono::steady_clock::now();
        
        auto it = messages_.begin();
        while (it != messages_.end()) {
            // 只有到达投递时间的消息才处理
            if (it->deliver_time <= now) { // 时间到了，投递消息
                deliver(*it, nodes);
                it = messages_.erase(it); // 返回值是下一个元素的迭代器
            } else {
                ++it;
            }
        }
    }
    
    void enable() { enabled_ = true; }
    void disable() { enabled_ = false; }
    void set_loss_rate(double rate) { loss_rate_ = rate; }
    void set_delay(int min_ms, int max_ms) { 
        delay_min_ms_ = min_ms; 
        delay_max_ms_ = max_ms; 
    }
    
    // 将节点划分为两组，组间通信被阻断
    void partition(const std::set<int>& group1, const std::set<int>& group2) {
        partition_group1_ = group1;
        partition_group2_ = group2;
        partitioned_ = true;
    }
    
    void heal_partition() {
        partitioned_ = false;
        partition_group1_.clear();
        partition_group2_.clear();
    }
    
    size_t pending_count() const { return messages_.size(); }

private:
    bool enabled_;
    double loss_rate_;
    int delay_min_ms_;
    int delay_max_ms_;
    std::vector<Message> messages_; // 待投递的消息队列
    std::mutex mutex_;
    
    bool partitioned_;
    std::set<int> partition_group1_;
    std::set<int> partition_group2_;

    // 新增：存储投票结果的映射
    std::map<int, int> vote_results;  // candidate_id -> votes_count
    std::map<int, int> vote_terms;    // candidate_id -> term
    
    double random_double() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> dis(0.0, 1.0);
        return dis(gen);
    }
    
    int random_delay() {
        if (delay_min_ms_ >= delay_max_ms_) return delay_min_ms_;
        static std::random_device rd;
        static std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(delay_min_ms_, delay_max_ms_);
        return dis(gen);
    }
    
    void deliver(const Message& msg, std::map<int, RaftNode*>& nodes) {
        // 1. 【分区检查】如果启用了分区，检查消息是否跨分区
        if (partitioned_) {
            bool from_in_g1 = partition_group1_.count(msg.from) > 0; // partition_group1_.count 返回 1 表示 msg.from 在 group1 中，0 表示不在
            bool to_in_g1 = partition_group1_.count(msg.to) > 0; 
            // 发送方和接收方不在同一分区 -> 丢弃消息
            if (from_in_g1 != to_in_g1) {
                return;  // 跨分区消息丢弃
            }
        }
        
        auto it = nodes.find(msg.to); // it 即为接收方节点的迭代器
        if (it == nodes.end()) return; // 接收方节点不存在
        
        // 2. 【RPC 分发】根据 RPC 类型调用对应的处理函数
        if (msg.rpc_type == "RequestVote") { // 处理 RequestVote RPC
            auto args = parse_request_vote_args(msg.data);
            auto reply = it->second->handle_request_vote(args);
            // 自动发送回复
            send(msg.to, msg.from, "RequestVoteReply", serialize_request_vote_reply(reply));
        } else if (msg.rpc_type == "AppendEntries") { // 处理 AppendEntries RPC
            auto args = parse_append_entries_args(msg.data);
            auto reply = it->second->handle_append_entries(args);
            send(msg.to, msg.from, "AppendEntriesReply", serialize_append_entries_reply(reply));
        }
        else if (msg.rpc_type == "RequestVoteReply") {
            // 解析回复
            // 格式: "term|vote_granted"
            std::stringstream ss(msg.data);
            std::string token;
            getline(ss, token, '|');
            uint64_t term = std::stoull(token);
            getline(ss, token, '|');
            bool granted = (token == "1");
            
            if (granted) {
                vote_results[msg.to]++;  // msg.to 是 candidate_id
                std::cout << "[Network] Node " << msg.from << " voted for Node " << msg.to << std::endl;
            }
        }
    }

    int get_votes(int candidate_id) {
        return vote_results[candidate_id] + 1;  // +1 是自己的票
    }
    
    void reset_votes(int candidate_id) {
        vote_results.erase(candidate_id);
    }
    
    // 序列化/反序列化方法...
    RaftNode::RequestVoteArgs parse_request_vote_args(const std::string& data) {
        RaftNode::RequestVoteArgs args;
        std::stringstream ss(data);
        std::string token;
        getline(ss, token, '|'); args.term = std::stoull(token);
        getline(ss, token, '|'); args.candidate_id = std::stoi(token);
        getline(ss, token, '|'); args.last_log_index = std::stoull(token);
        getline(ss, token, '|'); args.last_log_term = std::stoull(token);
        return args;
    }
    
    std::string serialize_request_vote_reply(const RaftNode::RequestVoteReply& reply) {
        return std::to_string(reply.term) + "|" + (reply.vote_granted ? "1" : "0");
    }
    
    RaftNode::AppendEntriesArgs parse_append_entries_args(const std::string& data) {
        RaftNode::AppendEntriesArgs args;
        std::stringstream ss(data);
        std::string token;
        getline(ss, token, '|'); args.term = std::stoull(token);
        getline(ss, token, '|'); args.leader_id = std::stoi(token);
        getline(ss, token, '|'); args.prev_log_index = std::stoull(token);
        getline(ss, token, '|'); args.prev_log_term = std::stoull(token);
        getline(ss, token, '|'); args.leader_commit = std::stoull(token);
        return args;
    }
    
    std::string serialize_append_entries_reply(const RaftNode::AppendEntriesReply& reply) {
        return std::to_string(reply.term) + "|" + (reply.success ? "1" : "0") + "|" + 
               std::to_string(reply.conflict_index);
    }
};

// ============================================================================
// 测试辅助函数
// ============================================================================

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(60, '=') << std::endl;
    std::cout << "  " << title << std::endl;
    std::cout << std::string(60, '=') << std::endl;
}

void print_node_state(int node_id, RaftNode* node) {
    std::string state_str;
    switch (node->get_state()) {
        case RaftState::FOLLOWER: state_str = "FOLLOWER"; break;
        case RaftState::CANDIDATE: state_str = "CANDIDATE"; break;
        case RaftState::LEADER: state_str = "LEADER"; break;
    }
    std::cout << "Node " << node_id << ": " << state_str 
              << ", term=" << node->get_current_term()
              << ", commit_idx=" << node->get_commit_index()
              << ", applied=" << node->get_last_applied() << std::endl;
}

void print_cluster_state(const std::map<int, RaftNode*>& nodes) {
    for (const auto& [id, node] : nodes) {
        print_node_state(id, node);
    }
}

int count_leaders(const std::map<int, RaftNode*>& nodes) {
    int count = 0;
    for (const auto& [_, node] : nodes) {
        if (node->is_leader()) count++;
    }
    return count;
}

int find_leader(const std::map<int, RaftNode*>& nodes) {
    for (const auto& [id, node] : nodes) {
        if (node->is_leader()) return id;
    }
    return -1;
}

// ============================================================================
// 测试用例 1：单节点基本功能
// ============================================================================

/**
 * @brief 测试单节点的基本功能
 * 
 * 验证：
 * - 单节点能否正常启动
 * - 能否正常成为 Leader
 * - 能否正常提议和提交日志
 */
void test_single_node() {
    print_separator("Test 1: Single Node Basic Functionality");
    
    // 1. 创建单节点配置（peer_ids 只包含自己）
    RaftConfig cfg;
    cfg.node_id = 1;
    cfg.peer_ids = {1};
    cfg.data_dir = "./test_data_node1";
    cfg.heartbeat_interval_ms = 100;
    cfg.election_timeout_min_ms = 150;
    cfg.election_timeout_max_ms = 300;
    
    // 清理旧数据
    std::system("rm -rf ./test_data_node1");
    
    RaftNode node(cfg);
    
    int applied_count = 0;

    // 2. 设置应用回调，统计 applied 数量
    // std::function<void(const RaftLogEntry&)>;
    node.set_apply_callback([&applied_count](const RaftLogEntry& entry) {
        applied_count++;
        std::cout << "  Applied: " << entry.command << " " << entry.key << "=" << entry.value << std::endl;
    });
    
    // 3. 等待节点成为 Leader（单节点立即当选）
    std::cout << "Waiting for node to become leader..." << std::endl;

    while (!node.is_leader()) {
        ;
    }

    std::cout << "Node state: " << (node.is_leader() ? "LEADER" : "NOT LEADER") << std::endl;
    assert(node.is_leader() && "Single node should become leader");
    
    // 4. 提议三条命令
    std::cout << "\nProposing commands..." << std::endl;
    uint64_t idx;
    
    node.propose("PUT", "name", "Alice", idx);
    std::this_thread::sleep_for(100ms);
    
    node.propose("PUT", "age", "25", idx);
    std::this_thread::sleep_for(100ms);
    
    node.propose("PUT", "city", "Beijing", idx);
    std::this_thread::sleep_for(200ms);
    
    std::cout << "Applied count: " << applied_count << std::endl;
    assert(applied_count == 3 && "All commands should be applied");
    
    node.stop();
    std::system("rm -rf ./test_data_node1");
    std::cout << "✅ Single node test passed!" << std::endl;
    
}

// ============================================================================
// 测试用例 2：领导者选举
// ============================================================================

/**
 * @brief 测试多节点集群的领导者选举
 * 
 * 验证：
 * - 3个节点能否正确选出一个 Leader
 * - 任期内只有一个 Leader
 * - Leader 宕机后能否选出新 Leader
 */
void test_leader_election() {
    print_separator("Test 2: Leader Election (3 nodes)");
    
    // 清理旧数据
    std::system("rm -rf ./test_data_election_*");
    
    std::map<int, RaftNode*> nodes; // node_id -> RaftNode*
    MockNetwork network;
    
    // 1. 创建 3 节点集群
    for (int i = 1; i <= 3; i++) {
        RaftConfig cfg;
        cfg.node_id = i;
        cfg.peer_ids = {1, 2, 3};
        cfg.data_dir = "./test_data_election_" + std::to_string(i);
        cfg.heartbeat_interval_ms = 100;
        cfg.election_timeout_min_ms = 200;
        cfg.election_timeout_max_ms = 400;
        
        RaftNode* node = new RaftNode(cfg);
        
        // 设置 RPC 发送回调
        node->set_send_rpc_callback([&network, i](int peer_id, const std::string& rpc_type, const std::string& data) {
            network.send(i, peer_id, rpc_type, data);
            return true;
        });
        
        nodes[i] = node; // nodes 是一个 map，key 是 node_id，value 是 RaftNode* 指针
    }
    
    // 运行网络事件循环
    std::cout << "Starting 3-node cluster, waiting for election..." << std::endl;
    
    auto start_time = std::chrono::steady_clock::now();
    int leader_id = -1;
    
    // 2. 事件循环：不断处理消息，直到选出 Leader
    while (std::chrono::steady_clock::now() - start_time < 3s) { // 每3秒检查一次是否选出 Leader
        network.process_messages(nodes);
        
        leader_id = find_leader(nodes);
        if (leader_id != -1) {
            std::cout << "Leader elected: Node " << leader_id << std::endl;
            break;
        }
        
        std::this_thread::sleep_for(10ms);
    }
    
    print_cluster_state(nodes);
    
    // 3. 验证只有一个 Leader
    assert(leader_id != -1 && "No leader elected");
    assert(count_leaders(nodes) == 1 && "Should have exactly one leader");
    
    // 测试 Leader 故障转移
    std::cout << "\n--- Simulating leader failure ---" << std::endl;
    std::cout << "Stopping Node " << leader_id << std::endl;
    
    // 4. 模拟 Leader 宕机
    nodes[leader_id]->stop();
    nodes.erase(leader_id);
    
    std::cout << "Waiting for new leader..." << std::endl;
    start_time = std::chrono::steady_clock::now();
    int new_leader = -1;
    
    // 5. 等待新 Leader 选出
    while (std::chrono::steady_clock::now() - start_time < 2s) {
        network.process_messages(nodes);
        
        new_leader = find_leader(nodes);
        if (new_leader != -1 && new_leader != leader_id) {
            std::cout << "New leader elected: Node " << new_leader << std::endl;
            break;
        }
        
        std::this_thread::sleep_for(10ms);
    }
    
    // 6. 验证新 Leader 选出且只有一个
    print_cluster_state(nodes);
    assert(new_leader != -1 && "No new leader after failure");
    assert(new_leader != leader_id && "New leader should be different");
    assert(count_leaders(nodes) == 1 && "Should have exactly one leader");
    
    // 清理
    for (auto& [_, node] : nodes) {
        node->stop();
        delete node;
    }
    std::system("rm -rf ./test_data_election_*");
    
    std::cout << "✅ Leader election test passed!" << std::endl;
}

// ============================================================================
// 测试用例 3：日志复制
// ============================================================================

/**
 * @brief 测试日志复制功能
 * 
 * 验证：
 * - Leader 能否将日志复制到所有 Follower
 * - 所有节点的提交索引是否一致
 * - 回调是否在所有节点上被调用
 */
void test_log_replication() {
    print_separator("Test 3: Log Replication");
    
    std::system("rm -rf ./test_data_repl_*");
    
    std::map<int, RaftNode*> nodes;

    // 1. 创建 3 节点集群，每个节点记录自己的 apply 次数
    std::map<int, int> apply_counts;
    MockNetwork network;
    
    // 创建3个节点
    for (int i = 1; i <= 3; i++) {
        RaftConfig cfg;
        cfg.node_id = i;
        cfg.peer_ids = {1, 2, 3};
        cfg.data_dir = "./test_data_repl_" + std::to_string(i);
        cfg.heartbeat_interval_ms = 50;
        cfg.election_timeout_min_ms = 200;
        cfg.election_timeout_max_ms = 400;
        
        RaftNode* node = new RaftNode(cfg);
        
        node->set_send_rpc_callback([&network, i](int peer_id, const std::string& rpc_type, const std::string& data) {
            network.send(i, peer_id, rpc_type, data);
            return true;
        });
        
        node->set_apply_callback([i, &apply_counts](const RaftLogEntry& entry) {
            apply_counts[i]++;
            std::cout << "  Node " << i << " applied: " << entry.command << " " << entry.key << "=" << entry.value << " (idx=" << entry.index << ")" << std::endl;
        });
        
        nodes[i] = node;
    }
    
    // 等待选举完成
    std::cout << "Waiting for leader election..." << std::endl;
    int leader_id = -1;
    auto start_time = std::chrono::steady_clock::now();
    
    while (std::chrono::steady_clock::now() - start_time < 3s) {
        network.process_messages(nodes);
        leader_id = find_leader(nodes);
        if (leader_id != -1) break;
        std::this_thread::sleep_for(10ms);
    }
    
    std::cout << "Leader: Node " << leader_id << std::endl;
    assert(leader_id != -1);
    
    // 通过 Leader 提议命令
    std::cout << "\nProposing commands through leader..." << std::endl;
    RaftNode* leader = nodes[leader_id];
    
    uint64_t idx;
    for (int i = 1; i <= 5; i++) {
        std::string key = "key" + std::to_string(i);
        std::string val = "value" + std::to_string(i);
        leader->propose("PUT", key, val, idx);
        std::cout << "  Proposed: PUT " << key << "=" << val << " (idx=" << idx << ")" << std::endl;
        std::this_thread::sleep_for(50ms);
    }
    
    // 等待复制和应用
    std::cout << "\nWaiting for replication..." << std::endl;
    start_time = std::chrono::steady_clock::now();
    
    while (std::chrono::steady_clock::now() - start_time < 2s) {
        network.process_messages(nodes);
        std::this_thread::sleep_for(10ms);
    }
    
    // 验证所有节点都应用了相同数量的命令
    print_cluster_state(nodes);
    std::cout << "\nApply counts per node:" << std::endl;
    for (int i = 1; i <= 3; i++) {
        std::cout << "  Node " << i << ": " << apply_counts[i] << " applied" << std::endl;
    }
    
    // 所有存活节点应该有相同的应用计数
    assert(apply_counts[1] == 5);
    assert(apply_counts[2] == 5);
    assert(apply_counts[3] == 5);
    
    // 清理
    for (auto& [_, node] : nodes) {
        node->stop();
        delete node;
    }
    std::system("rm -rf ./test_data_repl_*");
    
    std::cout << "✅ Log replication test passed!" << std::endl;
}

// ============================================================================
// 测试用例 4：网络分区（脑裂测试）
// ============================================================================

/**
 * @brief 测试网络分区场景
 * 
 * 验证：
 * - 网络分区后 minority 分区不能选出 Leader
 * - majority 分区能正常工作
 * - 分区恢复后数据能同步
 */
void test_network_partition() {
    print_separator("Test 4: Network Partition (Split-Brain Test)");
    
    std::system("rm -rf ./test_data_part_*");
    
    std::map<int, RaftNode*> nodes;
    std::map<int, int> apply_counts;
    MockNetwork network;
    
    // 创建5个节点（更容易观察分区效果）
    for (int i = 1; i <= 5; i++) {
        RaftConfig cfg;
        cfg.node_id = i;
        cfg.peer_ids = {1, 2, 3, 4, 5};
        cfg.data_dir = "./test_data_part_" + std::to_string(i);
        cfg.heartbeat_interval_ms = 50;
        cfg.election_timeout_min_ms = 200;
        cfg.election_timeout_max_ms = 400;
        
        RaftNode* node = new RaftNode(cfg);
        
        node->set_send_rpc_callback([&network, i](int peer_id, const std::string& rpc_type, const std::string& data) {
            network.send(i, peer_id, rpc_type, data);
            return true;
        });
        
        node->set_apply_callback([i, &apply_counts](const RaftLogEntry& entry) {
            apply_counts[i]++;
        });
        
        nodes[i] = node;
    }
    
    // 等待选举
    std::cout << "Waiting for initial leader election..." << std::endl;
    auto start_time = std::chrono::steady_clock::now();
    int initial_leader = -1;
    
    while (std::chrono::steady_clock::now() - start_time < 3s) {
        network.process_messages(nodes);
        initial_leader = find_leader(nodes);
        if (initial_leader != -1) break;
        std::this_thread::sleep_for(10ms);
    }
    
    std::cout << "Initial leader: Node " << initial_leader << std::endl;
    
    // 写入一些数据
    if (initial_leader != -1) {
        uint64_t idx;
        nodes[initial_leader]->propose("PUT", "before_partition", "1", idx);
        std::this_thread::sleep_for(200ms);
    }
    
    // 创建网络分区：{1,2,3} 和 {4,5}
    std::cout << "\n--- Creating network partition ---" << std::endl;
    std::cout << "Partition 1: Nodes {1,2,3}, Partition 2: Nodes {4,5}" << std::endl;
    network.partition({1, 2, 3}, {4, 5});
    
    std::this_thread::sleep_for(1s);
    
    // 检查分区后的状态
    std::cout << "\nState after partition:" << std::endl;
    print_cluster_state(nodes);
    
    // Majority 分区应该能正常工作
    std::cout << "\n--- Testing majority partition ---" << std::endl;
    int majority_leader = -1;
    for (int i : {1, 2, 3}) {
        if (nodes[i]->is_leader()) {
            majority_leader = i;
            break;
        }
    }
    
    if (majority_leader != -1) {
        std::cout << "Majority partition has leader: Node " << majority_leader << std::endl;
        uint64_t idx;
        nodes[majority_leader]->propose("PUT", "in_partition", "majority", idx);
        std::this_thread::sleep_for(200ms);
    }
    
    // 恢复网络
    std::cout << "\n--- Healing network partition ---" << std::endl;
    network.heal_partition();
    
    // 等待恢复
    std::this_thread::sleep_for(2s);
    network.process_messages(nodes);
    
    std::cout << "\nState after healing:" << std::endl;
    print_cluster_state(nodes);
    
    // 验证最终只有一个 Leader
    int final_leaders = count_leaders(nodes);
    std::cout << "Final leader count: " << final_leaders << std::endl;
    
    // 清理
    for (auto& [_, node] : nodes) {
        node->stop();
        delete node;
    }
    std::system("rm -rf ./test_data_part_*");
    
    std::cout << "✅ Network partition test passed!" << std::endl;
}

// ============================================================================
// 测试用例 5：持久化和恢复
// ============================================================================

/**
 * @brief 测试持久化和崩溃恢复
 * 
 * 验证：
 * - 节点重启后能恢复状态
 * - 持久化的日志不会丢失
 */
void test_persistence() {
    print_separator("Test 5: Persistence and Recovery");
    
    std::system("rm -rf ./test_data_persist");
    
    // 第一阶段：创建节点并写入数据
    std::cout << "Phase 1: Creating node and writing data..." << std::endl;
    
    RaftConfig cfg;
    cfg.node_id = 1;
    cfg.peer_ids = {1};
    cfg.data_dir = "./test_data_persist";
    cfg.election_timeout_min_ms = 100;
    cfg.election_timeout_max_ms = 200;
    
    {
        RaftNode node(cfg);
        
        int applied = 0;
        node.set_apply_callback([&applied](const RaftLogEntry& entry) {
            applied++;
        });
        
        std::this_thread::sleep_for(300ms);
        
        uint64_t idx;
        node.propose("PUT", "persist_key1", "value1", idx);
        node.propose("PUT", "persist_key2", "value2", idx);
        
        std::this_thread::sleep_for(200ms);
        
        std::cout << "  Term before stop: " << node.get_current_term() << std::endl;
        std::cout << "  Applied before stop: " << applied << std::endl;
        
        node.stop();
    }
    
    // 第二阶段：重新创建节点（模拟重启）
    std::cout << "\nPhase 2: Restarting node..." << std::endl;
    
    {
        RaftNode node(cfg);
        
        int applied = 0;
        std::vector<RaftLogEntry> applied_entries;
        node.set_apply_callback([&applied, &applied_entries](const RaftLogEntry& entry) {
            applied++;
            applied_entries.push_back(entry);
        });
        
        std::this_thread::sleep_for(300ms);
        
        std::cout << "  Term after restart: " << node.get_current_term() << std::endl;
        std::cout << "  Commit index: " << node.get_commit_index() << std::endl;
        std::cout << "  Applied after restart: " << applied << std::endl;
        
        // 验证数据被恢复
        std::cout << "\n  Recovered entries:" << std::endl;
        for (const auto& e : applied_entries) {
            std::cout << "    " << e.command << " " << e.key << "=" << e.value 
                      << " (idx=" << e.index << ")" << std::endl;
        }
        
        assert(applied >= 2 && "Should recover at least 2 entries");
        
        node.stop();
    }
    
    std::system("rm -rf ./test_data_persist");
    std::cout << "✅ Persistence test passed!" << std::endl;
}

// ============================================================================
// 测试用例 6：交互式学习模式
// ============================================================================

/**
 * @brief 交互式测试，用于学习和观察 Raft 行为
 * 
 * 支持的命令：
 * - start N: 启动 N 个节点的集群
 * - stop M: 停止节点 M
 * - propose K V: 向 Leader 提议 PUT 命令
 * - state: 显示所有节点状态
 * - partition A,B,C / D,E: 创建网络分区
 * - heal: 恢复网络分区
 * - quit: 退出
 */
void test_interactive() {
    print_separator("Interactive Raft Learning Mode");
    
    std::cout << "Available commands:\n"
              << "  start <N>           - Start a cluster with N nodes\n"
              << "  stop <node_id>       - Stop a specific node\n"
              << "  put <key> <value>   - Propose a PUT command\n"
              << "  state               - Show cluster state\n"
              << "  partition <ids>/<ids> - Create network partition\n"
              << "  heal                - Heal network partition\n"
              << "  quit                - Exit\n"
              << std::endl;
    
    std::map<int, RaftNode*> nodes;
    MockNetwork network;
    std::map<int, int> apply_counts;
    
    auto create_cluster = [&](int n) {
        // 清理旧节点
        for (auto& [_, node] : nodes) {
            node->stop();
            delete node;
        }
        nodes.clear();
        apply_counts.clear();
        
        std::system("rm -rf ./test_data_interactive_*");
        
        std::vector<int> peer_ids;
        for (int i = 1; i <= n; i++) peer_ids.push_back(i);
        
        for (int i = 1; i <= n; i++) {
            RaftConfig cfg;
            cfg.node_id = i;
            cfg.peer_ids = peer_ids;
            cfg.data_dir = "./test_data_interactive_" + std::to_string(i);
            cfg.heartbeat_interval_ms = 100;
            cfg.election_timeout_min_ms = 200;
            cfg.election_timeout_max_ms = 400;
            
            RaftNode* node = new RaftNode(cfg);
            
            node->set_send_rpc_callback([&network, i](int peer_id, const std::string& rpc_type, const std::string& data) {
                network.send(i, peer_id, rpc_type, data);
                return true;
            });
            
            node->set_apply_callback([i, &apply_counts](const RaftLogEntry& entry) {
                apply_counts[i]++;
                std::cout << "\n[Node " << i << "] Applied: " << entry.command 
                          << " " << entry.key << "=" << entry.value 
                          << " (idx=" << entry.index << ")\n> " << std::flush;
            });
            
            nodes[i] = node;
        }
        
        std::cout << "Cluster with " << n << " nodes created.\n";
    };
    
    std::string line;
    std::cout << "> " << std::flush;
    
    while (std::getline(std::cin, line)) {
        network.process_messages(nodes);
        
        std::stringstream ss(line);
        std::string cmd;
        ss >> cmd;
        
        if (cmd == "start") {
            int n;
            ss >> n;
            if (n > 0) create_cluster(n);
        }
        else if (cmd == "stop") {
            int id;
            ss >> id;
            if (nodes.count(id)) {
                nodes[id]->stop();
                delete nodes[id];
                nodes.erase(id);
                std::cout << "Node " << id << " stopped.\n";
            }
        }
        else if (cmd == "put") {
            std::string key, value;
            ss >> key >> value;
            
            int leader = find_leader(nodes);
            if (leader != -1) {
                uint64_t idx;
                if (nodes[leader]->propose("PUT", key, value, idx)) {
                    std::cout << "Proposed to leader (Node " << leader << "), idx=" << idx << "\n";
                } else {
                    std::cout << "Propose failed\n";
                }
            } else {
                std::cout << "No leader available\n";
            }
        }
        else if (cmd == "state") {
            print_cluster_state(nodes);
        }
        else if (cmd == "partition") {
            std::string part1, part2;
            ss >> part1 >> part2;
            
            std::set<int> g1, g2;
            std::stringstream ss1(part1);
            std::string token;
            while (std::getline(ss1, token, ',')) {
                g1.insert(std::stoi(token));
            }
            std::stringstream ss2(part2);
            while (std::getline(ss2, token, ',')) {
                g2.insert(std::stoi(token));
            }
            
            network.partition(g1, g2);
            std::cout << "Partition created.\n";
        }
        else if (cmd == "heal") {
            network.heal_partition();
            std::cout << "Partition healed.\n";
        }
        else if (cmd == "quit" || cmd == "exit") {
            break;
        }
        else if (!cmd.empty()) {
            std::cout << "Unknown command: " << cmd << "\n";
        }
        
        std::cout << "> " << std::flush;
        std::this_thread::sleep_for(50ms);
    }
    
    // 清理
    for (auto& [_, node] : nodes) {
        node->stop();
        delete node;
    }
    std::system("rm -rf ./test_data_interactive_*");
}

// ============================================================================
// 主函数
// ============================================================================

void print_usage(const char* prog) {
    std::cout << "Usage: " << prog << " [test_name]\n"
              << "Available tests:\n"
              << "  all         - Run all automated tests\n"
              << "  single      - Single node test\n"
              << "  election    - Leader election test\n"
              << "  replication - Log replication test\n"
              << "  partition   - Network partition test\n"
              << "  persistence - Persistence and recovery test\n"
              << "  interactive - Interactive learning mode\n"
              << std::endl;
}

int main(int argc, char* argv[]) {
    std::cout << "Raft Consensus Algorithm Test Suite\n";
    std::cout << "====================================\n";
    
    try {
        // test_single_node();
        test_leader_election();
        // test_log_replication();
        // test_network_partition();
        // test_persistence();
        
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "All tests completed!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}