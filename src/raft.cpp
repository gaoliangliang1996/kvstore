/**
 * @file raft.cpp
 * @brief Raft 共识算法的核心实现文件。
 *
 * 本文件实现了 Raft 分布式共识算法的核心逻辑，包括：
 * - RaftNode 类的完整实现：领导者选举、日志复制、状态机应用
 * - RaftCluster 类的实现：集群管理和 RPC 通信
 * - 日志条目和持久化状态的序列化/反序列化
 * - 各种 RPC 请求的处理（RequestVote 和 AppendEntries）
 *
 * Raft 算法保证了分布式系统中的一致性，通过领导者选举和日志复制
 * 来确保所有节点的状态机以相同的顺序执行相同的命令。
 */

#include "raft.h"
#include <fstream>
#include <random>
#include <chrono>
#include <iostream>
#include <sstream>
#include <filesystem>

namespace kvstore {

using std::stringstream;

// 本文件实现 RaftNode 和 RaftCluster 的核心逻辑，包括状态转换、日志复制、选举等功能。

// ============== RaftLogEntry 序列化 ==============
// 日志条目格式：term|index|command|key|value
/**
 * @brief 将日志条目序列化为字符串格式。
 *
 * 序列化格式为：term|index|command|key|value
 * 使用管道符(|)作为字段分隔符。
 *
 * @return 序列化后的字符串。
 */
string RaftLogEntry::serialize() const {
    stringstream ss;
    ss << term << "|" << index << "|" << command << "|" << key << "|" << value;
    return ss.str();
}

/**
 * @brief 从字符串反序列化日志条目。
 *
 * 从给定的序列化字符串中解析出 term、index、command、key 和 value 字段。
 *
 * @param data 包含序列化数据的字符串。
 * @return 反序列化后的 RaftLogEntry 对象。
 */
RaftLogEntry RaftLogEntry::deserialize(const string& data) {
    RaftLogEntry entry;
    stringstream ss(data);
    string token;
    
    getline(ss, token, '|');
    entry.term = stoull(token);
    getline(ss, token, '|');
    entry.index = stoull(token);
    getline(ss, token, '|');
    entry.command = token;
    getline(ss, token, '|');
    entry.key = token;
    getline(ss, token, '|');
    entry.value = token;
    
    return entry;
}

// ============== RaftPersistentState 序列化 ==============
// 持久化状态格式： current_term|voted_for|log1;log2;log3;...
/**
 * @brief 将持久化状态序列化为字符串格式。
 *
 * 序列化格式为：current_term|voted_for|log1;log2;log3;...
 * 日志条目之间用分号(;)分隔，每个日志条目使用其自己的序列化格式。
 *
 * @return 序列化后的字符串。
 */
string RaftPersistentState::serialize() const {
    stringstream ss;
    ss << current_term << "|" << voted_for << "|";
    for (const auto& log : logs) { // 每个日志条目用分号分隔
        ss << log.serialize() << ";";
    }
    return ss.str();
}

/**
 * @brief 从字符串反序列化持久化状态。
 *
 * 从给定的序列化字符串中解析出 current_term、voted_for 和日志列表。
 *
 * @param data 包含序列化数据的字符串。
 */
void RaftPersistentState::deserialize(const string& data) {
    logs.clear();
    stringstream ss(data);
    string token;
    
    getline(ss, token, '|');
    current_term = stoull(token);
    getline(ss, token, '|');
    voted_for = stoull(token);
    getline(ss, token, '|');
    
    string log_str;
    while (getline(ss, log_str, ';')) {
        if (!log_str.empty()) {
            logs.push_back(RaftLogEntry::deserialize(log_str));
        }
    }
}

// ============== RaftNode 实现 ==============
/**
 * @brief RaftNode 构造函数，初始化 Raft 节点状态并启动后台线程。
 *
 * 初始化过程包括：
 * - 设置节点配置和初始状态
 * - 加载持久化状态和日志
 * - 初始化领导者状态变量
 * - 重置选举超时时间
 * - 启动后台运行线程
 *
 * @param cfg Raft 节点配置，包含节点 ID、对等节点列表、超时参数等。
 */
RaftNode::RaftNode(const RaftConfig& cfg) : config(cfg), node_id(cfg.node_id), running(true) {
    
    // 初始化状态
    state = RaftState::FOLLOWER;
    commit_index = 0;
    last_applied = 0;
    applied_count = 0;
    
    // 加载持久化状态
    load_persistent_state();
    load_logs();
    
    // 初始化领导者状态
    next_index.resize(config.peer_ids.size() + 1);  // 节点 ID 从 1 开始，0 位置不使用
    match_index.resize(config.peer_ids.size() + 1); // 节点 ID 从 1 开始，0 位置不使用
    
    // 重置选举超时
    reset_election_timeout();
    
    // 启动后台线程
    background_thread = std::thread(&RaftNode::run, this);
    
    std::cout << "[Raft] Node " << node_id << " started as FOLLOWER, term=" << persistent_state.current_term << std::endl;
}

/**
 * @brief RaftNode 析构函数，停止后台线程并清理资源。
 *
 * 设置运行标志为 false，通知条件变量唤醒后台线程，然后等待线程结束。
 */
RaftNode::~RaftNode() {
    running = false;
    cv.notify_all();
    if (background_thread.joinable()) {
        background_thread.join();
    }
}

/**
 * @brief Raft 节点的主运行循环。
 *
 * 根据当前节点状态执行不同的逻辑：
 * - FOLLOWER: 等待选举超时或心跳，如果超时则发起选举
 * - CANDIDATE: 发起选举过程
 * - LEADER: 定期发送心跳 维持领导地位
 *
 * 循环持续直到 running 标志被设置为 false。
 */
void RaftNode::run() {
    while (running) {
        std::unique_lock<std::mutex> lock(mutex);
        
        switch (state) {
            // 【核心等待】等待超时或收到心跳信号
            // cv.wait_until 会阻塞线程，直到：
            //   1. 到达 election_timeout（超时）
            //   2. 被 cv.notify_all() 唤醒（收到 RPC 消息）
            case RaftState::FOLLOWER:
                // 等待选举超时或收到心跳
                if (cv.wait_until(lock, election_timeout) == std::cv_status::timeout) {
                    // 超时意味着长时间没收到 Leader 心跳 -> 发起选举
                    if (running) {
                        become_candidate();
                    }
                }
                // 如果被唤醒，说明收到了 RPC，循环会再次进入 wait_until 等待下一个超时
                break;
                
            case RaftState::CANDIDATE:
                // 候选者立即开始选举
                start_election();
                break;
                
            case RaftState::LEADER:
                // 【定期心跳】等待到下次心跳时间
                if (cv.wait_until(lock, last_heartbeat + std::chrono::milliseconds(config.heartbeat_interval_ms)) == std::cv_status::timeout) { // 到达心跳时间
                    // 发送心跳，维持领导地位
                    send_heartbeats();
                    last_heartbeat = std::chrono::steady_clock::now();
                }
                break;
        }
    }
}

/**
 * @brief 将节点状态转换为跟随者。
 *
 * 当收到更高任期的消息时，节点会转换为跟随者状态。
 * 如果新任期大于当前任期，则重置投票记录并持久化状态。
 *
 * @param term 新的任期号。
 */
void RaftNode::become_follower(uint64_t term) {
    state = RaftState::FOLLOWER;
    
    // 只有当请求的任期 > 本地记录 时，才更新任期并清空投票纪录
    if (term > persistent_state.current_term) {
        persistent_state.current_term = term;
        persistent_state.voted_for = 0; // 新任期，旧票作废，可以重新投票
        persist_state();                // 立即刷盘，保证崩溃恢复后任期正确
    }
    
    // 重置随机定时器，防止刚变成 Follower 立即又超时选举
    reset_election_timeout();
    std::cout << "[Raft] Node " << node_id << " became FOLLOWER, term=" << persistent_state.current_term << std::endl;
}

/**
 * @brief 将节点状态转换为候选者。
 *
 * 候选者状态是选举过程中的中间状态：
 * - 增加当前任期
 * - 给自己投票
 * - 持久化状态
 * - 重置选举超时
 */
void RaftNode::become_candidate() {
    // 1. 状态变更为候选者
    state = RaftState::CANDIDATE;

    // 2. 自增任期号，这是发起选举的标志
    persistent_state.current_term++;

    // 3. 【关键】先投票给自己，这是合法的第一票
    persistent_state.voted_for = node_id;

    // 4. 立即持久化，防止重启后在同一任期再次发起选举导致分裂投票
    persist_state();
    
    // 5. 重置随机超时，为等待投票结果做准备
    reset_election_timeout();
    
    std::cout << "[Raft] Node " << node_id << " became CANDIDATE, term=" << persistent_state.current_term << std::endl;
}

/**
 * @brief 将节点状态转换为领导者。
 *
 * 当候选者赢得选举后，成为领导者：
 * - 初始化 next_index 和 match_index 数组
 * - 设置最后心跳时间
 * - 立即发送心跳以确立领导地位
 */
void RaftNode::become_leader() {
    // 1. 状态升级为领导者
    state = RaftState::LEADER;
    
    // 2. 【关键】初始化领导者独有的易失性状态数组
    //    next_index: 记录下一个要发给每个 Follower 的日志索引
    //    match_index: 记录每个 Follower 已经确认复制的最高日志索引
    uint64_t last_log_index = persistent_state.logs.empty() ? 0 : persistent_state.logs.back().index;
    for (size_t i = 0; i <= config.peer_ids.size(); i++) {
        // 初始化为乐观值：假设所有 Follower 都和 Leader 日志一样新
        next_index[i] = last_log_index + 1;
        match_index[i] = 0;
    }
    
    // 3. 记录就任时间
    last_heartbeat = std::chrono::steady_clock::now();
    
    std::cout << "[Raft] Node " << node_id << " became LEADER, term=" << persistent_state.current_term << std::endl;
    
    // 4. 【立即行使权力】发送一轮心跳，宣告自己的领导地位，阻止其他节点超时选举
    send_heartbeats();
}

/**
 * @brief 重置选举超时时间。
 *
 * 使用随机超时时间（在配置的 min-max 范围内）来避免选举冲突。
 * 跟随者使用此超时来检测领导者故障，候选者使用此超时等待选举结果。
 */
void RaftNode::reset_election_timeout() {
    // 1. 使用静态变量保证随机数引擎只初始化一次（线程安全由调用方的 mutex 保证）
    static std::random_device rd;
    static std::mt19937 gen(rd());

    // 2. 在配置的范围内生成随机毫秒数
    std::uniform_int_distribution<> dis(config.election_timeout_min_ms, 
                                        config.election_timeout_max_ms);
    
    int timeout_ms = dis(gen);

    // 3. 计算超时的绝对时间点，用于条件变量的 wait_until
    election_timeout = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
}

/**
 * @brief 开始选举过程。
 *
 * 候选者向所有对等节点发送 RequestVote 请求：
 * - 构造投票请求参数（当前任期、候选者ID、最后日志信息）
 * - 给自己投票（初始1票）
 * - 向其他节点发送投票请求
 * - 等待并检查是否获得多数票
 *
 * 如果获得多数票，成为领导者；否则回到跟随者状态。
 */
void RaftNode::start_election() {
    std::cout << "[Raft] Node " << node_id << " starting election for term " << persistent_state.current_term << std::endl;
    
    // 1. 构造投票请求参数
    RequestVoteArgs args;
    args.term = persistent_state.current_term;
    args.candidate_id = node_id;

    // 携带最后一条日志的元数据，供对方判断日志新旧
    args.last_log_index = persistent_state.logs.empty() ? 0 : persistent_state.logs.back().index;
    args.last_log_term = persistent_state.logs.empty() ? 0 : persistent_state.logs.back().term;
    
    // 2. 投票给自己 （已在 become_candidate 中记录）
    int votes = 1;

    if (config.peer_ids.size() == 1) {
        // 单节点直接成为 Leader，不需要网络投票
        become_leader();
        return;
    }
    
    // 3. 向所有 Peer 并行发送投票请求
    for (int peer_id : config.peer_ids) {
        if (peer_id == node_id) continue;
        
        // 发送投票请求
        if (send_rpc_callback) {
            // 将参数序列化为字符串发送
            string data = std::to_string(args.term) + "|" + 
                         std::to_string(args.candidate_id) + "|" +
                         std::to_string(args.last_log_index) + "|" +
                         std::to_string(args.last_log_term); // data 格式： term|candidate_id|last_log_index|last_log_term
            
            send_rpc_callback(peer_id, "RequestVote", data);
        }
    }
    
    // 4. 【简化处理】等待投票结果
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // 5. 检查是否获得多数票
    int majority = (config.peer_ids.size() + 1) / 2 + 1;
    if (votes >= majority) {
        become_leader();
    } else { // 选举失败，退回 Follower（可能发现已有 Leader 当选）
        become_follower(persistent_state.current_term);
    }
}

// 心跳就是空的 AppendEntries 。如果 entries 为空，这就是纯心跳；如果有条目，就是日志复制。
void RaftNode::send_heartbeats() {
    if (state != RaftState::LEADER) return; // 只有 leader 才能发送心跳
    
    // 1. 构造 AppendEntries 请求的公共部分
    AppendEntriesArgs args;
    args.term = persistent_state.current_term;
    args.leader_id = node_id;
    args.leader_commit = commit_index;
    
    // 2. 为每个 Follower 单独构造请求
    for (int peer_id : config.peer_ids) {
        if (peer_id == node_id) continue;
        
        // 2.1 确定 prev_log_index （该 Follower 已有日志的最后一条索引）
        uint64_t prev_log_idx = next_index[peer_id] - 1;
        args.prev_log_index = prev_log_idx;
        args.prev_log_term = 0;
        
        // 2.2 获取 prev_log_term （用于一致性校验）
        if (prev_log_idx > 0 && prev_log_idx <= persistent_state.logs.size()) {
            args.prev_log_term = persistent_state.logs[prev_log_idx - 1].term;
        }
        
        // 2.3 打包需要发送的日志条目（从 next_index 开始到最新）
        args.entries.clear();
        for (uint64_t i = next_index[peer_id]; i <= persistent_state.logs.size(); i++) {
            args.entries.push_back(persistent_state.logs[i - 1]);
        }
        
        // 2.4 序列化并发送
        if (send_rpc_callback) {
            string data = std::to_string(args.term) + "|" +
                         std::to_string(args.leader_id) + "|" +
                         std::to_string(args.prev_log_index) + "|" +
                         std::to_string(args.prev_log_term) + "|" +
                         std::to_string(args.leader_commit) + "|";
            
            for (const auto& entry : args.entries) {
                data += entry.serialize() + ";";
            }
            
            send_rpc_callback(peer_id, "AppendEntries", data);
        }
    }
}

/**
 * @brief 处理来自候选者的投票请求。
 *
 * 根据 Raft 论文的投票规则决定是否授予投票：
 * 1. 如果请求任期更大，转为跟随者
 * 2. 检查是否已投票给其他候选者
 * 3. 检查候选者的日志是否至少和自己一样新
 *
 * @param args 投票请求参数。
 * @return 投票回复，包含当前任期和投票结果。
 */

// 当收到候选人的拉票请求时，Follower 根据 Raft 论文的投票规则决定是否授予投票：
// 1. 如果请求的任期比当前任期大，转为 Follower
// 2. 如果已经投票给其他候选人，拒绝投票
// 3. 如果候选人的日志至少和自己一样新，授予投票
RaftNode::RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mutex);
    
    RequestVoteReply reply;
    reply.term = persistent_state.current_term;
    reply.vote_granted = false;
    
    // 规则 1：任期小 -> 直接拒绝；任期大 -> 立刻下台成为 Follower
    if (args.term > persistent_state.current_term) {
        become_follower(args.term);
    }
    
    // 规则 2：检查是否已经投过票给别人了
    //        voted_for 非空且不等于对方 ID，说明票已投出，拒绝
    if (persistent_state.voted_for != 0 && persistent_state.voted_for != args.candidate_id) {
        return reply;
    }
    
    // 规则 3：【日志新旧比较】这是防止已提交数据丢失的最后一道防线
    uint64_t last_log_term = persistent_state.logs.empty() ? 0 : persistent_state.logs.back().term;   // 本地最后一条日志的任期
    uint64_t last_log_index = persistent_state.logs.empty() ? 0 : persistent_state.logs.back().index; // 本地最后一条日志的索引
    
    // 判断逻辑：候选人任期大，或者任期相同但索引更大（即日志更长）
    bool log_ok = (args.last_log_term > last_log_term) // 候选人日志任期更大，说明更新
               || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index); // 候选人日志任期相同但索引更大或相等，说明至少一样新
    
    if (log_ok) {
        reply.vote_granted = true;
        persistent_state.voted_for = args.candidate_id; // 记录投票对象
        persist_state();
        reset_election_timeout();
    }
    
    return reply;
}

/**
 * @brief 处理来自领导者的日志附加请求。
 *
 * AppendEntries 同时用作心跳和日志复制：
 * 1. 检查任期，如果更大则转为跟随者
 * 2. 重置选举超时（收到有效心跳）
 * 3. 验证 prev_log_index 和 prev_log_term 的一致性
 * 4. 添加新的日志条目，覆盖冲突的条目
 * 5. 更新提交索引并应用日志
 *
 * @param args 日志附加请求参数。
 * @return 附加回复，包含成功状态和可能的冲突信息。
 */
RaftNode::AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mutex);
    
    AppendEntriesReply reply;
    reply.term = persistent_state.current_term;
    reply.success = false;
    
    // 1. 任期检查与心跳重置
    if (args.term > persistent_state.current_term) { // 如果请求的任期更大，说明领导者更新了，立刻下台成为 Follower
        become_follower(args.term);
    }
    
    // 检查任期
    if (args.term < persistent_state.current_term) { // 拒绝过期 Leader 的指令
        return reply;
    }
    
    // 收到 Leader 消息，证明 Leader 还活着，重置选举时钟
    reset_election_timeout();
    
    // 2. 【一致性检查核心】检查本地是否有 prev_log_index 处的日志，且任期匹配
    if (args.prev_log_index > 0) {
        // 情况 A：Follower 日志太短，缺失了 Leader 要求的前置日志
        if (args.prev_log_index > persistent_state.logs.size()) { // 如果前置日志索引超过了本地日志长度，说明日志不连续，告诉 Leader 从哪里开始补齐
            reply.conflict_index = persistent_state.logs.size() + 1;
            return reply;
        }
        
        // 情况 B：有这条日志，但任期对不上（说明发生了冲突/覆盖）
        if (persistent_state.logs[args.prev_log_index - 1].term != args.prev_log_term) {
            // 快速回退优化：找到冲突任期的起始位置，减少网络往返次数
            uint64_t conflict_term = persistent_state.logs[args.prev_log_index - 1].term;
            for (uint64_t i = args.prev_log_index; i > 0; i--) {
                if (persistent_state.logs[i - 1].term != conflict_term) {
                    reply.conflict_index = i;
                    return reply;
                }
            }
            reply.conflict_index = 1;
            return reply;
        }
    }
    
    // 3. 日志覆盖与追加
    for (size_t i = 0; i < args.entries.size(); i++) {
        const auto& entry = args.entries[i];
        uint64_t index = args.prev_log_index + 1 + i;
        
        // 如果存在冲突（相同索引不同任期），强制截断 Follower 日志
        if (index <= persistent_state.logs.size()) {
            if (persistent_state.logs[index - 1].term != entry.term) {
                persistent_state.logs.resize(index - 1);
                persist_logs();
            }
        }
        
        // 追加新日志
        if (index > persistent_state.logs.size()) {
            persistent_state.logs.push_back(entry);
            persist_logs();
        }
    }
    
    // 4. 推进提交索引（Commit Index）
    if (args.leader_commit > commit_index) {
        commit_index = std::min(args.leader_commit, (uint64_t)persistent_state.logs.size());
        apply_logs();
    }
    
    reply.success = true;
    return reply;
}

/**
 * @brief 更新领导者的提交索引。
 *
 * 领导者检查是否有新的日志条目可以提交：
 * - 从后往前检查每个可能的提交索引
 * - 计算有多少跟随者已经复制了该索引的日志
 * - 如果获得多数派且日志条目属于当前任期，则提交
 * - 应用已提交但未应用的日志
 */
void RaftNode::update_commit_index() {
    if (state != RaftState::LEADER) return;
    
    // 从最新日志往前找，找到第一个满足多数派条件的索引
    uint64_t n = persistent_state.logs.size();
    for (uint64_t i = n; i > commit_index; i--) {
        int match_count = 1; // 自己默认算 1 票
        for (int peer_id : config.peer_ids) {
            if (peer_id == node_id) continue;
            if (match_index[peer_id] >= i) {
                match_count++;
            }
        }
        
        int majority = (config.peer_ids.size() + 1) / 2 + 1;

        // 【关键约束】只能提交当前任期的日志，不能提交旧任期的日志（即使复制到了多数派）
        // 这是为了避免 Figure 8 描述的数据回滚问题。
        if (match_count >= majority && persistent_state.logs[i - 1].term == persistent_state.current_term) {
            commit_index = i;
            apply_logs();
            break;
        }
    }
}

/**
 * @brief 应用已提交但未应用的日志条目到状态机。
 *
 * 逐个应用从 last_applied + 1 到 commit_index 的日志条目：
 * - 调用应用回调函数处理每个日志条目
 * - 更新 last_applied 索引
 * - 增加已应用计数器
 * - 输出应用日志的调试信息
 */
void RaftNode::apply_logs() {
    // 逐条追赶，直到 last_applied 追上 commit_index
    while (last_applied < commit_index) {
        last_applied++;
        const auto& entry = persistent_state.logs[last_applied - 1];
        
        // 调用上层注册的回调函数
        if (apply_callback) {
            apply_callback(entry); // 这里会将 "PUT key val" 写入底层的 MVCCStore
            applied_count++;
        }
        
        std::cout << "[Raft] Node " << node_id << " applied log " << last_applied << ", command=" << entry.command << std::endl;
    }
}

/**
 * @brief 提议一个新命令到 Raft 日志。
 *
 * 只有领导者可以接受客户端请求：
 * - 检查当前是否为领导者
 * - 创建新的日志条目并添加到本地日志
 * - 持久化日志
 * - 返回分配的日志索引
 * - 立即发送心跳以复制日志到跟随者
 *
 * @param command 命令类型（"PUT" 或 "DELETE"）。
 * @param key 操作的键。
 * @param value 操作的值（DELETE 时为空）。
 * @param index 输出参数，返回分配的日志索引。
 * @return 如果提议成功返回 true，否则返回 false。
 */

// 客户端写请求的入口函数，负责将命令转换为日志条目并复制到集群
bool RaftNode::propose(const string& command, const string& key, const string& value, uint64_t& index) {
    // 1. 只有 Leader 能接受写请求
    if (state != RaftState::LEADER) {
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex);
    
    // 2. 构造日志条目并追加到本地日志末尾（先落地，再复制）
    uint64_t new_index = persistent_state.logs.size() + 1;
    RaftLogEntry entry(persistent_state.current_term, new_index, command, key, value);
    
    persistent_state.logs.push_back(entry);
    persist_logs(); // 必须持久化才能向客户端返回成功（强一致性）
    
    index = new_index; // 返回给客户端作为唯一 ID
    
    // 3. 【异步复制】立即发送心跳/AppendEntries 将日志推向 Follower
    //    实际生产环境不会在此处阻塞等待多数派，而是异步等待回调。
    if (config.peer_ids.size() == 1) {
        commit_index = new_index;
        apply_logs();  // 立即应用
    } else {
        // 多节点：发送心跳复制日志
        send_heartbeats();
    }
    
    return true;
}

/**
 * @brief 将当前持久化状态写入磁盘。
 *
 * 将 current_term、voted_for 和日志列表序列化为字符串并写入文件。
 * 文件名格式：raft_state_{node_id}.txt
 */
void RaftNode::persist_state() {
    // 1. 确保目录存在
    std::filesystem::create_directories(config.data_dir);
    
    // 2. 先写入临时文件
    string filename = config.data_dir + "/raft_state_" + std::to_string(node_id) + ".txt";
    string temp_filename = filename + ".tmp";
    
    // 3. 写入临时文件
    {
        std::ofstream file(temp_filename, std::ios::out | std::ios::trunc);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open state file for writing");
        }
        
        string data = persistent_state.serialize();
        file.write(data.c_str(), data.size());
        
        if (file.fail()) {
            throw std::runtime_error("Failed to write state data");
        }
        
        // 4. 强制刷新到磁盘
        file.flush();
        // 在 POSIX 系统上还需要 fsync
        // fsync(file.rdbuf()->fd());
    }
    
    // 5. 原子性重命名
    std::filesystem::rename(temp_filename, filename);
}

/**
 * @brief 从磁盘加载持久化状态。
 *
 * 从文件中读取序列化的状态数据并反序列化。
 * 如果文件不存在或读取失败，保持默认状态。
 */
void RaftNode::load_persistent_state() {
    string filename = config.data_dir + "/raft_state_" + std::to_string(node_id) + ".txt";
    std::ifstream file(filename);
    if (file.is_open()) {
        string data;
        getline(file, data);
        persistent_state.deserialize(data);
    }
}

/**
 * @brief 将日志条目持久化到磁盘。
 *
 * 将所有日志条目逐行写入文件，每行一个日志条目的序列化字符串。
 * 文件名格式：raft_logs_{node_id}.txt
 */
void RaftNode::persist_logs() {
    string filename = config.data_dir + "/raft_logs_" + std::to_string(node_id) + ".txt";
    std::ofstream file(filename);
    if (file.is_open()) {
        // 每条日志一行，便于逐行读取恢复
        for (const auto& log : persistent_state.logs) {
            file << log.serialize() << std::endl;
        }
    }
}

/**
 * @brief 从磁盘加载日志条目。
 *
 * 从文件中读取所有日志条目并反序列化添加到内存中。
 * 如果文件不存在，日志列表保持为空。
 */
void RaftNode::load_logs() {
    string filename = config.data_dir + "/raft_logs_" + std::to_string(node_id) + ".txt";
    std::ifstream file(filename);
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            persistent_state.logs.push_back(RaftLogEntry::deserialize(line));
        }
    }
}

/**
 * @brief 设置日志应用回调函数。
 *
 * 当日志条目被提交时，此回调会被调用来将命令应用到状态机。
 *
 * @param callback 日志应用回调函数。
 */
void RaftNode::set_apply_callback(ApplyCallback callback) {
    apply_callback = callback;
}

/**
 * @brief 设置 RPC 发送回调函数。
 *
 * 用于向其他节点发送 RPC 请求的回调函数。
 *
 * @param callback RPC 发送回调函数。
 */
void RaftNode::set_send_rpc_callback(SendRPCCallback callback) {
    send_rpc_callback = callback;
}

/**
 * @brief 停止 Raft 节点运行。
 *
 * 设置运行标志为 false，后台线程将在下一次循环检查时退出。
 */
void RaftNode::stop() {
    running = false;
}

/**
 * @brief 获取当前领导者的节点 ID。
 *
 * @return 领导者节点 ID，如果未知则返回 0。
 * @note 此实现是简化的，实际应该维护 leader_id 状态。
 */
int RaftNode::get_leader_id() const {
    // 如果自己是 Leader，返回自己；否则返回 0（未知）
    return state == RaftState::LEADER ? node_id : 0;
}

// ============== RaftCluster 实现 ==============
/**
 * @brief RaftCluster 构造函数。
 *
 * 为每个节点地址创建一个 RaftNode 实例，并配置对等节点列表。
 *
 * @param node_addresses 节点 ID 到地址的映射。
 */
RaftCluster::RaftCluster(const std::map<int, string>& node_addresses) 
    : addresses(node_addresses), running(false) {
    
    // 为每个节点地址创建一个 RaftNode 实例
    for (const auto& [id, addr] : node_addresses) {
        RaftConfig cfg;
        cfg.node_id = id;

        // 填充对等节点列表（包含自己）
        cfg.peer_ids.clear();
        for (const auto& [pid, _] : node_addresses) {
            cfg.peer_ids.push_back(pid);
        }

        // 保存所有节点的地址映射（用于 RPC 通信）
        cfg.peer_addresses = node_addresses;

        // 为每个节点分配独立的数据目录，避免文件冲突
        cfg.data_dir = "./raft_data_" + std::to_string(id);
        
        // 创建节点对象
        nodes[id] = std::make_unique<RaftNode>(cfg);
    }
}

/**
 * @brief RaftCluster 析构函数。
 *
 * 停止集群运行并清理资源。
 */
RaftCluster::~RaftCluster() {
    stop();
}

/**
 * @brief 启动 Raft 集群。
 *
 * 为每个节点启动 RPC 服务器线程，并设置运行标志。
 */
void RaftCluster::start() {
    running = true;
    
    // 为每个节点启动独立的 RPC 服务线程
    for (auto& [id, node] : nodes) {
        rpc_threads[id] = std::make_unique<std::thread>(&RaftCluster::run_rpc_server, this, id, addresses[id]);
    }
    
    std::cout << "[RaftCluster] Cluster started with " << nodes.size() << " nodes" << std::endl;
}

/**
 * @brief 停止 Raft 集群。
 *
 * 停止所有节点，等待 RPC 线程结束。
 */
void RaftCluster::stop() {
    running = false;
    
    // 1. 停止所有 RaftNode 的后台循环
    for (auto& [id, node] : nodes) {
        node->stop();
    }
    
    // 2. 等待所有 RPC 线程结束
    for (auto& [id, thread] : rpc_threads) {
        if (thread && thread->joinable()) {
            thread->join();
        }
    }
}

/**
 * @brief 为指定节点运行 RPC 服务器。
 *
 * 启动一个简单的 TCP 服务器来处理来自其他节点的 RPC 请求。
 * 注意：当前实现是简化的占位符，实际应该使用 gRPC 或其他 RPC 框架。
 *
 * @param node_id 要运行服务器的节点 ID。
 * @param address 服务器监听地址。
 */
void RaftCluster::run_rpc_server(int node_id, const string& address) {
    // 这里实现简单的 TCP RPC 服务器
    // 实际应该使用 gRPC 或其他 RPC 框架
    std::cout << "[RPC] Node " << node_id << " listening on " << address << std::endl;
    
    // 简化：这里只是占位，实际需要实现 RPC 通信
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

/**
 * @brief 获取指定节点的 RaftNode 指针。
 *
 * @param node_id 节点 ID。
 * @return 节点指针，如果不存在则返回 nullptr。
 */
RaftNode* RaftCluster::get_node(int node_id) {
    auto it = nodes.find(node_id);
    return it != nodes.end() ? it->second.get() : nullptr;
}

/**
 * @brief 检查指定节点是否为领导者。
 *
 * @param node_id 节点 ID。
 * @return 如果是领导者则返回 true。
 */
bool RaftCluster::is_leader(int node_id) const {
    auto it = nodes.find(node_id);
    return it != nodes.end() && it->second->is_leader();
}

/**
 * @brief 获取当前集群领导者的节点 ID。
 *
 * @return 领导者节点 ID，如果没有领导者则返回 0。
 */
int RaftCluster::get_leader_id() const {
    for (const auto& [id, node] : nodes) {
        if (node->is_leader()) {
            return id;
        }
    }
    return 0;
}

} // namespace kvstore