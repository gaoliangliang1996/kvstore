#pragma once
/**
 * @file raft.h
 * @brief Raft 共识算法的实现。
 *
 * 此文件定义了 Raft 分布式共识算法的核心组件，
 * 包括日志复制、领导者选举和安全性保证。
 */

#include "common.h"
#include "mvcc_kvstore.h"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <map>
#include <functional>
#include <chrono>

namespace kvstore {

// Raft 节点状态
enum class RaftState {
    FOLLOWER,   ///< 跟随者状态
    CANDIDATE,  ///< 候选者状态
    LEADER      ///< 领导者状态
};

/**
 * @struct RaftLogEntry
 * @brief Raft 日志条目。
 *
 * 每个日志条目包含一个命令（PUT 或 DELETE）、键值数据和元数据。
 */
struct RaftLogEntry {
    uint64_t term;  ///< 该条目所属的任期
    uint64_t index; ///< 日志索引
    string command; ///< 命令类型（"PUT" 或 "DELETE"）
    string key;     ///< 操作的键
    string value;   ///< 操作的值（DELETE 时为空）

    RaftLogEntry() : term(0), index(0) {}
    RaftLogEntry(uint64_t t, uint64_t i, const string& cmd, const string& k, const string& v) 
        :   term(t), index(i), command(cmd), key(k), value(v)
    {}

    /**
     * @brief 序列化日志条目为字符串。
     * @return 序列化的字符串。
     */
    string serialize() const;
    
    /**
     * @brief 从字符串反序列化日志条目。
     * @param data 序列化的字符串。
     * @return 反序列化的日志条目。
     */
    static RaftLogEntry deserialize(const string& data);
};

/**
 * @struct RaftConfig
 * @brief Raft 节点配置。
 *
 * 包含节点 ID、集群配置和各种超时参数。
 */
struct RaftConfig {
    int node_id;                            ///< 当前节点 ID
    std::vector<int> peer_ids;              ///< 所有节点 ID 列表
    std::map<int, string> peer_addresses;   ///< 节点 ID 到网络地址的映射
    int heartbeat_interval_ms = 100;        ///< 心跳间隔（毫秒）
    int election_timeout_min_ms = 150;      ///< 最小选举超时（毫秒）
    int election_timeout_max_ms = 300;      ///< 最大选举超时（毫秒）
    string data_dir = "./raft_data";        ///< 持久化数据目录
};

/**
 * @struct RaftPersistentState
 * @brief Raft 持久化状态。
 *
 * 需要持久化到磁盘以在崩溃后恢复的状态。
 */
struct RaftPersistentState {
    uint64_t current_term = 0;          ///< 当前任期
    uint64_t voted_for = 0;             ///< 在当前任期投票给的候选者 ID. 投票给了谁
    std::vector<RaftLogEntry> logs;     ///< 日志条目列表

    /**
     * @brief 序列化持久化状态。
     * @return 序列化的字符串。
     */
    string serialize() const;
    
    /**
     * @brief 反序列化持久化状态。
     * @param data 序列化的字符串。
     */
    void deserialize(const string& data);
};

/**
 * @class RaftNode
 * @brief Raft 共识算法的节点实现。
 *
 * 实现了一个完整的 Raft 节点，包括领导者选举、日志复制和状态机应用。
 */
class RaftNode {
public:
    using ApplyCallback = std::function<void(const RaftLogEntry&)>;  ///< 日志应用回调类型
    using SendRPCCallback = std::function<bool (int peer_id, const string& rpc_type, const string& data)>;  ///< RPC 发送回调类型
private:
    // 配置
    RaftConfig config;  ///< Raft 配置
    int node_id;        ///< 当前节点 ID

    // 持久化状态（需要持久化到磁盘）
    RaftPersistentState persistent_state;  ///< 持久化状态

    // 易失性状态
    RaftState state;                    ///< 当前节点状态           FOLLOWER/CANDIDATE/LEADER
    std::atomic<uint64_t> commit_index; ///< 已提交的最高日志索引
    std::atomic<uint64_t> last_applied; ///< 已应用的最高日志索引

    // 领导者状态 仅 Leader 有效
    std::vector<uint64_t> next_index;   ///< 对每个跟随者，下一条要发送的日志索引
    std::vector<uint64_t> match_index;  ///< 对每个跟随者，已复制的最高日志索引

    // 定时器和同步
    std::mutex mutex;                           ///< 保护共享状态的互斥锁
    std::condition_variable cv;                 ///< 条件变量
    std::thread background_thread;              ///< 后台运行线程
    std::atomic<bool> running;                  ///< 运行标志
    std::chrono::steady_clock::time_point election_timeout;  ///< 选举超时时间点
    std::chrono::steady_clock::time_point last_heartbeat;    ///< 最后心跳时间

    // 回调函数
    ApplyCallback apply_callback;       ///< 日志应用回调
    SendRPCCallback send_rpc_callback;  ///< RPC 发送回调

    // 统计信息
    std::atomic<uint64_t> applied_count; ///< 已应用的日志条目数量

    // 内部方法
    void run();                    ///< 主循环
    void become_follower(uint64_t term);  ///< 成为跟随者
    void become_candidate();       ///< 成为候选者
    void become_leader();          ///< 成为领导者
    void reset_election_timeout(); ///< 重置选举超时
    void send_heartbeats();        ///< 发送心跳
    void start_election();         ///< 开始选举
    void update_commit_index();    ///< 更新提交索引
    void apply_logs();             ///< 应用日志
    void persist_state();          ///< 持久化状态
    void load_persistent_state();  ///< 加载持久化状态
    void persist_logs();           ///< 持久化日志
    void load_logs();              ///< 加载日志

public:
    RaftNode(const RaftConfig& cfg);
    ~RaftNode();

    bool propose(const string& command, const string& key, const string& value, uint64_t& index);
    bool is_leader() const { return state == RaftState::LEADER; }
    int get_leader_id() const;

    struct RequestVoteArgs {
        uint64_t term;          ///< 候选者的任期
        int candidate_id;       ///< 候选者 ID
        uint64_t last_log_index; ///< 候选者最后日志索引
        uint64_t last_log_term;  ///< 候选者最后日志任期
    };

    struct RequestVoteReply {
        uint64_t term;      ///< 当前任期
        bool vote_granted;  ///< 是否授予投票
    };
    
    struct AppendEntriesArgs {
        uint64_t term;                  ///< 领导者任期
        int leader_id;                  ///< 领导者 ID
        uint64_t prev_log_index;        ///< 前一条日志索引
        uint64_t prev_log_term;         ///< 前一条日志任期
        std::vector<RaftLogEntry> entries;  ///< 要附加的日志条目 log entries to store (empty for heartbeat)
        uint64_t leader_commit;         ///< 领导者提交索引
    };
    
    struct AppendEntriesReply {
        uint64_t term;          ///< 当前任期
        bool success;           ///< 是否成功
        uint64_t conflict_index; ///< 冲突索引（用于快速回退）
    };

    RequestVoteReply handle_request_vote(const RequestVoteArgs& args);
    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args);
    void set_apply_callback(ApplyCallback callback);
    void set_send_rpc_callback(SendRPCCallback callback);
    uint64_t get_current_term() const { return persistent_state.current_term; }
    uint64_t get_commit_index() const { return commit_index; }
    uint64_t get_last_applied() const { return last_applied; }
    RaftState get_state() const { return state; }
    void stop();
};

/**
 * @class RaftCluster
 * @brief Raft 集群管理类。
 *
 * 管理多个 Raft 节点，处理集群启动和停止。
 */
class RaftCluster {
private:
    std::map<int, std::unique_ptr<RaftNode>> nodes;          // 节点 ID -> RaftNode 对象的映射
    std::map<int, std::unique_ptr<std::thread>> rpc_threads; // 节点 ID -> RPC 服务器线程的映射
    std::map<int, string> addresses;                         // 节点 ID -> 网络地址的映射
    bool running;                                            // 集群运行标志

    /**
     * @brief 为指定节点运行 RPC 服务器。
     * @param node_id 节点 ID。
     * @param address 服务器地址。
     */
    void run_rpc_server(int node_id, const string& address);
public:
    /**
     * @brief RaftCluster 构造函数。
     * @param node_addresses 节点 ID 到地址的映射。
     */
    RaftCluster(const std::map<int, string>& node_addresses);
    
    /**
     * @brief RaftCluster 析构函数。
     */
    ~RaftCluster();

    /**
     * @brief 启动集群。
     */
    void start();
    
    /**
     * @brief 停止集群。
     */
    void stop();
    
    /**
     * @brief 获取指定节点。
     * @param node_id 节点 ID。
     * @return 节点指针，如果不存在则返回 nullptr。
     */
    RaftNode* get_node(int node_id);
    
    /**
     * @brief 检查指定节点是否为领导者。
     * @param node_id 节点 ID。
     * @return 如果是领导者则返回 true。
     */
    bool is_leader(int node_id) const;
    
    /**
     * @brief 获取当前领导者 ID。
     * @return 领导者 ID，如果没有则返回 -1。
     */
    int get_leader_id() const;
};

}