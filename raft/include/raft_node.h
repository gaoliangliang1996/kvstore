#pragma once
#include "raft.h"
#include "raft_log.h"
#include "raft_state_machine.h"
#include "raft_transport.h"
#include "raft_persistence.h"
#include <thread>
#include <random>
#include <queue>
#include <unordered_map>

namespace raft {

class RaftNode : public Raft {
public:
    explicit RaftNode(const RaftConfig& config);
    ~RaftNode() override;
    
    // Raft 接口实现
    void Propose(const std::string& command, const std::vector<uint8_t>& data,
                 std::function<void(bool, uint64_t)> callback) override;
    
    NodeState GetState() const override { return state_; }
    std::string GetLeaderId() const override { return leader_id_; }
    bool IsLeader() const override { return state_ == NodeState::LEADER; }
    uint64_t GetCurrentTerm() const override { return current_term_; }
    uint64_t GetCommitIndex() const override { return commit_index_; }
    
    void Start() override;
    void Stop() override;
    
    // RPC 处理
    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
    InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& req);
    
    // 状态变化回调
    void SetLeaderChangeCallback(std::function<void(const std::string&)> callback);
    void SetCommitCallback(std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> callback);
    
private:
    // 配置
    RaftConfig config_;
    std::string node_id_;
    std::vector<std::string> peer_ids_;
    
    // 持久化状态
    uint64_t current_term_;         // 当前任期，严格单调递增
    std::string voted_for_;         // 本任期投票给了谁
    std::unique_ptr<RaftLog> log_;  // 日志存储 (entries + snapshot)
    std::unique_ptr<RaftPersistence> persistence_;  // hard_state 读写
    
    // 易失性状态
    NodeState state_;       // FOLLOWER/CANDIDATE/LEADER
    std::string leader_id_; // 当前已知的 leader id
    uint64_t commit_index_; // 已提交的最大日志索引
    uint64_t last_applied_; // 已应用到状态机的最大索引 (≤ commit_index)
    
    // 领导者状态
    std::map<std::string, uint64_t> next_index_;  // next_index_[peer] 下一次从哪条开始发送
    std::map<std::string, uint64_t> match_index_; // next_index_[peer] 已确认复制到该 peer 的最大日志索引
    
    // 心跳和选举定时器
    std::unique_ptr<std::thread> election_timer_; // 选举超时线程
    std::unique_ptr<std::thread> heartbeat_timer_;// Leader 心跳线程
    std::atomic<bool> running_;
    
    // 随机数生成器
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_int_distribution<> election_timeout_dist_;
    
    // 客户端请求队列
    struct PendingProposal {
        std::string command;
        std::vector<uint8_t> data;
        std::function<void(bool, uint64_t)> callback;
    };
    std::queue<PendingProposal> pending_proposals_; // FIFO 队列
    mutable std::mutex proposal_mutex_;
    std::condition_variable proposal_cv_;
    
    // 状态机
    std::unique_ptr<RaftStateMachine> state_machine_;
    
    // 传输层
    std::unique_ptr<RaftTransport> transport_;
    
    // 回调
    std::function<void(const std::string&)> leader_change_callback_; // leader 变更通知
    std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> commit_callback_; // 日志提交通知
    
    // 互斥锁
    mutable std::mutex mutex_;
    
    // 内部方法
    void RunElectionTimer();  // 选举线程：等待超时、发起选举
    void RunHeartbeatTimer(); // 心跳线程：每 50ms 发送心跳
    
    void BecomeFollower(uint64_t term, const std::string& leader_id);
    void BecomeCandidate();
    void BecomeLeader();
    
    void SendRequestVote(const std::string& peer_id);
    void SendAppendEntries(const std::string& peer_id);
    void SendHeartbeat(const std::string& peer_id);
    
    void ProcessElectionTimeout();  // Follower 超时 -> BecomeCandidate
    void ProcessHeartbeatTimeout(); // Leader 超时   -> SendAppendEntries(心跳)
    
    void UpdateCommitIndex();       // 检查多数节点确认 -> 推进 commit_index_
    void ApplyCommittedEntries();   // 将 commit 的日志应用到状态机 + 回调客户端
    
    void PersistState();            // 保存 term + votedFor 到磁盘
    void RecoverState();            // 从磁盘恢复 term + votedFor + 日志
    
    bool CheckLogMatch(uint64_t index, uint64_t term) const;
    void AppendEntries(const std::vector<LogEntry>& entries);
};

} // namespace raft