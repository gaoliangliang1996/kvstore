// raft/include/raft_node.h
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
    void SetLeaderChangeCallback(std::function<void(const std::string&)> callback) override {
        leader_change_callback_ = callback;
    }
    
    void SetCommitCallback(std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> callback) override {
        commit_callback_ = callback;
    }
    
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

    std::mutex timer_mutex_;
    std::condition_variable timer_cv_;
    std::atomic<bool> timer_reset_{false};
    void ResetElectionTimer();
    
    // 客户端请求队列
    struct PendingProposal {
        std::string command;
        std::vector<uint8_t> data;
        std::function<void(bool, uint64_t)> callback;
    };
    std::queue<PendingProposal> pending_proposals_;
    mutable std::mutex proposal_mutex_;
    std::condition_variable proposal_cv_;
    
    // 状态机
    std::unique_ptr<RaftStateMachine> state_machine_;
    
    // 传输层
    std::unique_ptr<RaftTransport> transport_;
    
    // 回调
    std::function<void(const std::string&)> leader_change_callback_;
    std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> commit_callback_;
    
    // 互斥锁
    mutable std::mutex mutex_;
    
    // 内部方法
    void RunElectionTimer();
    void RunHeartbeatTimer();
    
    void BecomeFollower(uint64_t term, const std::string& leader_id);
    void BecomeCandidate();
    void BecomeLeader();
    
    void SendRequestVote(const std::string& peer_id);
    void SendAppendEntries(const std::string& peer_id);
    void SendHeartbeat(const std::string& peer_id);
    
    void ProcessElectionTimeout();
    void ProcessHeartbeatTimeout();
    
    void UpdateCommitIndex();
    void ApplyCommittedEntries();
    
    void PersistState();
    void RecoverState();
    
    bool CheckLogMatch(uint64_t index, uint64_t term) const;
    void AppendEntries(const std::vector<LogEntry>& entries);
};

} // namespace raft