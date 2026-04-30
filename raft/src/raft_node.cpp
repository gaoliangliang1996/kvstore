// raft/src/raft_node.cpp
#include "../include/raft_node.h"
#include <chrono>
#include <algorithm>
#include <iostream>
#include <sstream>

namespace raft {

RaftNode::RaftNode(const RaftConfig& config)
    : config_(config),
      node_id_(config.node_id),
      peer_ids_(config.peer_ids),
      current_term_(0),
      state_(NodeState::FOLLOWER),
      commit_index_(0),
      last_applied_(0),
      running_(false),
      gen_(rd_()),
      election_timeout_dist_(config.election_timeout_ms, config.election_timeout_ms * 2) {
    
    std::cout << "[Raft] Initializing node " << node_id_ << std::endl;
    
    // 初始化组件
    persistence_ = std::make_unique<RaftPersistence>(config.data_dir);
    log_ = std::make_unique<RaftLog>(config.data_dir);
    state_machine_ = std::make_unique<KVStateMachine>();
    transport_ = std::make_unique<GrpcTransport>();
    
    // 恢复状态
    RecoverState();
    
    // 初始化领导者状态
    for (const auto& peer : peer_ids_) {
        if (peer != node_id_) {
            next_index_[peer] = log_->GetLastLogIndex() + 1;
            match_index_[peer] = 0;
        }
    }
    
    // 设置传输层回调
    transport_->SetHandlers(
        [this](const RequestVoteRequest& req) { return HandleRequestVote(req); },
        [this](const AppendEntriesRequest& req) { return HandleAppendEntries(req); },
        [this](const InstallSnapshotRequest& req) { return HandleInstallSnapshot(req); }
    );
    
    std::cout << "[Raft] Node " << node_id_ << " initialized" << std::endl;
}

RaftNode::~RaftNode() {
    Stop();
}

void RaftNode::Start() {
    if (running_) return;
    
    running_ = true;
    transport_->Initialize(node_id_, peer_ids_);
    transport_->Start();
    
    // 启动选举定时器
    election_timer_ = std::make_unique<std::thread>(&RaftNode::RunElectionTimer, this);
    
    std::cout << "[Raft] Node " << node_id_ << " started" << std::endl;
}

void RaftNode::Stop() {
    running_ = false;
    
    if (election_timer_ && election_timer_->joinable()) {
        election_timer_->join();
    }
    if (heartbeat_timer_ && heartbeat_timer_->joinable()) {
        heartbeat_timer_->join();
    }
    
    transport_->Stop();
    
    std::cout << "[Raft] Node " << node_id_ << " stopped" << std::endl;
}

void RaftNode::RunElectionTimer() {
    while (running_) {
        int timeout = election_timeout_dist_(gen_);
        
        // 分段睡眠，以便及时响应停止信号
        for (int i = 0; i < timeout && running_; i += 10) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        if (!running_) break;
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 只有 follower 才需要处理超时
        if (state_ == NodeState::FOLLOWER && running_) {
            ProcessElectionTimeout();
        }
    }
}

void RaftNode::ProcessElectionTimeout() {
    std::cout << "[Raft] Node " << node_id_ << " election timeout at term " 
              << current_term_ << std::endl;
    BecomeCandidate();
}

void RaftNode::BecomeCandidate() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    state_ = NodeState::CANDIDATE;
    current_term_++;
    voted_for_ = node_id_;
    PersistState();
    
    std::cout << "[Raft] Node " << node_id_ << " becomes candidate for term " 
              << current_term_ << std::endl;
    
    // 重置选举定时器
    election_timeout_dist_ = std::uniform_int_distribution<>(
        config_.election_timeout_ms, config_.election_timeout_ms * 2);
    
    // 请求投票
    RequestVoteRequest req;
    req.set_term(current_term_);
    req.set_candidate_id(node_id_);
    req.set_last_log_index(log_->GetLastLogIndex());
    req.set_last_log_term(log_->GetLastLogTerm());
    
    int votes = 1;  // 自己投票
    int total_peers = 0;
    
    for (const auto& peer : peer_ids_) {
        if (peer == node_id_) continue;
        total_peers++;
        
        transport_->SendRequestVote(peer, req, 
            [this, peer, &votes, total_peers](const RequestVoteResponse& resp) {
                std::lock_guard<std::mutex> lock(mutex_);
                
                if (resp.term() > current_term_) {
                    BecomeFollower(resp.term(), "");
                    return;
                }
                
                if (resp.vote_granted() && state_ == NodeState::CANDIDATE) {
                    votes++;
                    if (votes > total_peers / 2) {
                        BecomeLeader();
                    }
                }
            });
    }
}

void RaftNode::BecomeLeader() {
    state_ = NodeState::LEADER;
    leader_id_ = node_id_;
    
    std::cout << "[Raft] Node " << node_id_ << " becomes leader for term " 
              << current_term_ << std::endl;
    
    // 初始化领导者状态
    for (const auto& peer : peer_ids_) {
        if (peer == node_id_) continue;
        next_index_[peer] = log_->GetLastLogIndex() + 1;
        match_index_[peer] = 0;
    }
    
    // 启动心跳定时器
    heartbeat_timer_ = std::make_unique<std::thread>(&RaftNode::RunHeartbeatTimer, this);
    
    if (leader_change_callback_) {
        leader_change_callback_(node_id_);
    }
    
    // 发送一次心跳
    for (const auto& peer : peer_ids_) {
        if (peer != node_id_) {
            SendAppendEntries(peer);
        }
    }
}

void RaftNode::RunHeartbeatTimer() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.heartbeat_interval_ms));
        
        if (!running_) break;
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == NodeState::LEADER && running_) {
                ProcessHeartbeatTimeout();
            }
        }
    }
}

void RaftNode::ProcessHeartbeatTimeout() {
    for (const auto& peer : peer_ids_) {
        if (peer != node_id_) {
            SendAppendEntries(peer);
        }
    }
}

void RaftNode::SendAppendEntries(const std::string& peer_id) {
    uint64_t next_idx = next_index_[peer_id];
    uint64_t prev_log_idx = next_idx - 1;
    uint64_t prev_log_term = 0;
    
    // 获取前一条日志的 term (用于一致性检查)
    if (prev_log_idx > 0) {
        auto entry = log_->GetEntry(prev_log_idx);
        prev_log_term = entry.term();
    }
    
    // 构造请求
    AppendEntriesRequest req;
    req.set_term(current_term_);
    req.set_leader_id(node_id_);
    req.set_prev_log_index(prev_log_idx);
    req.set_prev_log_term(prev_log_term);
    req.set_leader_commit(std::min(commit_index_, log_->GetLastLogIndex()));
    
    // 批量获取日志
    std::vector<raft::LogEntry> entries = log_->GetEntriesFrom(next_idx);
    if (entries.size() > config_.max_append_entries) {
        entries.resize(config_.max_append_entries);
    }
    
    // Protobuf 序列化
    for (const auto& entry : entries) {
        auto* pb_entry = req.add_entries();
        pb_entry->set_term(entry.term());
        pb_entry->set_index(entry.index());
        pb_entry->set_command(entry.command());
        pb_entry->set_data(entry.data());
    }
    
    // 异步发送 + 回调处理
    transport_->SendAppendEntries(peer_id, req, 
        [this, peer_id, next_idx, entries_size = req.entries_size()]
        (const AppendEntriesResponse& resp) {
            std::lock_guard<std::mutex> lock(mutex_);
            
            if (resp.term() > current_term_) {
                BecomeFollower(resp.term(), "");
                return;
            }
            
            if (resp.success()) {
                // 成功：更新 next_index_ 和 match_index_
                if (next_index_[peer_id] < next_idx + entries_size) {
                    next_index_[peer_id] = next_idx + entries_size;
                }
                if (resp.match_index() > match_index_[peer_id]) {
                    match_index_[peer_id] = resp.match_index();
                }
                
                UpdateCommitIndex(); // 检查是否可以提交
            } else if (next_index_[peer_id] > 1) {
                // 失败，回退 next_index_
                next_index_[peer_id]--;
            }
        });
}

void RaftNode::UpdateCommitIndex() {
    uint64_t old_commit = commit_index_;
    uint64_t last_log = log_->GetLastLogIndex();
    
    // 从当前 commit 之后开始检查
    for (uint64_t idx = commit_index_ + 1; idx <= last_log; idx++) {
        auto entry = log_->GetEntry(idx);
        if (entry.term() != current_term_) continue;
        
        // 统计已复制的节点数
        int count = 1;  // 自己
        for (const auto& peer : peer_ids_) {
            if (peer == node_id_) continue;
            if (match_index_[peer] >= idx) {
                count++;
            }
        }
        
        // 多数节点确认 -> 提交
        if (count > (peer_ids_.size()) / 2) {
            commit_index_ = idx;
        } else {
            break;
        }
    }
    
    if (commit_index_ > old_commit) {
        ApplyCommittedEntries();
    }
}

void RaftNode::ApplyCommittedEntries() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        auto entry = log_->GetEntry(last_applied_);
        
        // 转换为 vector<uint8_t>
        const std::string& data_str = entry.data();
        std::vector<uint8_t> data_vec(data_str.begin(), data_str.end());
        
        // 应用到状态机
        state_machine_->Apply(entry.command(), data_vec, entry.index());
        
        // 通知上层回调
        if (commit_callback_) {
            commit_callback_(entry.index(), entry.command(), data_vec);
        }
        
        // 通知客户端 FIFO 队列
        std::lock_guard<std::mutex> lock(proposal_mutex_);
        if (!pending_proposals_.empty()) {
            auto& proposal = pending_proposals_.front();
            if (proposal.callback) {
                proposal.callback(true, entry.index());
            }
            pending_proposals_.pop();
        }
    }
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    RequestVoteResponse resp;
    resp.set_term(current_term_);
    
    std::cout << "[Raft] Node " << node_id_ << " received RequestVote from " 
              << req.candidate_id() << " term=" << req.term() << std::endl;
    
    // 拒绝低 term 请求
    if (req.term() < current_term_) {
        resp.set_vote_granted(false);
        return resp;
    }
    
    // 发现更高 term, 退位
    if (req.term() > current_term_) {
        BecomeFollower(req.term(), "");
    }
    
    // 检查日志是否至少和本地一样新
    bool log_ok = false;
    uint64_t last_log_index = log_->GetLastLogIndex();
    uint64_t last_log_term = log_->GetLastLogTerm();
    
    if (req.last_log_index() > last_log_index) {
        log_ok = true;
    } else if (req.last_log_index() == last_log_index) {
        log_ok = (req.last_log_term() >= last_log_term);
    } else {
        auto entry = log_->GetEntry(req.last_log_index());
        log_ok = (entry.term() == req.last_log_term());
    }
    
    bool voted_ok = voted_for_.empty() || voted_for_ == req.candidate_id();
    
    if (log_ok && voted_ok) {
        voted_for_ = req.candidate_id();
        PersistState();
        resp.set_vote_granted(true);
        
        // 重置选举定时器
        election_timeout_dist_ = std::uniform_int_distribution<>(
            config_.election_timeout_ms, config_.election_timeout_ms * 2);
            
        std::cout << "[Raft] Node " << node_id_ << " voted for " << req.candidate_id() << std::endl;
    } else {
        resp.set_vote_granted(false);
    }
    
    return resp;
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    AppendEntriesResponse resp;
    resp.set_term(current_term_);
    
    std::cout << "[Raft] Node " << node_id_ << " received AppendEntries from " 
              << req.leader_id() << " term=" << req.term() 
              << " entries=" << req.entries_size() << std::endl;
    
    if (req.term() < current_term_) {
        resp.set_success(false);
        return resp;
    }
    
    if (req.term() > current_term_) {
        BecomeFollower(req.term(), req.leader_id());
    } else if (state_ != NodeState::FOLLOWER) {
        BecomeFollower(req.term(), req.leader_id());
    }
    
    leader_id_ = req.leader_id();
    election_timeout_dist_ = std::uniform_int_distribution<>(
        config_.election_timeout_ms, config_.election_timeout_ms * 2);
    
    // 检查日志匹配
    if (req.prev_log_index() > 0) {
        auto entry = log_->GetEntry(req.prev_log_index());
        if (entry.term() != req.prev_log_term()) {  // 使用 term()
            log_->TruncateFrom(req.prev_log_index());
            resp.set_success(false);
            return resp;
        }
    }
    
    // 追加新日志
    std::vector<LogEntry> entries;
    for (const auto& pb_entry : req.entries()) {
        LogEntry entry;
        entry.set_term(pb_entry.term());
        entry.set_index(pb_entry.index());
        entry.set_command(pb_entry.command());
        entry.set_data(pb_entry.data());
        entries.push_back(entry);
    }
    
    log_->AppendEntries(entries);
    resp.set_success(true);
    resp.set_match_index(log_->GetLastLogIndex());
    
    // 更新 commit index
    uint64_t leader_commit = req.leader_commit();
    if (leader_commit > commit_index_) {
        commit_index_ = std::min(leader_commit, log_->GetLastLogIndex());
        ApplyCommittedEntries();
    }
    
    return resp;
}

InstallSnapshotResponse RaftNode::HandleInstallSnapshot(const InstallSnapshotRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    InstallSnapshotResponse resp;
    resp.set_term(current_term_);
    
    if (req.term() < current_term_) {
        return resp;
    }
    
    if (req.term() > current_term_) {
        BecomeFollower(req.term(), req.leader_id());
    }
    
    // 安装快照
    std::vector<uint8_t> data(req.data().begin(), req.data().end());
    log_->InstallSnapshot(req.last_included_index(), req.last_included_term(), data);
    state_machine_->RestoreSnapshot(data);
    
    if (commit_index_ < req.last_included_index()) {
        commit_index_ = req.last_included_index();
        last_applied_ = req.last_included_index();
    }
    
    return resp;
}

void RaftNode::BecomeFollower(uint64_t term, const std::string& leader_id) {
    state_ = NodeState::FOLLOWER;
    
    if (term > current_term_) {
        current_term_ = term;
        voted_for_ = "";
        PersistState();
    }
    
    leader_id_ = leader_id;
    
    // 停止心跳定时器
    if (heartbeat_timer_ && heartbeat_timer_->joinable()) {
        heartbeat_timer_->join();
    }
    heartbeat_timer_.reset();
    
    std::cout << "[Raft] Node " << node_id_ << " becomes follower, term=" 
              << current_term_ << ", leader=" << leader_id_ << std::endl;
    
    if (leader_change_callback_) {
        leader_change_callback_(leader_id);
    }
}

void RaftNode::Propose(const std::string& command, const std::vector<uint8_t>& data,
                       std::function<void(bool, uint64_t)> callback) {
    // 非 leader 直接拒绝
    if (!IsLeader()) {
        if (callback) callback(false, 0);
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 创建日志条目
    LogEntry entry;
    entry.set_term(current_term_);      // 使用 set_term
    entry.set_index(log_->GetLastLogIndex() + 1);  // 使用 set_index
    entry.set_command(command);          // 使用 set_command
    entry.set_data(std::string(data.begin(), data.end()));  // 使用 set_data
    
    // 追加到本地日志
    log_->AppendEntry(entry);
    
    // 保存客户端回调
    if (callback) {
        std::lock_guard<std::mutex> proposal_lock(proposal_mutex_);
        pending_proposals_.push({command, data, callback});
    }
    
    // 立即发送到所有 Follower
    for (const auto& peer : peer_ids_) {
        if (peer != node_id_) {
            SendAppendEntries(peer);
        }
    }
}

void RaftNode::PersistState() {
    RaftPersistence::HardState state;
    state.term = current_term_;
    state.voted_for = voted_for_;
    persistence_->SetHardState(state); // 写入 raft_data / hard_state
}

void RaftNode::RecoverState() {
    auto state = persistence_->GetHardState(); // 读取 raft_data / hard_state
    current_term_ = state.term;
    voted_for_ = state.voted_for;
    
    log_->Load();
    
    std::cout << "[Raft] Node " << node_id_ << " recovered, term=" 
              << current_term_ << ", voted_for=" << voted_for_ << std::endl;
}

void RaftNode::SetLeaderChangeCallback(std::function<void(const std::string&)> callback) {
    leader_change_callback_ = callback;
}

void RaftNode::SetCommitCallback(std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> callback) {
    commit_callback_ = callback;
}

bool RaftNode::CheckLogMatch(uint64_t index, uint64_t term) const {
    if (index == 0) return true;
    if (index > log_->GetLastLogIndex()) return false;
    return log_->GetEntry(index).term() == term;
}

} // namespace raft