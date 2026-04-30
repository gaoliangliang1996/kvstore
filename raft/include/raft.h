// raft/include/raft.h
#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>

// 包含 protobuf 生成的头文件
#include "raft.pb.h"

namespace raft {

// 节点状态
enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// 使用 protobuf 的 LogEntry
using LogEntry = ::raft::LogEntry;

// 辅助函数：创建 LogEntry
inline LogEntry MakeLogEntry(uint64_t term, uint64_t index, 
                             const std::string& command, 
                             const std::vector<uint8_t>& data) {
    LogEntry entry;
    entry.set_term(term);
    entry.set_index(index);
    entry.set_command(command);
    entry.set_data(data.data(), data.size());
    return entry;
}

// Raft 配置
struct RaftConfig {
    std::string node_id;                    // "192.168.1.1:50051" 或 "node-1"
    std::vector<std::string> peer_ids;      // {"192.168.1.1:50051", "192.168.1.2:50051", "192.168.1.3:50051"}
    int election_timeout_ms = 150;          // 选举超时基准值（毫秒）。实际超时 = [election_timeout_ms, election_timeout_ms * 2] 区间随机值
    int heartbeat_interval_ms = 50;         // Leader 心跳间隔（毫秒）
    int max_append_entries = 100;           // 单次 AppendEntries RPC 携带的最大日志条目数
    int snapshot_threshold = 1000;          // 快照触发阈值。当日志条目数 >= snapshot_threshold 时触发创建快照
    std::string data_dir = "./raft_data";
};

// 前向声明
class RaftNode;
class RaftLog;
class RaftStateMachine;
class RaftTransport;

// Raft 节点接口
class Raft {
public:
    virtual ~Raft() = default;
    
    // 客户端操作
    virtual void Propose(const std::string& command, const std::vector<uint8_t>& data,
                         std::function<void(bool, uint64_t)> callback) = 0;
    
    // 状态查询
    virtual NodeState GetState() const = 0;
    virtual std::string GetLeaderId() const = 0;
    virtual bool IsLeader() const = 0;
    virtual uint64_t GetCurrentTerm() const = 0;
    virtual uint64_t GetCommitIndex() const = 0;
    
    // 生命周期
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

// 辅助函数
inline std::string NodeStateToString(NodeState state) {
    switch (state) {
        case NodeState::FOLLOWER:  return "FOLLOWER";
        case NodeState::CANDIDATE: return "CANDIDATE";
        case NodeState::LEADER:    return "LEADER";
        default:                   return "UNKNOWN";
    }
}

} // namespace raft