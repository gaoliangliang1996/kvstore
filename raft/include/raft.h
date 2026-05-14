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

namespace raft {

// 前向声明
class RaftNode;
class RaftLog;
class RaftStateMachine;
class RaftTransport;

// 节点状态
enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// 注意：LogEntry 由 protobuf 生成，不要在这里定义！

// 节点地址信息
struct NodeAddress {
    std::string node_id;
    std::string host;
    int port;
    
    NodeAddress() : port(0) {}
    NodeAddress(const std::string& id, const std::string& h, int p)
        : node_id(id), host(h), port(p) {}
    
    std::string GetAddress() const {
        return host + ":" + std::to_string(port);
    }
};

// Raft 配置
struct RaftConfig {
    std::string node_id;
    std::vector<NodeAddress> peers;
    int election_timeout_ms = 150;
    int heartbeat_interval_ms = 50;
    int max_append_entries = 100;
    int snapshot_threshold = 1000;
    std::string data_dir = "./raft_data";
    
    std::string listen_host = "0.0.0.0";
    int listen_port = 50051;
    
    std::string GetPeerAddress(const std::string& peer_id) const {
        for (const auto& peer : peers) {
            if (peer.node_id == peer_id) {
                return peer.GetAddress();
            }
        }
        return "";
    }
    
    std::vector<std::string> GetPeerIds() const {
        std::vector<std::string> ids;
        for (const auto& peer : peers) {
            if (peer.node_id != node_id) {
                ids.push_back(peer.node_id);
            }
        }
        return ids;
    }
    
    std::vector<std::string> GetPeerAddresses() const {
        std::vector<std::string> addresses;
        for (const auto& peer : peers) {
            if (peer.node_id != node_id) {
                addresses.push_back(peer.GetAddress());
            }
        }
        return addresses;
    }
};

// Raft 节点接口
class Raft {
public:
    virtual ~Raft() = default;
    
    virtual void Propose(const std::string& command, const std::vector<uint8_t>& data,
                         std::function<void(bool, uint64_t)> callback) = 0;
    
    virtual NodeState GetState() const = 0;
    virtual std::string GetLeaderId() const = 0;
    virtual bool IsLeader() const = 0;
    virtual uint64_t GetCurrentTerm() const = 0;
    virtual uint64_t GetCommitIndex() const = 0;
    
    virtual void Start() = 0;
    virtual void Stop() = 0;
    
    virtual void SetLeaderChangeCallback(std::function<void(const std::string&)> callback) = 0;
    virtual void SetCommitCallback(std::function<void(uint64_t, const std::string&, const std::vector<uint8_t>&)> callback) = 0;
};

inline std::string NodeStateToString(NodeState state) {
    switch (state) {
        case NodeState::FOLLOWER:  return "FOLLOWER";
        case NodeState::CANDIDATE: return "CANDIDATE";
        case NodeState::LEADER:    return "LEADER";
        default:                   return "UNKNOWN";
    }
}

} // namespace raft