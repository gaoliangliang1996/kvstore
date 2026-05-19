// include/raft_kvstore.h
#pragma once
#include "mvcc_kvstore.h"
#include "../raft/include/raft.h"
#include "../raft/include/raft_node.h"
#include <memory>
#include <functional>
#include <vector>
#include <future>

namespace kvstore {

// Raft 命令类型
enum class RaftCommandType : uint8_t {
    PUT = 0x01,
    DELETE = 0x02,
    BATCH_PUT = 0x03,
    BATCH_DELETE = 0x04
};

// Raft 命令结构
struct RaftCommand {
    RaftCommandType type;
    std::string key;
    std::string value;
    std::vector<std::pair<std::string, std::string>> batch_puts;
    std::vector<std::string> batch_deletes;
    uint64_t request_id;
    
    std::vector<uint8_t> Serialize() const;
    static RaftCommand Deserialize(const std::vector<uint8_t>& data);
};

// RaftKVStore 类（不继承 MVCCKVStore，而是组合）
class RaftKVStore {
public:
    explicit RaftKVStore(const Config& cfg, const raft::RaftConfig& raft_cfg);
    ~RaftKVStore();  // 不需要 override
    
    // 写操作
    Version put(const std::string& key, const std::string& value);  // 不需要 override
    Version del(const std::string& key);  // 不需要 override
    
    // 批量操作
    struct BatchResult {
        bool success;
        std::vector<Version> versions;
        std::string error;
    };
    BatchResult batch_put(const std::vector<std::pair<std::string, std::string>>& pairs);
    BatchResult batch_del(const std::vector<std::string>& keys);
    
    // 读操作
    bool get(const std::string& key, std::string& value, Version snap_ver = 0);  // 不需要 override
    
    // Raft 状态查询
    bool IsLeader() const;
    std::string GetLeaderId() const;
    raft::NodeState GetRaftState() const;
    uint64_t GetCurrentTerm() const;
    
    // 集群管理
    void AddPeer(const std::string& peer_id);
    void RemovePeer(const std::string& peer_id);
    std::vector<std::string> GetPeers() const;
    
    // 等待领导者选举
    bool WaitForLeader(int timeout_ms = 5000);
    
    // 重定向请求到领导者
    void SetRedirectHandler(std::function<std::string(const std::string&)> handler);
    
private:
    std::unique_ptr<raft::RaftNode> raft_node_;
    std::unique_ptr<MVCCKVStore> storage_;  // 组合而不是继承
    
    // 请求管理
    std::unordered_map<uint64_t, std::function<void(bool, Version)>> pending_requests_;
    std::mutex pending_mutex_;
    std::atomic<uint64_t> next_request_id_{1};
    
    // 配置
    Config kv_config_;
    raft::RaftConfig raft_config_;
    
    // 回调处理
    void OnCommit(uint64_t index, const std::string& command, const std::vector<uint8_t>& data);
    void OnLeaderChange(const std::string& leader_id);
    
    // 命令处理
    void ApplyPut(const std::string& key, const std::string& value, uint64_t request_id);
    void ApplyDelete(const std::string& key, uint64_t request_id);
    void ApplyBatchPut(const std::vector<std::pair<std::string, std::string>>& pairs, uint64_t request_id);
    void ApplyBatchDelete(const std::vector<std::string>& keys, uint64_t request_id);
    
    // 请求转发
    bool ForwardToLeader(const std::string& operation, const std::string& key, 
                         const std::string& value, Version& result);
    
    // 重定向处理器
    std::function<std::string(const std::string&)> redirect_handler_;
};

} // namespace kvstore