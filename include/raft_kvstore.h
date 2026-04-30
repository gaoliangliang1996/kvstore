#pragma once
#include "mvcc_kvstore.h"
#include "../raft/include/raft.h"
#include <memory>

namespace kvstore {

class RaftKVStore : public MVCCKVStore {
public:
    explicit RaftKVStore(const Config& cfg, const raft::RaftConfig& raft_cfg);
    ~RaftKVStore() override;
    
    // 重写写操作（需要通过 Raft 共识）
    Version put(const string& key, const string& value) override;
    Version del(const string& key) override;
    
    // 读操作可以直接从本地读取
    bool get(const string& key, string& value, Version snap_ver = 0) override;
    
    // Raft 状态查询
    bool IsLeader() const { return raft_->IsLeader(); }
    std::string GetLeaderId() const { return raft_->GetLeaderId(); }
    
private:
    std::unique_ptr<raft::Raft> raft_;
    std::map<uint64_t, std::function<void(Version)>> pending_proposals_;
    std::mutex pending_mutex_;
    
    void OnCommit(uint64_t index, const std::string& command, const std::vector<uint8_t>& data);
    void OnLeaderChange(const std::string& leader_id);
};

} // namespace kvstore