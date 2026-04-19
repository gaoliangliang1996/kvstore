#pragma once

#include "mvcc_kvstore.h"
#include "raft.h"
#include <queue>
#include <unordered_map>

namespace kvstore {

class RaftKVStore : public MVCCKVStore {
private:
    std::unique_ptr<RaftNode> raft_node;
    std::unordered_map<uint64_t, std::pair<string, string>> pending_proposals;
    std::mutex pending_mutex;
    std::condition_variable pending_cv;
    int node_id;
    
    void on_log_applied(const RaftLogEntry& entry);
    
public:
    RaftKVStore(const Config& cfg, const RaftConfig& raft_cfg);
    
    ~RaftKVStore();
    
    Version put(const string& key, const string& value);
    
    Version del(const string& key);
    
    bool get(const string& key, string& value, Version snap_ver = 0);
    
    bool is_leader() const { return raft_node->is_leader(); }
    
    int get_leader_id() const { return raft_node->get_leader_id(); }
    
    bool wait_for_commit(uint64_t index, int timeout_ms = 5000);
};

} // namespace kvstore