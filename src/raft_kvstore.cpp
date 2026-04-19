#include "raft_kvstore.h"
#include <chrono>
#include <iostream>

namespace kvstore {

RaftKVStore::RaftKVStore(const Config& cfg, const RaftConfig& raft_cfg)
    : MVCCKVStore(cfg), node_id(raft_cfg.node_id) {
    
    raft_node = std::make_unique<RaftNode>(raft_cfg);
    
    raft_node->set_apply_callback([this](const RaftLogEntry& entry) {
        this->on_log_applied(entry);
    });
    
    raft_node->set_send_rpc_callback([this](int peer_id, const string& rpc_type, const string& data) -> bool {
        std::cout << "[RPC] Sending " << rpc_type << " to node " << peer_id << std::endl;
        return true;
    });
    
    std::cout << "[RaftKVStore] Node " << node_id << " initialized" << std::endl;
}

RaftKVStore::~RaftKVStore() {
    raft_node->stop();
}

Version RaftKVStore::put(const string& key, const string& value) {
    if (!is_leader()) {
        std::cerr << "Not leader, cannot write" << std::endl;
        return 0;
    }
    
    uint64_t index = 0;  // 声明 index 变量
    string command = "PUT";

    if (!raft_node->propose(command, key, value, index)) {
        return 0;
    }
    
    if (!wait_for_commit(index)) {
        return 0;
    }
    
    return index;
}

Version RaftKVStore::del(const string& key) {
    if (!is_leader()) {
        std::cerr << "Not leader, cannot delete" << std::endl;
        return 0;
    }
    
    uint64_t index;
    string command = "DELETE";
    
    if (!raft_node->propose(command, key, "", index)) {
        return 0;
    }
    
    if (!wait_for_commit(index)) {
        return 0;
    }
    
    return index;
}

bool RaftKVStore::get(const string& key, string& value, Version snap_ver) {
    return MVCCKVStore::get(key, value, snap_ver);
}

void RaftKVStore::on_log_applied(const RaftLogEntry& entry) {
    std::cout << "[RaftKVStore] Applying log: " << entry.index 
              << ", command=" << entry.command << std::endl;
    
    if (entry.command == "PUT") {
        MVCCKVStore::put(entry.key, entry.value);
    } else if (entry.command == "DELETE") {
        MVCCKVStore::del(entry.key);
    }
    
    {
        std::lock_guard<std::mutex> lock(pending_mutex);
        pending_proposals[entry.index] = {entry.key, entry.value};
        pending_cv.notify_all();
    }
}

bool RaftKVStore::wait_for_commit(uint64_t index, int timeout_ms) {
    std::unique_lock<std::mutex> lock(pending_mutex);
    
    return pending_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
        [this, index]() {
            return pending_proposals.find(index) != pending_proposals.end();
        });
}

} // namespace kvstore