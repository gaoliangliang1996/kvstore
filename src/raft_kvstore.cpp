// src/raft_kvstore.cpp
#include "raft_kvstore.h"
#include <sstream>
#include <future>
#include <chrono>
#include <thread>

namespace kvstore {

// ============== RaftCommand 序列化/反序列化 ==============

std::vector<uint8_t> RaftCommand::Serialize() const {
    std::stringstream ss;
    
    ss.write(reinterpret_cast<const char*>(&type), sizeof(type));
    ss.write(reinterpret_cast<const char*>(&request_id), sizeof(request_id));
    
    switch (type) {
        case RaftCommandType::PUT:
            {
                uint32_t key_len = key.size();
                ss.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                ss.write(key.c_str(), key_len);
                
                uint32_t val_len = value.size();
                ss.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
                ss.write(value.c_str(), val_len);
            }
            break;
            
        case RaftCommandType::DELETE:
            {
                uint32_t key_len = key.size();
                ss.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                ss.write(key.c_str(), key_len);
            }
            break;
            
        case RaftCommandType::BATCH_PUT:
            {
                uint32_t batch_size = batch_puts.size();
                ss.write(reinterpret_cast<const char*>(&batch_size), sizeof(batch_size));
                
                for (const auto& [k, v] : batch_puts) {
                    uint32_t key_len = k.size();
                    ss.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                    ss.write(k.c_str(), key_len);
                    
                    uint32_t val_len = v.size();
                    ss.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
                    ss.write(v.c_str(), val_len);
                }
            }
            break;
            
        case RaftCommandType::BATCH_DELETE:
            {
                uint32_t batch_size = batch_deletes.size();
                ss.write(reinterpret_cast<const char*>(&batch_size), sizeof(batch_size));
                
                for (const auto& k : batch_deletes) {
                    uint32_t key_len = k.size();
                    ss.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
                    ss.write(k.c_str(), key_len);
                }
            }
            break;
    }
    
    std::string str = ss.str();
    return std::vector<uint8_t>(str.begin(), str.end());
}

RaftCommand RaftCommand::Deserialize(const std::vector<uint8_t>& data) {
    RaftCommand cmd;
    std::stringstream ss(std::string(data.begin(), data.end()));
    
    ss.read(reinterpret_cast<char*>(&cmd.type), sizeof(cmd.type));
    ss.read(reinterpret_cast<char*>(&cmd.request_id), sizeof(cmd.request_id));
    
    switch (cmd.type) {
        case RaftCommandType::PUT:
            {
                uint32_t key_len, val_len;
                ss.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                cmd.key.resize(key_len);
                ss.read(&cmd.key[0], key_len);
                
                ss.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
                cmd.value.resize(val_len);
                ss.read(&cmd.value[0], val_len);
            }
            break;
            
        case RaftCommandType::DELETE:
            {
                uint32_t key_len;
                ss.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                cmd.key.resize(key_len);
                ss.read(&cmd.key[0], key_len);
            }
            break;
            
        case RaftCommandType::BATCH_PUT:
            {
                uint32_t batch_size;
                ss.read(reinterpret_cast<char*>(&batch_size), sizeof(batch_size));
                
                for (uint32_t i = 0; i < batch_size; i++) {
                    uint32_t key_len, val_len;
                    std::string key, value;
                    
                    ss.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                    key.resize(key_len);
                    ss.read(&key[0], key_len);
                    
                    ss.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
                    value.resize(val_len);
                    ss.read(&value[0], val_len);
                    
                    cmd.batch_puts.emplace_back(key, value);
                }
            }
            break;
            
        case RaftCommandType::BATCH_DELETE:
            {
                uint32_t batch_size;
                ss.read(reinterpret_cast<char*>(&batch_size), sizeof(batch_size));
                
                for (uint32_t i = 0; i < batch_size; i++) {
                    uint32_t key_len;
                    std::string key;
                    
                    ss.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
                    key.resize(key_len);
                    ss.read(&key[0], key_len);
                    
                    cmd.batch_deletes.push_back(key);
                }
            }
            break;
    }
    
    return cmd;
}

// ============== RaftKVStore 实现 ==============

RaftKVStore::RaftKVStore(const Config& cfg, const raft::RaftConfig& raft_cfg)
    : kv_config_(cfg), raft_config_(raft_cfg) {
    
    std::cout << "[RaftKVStore] Initializing..." << std::endl;
    
    // 创建存储层
    storage_ = std::make_unique<MVCCKVStore>(cfg);
    
    // 创建 Raft 节点
    raft_node_ = std::make_unique<raft::RaftNode>(raft_cfg);
    
    // 设置回调
    raft_node_->SetCommitCallback(
        [this](uint64_t index, const std::string& command, const std::vector<uint8_t>& data) {
            OnCommit(index, command, data);
        });
    
    raft_node_->SetLeaderChangeCallback(
        [this](const std::string& leader_id) {
            OnLeaderChange(leader_id);
        });
    
    raft_node_->Start();
    
    std::cout << "[RaftKVStore] Started, node_id=" << raft_cfg.node_id << std::endl;
}

RaftKVStore::~RaftKVStore() {
    if (raft_node_) {
        raft_node_->Stop();
    }
}

Version RaftKVStore::put(const std::string& key, const std::string& value) {
    if (!IsLeader()) {
        Version result;
        if (ForwardToLeader("put", key, value, result)) {
            return result;
        }
        return 0;
    }
    
    uint64_t request_id = next_request_id_++;
    
    RaftCommand cmd;
    cmd.type = RaftCommandType::PUT;
    cmd.key = key;
    cmd.value = value;
    cmd.request_id = request_id;
    
    auto data = cmd.Serialize();
    
    std::promise<std::pair<bool, Version>> promise;
    auto future = promise.get_future();
    
    raft_node_->Propose("put", data,
        [&promise](bool success, uint64_t index) {
            promise.set_value({success, index});
        });
    
    auto result = future.get();
    return result.second;
}

Version RaftKVStore::del(const std::string& key) {
    if (!IsLeader()) {
        Version result;
        if (ForwardToLeader("del", key, "", result)) {
            return result;
        }
        return 0;
    }
    
    uint64_t request_id = next_request_id_++;
    
    RaftCommand cmd;
    cmd.type = RaftCommandType::DELETE;
    cmd.key = key;
    cmd.request_id = request_id;
    
    auto data = cmd.Serialize();
    
    std::promise<std::pair<bool, Version>> promise;
    auto future = promise.get_future();
    
    raft_node_->Propose("del", data,
        [&promise](bool success, uint64_t index) {
            promise.set_value({success, index});
        });
    
    auto result = future.get();
    return result.second;
}

RaftKVStore::BatchResult RaftKVStore::batch_put(
    const std::vector<std::pair<std::string, std::string>>& pairs) {
    
    BatchResult result;
    result.success = false;
    
    if (!IsLeader()) {
        result.error = "Not leader";
        return result;
    }
    
    uint64_t request_id = next_request_id_++;
    
    RaftCommand cmd;
    cmd.type = RaftCommandType::BATCH_PUT;
    cmd.batch_puts = pairs;
    cmd.request_id = request_id;
    
    auto data = cmd.Serialize();
    
    std::promise<std::pair<bool, uint64_t>> promise;
    auto future = promise.get_future();
    
    raft_node_->Propose("batch_put", data,
        [&promise](bool success, uint64_t index) {
            promise.set_value({success, index});
        });
    
    auto commit_result = future.get();
    
    if (commit_result.first) {
        result.success = true;
        for (size_t i = 0; i < pairs.size(); i++) {
            result.versions.push_back(commit_result.second);
        }
    } else {
        result.error = "Commit failed";
    }
    
    return result;
}

RaftKVStore::BatchResult RaftKVStore::batch_del(const std::vector<std::string>& keys) {
    BatchResult result;
    result.success = false;
    
    if (!IsLeader()) {
        result.error = "Not leader";
        return result;
    }
    
    uint64_t request_id = next_request_id_++;
    
    RaftCommand cmd;
    cmd.type = RaftCommandType::BATCH_DELETE;
    cmd.batch_deletes = keys;
    cmd.request_id = request_id;
    
    auto data = cmd.Serialize();
    
    std::promise<std::pair<bool, uint64_t>> promise;
    auto future = promise.get_future();
    
    raft_node_->Propose("batch_del", data,
        [&promise](bool success, uint64_t index) {
            promise.set_value({success, index});
        });
    
    auto commit_result = future.get();
    
    if (commit_result.first) {
        result.success = true;
        for (size_t i = 0; i < keys.size(); i++) {
            result.versions.push_back(commit_result.second);
        }
    } else {
        result.error = "Commit failed";
    }
    
    return result;
}

bool RaftKVStore::get(const std::string& key, std::string& value, Version snap_ver) {
    return storage_->get(key, value, snap_ver);
}

void RaftKVStore::OnCommit(uint64_t index, const std::string& command, 
                           const std::vector<uint8_t>& data) {
    std::cout << "[RaftKVStore] OnCommit: index=" << index 
              << " command=" << command << std::endl;
    
    RaftCommand cmd = RaftCommand::Deserialize(data);
    
    switch (cmd.type) {
        case RaftCommandType::PUT:
            ApplyPut(cmd.key, cmd.value, cmd.request_id);
            break;
        case RaftCommandType::DELETE:
            ApplyDelete(cmd.key, cmd.request_id);
            break;
        case RaftCommandType::BATCH_PUT:
            ApplyBatchPut(cmd.batch_puts, cmd.request_id);
            break;
        case RaftCommandType::BATCH_DELETE:
            ApplyBatchDelete(cmd.batch_deletes, cmd.request_id);
            break;
    }
}

void RaftKVStore::ApplyPut(const std::string& key, const std::string& value, 
                           uint64_t request_id) {
    storage_->put(key, value);
    
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_requests_.find(request_id);
    if (it != pending_requests_.end()) {
        it->second(true, 0);
        pending_requests_.erase(it);
    }
}

void RaftKVStore::ApplyDelete(const std::string& key, uint64_t request_id) {
    storage_->del(key);
    
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_requests_.find(request_id);
    if (it != pending_requests_.end()) {
        it->second(true, 0);
        pending_requests_.erase(it);
    }
}

void RaftKVStore::ApplyBatchPut(const std::vector<std::pair<std::string, std::string>>& pairs,
                                 uint64_t request_id) {
    for (const auto& [key, value] : pairs) {
        storage_->put(key, value);
    }
    
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_requests_.find(request_id);
    if (it != pending_requests_.end()) {
        it->second(true, 0);
        pending_requests_.erase(it);
    }
}

void RaftKVStore::ApplyBatchDelete(const std::vector<std::string>& keys, uint64_t request_id) {
    for (const auto& key : keys) {
        storage_->del(key);
    }
    
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_requests_.find(request_id);
    if (it != pending_requests_.end()) {
        it->second(true, 0);
        pending_requests_.erase(it);
    }
}

void RaftKVStore::OnLeaderChange(const std::string& leader_id) {
    std::cout << "[RaftKVStore] Leader changed to: " << leader_id << std::endl;
}

bool RaftKVStore::ForwardToLeader(const std::string& operation, const std::string& key,
                                   const std::string& value, Version& result) {
    std::string leader_id = GetLeaderId();
    
    if (leader_id.empty() || leader_id == raft_config_.node_id) {
        return false;
    }
    
    std::cout << "[RaftKVStore] Forwarding " << operation << " to leader " << leader_id << std::endl;
    
    if (redirect_handler_) {
        std::string leader_addr = redirect_handler_(leader_id);
        if (!leader_addr.empty()) {
            // 转发请求
        }
    }
    
    return false;
}

bool RaftKVStore::IsLeader() const {
    return raft_node_ && raft_node_->IsLeader();
}

std::string RaftKVStore::GetLeaderId() const {
    return raft_node_ ? raft_node_->GetLeaderId() : "";
}

raft::NodeState RaftKVStore::GetRaftState() const {
    return raft_node_ ? raft_node_->GetState() : raft::NodeState::FOLLOWER;
}

uint64_t RaftKVStore::GetCurrentTerm() const {
    return raft_node_ ? raft_node_->GetCurrentTerm() : 0;
}

void RaftKVStore::AddPeer(const std::string& peer_id) {
    std::cout << "[RaftKVStore] Adding peer: " << peer_id << std::endl;
}

void RaftKVStore::RemovePeer(const std::string& peer_id) {
    std::cout << "[RaftKVStore] Removing peer: " << peer_id << std::endl;
}

std::vector<std::string> RaftKVStore::GetPeers() const {
    std::vector<std::string> result;
    // 将 raft::NodeAddress 转换为 string
    for (const auto& peer : raft_config_.peers) {
        // 假设 NodeAddress 有 node_id 或 address 成员
        // 根据 raft.h 中 NodeAddress 的实际定义调整
        result.push_back(peer.node_id);  // 或者 peer.address
    }
    return result;
}

bool RaftKVStore::WaitForLeader(int timeout_ms) {
    auto start = std::chrono::steady_clock::now();
    
    while (GetLeaderId().empty()) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count() > timeout_ms) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return true;
}

void RaftKVStore::SetRedirectHandler(std::function<std::string(const std::string&)> handler) {
    redirect_handler_ = handler;
}

} // namespace kvstore