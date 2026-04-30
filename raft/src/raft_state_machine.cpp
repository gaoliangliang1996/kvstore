// raft/src/raft_state_machine.cpp
#include "../include/raft_state_machine.h"
#include <sstream>
#include <iostream>
#include <mutex>

namespace raft {

// KVStateMachine 实现
KVStateMachine::KVStateMachine() {}

void KVStateMachine::Apply(const std::string& command, const std::vector<uint8_t>& data, uint64_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 解析命令
    if (command == "put") {
        // 简单解析：key:value 格式
        std::string data_str(data.begin(), data.end());
        size_t pos = data_str.find(':');
        if (pos != std::string::npos) {
            std::string key = data_str.substr(0, pos);
            std::string value = data_str.substr(pos + 1);
            kv_store_[key] = value;
            std::cout << "[StateMachine] Applied PUT " << key << "=" << value 
                      << " at index " << index << std::endl;
        }
    } else if (command == "delete") {
        std::string key(data.begin(), data.end());
        kv_store_.erase(key);
        std::cout << "[StateMachine] Applied DELETE " << key 
                  << " at index " << index << std::endl;
    }
}

std::vector<uint8_t> KVStateMachine::TakeSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 遍历 kv_store_ 序列化为文本格式
    std::stringstream ss;
    for (const auto& [key, value] : kv_store_) {
        ss << key << ":" << value << "\n";
    }
    
    std::string snapshot_str = ss.str();
    std::vector<uint8_t> snapshot(snapshot_str.begin(), snapshot_str.end());
    
    std::cout << "[StateMachine] Taking snapshot, size=" << snapshot.size() 
              << " bytes, keys=" << kv_store_.size() << std::endl;
    
    return snapshot;
}

void KVStateMachine::RestoreSnapshot(const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    kv_store_.clear();
    
    std::string snapshot_str(data.begin(), data.end());
    std::stringstream ss(snapshot_str);
    std::string line;
    
    while (std::getline(ss, line)) {
        size_t pos = line.find(':');
        if (pos != std::string::npos) {
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);
            kv_store_[key] = value;
        }
    }
    
    std::cout << "[StateMachine] Restored snapshot, keys=" << kv_store_.size() << std::endl;
}

size_t KVStateMachine::GetStateSize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return kv_store_.size();
}

bool KVStateMachine::Get(const std::string& key, std::string& value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = kv_store_.find(key);
    if (it != kv_store_.end()) {
        value = it->second;
        return true;
    }
    return false;
}

} // namespace raft