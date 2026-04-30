// raft/src/raft_persistence.cpp
#include "../include/raft_persistence.h"
#include <fstream>
#include <iostream>
#include <mutex>

namespace raft {

RaftPersistence::RaftPersistence(const std::string& data_dir) : data_dir_(data_dir) {
    system(("mkdir -p " + data_dir).c_str());
    Load();
}

RaftPersistence::~RaftPersistence() {
    Save();
}

RaftPersistence::HardState RaftPersistence::GetHardState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return hard_state_;
}

void RaftPersistence::SetHardState(const HardState& state) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (hard_state_ != state) {
        hard_state_ = state;
        Save();
    }
}

std::vector<std::string> RaftPersistence::GetPeers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peers_;
}

void RaftPersistence::SetPeers(const std::vector<std::string>& peers) {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_ = peers;
    Save();
}

void RaftPersistence::Load() {
    std::string filepath = data_dir_ + "/hard_state";
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs.is_open()) {
        return;
    }
    
    ifs.read(reinterpret_cast<char*>(&hard_state_.term), sizeof(hard_state_.term));
    
    uint32_t voted_len;
    ifs.read(reinterpret_cast<char*>(&voted_len), sizeof(voted_len));
    hard_state_.voted_for.resize(voted_len);
    ifs.read(&hard_state_.voted_for[0], voted_len);
    
    uint32_t peers_len;
    ifs.read(reinterpret_cast<char*>(&peers_len), sizeof(peers_len));
    peers_.resize(peers_len);
    for (uint32_t i = 0; i < peers_len; i++) {
        uint32_t peer_len;
        ifs.read(reinterpret_cast<char*>(&peer_len), sizeof(peer_len));
        ifs.read(&peers_[i][0], peer_len);
    }
    
    ifs.close();
}

void RaftPersistence::Save() {
    std::string filepath = data_dir_ + "/hard_state";
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs.is_open()) {
        std::cerr << "[RaftPersistence] Failed to open " << filepath << std::endl;
        return;
    }
    
    ofs.write(reinterpret_cast<const char*>(&hard_state_.term), sizeof(hard_state_.term));
    
    uint32_t voted_len = hard_state_.voted_for.size();
    ofs.write(reinterpret_cast<const char*>(&voted_len), sizeof(voted_len));
    ofs.write(hard_state_.voted_for.c_str(), voted_len);
    
    uint32_t peers_len = peers_.size();
    ofs.write(reinterpret_cast<const char*>(&peers_len), sizeof(peers_len));
    for (const auto& peer : peers_) {
        uint32_t peer_len = peer.size();
        ofs.write(reinterpret_cast<const char*>(&peer_len), sizeof(peer_len));
        ofs.write(peer.c_str(), peer_len);
    }
    
    ofs.close();
}

} // namespace raft