// raft/include/raft_persistence.h
#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <mutex>

namespace raft {

class RaftPersistence {
public:
    explicit RaftPersistence(const std::string& data_dir);
    ~RaftPersistence();
    
    struct HardState {
        uint64_t term = 0;
        std::string voted_for;
        
        bool operator==(const HardState& other) const {
            return term == other.term && voted_for == other.voted_for;
        }
        bool operator!=(const HardState& other) const {
            return !(*this == other);
        }
    };
    
    HardState GetHardState() const;
    void SetHardState(const HardState& state);
    
    std::vector<std::string> GetPeers() const;
    void SetPeers(const std::vector<std::string>& peers);
    
private:
    std::string data_dir_;
    HardState hard_state_;
    std::vector<std::string> peers_;
    mutable std::mutex mutex_;
    
    void Load();
    void Save();
};

} // namespace raft