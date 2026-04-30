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
    
    // 硬状态
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
    
    HardState GetHardState() const;             // 读取当前的 HardState
    void SetHardState(const HardState& state);
    
    // 配置
    std::vector<std::string> GetPeers() const;  // 读取集群节点列表
    void SetPeers(const std::vector<std::string>& peers); // 更新集群节点列表并持久化
    
private:
    std::string data_dir_;              // 数据目录路径
    HardState hard_state_;              // 内存中的硬状态缓存
    std::vector<std::string> peers_;    // 内存中的集群配置
    mutable std::mutex mutex_;
    
    void Load();
    void Save();
};

} // namespace raft