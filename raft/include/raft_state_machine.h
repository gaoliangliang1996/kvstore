#pragma once
#include <functional>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <mutex>

namespace raft {

// 状态机接口（与 KVStore 集成）
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    
    // 应用命令
    virtual void Apply(const std::string& command, const std::vector<uint8_t>& data, uint64_t index) = 0;
    
    // 创建快照
    virtual std::vector<uint8_t> TakeSnapshot() = 0;
    
    // 恢复快照
    virtual void RestoreSnapshot(const std::vector<uint8_t>& data) = 0;
    
    // 获取状态大小
    virtual size_t GetStateSize() const = 0;
};

// KV 存储状态机实现
class KVStateMachine : public RaftStateMachine {
public:
    explicit KVStateMachine();
    ~KVStateMachine() override = default;
    
    void Apply(const std::string& command, const std::vector<uint8_t>& data, uint64_t index) override;
    std::vector<uint8_t> TakeSnapshot() override;
    void RestoreSnapshot(const std::vector<uint8_t>& data) override;
    size_t GetStateSize() const override;
    
    // 读取操作（不需要共识）
    bool Get(const std::string& key, std::string& value) const;
    
private:
    std::map<std::string, std::string> kv_store_;
    mutable std::mutex mutex_;
};

} // namespace raft