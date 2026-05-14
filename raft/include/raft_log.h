// raft/include/raft_log.h
#pragma once
#include "raft.h"
#include "raft.pb.h"  // 包含 protobuf 生成的 LogEntry
#include <vector>
#include <mutex>
#include <fstream>

namespace raft {

class RaftLog {
public:
    explicit RaftLog(const std::string& data_dir);
    ~RaftLog();
    
    // 日志操作
    uint64_t GetLastLogIndex() const;
    uint64_t GetLastLogTerm() const;
    LogEntry GetEntry(uint64_t index) const;
    std::vector<LogEntry> GetEntriesFrom(uint64_t index) const;
    std::vector<LogEntry> GetEntriesBetween(uint64_t start, uint64_t end) const;
    
    void AppendEntry(const LogEntry& entry);
    void AppendEntries(const std::vector<LogEntry>& entries);
    void TruncateFrom(uint64_t index);
    
    // 快照
    void InstallSnapshot(uint64_t last_included_index, uint64_t last_included_term,
                         const std::vector<uint8_t>& data);
    bool HasSnapshot() const;
    uint64_t GetSnapshotLastIndex() const;
    uint64_t GetSnapshotLastTerm() const;
    const std::vector<uint8_t>& GetSnapshotData() const;
    
    // 持久化
    void Persist();
    void Load();
    
    // 统计信息
    size_t Size() const;
    
private:
    std::string data_dir_;
    std::vector<LogEntry> entries_;
    mutable std::mutex mutex_;
    
    bool has_snapshot_;
    uint64_t snapshot_last_index_;
    uint64_t snapshot_last_term_;
    std::vector<uint8_t> snapshot_data_;
    
    std::string log_file_;
    std::string snapshot_file_;
    
    void LoadFromFile();
    void SaveToFile();
    void LoadSnapshot();
    void SaveSnapshot();
};

} // namespace raft