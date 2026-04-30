// raft/include/raft_log.h
#pragma once
#include "raft.h"
#include <vector>
#include <mutex>
#include <fstream>

namespace raft {

class RaftLog {
public:
    explicit RaftLog(const std::string& data_dir);
    ~RaftLog();
    
    // 日志操作
    uint64_t GetLastLogIndex() const; // 获取最新日志索引
    uint64_t GetLastLogTerm() const;  // 获取最新日志的 term
    LogEntry GetEntry(uint64_t index) const; // 按索引获取单条日志
    std::vector<LogEntry> GetEntriesFrom(uint64_t index) const; // 获取 [index, 末尾] 的日志
    std::vector<LogEntry> GetEntriesBetween(uint64_t start, uint64_t end) const; // 获取 [start, end) 的日志
    
    // 日志写入
    void AppendEntry(const LogEntry& entry);                  // 追加单条日志
    void AppendEntries(const std::vector<LogEntry>& entries); // 批量追加
    void TruncateFrom(uint64_t index);                        // 从指定索引截断
    
    // 快照
    void InstallSnapshot(uint64_t last_included_index, uint64_t last_included_term,
                         const std::vector<uint8_t>& data);
    bool HasSnapshot() const;
    uint64_t GetSnapshotLastIndex() const; // 快照最后索引
    uint64_t GetSnapshotLastTerm() const;  // 快照最后 term
    
    // 持久化
    void Persist(); // 手动落盘
    void Load();    // 从磁盘加载
    
    // 统计信息
    size_t Size() const; // 内存中日志条目数量
    
private:
    std::string data_dir_;          // 数据目录，"./raft_data"

    std::vector<LogEntry> entries_; // 日志条目数据（仅存储快照之后的日志）
    mutable std::mutex mutex_;
    
    // 快照
    bool has_snapshot_;
    uint64_t snapshot_last_index_;  // 快照覆盖的最后一条日志索引
    uint64_t snapshot_last_term_;   // 快照覆盖的最后一条日志的 term
    std::vector<uint8_t> snapshot_data_; // 快照数据
    
    // 持久化文件
    std::string log_file_;      // 日志文件路径：data_dir/raft.log
    std::string snapshot_file_; // 快照文件路径：data_dir/raft.snapshot
    
    void LoadFromFile();        // 从 raft.log 读取日志
    void SaveToFile();          // 将日志写入 raft.log
    void LoadSnapshot();        // 从 raft.snapshot 读取快照
    void SaveSnapshot();        // 将快照写入 raft.snapshot
    
    // 辅助函数
    void CopyEntry(const LogEntry& src, LogEntry& dst);
};

} // namespace raft