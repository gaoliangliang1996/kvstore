// raft/src/raft_log.cpp
#include "../include/raft_log.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstring>
#include <sys/stat.h>

namespace raft {

RaftLog::RaftLog(const std::string& data_dir)
    : data_dir_(data_dir),
      has_snapshot_(false),
      snapshot_last_index_(0),
      snapshot_last_term_(0) {
    
    log_file_ = data_dir + "/raft.log";
    snapshot_file_ = data_dir + "/raft.snapshot";
    
    std::string cmd = "mkdir -p " + data_dir;
    system(cmd.c_str());
    
    Load();
}

RaftLog::~RaftLog() {
    Persist();
}

uint64_t RaftLog::GetLastLogIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.empty()) {
        return snapshot_last_index_;
    }
    return entries_.back().index();
}

uint64_t RaftLog::GetLastLogTerm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.empty()) {
        return snapshot_last_term_;
    }
    return entries_.back().term();
}

LogEntry RaftLog::GetEntry(uint64_t index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (index <= snapshot_last_index_) {
        LogEntry entry;
        entry.set_index(index);
        entry.set_term(snapshot_last_term_);
        return entry;
    }
    
    size_t pos = index - snapshot_last_index_ - 1;
    if (pos < entries_.size()) {
        return entries_[pos];
    }
    
    return LogEntry();
}

std::vector<LogEntry> RaftLog::GetEntriesFrom(uint64_t index) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<LogEntry> result;
    
    if (index <= snapshot_last_index_) {
        index = snapshot_last_index_ + 1;
    }
    
    if (index > snapshot_last_index_) {
        size_t start = index - snapshot_last_index_ - 1;
        for (size_t i = start; i < entries_.size(); i++) {
            result.push_back(entries_[i]);
        }
    }
    
    return result;
}

std::vector<LogEntry> RaftLog::GetEntriesBetween(uint64_t start, uint64_t end) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<LogEntry> result;
    
    for (const auto& entry : entries_) {
        uint64_t idx = entry.index();
        if (idx >= start && idx < end) {
            result.push_back(entry);
        }
    }
    
    return result;
}

void RaftLog::AppendEntry(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.push_back(entry);
    Persist();
}

void RaftLog::AppendEntries(const std::vector<LogEntry>& entries) {
    if (entries.empty()) return;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (const auto& entry : entries) {
        uint64_t idx = entry.index();
        if (idx > snapshot_last_index_) {
            size_t pos = idx - snapshot_last_index_ - 1;
            if (pos < entries_.size()) {
                entries_[pos] = entry;
            } else {
                entries_.push_back(entry);
            }
        }
    }
    
    Persist();
}

void RaftLog::TruncateFrom(uint64_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (index <= snapshot_last_index_) {
        entries_.clear();
    } else {
        size_t pos = index - snapshot_last_index_ - 1;
        if (pos < entries_.size()) {
            entries_.resize(pos);
        }
    }
    
    Persist();
}

void RaftLog::InstallSnapshot(uint64_t last_included_index, uint64_t last_included_term,
                              const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    has_snapshot_ = true;
    snapshot_last_index_ = last_included_index;
    snapshot_last_term_ = last_included_term;
    snapshot_data_ = data;
    
    entries_.clear();
    
    Persist();
    SaveSnapshot();
}

bool RaftLog::HasSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return has_snapshot_;
}

uint64_t RaftLog::GetSnapshotLastIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_last_index_;
}

uint64_t RaftLog::GetSnapshotLastTerm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_last_term_;
}

const std::vector<uint8_t>& RaftLog::GetSnapshotData() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_data_;
}

size_t RaftLog::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

void RaftLog::Persist() {
    SaveToFile();
}

void RaftLog::Load() {
    LoadFromFile();
    LoadSnapshot();
}

void RaftLog::SaveToFile() {
    std::ofstream ofs(log_file_, std::ios::binary);
    if (!ofs.is_open()) {
        std::cerr << "[RaftLog] Failed to open " << log_file_ << std::endl;
        return;
    }
    
    uint64_t size = entries_.size();
    ofs.write(reinterpret_cast<const char*>(&size), sizeof(size));
    
    for (const auto& entry : entries_) {
        uint64_t term = entry.term();
        uint64_t index = entry.index();
        ofs.write(reinterpret_cast<const char*>(&term), sizeof(term));
        ofs.write(reinterpret_cast<const char*>(&index), sizeof(index));
        
        uint32_t cmd_len = entry.command().size();
        ofs.write(reinterpret_cast<const char*>(&cmd_len), sizeof(cmd_len));
        ofs.write(entry.command().c_str(), cmd_len);
        
        uint32_t data_len = entry.data().size();
        ofs.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
        ofs.write(entry.data().c_str(), data_len);
    }
    
    ofs.close();
}

void RaftLog::LoadFromFile() {
    std::ifstream ifs(log_file_, std::ios::binary);
    if (!ifs.is_open()) {
        return;
    }
    
    entries_.clear();
    
    uint64_t size;
    ifs.read(reinterpret_cast<char*>(&size), sizeof(size));
    
    for (uint64_t i = 0; i < size; i++) {
        LogEntry entry;
        uint64_t term, index;
        ifs.read(reinterpret_cast<char*>(&term), sizeof(term));
        ifs.read(reinterpret_cast<char*>(&index), sizeof(index));
        entry.set_term(term);
        entry.set_index(index);
        
        uint32_t cmd_len;
        ifs.read(reinterpret_cast<char*>(&cmd_len), sizeof(cmd_len));
        std::string cmd(cmd_len, '\0');
        ifs.read(&cmd[0], cmd_len);
        entry.set_command(cmd);
        
        uint32_t data_len;
        ifs.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
        std::string data_str(data_len, '\0');
        ifs.read(&data_str[0], data_len);
        entry.set_data(data_str);
        
        entries_.push_back(entry);
    }
    
    ifs.close();
}

void RaftLog::SaveSnapshot() {
    std::ofstream ofs(snapshot_file_, std::ios::binary);
    if (!ofs.is_open()) {
        std::cerr << "[RaftLog] Failed to open " << snapshot_file_ << std::endl;
        return;
    }
    
    ofs.write(reinterpret_cast<const char*>(&snapshot_last_index_), sizeof(snapshot_last_index_));
    ofs.write(reinterpret_cast<const char*>(&snapshot_last_term_), sizeof(snapshot_last_term_));
    
    uint32_t data_len = snapshot_data_.size();
    ofs.write(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
    ofs.write(reinterpret_cast<const char*>(snapshot_data_.data()), data_len);
    
    ofs.close();
}

void RaftLog::LoadSnapshot() {
    std::ifstream ifs(snapshot_file_, std::ios::binary);
    if (!ifs.is_open()) {
        has_snapshot_ = false;
        return;
    }
    
    ifs.read(reinterpret_cast<char*>(&snapshot_last_index_), sizeof(snapshot_last_index_));
    ifs.read(reinterpret_cast<char*>(&snapshot_last_term_), sizeof(snapshot_last_term_));
    
    uint32_t data_len;
    ifs.read(reinterpret_cast<char*>(&data_len), sizeof(data_len));
    snapshot_data_.resize(data_len);
    ifs.read(reinterpret_cast<char*>(snapshot_data_.data()), data_len);
    
    has_snapshot_ = true;
    ifs.close();
}

} // namespace raft