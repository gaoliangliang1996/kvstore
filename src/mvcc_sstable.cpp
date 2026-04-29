// src/mvcc_sstable.cpp
#include "mvcc_sstable.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>

namespace kvstore {

MVCCSSTable::MVCCSSTable(const string& path) 
    : SSTable(path), min_version(0), max_version(0), total_versions_(0) {
    // 从文件读取版本范围（需要扩展文件格式）
    // 实际实现中应该从文件头读取
}

// 完整的创建方法：存储所有版本到 version_data_
MVCCSSTable* MVCCSSTable::createFromVersionedData(
    const string& path,
    const std::map<string, std::map<Version, VersionedValue>, NaturalLess>& data,
    Version min_ver,
    Version max_ver) {
    
    MVCCSSTable* sstable = new MVCCSSTable(path);
    sstable->set_version_range(min_ver, max_ver);
    
    size_t total_versions = 0;
    
    // 1. 存储所有版本到 version_data_（内存缓存）
    for (const auto& [key, versions] : data) {
        for (const auto& [ver, vv] : versions) {
            if (!vv.deleted) {
                // 存储到内存缓存
                sstable->version_data_[key][ver] = vv.value;
                total_versions++;
            }
        }
    }
    sstable->total_versions_ = total_versions;
    
    // 2. 写入 SSTable 文件（只写最新版本，减少磁盘占用）
    // 选项 A: 只写最新版本（节省磁盘空间）
    std::map<string, string, NaturalLess> latest_data;
    for (const auto& [key, versions] : data) {
        if (!versions.empty()) {
            for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
                if (!it->second.deleted) {
                    latest_data[key] = it->second.value;
                    break;
                }
            }
        }
    }
    
    // 写入文件
    for (const auto& [key, value] : latest_data) {
        sstable->put(key, value);
    }
    
    // 或者选项 B: 写入所有版本（完整 MVCC 支持，但占用更多空间）
    // for (const auto& [key, versions] : data) {
    //     for (const auto& [ver, vv] : versions) {
    //         if (!vv.deleted) {
    //             string version_key = key + "\x00" + std::to_string(ver);
    //             sstable->put(version_key, vv.value);
    //         }
    //     }
    // }
    
    // 3. 构建 Bloom Filter
    sstable->buildBloomFilter();
    
    return sstable;
}

MVCCSSTable::GCStats MVCCSSTable::garbage_collect(Version min_keep_version) {
    GCStats stats;
    
    // 统计清理前的版本数
    for (const auto& [key, versions] : version_data_) {
        stats.versions_before += versions.size();
    }
    
    // 如果整个 SSTable 的所有版本都太旧，直接标记删除
    if (max_version < min_keep_version) {
        stats.versions_removed = stats.versions_before;
        stats.file_deleted = true;
        
        // 清空内存缓存
        version_data_.clear();
        total_versions_ = 0;
        
        return stats;
    }
    
    // 清理每个 key 的旧版本
    std::vector<string> keys_to_remove;
    
    for (auto& [key, versions] : version_data_) {
        auto it = versions.begin();
        while (it != versions.end()) {
            if (it->first < min_keep_version) {
                it = versions.erase(it);
                stats.versions_removed++;
                total_versions_--;
            } else {
                ++it;
            }
        }
        
        if (versions.empty()) {
            keys_to_remove.push_back(key);
        }
    }
    
    // 删除空 key
    for (const auto& key : keys_to_remove) {
        version_data_.erase(key);
        stats.keys_removed++;
    }
    
    // 统计清理后的版本数
    for (const auto& [key, versions] : version_data_) {
        stats.versions_after += versions.size();
    }
    
    // 更新版本范围
    if (!version_data_.empty()) {
        min_version = min_keep_version;
    }
    
    return stats;
}

// 读取指定版本的值
bool MVCCSSTable::get_version(const string& key, Version snap_ver, string& value) const {
    auto it = version_data_.find(key);
    if (it == version_data_.end()) {
        return false;
    }
    
    // 找 <= snap_ver 的最大版本
    for (const auto& [ver, val] : it->second) {
        if (ver <= snap_ver) {
            value = val;
            return true;
        }
    }
    return false;
}

// 获取统计信息
void MVCCSSTable::get_stats(size_t& keys, size_t& versions) const {
    keys = version_data_.size();
    versions = total_versions_;
}

} // namespace kvstore