// include/mvcc_sstable.h
#pragma once
#include "sstable.h"
#include "mvcc.h"
#include "bloom_filter.h"
#include <memory>

namespace kvstore {

class MVCCSSTable : public SSTable {
private:
    Version min_version;
    Version max_version;
    std::unique_ptr<BloomFilter> bloom_filter;
    
    // 内存缓存：存储所有版本（用于快速查询）
    // key -> (version -> value, 版本降序)
    std::map<string, std::map<Version, string, std::greater<Version>>, NaturalLess> version_data_;
    size_t total_versions_;
    
public:
    MVCCSSTable(const string& path);
    ~MVCCSSTable() = default;
    
    void set_version_range(Version min_ver, Version max_ver) {
        min_version = min_ver;
        max_version = max_ver;
    }
    
    void set_bloom_filter(std::unique_ptr<BloomFilter> filter) {
        bloom_filter = std::move(filter);
    }
    
    void buildBloomFilter() {
        if (version_data_.empty()) return;
        
        bloom_filter = std::make_unique<BloomFilter>(version_data_.size() * 10);
        for (const auto& [key, _] : version_data_) {
            bloom_filter->add(key);
        }
    }
    
    bool may_contain_version(Version snap_ver) const {
        return snap_ver >= min_version;
    }
    
    bool may_contain_key(const string& key) const {
        if (!bloom_filter) return true;
        return bloom_filter->may_contain(key);
    }
    
    Version get_min_version() const { return min_version; }
    Version get_max_version() const { return max_version; }
    
    // 读取指定版本
    bool get_version(const string& key, Version snap_ver, string& value) const;
    
    // GC
    struct GCStats {
        size_t versions_before = 0;
        size_t versions_after = 0;
        size_t versions_removed = 0;
        size_t keys_removed = 0;
        bool file_deleted = false;
    };
    
    GCStats garbage_collect(Version min_keep_version);
    
    // 统计
    void get_stats(size_t& keys, size_t& versions) const;
    
    // 工厂方法
    static MVCCSSTable* createFromVersionedData(
        const string& path,
        const std::map<string, std::map<Version, VersionedValue>, NaturalLess>& data,
        Version min_ver,
        Version max_ver);
    
    // 获取版本数据（用于 compaction）
    const std::map<string, std::map<Version, string, std::greater<Version>>, NaturalLess>& 
    get_version_data() const {
        return version_data_;
    }
};

} // namespace kvstore