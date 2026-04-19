#pragma once
#include "sstable.h"
#include "mvcc.h"

namespace kvstore {

// 支持 MVCC 的 SSTable
class MVCCSSTable : public SSTable {
private:
    Version min_version;   // 该 SSTable 中最小的版本号
    Version max_version;   // 该 SSTable 中最大的版本号
    std::unique_ptr<BloomFilter> bloom_filter;
    
public:
    MVCCSSTable(const string& path);
    
    // 记录版本范围
    void set_version_range(Version min_ver, Version max_ver) {
        min_version = min_ver;
        max_version = max_ver;
    }
    
    Version get_min_version() const { return min_version; }
    Version get_max_version() const { return max_version; }
    
    // 检查该 SSTable 是否可能包含指定快照版本的数据 只要快照版本 >= 该 SSTable 中最小的版本号，就可能包含数据
    bool may_contain_version(Version snap_ver) const {
        return snap_ver >= min_version;
    }
    
    // 从带版本的数据创建 SSTable
    static MVCCSSTable* createFromVersionedData(
        const string& path,
        const std::map<string, std::map<Version, VersionedValue>, NaturalLess>& data,
        Version min_ver,
        Version max_ver
    );
};

} // namespace kvstore