#include "mvcc_sstable.h"
#include <fcntl.h>
#include <unistd.h>

namespace kvstore {

// 这个文件实现了 MVCCSSTable 的核心逻辑，包括版本范围管理、数据写入和读取等功能。 
// MVCCSSTable 继承自 SSTable，增加了版本范围的记录和检查功能。
// 每个 SSTable 文件会记录该文件中数据的最小版本号和最大版本号，读取时可以根据快照版本来判断是否需要访问该 SSTable，从而提高读取效率。
// MVCCSSTable 还提供了一个静态工厂方法 createFromVersionedData ，用于从带版本的数据创建一个新的 SSTable 文件，并设置正确的版本范围。
// 实际实现中，版本范围应该存储在 SSTable 文件的元数据中，而不是从文件名解析，这里为了简化实现而采用了这种方式。

MVCCSSTable::MVCCSSTable(const string& path) 
    : SSTable(path), min_version(0), max_version(0) {
    // 从文件读取版本范围（需要扩展文件格式）
    // 简化实现：从文件名解析
    // 实际应该存储在文件头中
}

MVCCSSTable* MVCCSSTable::createFromVersionedData(
    const string& path,
    const std::map<string, std::map<Version, VersionedValue>, NaturalLess>& data, // key -> (version -> VersionedValue)
    Version min_ver,
    Version max_ver) {
    
    // 转换格式：每个 key 只保留最新的有效版本（用于持久化）
    std::map<string, string, NaturalLess> latest_data; // key -> value
    for (const auto& [key, versions] : data) { // 遍历每个 key 和对应的版本数据
        if (!versions.empty()) {
            // 反向遍历找最新的非删除版本
            for (auto it = versions.rbegin(); it != versions.rend(); ++it) { // 反向遍历版本，找到最新的版本
                if (!it->second.deleted) {
                    latest_data[key] = it->second.value;
                    break;
                }
            }
        }
    }
    
    // 创建普通 SSTable
    MVCCSSTable* sstable = new MVCCSSTable(path);
    
    // 写入数据
    for (const auto& [key, value] : latest_data) {
        sstable->put(key, value);
    }
    
    sstable->set_version_range(min_ver, max_ver);
    sstable->buildBloomFilter();
    
    return sstable;
}

} // namespace kvstore