#pragma once
#include "common.h"
#include <atomic>
#include <map>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <mutex>

namespace kvstore {

// 带版本号的 Value
// VersionedValue 结构体表示一个带版本号的值，包含实际的 value、版本号和删除标记。deleted 字段用于标记该版本是否被删除，这样在读取时可以正确处理被删除的版本。
struct VersionedValue {
    string value;
    Version version;
    bool deleted;  // 删除标记
    
    VersionedValue() : version(0), deleted(false) {}
    
    VersionedValue(const string& v, Version ver) : value(v), version(ver), deleted(false) {}
    
    VersionedValue(Version ver, bool is_deleted = true) : version(ver), deleted(is_deleted) {}
    
    // 三参数构造函数（用于 emplace）
    VersionedValue(const string& v, Version ver, bool is_deleted) : value(v), version(ver), deleted(is_deleted) {}
};

// MVCC 跳表节点（支持多版本）
// 每个节点存储一个 key 和该 key 的多个版本数据; 版本数据使用 std::map 存储，key 是版本号，value 是 VersionedValue。
// 这样可以支持同一个 key 的多个版本，并且可以根据快照版本来访问对应版本的数据。
class MVCCNode { 
public:
    string key;
    std::map<Version, VersionedValue, std::greater<Version>> versions; // version -> VersionedValue  版本降序
    
    MVCCNode(const string& k) : key(k) {}
    
    void add_version(Version ver, const string& val, bool deleted = false) {
        versions[ver] = VersionedValue(val, ver, deleted);
    }

    bool get_value(Version snapshot_ver, string& value) const {
        for (const auto& [ver, vv] : versions) {
            if (ver <= snapshot_ver) {
                if (!vv.deleted) {
                    value = vv.value;
                    return true;
                }
                return false;
            }
        }
        return false;
    }
    
    // 获取最新版本号
    Version get_latest_version() const {
        if (versions.empty()) return 0;
        return versions.begin()->first;
    }

    // 清理旧版本（GC）, 保留 >= min_keep_version 的版本
    size_t cleanup_old_versions(Version min_keep_version) { // min_keep_version 是要保留的最小版本号，小于这个版本的都可以删除
        size_t removed = 0;
        auto it = versions.begin();
        while (it != versions.end()) {
            if (it->first < min_keep_version) {
                it = versions.erase(it);
                removed++;
            } else {
                ++it;
            }
        }
        return removed;
    }

    size_t version_count() const { return versions.size(); }
};

// MemTable 实现类（可读可写）
class MemTableImpl {
private:
    std::map<string, std::unique_ptr<MVCCNode>, NaturalLess> data;
    mutable std::shared_mutex mutex;
    size_t total_size;
    size_t total_versions;
    
public:
    MemTableImpl() : total_size(0), total_versions(0) {}
    
    void put(const string& key, const string& value, Version version) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        auto it = data.find(key);
        if (it == data.end()) {
            std::unique_ptr<MVCCNode> node = std::make_unique<MVCCNode>(key);
            node->add_version(version, value);
            data[key] = std::move(node);
        } else {
            it->second->add_version(version, value);
        }
        total_size += key.size() + value.size();
        total_versions++;
    }
    
    void del(const string& key, Version version) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        auto it = data.find(key);
        if (it == data.end()) {
            auto node = std::make_unique<MVCCNode>(key);
            node->add_version(version, "", true);
            data[key] = std::move(node);
        } else {
            it->second->add_version(version, "", true);
        }
        total_size += key.size();
        total_versions++;
    }
    
    bool get(const string& key, string& value, Version snap_ver) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        auto it = data.find(key);
        if (it == data.end()) {
            return false;
        }
        return it->second->get_value(snap_ver, value);
    }
    
    // GC: 清理旧版本
    struct GCStats {
        size_t keys_processed = 0;
        size_t versions_removed = 0;
        size_t keys_removed = 0;
        size_t bytes_freed = 0;
    };

    GCStats garbage_collect(Version min_keep_version) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        GCStats stats;
        std::vector<string> keys_to_remove;
        
        for (auto& [key, node] : data) {
            stats.keys_processed++;
            
            size_t before = node->version_count();
            size_t removed = node->cleanup_old_versions(min_keep_version);
            
            if (removed > 0) {
                stats.versions_removed += removed;
                total_versions -= removed;
                
                // 估算释放的字节数
                for (const auto& [ver, vv] : node->versions) {
                    if (ver < min_keep_version) {
                        stats.bytes_freed += key.size() + vv.value.size();
                    }
                }
            }
            
            // 如果没有版本了，删除整个节点
            if (node->version_count() == 0) {
                keys_to_remove.push_back(key);
            }
        }
        
        // 删除空节点
        for (const auto& key : keys_to_remove) {
            auto it = data.find(key);
            if (it != data.end()) {
                stats.bytes_freed += key.size();
                data.erase(it);
                stats.keys_removed++;
            }
        }
        
        // 重新计算总大小
        total_size = 0;
        for (const auto& [key, node] : data) {
            total_size += key.size();
            for (const auto& [ver, vv] : node->versions) {
                total_size += vv.value.size();
            }
        }
        
        return stats;
    }

    // 获取所有数据（用于 flush）
    std::map<string, std::map<Version, VersionedValue>, NaturalLess> get_all_data() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        std::map<string, std::map<Version, VersionedValue>, NaturalLess> result;
        for (const auto& [key, node] : data) {
            for (const auto& [ver, vv] : node->versions) {
                result[key][ver] = vv;
            }
        }
        return result;
    }
    
    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return data.size();
    }
    
    size_t total_bytes() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return total_size;
    }

    size_t total_versions_count() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return total_versions;
    }
    
    bool empty() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return data.empty();
    }
    
    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        data.clear();
        total_size = 0;
        total_versions = 0;
    }

    // 获取范围内的 key
    std::vector<string> get_keys_in_range(const string& start_key, 
                                           const string& end_key) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        std::vector<string> keys;
        auto it = data.lower_bound(start_key);
        while (it != data.end() && !NaturalLess()(end_key, it->first)) { // end >= it->first
            keys.push_back(it->first);
            ++it;
        }
        return keys;
    }

    // 获取所有键
    std::vector<string> get_all_keys() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        std::vector<string> keys;
        for (const auto& [key, _] : data) {
            keys.push_back(key);
        }
        return keys;
    }

    // 范围迭代器
    class MemTableIterator {
    private:
        std::map<string, std::unique_ptr<MVCCNode>, NaturalLess>::const_iterator it;
        std::map<string, std::unique_ptr<MVCCNode>, NaturalLess>::const_iterator end;
        
    public:
        MemTableIterator(decltype(it) i, decltype(end) e) : it(i), end(e) {}
        
        bool valid() const { return it != end; }
        void next() { ++it; }
        string key() const { return it->first; }
        const MVCCNode* node() const { return it->second.get(); }
    };
    
    MemTableIterator get_range_iterator(const string& start_key, const string& end_key) const {
        std::shared_lock<std::shared_mutex> lock(mutex);

        // std::map<string, std::unique_ptr<MVCCNode>, NaturalLess> data;
        auto start_it = data.lower_bound(start_key);
        auto end_it = data.upper_bound(end_key);
        return MemTableIterator(start_it, end_it);
    }
};

// 快照
// Snapshot 类表示一个快照，包含一个版本号。 MVCCKVStore 中会维护一个活跃快照列表，定期清理过期快照，并在垃圾回收时根据活跃快照的版本来决定哪些旧版本可以被删除。
class Snapshot {
private:
    Version version;
    
public:
    Snapshot(Version ver) : version(ver) {}
    
    Version get_version() const { return version; }
    
    bool operator<(const Snapshot& other) const {
        return version < other.version;
    }
};

// MVCC MemTable（管理 active 和 immutable）
class MVCCMemTable {
private:
    std::unique_ptr<MemTableImpl> active_memtable;
    std::unique_ptr<MemTableImpl> immutable_memtable;
    mutable std::shared_mutex mutex;
    std::atomic<Version> current_version;
    std::atomic<bool> has_immutable;
    
public:
    MVCCMemTable() : current_version(0), has_immutable(false) {
        active_memtable = std::make_unique<MemTableImpl>();
        immutable_memtable = nullptr;
    }
    
    // 写入（只写 active）
    Version put(const string& key, const string& value) {
        Version new_version = ++current_version;
        active_memtable->put(key, value, new_version);
        return new_version;
    }
    
    Version del(const string& key) {
        Version new_version = ++current_version;
        active_memtable->del(key, new_version);
        return new_version;
    }
    
    // 读取（先读 active，再读 immutable）
    bool get(const string& key, string& value, Version snap_ver) const {
        // 先读 active
        if (active_memtable->get(key, value, snap_ver)) {
            return true;
        }
        
        // 再读 immutable
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (immutable_memtable && immutable_memtable->get(key, value, snap_ver)) {
            return true;
        }
        
        return false;
    }
    
    // 切换 MemTable：将 active 变为 immutable，创建新的 active
    std::unique_ptr<MemTableImpl> switch_memtable() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        // 将当前的 active 变为 immutable
        immutable_memtable = std::move(active_memtable);
        has_immutable = true;
        
        // 创建新的 active
        active_memtable = std::make_unique<MemTableImpl>();
        
        // 返回 immutable 的指针（用于 flush，但所有权仍保留）
        // 注意：这里返回的是原始指针，不能转移所有权
        return std::move(immutable_memtable);
    }
    
    // 完成 flush，释放 immutable
    void finish_flush() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        immutable_memtable.reset();
        has_immutable = false;
    }
    
    // 获取当前活跃 MemTable 的大小
    size_t size() const {
        return active_memtable->size();
    }
    
    size_t total_bytes() const { return active_memtable->total_bytes(); }
    size_t total_versions() const { return active_memtable->total_versions_count(); }
    
    // 检查是否需要切换
    bool need_switch(size_t threshold) const {
        return active_memtable->total_bytes() >= threshold;
    }
    
    // 是否有 immutable
    bool get_has_immutable() const {
        return has_immutable.load();
    }
    
    // 获取当前版本号
    Version get_current_version() const {
        return current_version.load();
    }
    
    // 创建快照
    std::shared_ptr<Snapshot> create_snapshot() {
        return std::make_shared<Snapshot>(current_version.load());
    }
    
    // GC: 清理 active 和 immutable 中的旧版本
    struct GCStats {
        size_t active_keys_processed = 0;
        size_t active_versions_removed = 0;
        size_t active_keys_removed = 0;
        size_t active_bytes_freed = 0;
        size_t immutable_keys_processed = 0;
        size_t immutable_versions_removed = 0;
        size_t immutable_keys_removed = 0;
        size_t immutable_bytes_freed = 0;
    };

    GCStats garbage_collect(Version min_keep_version) {
        GCStats stats;
        
        // 清理 active memtable
        auto active_stats = active_memtable->garbage_collect(min_keep_version);
        stats.active_keys_processed = active_stats.keys_processed;
        stats.active_versions_removed = active_stats.versions_removed;
        stats.active_keys_removed = active_stats.keys_removed;
        stats.active_bytes_freed = active_stats.bytes_freed;
        
        // 清理 immutable memtable
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (immutable_memtable) {
            auto immutable_stats = immutable_memtable->garbage_collect(min_keep_version);
            stats.immutable_keys_processed = immutable_stats.keys_processed;
            stats.immutable_versions_removed = immutable_stats.versions_removed;
            stats.immutable_keys_removed = immutable_stats.keys_removed;
            stats.immutable_bytes_freed = immutable_stats.bytes_freed;
        }
        
        return stats;
    }

    // 获取统计信息
    void get_stats(size_t& active_keys, size_t& active_versions, size_t& active_bytes, size_t& immutable_keys, size_t& immutable_versions, size_t& immutable_bytes) const {
        active_keys = active_memtable->size();
        active_versions = active_memtable->total_versions_count();
        active_bytes = active_memtable->total_bytes();
        
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (immutable_memtable) {
            immutable_keys = immutable_memtable->size();
            immutable_versions = immutable_memtable->total_versions_count();
            immutable_bytes = immutable_memtable->total_bytes();
        } else {
            immutable_keys = 0;
            immutable_versions = 0;
            immutable_bytes = 0;
        }
    }
    
    // 获取 immutable 的数据用于 flush
    std::map<string, std::map<Version, VersionedValue>, NaturalLess> get_immutable_data() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (immutable_memtable) {
            return immutable_memtable->get_all_data();
        }
        return {};
    }

    std::map<string, std::map<Version, VersionedValue>, NaturalLess> get_active_data() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (active_memtable) {
            return active_memtable->get_all_data();
        }
        return {};
    }

    std::map<string, std::map<Version, VersionedValue>, NaturalLess> get_all_data() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        std::map<string, std::map<Version, VersionedValue>, NaturalLess> result;
        
        // 1. 从 active_memtable 收集数据
        auto active_data = get_active_data();
        for (const auto& [key, versions] : active_data) {
            for (const auto& [ver, vv] : versions) {
                result[key][ver] = vv;
            }
        }
        
        // 2. 从 immutable_memtable 收集数据（如果存在）
        auto immutable_data = get_immutable_data();
        for (const auto& [key, versions] : immutable_data) {
            for (const auto& [ver, vv] : versions) {
                result[key][ver] = vv;
            }
        }
        
        return result;
    }
    
    // 清空所有（用于测试）
    void clear_all() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        active_memtable->clear();
        immutable_memtable.reset();
        has_immutable = false;
    }
};

} // namespace kvstore