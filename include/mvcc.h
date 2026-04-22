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
    // 使用 std::map，默认升序，但我们需要降序访问，所以存储时用升序，访问时反向迭代
    std::map<Version, VersionedValue> versions; // version -> VersionedValue
    
    MVCCNode(const string& k) : key(k) {}
    
    void add_version(Version ver, const string& val, bool deleted = false) {
        versions[ver] = VersionedValue(val, ver, deleted);
    }
    
    bool get_value(Version snapshot_ver, string& value) const { // snapshot_ver 是读取时指定的快照版本
        // 反向遍历版本，找到第一个版本号小于等于 snapshot_ver 的条目
        for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
            if (it->first <= snapshot_ver) { // 找到第一个版本号小于等于快照版本 snapshot_ver 的条目
                if (!it->second.deleted) {
                    value = it->second.value;
                    return true;
                }
                return false;  // 找到删除标记
            }
        }
        return false;
    }
    
    // 获取最新版本号
    Version get_latest_version() const {
        if (versions.empty()) return 0;
        return versions.rbegin()->first;
    }
    
    // 清理旧版本（GC）, 保留 >= min_keep_version 的版本
    void cleanup_old_versions(Version min_keep_version) { // min_keep_version 是要保留的最小版本号，小于这个版本的都可以删除
        auto it = versions.begin();
        while (it != versions.end()) {
            if (it->first < min_keep_version) {
                it = versions.erase(it);
            } else {
                ++it;
            }
        }
    }

    size_t size() const { return versions.size(); }
    bool empty() const { return versions.empty(); }
};

// MemTable 实现类（可读可写）
class MemTableImpl {
private:
    std::map<string, std::unique_ptr<MVCCNode>, NaturalLess> data;
    mutable std::shared_mutex mutex;
    size_t total_size;
    
public:
    MemTableImpl() : total_size(0) {}
    
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
    }
    
    bool get(const string& key, string& value, Version snap_ver) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        
        auto it = data.find(key);
        if (it == data.end()) {
            return false;
        }
        return it->second->get_value(snap_ver, value);
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
    
    bool empty() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return data.empty();
    }
    
    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        data.clear();
        total_size = 0;
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
    
    size_t total_bytes() const {
        return active_memtable->total_bytes();
    }
    
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
    
    // 垃圾回收（清理旧版本）
    void garbage_collect(Version min_keep_version) {
        // 需要在 active 和 immutable 中都清理
        active_memtable->get_all_data();  // 触发清理需要遍历
        if (immutable_memtable) {
            // immutable 的清理
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