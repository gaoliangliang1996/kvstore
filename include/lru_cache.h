#pragma once
#include "common.h"
#include <list>
#include <unordered_map>
#include <mutex>
#include <memory>

namespace kvstore {

/*
LRUCache 是一个基于链表和哈希表实现的简单 LRU（Least Recently Used）缓存。它支持插入、获取、删除和检查缓存项，并且在达到最大容量时会自动淘汰最久未使用的项。LRUCache 还提供了统计信息功能，可以跟踪缓存的命中率。
- put(key, value)：插入或更新缓存项。如果项已经存在，则更新其值并将其移动到链表头部；如果项不存在，则插入新项并将其移动到链表头部。插入新项后，如果当前缓存大小超过最大容量，则调用 evict() 方法淘汰最久未使用的项。
- get(key, value)：获取缓存项。如果项存在，则将其移动到链表头部并返回 true；如果项不存在，则返回 false。
- del(key)：删除缓存项。如果项存在，则将其从链表和哈希表中删除，并更新当前缓存大小。
- exists(key)：检查缓存项是否存在。
- clear()：清空缓存，删除所有项并重置当前缓存大小。
- get_stats()：获取缓存的统计信息，包括命中次数、未命中次数和命中率。
- reset_stats()：重置缓存的统计信息。
*/

class LRUCache {
private:
    struct Node {
        string key;
        string value;
        size_t size;
    };

    // lru_list 和 cache_map 共同维护缓存项的存储和访问。lru_list 是一个双向链表，存储缓存项，最近使用的项在前面，最久未使用的项在后面；cache_map 是一个哈希表，存储 key - 链表迭代器 的映射，用于快速访问链表中的节点。
    // 为什么还要有 cache_map？因为链表只能通过迭代器访问节点，而哈希表可以通过 key 快速定位到对应的链表节点，从而实现 O(1) 的访问时间。
    std::list<Node> lru_list; // 双向链表，存储缓存项，最近使用的项在前面，最久未使用的项在后面
    std::unordered_map<string, std::list<Node>::iterator> cache_map; // 哈希表，存储 key - 链表迭代器 的映射
    mutable std::mutex mutex;

    size_t max_size;        // 最大缓存大小
    size_t current_size;    // 当前缓存大小

    void evict();
public:
    LRUCache(size_t max_cache_size = 100 * 1024 * 1024); // 默认 100 MB
    ~LRUCache() = default;

    // 插入/更新 缓存
    void put(const string& key, const string& value);

    // 获取缓存
    bool get(const string& key, string& value);

    // 删除缓存
    void del(const string& key);

    // 检查是否存在
    bool exists(const string& key);

    // 清空缓存
    void clear();

    // 统计信息
    size_t size() const { return current_size; }
    size_t count() const { return cache_map.size(); }
    size_t maxSize() const { return max_size; }

    // 命中率统计
    struct Stats {
        size_t hits = 0;
        size_t misses = 0;
        double hit_rate() const {
            return (hits + misses) == 0 ? 0 : 100.0 * hits / (hits + misses);
        }
    };

    Stats get_stats() const {
        std::lock_guard<std::mutex> lock(mutex);
        return stats;
    }

    void reset_stats() {
        std::lock_guard<std::mutex> lock(mutex);
        stats = Stats();
    }
private:
    Stats stats;
};

} // namespace kvstore