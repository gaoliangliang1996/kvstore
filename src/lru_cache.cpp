#include "lru_cache.h"
#include <algorithm>

namespace kvstore {

LRUCache::LRUCache(size_t max_cache_size) 
    : max_size(max_cache_size), current_size(0) {}

void LRUCache::evict() {
    while (current_size > max_size && !lru_list.empty()) { // 当当前缓存大小超过最大容量时，淘汰最久未使用的项（链表尾部）
        Node& last = lru_list.back();
        cache_map.erase(last.key);
        current_size -= last.size;
        lru_list.pop_back();
    }
}

void LRUCache::put(const string& key, const string& value) {
    std::lock_guard<std::mutex> lock(mutex);
    
    size_t new_size = key.size() + value.size();
    
    // 如果单个 key 太大，不缓存
    if (new_size > max_size) {
        return;
    }
    
    auto it = cache_map.find(key);
    if (it != cache_map.end()) { // 缓存中已存在，更新值并移动到头部
        // 更新现有节点
        current_size -= it->second->size;
        it->second->value = value;
        it->second->size = new_size;
        current_size += new_size;
        
        // 移到头部
        lru_list.splice(lru_list.begin(), lru_list, it->second); // splice 将 it->second 指向的节点移动到 lru_list 的开头
    } else {
        // 插入新节点
        lru_list.push_front({key, value, new_size});
        cache_map[key] = lru_list.begin();
        current_size += new_size;
    }
    
    // 淘汰多余节点
    evict();
}

bool LRUCache::get(const string& key, string& value) {
    std::lock_guard<std::mutex> lock(mutex);
    
    auto it = cache_map.find(key);
    if (it == cache_map.end()) {
        stats.misses++;
        return false;
    }
    
    // 移到头部
    lru_list.splice(lru_list.begin(), lru_list, it->second); // splice 将 it->second 指向的节点移动到 lru_list 的开头
    value = it->second->value;
    stats.hits++;
    return true;
}

void LRUCache::del(const string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    
    auto it = cache_map.find(key);
    if (it != cache_map.end()) {
        current_size -= it->second->size;
        lru_list.erase(it->second);
        cache_map.erase(it);
    }
}

bool LRUCache::exists(const string& key) {
    std::lock_guard<std::mutex> lock(mutex);
    return cache_map.find(key) != cache_map.end();
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(mutex);
    lru_list.clear();
    cache_map.clear();
    current_size = 0;
}

} // namespace kvstore