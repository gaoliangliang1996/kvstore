#pragma once
#include "skiplist.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <functional>
#include <string>
#include <iostream>

namespace kvstore {

using std::string;

class MemTableManager {
private:
    // 双 MemTable
    std::unique_ptr<SkipList<std::string, std::string>> active_memtable;
    std::unique_ptr<SkipList<std::string, std::string>> immutable_memtable;

    // 状态管理
    std::mutex mutex;
    
    std::atomic<size_t> active_size; // 当前 active MemTable 的大小
    std::atomic<bool> is_flushing;   // 是否正在 flush 中

    // 配置
    size_t max_size; // MemTable 切换阈值

    // 回调函数
    std::function<void(SkipList<std::string, std::string>*)> flush_callback;
public:
    MemTableManager(size_t max_mem_size);
    ~MemTableManager();

    // 写入操作
    void put(const string& key, const string& value);
    bool get(const string& key, string& value);
    void del(const string& key);

    // 切换 MemTable
    void switch_memtable();

    // 检查是否需要切换
    bool need_switch() const { 
        // std::cout << "active_size: " << active_size << std::endl;
        return active_size >= max_size; 
    }

    // 获取当前大小
    size_t size() const { return active_size; }

    // 设置 flush 回调
    void set_flush_callback(std::function<void(SkipList<string, string>*)> callback);

    // 检查是否正在 flush 
    bool flushing() const { return is_flushing; }

    // 等待 flush 完成
    void wait_flush();

    // 统计信息
    size_t get_active_count() const { return active_memtable->size(); }
    size_t get_immutable_count() const {
        return immutable_memtable ? immutable_memtable->size() : 0;
    }
};

}