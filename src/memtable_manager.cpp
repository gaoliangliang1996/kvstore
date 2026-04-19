#include "memtable_manager.h"
#include "chrono"
#include <iostream>

namespace kvstore {

MemTableManager::MemTableManager(size_t max_mem_size) : max_size(max_mem_size), is_flushing(false) {
    active_memtable = std::make_unique<SkipList<string, string>>();
    immutable_memtable = nullptr;
    active_size = 0;
}

MemTableManager::~MemTableManager() {
    wait_flush();
}


// 写入操作
void MemTableManager::put(const string& key, const string& value) {
    {
        std::lock_guard<std::mutex> lock(mutex);

        active_memtable->put(key, value);
        active_size += key.size() + value.size();
    }

    // 检查是否需要切换
    if (need_switch()) {
        switch_memtable();
    }    
}

bool MemTableManager::get(const string& key, string& value) {
    // 先查 active 
    if (active_memtable->get(key, value))
        return true;
    
    // 再查 immutable
    if (immutable_memtable && immutable_memtable->get(key, value))
        return true;

    return false;
}

void MemTableManager::del(const string& key) {
    std::lock_guard<std::mutex> lock(mutex);

    active_memtable->put(key, "__DELETED__");
    active_size += key.size();

    if (need_switch())
        switch_memtable();
}


// 切换 MemTable
void MemTableManager::switch_memtable() {
    // 如果已经有 immutable，等待 flush 完成
    if (immutable_memtable) {
        wait_flush();
    }

    {
        std::lock_guard<std::mutex> lock(mutex);
        // 切换 active -> immutable , 创建新的 active
        immutable_memtable = std::move(active_memtable); // std::move 的作用是将 active_memtable 的所有权转移到 immutable_memtable 中，active_memtable 变为 nullptr。
        active_memtable = std::make_unique<SkipList<string, string>>();
        active_size = 0;
    }

    // 触发后台 flush 
    if (flush_callback && immutable_memtable) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            is_flushing = true;
        }
        std::thread([this]() {
            // std::cout << "flush memtable to sstable..." << std::endl;
            flush_callback(immutable_memtable.get()); // ->  KVStore::flushMemtable()

            { // flush 完成后，用 mutex 清理
                std::lock_guard<std::mutex> lock(mutex);
                immutable_memtable.reset();
                is_flushing = false;
            }
        }).detach();
    }
}

// 设置 flush 回调
void MemTableManager::set_flush_callback(std::function<void(SkipList<string, string>*)> callback) {
    flush_callback = callback;
}

// 等待 flush 完成
void MemTableManager::wait_flush() {
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex);

            if (!is_flushing && !immutable_memtable)
                return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}


} // namespace kvstore