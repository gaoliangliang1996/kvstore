#include "kvstore.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <iostream>

namespace kvstore {
    
KVStore::KVStore(const Config& cfg) : config(cfg), memtable_size(0), running(true) {
    // 创建数据目录
    std::filesystem::create_directories(config.data_dir);

    // 初始化日志
    
    // 初始化 wal
    wal = std::make_unique<WAL>(config.wal_file);

    // 从 WAL 恢复数据
    wal->recover([this](const Record& rec) -> bool {
        switch (rec.type) {
            case Optype::PUT:
                memtable.put(rec.key, rec.value);
                memtable_size += rec.key.size() + rec.value.size();
                break;
            case Optype::DELETE:
                memtable.del(rec.key);
                break;
        }
        return true;
    });

    // 加载已有的 SSTable
    for (const auto& entry : std::filesystem::directory_iterator(config.data_dir)) {
        string path = entry.path().string();
        if (path.find(".sst") != string::npos) {
            sstables.push_back(std::make_unique<SSTable>(path));
        }
    }

    // 按创建时间排序，新的在前
    std::sort(sstables.begin(), sstables.end(), [](const auto& a, const auto& b) {
        return a->size() > b->size();
    });

    // 启动后台刷写线程
    flush_thread = std::thread(&KVStore::backgroundFlush, this);
}

KVStore::~KVStore() {
    running = false;

    if (flush_thread.joinable())
        flush_thread.join();

    sync();
}

Status KVStore::put(const string& key, const string& value) {
    // 1. 写 WAL
    Record rec{Optype::PUT, key, value};
    if (!wal->append(rec)) {
        std::cout << "error";
        return Status::IO_ERROR;
    }

    // 2. 写 MemTable
    memtable.put(key, value);
    memtable_size += key.size() + value.size();

    // 3. 如果 MemTable 满了，触发 flush
    if (memtable_size >= config.memtable_size) {
        flushMemtable();
    }

    return Status::OK;
}

Status KVStore::get(const string& key, string& value) {
    // 1. 先查 MemTable
    if (memtable.get(key, value)) {
        if (value == "__DELETED__") 
            return Status::NOT_FOUNT;

        return Status::OK;
    }

    // 2. 查 SSTable
    if (getFromSSTables(key, value)) {
        if (value == "__DELETED__") 
            return Status::NOT_FOUNT;

        return Status::OK;
    }

    return Status::OK;
}

Status KVStore::del(const string& key) {
    // 1. 写删除标记到 WAL
    Record rec{Optype::DELETE, key, ""};
    if (!wal->append(rec)) {
        return Status::IO_ERROR;
    }

    // 2. 写删除标记到 MemTable
    memtable.put(key, "__DELETED__");
    memtable_size += key.size();

    return Status::OK;
}

void KVStore::flushMemtable() {
    // 1. 收集当前 MemTable 的所有数据
    std::map<string, string> data;
    for (auto it = memtable.begin(); it.valid(); it.next()) {
        data[it.key()] = it.value();
    }

    if (data.empty())
        return;

    // 2. 生成新的 SSTable 文件名
    static int sst_id = 0;
    string sst_path = config.data_dir + "/" + std::to_string(sst_id++) + ".sst";

    // 3. 创建 SSTable
    auto sst = SSTable::createFromMemTable(sst_path, data);
    sstables.insert(sstables.begin(), std::unique_ptr<SSTable>(sst));

    // 4. 清空 MemTable

    // 5. 截断 WAL （已刷入 SSTable 的数据可以从 WAL 删除）
    wal->truncate();
    memtable_size = 0;
}

void KVStore::backgroundFlush() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (memtable_size >= config.memtable_size / 2) {
            flushMemtable();
        }
    }
}

bool KVStore::getFromSSTables(const string& key, string& value) {
    // 从最新的 SSTable 开始查找
    for (auto& sst : sstables) {
        if (sst->get(key, value))
            return true;
    }

    return false;
}

void KVStore::sync() {
    wal->sync();
}


}