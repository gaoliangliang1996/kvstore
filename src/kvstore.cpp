#include "kvstore.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <iostream>

namespace kvstore {
    
KVStore::KVStore(const Config& cfg) : config(cfg), running(true) {
    // 创建数据目录
    std::filesystem::create_directories(config.data_dir);

    // 初始化日志
    
    // 初始化 MemTable Manager
    memtable_manager = std::make_unique<MemTableManager>(config.memtable_size);
    memtable_manager->set_flush_callback(
        [this](SkipList<string, string>* memtable) {
            this->flushMemtable(memtable);
        }
    );

    // 初始化 wal
    wal = std::make_unique<WAL>(config.wal_file);

    // 初始化 Compaction
    compaction = std::make_unique<Compaction>(config.data_dir);

    // 从 WAL 恢复数据
    wal->recover([this](const Record& rec) -> bool {
        switch (rec.type) {
            case Optype::PUT:
                memtable_manager->put(rec.key, rec.value);
                break;
            case Optype::DELETE:
                memtable_manager->del(rec.key);
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

    // 初始化 Block Cache
    cache = std::make_unique<LRUCache>(100 * 1024 * 1024);

    // 初始化 Bloom Filter (为每个 SSTable 添加)
    for (auto& sst : sstables) {
        auto bloom = std::make_unique<BloomFilter>(sst->size() * 10);
        // 这里需要为 SSTable 构建布隆过滤器
        for (auto it = sst->begin(); it.valid(); it.next()) {
            bloom->add(it.key());
        }
        // 需要将 bloom filter 与 SSTable 关联
    }


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
    memtable_manager->put(key, value);

    return Status::OK;
}

Status KVStore::get(const string& key, string& value) {
    // 0. 先查 Block Cache
    if (cache->get(key, value)) {
        return Status::OK;
    }

    // 1. 先查 MemTable
    if (memtable_manager->get(key, value)) {
        if (value == "__DELETED__") 
            return Status::NOT_FOUNT;

        // 回写缓存
        cache->put(key, value);
        return Status::OK;
    }

    // 2. 查 SSTable
    if (getFromSSTables(key, value)) {
        if (value == "__DELETED__") 
            return Status::NOT_FOUNT;

        cache->put(key, value);
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
    memtable_manager->put(key, "__DELETED__");

    // 3. 从缓存中删除
    cache->del(key);

    return Status::OK;
}

void KVStore::flushMemtable(SkipList<string, string>* memtable) {
    // 1. 收集当前 MemTable 的所有数据
    std::map<string, string> data;
    for (auto it = memtable->begin(); it.valid(); it.next()) {
        data[it.key()] = it.value();
    }

    if (data.empty())
        return;

    // 2. 生成新的 SSTable 文件名
    static int sst_id = 0;
    string sst_path = config.data_dir + "/" + std::to_string(sst_id++) + ".sst";

    // 3. 创建 SSTable
    std::shared_ptr<SSTable> sst = std::shared_ptr<SSTable>(SSTable::createFromMemTable(sst_path, data));

    // 构建 bloom filter
    auto bloom = std::make_unique<BloomFilter>(sst->size() * 10);
    for (auto it = sst->begin(); it.valid(); it.next()) {
        bloom->add(it.key());
    }
    // 将 bloom filter 与 SSTable 关联
    // sst->set_bloom_filter(std::move(bloom));

    // 添加到内存中的 SSTable 列表
    sstables.push_back(sst);

    // 添加到 Compaction 管理器（转移所有权）
    compaction->add_sstable(0, sst);


    // 4. 清空 MemTable

    // 5. 截断 WAL （已刷入 SSTable 的数据可以从 WAL 删除）
    wal->truncate();
}

void KVStore::backgroundFlush() {
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(5));

    }
}

bool KVStore::getFromSSTables(const string& key, string& value) {
    // 从最新的 SSTable 开始查找
    for (int level = 0; level <= 6; level++) {
        std::vector<SSTable*> files = compaction->get_level_files(level);
        // 从最新的 SSTable 开始查找
        for (auto it = files.rbegin(); it != files.rend(); ++it) {
            if ((*it)->get(key, value))
                return true;
        }
    }

    return false;
}

void KVStore::sync() {
    wal->sync();
}

KVStore::Stats KVStore::get_stats() {
    Stats stats;
    auto cache_stats = cache->get_stats();
    stats.cache_hits = cache_stats.hits;
    stats.cache_misses = cache_stats.misses;
    return stats;
}

}