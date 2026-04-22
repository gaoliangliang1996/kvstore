// src/mvcc_kvstore.cpp
#include "mvcc_kvstore.h"
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <thread>
#include <iostream>

namespace kvstore {

MVCCKVStore::MVCCKVStore(const Config& cfg) 
    : config(cfg), running(true), is_flushing(false) {
    
    std::filesystem::create_directories(config.data_dir);
    
    // 初始化日志
    string log_path = config.data_dir + "/mvcc.log";
    g_logger = new Logger(log_path);
    logger.reset(g_logger);
    
    LOG_INFO("MVCC KVStore starting...");
    
    // 初始化 MemTable
    memtable = std::make_unique<MVCCMemTable>();
    
    // 初始化 WAL
    wal = std::make_unique<WAL>(config.wal_file);
    
    // 从 WAL 恢复数据
    LOG_INFO("Recovering from WAL...");
    wal->recover([this](const Record& rec) -> bool {
        switch (rec.type) {
            case Optype::PUT:
                memtable->put(rec.key, rec.value);
                break;
            case Optype::DELETE: 
                memtable->del(rec.key);
                break;
        }
        return true;
    });
    
    // 加载已有的 SSTable
    LOG_INFO("Loading SSTables...");
    for (const auto& entry : std::filesystem::directory_iterator(config.data_dir)) {
        string path = entry.path().string();
        if (path.find(".sst") != string::npos) {
            sstables.push_back(std::make_shared<MVCCSSTable>(path));
        }
    }
    
    // 按版本范围排序
    std::sort(sstables.begin(), sstables.end(),
              [](const auto& a, const auto& b) {
                  return a->get_max_version() > b->get_max_version();
              });
    
    LOG_INFO("MVCC KVStore started successfully");
}

MVCCKVStore::~MVCCKVStore() {
    running = false;
    
    while (is_flushing) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    sync();
    LOG_INFO("MVCC KVStore shutdown");
    delete g_logger;
}

Version MVCCKVStore::put(const string& key, const string& value) {
    LOG_DEBUG("Put: " + key + " = " + value);
    
    // 1. 写 WAL
    Record rec{Optype::PUT, key, value};
    if (!wal->append(rec)) {
        LOG_ERROR("Failed to append to WAL");
        return 0;
    }
    
    // 2. 写 MemTable（返回版本号）
    Version ver = memtable->put(key, value);
    
    // 3. 检查是否需要切换 MemTable
    if (memtable->need_switch(config.memtable_size) && !is_flushing) {
        flush_async();
    }
    
    return ver;
}

Version MVCCKVStore::del(const string& key) {
    LOG_DEBUG("Delete: " + key);
    
    Record rec{Optype::DELETE, key, ""};
    if (!wal->append(rec)) {
        LOG_ERROR("Failed to append to WAL");
        return 0;
    }
    
    Version ver = memtable->del(key);
    
    if (memtable->need_switch(config.memtable_size) && !is_flushing) {
        flush_async();
    }
    
    return ver;
}

bool MVCCKVStore::get(const string& key, string& value, Version snap_ver) {
    LOG_DEBUG("Get: " + key + " snap_ver=" + std::to_string(snap_ver));
    
    if (snap_ver == 0) {
        snap_ver = memtable->get_current_version();
    }
    
    // 1. 先查 MemTable（包括 active 和 immutable）
    if (memtable->get(key, value, snap_ver)) {
        return true;
    }
    
    // 2. 查 SSTable
    if (getFromSSTables(key, value, snap_ver)) {
        return true;
    }
    
    return false;
}

void MVCCKVStore::collect_from_memtable(const string& start_key, const string& end_key, Version snap_ver, std::map<string, std::pair<string, Version>>& merged) {
    if (!memtable)
        return;

    // std::map<string, std::map<Version, VersionedValue>, NaturalLess>
    auto all_data = memtable->get_all_data();

    for (const auto& [key, version] : all_data) {
        if (is_key_in_range(key, start_key, end_key)) {
            string value;

            if (memtable->get(key, value, snap_ver))
                if (merged.find(key) == merged.end())
                    merged[key] = {value, snap_ver};
        }
    }
}

void MVCCKVStore::collect_from_sstables(const string& start_key, const string& end_key, std::map<string, std::pair<string, Version>>& merged) {
    for (const auto& sst : sstables) {
        // sst->get_max_key() < start_key                || sst->get_min_key() > end_key
        if (NaturalLess()(sst->get_max_key(), start_key) || NaturalLess()(end_key, sst->get_min_key())) {
            continue;  // 这个 SSTable 完全没有范围内的键
        }

        // 遍历 SSTable 中的键
        for (auto it = sst->begin(); it.valid(); it.next()) {
            string key = it.key();
            
            if (is_key_in_range(key, start_key, end_key)) {
                // 只处理未见过的键（MemTable 优先级更高）
                if (merged.find(key) == merged.end()) {
                    string value;
                    if (sst->get(key, value)) {
                        merged[key] = {value, 0};
                    }
                }
            }
        }
    }
}

// NaturalLess()(key, start) 表示 key < start
bool MVCCKVStore::is_key_in_range(const string& key, const string& start, const string& end) const {
    //     key >= start               && end >= key
    return !NaturalLess()(key, start) && !NaturalLess()(end, key); // start <= key <= end, 使用 NaturalLess 进行自然排序比较
}

RangeIterator MVCCKVStore::range_scan(const string& start_key, const string& end_key, Version snap_ver) {
    // 1. 确定快照版本
    if (snap_ver == 0) {
        snap_ver = memtable->get_current_version();
    }
    
    // 2. 收集结果（使用 map 自动按键排序）
    std::map<string, std::pair<string, Version>> merged;
    
    // 3. 从 MemTable 收集（优先级最高）
    collect_from_memtable(start_key, end_key, snap_ver, merged);
    
    // 4. 从 SSTable 收集
    collect_from_sstables(start_key, end_key, merged);
    
    // 5. 构建迭代器
    RangeIterator iter;
    for (const auto& [key, kv] : merged) {
        iter.add_result(key, kv.first, kv.second);
    }
    iter.sort_results();
    
    return iter;
}

RangeIterator MVCCKVStore::prefix_scan(const string& prefix, Version snap_ver) {
    string start_key = prefix;
    string end_key = prefix;
    if (!end_key.empty()) {
        end_key.back()++;
    }

    return range_scan(start_key, end_key, snap_ver);
}

MVCCKVStore::PageResult MVCCKVStore::paginated_scan(const string& start_key, const string& end_key, size_t page_size, const string& page_token, Version snap_ver) {
    PageResult result;
    
    string actual_start = page_token.empty() ? start_key : page_token;
    auto iter = range_scan(actual_start, end_key, snap_ver);
    
    size_t count = 0;
    while (iter.valid() && count < page_size) {
        result.data.emplace_back(iter.key(), iter.value());
        count++;
        iter.next();
    }
    
    if (iter.valid()) {
        result.has_more = true;
        result.next_token = iter.key();
    }
    
    return result;
}

std::vector<string> MVCCKVStore::get_all_keys(Version snap_ver) {
    if (snap_ver == 0) {
        snap_ver = memtable->get_current_version();
    }
    
    std::vector<string> keys;
    std::map<string, std::pair<string, Version>> merged;
    
    collect_from_memtable("", "\xFF", snap_ver, merged); // end_key = "\xFF"
    collect_from_sstables("", "\xFF", merged);
    
    for (const auto& [key, _] : merged) {
        keys.push_back(key);
    }
    
    return keys;
}

std::shared_ptr<Snapshot> MVCCKVStore::create_snapshot() {
    std::shared_ptr<Snapshot> snapshot = memtable->create_snapshot();
    
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        active_snapshots.push_back(snapshot);
    }
    
    LOG_INFO("Created snapshot with version: " + std::to_string(snapshot->get_version()));
    return snapshot;
}

void MVCCKVStore::release_snapshot(std::shared_ptr<Snapshot> snapshot) {
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    
    // 从 active_snapshots 中移除对应的快照
    auto it = std::find_if(active_snapshots.begin(), active_snapshots.end(),
                           [&snapshot](const auto& weak) {
                               auto sp = weak.lock();
                               return sp && sp->get_version() == snapshot->get_version();
                           });
    
    if (it != active_snapshots.end()) {
        active_snapshots.erase(it);
    }
    
    LOG_INFO("Released snapshot with version: " + std::to_string(snapshot->get_version()));
}

// ============== Flush 实现 ==============

void MVCCKVStore::flush() {
    std::cout << "[Flush] Starting synchronous flush..." << std::endl;
    
    if (is_flushing) {
        std::cout << "[Flush] Already flushing, waiting..." << std::endl;
        while (is_flushing) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return;
    }
    
    flushImmutableMemtable();
    std::cout << "[Flush] Synchronous flush completed" << std::endl;
}

void MVCCKVStore::flush_async() {
    std::cout << "[Flush] Starting asynchronous flush..." << std::endl;
    
    if (is_flushing) {
        std::cout << "[Flush] Already flushing, skipping..." << std::endl;
        return;
    }
    
    std::thread([this]() {
        flushImmutableMemtable();
        std::cout << "[Flush] Asynchronous flush completed" << std::endl;
    }).detach();
}

void MVCCKVStore::flushImmutableMemtable() {
    std::lock_guard<std::mutex> lock(flush_mutex);
    
    if (is_flushing) {
        return;
    }
    
    is_flushing = true;
    
    LOG_INFO("Flushing memtable to SSTable...");
    std::cout << "[FlushMemtable] Starting flush" << std::endl;
    
    // 1. 切换 MemTable（active -> immutable）
    auto immutable = memtable->switch_memtable();
    
    if (!immutable || immutable->empty()) {
        std::cout << "[FlushMemtable] No data to flush" << std::endl;
        memtable->finish_flush();
        is_flushing = false;
        return;
    }
    
    std::cout << "[FlushMemtable] Flushing " << immutable->size() << " keys" << std::endl;
    
    // 2. 获取 immutable 中的数据
    std::map<string, std::map<Version, VersionedValue>, NaturalLess> data = immutable->get_all_data();
    
    if (data.empty()) {
        std::cout << "[FlushMemtable] No data to flush" << std::endl;
        memtable->finish_flush();
        is_flushing = false;
        return;
    }
    
    // 3. 计算版本范围
    Version min_version = UINT64_MAX;
    Version max_version = 0;
    for (const auto& [key, versions] : data) {
        for (const auto& [ver, vv] : versions) {
            min_version = std::min(min_version, ver);
            max_version = std::max(max_version, ver);
        }
    }
    
    if (min_version == UINT64_MAX) {
        min_version = 1;
        max_version = 1;
    }
    
    std::cout << "[FlushMemtable] Version range: " << min_version 
              << " - " << max_version << std::endl;
    
    // 4. 生成 SSTable 文件名
    static int sst_id = 0;
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();
    string sst_path = config.data_dir + "/sst_" + std::to_string(timestamp) 
                      + "_" + std::to_string(sst_id++) + ".sst";
    
    // 5. 创建 SSTable
    MVCCSSTable* raw_sst = MVCCSSTable::createFromVersionedData(
        sst_path, data, min_version, max_version
    );
    std::shared_ptr<MVCCSSTable> sst = std::shared_ptr<MVCCSSTable>(raw_sst);
    sstables.insert(sstables.begin(), sst); // std::vector<std::shared_ptr<MVCCSSTable>> sstables;
    
    std::cout << "[FlushMemtable] Created SSTable: " << sst_path << std::endl;
    
    // 6. 完成 flush，释放 immutable
    memtable->finish_flush();
    
    // 7. 截断 WAL（清空已刷新的日志）
    wal->truncate();
    
    is_flushing = false;
    
    LOG_INFO("Flush completed, created " + sst_path);
    std::cout << "[FlushMemtable] Flush completed, SSTable count: " 
              << sstables.size() << std::endl;
}

bool MVCCKVStore::getFromSSTables(const string& key, string& value, Version snap_ver) {
    for (auto& sst : sstables) {
        // 第一层过滤：版本范围
        if (!sst->may_contain_version(snap_ver)) {
            continue;
        }

        // 第二层过滤：Bloom filter
        if (!sst->mayContain(key)) {
            continue;
        }
        
        // 实际查找
        if (sst->get(key, value)) {
            return true;
        }
    }
    return false;
}

// 获取当前所有快照中最小的版本号（用于垃圾回收时确定安全的版本范围）
Version MVCCKVStore::getMinActiveSnapshotVersion() {
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    
    Version min_version = memtable->get_current_version();
    
    for (auto it = active_snapshots.begin(); it != active_snapshots.end();) {
        auto snapshot = it->lock();
        if (!snapshot) {
            it = active_snapshots.erase(it);
            continue;
        }
        min_version = std::min(min_version, snapshot->get_version());
        ++it;
    }
    
    return min_version;
}

// 清理已过期的快照（没有外部引用了）
void MVCCKVStore::cleanupOldSnapshots() {
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    
    for (auto it = active_snapshots.begin(); it != active_snapshots.end();) {
        if (it->expired()) {
            it = active_snapshots.erase(it);
        } else {
            ++it;
        }
    }
}

void MVCCKVStore::garbage_collect() {
    LOG_INFO("Running garbage collection...");
    
    Version min_keep = getMinActiveSnapshotVersion();
    memtable->garbage_collect(min_keep);
    
    LOG_INFO("Garbage collection completed");
}

void MVCCKVStore::sync() {
    if (wal) {
        wal->sync();
    }
    LOG_INFO("Synced all data");
}

MVCCKVStore::Stats MVCCKVStore::get_stats() const {
    Stats stats;
    stats.active_memtable_size = memtable->size();
    stats.active_memtable_bytes = memtable->total_bytes();
    stats.has_immutable = memtable->get_has_immutable();
    stats.sstable_count = sstables.size();
    stats.current_version = memtable->get_current_version();
    stats.flushing = is_flushing.load();
    
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        stats.active_snapshots = active_snapshots.size();
    }
    
    return stats;
}

// ================= 锁管理实现 ==================

// 尝试获取键的锁，成功返回 true，失败返回 false
bool MVCCKVStore::try_lock_key(const string& key, uint64_t txn_id) {
    // 获取全局map锁，查找或创建KeyLock
    std::unique_lock<std::mutex> lock(lock_manager_mutex);
    
    KeyLock& key_lock = key_locks[key];
    
    // 如果已经被当前事务锁定，直接返回成功
    if (key_lock.locked && key_lock.owner_txn == txn_id) {
        return true; // 同一事务可以多次获取同一锁
    }
    
    // 尝试获取具体key的锁
    std::unique_lock<std::mutex> key_lock_mutex(key_lock.mutex);
    
    if (!key_lock.locked) {
        key_lock.locked = true;
        key_lock.owner_txn = txn_id;
        return true;
    }
    
    return false;
}

// 释放键的锁
void MVCCKVStore::unlock_key(const string& key, uint64_t txn_id) {
    std::unique_lock<std::mutex> lock(lock_manager_mutex);
    
    auto it = key_locks.find(key);
    if (it == key_locks.end()) {
        return;
    }
    
    KeyLock& key_lock = it->second;
    if (key_lock.owner_txn == txn_id) { // 只能自己释放自己的锁
        std::unique_lock<std::mutex> key_lock_mutex(key_lock.mutex);
        key_lock.locked = false;
        key_lock.owner_txn = 0;
        key_lock.cv.notify_all(); // 唤醒等待的事务
    }
}

// 检查键是否在指定版本后被修改
bool MVCCKVStore::is_key_modified_after(const string& key, Version version) {
    // 检查 key 是否在指定版本后被修改
    string value;
    Version current_version = memtable->get_current_version();
    
    // 如果 当前版本 大于 快照版本，说明可能有修改
    if (current_version > version) {
        // 需要检查实际是否修改
        // 这里简化处理：返回 true 表示有修改
        // 实际应该检查 WAL 或版本历史
        return true;
    }
    
    return false;
}

MVCCKVStore::BatchWriteResult MVCCKVStore::BatchWrite(const WriteBatch& batch) {
    BatchWriteResult result;
    result.success = true;

    // 1. 批量写 WAL
    std::vector<Record> wal_records;
    for (const auto& op : batch.GetOps()) {
        Record rec;
        rec.type = (op.type == WriteBatch::OpType::PUT) ? Optype::PUT : Optype::DELETE;
        rec.key = op.key;
        rec.value = op.value;
        wal_records.push_back(rec);
    }

    // 批量写入 WAL
    if (!wal->batch_append(wal_records)) {
        result.success = false;
        result.error = "WAL write failed";
        return result;
    }

    // 2. 批量写入 MemTable
    Version base_version = memtable->get_current_version();
    for (const auto& op : batch.GetOps()) {
        Version ver;
        if (op.type == WriteBatch::OpType::PUT)
            ver = memtable->put(op.key, op.value);
        else
            ver = memtable->del(op.key);
        result.versions.push_back(ver);
        
        if (ver == 0)
            result.failed_keys.push_back(op.key);
    }
    result.success = result.failed_keys.empty();

    // 3. 检查是否需要 flush
    if (memtable->need_switch(config.memtable_size) && !is_flushing)
        flush_async();

    return result;
}

MVCCKVStore::BatchReadResult MVCCKVStore::BatchRead(const std::vector<string>& keys, Version snap_ver) {
    BatchReadResult result;
    result.success = true;
    result.found_count = 0;
    
    if (snap_ver == 0) {
        snap_ver = memtable->get_current_version();
    }
    
    for (const auto& key : keys) {
        BatchReadResult::Item item;
        item.key = key;
        item.found = false;
        item.version = 0;
        
        // 先查 MemTable
        string value;
        if (memtable->get(key, value, snap_ver)) {
            item.found = true;
            item.value = value;
            result.found_count++;
        }
        // 再查 SSTable
        else {
            for (auto& sst : sstables) {
                if (sst->may_contain_version(snap_ver) && sst->get(key, value)) {
                    item.found = true;
                    item.value = value;
                    result.found_count++;
                    break;
                }
            }
        }
        
        result.items.push_back(item);
    }
    
    return result;
}

MVCCKVStore::BatchWriteResult MVCCKVStore::BatchDelete(const std::vector<string>& keys) {
    WriteBatch batch;
    for (const auto& key : keys) {
        batch.Delete(key);
    }
    return BatchWrite(batch);
}

} // namespace kvstore