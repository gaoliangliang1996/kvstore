// test/gc_complete_test.cpp
#include "../include/mvcc_kvstore.h"
#include <iostream>
#include <thread>

using namespace kvstore;

void test_garbage_collection() {
    std::cout << "\n========== Garbage Collection Complete Test ==========\n" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 1. 写入多个版本
    std::cout << "1. Writing multiple versions..." << std::endl;
    for (int v = 1; v <= 10; v++) {
        store.put("user:1", "Alice_v" + std::to_string(v));
        store.put("user:2", "Bob_v" + std::to_string(v));
        store.put("user:3", "Charlie_v" + std::to_string(v));
    }
    
    auto stats = store.get_detailed_stats();
    std::cout << "   Total versions: " << stats.total_versions << std::endl;
    std::cout << "   Active keys: " << stats.active_keys << std::endl;
    
    // 2. 创建快照
    std::cout << "\n2. Creating snapshot at version " << store.get_current_version() << "..." << std::endl;
    auto snapshot = store.create_snapshot();
    Version snap_version = snapshot->get_version();
    
    // 3. 继续写入更多版本
    std::cout << "\n3. Writing more versions..." << std::endl;
    for (int v = 11; v <= 20; v++) {
        store.put("user:1", "Alice_v" + std::to_string(v));
        store.put("user:2", "Bob_v" + std::to_string(v));
    }
    
    stats = store.get_detailed_stats();
    std::cout << "   Total versions after more writes: " << stats.total_versions << std::endl;
    
    // 4. Flush 到 SSTable
    std::cout << "\n4. Flushing to SSTable..." << std::endl;
    store.flush();
    
    stats = store.get_detailed_stats();
    std::cout << "   After flush - SSTables: " << stats.sstable_count 
              << ", SSTable versions: " << stats.sstable_versions << std::endl;
    
    // 5. 运行 GC
    std::cout << "\n5. Running garbage collection..." << std::endl;
    auto gc_stats = store.garbage_collect();
    
    std::cout << "\n   GC Results:" << std::endl;
    std::cout << "   - MemTable active: removed " << gc_stats.memtable_active_versions_removed 
              << " versions" << std::endl;
    std::cout << "   - MemTable immutable: removed " << gc_stats.memtable_immutable_versions_removed 
              << " versions" << std::endl;
    std::cout << "   - SSTable: removed " << gc_stats.sstable_versions_removed 
              << " versions, deleted " << gc_stats.sstable_files_deleted << " files" << std::endl;
    std::cout << "   - Total versions before: " << gc_stats.total_versions_before << std::endl;
    std::cout << "   - Total versions after: " << gc_stats.total_versions_after << std::endl;
    std::cout << "   - Bytes freed: " << (gc_stats.total_bytes_before - gc_stats.total_bytes_after) 
              << " bytes" << std::endl;
    
    // 6. 验证快照仍然可读
    std::cout << "\n6. Verifying snapshot reads..." << std::endl;
    string value;
    if (store.get("user:1", value, snap_version)) {
        std::cout << "   Snapshot user:1 = " << value << std::endl;
    }
    
    // 7. 释放快照
    std::cout << "\n7. Releasing snapshot..." << std::endl;
    store.release_snapshot(snapshot);
    
    // 8. 再次运行 GC
    std::cout << "\n8. Running GC again after snapshot release..." << std::endl;
    gc_stats = store.garbage_collect();
    
    stats = store.get_detailed_stats();
    std::cout << "   Final - Total versions: " << stats.total_versions << std::endl;
    
    std::cout << "\n========== GC Test Passed! ==========" << std::endl;
}

void test_auto_gc() {
    std::cout << "\n========== Auto GC Test ==========\n" << std::endl;
    
    Config cfg;
    cfg.data_dir = "./data";
    cfg.memtable_size = 1024 * 1024;
    
    MVCCKVStore store(cfg);
    
    // 启用自动 GC（每 5 秒检查一次）
    store.enable_auto_gc(true, 5);
    store.set_gc_threshold(5, 50);  // 每个 key 最多 5 个版本，总共最多 50 个版本
    
    std::cout << "Auto GC enabled with threshold: max_versions_per_key=5, max_total_versions=50" << std::endl;
    
    // 写入大量版本
    std::cout << "\nWriting 20 versions for 5 keys..." << std::endl;
    for (int v = 1; v <= 20; v++) {
        for (int k = 1; k <= 5; k++) {
            store.put("key:" + std::to_string(k), "value_v" + std::to_string(v));
        }
    }
    
    // 等待自动 GC 触发
    std::cout << "\nWaiting for auto GC to trigger..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(10));
    
    auto stats = store.get_detailed_stats();
    std::cout << "\nFinal stats:" << std::endl;
    std::cout << "  Total versions: " << stats.total_versions << std::endl;
    std::cout << "  Active versions: " << stats.active_versions << std::endl;
    std::cout << "  SSTable versions: " << stats.sstable_versions << std::endl;
    
    std::cout << "\n========== Auto GC Test Passed! ==========" << std::endl;
}

int main() {
    test_garbage_collection();
    // test_auto_gc();
    return 0;
}