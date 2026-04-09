#pragma once
#include "common.h"
#include "sstable.h"
#include <vector>
#include <memory>
#include <queue>
#include <mutex>
#include <thread>
#include <atomic>

/*
Compaction 设计说明：

1. 目标
   Compaction 负责将 LSM-Tree 中不同层级的 SSTable 进行归并，
   通过删除旧版本、过滤 tombstone 、合并重叠范围，减少文件数量和读放大。

2. 层级模型
   - Level 0：使用文件数量阈值触发 compaction 。
        由于 Level 0 文件之间可能存在 key 范围重叠，合并时会把所有 Level 0 文件与 与之范围重叠的 Level 1 文件一起归并。
   - Level 1 及以上：使用层级总大小阈值触发 compaction。
     只选择一个本层文件（当前实现为最大文件）与下一层重叠文件合并。

3. 主要组件
   - Compaction
     管理各层级的 SSTable 列表、合并触发逻辑、后台线程和文件迁移。
   - MergeIterator
     对多个 SSTable 迭代器进行多路归并，返回按 key 升序排序的流。
     它负责跳过相同 key 的旧版本，保证同一 key 只返回一次。

4. 工作流程
   - 后台线程周期性检查 need_compaction()。
   - 如果满足 compaction 条件，调用 pick_files_for_compaction() 选取待合并文件。
   - 调用 do_compaction() 执行合并：
       1) 创建 MergeIterator，将本层文件和下一层重叠文件归并。
       2) 遍历归并结果，过滤 tombstone（"__DELETED__"）并仅保留最新 key。
       3) 将合并后数据写入新的 SSTable，写入目标层级。
       4) 从原层级中移除旧的 SSTable。

5. 并发与一致性
   - 使用 mutex 保护 level_files 与 compaction 过程中对文件列表的修改。
   - 外部写入新 SSTable 时应调用 add_sstable()，该方法也会对当前层级列表加锁。

6. 可扩展性
   - 目前 Compaction 仅实现最基础的层级合并策略，未来可扩展：
       * 按最少重叠范围选择参与合并文件
       * 支持多文件合并到下层而非仅一个本层文件
       * 引入级别间大小平衡策略和更复杂的 compaction 策略
*/

namespace kvstore {

class SSTable;

// 层级配置
struct LevelConfig {
    int level;                          // 层级编号
    size_t max_file_size;               // 单文件最大大小
    size_t max_total_size;              // 该层总大小限制
    size_t max_file_count;              // 最大文件数（Level 0 使用）
};

// 待合并的文件信息，用于描述一次 compaction 需处理的文件集合
// `target_level` 表示当前合并操作的起始层级，
// `files` 是本层参与合并的 SSTable，
// `next_level_files` 是与之范围重叠的下一层文件
struct CompactionInput {
    int target_level;
    std::vector<SSTable*> files;
    std::vector<SSTable*> next_level_files;
};

// MergeIterator 负责归并多个有序的 SSTable 迭代器，
// 输出结果按 key 升序，且同一 key 只返回最新版本。
class MergeIterator {
private:
    struct Item {
        string key;
        string value;
        int file_idx;       // 来源文件索引，标识该 key 来自哪个 SSTable 迭代器
        uint64_t sequence;  // 用于处理重复 key 的顺序，后写入的记录优先。
                            // 当前实现中 sequence 尚未显式赋值，但保留该字段便于未来扩展。
        
        bool operator>(const Item& other) const {
            if (key != other.key) return key > other.key;
            return sequence > other.sequence;  // key 相同时按写入顺序比较
        }
    };

    std::vector<SSTable::Iterator> iters; // 每个参与合并的 SSTable 的迭代器列表，iters[i] 对应第 i 个 SSTable 的迭代器。
    std::priority_queue<Item, // Item 是堆中元素的类型，包含 key、value、来源文件索引和 sequence 信息。
                         std::vector<Item>, // std::vector<Item> 是底层容器类型，用于存储堆中的元素。优先队列会使用这个 vector 来维护堆的结构。
                         std::greater<Item> // std::greater<Item> 是比较器，用于定义堆的排序规则。在这里，使用 std::greater<Item> 表示这是一个最小堆，堆顶元素是 key 最小的那个 Item。
                        > heap;             // 最小堆，按 key 升序输出。heap 的作用是维护当前所有迭代器的最小元素，保证每次输出的都是当前所有迭代器中 key 最小的那个。
    // 在哪里实现归并？通过 MergeIterator 来实现归并。MergeIterator 内部维护一个最小堆，堆中元素包含当前所有参与合并的 SSTable 迭代器的当前 key/value。每次调用 iter.next() 时，MergeIterator 会弹出堆顶元素（当前最小 key），然后推进对应文件的迭代器，并将新的 key/value 压入堆中。这样就保证了每次输出的都是当前所有迭代器中 key 最小的那个，实现了多路归并。


    std::string current_key; // current_key 存储当前输出的 key，便于外部调用 key() 方法获取当前 key。
    bool has_current; // has_current 标志当前 heap 是否有有效元素

public:
    MergeIterator(std::vector<SSTable*> files);
    bool valid();
    void next();
    string key();
    string value();
};

class Compaction {
private:
    std::vector<LevelConfig> levels;
    // 每个层级持有自己的 SSTable 列表，使用 unique_ptr 管理生命周期
    std::vector<std::vector<std::shared_ptr<SSTable>>> level_files; // level_files[level] 存储该层级的 SSTable 列表，使用 unique_ptr 管理生命周期，确保文件资源正确释放。
    std::mutex mutex;                     // 保护 level_files 等共享状态
    std::thread background_thread;        // 后台合并线程
    std::atomic<bool> running;
    std::atomic<bool> compaction_needed;  // 目前仅作为外部触发标识使用

    string data_dir;                      // SSTable 文件所在目录

    // 配置参数
    size_t level0_file_count_threshold = 10;                 // level 0 文件数阈值
    size_t level0_file_size_threshold = 4 * 1024 * 1024;    // 4 MB
    size_t level_base_size = 10 * 1024 * 1024;              // level 1 基础大小 10MB; level 2 基础大小 100MB ...
    double level_multiplier = 10.0;                         // 层级倍数

    // 内部方法
    bool need_compaction();
    CompactionInput pick_files_for_compaction();
    void do_compaction(const CompactionInput& input);
    // 以下方法当前保留为未来扩展，部分尚未在源文件中实现。
    void merge_and_generate(const CompactionInput& input, std::vector<std::pair<string, string>>& merged_data);
    void write_new_sstable(int level, std::vector<std::pair<string, string>>& data);
    void remove_old_files(const CompactionInput& input);
    void adjust_level_sizes();
    void background_worker();

    // 文件删除辅助函数
    bool remove_file(const string& filepath);
    bool rename_file(const string& old_path, const string& new_path);
    bool file_exists(const string& filepath);

    // 异步删除，避免阻塞
    void async_remove_file(const string& filename);
public:
    Compaction(const string& dir);
    ~Compaction();

    // 添加新生成的 SSTable
    void add_sstable(int level, std::shared_ptr<SSTable> sstable);

    // 触发合并（可异步）
    void trigger_compaction();

    // 获取指定层级的文件列表
    std::vector<SSTable*> get_level_files(int level);

    // 统计信息
    size_t get_level_size(int level);
    size_t get_level_count(int level);

    // 停止后台线程
    void stop();
};

}