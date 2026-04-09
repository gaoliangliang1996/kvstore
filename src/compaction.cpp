#include "compaction.h"
#include <algorithm>
#include <iostream>
#include <chrono>
#include <map>
#include <cmath>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

namespace kvstore {

// ============== MergeIterator 实现 ==============
MergeIterator::MergeIterator(std::vector<SSTable*> files) : has_current(false) {
    // 将每个 SSTable 的开始迭代器压入优先队列
    // 优先队列按照 key 升序排序，便于多路归并。
    for (size_t i = 0; i < files.size(); i++) { // 遍历每个参与合并的 SSTable，每个 SSTable 创建一个迭代器
        SSTable::Iterator it = files[i]->begin();
        if (it.valid()) {
            iters.push_back(std::move(it)); // 将迭代器存储在 iters 中，便于后续推进迭代器。std::move 的作用是将 it 资源转移到 iters 中，避免不必要的复制。
            heap.push({it.key(), it.value(), (int)i}); // sequence 暂时未使用，默认为 0；把当前 key、value 和文件索引压入堆中，以便后续归并输出。
                                                       // {it.key(), it.value(), (int)i} 是一个 Item 结构体的初始化列表，表示当前迭代器指向的 key、value 和文件索引。这个 Item 会被压入 heap 中，heap 会根据 key 的升序来维护这些 Item，从而实现多路归并的功能。
        }
    }
    
    if (!heap.empty()) {
        has_current = true;
        current_key = heap.top().key;
    }
}

bool MergeIterator::valid() {
    return has_current;
}

// MergeIterator::next() 的实现较为复杂，涉及到堆的维护和迭代器的推进。核心逻辑是每次弹出堆顶元素（当前最小 key），然后推进对应文件的迭代器，并将新的 key/value 压入堆中。同时需要跳过后续重复 key，保证每个 key 只返回一次。
// 怎么实现归并的？通过维护一个最小堆（priority_queue）来实现多路归并。每个参与合并的 SSTable 都有一个迭代器，初始时将每个迭代器的第一个元素（key/value）压入堆中。
// 堆会根据 key 的升序来维护这些元素，每次弹出堆顶元素就是当前所有迭代器中 key 最小的那个。弹出后，推进对应文件的迭代器，如果该迭代器仍然有效，则将新的 key/value 压入堆中。
// 这样就保证了每次输出的都是当前所有迭代器中 key 最小的那个，实现了多路归并。

// 同时，如果堆顶元素与刚弹出的元素 key 相同，说明是重复 key，需要跳过，继续推进对应文件的迭代器，并将新的 key/value 压入堆中，直到堆顶元素的 key 不再与当前 key 相同为止。
void MergeIterator::next() {
    if (!has_current) return;
    
    // 弹出当前最小 key，并推进对应 SSTable 的迭代器。
    Item current = heap.top();
    heap.pop();
    
    // 推进对应文件的迭代器，并将新的 key/value 压入堆中。
    iters[current.file_idx].next(); // 推进当前文件的迭代器到下一个位置
    if (iters[current.file_idx].valid()) { // 如果推进后迭代器仍然有效，说明该文件还有更多 key，可以继续参与归并
        // 将新的 key/value 压入堆中，以便后续归并输出。
        heap.push({iters[current.file_idx].key(), 
                   iters[current.file_idx].value(),
                   current.file_idx});
    }
    
    // 因为同一个 key 可能在多个文件中存在旧版本，
    // 这里跳过后续重复 key，保证每个 key 只返回一次。
    while (!heap.empty() && heap.top().key == current.key) { // 当前堆顶元素与刚弹出的 current key 相同，说明是重复 key，需要跳过。怎么跳过的？通过推进对应文件的迭代器，并将新的 key/value 压入堆中，直到堆顶元素的 key 不再与 current.key 相同。
        Item dup = heap.top(); // 当前堆顶元素与刚弹出的 current key 相同，说明是重复 key，需要跳过
        heap.pop();

        // 推进对应文件的迭代器，并将新的 key/value 压入堆中。
        iters[dup.file_idx].next();
        if (iters[dup.file_idx].valid()) {
            heap.push({iters[dup.file_idx].key(),
                       iters[dup.file_idx].value(),
                       dup.file_idx});
        }
    }
    
    // 更新当前输出 key 状态
    if (heap.empty()) {
        has_current = false;
    } else {
        current_key = heap.top().key; // 更新 current_key 为新的堆顶 key，准备下一次输出。
    }
}


string MergeIterator::key() {
    // 返回当前归并结果的 key
    return has_current ? current_key : "";
}

string MergeIterator::value() {
    // 返回当前归并结果对应的 value
    return has_current ? heap.top().value : "";
}

// ============== Compaction 实现 ==============
Compaction::Compaction(const string& dir) 
    : running(true), compaction_needed(false), data_dir(dir) {
    
    // 初始化层级配置，默认支持 0~6 共 7 个层级。
    // Level 0 采用文件数量阈值判断，其他层级根据总大小判断。
    levels.resize(7);  // Level 0-6
    for (int i = 0; i <= 6; i++) {
        levels[i].level = i;
        levels[i].max_file_size = 2 * 1024 * 1024;  // 2MB
        if (i == 0) {
            levels[i].max_file_count = level0_file_count_threshold; // 最大文件数 4
            levels[i].max_total_size = level0_file_size_threshold;  // 该层总大小 4MB
        } else {
            levels[i].max_total_size = level_base_size * pow(level_multiplier, i - 1); // 该层总大小 逐层递增，Level 1 是 10MB，Level 2 是 100MB，依此类推。
        }
    }
    
    level_files.resize(7); // 为每个层级初始化文件列表
    
    // 启动后台合并线程，在线程中周期性检查是否需要 compaction。
    background_thread = std::thread(&Compaction::background_worker, this); // this的作用是将当前 Compaction 实例的指针传递给 background_worker 方法，使得该方法能够访问和修改 Compaction 实例的成员变量。
                                                                           // background_worker 方法是 Compaction 类的成员函数，负责在后台线程中执行合并操作。通过将 this 作为参数传递给 std::thread 构造函数，background_worker 方法能够访问 Compaction 实例的成员变量和方法，从而实现合并逻辑。
                                                                           // 为什么 background_worker() 没有接收形参？因为 background_worker 是 Compaction 类的成员函数，它可以直接访问 Compaction 实例的成员变量和方法，无需通过参数传递实例指针。std::thread 会自动将 this 作为参数传递给 background_worker 方法，使得该方法能够访问 Compaction 实例的成员变量和方法。
                                                                           // 那 this 是不是可以省略？不可以省略，因为 background_worker 是 Compaction 类的成员函数，必须通过实例调用才能访问成员变量和方法。std::thread 需要一个可调用对象作为参数，而 background_worker 是一个成员函数指针，需要与实例绑定才能成为可调用对象。因此，必须使用 this 来绑定当前实例，使得 background_worker 方法能够访问 Compaction 实例的成员变量和方法。
}

Compaction::~Compaction() {
    running = false;
    if (background_thread.joinable()) {
        background_thread.join();
    }
}

bool Compaction::need_compaction() {
    // Level 0：使用文件数量阈值判断，因为 Level 0 文件可能存在重叠。
    if (level_files[0].size() >= level0_file_count_threshold) {
        return true;
    }
    
    // 其他层级：根据层级总大小判断是否需要触发 compaction。
    for (int i = 1; i <= 6; i++) {
        size_t total_size = 0;
        for (auto& sst : level_files[i]) {
            total_size += sst->size();
        }
        if (total_size > levels[i].max_total_size) {
            return true;
        }
    }
    
    return false;
}

// 根据当前层级文件状态选择需要合并的文件集合，优先处理 Level 0。
CompactionInput Compaction::pick_files_for_compaction() {
    CompactionInput input;
    
    // 优先处理 Level 0：将所有 Level 0 文件合并到 Level 1。
    if (level_files[0].size() >= level0_file_count_threshold) {
        input.target_level = 0;
        
        // 选择当前所有 Level 0 的文件参与合并
        for (auto& sst : level_files[0]) {
            input.files.push_back(sst.get()); // sst.get() 获取原始指针，input.files 存储的是裸指针，实际管理权仍在 level_files 中的 unique_ptr。
        }
        
        // 找出 Level 1 中与这些文件 key 范围重叠的文件
        if (!level_files[1].empty()) {
            string min_key = input.files[0]->get_min_key(); // 以第一个 Level 0 文件的最小 key 作为合并范围的起点
            string max_key = input.files[0]->get_max_key(); // 以第一个 Level 0 文件的最大 key 作为合并范围的终点，实际中可以根据所有 Level 0 文件的 key 范围来确定更准确的 min_key 和 max_key 。
            
            for (auto& sst : level_files[1]) {
                if (!(sst->get_max_key() < min_key || sst->get_min_key() > max_key)) {
                    input.next_level_files.push_back(sst.get()); // 把 Level 1 中与 Level 0 文件 key 范围重叠的文件加入 next_level_files，准备参与合并。
                }
            }
        }
        
        return input;
    }
    
    // 处理其他层级：找到第一个超过大小阈值的层级，并合并其中一个文件与下层重叠文件。
    for (int level = 1; level <= 5; level++) {
        size_t total_size = 0;
        for (auto& sst : level_files[level]) { // 计算当前层级的总大小，以判断是否需要触发 compaction。
            total_size += sst->size();
        }
        
        if (total_size > levels[level].max_total_size) {
            input.target_level = level;
            
            // 选择本层最大的文件进行合并，以控制层级数据膨胀。
            SSTable* largest = nullptr;
            size_t largest_size = 0;
            for (auto& sst : level_files[level]) { // 遍历当前层级的 SSTable ，找到最大的那个文件，准备参与合并。选择最大的文件有助于控制层级数据膨胀，因为较大的文件更可能包含更多的 key，合并后可以更有效地减少重叠和冗余数据。
                if (sst->size() > largest_size) {
                    largest_size = sst->size();
                    largest = sst.get();
                }
            }
            
            if (largest) {
                input.files.push_back(largest);
                
                // 找出下一层中与该文件 key 范围重叠的文件
                string min_key = largest->get_min_key();
                string max_key = largest->get_max_key();
                
                // 遍历下一层的文件，找出与当前文件 key 范围重叠的文件，加入 next_level_files 以准备参与合并。
                for (auto& sst : level_files[level + 1]) {
                    if (!(sst->get_max_key() < min_key || sst->get_min_key() > max_key)) {
                        input.next_level_files.push_back(sst.get());
                    }
                }
            }
            
            break;
        }
    }
    
    return input;
}

// 执行 compaction：归并输入文件，生成新的 SSTable，并移除旧文件。
void Compaction::do_compaction(const CompactionInput& input) {
    // 使用互斥锁保护 level_files 的读写，避免与外部写入冲突。
    std::lock_guard<std::mutex> lock(mutex);
    
    std::cout << "[Compaction] Starting compaction at level " << input.target_level << std::endl;
    
    // 1. 收集本层与下一层的所有参与合并的文件
    std::vector<SSTable*> all_files;
    all_files.insert(all_files.end(), input.files.begin(), input.files.end());
    all_files.insert(all_files.end(), input.next_level_files.begin(), input.next_level_files.end());
    
    if (all_files.empty()) return;
    
    // 2. 创建归并迭代器，将多个 SSTable 的有序流合并成单条输出流
    MergeIterator iter(all_files); // 每个参与合并的 SSTable 的迭代器列表，iters[i] 对应第 i 个 SSTable 的迭代器。
    
    // 3. 遍历归并结果：跳过 tombstone 删除标记并去重保留最新版本
    std::vector<std::pair<string, string>> merged_data;
    string last_key;
    
    while (iter.valid()) {
        string key = iter.key();
        string value = iter.value();
        
        // 遇到删除标记则忽略该 key
        if (value == "__DELETED__") {
            iter.next();
            continue;
        }
        
        // 相同 key 只添加一次， MergeIterator 已经保证读取的是最新版本。
        if (key != last_key) {
            merged_data.emplace_back(key, value);
            last_key = key;
        }
        
        iter.next();
    }
    
    std::cout << "[Compaction] Merged " << merged_data.size() << " unique keys" << std::endl;
    
    // 4. 生成新的 SSTable：Level 0 合并结果写入 Level 1，其它层写入当前层
    int target_level = input.target_level;
    if (input.target_level == 0) {
        target_level = 1;  // Level 0 合并到 Level 1
    }
    
    write_new_sstable(target_level, merged_data);
    
    // 5. 从原层级中移除旧的 SSTable
    remove_old_files(input);
    
    std::cout << "[Compaction] Compaction completed" << std::endl;
}

// 将合并后的数据写入新的 SSTable，并添加到目标层级。使用 unique_ptr 管理 SSTable 生命周期，确保资源正确释放。
void Compaction::write_new_sstable(int level, std::vector<std::pair<string, string>>& data) {
    if (data.empty()) return;
    
    // 按 key 排序，确保写入的 SSTable 数据是有序的。
    std::sort(data.begin(), data.end());
    
    // 生成新的合并后 SSTable 文件名
    static int file_id = 0;
    string filename = data_dir + "/compacted_" + std::to_string(level) 
                      + "_" + std::to_string(file_id++) + ".sst";
    
    // 使用 map 构造数据并创建 SSTable
    std::map<string, string> data_map(data.begin(), data.end());
    std::unique_ptr<SSTable> new_sst = std::unique_ptr<SSTable>(
        SSTable::createFromMemTable(filename, data_map)
    );
    
    // 把新 SSTable 添加到目标层级，并按 key 范围排序
    level_files[level].push_back(std::move(new_sst));

    // 排序层级内的 SSTable ，确保它们按 key 范围有序，保持层级内文件有序，便于后续合并和查询。
    std::sort(level_files[level].begin(), 
              level_files[level].end(),
              [](const auto& a, const auto& b) { return a->get_min_key() < b->get_min_key(); }
             );
}

// 从层级中移除参与合并的旧文件。当前实现中仅从内存结构中移除，实际文件删除操作可在此基础上扩展实现。
void Compaction::remove_old_files(const CompactionInput& input) {
    // 删除当前层级中参与合并的旧文件
    for (size_t i = 0; i < input.files.size(); i++) { // 遍历当前层级中参与合并的旧文件，找到它们在 level_files 中的位置并移除。使用 std::find_if 查找对应的 unique_ptr，然后调用 erase 移除。
        auto it = std::find_if(level_files[input.target_level].begin(),
                               level_files[input.target_level].end(),
                               [&](const auto& sst) { return sst.get() == input.files[i]; } // lambda 函数用于比较 level_files 中的 unique_ptr 是否指向 input.files[i]，找到对应的文件后返回 true，std::find_if 就会返回该位置的迭代器。
                              );
        if (it != level_files[input.target_level].end()) {
            // 获取文件路径并删除磁盘文件
            string filepath = (*it)->get_filename();
            if (remove_file(filepath)) {
                std::cout << "[Compaction] Deleted file: " << filepath << std::endl;
            }
            else {
                std::cerr << "[Compaction] Failed to deleted file: " << filepath << std::endl;
            }

            // 从内存结构中移除
            level_files[input.target_level].erase(it);
        }
    }
    
    // 删除与下一层重叠的旧文件
    int next_level = input.target_level + 1;
    for (size_t i = 0; i < input.next_level_files.size(); i++) { // 遍历下一层中参与合并的旧文件，找到它们在 level_files 中的位置并移除。使用 std::find_if 查找对应的 unique_ptr，然后调用 erase 移除。
        auto it = std::find_if(level_files[next_level].begin(),
                               level_files[next_level].end(),
                               [&](const auto& sst) { return sst.get() == input.next_level_files[i]; });
        if (it != level_files[next_level].end()) {
            // 获取文件路径并删除磁盘文件
            string filepath = (*it)->get_filename();
            if (remove_file(filepath)) {
                std::cout << "[Compaction] Deleted file: " << filepath << std::endl;
            }
            else {
                std::cerr << "[Compaction] Failed to deleted file: " << filepath << std::endl;
            }

            // 从内存结构中移除

            level_files[next_level].erase(it);
        }
    }
}

bool Compaction::remove_file(const string& filepath) {
    if (!file_exists(filepath)) // 文件不存在
        return true;

    if (unlink(filepath.c_str()) != 0) {
        std::cerr << "[Compaction] unlink failed: " << filepath << ", error: " << strerror(errno) << std::endl;
        return false;
    }

    return true;
}

bool Compaction::rename_file(const string& old_path, const string& new_path) {
    if ((rename(old_path.c_str(), new_path.c_str())) != 0) {
        std::cerr << "[Compaction] rename failed: " << old_path << " -> " << new_path << ", error: " << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

bool Compaction::file_exists(const string& filepath) {
    struct stat buffer;
    return (stat(filepath.c_str(), &buffer) == 0);
}

void async_remove_file(const string& filepath) {
    // 异步删除文件，避免阻塞主流程
    std::thread([filepath]() {
        // 稍等一下，确保没有其他线程在使用文件
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        if (unlink(filepath.c_str()) != 0) {
            std::cerr << "[Compaction] Async delete failed: " << filepath << ", error: " << strerror(errno) << std::endl;
        } else {
            std::cout << "[Compaction] Async deleted: " << filepath << std::endl;
        }
    }).detach();
}

void Compaction::background_worker() {
    while (running) {
        if (need_compaction()) {
            auto input = pick_files_for_compaction();
            if (!input.files.empty()) {
                do_compaction(input);
            }
        }
        
        // 后台线程周期性检查 compaction 需求，避免 busy loop。
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

void Compaction::trigger_compaction() {
    // 外部调用时仅设置触发标志，当前实现中后台线程仍以定时检查为主。
    compaction_needed = true;
}

// 添加新 SSTable，保证层级内文件按 key 范围有序。使用 unique_ptr 管理 SSTable 生命周期，确保资源正确释放。
void Compaction::add_sstable(int level, std::shared_ptr<SSTable> sstable) {
    std::lock_guard<std::mutex> lock(mutex);
    
    if (level < 0 || level >= (int)level_files.size()) {
        return;
    }
    
    // 添加新 SSTable，保证所有层级保持按 key 范围有序
    level_files[level].push_back(sstable);

    // 排序层级内的 SSTable，确保它们按 key 范围有序，保持层级内文件有序，便于后续合并和查询。
    std::sort(level_files[level].begin(), 
              level_files[level].end(), 
              [](const auto& a, const auto& b) { return a->get_min_key() < b->get_min_key(); } // 排序 lambda 函数，按照 SSTable 的最小 key 进行排序，确保层级内的 SSTable 按 key 范围有序。
            );
}

std::vector<SSTable*> Compaction::get_level_files(int level) {
    std::lock_guard<std::mutex> lock(mutex);
    
    std::vector<SSTable*> result;
    for (auto& sst : level_files[level]) {
        result.push_back(sst.get());
    }
    return result;
}

size_t Compaction::get_level_size(int level) {
    std::lock_guard<std::mutex> lock(mutex);
    
    size_t total = 0;
    for (auto& sst : level_files[level]) {
        total += sst->size();
    }
    return total;
}

size_t Compaction::get_level_count(int level) {
    std::lock_guard<std::mutex> lock(mutex);
    return level_files[level].size();
}

void Compaction::stop() {
    running = false;
}

} // namespace kvstore