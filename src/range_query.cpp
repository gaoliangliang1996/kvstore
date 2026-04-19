/**
 * @file range_query.cpp
 * @brief 范围查询功能的实现。
 *
 * 本文件实现了范围查询的核心逻辑，包括：
 * - 从 MemTable 和 SSTable 中收集数据
 * - 合并和排序查询结果
 * - 支持前缀查询和分页查询
 *
 * 范围查询通过遍历所有相关的数据源来收集结果，
 * 然后进行合并去重和排序操作。
 */

#include "range_query.h"
#include <algorithm>
#include <iostream>

namespace kvstore {

/**
 * @brief 执行范围查询 [start_key, end_key]。
 *
 * 查询过程：
 * 1. 从 MemTable 中收集符合范围的键值对（当前实现简化）
 * 2. 从所有 SSTable 中遍历查找符合范围的键
 * 3. 合并结果，去除重复键
 * 4. 对结果进行排序
 *
 * @note 当前 MemTable 查询部分是简化的，需要实现 MemTable 的范围迭代器。
 *
 * @param sstables SSTable 列表，按层级或时间顺序排列。
 * @param memtable MemTable 指针，可能为 nullptr。
 * @param start_key 起始键（包含在结果中）。
 * @param end_key 结束键（包含在结果中）。
 * @param snap_ver 快照版本，用于 MVCC 版本控制。
 * @return 包含查询结果的 RangeIterator，按键排序。
 */
RangeIterator RangeQuerySupport::scan(
    const std::vector<std::shared_ptr<SSTable>>& sstables,
    const MVCCMemTable* memtable,
    const string& start_key,
    const string& end_key,
    Version snap_ver) {
    
    RangeIterator iter;
    std::map<string, std::pair<string, Version>> merged; // key -> (value, version)
    
    // 1. 从 MemTable 收集数据
    if (memtable) {
        // 这里需要实现 MemTable 的范围扫描
        // 简化实现：遍历所有 key（实际应该用迭代器）
        // auto data = memtable->get_all_data();
        // for (const auto& [key, versions] : data) {
        //     if (key >= start_key && key <= end_key) {
        //         string value;
        //         if (memtable->get(key, value, snap_ver)) {
        //             merged[key] = {value, snap_ver};
        //         }
        //     }
        // }
    }
    
    // 2. 从 SSTable 收集数据
    // 遍历所有 SSTable，查找符合范围的键
    for (const auto& sst : sstables) {
        // 使用 SSTable 的迭代器遍历所有键值对
        for (auto it = sst->begin(); it.valid(); it.next()) {
            string key = it.key();
            // 检查键是否在查询范围内，使用 NaturalLess 进行比较
            // key >= start_key 等价于 !NaturalLess()(key, start_key)
            // end_key >= key   等价于 !NaturalLess()(end_key, key)
            if (!NaturalLess()(key, start_key) && !NaturalLess()(end_key, key)) { // 如果 key 在 [start_key, end_key] 范围内

                // 只处理未见过的键（MemTable 优先级更高）
                if (merged.find(key) == merged.end()) { // 如果这个键还没有被 MemTable 或之前的 SSTable 处理过
                    string value;
                    // 从 SSTable 获取键的值
                    if (sst->get(key, value)) {
                        merged[key] = {value, 0};
                        iter.add_result(key, value, 0);
                    }
                }
            }
        }
    }
    
    // 对结果进行排序，确保键的字典序
    iter.sort_results();
    return iter;
}

/**
 * @brief 执行前缀查询。
 *
 * 通过构造前缀的起始和结束范围来实现前缀查询。
 * 例如，前缀 "abc" 的范围是 ["abc", "abd")。
 *
 * @param sstables SSTable 列表。
 * @param memtable MemTable 指针。
 * @param prefix 键前缀。
 * @param snap_ver 快照版本。
 * @return 包含前缀匹配结果的 RangeIterator。
 */
RangeIterator RangeQuerySupport::prefix_scan(
    const std::vector<std::shared_ptr<SSTable>>& sstables,
    const MVCCMemTable* memtable,
    const string& prefix,
    Version snap_ver) {
    
    string start_key = prefix;
    string end_key = prefix;
    // 构造前缀的结束键：将最后一个字符 +1
    if (!end_key.empty()) {
        end_key.back()++;
    }
    
    return scan(sstables, memtable, start_key, end_key, snap_ver);
}

/**
 * @brief 执行分页范围查询。
 *
 * 分页查询支持大量数据的分批返回：
 * - 使用 page_token 指定查询起始位置
 * - 限制返回结果数量为 page_size
 * - 如果还有更多数据，设置 has_more 并提供 next_token
 *
 * @param sstables SSTable 列表。
 * @param memtable MemTable 指针。
 * @param start_key 起始键。
 * @param end_key 结束键。
 * @param page_size 每页最大结果数。
 * @param page_token 分页 token，空字符串表示从头开始。
 * @param snap_ver 快照版本。
 * @return 分页查询结果，包含当前页数据和分页信息。
 */
RangeQuerySupport::PageResult RangeQuerySupport::paginated_scan(
    const std::vector<std::shared_ptr<SSTable>>& sstables,
    const MVCCMemTable* memtable,
    const string& start_key,
    const string& end_key,
    size_t page_size,
    const string& page_token,
    Version snap_ver) {
    
    PageResult result;
    
    // 解析 page_token ：空 token 使用 start_key，否则使用 token 作为实际起始键
    string actual_start = page_token.empty() ? start_key : page_token;
    
    // 执行范围查询获取所有匹配结果
    auto iter = scan(sstables, memtable, actual_start, end_key, snap_ver);
    
    size_t count = 0;
    // 收集当前页的数据
    while (iter.valid() && count < page_size) {
        result.data.emplace_back(iter.key(), iter.value());
        count++;
        iter.next();
    }
    
    // 检查是否还有更多数据
    if (iter.valid()) {
        result.has_more = true;
        // 使用下一个键作为下一页的 token
        result.next_token = iter.key();
    }
    
    return result;
}

} // namespace kvstore