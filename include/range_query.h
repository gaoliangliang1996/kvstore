#pragma once
/**
 * @file range_query.h
 * @brief 范围查询功能的核心接口定义。
 *
 * 本文件定义了支持键值存储范围查询的类和接口，包括：
 * - RangeIterator：范围查询结果的迭代器
 * - RangeQuerySupport：提供范围扫描、前缀查询和分页查询功能
 *
 * 范围查询能够高效地检索键在指定范围内的所有键值对，
 * 支持从 MemTable 和 SSTable 中合并查询结果。
 */

#include "common.h"
#include "mvcc.h"
#include "sstable.h"
#include <vector>
#include <memory>
#include <algorithm>

namespace kvstore {

class RangeIterator {
private:
    struct KeyValue {
        string key;     ///< 键
        string value;   ///< 值
        Version version; ///< 版本号
    };
    
    std::vector<KeyValue> results; ///< 查询结果列表
    size_t current_pos;            ///< 当前迭代位置
    
public:
    RangeIterator() : current_pos(0) {}
    
    void add_result(const string& key, const string& value, Version ver) {
        results.push_back({key, value, ver});
    }
    
    void sort_results() {
        std::sort(results.begin(), results.end(),
                  [](const KeyValue& a, const KeyValue& b) {
                      return NaturalLess()(a.key, b.key);
                  });
    }
    

    bool valid() const { return current_pos < results.size(); }
    void next() { current_pos++; }
    string key() const { return results[current_pos].key; }
    string value() const { return results[current_pos].value; }
    Version version() const { return results[current_pos].version; }
    size_t size() const { return results.size(); }
};

/**
 * @class RangeQuerySupport
 * @brief 范围查询支持类。
 *
 * 提供多种范围查询功能，包括基本的范围扫描、前缀查询和分页查询。
 * 支持从多个 SSTable 和 MemTable 中合并查询结果。
 */
class RangeQuerySupport {
public:
    /**
     * @brief 执行范围查询 [start_key, end_key]。
     *
     * 从指定的 SSTable 列表和 MemTable 中查询键在 [start_key, end_key] 范围内的所有键值对。
     * 查询结果按键排序，去除重复键（保留最新版本）。
     *
     * @param sstables SSTable 列表。
     * @param memtable MemTable 指针，可以为 nullptr。
     * @param start_key 起始键（包含）。
     * @param end_key 结束键（包含）。
     * @param snap_ver 快照版本，用于 MVCC。
     * @return 包含查询结果的 RangeIterator。
     */
    static RangeIterator scan(const std::vector<std::shared_ptr<SSTable>>& sstables,
                              const MVCCMemTable* memtable,
                              const string& start_key,
                              const string& end_key,
                              Version snap_ver);
    
    /**
     * @brief 执行前缀查询。
     *
     * 查询所有以指定前缀开头的键值对。
     * 通过构造适当的起始和结束键来实现范围查询。
     *
     * @param sstables SSTable 列表。
     * @param memtable MemTable 指针，可以为 nullptr。
     * @param prefix 键前缀。
     * @param snap_ver 快照版本，用于 MVCC。
     * @return 包含查询结果的 RangeIterator 。
     */
    static RangeIterator prefix_scan(const std::vector<std::shared_ptr<SSTable>>& sstables,
                                     const MVCCMemTable* memtable,
                                     const string& prefix,
                                     Version snap_ver);
    
    /**
     * @struct PageResult
     * @brief 分页查询结果结构体。
     * 在分页查询中，next_token 是一个字符串，用于标识下一页查询的起始位置。
     * 它通常包含上一次查询的最后一个键，或者是一个特殊的标记，告诉系统从哪里继续查询下一批数据。
     * 客户端在请求下一页数据时，会将这个 next_token 作为参数传回服务器，
     * 服务器根据这个 token 来确定从哪个键开始返回下一页的数据。
     * 这种机制允许客户端在处理大量数据时，分批次地获取结果，而不需要一次性加载所有数据。
     */
    struct PageResult {
        std::vector<std::pair<string, string>> data; ///< 当前页的数据
        string next_token;                            ///< 下一页的起始 token
        bool has_more;                                ///< 是否还有更多数据
    };
    
    /**
     * @brief 执行分页范围查询。
     *
     * 支持分页的范围查询，每次返回指定数量的结果，并提供下一页的 token。
     * 用于处理大量数据的场景，避免一次性加载过多数据。
     *
     * @param sstables SSTable 列表。
     * @param memtable MemTable 指针，可以为 nullptr。
     * @param start_key 起始键（包含）。
     * @param end_key 结束键（包含）。
     * @param page_size 每页大小。
     * @param page_token 分页 token，用于指定起始位置。
     * @param snap_ver 快照版本，用于 MVCC。
     * @return 分页查询结果。
     */
    static PageResult paginated_scan(const std::vector<std::shared_ptr<SSTable>>& sstables,
                                     const MVCCMemTable* memtable,
                                     const string& start_key,
                                     const string& end_key,
                                     size_t page_size,
                                     const string& page_token,
                                     Version snap_ver);
};

} // namespace kvstore