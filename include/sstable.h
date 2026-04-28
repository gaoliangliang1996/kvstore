#pragma once
#include "common.h"
#include "bloom_filter.h"
#include <map>
#include <fstream>
#include <unistd.h>

namespace kvstore {

const uint32_t SSTABLE_MAGIC = 0xABCD5678;
const uint32_t SSTABLE_VERSION = 3; // 版本3：Bloom Filter 在文件末尾

// 文件头结构
struct SSTableHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t bloom_offset;  // Bloom Filter 数据起始位置（在文件末尾）
    uint64_t record_offset;  // 第一条 Record 的起始位置
    uint32_t reserved[4];    // 预留空间便于后续扩展
};

class SSTable {
private:
    struct Block {
        string data;
        uint64_t offset;
        uint32_t size;
    };

    string filename;
    int fd;
    std::vector<string> keys;       // 索引：所有 key
    std::vector<uint64_t> offsets;  // 索引：每个 key 对应的 offset
    std::unique_ptr<BloomFilter> bloom_filter;

    void buildIndex();
    uint64_t writeData(const string& data);
public:
    SSTable(const string& path);
    ~SSTable();

    bool put(const string& key, const string& value);
    bool get(const string& key, string& value) const;
    bool del(const string& key);

    void buildBloomFilter();
    void writeBloomFilter();
    void readBloomFilter();

    // 从 MemTable 构建 SSTable 
    static SSTable* createFromMemTable(const string& path, std::map<string, string, NaturalLess>& data);

    size_t size() const {
        return keys.size();
    }

    // 获取 key 范围
    string get_min_key() const { return keys.empty() ? "" : keys.front(); } // get_min_key() 返回 SSTable 中最小的 key，如果没有 key 则返回空字符串。keys 是有序的吗？在哪里设置排序的？在 SSTable::createFromMemTable() 中，使用 std::map 构造数据时，std::map 会自动按 key 升序排序，因此 keys 向量中的 key 也是有序的。
    string get_max_key() const { return keys.empty() ? "" : keys.back();  } // get_max_key() 返回 SSTable 中最大的 key，如果没有 key 则返回空字符串。

    // 获取文件名
    string get_filename() const { return filename; }

    // Bloom Filter 相关
    void setBloomFilter(std::unique_ptr<BloomFilter> filter);
    const BloomFilter* getBloomFilter() const { return bloom_filter.get(); }
    bool mayContain(const string& key) const;

    // 迭代器类
    class Iterator {
    private:
        const SSTable* sstable;
        size_t current_idx;

    public:
        Iterator(const SSTable* table, size_t idx = 0);

        bool valid() const;
        void next();
        void prev();
        void seek_to_first();
        void seek_to_last();
        void seek(const string& target);

        string key() const;
        string value() const;
        uint64_t offset() const;
    };

    Iterator begin() const;
    Iterator end() const;
    Iterator find(const string& key) const;

    void close();
    bool delete_file();
};

}