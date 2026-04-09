#pragma once
#include "common.h"
#include <map>
#include <fstream>
#include <unistd.h>

namespace kvstore {

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

    void buildIndex();
    uint64_t writeData(const string& data);
public:
    SSTable(const string& path);
    ~SSTable();

    bool put(const string& key, const string& value);
    bool get(const string& key, string& value) const;
    bool del(const string& key);

    // 从 MemTable 构建 SSTable 
    static SSTable* createFromMemTable(const string& path, std::map<string, string>& data);

    size_t size() const {
        return keys.size();
    }

    // 获取 key 范围
    string get_min_key() const { return keys.empty() ? "" : keys.front(); } // get_min_key() 返回 SSTable 中最小的 key，如果没有 key 则返回空字符串。keys 是有序的吗？在哪里设置排序的？在 SSTable::createFromMemTable() 中，使用 std::map 构造数据时，std::map 会自动按 key 升序排序，因此 keys 向量中的 key 也是有序的。
    string get_max_key() const { return keys.empty() ? "" : keys.back();  } // get_max_key() 返回 SSTable 中最大的 key，如果没有 key 则返回空字符串。

    // 获取文件名
    string get_filename() const { return filename; }
    bool delete_file() {
        if (fd >= 0) {
            close(fd);
            fd = -1;
        }
        return unlink(filename.c_str()) == 0;
    }

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
};

}