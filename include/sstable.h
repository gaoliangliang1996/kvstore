#pragma once
#include "common.h"
#include <map>
#include <fstream>

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
    bool get(const string& key, string& value);
    bool del(const string& key);

    // 从 MemTable 构建 SSTable 
    static SSTable* createFromMemTable(const string& path, std::map<string, string>& data);

    size_t size() const {
        return keys.size();
    }
};

}