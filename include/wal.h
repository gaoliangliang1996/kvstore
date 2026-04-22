#pragma once
#include "common.h"
#include <fstream>
#include <functional>

namespace kvstore {

class WAL {
private:
    int fd;
    uint64_t offset;
    string filename;

    // 编码格式：CRC32(4) + Type(1) + KeyLen(4) + ValLen(4) + Key + Value
    string encodeRecord(const Record& rec);
    bool decodeRecord(const string& data, Record& rec);
    uint32_t calculateCRC(const string& data);
public:
    WAL(const string& path);
    ~WAL();

    bool append(const Record& rec);
    bool batch_append(std::vector<Record>& records);
    bool recover(std::function<bool (const Record&)> callback);
    void sync();
    void truncate();
};

}