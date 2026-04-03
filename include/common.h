#pragma once
#include <string>
#include <cstdint>
#include <vector>
#include <memory>

namespace kvstore {

using std::string;

// 操作类型
enum class Optype : uint8_t {
    PUT = 0x01,
    DELETE = 0x02
};

// 公共结构
struct Record {
    Optype type;
    string key;
    string value;
};

// 错误码
enum class Status {
    OK = 0,
    NOT_FOUNT,
    CORRUPTION,
    IO_ERROR,
    INVALID_ARGUMENT
};

// 配置
struct Config {
    size_t memtable_size = 4 * 1024 * 1024;
    string data_dir = "./data";
    string wal_file = "./data/wal.log";
};


}