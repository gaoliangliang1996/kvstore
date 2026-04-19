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

    // Bloom Filter 配置
    size_t bloom_bits_per_key = 10;             // 每个 key 使用的位数（默认10）
    double bloom_false_positive_rate = 0.01;    // 期望的假阳性率
    bool enable_bloom_filter = true;            // 是否启用 Bloom Filter
};


// 自然排序比较器
struct NaturalLess {
    bool operator() (const string&a, const string& b) const {
        return natural_compare(a, b) < 0;
    }
private:
    static int natural_compare(const string&a, const string& b) {
        size_t i = 0, j = 0;
        while (i < a.size() && j < b.size()) {
            if (isdigit(a[i]) && isdigit(b[j])) { // 同时是数字，进行数字比较
                // 解析数字
                size_t i_start = i, j_start = j;
                while (i < a.size() && isdigit(a[i])) i++;
                while (j < b.size() && isdigit(b[j])) j++;

                unsigned long long num_a = stoull(a.substr(i_start, i - i_start));
                unsigned long long num_b = stoull(b.substr(j_start, j - j_start));

                if (num_a != num_b)
                    return num_a < num_b ? -1 : 1;
            }
            else { // 非数字，进行字符比较
                if (a[i] != b[j])
                    return a[i] < b[j] ? -1 : 1;

                i++;
                j++;
            }
        }

        return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
};

}