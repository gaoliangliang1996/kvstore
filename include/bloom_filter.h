#pragma once
#include "common.h"
#include <vector>
#include <functional>

namespace kvstore {

class BloomFilter {
private:
    std::vector<bool> bits; // 位数组，存储 Bloom Filter 的数据
    size_t bits_count;      // 位数组的大小
    size_t hash_count;      // 哈希函数的数量

    // 哈希函数
    size_t hash1(const string& key, size_t seed) const;
    size_t hash2(const string& key, size_t seed) const;

public:
    // bits_per_key: 每个 key 使用的位数（通常 10）
    // expected_keys: 预期的 key 数量
    BloomFilter(size_t expected_keys, double false_positive_rate = 0.01);

    // 添加 key
    void add(const string& key);

    // 检查 key 是否可能存在；返回 false, 一定不存在；返回 true , 可能存在
    bool may_contain(const string& key) const;

    // 清空
    void clear();

    // 序列化（用于持久化）
    string serialize() const;
    void deserialize(const string& data);

private:
    void init_hash_count(size_t expected_keys, double false_positive_rate);
};

} // namespace kvstore