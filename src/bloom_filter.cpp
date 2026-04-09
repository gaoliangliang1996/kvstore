#include "bloom_filter.h"
#include <cmath>
#include <functional>
#include <algorithm>
#include <cstring>
#include <iostream>

namespace kvstore {

// BloomFilter 的实现基于经典的 Bloom Filter 设计，使用两个哈希函数进行多重哈希。位数组的大小和哈希函数的数量 根据预期的 key 数量和误判率进行计算。
// 添加 key 时，将对应位置设置为 true；检查 key 是否存在时，验证所有对应位置是否为 true。如果有任何一个位置为 false，则一定不存在；如果所有位置都为 true，则可能存在（存在误判率）。
// 序列化和反序列化方法用于将 Bloom Filter 的状态保存到字符串中，便于持久化存储。

// 构造函数：根据预期的 key 数量和误判率计算 位数组大小和哈希函数数量，并初始化位数组。 expected_keys 是预期的 key 数量，false_positive_rate 是允许的误判率，默认值为 0.01（1%）。
BloomFilter::BloomFilter(size_t expected_keys, double false_positive_rate) {

    init_hash_count(expected_keys, false_positive_rate);

    // 计算需要的位数
    // m = -n * ln(p) / (ln(2)) ^ 2
    double ln2 = std::log(2);

    // 计算位数组大小 m，并限制最小值为 64 位，以避免过小的 Bloom Filter 导致过高的误判率。
    // 先将 expected_keys 转换为 double，再取负
    bits_count = static_cast<size_t>(
        -static_cast<double>(expected_keys) * std::log(false_positive_rate) / (ln2 * ln2)
    );
    bits_count = std::max(bits_count, (size_t)64);

    std::cout << "expected_keys: " << expected_keys 
        << ", false_positive_rate: " << false_positive_rate
        << ", bits_count: " << bits_count << std::endl;

    bits.resize(bits_count, false); // 将 bits 数组设置为 bits_count 大小，并初始化为 false。bits_count 的单位是位
}

// 初始化哈希函数数量：根据预期的 key 数量和误判率计算哈希函数数量，通常 k = m / n * ln(2)，并限制在 1~32 之间。
void BloomFilter::init_hash_count(size_t expected_keys, double false_positive_rate) {
    // k = m / n * ln(2)
    double ln2 = std::log(2.0);
    // 修复：先将 expected_keys 转换为 double
    double n = static_cast<double>(expected_keys);
    double p = false_positive_rate;
    double m = -n * std::log(p) / (ln2 * ln2);
    
    // 计算哈希函数数量 k = m / n * ln(2)
    hash_count = static_cast<size_t>(m / n * ln2);
    hash_count = std::max(hash_count, (size_t)1);
    hash_count = std::min(hash_count, (size_t)32);
}

// 哈希函数：使用两个简单的哈希函数（hash1 和 hash2）来生成多个哈希值。每个哈希函数都基于字符串的字符进行计算，并结合一个种子值来增加随机性。
size_t BloomFilter::hash1(const string& key, size_t seed) const {
    // 使用一个简单的哈希算法，结合种子值来生成哈希值。这个算法基于字符串的字符进行计算，并通过乘以一个质数（131）来增加散列效果。
    size_t hash = 0;
    for (char c : key) {
        hash = hash * 131 + c;
        hash ^= seed;
    }
    return hash % bits_count;
}

size_t BloomFilter::hash2(const string& key, size_t seed) const {
    size_t hash = 0;
    // 另一个简单的哈希算法，结合种子值来生成哈希值。这个算法基于字符串的字符进行计算，并通过左移和加法来增加散列效果。
    for (char c : key) {
        hash = ((hash << 5) + hash) + c;
        hash ^= (seed << 16);
    }

    return hash % bits_count;
}

// 添加 key
void BloomFilter::add(const string& key) {
    for (size_t i = 0; i < hash_count; i++) {
        size_t pos = (hash1(key, i) + i * hash2(key, i + 12345)) % bits_count;
        bits[pos] = true;
    }
}

// 检查 key 是否可能存在；返回 false, 一定不存在；返回 true , 可能存在
bool BloomFilter::may_contain(const string& key) const {
    for (size_t i = 0; i < hash_count; i++) {
        size_t pos = (hash1(key, i) + i * hash2(key, i + 12345)) % bits_count;
        if (!bits[pos]) { // 如果任何一个位置为 false，则一定不存在
            return false;
        }
    }
    return true;
}

// 清空
void BloomFilter::clear() {
    std::fill(bits.begin(), bits.end(), false);
}

// 序列化（用于持久化）
string BloomFilter::serialize() const {
    string result;

    // 写入元数据
    result.append(reinterpret_cast<const char*>(&bits_count), sizeof(bits_count));
    result.append(reinterpret_cast<const char*>(&hash_count), sizeof(hash_count));

    // 写入位数据
    size_t byte_size = (bits_count + 7) / 8; // 计算需要的字节数，每 8 位存储在一个字节中
    for (size_t i = 0; i < byte_size; i++) { // 遍历每个字节，将 bits 数组中的位转换为字节并写入结果字符串中
        uint8_t byte = 0; // byte 用于存储当前字节的值，初始为 0
        for (size_t j = 0; j < 8 && i * 8 + j < bits_count; j++) { // 遍历每个字节中的位，将 bits 数组中的位转换为字节
            if (bits[i * 8 + j])
                byte |= (1 << j); // 字节的第 j 位设置为 1，如果 bits 数组中的对应位为 true
        }
        result.push_back(static_cast<char>(byte));
    }
    
    return result;
}

void BloomFilter::deserialize(const string& data) {
    if (data.size() < sizeof(bits_count) + sizeof(hash_count))
        return;

    size_t offset = 0;
    memcpy(&bits_count, data.data() + offset, sizeof(bits_count));
    offset += sizeof(bits_count);
    memcpy(&hash_count, data.data() + offset, sizeof(hash_count));
    offset += sizeof(hash_count);
    
    bits.resize(bits_count);
    size_t byte_size = (bits_count + 7) / 8;
    
    for (size_t i = 0; i < byte_size && offset + i < data.size(); i++) { // 遍历每个字节，从数据字符串中读取字节并将其转换为 bits 数组中的位
        uint8_t byte = static_cast<uint8_t>(data[offset + i]);
        for (size_t j = 0; j < 8 && i * 8 + j < bits_count; j++) {
            if (byte & (1 << j)) {
                bits[i * 8 + j] = true;
            }
        }
    }
}


} // namespace kvstore