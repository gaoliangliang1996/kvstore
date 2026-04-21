#include "sstable.h"
#include <cstring>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <iostream>
#include "bloom_filter.h"

namespace kvstore {

// SSTable 文件格式：
// +------------------+
// | Header           |
// | - magic (4B)     |
// | - version (4B)   |
// | - bloom_offset (8B) |
// | - record_offset (8B) |
// +------------------+
// | Record 1         |
// | - len (4B)       |
// | - data           |
// +------------------+
// | Record 2         |
// | - len (4B)       |
// | - data           |
// +------------------+
// | ... 其他的 Record |
// +------------------+
// | bloom filter 数据 |
// | - len (4B)       |
// | - bits_count     |
// | - hash_count     |
// | - bits 生成的字节 |
// +------------------+

// const uint32_t SSTABLE_MAGIC = 0xABCD5678;
// const uint32_t SSTABLE_VERSION = 3; // 版本3：Bloom Filter 在文件末尾

// // 文件头结构
// struct SSTableHeader {
//     uint32_t magic;
//     uint32_t version;
//     uint64_t bloom_offset;  // Bloom Filter 数据起始位置（在文件末尾）
//     uint64_t record_offset;  // 第一条 Record 的起始位置
//     uint32_t reserved[4];    // 预留空间便于后续扩展
// };

SSTable::SSTable(const string& path) : filename(path), fd(-1) {
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        std::cerr << "[SSTable] Failed to open file: " << path << std::endl;
        return;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        std::cerr << "[SSTable] Failed to fstat" << std::endl;
        return;
    }

    if (st.st_size == 0) {
        // 新文件，写入文件头
        SSTableHeader header;
        header.magic = SSTABLE_MAGIC;
        header.version = SSTABLE_VERSION;
        header.bloom_offset = 0;    // 初始为0，表示还没有 Bloom Filter
        header.record_offset = sizeof(header);  // Record 紧跟在 Header 之后
        
        if (write(fd, &header, sizeof(header)) != sizeof(header)) {
            std::cerr << "[SSTable] Failed to write header" << std::endl;
        }
        fsync(fd);
    } else {
        // 已有文件，解析索引和 Bloom Filter
        buildIndex();
        readBloomFilter();
    }
}

SSTable::~SSTable() {
    if (fd >= 0) {
        close();
    }
}

void SSTable::buildBloomFilter() {
    if (keys.empty())
        return;

    // 创建 Bloom Filter，每个 key 使用 10 位
    bloom_filter = std::make_unique<BloomFilter>(keys.size() * 10);

    // 添加所有 key
    for (const auto& key : keys) {
        bloom_filter->add(key);
    }

    // 写入到文件末尾
    writeBloomFilter();
}

void SSTable::writeBloomFilter() {
    if (!bloom_filter)
        return;

    // 序列化 Bloom Filter
    string bloom_data = bloom_filter->serialize();

    // 移动到文件末尾
    uint64_t bloom_offset = lseek(fd, 0, SEEK_END);
    if (bloom_offset == (uint64_t)-1) {
        std::cerr << "[SSTable] Failed to seek to end of file" << std::endl;
        return;
    }

    // Bloom Filter 数据格式：len(4B) + data
    uint32_t data_size = bloom_data.size();
    
    // 写入长度
    if (write(fd, &data_size, 4) != 4) {
        std::cerr << "[SSTable] Failed to write bloom filter size" << std::endl;
        return;
    }
    
    // 写入数据
    if (write(fd, bloom_data.data(), data_size) != (ssize_t)data_size) {
        std::cerr << "[SSTable] Failed to write bloom filter data" << std::endl;
        return;
    }
    
    // 更新文件头中的 bloom_offset
    SSTableHeader header;
    if (pread(fd, &header, sizeof(header), 0) != sizeof(header)) {
        std::cerr << "[SSTable] Failed to read header" << std::endl;
        return;
    }
    
    header.bloom_offset = bloom_offset;
    
    if (pwrite(fd, &header, sizeof(header), 0) != sizeof(header)) {
        std::cerr << "[SSTable] Failed to update header" << std::endl;
        return;
    }
    
    fsync(fd);
}

void SSTable::readBloomFilter() {
    // 读取文件头
    SSTableHeader header;
    if (pread(fd, &header, sizeof(header), 0) != sizeof(header))
        return;

    // 检查是否有 Bloom Filter
    if (header.bloom_offset == 0 || header.version < 3)
        return;

    // 读取 Bloom Filter 数据长度
    uint32_t data_size;
    if (pread(fd, &data_size, 4, header.bloom_offset) != 4)
        return;

    // 检查数据大小是否合理
    if (data_size == 0 || data_size > 1024 * 1024)  // 最大 1MB
        return;

    // 读取 Bloom Filter 数据
    string bloom_data(data_size, '\0');
    if (pread(fd, &bloom_data[0], data_size, header.bloom_offset + 4) != (ssize_t)data_size)
        return;

    // 反序列化
    bloom_filter = std::make_unique<BloomFilter>(0);
    bloom_filter->deserialize(bloom_data);
}

void SSTable::setBloomFilter(std::unique_ptr<BloomFilter> filter) {
    bloom_filter = std::move(filter);
    writeBloomFilter();
}

bool SSTable::mayContain(const string& key) const {
    if (!bloom_filter)
        return true; // 没有 Bloom Filter 时，默认可能存在
    return bloom_filter->may_contain(key);
}

// 构建索引：读取文件，解析每条记录，提取 key 和对应的 offset
void SSTable::buildIndex() {
    // 读取文件头，找到 Record 起始位置
    SSTableHeader header;
    uint64_t record_start = 0;

    if (pread(fd, &header, sizeof(header), 0) == sizeof(header)) {
        if (header.magic == SSTABLE_MAGIC) {
            record_start = header.record_offset;
        }
    }
    
    // 如果 record_offset 无效，使用默认值
    if (record_start == 0) {
        record_start = sizeof(header);
    }

    // 获取文件大小
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return;
    }
    uint64_t file_size = st.st_size;
    
    // 确定记录区域的结束位置
    // 如果有 Bloom Filter，记录区域在 Bloom Filter 之前结束
    uint64_t record_end = file_size;
    if (header.bloom_offset > 0 && header.bloom_offset < file_size) {
        record_end = header.bloom_offset;
    }

    keys.clear();
    offsets.clear();

    uint64_t offset = record_start;
    while (offset < record_end) {
        // 读取记录长度
        uint32_t rec_len;
        ssize_t n = pread(fd, &rec_len, 4, offset);
        if (n != 4) {
            break;
        }

        // 检查记录长度是否合理
        if (rec_len == 0 || rec_len > 1024 * 1024) {
            break;
        }

        // 读取完整记录
        string record(rec_len, '\0');
        n = pread(fd, &record[0], rec_len, offset + 4);
        if (n != static_cast<ssize_t>(rec_len)) {
            break;
        }

        // 解析 key：格式 KeyLen(4) + Key + ValLen(4) + Value
        if (rec_len >= 4) {
            uint32_t key_len;
            memcpy(&key_len, record.data(), 4);

            if (key_len > 0 && key_len <= rec_len - 4) {
                string key = record.substr(4, key_len);
                keys.push_back(key);
                offsets.push_back(offset);
            }
        }

        offset += 4 + rec_len;
    }
}

// 写入数据：将记录数据写入文件，并返回记录的 offset
uint64_t SSTable::writeData(const string& data) {
    // 获取当前 Record 区域结束位置
    SSTableHeader header;
    uint64_t record_end = 0;
    
    if (pread(fd, &header, sizeof(header), 0) == sizeof(header)) {
        if (header.bloom_offset > 0) {
            record_end = header.bloom_offset;
        }
    }
    
    uint64_t offset;
    
    if (record_end > 0) {
        // 如果有 Bloom Filter，需要在 Bloom Filter 之前插入
        // 简单实现：移动到 record_end 位置（会覆盖 Bloom Filter）
        offset = lseek(fd, record_end, SEEK_SET);
        if (offset == (uint64_t)-1) {
            return 0;
        }
    } else {
        // 没有 Bloom Filter，移动到文件末尾
        offset = lseek(fd, 0, SEEK_END);
        if (offset == (uint64_t)-1) {
            return 0;
        }
    }
    
    uint32_t len = data.size();
    
    // 写入记录长度
    ssize_t n = write(fd, &len, 4);
    if (n != 4) return 0;
    
    // 写入记录数据
    n = write(fd, data.data(), data.size());
    if (n != (ssize_t)data.size()) return 0;
    
    // 如果之前有 Bloom Filter，需要重新写入 Bloom Filter
    if (record_end > 0 && bloom_filter) {
        // 保存当前的 Bloom Filter
        auto temp_filter = std::move(bloom_filter);
        bloom_filter = nullptr;
        
        // 重新写入 Bloom Filter（会写到新的文件末尾）
        setBloomFilter(std::move(temp_filter));
    }
    
    return offset;
}

bool SSTable::put(const string& key, const string& value) {
    if (fd < 0) return false;
    
    // 编码：KeyLen(4) + Key + ValLen(4) + Value
    string data;
    uint32_t key_len = key.size();
    uint32_t val_len = value.size();

    data.append(reinterpret_cast<char*>(&key_len), 4);
    data.append(key);
    data.append(reinterpret_cast<char*>(&val_len), 4);
    data.append(value);

    uint64_t offset = writeData(data);
    if (offset == 0) return false;
    
    keys.push_back(key);
    offsets.push_back(offset);

    return true;
}

bool SSTable::get(const string& key, string& value) const {
    if (fd < 0) return false;
    
    // 先用 Bloom Filter 过滤
    if (!mayContain(key)) {
        return false;  // 一定不存在
    }
    
    // 二分查找 key
    auto it = std::lower_bound(keys.begin(), keys.end(), key, NaturalLess());
    if (it == keys.end() || *it != key) {
        return false;
    }
    
    size_t idx = it - keys.begin();
    uint64_t offset = offsets[idx];
    
    // 读取记录长度
    uint32_t rec_len;
    ssize_t n = pread(fd, &rec_len, 4, offset);
    if (n != 4) return false;
    
    // 读取记录
    string record(rec_len, '\0');
    n = pread(fd, &record[0], rec_len, offset + 4);
    if (n != rec_len) return false;
    
    // 解析 value
    if (record.size() < 8) return false;
    
    uint32_t key_len;
    memcpy(&key_len, record.data(), 4);
    
    if (record.size() < 4 + key_len + 4) return false;
    
    uint32_t val_len;
    memcpy(&val_len, record.data() + 4 + key_len, 4);
    
    if (record.size() < 4 + key_len + 4 + val_len) return false;
    
    value = record.substr(4 + key_len + 4, val_len);
    return true;
}

bool SSTable::del(const string& key) {
    // 删除标记：写入一个特殊的 tombstone
    return put(key, "__DELETED__");
}

// 从 MemTable 构建 SSTable
SSTable* SSTable::createFromMemTable(const string& path, std::map<string, string>& data) {
    SSTable* sstable = new SSTable(path);
    
    // 写入所有数据
    for (const auto& kv : data) {
        if (kv.second != "__DELETED__") {
            sstable->put(kv.first, kv.second);
        }
    }
    
    // 构建 Bloom Filter（会自动写入文件末尾）
    sstable->buildBloomFilter();
    
    return sstable;
}

// ============== Iterator 实现 ==============
SSTable::Iterator::Iterator(const SSTable* table, size_t idx) 
    : sstable(table), current_idx(idx) {}

bool SSTable::Iterator::valid() const { 
    return sstable && current_idx < sstable->keys.size(); 
}

void SSTable::Iterator::next() { 
    if (valid()) current_idx++; 
}

void SSTable::Iterator::prev() {
    if (current_idx > 0) {
        current_idx--;
    } else if (current_idx == 0 && valid()) {
        // 已经达到第一个元素，再往前就设置为无效
        current_idx = sstable->keys.size();
    }
}

void SSTable::Iterator::seek_to_first() {
    current_idx = 0;
}

void SSTable::Iterator::seek_to_last() {
    current_idx = sstable->keys.empty() ? 0 : sstable->keys.size() - 1;
}

void SSTable::Iterator::seek(const string& target) {
    auto it = std::lower_bound(sstable->keys.begin(), sstable->keys.end(), target);
    current_idx = it - sstable->keys.begin();
}

string SSTable::Iterator::key() const {
    if (!valid()) return "";
    return sstable->keys[current_idx];
}

string SSTable::Iterator::value() const {
    if (!valid()) return "";
    string value;
    sstable->get(sstable->keys[current_idx], value);
    return value;
}

uint64_t SSTable::Iterator::offset() const {
    if (!valid()) return 0;
    return sstable->offsets[current_idx];
}

// ============== 迭代器获取 ==============
SSTable::Iterator SSTable::begin() const {
    return Iterator(this, 0);
}

SSTable::Iterator SSTable::end() const {
    return Iterator(this, keys.size());
}

SSTable::Iterator SSTable::find(const string& key) const {
    Iterator it = begin();
    it.seek(key);
    if (it.valid() && it.key() == key) {
        return it;
    }
    return end();
}

// ============== 文件管理 ==============
void SSTable::close() {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

bool SSTable::delete_file() {
    close();
    if (filename.empty()) return false;
    return unlink(filename.c_str()) == 0;
}

} // namespace kvstore