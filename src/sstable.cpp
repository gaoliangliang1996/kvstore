// 此类的作用是实现一个简单的 SSTable（Sorted String Table）类，用于存储和管理键值对数据。SSTable 是一种不可变的数据结构，通常用于持久化存储大量数据，并且支持高效的读取操作。该类提供了以下功能：
// 1. 构造函数和析构函数：用于打开和关闭 SSTable 文件。
// 2. put 方法：将一个键值对写入 SSTable 中。
// 3. get 方法：根据键从 SSTable 中读取对应的值。
// 4. del 方法：从 SSTable 中删除一个键值对。
// 5. createFromMemTable 静态方法：从一个内存中的键值对集合（MemTable）创建一个新的 SSTable 实例。

// 格式：
// 每条记录的格式为：KeyLen(4) + Key + ValLen(4) + Value
// 索引：在内存中维护一个索引，记录每个 key 对应的 offset，便于快速查找。

#include <sstable.h>
#include <cstring>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>


namespace kvstore {

SSTable::SSTable(const string& path) : filename(path) {
    fd = open(path.c_str(), O_RDWR | O_CREAT, 0644); // 以读写模式打开文件，如果文件不存在则创建
    if (fd >= 0) {
        buildIndex();
    }
}

SSTable::~SSTable() {
    if (fd >= 0) {
        close(fd); // 关闭文件描述符
    }
}

// 构建索引：读取文件，解析每条记录，提取 key 和对应的 offset
void SSTable::buildIndex() {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return;
    }
    uint64_t file_size = st.st_size;

    uint64_t offset = 0;
    while (offset < file_size) { // 循环读取每条记录，直到文件末尾
        // 读取记录长度
        uint32_t rec_len;
        ssize_t n = pread(fd, &rec_len, 4, offset); // 直接使用 pread 从指定 offset 位置读取数据
        if (n != 4) {
            break; // 读取失败或文件末尾
        }

        if (rec_len == 0 || rec_len > 1024 * 1024) {
            break; // 记录长度为 0，可能是文件末尾
        }

        // 读取完整记录
        string record(rec_len, '\0');
        n = pread(fd, &record[0], rec_len, offset + 4);
        if (n != static_cast<ssize_t>(rec_len)) {
            break; // 读取失败
        }

        // 解析 key
        uint32_t key_len;
        memcpy(&key_len, record.data(), 4);

        string key = record.substr(4, key_len);

        keys.push_back(key);
        offsets.push_back(offset);

        offset += 4 + rec_len;
    }
}

// 写入数据：将记录数据写入文件，并返回记录的 offset
// 文件中的格式为：len + data
uint64_t SSTable::writeData(const string& data) {
    // 移动到文件末尾
    uint64_t offset = lseek(fd, 0, SEEK_END);
    if (offset == (uint64_t)-1) { // lseek 失败
        return 0;
    }
    
    uint32_t len = data.size();
    
    // 写入记录长度
    ssize_t n = write(fd, &len, 4);
    if (n != 4) return 0;
    
    // 写入记录数据
    n = write(fd, data.data(), data.size());
    if (n != (ssize_t)data.size()) return 0;
    
    return offset;
}

bool SSTable::put(const string& key, const string& value) {
    // 编码：KeyLen(4) + Key + ValLen(4) + Value
    string data;
    uint32_t key_len = key.size();
    uint32_t val_len = value.size();

    data.append(reinterpret_cast<char*>(&key_len), 4);
    data.append(key);
    data.append(reinterpret_cast<char*>(&val_len), 4);
    data.append(value);

    uint64_t offset = writeData(data); // 写入数据并获取 offset
    keys.push_back(key);
    offsets.push_back(offset); // key 和 offset 的索引是对应的，便于后续查找

    return true;
}

bool SSTable::get(const string& key, string& value) const {
    if (fd < 0) return false;
    
    // 二分查找 key
    auto it = std::lower_bound(keys.begin(), keys.end(), key);
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
// 参数 path：SSTable 文件的路径
// 参数 data：内存中的键值对数据
SSTable* SSTable::createFromMemTable(const string& path, std::map<string, string>& data) {
    SSTable* sstable = new SSTable(path);
    for (const auto& kv : data) {
        sstable->put(kv.first, kv.second);
    }
    return sstable;
}

// ============== Iterator 实现 ==============
SSTable::Iterator::Iterator(const SSTable* table, size_t idx) : sstable(table), current_idx(idx) {}

bool SSTable::Iterator::valid() const { return sstable && current_idx < sstable->keys.size(); }

void SSTable::Iterator::next() { if (valid()) current_idx++; }

void SSTable::Iterator::prev() {
    if (current_idx > 0)
        current_idx--;
    else {
        // 已经达到第一个元素，再往前就设置为无效
        current_idx = sstable->keys.size(); // 设置为 end() 位置
    }
}

void SSTable::Iterator::seek_to_first() {
    current_idx = 0;
}

void SSTable::Iterator::seek_to_last() {
    current_idx = sstable->keys.empty() ? 0 : sstable->keys.size() - 1;
}

void SSTable::Iterator::seek(const string& target) {
    // 二分查找第一个 >= target 的位置
    auto it = std::lower_bound(sstable->keys.begin(), sstable->keys.end(), target);
    current_idx = it - sstable->keys.begin();
}

string SSTable::Iterator::key() const {
    if (!valid())
        return "";

    return sstable->keys[current_idx];
}

string SSTable::Iterator::value() const {
    if (!valid())
        return "";

    string value;
    sstable->get(sstable->keys[current_idx], value);
    return value;
}

uint64_t SSTable::Iterator::offset() const {
    if (!valid())
        return 0;

    return sstable->offsets[current_idx];
}

// ============== 迭代器获取 ==============

SSTable::Iterator SSTable::begin() const {
    return Iterator(this, 0);
}
SSTable::Iterator SSTable::end() const {
    return Iterator(this, keys.size());
}
SSTable:: Iterator SSTable::find(const string& key) const {
    Iterator it = begin();
    it.seek(key);
    if (it.valid() && it.key() == key) {
        return it;
    }
    return end();
}


}