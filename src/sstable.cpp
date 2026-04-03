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

namespace kvstore {

SSTable::SSTable(const string& path) : filename(path) {
    file.open(path, std::ios::binary | std::ios::in | std::ios::out); // 以读写模式打开文件，如果文件不存在则创建
    if (file.is_open()) {
        buildIndex();
    }
}

SSTable::~SSTable() {
    if (file.is_open()) {
        file.close();
    }
}

// 构建索引：读取文件，解析每条记录，提取 key 和对应的 offset
void SSTable::buildIndex() {
    file.seekg(0, std::ios::end); // 移动到文件末尾
    uint64_t file_size = file.tellg(); // 获取文件大小
    file.seekg(0, std::ios::beg); // 移动回文件开头

    uint64_t offset = 0;
    while (offset < file_size) { // 循环读取每条记录，直到文件末尾
        // 读取记录长度
        uint32_t rec_len;
        file.seekg(offset); // 移动到当前记录的起始位置
        file.read(reinterpret_cast<char*>(&rec_len), 4); // 读取记录长度（4 字节）

        if (file.eof())
            break;

        // 读取完整记录
        string record(rec_len, '\0');
        file.read(&record[0], rec_len);

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
    file.seekp(0, std::ios::end); // 移动到文件末尾
    uint64_t offset = file.tellp(); // 获取当前写入位置的 offset

    uint32_t len = data.size();
    file.write(reinterpret_cast<const char*>(&len), 4); // 写入记录长度
    file.write(data.data(), data.size()); // 写入记录数据

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

bool SSTable::get(const string& key, string& value) {
    // 二分查找 key
    auto it = std::lower_bound(keys.begin(), keys.end(), key); // 使用二分查找定位 key 的位置
    if (it == keys.end() || *it != key) {
        return false;
    }
    
    size_t idx = it - keys.begin();
    file.seekg(offsets[idx]); // 移动到对应记录的 offset
    
    uint32_t rec_len;
    file.read(reinterpret_cast<char*>(&rec_len), 4); // 读取记录长度
    
    string record(rec_len, '\0');
    file.read(&record[0], rec_len); // 读取完整记录
    
    // 解析 value
    uint32_t key_len = record.size() > 4 ? *reinterpret_cast<uint32_t*>(&record[0]) : 0; // 解析 KeyLen
    if (record.size() > 4 + key_len + 4) {
        uint32_t val_len = *reinterpret_cast<uint32_t*>(&record[4 + key_len]); // 解析 ValLen
        value = record.substr(4 + key_len + 4, val_len); // 提取 value
        return true;
    }
    
    return false;
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

}