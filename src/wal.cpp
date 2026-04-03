#include "wal.h"
#include <unistd.h>
#include <cstring>
#include <zlib.h> // 用于 CRC32 计算
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// 编码格式：CRC32(4) + Type(1) + KeyLen(4) + ValLen(4) + Key + Value
namespace kvstore {

WAL::WAL(const string& path) : filename(path) {
    file.open(path, std::ios::binary | std::ios::in | std::ios::out | std::ios::app); // 以读写模式打开文件，如果文件不存在则创建

    if (!file.is_open()) { // 如果文件打开失败，尝试创建文件
        file.open(path, std::ios::binary | std::ios::out); // 创建文件
        file.close(); // 关闭后再以读写模式打开
        file.open(path, std::ios::binary | std::ios::in | std::ios::out | std::ios::app); // 以读写模式打开文件
    }
}

WAL::~WAL() {
    if (file.is_open()) {
        sync();
        file.close();
    }
}

string WAL::encodeRecord(const Record& rec) {
    string result;
    
    // 预留 CRC 空间
    result.append(4, '\0');

    // Type
    result.push_back(static_cast<char>(rec.type));

    // KeyLen
    uint32_t key_len = rec.key.size();
    result.append(reinterpret_cast<char*>(&key_len), 4);

    // ValLen
    uint32_t val_len = rec.value.size();
    result.append(reinterpret_cast<char*>(&val_len), 4);

    // key + value
    result.append(rec.key);
    result.append(rec.value);

    // 计算并写入 CRC
    uint32_t crc = calculateCRC(result.substr(4));
    memcpy(&result[0], &crc, 4);

    return result;
}

bool WAL::decodeRecord(const string& data, Record& rec) {
    if (data.size() < 13)
        return false;

    uint32_t stored_crc;
    memcpy(&stored_crc, &data[0], 4);

    uint32_t calc_crc = calculateCRC(data.substr(4));
    if (stored_crc != calc_crc)
        return false;

    rec.type = static_cast<Optype>(data[4]);

    uint32_t key_len, val_len;
    memcpy(&key_len, &data[5], 4);
    memcpy(&val_len, &data[9], 4);

    if (data.size() < 13 + key_len + val_len)
        return false;

    rec.key = data.substr(13, key_len);
    rec.value = data.substr(13 + key_len, val_len);

    return true;
}

uint32_t WAL::calculateCRC(const string& data) {
    return crc32(0L, reinterpret_cast<const Bytef*>(data.data()), data.size()); // 这里使用 zlib 的 crc32 函数计算 CRC
}


bool WAL::append(const Record& rec) {
    string encoded = encodeRecord(rec);
    file.write(encoded.data(), encoded.size());
    return file.good(); // 检查写入是否成功
}

bool WAL::recover(std::function<bool (const Record&)> callback) {
    file.seekg(0, std::ios::beg); // 从文件开头开始读取

    string buffer;
    char ch;
    while (file.get(ch)) {
        buffer.push_back(ch);

        Record rec;
        if (decodeRecord(buffer, rec)) {
            if (!callback(rec)) {
                return false;
            }
            buffer.clear();
        }
    }

    return true;
}

void WAL::sync() {
    file.flush();
    
    // 获取文件描述符的正确方法
    int fd = -1;
    
    // 方法1：通过标准库的 native_handle （如果可用）
    // 方法2：重新打开文件获取 fd
    // 这里使用最简单的方法：关闭并重新打开来强制同步
    
    // 实际上，file.rdbuf()->pubsync() 已经做了同步
    // 但为了确保数据真正落盘，我们可以用更直接的方法
    
    // 获取当前文件路径，重新打开获取 fd
    file.close();
    
    // 以只读方式打开只是为了获取 fd 并调用 fsync
    int temp_fd = open(filename.c_str(), O_RDONLY);
    if (temp_fd >= 0) {
        fsync(temp_fd);
        close(temp_fd);
    }
    
    // 重新打开文件
    file.open(filename, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
}

// 截断日志文件，通常在 checkpoint 后调用
void WAL::truncate() {
    file.close(); // 关闭文件
    file.open(filename, std::ios::binary | std::ios::out | std::ios::trunc); // 以截断模式打开文件，清空内容
    file.close(); // 关闭后再以读写模式打开
    file.open(filename, std::ios::binary  | std::ios::in | std::ios::out | std::ios::app); // 以读写模式打开文件
}


}