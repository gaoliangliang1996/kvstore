#include "wal.h"
#include <unistd.h>
#include <cstring>
#include <zlib.h> // 用于 CRC32 计算
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

// 编码格式：CRC32(4) + Type(1) + KeyLen(4) + ValLen(4) + Key + Value
namespace kvstore {

WAL::WAL(const string& path) : fd(-1), filename(path), offset(0) {
    fd = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);

    if (fd < 0) {
        std::cerr << "Failed to open wal file" << path << std::endl;
        return;
    }

    // 检查文件是否为空 （新文件）
    struct stat st;
    if (fstat(fd, &st) == 0 && st.st_size == 0) {
        // 写入文件头 magic(4) + version(4)
        uint32_t magic = 0xABCD1234;
        uint32_t version = 1;
        write(fd, &magic, 4);
        write(fd, &version, 4);
        fsync(fd);
        offset = 0;
    }
    else {
        // 获取当前大小
        offset = lseek(fd, 0, SEEK_END);
        std::cout << "[WAL] opened " << path << " , fd = " << fd << " , size = " << offset << std::endl;
    }
}


WAL::~WAL() {
    if (fd > 0) {
        sync();
        close(fd);
        std::cout << "[WAL] closed" << filename << std::endl;
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

    ssize_t written = write(fd, encoded.data(), encoded.size());
    if (written != static_cast<ssize_t>(encoded.size())) {
        std::cerr << "[WAL] write failed" << std::endl;
        return false;
    }

    offset += written;
    return true;
}

bool WAL::recover(std::function<bool (const Record&)> callback) {
    lseek(fd, 8, SEEK_SET); // 跳过文件头

    string buffer;
    char ch;
    ssize_t n;

    while ((n = read(fd, &ch, 1) > 0)) { // 为什么每次只读一字节？因为我们不知道每条记录的长度，必须逐字节读取直到能成功解析出一条完整的记录
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
    if (fd >= 0) {
        fsync(fd);
        std::cout << "[WAL] synced" << std::endl;
    }
}

void WAL::truncate() {
    if (fd >= 0) {
        ftruncate(fd, 8); // 截断文件
        lseek(fd, 8, SEEK_SET); // 移动文件指针到开头
        offset = 8;
        sync();
    }
}


}