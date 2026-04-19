#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <string>
#include <iomanip>
#include <zlib.h>
#include <cstdint>

using namespace std;

// CRC32 计算
uint32_t calculateCRC(const string& data) {
    return crc32(0L, reinterpret_cast<const Bytef*>(data.data()), data.size());
}

// 解析 WAL 文件
void parse_wal(const string& filename) {
    cout << "\n========== Parsing WAL: " << filename << " ==========" << endl;
    
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        cerr << "Failed to open file: " << filename << endl;
        return;
    }
    
    // 获取文件大小
    struct stat st;
    fstat(fd, &st);
    uint64_t file_size = st.st_size;
    
    cout << "File size: " << file_size << " bytes" << endl;
    
    if (file_size < 8) {
        cerr << "File too small, invalid WAL" << endl;
        close(fd);
        return;
    }
    
    // 读取文件头
    uint32_t magic, version;
    pread(fd, &magic, 4, 0);
    pread(fd, &version, 4, 4);
    
    cout << "Magic: 0x" << hex << magic << dec << endl;
    cout << "Version: " << version << endl;
    
    if (magic != 0xABCD1234) {
        cerr << "Invalid magic number, not a valid WAL file" << endl;
        close(fd);
        return;
    }
    
    cout << "\nRecords:" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << left << setw(10) << "Offset" 
         << setw(8) << "CRC32" 
         << setw(14) << "Type" 
         << setw(10) << "KeyLen" 
         << setw(20) << "Key" 
         << setw(10) << "ValLen" 
         << "Value" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    
    uint64_t offset = 8;  // 跳过文件头
    int record_count = 0;
    int corrupt_count = 0;
    
    while (offset < file_size) {
        // 读取完整记录（先尝试读最小长度）
        if (offset + 13 > file_size) break;  // 最小记录长度
        
        // 读取记录头
        uint32_t crc_stored;
        uint8_t type;
        uint32_t key_len, val_len;
        
        pread(fd, &crc_stored, 4, offset);
        pread(fd, &type, 1, offset + 4);
        pread(fd, &key_len, 4, offset + 5);
        pread(fd, &val_len, 4, offset + 9);
        
        // 检查长度是否合理
        if (key_len > 1024 || val_len > 1024 * 1024) {
            cerr << "Invalid length at offset " << offset << endl;
            break;
        }
        
        // 计算总记录长度
        uint32_t total_len = 13 + key_len + val_len;
        
        if (offset + total_len > file_size) break;
        
        // 读取完整记录
        string record_data(total_len - 4, '\0');  // 不包括 CRC
        pread(fd, &record_data[0], total_len - 4, offset + 4);
        
        // 验证 CRC
        uint32_t crc_calc = calculateCRC(record_data);
        
        // 读取 key 和 value
        string key, value;
        if (key_len > 0) {
            key = record_data.substr(9, key_len);  // 跳过 Type(1)+KeyLen(4)+ValLen(4)
        }
        if (val_len > 0 && 9 + key_len + val_len <= record_data.size()) {
            value = record_data.substr(9 + key_len, val_len);
        }
        
        // 打印记录
        cout << left << setw(10) << offset
             << "0x" << hex << setw(10) << crc_stored << dec
             << setw(10) << (type == 0x01 ? "PUT" : "DEL");
        
        if (crc_stored != crc_calc) {
            cout << " [CRC ERROR!]";
            corrupt_count++;
        }
        
        cout << setw(10) << key_len
             << setw(20) << (key.length() > 18 ? key.substr(0, 18) + "..." : key)
             << setw(10) << val_len;
        
        if (type == 0x01) {
            string display_value = value;
            if (value.length() > 50) {
                display_value = value.substr(0, 47) + "...";
            }
            cout << display_value;
        } else if (type == 0x02) {
            cout << "[DELETE]";
        }
        
        cout << endl;
        
        record_count++;
        offset += total_len;
    }
    
    close(fd);
    
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << "Total records: " << record_count << endl;
    if (corrupt_count > 0) {
        cout << "Corrupt records: " << corrupt_count << endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <wal_file>" << endl;
        cerr << "Example: " << argv[0] << " wal.log" << endl;
        return 1;
    }
    
    parse_wal(argv[1]);
    
    return 0;
}