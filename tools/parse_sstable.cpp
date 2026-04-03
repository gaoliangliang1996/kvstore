#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <cstdint>

using namespace std;

// 解析 SSTable 文件
void parse_sstable(const string& filename) {
    cout << "\n========== Parsing SSTable: " << filename << " ==========" << endl;
    
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
    cout << "\nRecords:" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << left << setw(10) << "Offset" 
         << setw(10) << "KeyLen" 
         << setw(20) << "Key" 
         << setw(10) << "ValLen" 
         << "Value" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    
    uint64_t offset = 0;
    int record_count = 0;
    
    while (offset < file_size) {
        // 读取记录长度
        uint32_t rec_len;
        ssize_t n = pread(fd, &rec_len, 4, offset);
        if (n != 4) break;
        
        if (rec_len == 0 || rec_len > 1024 * 1024) {
            cerr << "Invalid record length at offset " << offset << endl;
            break;
        }
        
        // 读取记录数据
        string record(rec_len, '\0');
        n = pread(fd, &record[0], rec_len, offset + 4);
        if (n != (ssize_t)rec_len) break;
        
        // 解析 key
        if (rec_len >= 4) {
            uint32_t key_len;
            memcpy(&key_len, record.data(), 4);
            
            if (key_len > 0 && key_len <= rec_len - 4) {
                string key = record.substr(4, key_len);
                
                // 解析 value
                if (rec_len >= 4 + key_len + 4) {
                    uint32_t val_len;
                    memcpy(&val_len, record.data() + 4 + key_len, 4);
                    
                    if (rec_len >= 4 + key_len + 4 + val_len) {
                        string value = record.substr(4 + key_len + 4, val_len);
                        
                        // 打印记录
                        cout << left << setw(10) << offset 
                             << setw(10) << key_len 
                             << setw(20) << (key.length() > 18 ? key.substr(0, 18) + "..." : key)
                             << setw(10) << val_len;
                        
                        // 处理特殊标记
                        if (value == "__DELETED__") {
                            cout << "[TOMBSTONE - Deleted]" << endl;
                        } else {
                            string display_value = value;
                            if (value.length() > 50) {
                                display_value = value.substr(0, 47) + "...";
                            }
                            cout << display_value << endl;
                        }
                        record_count++;
                    }
                }
            }
        }
        
        offset += 4 + rec_len;
    }
    
    close(fd);
    
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << "Total records: " << record_count << endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <sstable_file> [sstable_file2...]" << endl;
        cerr << "Example: " << argv[0] << " 0.sst 1.sst" << endl;
        return 1;
    }
    
    for (int i = 1; i < argc; i++) {
        parse_sstable(argv[i]);
    }
    
    return 0;
}