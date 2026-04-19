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
#include <memory>

using namespace std;

// SSTable 文件格式常量
const uint32_t SSTABLE_MAGIC = 0xABCD5678;
const uint32_t SSTABLE_VERSION = 3;

// SSTable 文件头结构
struct SSTableHeader {
    uint32_t magic;
    uint32_t version;
    uint64_t bloom_offset;   // Bloom Filter 的起始位置
    uint64_t record_offset;  // 第一条 Record 的起始位置
    uint32_t reserved[4];    // 预留空间
};

// Bloom Filter 信息
struct BloomFilterInfo {
    uint32_t data_len;
    uint32_t bits_count;
    uint32_t hash_count;
    string bits_data;
    
    void print() const {
        cout << "  Bits count: " << bits_count << endl;
        cout << "  Hash count: " << hash_count << endl;
        cout << "  Data size: " << data_len << " bytes" << endl;
        cout << "  Bits array size: " << bits_data.size() << " bytes" << endl;
    }
};

// 解析 Bloom Filter
BloomFilterInfo parse_bloom_filter(int fd, uint64_t bloom_offset) {
    BloomFilterInfo info;
    info.data_len = 0;
    info.bits_count = 0;
    info.hash_count = 0;
    
    if (bloom_offset == 0) {
        return info;
    }
    
    // 读取 Bloom Filter 数据长度
    if (pread(fd, &info.data_len, 4, bloom_offset) != 4) {
        cerr << "Failed to read bloom filter data length" << endl;
        return info;
    }
    
    if (info.data_len == 0) {
        return info;
    }
    
    // 读取 Bloom Filter 数据
    string bloom_data(info.data_len, '\0');
    if (pread(fd, &bloom_data[0], info.data_len, bloom_offset + 4) != info.data_len) {
        cerr << "Failed to read bloom filter data" << endl;
        return info;
    }
    
    // 解析 Bloom Filter 数据
    // 格式: bits_count (8 bytes) + hash_count (8 bytes) + bits_data
    if (info.data_len >= 16) {
        memcpy(&info.bits_count, bloom_data.data(), 8);
        memcpy(&info.hash_count, bloom_data.data() + 8, 8);
        if (info.data_len > 16) {
            info.bits_data = bloom_data.substr(16);
        }
    }
    
    return info;
}

// 解析 Record
struct Record {
    uint32_t rec_len;
    uint32_t key_len;
    string key;
    uint32_t val_len;
    string value;
    bool is_tombstone;
    
    void print(uint64_t offset) const {
        cout << left << setw(10) << offset
             << setw(10) << rec_len
             << setw(10) << key_len
             << setw(25) << (key.length() > 22 ? key.substr(0, 22) + "..." : key)
             << setw(10) << val_len;
        
        if (is_tombstone) {
            cout << "[TOMBSTONE - Deleted]";
        } else {
            string display_value = value;
            if (value.length() > 50) {
                display_value = value.substr(0, 47) + "...";
            }
            cout << display_value;
        }
        cout << endl;
    }
};

// 解析 Record
Record parse_record(int fd, uint64_t offset) {
    Record rec;
    rec.rec_len = 0;
    rec.key_len = 0;
    rec.val_len = 0;
    rec.is_tombstone = false;
    
    // 读取记录长度
    if (pread(fd, &rec.rec_len, 4, offset) != 4) {
        return rec;
    }
    
    if (rec.rec_len == 0 || rec.rec_len > 1024 * 1024) {
        return rec;
    }
    
    // 读取记录数据
    string record_data(rec.rec_len, '\0');
    if (pread(fd, &record_data[0], rec.rec_len, offset + 4) != rec.rec_len) {
        return rec;
    }
    
    // 解析 key
    if (rec.rec_len >= 4) {
        memcpy(&rec.key_len, record_data.data(), 4);
        
        if (rec.key_len > 0 && rec.key_len <= rec.rec_len - 4) {
            rec.key = record_data.substr(4, rec.key_len);
            
            // 解析 value
            if (rec.rec_len >= 4 + rec.key_len + 4) {
                memcpy(&rec.val_len, record_data.data() + 4 + rec.key_len, 4);
                
                if (rec.rec_len >= 4 + rec.key_len + 4 + rec.val_len) {
                    rec.value = record_data.substr(4 + rec.key_len + 4, rec.val_len);
                    rec.is_tombstone = (rec.value == "__DELETED__");
                }
            }
        }
    }
    
    return rec;
}

// 打印文件头信息
void print_header(const SSTableHeader& header) {
    cout << "File Header:" << endl;
    cout << "  Magic: 0x" << hex << header.magic << dec << endl;
    cout << "  Version: " << header.version << endl;
    cout << "  Bloom offset: " << header.bloom_offset << endl;
    cout << "  Record offset: " << header.record_offset << endl;
}

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
    if (fstat(fd, &st) != 0) {
        cerr << "Failed to get file stats" << endl;
        close(fd);
        return;
    }
    uint64_t file_size = st.st_size;
    
    cout << "File size: " << file_size << " bytes" << endl;
    cout << endl;
    
    // 读取文件头
    SSTableHeader header;
    if (pread(fd, &header, sizeof(header), 0) != sizeof(header)) {
        cerr << "Failed to read file header" << endl;
        close(fd);
        return;
    }
    
    // 验证 magic number
    if (header.magic != SSTABLE_MAGIC) {
        cerr << "Invalid magic number: 0x" << hex << header.magic << dec << endl;
        cerr << "This may not be a valid SSTable file" << endl;
        close(fd);
        return;
    }
    
    print_header(header);
    cout << endl;
    
    // 解析 Bloom Filter
    if (header.bloom_offset > 0 && header.bloom_offset < file_size) {
        cout << "Bloom Filter:" << endl;
        auto bloom_info = parse_bloom_filter(fd, header.bloom_offset);
        if (bloom_info.data_len > 0) {
            bloom_info.print();
        } else {
            cout << "  No Bloom Filter data" << endl;
        }
        cout << endl;
    }
    
    // 确定记录区域的结束位置
    uint64_t record_start = header.record_offset;
    uint64_t record_end = file_size;
    
    // 如果有 Bloom Filter，记录区域在 Bloom Filter 之前结束
    if (header.bloom_offset > 0 && header.bloom_offset < file_size) {
        record_end = header.bloom_offset;
    }
    
    cout << "Records:" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << left << setw(10) << "Offset"
         << setw(10) << "RecLen"
         << setw(10) << "KeyLen"
         << setw(25) << "Key"
         << setw(10) << "ValLen"
         << "Value" << endl;
    cout << "-------------------------------------------------------------------------------" << endl;
    
    uint64_t offset = record_start;
    int record_count = 0;
    int tombstone_count = 0;
    
    while (offset < record_end) {
        Record rec = parse_record(fd, offset);
        if (rec.rec_len == 0) {
            break;
        }
        
        rec.print(offset);
        
        if (rec.is_tombstone) {
            tombstone_count++;
        }
        record_count++;
        
        offset += 4 + rec.rec_len;
    }
    
    cout << "-------------------------------------------------------------------------------" << endl;
    cout << "Total records: " << record_count << endl;
    cout << "  - Active records: " << (record_count - tombstone_count) << endl;
    cout << "  - Tombstones: " << tombstone_count << endl;
    
    close(fd);
}

// 主函数
int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <sstable_file> [sstable_file2...]" << endl;
        cerr << "Example: " << argv[0] << " sst_0.sst sst_1.sst" << endl;
        cerr << "         " << argv[0] << " data/*.sst" << endl;
        return 1;
    }
    
    for (int i = 1; i < argc; i++) {
        parse_sstable(argv[i]);
    }
    
    return 0;
}