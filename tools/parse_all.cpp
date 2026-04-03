#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <algorithm>

using namespace std;
namespace fs = std::filesystem;

// 声明外部函数
void parse_sstable(const string& filename);
void parse_wal(const string& filename);

int main(int argc, char* argv[]) {
    string data_dir = "./test_data";
    
    if (argc >= 2) {
        data_dir = argv[1];
    }
    
    cout << "Parsing KVStore data in directory: " << data_dir << endl;
    
    // 解析 WAL 文件
    string wal_path = data_dir + "/wal.log";
    if (fs::exists(wal_path)) {
        parse_wal(wal_path);
    } else {
        cout << "WAL file not found: " << wal_path << endl;
    }
    
    // 解析所有 SSTable 文件
    vector<string> sst_files;
    for (const auto& entry : fs::directory_iterator(data_dir)) {
        string path = entry.path().string();
        if (path.find(".sst") != string::npos) {
            sst_files.push_back(path);
        }
    }
    
    sort(sst_files.begin(), sst_files.end());
    
    for (const auto& file : sst_files) {
        parse_sstable(file);
    }
    
    return 0;
}