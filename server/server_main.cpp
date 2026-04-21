// server/server_main.cpp
#include "mvcc_kvstore.h"
#include "server.h"
#include <iostream>
#include <signal.h>
#include <atomic>
#include <thread>

std::atomic<bool> running(true);

void signal_handler(int signum) {
    std::cout << "\nReceived signal " << signum << ", shutting down..." << std::endl;
    running = false;
}

int main(int argc, char* argv[]) {
    std::string listen_addr = "0.0.0.0:50051";
    std::string data_dir = "./data";
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--addr" && i + 1 < argc) {
            listen_addr = argv[++i];
        } else if (arg == "--data" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "  --addr <addr>  Listen address (default: 0.0.0.0:50051)\n"
                      << "  --data <dir>   Data directory (default: ./data)\n"
                      << "  --help         Show this help\n";
            return 0;
        }
    }
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::cout << "KVStore Server starting..." << std::endl;
    std::cout << "  Listen address: " << listen_addr << std::endl;
    std::cout << "  Data directory: " << data_dir << std::endl;
    
    // 创建存储引擎
    kvstore::Config cfg;
    cfg.data_dir = data_dir;
    cfg.memtable_size = 64 * 1024 * 1024;  // 64MB
    
    auto store = std::make_shared<kvstore::MVCCKVStore>(cfg);
    
    // 创建并启动服务器
    kvstore::KVServer server(listen_addr, store);
    server.Start();
    
    std::cout << "Server started successfully!" << std::endl;
    
    // 等待停止信号
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    server.Shutdown();
    std::cout << "Server shutdown complete" << std::endl;
    
    return 0;
}