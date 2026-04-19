/**
 * @file server.cpp
 * @brief KVStore gRPC 服务器测试程序。
 *
 * 本文件实现了一个简单的 KVStore 服务器测试程序，用于：
 * - 初始化 MVCCKVStore 存储引擎
 * - 启动 gRPC 服务
 * - 监听客户端请求
 *
 * 服务器提供键值存储的网络接口，支持远程客户端的读写操作。
 */

#include "mvcc_kvstore.h"
#include "grpc_service.h"

/**
 * @brief 主函数，启动 KVStore gRPC 服务器。
 *
 * 程序执行流程：
 * 1. 配置存储引擎参数（数据目录、内存表大小等）
 * 2. 初始化 MVCCKVStore 实例
 * 3. 创建并启动 gRPC 服务器
 * 4. 等待服务器运行，处理客户端请求
 *
 * @return 程序退出码，0表示正常退出。
 */
int main() {
    // 配置存储引擎参数
    kvstore::Config cfg;
    cfg.data_dir = "./data";                    // 数据存储目录
    cfg.memtable_size = 64 * 1024 * 1024;       // 内存表大小：64MB
    
    // 初始化 MVCC 键值存储引擎
    kvstore::MVCCKVStore store(cfg);
    
    // 创建 gRPC 服务器，监听地址 0.0.0.0:50051
    kvstore::GRPCServer server(&store, "0.0.0.0:50051");
    
    // 启动服务器
    server.start();
    std::cout << "KVStore server running on 0.0.0.0:50051" << std::endl;
    
    // 等待服务器运行，阻塞直到收到停止信号
    server.wait(); // 服务器运行，处理客户端请求
    
    return 0;
}