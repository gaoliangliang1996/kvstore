/**
 * @file client.cpp
 * @brief KVStore gRPC 客户端测试程序。
 *
 * 本文件实现了一个简单的 KVStore 客户端测试程序，用于：
 * - 连接到 KVStore gRPC 服务器
 * - 执行基本的键值操作（PUT/GET）
 * - 演示事务的使用
 *
 * 客户端通过 gRPC 调用远程服务器提供的键值存储服务。
 */

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

/**
 * @brief 主函数，演示 KVStore 客户端的基本操作。
 *
 * 程序执行流程：
 * 1. 建立到服务器的 gRPC 连接
 * 2. 执行简单的 PUT 和 GET 操作
 * 3. 演示事务操作（开始事务、PUT、提交事务）
 *
 * 这个程序展示了客户端如何使用 KVStore 服务进行键值操作和事务处理。
 *
 * @return 程序退出码，0表示正常退出。
 */
int main() {
    // 创建到服务器的 gRPC 通道，使用不安全连接（开发环境）
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
    // 创建服务存根，用于调用远程方法
    std::unique_ptr<kvstore::KVStoreService::Stub> stub = kvstore::KVStoreService::NewStub(channel);
    
    // ===== 基本 PUT 操作 =====
    // 创建 PUT 请求，设置键值对
    kvstore::PutRequest put_req;
    put_req.set_key("name");
    put_req.set_value("Alice");
    
    kvstore::PutResponse put_resp;
    grpc::ClientContext context;
    // 调用远程 PUT 方法
    stub->Put(&context, put_req, &put_resp);
    
    // ===== 基本 GET 操作 =====
    // 创建 GET 请求，查询键的值
    kvstore::GetRequest get_req;
    get_req.set_key("name");
    
    kvstore::GetResponse get_resp;
    // 调用远程 GET 方法
    stub->Get(&context, get_req, &get_resp);
    
    // 检查 GET 操作是否成功并输出结果
    if (get_resp.success()) {
        std::cout << "name = " << get_resp.value() << std::endl;
    }
    
    // // ===== 事务操作演示 =====
    // // 开始新事务
    // kvstore::BeginTxnRequest begin_req;
    // kvstore::BeginTxnResponse begin_resp;
    // stub->BeginTransaction(&context, begin_req, &begin_resp);
    
    // // 获取事务ID，用于后续操作
    // uint64_t txn_id = begin_resp.txn_id();
    
    // // 在事务中执行 PUT 操作
    // kvstore::PutRequest txn_put_req;
    // txn_put_req.set_key("age");
    // txn_put_req.set_value("25");
    // txn_put_req.set_txn_id(txn_id);  // 指定事务ID
    // stub->Put(&context, txn_put_req, &put_resp);
    
    // // 提交事务
    // kvstore::CommitTxnRequest commit_req;
    // commit_req.set_txn_id(txn_id);
    // kvstore::CommitTxnResponse commit_resp;
    // stub->CommitTransaction(&context, commit_req, &commit_resp);
    
    return 0;
}