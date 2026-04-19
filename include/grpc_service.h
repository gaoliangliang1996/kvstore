#pragma once
/**
 * @file grpc_service.h
 * @brief 键值存储的 gRPC 服务实现。
 *
 * 此文件定义了提供对键值存储操作远程访问的 gRPC 服务类，
 * 包括基本的 CRUD 操作和事务管理。
 */

#include "mvcc_kvstore.h"
#include "transaction.h"
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <unordered_map>
#include <mutex>

namespace kvstore {

/**
 * @class KVStoreServiceImpl
 * @brief KVStore gRPC 服务的实现。
 *
 * 此类实现了 kvstore.proto 中定义的 gRPC 服务接口。
 * 它处理传入的 gRPC 请求并将其委托给底层的 MVCC 键值存储，
 * 管理事务和并发性。
 */
class KVStoreServiceImpl final : public KVStoreService::Service {
private:
    MVCCKVStore* store;  ///< 底层键值存储的指针
    std::unordered_map<uint64_t, std::unique_ptr<Transaction>> active_transactions;  // 活跃事务映射：事务 ID -> 事务对象
    std::mutex txn_mutex;  ///< 事务管理的互斥锁
    
public:
    /**
     * @brief KVStoreServiceImpl 的构造函数。
     * @param kvstore MVCC 键值存储实例的指针。
     */
    explicit KVStoreServiceImpl(MVCCKVStore* kvstore);
    
    /**
     * @brief 处理存储键值对的 PUT 请求。
     * @param context gRPC 服务器上下文。
     * @param request 包含键和值的 PUT 请求。
     * @param response 要发送回的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status Put(grpc::ServerContext* context,
                     const PutRequest* request,
                     PutResponse* response) override;
    
    /**
     * @brief 处理通过键检索值的 GET 请求。
     * @param context gRPC 服务器上下文。
     * @param request 包含键的 GET 请求。
     * @param response 包含检索值的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status Get(grpc::ServerContext* context,
                     const GetRequest* request,
                     GetResponse* response) override;
    
    /**
     * @brief 处理删除键值对的 DELETE 请求。
     * @param context gRPC 服务器上下文。
     * @param request 包含键的 DELETE 请求。
     * @param response 要发送回的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status Delete(grpc::ServerContext* context,
                        const DeleteRequest* request,
                        DeleteResponse* response) override;
    
    /**
     * @brief 处理检索键值对范围的 SCAN 请求。
     * @param context gRPC 服务器上下文。
     * @param request 带有范围参数的 SCAN 请求。
     * @param response 包含扫描结果的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status Scan(grpc::ServerContext* context,
                      const ScanRequest* request,
                      ScanResponse* response) override;
    
    /**
     * @brief 处理事务开始请求。
     * @param context gRPC 服务器上下文。
     * @param request 开始事务请求。
     * @param response 包含事务 ID 的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status BeginTransaction(grpc::ServerContext* context,
                                  const BeginTxnRequest* request,
                                  BeginTxnResponse* response) override;
    
    /**
     * @brief 处理事务提交请求。
     * @param context gRPC 服务器上下文。
     * @param request 带有事务 ID 的提交事务请求。
     * @param response 指示提交状态的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status CommitTransaction(grpc::ServerContext* context,
                                   const CommitTxnRequest* request,
                                   CommitTxnResponse* response) override;
    
    /**
     * @brief 处理事务回滚请求。
     * @param context gRPC 服务器上下文。
     * @param request 带有事务 ID 的回滚事务请求。
     * @param response 指示回滚状态的响应。
     * @return 指示成功或失败的 gRPC 状态。
     */
    grpc::Status RollbackTransaction(grpc::ServerContext* context,
                                     const RollbackTxnRequest* request,
                                     RollbackTxnResponse* response) override;
};

/**
 * @class GRPCServer
 * @brief 用于管理 gRPC 服务器生命周期的包装类。
 *
 * 此类封装了 gRPC 服务器设置、启动和关闭操作，
 * 为运行 gRPC 服务提供简单的接口。
 */
class GRPCServer {
private:
    std::unique_ptr<grpc::Server> server;  ///< gRPC 服务器实例
    std::unique_ptr<KVStoreServiceImpl> service;  ///< 服务实现
    
public:
    /**
     * @brief GRPCServer 的构造函数。
     * @param store 键值存储的指针。
     * @param address 要绑定到的服务器地址（例如 "0.0.0.0:50051"）。
     */
    GRPCServer(MVCCKVStore* store, const string& address);
    
    /**
     * @brief GRPCServer 的析构函数。
     */
    ~GRPCServer();
    
    /**
     * @brief 启动 gRPC 服务器。
     */
    void start();
    
    /**
     * @brief 关闭 gRPC 服务器。
     */
    void shutdown();
    
    /**
     * @brief 等待 gRPC 服务器完成。
     */
    void wait();
};

} // namespace kvstore