// include/server.h
#pragma once
#include "mvcc_kvstore.h"
#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <unordered_map>

namespace kvstore {

class KVStoreServiceImpl final : public KVStoreService::Service {
public:
    explicit KVStoreServiceImpl(std::shared_ptr<MVCCKVStore> store);
    
    // 基本操作
    grpc::Status Put(grpc::ServerContext* context,
                     const PutRequest* request,
                     PutResponse* response) override;
    
    grpc::Status Get(grpc::ServerContext* context,
                     const GetRequest* request,
                     GetResponse* response) override;
    
    grpc::Status Delete(grpc::ServerContext* context,
                        const DeleteRequest* request,
                        DeleteResponse* response) override;
    
    // 批量操作
    grpc::Status MultiPut(grpc::ServerContext* context,
                          const MultiPutRequest* request,
                          MultiPutResponse* response) override;
    
    grpc::Status MultiGet(grpc::ServerContext* context,
                          const MultiGetRequest* request,
                          MultiGetResponse* response) override;
    
    // 范围查询
    grpc::Status Scan(grpc::ServerContext* context,
                      const ScanRequest* request,
                      ScanResponse* response) override;
    
    // 事务操作
    grpc::Status BeginTransaction(grpc::ServerContext* context,
                                  const BeginTxnRequest* request,
                                  BeginTxnResponse* response) override;
    
    grpc::Status CommitTransaction(grpc::ServerContext* context,
                                   const CommitTxnRequest* request,
                                   CommitTxnResponse* response) override;
    
    grpc::Status RollbackTransaction(grpc::ServerContext* context,
                                     const RollbackTxnRequest* request,
                                     RollbackTxnResponse* response) override;

    // 隔离级别管理
    grpc::Status SetIsolationLevel(grpc::ServerContext* context,
                                    const SetIsolationLevelRequest* request,
                                    SetIsolationLevelResponse* response) override;
    
    grpc::Status GetIsolationLevel(grpc::ServerContext* context,
                                    const GetIsolationLevelRequest* request,
                                    GetIsolationLevelResponse* response) override;                                     
    
    // 管理操作
    grpc::Status Flush(grpc::ServerContext* context,
                       const FlushRequest* request,
                       FlushResponse* response) override;
    
    grpc::Status Stats(grpc::ServerContext* context,
                       const StatsRequest* request,
                       StatsResponse* response) override;
    
    grpc::Status Ping(grpc::ServerContext* context,
                      const PingRequest* request,
                      PingResponse* response) override;
    
    // 获取当前连接的隔离级别
    IsolationLevel get_client_isolation_level(grpc::ServerContext* context) {
        std::string client_id = get_client_id(context);
        std::lock_guard<std::mutex> lock(client_contexts_mutex_);
        
        auto it = client_contexts_.find(client_id);
        if (it != client_contexts_.end()) {
            it->second.last_activity = std::chrono::steady_clock::now();
            return it->second.isolation_level;
        }
        
        // 新客户端，使用默认隔离级别
        return store_->get_default_isolation_level();
    }
    
    void set_client_isolation_level(grpc::ServerContext* context, IsolationLevel level) {
        std::string client_id = get_client_id(context);
        std::lock_guard<std::mutex> lock(client_contexts_mutex_);
        
        client_contexts_[client_id] = {level, std::chrono::steady_clock::now()};
    }

private:
    std::shared_ptr<MVCCKVStore> store_;
    std::unordered_map<uint64_t, std::unique_ptr<Transaction>> active_txns_; // txn_id -> Transaction
    std::mutex txn_mutex_;
    std::atomic<uint64_t> txn_counter_{1};
    
    uint64_t get_new_txn_id() { return txn_counter_++; }

    // 每个客户端拥有一个隔离级别
    struct ClientContext {
        IsolationLevel isolation_level;
        std::chrono::steady_clock::time_point last_activity;
    };
    
    std::mutex client_contexts_mutex_;
    std::unordered_map<std::string, ClientContext> client_contexts_; // client_id -> ClientContext
    
    std::string get_client_id(grpc::ServerContext* context) {
        // 获取客户端标识（IP + 端口 或 自定义 token）
        std::string peer = context->peer(); // 例如：ipv4:127.0.0.1:54321
        return peer;
    }
};

class KVServer {
public:
    KVServer(const std::string& address, std::shared_ptr<MVCCKVStore> store);
    ~KVServer();
    
    void Start();
    void Shutdown();
    void Wait();
    
private:
    std::string address_;
    std::shared_ptr<MVCCKVStore> store_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<KVStoreServiceImpl> service_;
};

} // namespace kvstore