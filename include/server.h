// include/server.h
#pragma once
// #include "kvstore.h"
#include "mvcc_kvstore.h"
#include "kvstore.grpc.pb.h"
#include "transaction.h"
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
    
private:
    std::shared_ptr<MVCCKVStore> store_;
    std::unordered_map<uint64_t, std::unique_ptr<Transaction>> active_txns_; // txn_id -> Transaction
    std::mutex txn_mutex_;
    std::atomic<uint64_t> txn_counter_{1};
    
    uint64_t get_new_txn_id() { return txn_counter_++; }
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