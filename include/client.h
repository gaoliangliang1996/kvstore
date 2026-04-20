// include/client.h
#pragma once
#include "kvstore.grpc.pb.h"
#include <memory>
#include <string>
#include <vector>

namespace kvstore {

class KVClient {
public:
    explicit KVClient(const std::string& address);
    ~KVClient();
    
    // 基本操作
    bool Put(const std::string& key, const std::string& value, uint64_t* version = nullptr);
    bool Get(const std::string& key, std::string& value, uint64_t snapshot_version = 0);
    bool Delete(const std::string& key);
    
    // 批量操作
    bool MultiPut(const std::vector<std::pair<std::string, std::string>>& pairs);
    std::vector<std::pair<std::string, std::string>> MultiGet(const std::vector<std::string>& keys);
    
    // 范围查询
    std::vector<std::pair<std::string, std::string>> Scan(const std::string& start_key,
                                                           const std::string& end_key,
                                                           uint32_t limit = 0);
    
    // 事务支持
    uint64_t BeginTransaction(const std::string& isolation_level = "SNAPSHOT");
    bool CommitTransaction(uint64_t txn_id);
    bool RollbackTransaction(uint64_t txn_id);
    
    // 事务内操作
    bool TxnPut(uint64_t txn_id, const std::string& key, const std::string& value);
    bool TxnGet(uint64_t txn_id, const std::string& key, std::string& value);
    bool TxnDelete(uint64_t txn_id, const std::string& key);
    
    // 管理操作
    bool Flush();
    std::string GetStats();
    bool Ping();
    
    // 连接状态
    bool IsConnected() const { return channel_ != nullptr; }
    
private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<KVStoreService::Stub> stub_;
};

} // namespace kvstore