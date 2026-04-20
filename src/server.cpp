// src/server.cpp
#include "server.h"
#include <iostream>

namespace kvstore {

// ============== KVStoreServiceImpl 实现 ==============

KVStoreServiceImpl::KVStoreServiceImpl(std::shared_ptr<MVCCKVStore> store)
    : store_(store) {}

grpc::Status KVStoreServiceImpl::Put(grpc::ServerContext* context,
                                      const PutRequest* request,
                                      PutResponse* response) {
    if (request->txn_id() > 0) {
        // 事务内操作
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = active_txns_.find(request->txn_id());
        if (it != active_txns_.end()) { // 找到事务
            it->second->put(request->key(), request->value()); // 事务内写入
            response->set_success(true);
            return grpc::Status::OK;
        }

        // 没有找到事务
        response->set_success(false);
        response->set_error("Transaction not found");
        return grpc::Status::OK;
    }
    
    // 非事务操作
    Version ver = store_->put(request->key(), request->value());
    response->set_success(ver > 0);
    response->set_version(ver);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Get(grpc::ServerContext* context,
                                      const GetRequest* request,
                                      GetResponse* response) {
    std::string value;
    bool found = false;
    
    if (request->txn_id() > 0) {
        // 事务内读取
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = active_txns_.find(request->txn_id());
        if (it != active_txns_.end()) {
            found = it->second->get(request->key(), value);
        }
    } else {
        // 非事务读取
        Version snap_ver = request->snapshot_version();
        found = store_->get(request->key(), value, snap_ver);
    }
    
    if (found) {
        response->set_success(true);
        response->set_value(value);
    } else {
        response->set_success(false);
        response->set_error("Key not found");
    }
    
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Delete(grpc::ServerContext* context,
                                         const DeleteRequest* request,
                                         DeleteResponse* response) {
    if (request->txn_id() > 0) {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = active_txns_.find(request->txn_id());
        if (it != active_txns_.end()) {
            it->second->del(request->key());
            response->set_success(true);
            return grpc::Status::OK;
        }
        response->set_success(false);
        response->set_error("Transaction not found");
        return grpc::Status::OK;
    }
    
    Version ver = store_->del(request->key());
    response->set_success(ver > 0);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::MultiPut(grpc::ServerContext* context,
                                           const MultiPutRequest* request,
                                           MultiPutResponse* response) {
    for (const auto& put_req : request->puts()) {
        Version ver = store_->put(put_req.key(), put_req.value());
        response->add_versions(ver);
        if (ver == 0) { // 写入失败
            response->set_success(false);
            response->set_error("Failed to put: " + put_req.key());
            return grpc::Status::OK;
        }
    }
    
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::MultiGet(grpc::ServerContext* context,
                                           const MultiGetRequest* request,
                                           MultiGetResponse* response) {
    for (const auto& key : request->keys()) {
        std::string value;
        if (store_->get(key, value, request->snapshot_version())) {
            auto* kv = response->add_results();
            kv->set_key(key);
            kv->set_value(value);
        }
    }
    
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Scan(grpc::ServerContext* context,
                                       const ScanRequest* request,
                                       ScanResponse* response) {
    // // 简化实现：使用存储引擎的范围扫描
    // // 实际应该从 MemTable 和 SSTable 中获取数据
    // auto results = store_->scan(request->start_key(), 
    //                              request->end_key(),
    //                              request->limit());
    
    // for (const auto& kv : results) {
    //     auto* item = response->add_results();
    //     item->set_key(kv.first);
    //     item->set_value(kv.second);
    // }
    
    // response->set_success(true);
    // response->set_has_more(false);
    // return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::BeginTransaction(grpc::ServerContext* context,
                                                   const BeginTxnRequest* request,
                                                   BeginTxnResponse* response) {
    uint64_t txn_id = get_new_txn_id();
    IsolationLevel level = IsolationLevel::SNAPSHOT_ISOLATION;
    
    if (request->isolation_level() == "SERIALIZABLE") {
        level = IsolationLevel::SERIALIZABLE;
    }
    
    auto txn = std::make_unique<Transaction>(store_.get(), level);
    
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        active_txns_[txn_id] = std::move(txn); // std::move 将 txn 转移到 active_txns_ 中，避免不必要的复制
    }
    
    response->set_success(true);
    response->set_txn_id(txn_id);
    response->set_snapshot_version(store_->get_current_version());
    
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::CommitTransaction(grpc::ServerContext* context,
                                                    const CommitTxnRequest* request,
                                                    CommitTxnResponse* response) {
    std::unique_ptr<Transaction> txn;
    
    {
        std::lock_guard<std::mutex> lock(txn_mutex_);
        auto it = active_txns_.find(request->txn_id());
        if (it == active_txns_.end()) { // 没有找到事务
            response->set_success(false);
            response->set_error("Transaction not found");
            return grpc::Status::OK;
        }

        // 找到事务，准备提交
        txn = std::move(it->second);
        active_txns_.erase(it);
    }
    
    bool success = txn->commit();
    response->set_success(success);
    
    if (!success) {
        response->set_error("Commit failed due to conflict");
    }
    
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::RollbackTransaction(grpc::ServerContext* context,
                                                      const RollbackTxnRequest* request,
                                                      RollbackTxnResponse* response) {
    std::lock_guard<std::mutex> lock(txn_mutex_);
    auto it = active_txns_.find(request->txn_id());
    if (it != active_txns_.end()) { // 找到事务，执行回滚
        it->second->rollback();
        active_txns_.erase(it);
    }
    
    // 无论是否找到事务，都返回成功（幂等操作）
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Flush(grpc::ServerContext* context,
                                        const FlushRequest* request,
                                        FlushResponse* response) {
    store_->flush();
    response->set_success(true);
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Stats(grpc::ServerContext* context,
                                        const StatsRequest* request,
                                        StatsResponse* response) {
    auto stats = store_->get_stats();
    // response->set_total_keys(stats.total_keys);
    // response->set_total_versions(stats.total_versions);
    // response->set_memtable_size(stats.memtable_size);
    // response->set_sstable_count(stats.sstable_count);
    // response->set_cache_hits(stats.cache_hits);
    // response->set_cache_misses(stats.cache_misses);
    // response->set_cache_hit_rate(stats.cache_hit_rate);
    
    return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Ping(grpc::ServerContext* context,
                                       const PingRequest* request,
                                       PingResponse* response) {
    response->set_message("pong: " + request->message());
    response->set_timestamp(std::time(nullptr));
    return grpc::Status::OK;
}

// ============== KVServer 实现 ==============

KVServer::KVServer(const std::string& address, std::shared_ptr<MVCCKVStore> store)
    : address_(address), store_(store) {
    service_ = std::make_unique<KVStoreServiceImpl>(store_);
}

KVServer::~KVServer() {
    Shutdown();
}

void KVServer::Start() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
    builder.RegisterService(service_.get());
    
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << address_ << std::endl;
}

void KVServer::Shutdown() {
    if (server_) {
        server_->Shutdown();
    }
}

void KVServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

} // namespace kvstore