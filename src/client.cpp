// src/client.cpp
#include "client.h"
#include <sstream>
#include <grpcpp/grpcpp.h>

namespace kvstore {

KVClient::KVClient(const std::string& address) {
    channel_ = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    stub_ = KVStoreService::NewStub(channel_);
}

KVClient::~KVClient() = default;

bool KVClient::Put(const std::string& key, const std::string& value, uint64_t* version) {
    PutRequest request;
    request.set_key(key);
    request.set_value(value);
    
    PutResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Put(&context, request, &response);
    
    if (status.ok() && response.success()) {
        if (version) {
            *version = response.version();
        }
        return true;
    }
    
    return false;
}

bool KVClient::Get(const std::string& key, std::string& value, uint64_t snapshot_version) {
    GetRequest request;
    request.set_key(key);
    request.set_snapshot_version(snapshot_version);
    
    GetResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Get(&context, request, &response);
    
    if (status.ok() && response.success()) {
        value = response.value();
        return true;
    }
    
    return false;
}

bool KVClient::Delete(const std::string& key) {
    DeleteRequest request;
    request.set_key(key);
    
    DeleteResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Delete(&context, request, &response);
    
    return status.ok() && response.success();
}

bool KVClient::MultiPut(const std::vector<std::pair<std::string, std::string>>& pairs) {
    MultiPutRequest request;
    for (const auto& [key, value] : pairs) {
        auto* put = request.add_puts();
        put->set_key(key);
        put->set_value(value);
    }
    
    MultiPutResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->MultiPut(&context, request, &response);
    
    return status.ok() && response.success();
}

std::vector<std::pair<std::string, std::string>> KVClient::MultiGet(const std::vector<std::string>& keys) {
    MultiGetRequest request;
    for (const auto& key : keys) {
        request.add_keys(key);
    }
    
    MultiGetResponse response;
    grpc::ClientContext context;
    
    std::vector<std::pair<std::string, std::string>> results;
    
    grpc::Status status = stub_->MultiGet(&context, request, &response);
    
    if (status.ok() && response.success()) {
        for (const auto& kv : response.results()) {
            results.emplace_back(kv.key(), kv.value());
        }
    }
    
    return results;
}

std::vector<std::pair<std::string, std::string>> KVClient::Scan(const std::string& start_key,
                                                                  const std::string& end_key,
                                                                  uint32_t limit) {
    ScanRequest request;
    request.set_start_key(start_key);
    request.set_end_key(end_key);
    request.set_limit(limit);
    
    ScanResponse response;
    grpc::ClientContext context;
    
    std::vector<std::pair<std::string, std::string>> results;
    
    grpc::Status status = stub_->Scan(&context, request, &response);
    
    if (status.ok() && response.success()) {
        for (const auto& kv : response.results()) {
            results.emplace_back(kv.key(), kv.value());
        }
    }
    
    return results;
}

uint64_t KVClient::BeginTransaction(const std::string& isolation_level) {
    BeginTxnRequest request;
    request.set_isolation_level(isolation_level);
    
    BeginTxnResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->BeginTransaction(&context, request, &response);
    
    if (status.ok() && response.success()) {
        return response.txn_id();
    }
    
    return 0;
}

bool KVClient::CommitTransaction(uint64_t txn_id) {
    CommitTxnRequest request;
    request.set_txn_id(txn_id);
    
    CommitTxnResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->CommitTransaction(&context, request, &response);
    
    return status.ok() && response.success();
}

bool KVClient::RollbackTransaction(uint64_t txn_id) {
    RollbackTxnRequest request;
    request.set_txn_id(txn_id);
    
    RollbackTxnResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->RollbackTransaction(&context, request, &response);
    
    return status.ok() && response.success();
}

bool KVClient::TxnPut(uint64_t txn_id, const std::string& key, const std::string& value) {
    PutRequest request;
    request.set_key(key);
    request.set_value(value);
    request.set_txn_id(txn_id);
    
    PutResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Put(&context, request, &response);
    
    return status.ok() && response.success();
}

bool KVClient::TxnGet(uint64_t txn_id, const std::string& key, std::string& value) {
    GetRequest request;
    request.set_key(key);
    request.set_txn_id(txn_id);
    
    GetResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Get(&context, request, &response);
    
    if (status.ok() && response.success()) {
        value = response.value();
        return true;
    }
    
    return false;
}

bool KVClient::TxnDelete(uint64_t txn_id, const std::string& key) {
    DeleteRequest request;
    request.set_key(key);
    request.set_txn_id(txn_id);
    
    DeleteResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Delete(&context, request, &response);
    
    return status.ok() && response.success();
}

bool KVClient::Flush() {
    FlushRequest request;
    FlushResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Flush(&context, request, &response);
    
    return status.ok() && response.success();
}

std::string KVClient::GetStats() {
    StatsRequest request;
    StatsResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Stats(&context, request, &response);
    
    if (status.ok()) {
        // std::stringstream ss;
        // ss << "Total Keys: " << response.total_keys() << "\n";
        // ss << "Total Versions: " << response.total_versions() << "\n";
        // ss << "MemTable Size: " << response.memtable_size() << "\n";
        // ss << "SSTable Count: " << response.sstable_count() << "\n";
        // ss << "Cache Hit Rate: " << response.cache_hit_rate() << "%\n";
        // return ss.str();
    }
    
    return "Failed to get stats";
}

bool KVClient::Ping() {
    PingRequest request;
    request.set_message("hello");
    
    PingResponse response;
    grpc::ClientContext context;
    
    grpc::Status status = stub_->Ping(&context, request, &response);
    
    if (status.ok()) {
        std::cout << "Pong: " << response.message() << " (timestamp: " << response.timestamp() << ")" << std::endl;
        return true;
    }
    
    return false;
}

} // namespace kvstore