#include "grpc_service.h"
#include "range_query.h"

namespace kvstore {

KVStoreServiceImpl::KVStoreServiceImpl(MVCCKVStore* kvstore) : store(kvstore) {}

/*
message PutRequest {
    string key = 1;
    string value = 2;
    uint64 txn_id = 3; // 事务ID，0 表示非事务操作
}

message PutResponse {
    bool success = 1;
    string error = 2;
    uint64 version = 3;
}
*/
grpc::Status KVStoreServiceImpl::Put(grpc::ServerContext* context,
                                     const PutRequest* request,
                                     PutResponse* response) {
    std::cout << "Put request->key(): " << request->key() << std::endl;

    if (request->txn_id() > 0) {
        // 事务内操作
        std::lock_guard<std::mutex> lock(txn_mutex);

        // 查找对应的事务
        auto it = active_transactions.find(request->txn_id());
        if (it != active_transactions.end()) {
            it->second->put(request->key(), request->value()); // 事务内写入
            response->set_success(true);
            return grpc::Status::OK;
        }
    }
    
    // 非事务操作
    Version ver = store->put(request->key(), request->value());
    response->set_success(ver > 0);
    response->set_version(ver);
    return grpc::Status::OK;
}

/*
message GetRequest {
    string key = 1;
    uint64 snapshot_version = 2;
    uint64 txn_id = 3;
}

message GetResponse {
    bool success = 1;
    string value = 2;
    uint64 version = 3;
    string error = 4;
}

rpc Get(GetRequest) returns (GetResponse);
*/
grpc::Status KVStoreServiceImpl::Get(grpc::ServerContext* context,
                                     const GetRequest* request,
                                     GetResponse* response) {
    std::cout << "Get request->key(): " << request->key() << std::endl;

    string value;
    bool found = false;
    
    if (request->txn_id() > 0) {
        // 事务内读取
        std::lock_guard<std::mutex> lock(txn_mutex);
        auto it = active_transactions.find(request->txn_id());
        if (it != active_transactions.end()) {
            found = it->second->get(request->key(), value);
        }
    } else {
        // 非事务读取
        found = store->get(request->key(), value, request->snapshot_version());
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

/*
message DeleteRequest {
    string key = 1;
    uint64 txn_id = 2;
}

message DeleteResponse {
    bool success = 1;
    string error = 2;
}

rpc Delete(DeleteRequest) returns (DeleteResponse);
*/
grpc::Status KVStoreServiceImpl::Delete(grpc::ServerContext* context,
                                        const DeleteRequest* request,
                                        DeleteResponse* response) {
    if (request->txn_id() > 0) {
        std::lock_guard<std::mutex> lock(txn_mutex);
        auto it = active_transactions.find(request->txn_id());
        if (it != active_transactions.end()) {
            it->second->del(request->key());
            response->set_success(true);
            return grpc::Status::OK;
        }
    }
    
    Version ver = store->del(request->key());
    response->set_success(ver > 0);
    return grpc::Status::OK;
}

/*
message ScanRequest {
    string start_key = 1;
    string end_key = 2;
    uint32 page_size = 3;
    string page_token = 4;
    uint64 snapshot_version = 5;
}

    message KeyValue {
        string key = 1;
        string value = 2;
        uint64 version = 3;
    }

message ScanResponse {
    repeated KeyValue data = 1;
    string next_token = 2;
    bool has_more = 3;
}

rpc Scan(ScanRequest) returns (ScanResponse);
*/
grpc::Status KVStoreServiceImpl::Scan(grpc::ServerContext* context,
                                      const ScanRequest* request,
                                      ScanResponse* response) {
    auto iter = RangeQuerySupport::scan(
        {}, // sstables
        nullptr, // memtable
        request->start_key(),
        request->end_key(),
        request->snapshot_version()
    );
    
    size_t count = 0;
    while (iter.valid() && count < request->page_size()) {
        kvstore::KeyValue* kv = response->add_data();
        kv->set_key(iter.key());
        kv->set_value(iter.value());
        iter.next();
        count++;
    }
    
    if (iter.valid()) {
        response->set_next_token(iter.key());
        response->set_has_more(true);
    }
    
    return grpc::Status::OK;
}

/*
message BeginTxnRequest {
    string isolation_level = 1;
}

message BeginTxnResponse {
    uint64 txn_id = 1;
    uint64 snapshot_version = 2;
}
*/
grpc::Status KVStoreServiceImpl::BeginTransaction(grpc::ServerContext* context,
                                                   const BeginTxnRequest* request,
                                                   BeginTxnResponse* response) {
    IsolationLevel level = IsolationLevel::SNAPSHOT_ISOLATION;
    if (request->isolation_level() == "READ_COMMITTED") {
        level = IsolationLevel::READ_COMMITTED;
    } else if (request->isolation_level() == "REPEATABLE_READ") {
        level = IsolationLevel::REPEATABLE_READ;
    }
    
    auto txn = std::make_unique<Transaction>(store, level);
    uint64_t txn_id = txn->get_txn_id();
    
    {
        std::lock_guard<std::mutex> lock(txn_mutex);
        active_transactions[txn_id] = std::move(txn);
    }
    
    response->set_txn_id(txn_id);
    response->set_snapshot_version(store->create_snapshot()->get_version());
    
    return grpc::Status::OK;
}

/*
message CommitTxnRequest {
    uint64 txn_id = 1;
}

message CommitTxnResponse {
    bool success = 1;
    string error = 2;
}
*/
grpc::Status KVStoreServiceImpl::CommitTransaction(grpc::ServerContext* context,
                                                   const CommitTxnRequest* request,
                                                   CommitTxnResponse* response) {
    std::lock_guard<std::mutex> lock(txn_mutex);
    
    auto it = active_transactions.find(request->txn_id());
    if (it == active_transactions.end()) {
        response->set_success(false);
        response->set_error("Transaction not found");
        return grpc::Status::OK;
    }
    
    bool success = it->second->commit();
    response->set_success(success);
    
    active_transactions.erase(it);
    return grpc::Status::OK;
}

/*
message RollbackTxnRequest {
    uint64 txn_id = 1;
}

message RollbackTxnResponse {
    bool success = 1;
}
*/
grpc::Status KVStoreServiceImpl::RollbackTransaction(grpc::ServerContext* context,
                                                     const RollbackTxnRequest* request,
                                                     RollbackTxnResponse* response) {
    std::lock_guard<std::mutex> lock(txn_mutex);
    
    auto it = active_transactions.find(request->txn_id());
    if (it != active_transactions.end()) {
        it->second->rollback();
        active_transactions.erase(it);
    }
    
    response->set_success(true);
    return grpc::Status::OK;
}

GRPCServer::GRPCServer(MVCCKVStore* store, const string& address) {
    service = std::make_unique<KVStoreServiceImpl>(store);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    
    server = builder.BuildAndStart();
}

GRPCServer::~GRPCServer() {
    shutdown();
}

void GRPCServer::start() {
    if (server) {
        std::cout << "gRPC server started" << std::endl;
    }
}

void GRPCServer::shutdown() {
    if (server) {
        server->Shutdown();
    }
}

void GRPCServer::wait() {
    if (server) {
        server->Wait();
    }
}

} // namespace kvstore