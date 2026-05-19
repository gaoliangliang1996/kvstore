// src/raft_grpc_service.cpp
#include "raft_kvstore.h"
#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

namespace kvstore {

class RaftKVStoreServiceImpl : public KVStoreService::Service {
public:
    RaftKVStoreServiceImpl(RaftKVStore* store) : store_(store) {
        // 设置重定向处理器
        store_->SetRedirectHandler([this](const std::string& leader_id) -> std::string {
            return GetLeaderAddress(leader_id);
        });
    }
    
    grpc::Status Put(grpc::ServerContext* context,
                     const PutRequest* request,
                     PutResponse* response) override {
        
        // 如果不是领导者，返回重定向错误
        if (!store_->IsLeader()) {
            response->set_success(false);
            response->set_error("Not leader");
            context->AddInitialMetadata("leader-id", store_->GetLeaderId());
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
        }
        
        Version ver = store_->put(request->key(), request->value());
        response->set_success(ver > 0);
        response->set_version(ver);
        
        return grpc::Status::OK;
    }
    
    grpc::Status Get(grpc::ServerContext* context,
                     const GetRequest* request,
                     GetResponse* response) override {
        
        std::string value;
        bool found = store_->get(request->key(), value);
        
        if (found) {
            response->set_success(true);
            response->set_value(value);
        } else {
            response->set_success(false);
        }
        
        return grpc::Status::OK;
    }
    
    grpc::Status BatchPut(grpc::ServerContext* context,
                          const BatchPutRequest* request,
                          BatchPutResponse* response) override {
        
        if (!store_->IsLeader()) {
            response->set_success(false);
            response->set_error("Not leader");
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Not leader");
        }
        
        std::vector<std::pair<std::string, std::string>> pairs;
        for (const auto& put : request->puts()) {
            pairs.emplace_back(put.key(), put.value());
        }
        
        auto result = store_->batch_put(pairs);
        response->set_success(result.success);
        response->set_success_count(result.versions.size());
        
        if (!result.error.empty()) {
            response->set_error(result.error);
        }
        
        return grpc::Status::OK;
    }
    
private:
    RaftKVStore* store_;
    
    std::string GetLeaderAddress(const std::string& leader_id) {
        // 将节点 ID 映射到地址
        static std::map<std::string, std::string> node_addrs = {
            {"node-1", "localhost:50051"},
            {"node-2", "localhost:50052"},
            {"node-3", "localhost:50053"}
        };
        
        auto it = node_addrs.find(leader_id);
        if (it != node_addrs.end()) {
            return it->second;
        }
        return "";
    }
};

} // namespace kvstore