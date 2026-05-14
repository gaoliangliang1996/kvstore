// raft/src/raft_transport.cpp
#include "../include/raft_transport.h"
#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <map>

namespace raft {

// gRPC 客户端实现
class RaftGrpcClient {
public:
    RaftGrpcClient(const std::string& address)
        : address_(address) {
        // 创建 channel，支持重连
        channel_ = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        stub_ = RaftService::NewStub(channel_);
    }
    
    void RequestVote(const RequestVoteRequest& req,
                     std::function<void(const RequestVoteResponse&)> callback) {
        RequestVoteResponse resp;
        grpc::ClientContext context;
        auto status = stub_->RequestVote(&context, req, &resp);
        if (status.ok()) {
            callback(resp);
        } else {
            std::cerr << "[RaftGrpcClient] RequestVote to " << address_ 
                      << " failed: " << status.error_message() << std::endl;
            RequestVoteResponse empty;
            callback(empty);
        }
    }
    
    void AppendEntries(const AppendEntriesRequest& req,
                       std::function<void(const AppendEntriesResponse&)> callback) {
        AppendEntriesResponse resp;
        grpc::ClientContext context;
        auto status = stub_->AppendEntries(&context, req, &resp);
        if (status.ok()) {
            callback(resp);
        } else {
            std::cerr << "[RaftGrpcClient] AppendEntries to " << address_ 
                      << " failed: " << status.error_message() << std::endl;
            AppendEntriesResponse empty;
            callback(empty);
        }
    }
    
    void InstallSnapshot(const InstallSnapshotRequest& req,
                         std::function<void(const InstallSnapshotResponse&)> callback) {
        InstallSnapshotResponse resp;
        grpc::ClientContext context;
        auto status = stub_->InstallSnapshot(&context, req, &resp);
        if (status.ok()) {
            callback(resp);
        } else {
            std::cerr << "[RaftGrpcClient] InstallSnapshot to " << address_ 
                      << " failed: " << status.error_message() << std::endl;
            InstallSnapshotResponse empty;
            callback(empty);
        }
    }
    
private:
    std::string address_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<RaftService::Stub> stub_;
};

// gRPC 服务实现
class RaftServiceImpl final : public RaftService::Service {
public:
    void SetHandlers(RaftTransport::RequestVoteHandler vote_handler,
                    RaftTransport::AppendEntriesHandler append_handler,
                    RaftTransport::InstallSnapshotHandler snapshot_handler) {
        vote_handler_ = vote_handler;
        append_handler_ = append_handler;
        snapshot_handler_ = snapshot_handler;
    }
    
    grpc::Status RequestVote(grpc::ServerContext* context,
                             const RequestVoteRequest* request,
                             RequestVoteResponse* response) override {
        if (vote_handler_) {
            *response = vote_handler_(*request);
        }
        return grpc::Status::OK;
    }
    
    grpc::Status AppendEntries(grpc::ServerContext* context,
                               const AppendEntriesRequest* request,
                               AppendEntriesResponse* response) override {
        if (append_handler_) {
            *response = append_handler_(*request);
        }
        return grpc::Status::OK;
    }
    
    grpc::Status InstallSnapshot(grpc::ServerContext* context,
                                 const InstallSnapshotRequest* request,
                                 InstallSnapshotResponse* response) override {
        if (snapshot_handler_) {
            *response = snapshot_handler_(*request);
        }
        return grpc::Status::OK;
    }
    
private:
    RaftTransport::RequestVoteHandler vote_handler_;
    RaftTransport::AppendEntriesHandler append_handler_;
    RaftTransport::InstallSnapshotHandler snapshot_handler_;
};

// GrpcTransport 实现
class GrpcTransport::Impl {
public:
    Impl() : running_(false), server_(nullptr) {}
    
    void Initialize(const RaftConfig& config) {
        config_ = config;
        node_id_ = config.node_id;
        
        // 创建到所有 peer 的客户端连接
        for (const auto& peer_addr : config.GetPeerAddresses()) {
            clients_[peer_addr] = std::make_unique<RaftGrpcClient>(peer_addr);
            std::cout << "[RaftTransport] Created client for peer: " << peer_addr << std::endl;
        }
    }
    
    void Start() {
        running_ = true;
        
        // 启动 gRPC 服务器，使用配置的端口
        std::string server_address = config_.listen_host + ":" + std::to_string(config_.listen_port);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        
        std::cout << "[RaftTransport] Node " << node_id_ 
                  << " listening on " << server_address << std::endl;
    }
    
    void Stop() {
        running_ = false;
        if (server_) {
            server_->Shutdown();
            server_->Wait();
        }
    }
    
    void SendRequestVote(const std::string& target, const RequestVoteRequest& req,
                         std::function<void(const RequestVoteResponse&)> callback) {
        // target 是 peer 的 node_id，需要转换为地址
        std::string target_addr = config_.GetPeerAddress(target);
        if (target_addr.empty()) {
            std::cerr << "[RaftTransport] Unknown target: " << target << std::endl;
            RequestVoteResponse empty;
            callback(empty);
            return;
        }
        
        auto it = clients_.find(target_addr);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->RequestVote(req, callback);
            }).detach();
        } else {
            std::cerr << "[RaftTransport] No client for target: " << target << std::endl;
            RequestVoteResponse empty;
            callback(empty);
        }
    }
    
    void SendAppendEntries(const std::string& target, const AppendEntriesRequest& req,
                           std::function<void(const AppendEntriesResponse&)> callback) {
        std::string target_addr = config_.GetPeerAddress(target);
        if (target_addr.empty()) {
            AppendEntriesResponse empty;
            callback(empty);
            return;
        }
        
        auto it = clients_.find(target_addr);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->AppendEntries(req, callback);
            }).detach();
        } else {
            AppendEntriesResponse empty;
            callback(empty);
        }
    }
    
    void SendInstallSnapshot(const std::string& target, const InstallSnapshotRequest& req,
                             std::function<void(const InstallSnapshotResponse&)> callback) {
        std::string target_addr = config_.GetPeerAddress(target);
        if (target_addr.empty()) {
            InstallSnapshotResponse empty;
            callback(empty);
            return;
        }
        
        auto it = clients_.find(target_addr);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->InstallSnapshot(req, callback);
            }).detach();
        } else {
            InstallSnapshotResponse empty;
            callback(empty);
        }
    }
    
    void SetHandlers(RaftTransport::RequestVoteHandler vote_handler,
                    RaftTransport::AppendEntriesHandler append_handler,
                    RaftTransport::InstallSnapshotHandler snapshot_handler) {
        service_.SetHandlers(vote_handler, append_handler, snapshot_handler);
    }
    
private:
    RaftConfig config_;
    std::string node_id_;
    std::map<std::string, std::unique_ptr<RaftGrpcClient>> clients_;
    RaftServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;
    std::atomic<bool> running_;
};

GrpcTransport::GrpcTransport() : impl_(std::make_unique<Impl>()) {}

GrpcTransport::~GrpcTransport() = default;

void GrpcTransport::Initialize(const RaftConfig& config) {
    impl_->Initialize(config);
}

void GrpcTransport::Start() {
    impl_->Start();
}

void GrpcTransport::Stop() {
    impl_->Stop();
}

void GrpcTransport::SendRequestVote(const std::string& target, const RequestVoteRequest& req,
                                    std::function<void(const RequestVoteResponse&)> callback) {
    impl_->SendRequestVote(target, req, callback);
}

void GrpcTransport::SendAppendEntries(const std::string& target, const AppendEntriesRequest& req,
                                      std::function<void(const AppendEntriesResponse&)> callback) {
    impl_->SendAppendEntries(target, req, callback);
}

void GrpcTransport::SendInstallSnapshot(const std::string& target, const InstallSnapshotRequest& req,
                                        std::function<void(const InstallSnapshotResponse&)> callback) {
    impl_->SendInstallSnapshot(target, req, callback);
}

void GrpcTransport::SetHandlers(RequestVoteHandler vote_handler,
                                AppendEntriesHandler append_handler,
                                InstallSnapshotHandler snapshot_handler) {
    impl_->SetHandlers(vote_handler, append_handler, snapshot_handler);
}

} // namespace raft