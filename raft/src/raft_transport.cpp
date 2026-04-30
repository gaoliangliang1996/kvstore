// raft/src/raft_transport.cpp
#include "../include/raft_transport.h"
#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include <thread>
#include <chrono>
#include <iostream>

namespace raft {

// gRPC 客户端实现
class RaftGrpcClient {
public:
    RaftGrpcClient(const std::string& address)
        : stub_(RaftService::NewStub(grpc::CreateChannel(
            address, grpc::InsecureChannelCredentials()))) {}
    
    void RequestVote(const RequestVoteRequest& req,
                     std::function<void(const RequestVoteResponse&)> callback) {
        RequestVoteResponse resp;
        grpc::ClientContext context;
        auto status = stub_->RequestVote(&context, req, &resp);
        if (status.ok()) {
            callback(resp);
        } else {
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
            InstallSnapshotResponse empty;
            callback(empty);
        }
    }
    
private:
    std::unique_ptr<RaftService::Stub> stub_;
};

// GrpcTransport 实现
class GrpcTransport::Impl {
public:
    Impl() : running_(false), server_(nullptr) {}
    
    void Initialize(const std::string& node_id, const std::vector<std::string>& peer_ids) {
        node_id_ = node_id;
        peer_ids_ = peer_ids;
        
        // 创建客户端连接
        for (const auto& peer : peer_ids) {
            if (peer != node_id) {
                // 为每个 peer 创建一个 gRPC 客户端
                clients_[peer] = std::make_unique<RaftGrpcClient>(peer);
            }
        }
    }
    
    void Start() {
        running_ = true;
        
        // 启动 gRPC 服务器
        std::string server_address = "0.0.0.0:50051";
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        
        std::cout << "[RaftTransport] Server listening on " << server_address << std::endl;
    }
    
    void Stop() {
        running_ = false;
        if (server_) {
            server_->Shutdown();
        }
    }
    
    void SendRequestVote(const std::string& target, const RequestVoteRequest& req,
                         std::function<void(const RequestVoteResponse&)> callback) {
        auto it = clients_.find(target);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->RequestVote(req, callback);
            }).detach();
        }
    }
    
    void SendAppendEntries(const std::string& target, const AppendEntriesRequest& req,
                           std::function<void(const AppendEntriesResponse&)> callback) {
        auto it = clients_.find(target);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->AppendEntries(req, callback);
            }).detach();
        }
    }
    
    void SendInstallSnapshot(const std::string& target, const InstallSnapshotRequest& req,
                             std::function<void(const InstallSnapshotResponse&)> callback) {
        auto it = clients_.find(target);
        if (it != clients_.end()) {
            std::thread([client = it->second.get(), req, callback]() {
                client->InstallSnapshot(req, callback);
            }).detach();
        }
    }
    
    void SetHandlers(RequestVoteHandler vote_handler,
                    AppendEntriesHandler append_handler,
                    InstallSnapshotHandler snapshot_handler) {
        service_.SetHandlers(vote_handler, append_handler, snapshot_handler);
    }
    
private:
    // gRPC 服务实现
    class RaftServiceImpl : public RaftService::Service {
    public:
        void SetHandlers(RequestVoteHandler vote_handler,
                        AppendEntriesHandler append_handler,
                        InstallSnapshotHandler snapshot_handler) {
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
        RequestVoteHandler vote_handler_;
        AppendEntriesHandler append_handler_;
        InstallSnapshotHandler snapshot_handler_;
    };
    
    std::string node_id_;
    std::vector<std::string> peer_ids_;
    std::map<std::string, std::unique_ptr<RaftGrpcClient>> clients_;
    RaftServiceImpl service_;
    std::unique_ptr<grpc::Server> server_;
    std::atomic<bool> running_;
};

GrpcTransport::GrpcTransport() : impl_(std::make_unique<Impl>()) {}

GrpcTransport::~GrpcTransport() = default;

void GrpcTransport::Initialize(const std::string& node_id, const std::vector<std::string>& peer_ids) {
    impl_->Initialize(node_id, peer_ids);
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