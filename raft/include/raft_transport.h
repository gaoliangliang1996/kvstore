#pragma once
#include "raft.pb.h"
#include <functional>
#include <memory>
#include <map>

namespace raft {

class RaftTransport {
public:
    using RequestVoteHandler = std::function<RequestVoteResponse(const RequestVoteRequest&)>;
    using AppendEntriesHandler = std::function<AppendEntriesResponse(const AppendEntriesRequest&)>;
    using InstallSnapshotHandler = std::function<InstallSnapshotResponse(const InstallSnapshotRequest&)>;
    
    virtual ~RaftTransport() = default;
    
    virtual void Initialize(const std::string& node_id, 
                           const std::vector<std::string>& peer_ids) = 0;
    
    virtual void Start() = 0;
    virtual void Stop() = 0;
    
    // 发送投票请求
    virtual void SendRequestVote(const std::string& target, const RequestVoteRequest& req,
                                 std::function<void(const RequestVoteResponse&)> callback) = 0;
    // 发送日志复制/心跳请求
    virtual void SendAppendEntries(const std::string& target, const AppendEntriesRequest& req,
                                   std::function<void(const AppendEntriesResponse&)> callback) = 0;
    // 发送快照安装请求
    virtual void SendInstallSnapshot(const std::string& target, const InstallSnapshotRequest& req,
                                     std::function<void(const InstallSnapshotResponse&)> callback) = 0;
    
    virtual void SetHandlers(RequestVoteHandler vote_handler,
                            AppendEntriesHandler append_handler,
                            InstallSnapshotHandler snapshot_handler) = 0;
};

// 基于 gRPC 的传输层实现
class GrpcTransport : public RaftTransport {
public:
    GrpcTransport();
    ~GrpcTransport() override;
    
    void Initialize(const std::string& node_id, const std::vector<std::string>& peer_ids) override;
    void Start() override;
    void Stop() override;
    
    void SendRequestVote(const std::string& target, const RequestVoteRequest& req,
                         std::function<void(const RequestVoteResponse&)> callback) override;
    
    void SendAppendEntries(const std::string& target, const AppendEntriesRequest& req,
                           std::function<void(const AppendEntriesResponse&)> callback) override;
    
    void SendInstallSnapshot(const std::string& target, const InstallSnapshotRequest& req,
                             std::function<void(const InstallSnapshotResponse&)> callback) override;
    
    void SetHandlers(RequestVoteHandler vote_handler,
                    AppendEntriesHandler append_handler,
                    InstallSnapshotHandler snapshot_handler) override;
    
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace raft