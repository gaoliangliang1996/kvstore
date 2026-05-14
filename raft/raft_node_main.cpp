// raft/raft_node_main.cpp
#include "include/raft_node.h"
#include <iostream>
#include <csignal>
#include <atomic>
#include <getopt.h>

std::atomic<bool> running(true);

void signal_handler(int sig) {
    std::cout << "\nReceived signal " << sig << ", shutting down..." << std::endl;
    running = false;
}

raft::RaftConfig parse_config(int argc, char* argv[]) {
    raft::RaftConfig config;
    
    static struct option long_options[] = {
        {"node-id", required_argument, 0, 'i'},
        {"listen-host", required_argument, 0, 'h'},
        {"listen-port", required_argument, 0, 'p'},
        {"data-dir", required_argument, 0, 'd'},
        {"peers", required_argument, 0, 'e'},
        {"election-timeout", required_argument, 0, 't'},
        {"heartbeat-interval", required_argument, 0, 'b'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "i:h:p:d:e:t:b:", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'i':
                config.node_id = optarg;
                break;
            case 'h':
                config.listen_host = optarg;
                break;
            case 'p':
                config.listen_port = std::stoi(optarg);
                break;
            case 'd':
                config.data_dir = optarg;
                break;
            case 'e': {
                // 解析 peers: "node-1=127.0.0.1:50051,node-2=127.0.0.1:50052"
                std::string peers_str = optarg;
                size_t pos = 0;
                while ((pos = peers_str.find(',')) != std::string::npos) {
                    std::string peer = peers_str.substr(0, pos);
                    size_t eq_pos = peer.find('=');
                    if (eq_pos != std::string::npos) {
                        std::string id = peer.substr(0, eq_pos);
                        std::string addr = peer.substr(eq_pos + 1);
                        size_t colon_pos = addr.find(':');
                        if (colon_pos != std::string::npos) {
                            std::string host = addr.substr(0, colon_pos);
                            int port = std::stoi(addr.substr(colon_pos + 1));
                            config.peers.push_back({id, host, port});
                        }
                    }
                    peers_str = peers_str.substr(pos + 1);
                }
                // 处理最后一个
                size_t eq_pos = peers_str.find('=');
                if (eq_pos != std::string::npos) {
                    std::string id = peers_str.substr(0, eq_pos);
                    std::string addr = peers_str.substr(eq_pos + 1);
                    size_t colon_pos = addr.find(':');
                    if (colon_pos != std::string::npos) {
                        std::string host = addr.substr(0, colon_pos);
                        int port = std::stoi(addr.substr(colon_pos + 1));
                        config.peers.push_back({id, host, port});
                    }
                }
                break;
            }
            case 't':
                config.election_timeout_ms = std::stoi(optarg);
                break;
            case 'b':
                config.heartbeat_interval_ms = std::stoi(optarg);
                break;
            default:
                break;
        }
    }
    
    return config;
}

int main(int argc, char* argv[]) {
    std::cout << "Raft Node Starting..." << std::endl;
    
    // 解析配置
    raft::RaftConfig config = parse_config(argc, argv);
    
    if (config.node_id.empty()) {
        std::cerr << "Error: node-id is required" << std::endl;
        return 1;
    }
    
    if (config.listen_port == 0) {
        std::cerr << "Error: listen-port is required" << std::endl;
        return 1;
    }
    
    if (config.peers.empty()) {
        std::cerr << "Error: peers are required" << std::endl;
        return 1;
    }
    
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Node ID: " << config.node_id << std::endl;
    std::cout << "  Listen: " << config.listen_host << ":" << config.listen_port << std::endl;
    std::cout << "  Data Dir: " << config.data_dir << std::endl;
    std::cout << "  Peers: ";
    for (const auto& peer : config.peers) {
        std::cout << peer.node_id << "=" << peer.GetAddress() << " ";
    }
    std::cout << std::endl;
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // 创建并启动 Raft 节点
    raft::RaftNode node(config);
    
    // 设置回调
    node.SetLeaderChangeCallback([](const std::string& leader_id) {
        std::cout << "[Main] Leader changed to: " << leader_id << std::endl;
    });
    
    node.SetCommitCallback([](uint64_t index, const std::string& cmd, const std::vector<uint8_t>& data) {
        std::string data_str(data.begin(), data.end());
        std::cout << "[Main] Committed: index=" << index 
                  << " cmd=" << cmd << " data=" << data_str << std::endl;
    });
    
    node.Start();
    
    std::cout << "Node " << config.node_id << " started. Press Ctrl+C to stop." << std::endl;
    
    // 命令行交互
    std::string line;
    while (running) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        
        if (line == "quit" || line == "exit") {
            break;
        } else if (line.substr(0, 6) == "propose") {
            // propose key:value
            size_t pos = line.find(' ', 7);
            if (pos != std::string::npos) {
                std::string data = line.substr(pos + 1);
                std::vector<uint8_t> data_vec(data.begin(), data.end());
                node.Propose("put", data_vec, [](bool success, uint64_t index) {
                    std::cout << "Proposal result: " << (success ? "success" : "failed")
                              << " index=" << index << std::endl;
                });
            }
        } else if (line == "status") {
            std::cout << "State: " << (int)node.GetState() << std::endl;
            std::cout << "Term: " << node.GetCurrentTerm() << std::endl;
            std::cout << "Leader: " << node.GetLeaderId() << std::endl;
            std::cout << "Commit Index: " << node.GetCommitIndex() << std::endl;
            std::cout << "Is Leader: " << (node.IsLeader() ? "yes" : "no") << std::endl;
        }
    }
    
    std::cout << "Shutting down..." << std::endl;
    node.Stop();
    
    return 0;
}