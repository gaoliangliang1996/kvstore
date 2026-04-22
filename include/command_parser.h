// client/command_parser.h
#pragma once
#include "client.h"
#include <string>
#include <vector>

namespace kvstore {

class CommandParser {
public:
    explicit CommandParser(KVClient& client);
    
    void Execute(const std::string& command_line);
    
private:
    std::vector<std::string> tokenize(const std::string& line);
    
    void handle_put(const std::vector<std::string>& tokens);
    void handle_get(const std::vector<std::string>& tokens);
    void handle_del(const std::vector<std::string>& tokens);

    void handle_multiput(const std::vector<std::string>& tokens);
    void handle_multiget(const std::vector<std::string>& tokens);

    void handle_scan(const std::vector<std::string>& tokens);
    void handle_prefix(const std::vector<std::string>& tokens);

    void handle_begin(const std::vector<std::string>& tokens);
    void handle_commit(const std::vector<std::string>& tokens);
    void handle_rollback(const std::vector<std::string>& tokens);

    void handle_flush(const std::vector<std::string>& tokens);
    void handle_stats(const std::vector<std::string>& tokens);
    void handle_ping(const std::vector<std::string>& tokens);

    void handle_set_isolation(const std::vector<std::string>& tokens);
    void handle_get_isolation(const std::vector<std::string>& tokens);

    // 事务内操作
    void handle_txn_put(const std::vector<std::string>& tokens);
    void handle_txn_get(const std::vector<std::string>& tokens);
    void handle_txn_del(const std::vector<std::string>& tokens);
    
    KVClient& client_;
    uint64_t current_txn_id_ = 0;
    bool in_transaction_ = false;
    std::string current_txn_level_;  // 记录当前事务的隔离级别
};

} // namespace kvstore