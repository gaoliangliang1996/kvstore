// client/command_parser.cpp
#include "command_parser.h"
#include <iostream>
#include <sstream>
#include <iomanip>

namespace kvstore {

CommandParser::CommandParser(KVClient& client) : client_(client) {}

// 切分成 token 数组
std::vector<std::string> CommandParser::tokenize(const std::string& line) {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    
    while (ss >> token) {
        tokens.push_back(token);
    }
    
    return tokens;
}

void CommandParser::Execute(const std::string& command_line) {
    std::vector<std::string> tokens = tokenize(command_line);
    if (tokens.empty()) return;
    
    const std::string& cmd = tokens[0];
    
    if (cmd == "put") {
        handle_put(tokens);
    } else if (cmd == "get") {
        handle_get(tokens);
    } else if (cmd == "del" || cmd == "delete") {
        handle_del(tokens);
    } else if (cmd == "multiput") {
        handle_multiput(tokens);
    } else if (cmd == "multiget") {
        handle_multiget(tokens);
    } else if (cmd == "scan") {
        handle_scan(tokens);
    } else if (cmd == "prefix") {
        handle_prefix(tokens);
    } else if (cmd == "begin") {
        handle_begin(tokens);
    } else if (cmd == "commit") {
        handle_commit(tokens);
    } else if (cmd == "rollback") {
        handle_rollback(tokens);
    } else if (cmd == "flush") {
        handle_flush(tokens);
    } else if (cmd == "stats") {
        handle_stats(tokens);
    } else if (cmd == "ping") {
        handle_ping(tokens);
    } else {
        std::cout << "Unknown command: " << cmd << ". Type 'help' for available commands." << std::endl;
    }
}

void CommandParser::handle_put(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) {
        std::cout << "Usage: put <key> <value>" << std::endl;
        return;
    }
    
    std::string key = tokens[1];
    std::string value = tokens[2];
    
    if (in_transaction_) {
        // 事务内操作
        if (client_.TxnPut(current_txn_id_, key, value)) {
            std::cout << "Put operation added to transaction." << std::endl;
        } else {
            std::cout << "Failed to add put operation to transaction." << std::endl;    
        }
        std::cout << "In transaction mode. Use 'commit' to apply changes." << std::endl;

        return;
    }
    
    uint64_t version;
    if (client_.Put(key, value, &version)) {
        std::cout << "Success! Version: " << version << std::endl;
    } else {
        std::cout << "Failed to put key: " << key << std::endl;
    }
}

void CommandParser::handle_get(const std::vector<std::string>& tokens) {
    if (tokens.size() < 2) {
        std::cout << "Usage: get <key>" << std::endl;
        return;
    }
    
    std::string key = tokens[1];
    std::string value;

    if (in_transaction_) {
        // 事务内操作
        if (client_.TxnGet(current_txn_id_, key, value)) {
            std::cout << "Get operation added to transaction." << std::endl;
        } else {
            std::cout << "Failed to add get operation to transaction." << std::endl;    
        }
        std::cout << "In transaction mode. Use 'commit' to apply changes." << std::endl;

        return;
    }
    
    if (client_.Get(key, value)) {
        std::cout << key << " = " << value << std::endl;
    } else {
        std::cout << "Key not found: " << key << std::endl;
    }
}

void CommandParser::handle_del(const std::vector<std::string>& tokens) {
    if (tokens.size() < 2) {
        std::cout << "Usage: del <key>" << std::endl;
        return;
    }
    
    std::string key = tokens[1];

    if (in_transaction_) {
        // 事务内操作
        if (client_.TxnDelete(current_txn_id_, key)) {
            std::cout << "Delete operation added to transaction." << std::endl;
        } else {
            std::cout << "Failed to add delete operation to transaction." << std::endl;    
        }
        std::cout << "In transaction mode. Use 'commit' to apply changes." << std::endl;

        return;
    }
    
    if (client_.Delete(key)) {
        std::cout << "Deleted: " << key << std::endl;
    } else {
        std::cout << "Failed to delete: " << key << std::endl;
    }
}

void CommandParser::handle_multiput(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3 || (tokens.size() - 1) % 2 != 0) {
        std::cout << "Usage: multiput <key1> <val1> <key2> <val2> ..." << std::endl;
        return;
    }
    
    std::vector<std::pair<std::string, std::string>> pairs;
    for (size_t i = 1; i < tokens.size(); i += 2) {
        pairs.emplace_back(tokens[i], tokens[i + 1]);
    }
    
    if (client_.MultiPut(pairs)) {
        std::cout << "Successfully put " << pairs.size() << " items" << std::endl;
    } else {
        std::cout << "Failed to perform multi-put" << std::endl;
    }
}

void CommandParser::handle_multiget(const std::vector<std::string>& tokens) { 
    if (tokens.size() < 2) {
        std::cout << "Usage: multiget <key1> <key2> ..." << std::endl;
        return;
    }
    
    std::vector<std::string> keys(tokens.begin() + 1, tokens.end());
    
    auto result = client_.MultiGet(keys);
    
    if (result.success) {
        std::cout << "Found " << result.found_count << " of " << keys.size() << " keys:" << std::endl;
        
        for (const auto& item : result.items) {
            if (item.found) {
                // 处理空字符串值的情况
                if (item.value.empty()) {
                    std::cout << "  ✓ " << item.key << " = (empty string)" << std::endl;
                } else {
                    std::cout << "  ✓ " << item.key << " = " << item.value << std::endl;
                }
            } else {
                std::cout << "  ✗ " << item.key << " = (not found)" << std::endl;
            }
        }
    } else {
        std::cout << "MultiGet failed: " << result.error << std::endl;
    }
}

void CommandParser::handle_scan(const std::vector<std::string>& tokens) {
    if (tokens.size() < 3) {
        std::cout << "Usage: scan <start_key> <end_key> [limit]" << std::endl;
        return;
    }
    
    std::string start = tokens[1];
    std::string end = tokens[2];
    uint32_t limit = (tokens.size() > 3) ? std::stoul(tokens[3]) : 0;
    
    auto results = client_.Scan(start, end, limit);
    
    std::cout << "Found " << results.size() << " keys:" << std::endl;
    for (const auto& [key, value] : results) {
        std::cout << "  " << key << " = " << value << std::endl;
    }
}

void CommandParser::handle_prefix(const std::vector<std::string>& tokens) {
    if (tokens.size() < 2) {
        std::cout << "Usage: prefix <prefix>" << std::endl;
        return;
    }
    
    std::string prefix = tokens[1];
    // 使用范围查询实现前缀查询
    auto results = client_.Scan(prefix, prefix + "z");
    
    std::cout << "Found " << results.size() << " keys with prefix '" << prefix << "':" << std::endl;
    for (const auto& [key, value] : results) {
        std::cout << "  " << key << " = " << value << std::endl;
    }
}

void CommandParser::handle_begin(const std::vector<std::string>& tokens) {
    if (in_transaction_) {
        std::cout << "Already in a transaction. Commit or rollback first." << std::endl;
        return;
    }
    
    current_txn_id_ = client_.BeginTransaction();
    if (current_txn_id_ > 0) {
        in_transaction_ = true;
        std::cout << "Transaction started. ID: " << current_txn_id_ << std::endl;
        std::cout << "Use 'commit' to apply changes or 'rollback' to cancel." << std::endl;
    } else {
        std::cout << "Failed to start transaction" << std::endl;
    }
}

void CommandParser::handle_commit(const std::vector<std::string>& tokens) {
    if (!in_transaction_) {
        std::cout << "No active transaction. Use 'begin' to start one." << std::endl;
        return;
    }
    
    if (client_.CommitTransaction(current_txn_id_)) {
        std::cout << "Transaction committed successfully!" << std::endl;
    } else {
        std::cout << "Transaction commit failed due to conflict!" << std::endl;
    }
    
    in_transaction_ = false;
    current_txn_id_ = 0;
}

void CommandParser::handle_rollback(const std::vector<std::string>& tokens) {
    if (!in_transaction_) {
        std::cout << "No active transaction to rollback." << std::endl;
        return;
    }
    
    client_.RollbackTransaction(current_txn_id_);
    std::cout << "Transaction rolled back." << std::endl;
    
    in_transaction_ = false;
    current_txn_id_ = 0;
}

void CommandParser::handle_flush(const std::vector<std::string>& tokens) {
    if (client_.Flush()) {
        std::cout << "Memtable flushed to SSTable successfully!" << std::endl;
    } else {
        std::cout << "Failed to flush memtable" << std::endl;
    }
}

void CommandParser::handle_stats(const std::vector<std::string>& tokens) {
    std::cout << client_.GetStats();
}

void CommandParser::handle_ping(const std::vector<std::string>& tokens) {
    if (client_.Ping()) {
        std::cout << "Server is alive!" << std::endl;
    } else {
        std::cout << "Server is not responding!" << std::endl;
    }
}

} // namespace kvstore