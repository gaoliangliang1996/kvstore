// client/client_main.cpp
#include "client.h"
#include "command_parser.h"
#include <iostream>
#include <string>

void print_help() {
    std::cout << "\n========== KVStore Client Commands ==========\n"
              << "  put <key> <value>           - Store a key-value pair\n"
              << "  get <key>                   - Retrieve value for a key\n"
              << "  del <key>                   - Delete a key\n"
              << "  multiput <key1> <val1> <key2> <val2> ... - Batch put\n"
              << "  multiget <key1> <key2> ...  - Batch get\n"
              << "  scan <start> <end> [limit]  - Range scan\n"
              << "  prefix <prefix>             - Prefix scan\n"
              << "  begin                       - Begin transaction\n"
              << "  commit                      - Commit current transaction\n"
              << "  rollback                    - Rollback current transaction\n"
              << "  flush                       - Flush memtable to SSTable\n"
              << "  stats                       - Show server statistics\n"
              << "  ping                        - Ping the server\n"
              << "  help                        - Show this help\n"
              << "  quit / exit                 - Exit client\n"
              << "==============================================\n";
}

int main(int argc, char* argv[]) {
    std::string server_addr = "localhost:50051";
    
    // 解析命令行参数
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--addr" && i + 1 < argc) {
            server_addr = argv[++i];
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [--addr <server_address>]\n";
            return 0;
        }
    }
    
    std::cout << "KVStore Client starting..." << std::endl;
    std::cout << "Connecting to server: " << server_addr << std::endl;
    
    kvstore::KVClient client(server_addr);
    
    if (!client.Ping()) {
        std::cerr << "Failed to connect to server at " << server_addr << std::endl;
        return 1;
    }
    
    std::cout << "Connected to server successfully!" << std::endl;
    print_help();
    
    kvstore::CommandParser parser(client);
    std::string line;
    
    while (true) {
        std::cout << "\n>> ";
        std::getline(std::cin, line);
        
        if (line.empty()) continue;
        
        if (line == "quit" || line == "exit") {
            std::cout << "Goodbye!" << std::endl;
            break;
        }
        
        if (line == "help") {
            print_help();
            continue;
        }
        
        parser.Execute(line);
    }
    
    return 0;
}