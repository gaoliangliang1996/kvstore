// include/write_batch.h
#pragma once
#include "common.h"
#include <vector>

namespace kvstore {

class WriteBatch {
public:
    enum OpType { PUT, DELETE };
    
    struct Operation {
        OpType type;
        string key;
        string value;
    };
    
    void Put(const string& key, const string& value) {
        ops_.push_back({PUT, key, value});
    }
    
    void Delete(const string& key) {
        ops_.push_back({DELETE, key, ""});
    }
    
    size_t Size() const { return ops_.size(); }
    bool Empty() const { return ops_.empty(); }
    void Clear() { ops_.clear(); }
    
    const std::vector<Operation>& GetOps() const { return ops_; }
    
private:
    std::vector<Operation> ops_;
};

} // namespace kvstore