#pragma once
#include "common.h"
#include <string>
#include <unordered_map>

namespace kvstore {

enum class IsolationLevel {
    READ_UNCOMMITTED = 0,   // 读未提交
    READ_COMMITTED = 1,     // 读已提交
    REPEATABLE_READ = 2,    // 可重复读
    SNAPSHOT_ISOLATION = 3, // 快照隔离（默认）
    SERIALIZABLE = 4        // 可串行化
};

// 隔离级别工具类
class IsolationLevelUtils {
public:
    // 枚举转字符串
    static std::string toString(IsolationLevel level) {
        switch (level) {
            case IsolationLevel::READ_UNCOMMITTED:   return "READ_UNCOMMITTED";
            case IsolationLevel::READ_COMMITTED:     return "READ_COMMITTED";
            case IsolationLevel::REPEATABLE_READ:    return "REPEATABLE_READ";
            case IsolationLevel::SNAPSHOT_ISOLATION: return "SNAPSHOT_ISOLATION";
            case IsolationLevel::SERIALIZABLE:       return "SERIALIZABLE";
            default:                                 return "UNKNOWN";
        }
    }
    
    // 字符串转枚举
    static IsolationLevel fromString(const std::string& str) {
        static const std::unordered_map<std::string, IsolationLevel> map = {
            {"READ_UNCOMMITTED", IsolationLevel::READ_UNCOMMITTED},
            {"0", IsolationLevel::READ_UNCOMMITTED},
            {"READ_COMMITTED", IsolationLevel::READ_COMMITTED},
            {"1", IsolationLevel::READ_COMMITTED},
            {"REPEATABLE_READ", IsolationLevel::REPEATABLE_READ},
            {"2", IsolationLevel::REPEATABLE_READ},
            {"SNAPSHOT_ISOLATION", IsolationLevel::SNAPSHOT_ISOLATION},
            {"3", IsolationLevel::SNAPSHOT_ISOLATION},
            {"SERIALIZABLE", IsolationLevel::SERIALIZABLE},
            {"4", IsolationLevel::SERIALIZABLE}
        };
        
        auto it = map.find(str);
        if (it != map.end()) {
            return it->second;
        }
        return IsolationLevel::SNAPSHOT_ISOLATION;  // 默认
    }
    
    // 获取级别的描述信息
    static std::string getDescription(IsolationLevel level) {
        switch (level) {
            case IsolationLevel::READ_UNCOMMITTED:
                return "Allows dirty reads. Lowest consistency, highest performance.";
            case IsolationLevel::READ_COMMITTED:
                return "Prevents dirty reads. May have non-repeatable reads.";
            case IsolationLevel::REPEATABLE_READ:
                return "Prevents dirty reads and non-repeatable reads. May have phantom reads.";
            case IsolationLevel::SNAPSHOT_ISOLATION:
                return "Prevents all anomalies using snapshot version. Default level.";
            case IsolationLevel::SERIALIZABLE:
                return "Highest isolation level. Complete transaction isolation.";
            default:
                return "Unknown";
        }
    }
    
    // 获取级别的数值
    static int toInt(IsolationLevel level) {
        return static_cast<int>(level);
    }
    
    // 从数值获取级别
    static IsolationLevel fromInt(int value) {
        if (value >= 0 && value <= 4) {
            return static_cast<IsolationLevel>(value);
        }
        return IsolationLevel::SNAPSHOT_ISOLATION;
    }
};

} // namespace kvstore