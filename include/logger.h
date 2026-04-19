// include/logger.h
#pragma once
#include <string>
#include <fstream>
#include <chrono>
#include <mutex>

namespace kvstore {

using std::string;

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

class Logger {
private:
    std::ofstream file;
    LogLevel min_level;
    std::mutex mutex;
    
    string getTimestamp();
    string levelToString(LogLevel level);
    
public:
    Logger(const string& filename, LogLevel level = LogLevel::DEBUG);
    ~Logger();
    
    void log(LogLevel level, const string& message);
    
    void debug(const string& msg) { log(LogLevel::DEBUG, msg); }
    void info(const string& msg) { log(LogLevel::INFO, msg); }
    void warn(const string& msg) { log(LogLevel::WARN, msg); }
    void error(const string& msg) { log(LogLevel::ERROR, msg); }
};

extern Logger* g_logger;

#define LOG_DEBUG(msg) if (g_logger) g_logger->debug(msg)
#define LOG_INFO(msg)  if (g_logger) g_logger->info(msg)
#define LOG_WARN(msg)  if (g_logger) g_logger->warn(msg)
#define LOG_ERROR(msg) if (g_logger) g_logger->error(msg)

} // namespace kvstore