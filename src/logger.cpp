// src/logger.cpp
#include "logger.h"
#include <iomanip>
#include <ctime>

namespace kvstore {
using std::string;

Logger* g_logger = nullptr;

Logger::Logger(const string& filename, LogLevel level) : min_level(level) {
    file.open(filename, std::ios::out | std::ios::app);
}

Logger::~Logger() {
    if (file.is_open()) {
        file.close();
    }
}

string Logger::getTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    
    std::tm bt;
#ifdef _WIN32
    localtime_s(&bt, &now_c);
#else
    localtime_r(&now_c, &bt);
#endif
    
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &bt);
    
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    char result[64];
    snprintf(result, sizeof(result), "%s.%03lld", buffer, 
             static_cast<long long>(ms.count()));
    
    return result;
}

string Logger::levelToString(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        default: return "UNKN ";
    }
}

void Logger::log(LogLevel level, const string& message) {
    if (level < min_level) return;
    
    std::lock_guard<std::mutex> lock(mutex);
    file << getTimestamp() << " [" << levelToString(level) << "] " << message << std::endl;
    file.flush();
}

} // namespace kvstore