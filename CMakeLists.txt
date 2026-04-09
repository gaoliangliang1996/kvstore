cmake_minimum_required(VERSION 3.10)
project(KVStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖
find_package(ZLIB REQUIRED)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O0 -pthread -g")

# 头文件目录
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${ZLIB_INCLUDE_DIRS})
    
# 源文件
set(SOURCES
    src/kvstore.cpp
    src/wal.cpp
    src/sstable.cpp
    src/compaction.cpp
    src/memtable_manager.cpp
    src/bloom_filter.cpp
    src/lru_cache.cpp
)

# 静态库
add_library(kvstore STATIC ${SOURCES})

# 可执行文件
add_executable(kvstore_test test/test.cpp)
target_link_libraries(kvstore_test kvstore ${ZLIB_LIBRARIES} pthread)

# 解析 SSTable
add_executable(parse_sstable tools/parse_sstable.cpp)
target_link_libraries(parse_sstable z)

# 解析 WAL
add_executable(parse_wal tools/parse_wal.cpp)
target_link_libraries(parse_wal z)

# # 通用解析器
# add_executable(parse_all tools/parse_all.cpp tools/parse_sstable.cpp tools/parse_wal.cpp)
# target_link_libraries(parse_all z)

# # 安装
# install(TARGET kvstore DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)