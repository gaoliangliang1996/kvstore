cmake_minimum_required(VERSION 3.15)
project(KVStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

# 找到 grpc_cpp_plugin 的路径
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)

# 生成 protobuf 文件
set(PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/kvstore.proto)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# 手动生成 gRPC 文件
set(GRPC_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(GRPC_SRCS ${GRPC_OUTPUT_DIR}/kvstore.grpc.pb.cc)
set(GRPC_HDRS ${GRPC_OUTPUT_DIR}/kvstore.grpc.pb.h)

add_custom_command(
    OUTPUT ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --grpc_out=${GRPC_OUTPUT_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        --proto_path=${CMAKE_SOURCE_DIR}/proto
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Running gRPC C++ protocol buffer compiler"
)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O0 -pthread -g")

# 头文件目录
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${ZLIB_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# 源文件
set(SOURCES
    src/wal.cpp
    src/sstable.cpp
    src/logger.cpp
    src/compaction.cpp
    src/bloom_filter.cpp
    src/lru_cache.cpp
    src/mvcc_sstable.cpp
    src/mvcc_kvstore.cpp
    src/range_query.cpp
    src/transaction.cpp
    src/server.cpp
    src/client.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# 静态库
add_library(kvstore STATIC ${SOURCES})
target_link_libraries(kvstore 
    ${ZLIB_LIBRARIES} 
    Threads::Threads
    gRPC::grpc++
    protobuf::libprotobuf
)

# 解析 WAL
add_executable(parse_wal tools/parse_wal.cpp)
target_link_libraries(parse_wal z)

# 解析 SSTable
add_executable(parse_sstable tools/parse_sstable.cpp)
target_link_libraries(parse_sstable z)

# 可执行文件
add_executable(kvstore_server test/main.cpp ${SOURCES})
target_link_libraries(kvstore_server 
    Threads::Threads
    gRPC::grpc++
    protobuf::libprotobuf
)

# # 也可以编译客户端单独测试
add_executable(kvstore_client test/main.cpp ${SOURCES})
target_link_libraries(kvstore_client 
    Threads::Threads
    gRPC::grpc++
    protobuf::libprotobuf
)