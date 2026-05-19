# /home/ubuntu/kvstore/CMakeLists.txt

cmake_minimum_required(VERSION 3.15)
project(KVStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O0 -pthread -g")

# 查找依赖
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# 查找 grpc_cpp_plugin
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN)
    message(FATAL_ERROR "grpc_cpp_plugin not found!")
endif()

# 设置 protobuf 生成目录
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# 生成 kvstore.proto
set(PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/kvstore.proto)

# 生成 C++ 代码
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS} PROPERTIES GENERATED TRUE)

# 生成 gRPC 代码
set(GRPC_SRCS ${PROTO_GEN_DIR}/kvstore.grpc.pb.cc)
set(GRPC_HDRS ${PROTO_GEN_DIR}/kvstore.grpc.pb.h)

add_custom_command(
    OUTPUT ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --grpc_out=${PROTO_GEN_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        --proto_path=${CMAKE_SOURCE_DIR}/proto
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating gRPC code for kvstore.proto"
)

# 添加子目录
add_subdirectory(raft)

# 头文件目录
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/raft/include)
include_directories(${ZLIB_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${PROTO_GEN_DIR})  # 关键：添加生成目录

# KVStore 核心源文件
set(KVSTORE_SOURCES
    src/wal.cpp
    src/sstable.cpp
    src/logger.cpp
    src/compaction.cpp
    src/bloom_filter.cpp
    src/lru_cache.cpp
    src/mvcc_sstable.cpp
    src/mvcc_kvstore.cpp
    src/transaction.cpp
)

# RaftKVStore 源文件（暂时排除 raft_grpc_service.cpp）
set(RAFT_KVSTORE_SOURCES
    src/raft_kvstore.cpp
)

# 静态库
add_library(kvstore_core STATIC ${KVSTORE_SOURCES})
target_link_libraries(kvstore_core ${ZLIB_LIBRARIES} Threads::Threads)

add_library(kvstore_raft STATIC ${RAFT_KVSTORE_SOURCES})
target_link_libraries(kvstore_raft 
    kvstore_core 
    raft 
    ${ZLIB_LIBRARIES} 
    Threads::Threads
)

# 测试程序
add_executable(raft_kvstore_test test/raft_kvstore_test.cpp)
target_link_libraries(raft_kvstore_test 
    kvstore_raft 
    kvstore_core 
    raft 
    ${ZLIB_LIBRARIES} 
    pthread
)

# gRPC 服务器（可选，暂时禁用）
# add_executable(raft_kvstore_server
#     server/raft_server_main.cpp
#     src/raft_grpc_service.cpp
#     ${PROTO_SRCS}
#     ${GRPC_SRCS}
# )
# target_link_libraries(raft_kvstore_server
#     kvstore_raft
#     kvstore_core
#     raft
#     ${ZLIB_LIBRARIES}
#     Threads::Threads
#     gRPC::grpc++
#     protobuf::libprotobuf
# )