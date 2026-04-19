cmake_minimum_required(VERSION 3.15)
project(KVStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

# 查找 gRPC 插件
if(gRPC_FOUND)
    # 方法1：使用 gRPC 的 CMake 目标获取插件路径
    get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)
    
    # 方法2：如果方法1失败，尝试手动查找
    if(NOT GRPC_CPP_PLUGIN)
        find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
            PATHS
            /usr/local/bin
            /usr/bin
            ${gRPC_DIR}/bin
            ${GRPC_ROOT}/bin
            $ENV{HOME}/.local/bin
        )
    endif()
    
    if(NOT GRPC_CPP_PLUGIN)
        message(FATAL_ERROR "grpc_cpp_plugin not found. Please install gRPC properly.")
    endif()
    
    message(STATUS "Found grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")
endif()

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O0 -pthread -g")

# 头文件目录
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${ZLIB_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# 设置 protobuf 生成的文件
set(PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/kvstore.proto)
set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.h)
set(GRPC_SRCS ${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.cc)
set(GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.h)

# 生成 protobuf 和 grpc 文件
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --proto_path=${CMAKE_SOURCE_DIR}/proto
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
        --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf and gRPC files"
    VERBATIM
)

# 源文件
set(SOURCES
    src/kvstore.cpp
    src/wal.cpp
    src/sstable.cpp
    src/logger.cpp
    src/compaction.cpp
    # src/memtable_manager.cpp
    src/bloom_filter.cpp
    src/lru_cache.cpp
    src/mvcc_sstable.cpp
    src/mvcc_kvstore.cpp
    src/range_query.cpp
    src/transaction.cpp
    src/grpc_service.cpp
    src/raft.cpp
    src/raft_kvstore.cpp
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

# 可执行文件
add_executable(kvstore_test test/test.cpp)
target_link_libraries(kvstore_test kvstore ${ZLIB_LIBRARIES} pthread)

# 可执行文件
add_executable(range_query_test test/range_query_test.cpp)
target_link_libraries(range_query_test kvstore ${ZLIB_LIBRARIES} pthread)
add_executable(transaction_test test/transaction_test.cpp)
target_link_libraries(transaction_test kvstore ${ZLIB_LIBRARIES} pthread)

add_executable(test_raft test/test_raft.cpp src/raft.cpp)
target_link_libraries(test_raft pthread stdc++fs)

# 解析 WAL
add_executable(parse_wal tools/parse_wal.cpp)
target_link_libraries(parse_wal z)

# 解析 SSTable
add_executable(parse_sstable tools/parse_sstable.cpp)
target_link_libraries(parse_sstable z)


# 可执行文件
add_executable(kvstore_server test/server.cpp)
target_link_libraries(kvstore_server kvstore 
    ${ZLIB_LIBRARIES} 
    Threads::Threads
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(kvstore_client test/client.cpp)
target_link_libraries(kvstore_client kvstore
    ${ZLIB_LIBRARIES}
    Threads::Threads
    gRPC::grpc++
    protobuf::libprotobuf
)

# add_executable(raft_test test/raft_test.cpp)
# target_link_libraries(raft_test kvstore Threads::Threads)
