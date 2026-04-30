# raft/CMakeLists.txt
cmake_minimum_required(VERSION 3.15)
project(Raft)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找依赖
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# 查找 gRPC 和 Abseil
find_package(gRPC CONFIG)
find_package(absl REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(c-ares REQUIRED)

# 设置路径
set(RAFT_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(RAFT_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(RAFT_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../proto)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O0 -pthread -g")

# 查找 grpc_cpp_plugin
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin)
if(NOT GRPC_CPP_PLUGIN)
    set(GRPC_CPP_PLUGIN "/usr/bin/grpc_cpp_plugin")
endif()

if(GRPC_CPP_PLUGIN)
    message(STATUS "Found grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")
endif()

# 生成 protobuf 文件
set(PROTO_FILES ${RAFT_PROTO_DIR}/raft.proto)
get_filename_component(PROTO_WE ${PROTO_FILES} NAME_WE)
set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.pb.h)
set(GRPC_SRCS ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.grpc.pb.cc)
set(GRPC_HDRS ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.grpc.pb.h)

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
             --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             -I ${RAFT_PROTO_DIR}
             ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf files for raft"
)

# 头文件目录
include_directories(${RAFT_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${Protobuf_INCLUDE_DIRS})

if(gRPC_FOUND)
    include_directories(${gRPC_INCLUDE_DIRS})
endif()

# 源文件列表
set(RAFT_SOURCES
    ${RAFT_SRC_DIR}/raft.cpp
    ${RAFT_SRC_DIR}/raft_node.cpp
    ${RAFT_SRC_DIR}/raft_log.cpp
    ${RAFT_SRC_DIR}/raft_state_machine.cpp
    ${RAFT_SRC_DIR}/raft_persistence.cpp
    ${RAFT_SRC_DIR}/raft_transport.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# 创建 raft 静态库
add_library(raft STATIC ${RAFT_SOURCES})

# 链接依赖（包含 Abseil 库）
target_link_libraries(raft
    ${Protobuf_LIBRARIES}
    Threads::Threads
    gRPC::grpc++
    gRPC::grpc
    gRPC::gpr
    absl::synchronization
    absl::strings
    absl::time
    absl::base
    absl::malloc_internal
    ${OpenSSL_LIBRARIES}
    ${c-ares_LIBRARIES}
)

# 导出头文件路径
target_include_directories(raft PUBLIC
    ${RAFT_INCLUDE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
)

# 编译测试
option(BUILD_RAFT_TESTS "Build Raft tests" ON)
if(BUILD_RAFT_TESTS)
    add_executable(raft_test ${RAFT_SRC_DIR}/../test/raft_test.cpp)
    target_link_libraries(raft_test raft 
        ${Protobuf_LIBRARIES} 
        Threads::Threads
        gRPC::grpc++
        gRPC::grpc
        gRPC::gpr
        absl::synchronization
        absl::strings
        absl::time
        absl::base
        absl::malloc_internal
        ${OpenSSL_LIBRARIES}
        ${c-ares_LIBRARIES}
    )
    target_include_directories(raft_test PRIVATE ${RAFT_INCLUDE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
endif()

# 安装
install(TARGETS raft DESTINATION lib)
install(DIRECTORY ${RAFT_INCLUDE_DIR}/ DESTINATION include/raft)