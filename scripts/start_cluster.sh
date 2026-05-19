#!/bin/bash

# 启动 3 节点 Raft 集群

# 清理旧数据
rm -rf /tmp/raft-data-*

# 启动节点 1
./build/raft_kvstore_server \
    --node-id=node-1 \
    --addr=0.0.0.0:50051 \
    --peers=node-1:localhost:50051,node-2:localhost:50052,node-3:localhost:50053 \
    --data-dir=/tmp/raft-data-1 &

# 启动节点 2
./build/raft_kvstore_server \
    --node-id=node-2 \
    --addr=0.0.0.0:50052 \
    --peers=node-1:localhost:50051,node-2:localhost:50052,node-3:localhost:50053 \
    --data-dir=/tmp/raft-data-2 &

# 启动节点 3
./build/raft_kvstore_server \
    --node-id=node-3 \
    --addr=0.0.0.0:50053 \
    --peers=node-1:localhost:50051,node-2:localhost:50052,node-3:localhost:50053 \
    --data-dir=/tmp/raft-data-3 &

echo "Raft cluster started"