#!/bin/bash

# ------------------------------
# 配置
# ------------------------------
ETCDCTL="./bin/etcdctl"

ENDPOINTS="http://192.168.0.38:2379,http://192.168.0.38:3379,http://192.168.0.38:4379,\
http://192.168.0.82:2379,http://192.168.0.82:3379,http://192.168.0.82:4379,\
http://192.168.0.223:2379,http://192.168.0.223:3379,http://192.168.0.223:4379"

# ------------------------------
# 获取本机 IP (取第一块非 127.0.0.1 的地址)
# ------------------------------
local_ip=$(hostname -I | awk '{print $1}')

# ------------------------------
# 获取 Leader 地址 (http://ip:port)
# ------------------------------
leader=$($ETCDCTL --endpoints=$ENDPOINTS endpoint status --write-out=table \
    | grep true | awk '{print $2}')

# ------------------------------
# 提取 Leader IP
# ------------------------------
leader_ip=$(echo "$leader" | awk -F[/:] '{print $4}')

# ------------------------------
# 输出并比较
# ------------------------------
echo "本机 IP: $local_ip"
echo "Leader IP: $leader_ip"

if [ "$local_ip" == "$leader_ip" ]; then
    echo "✅ 本机是 Leader"
else
    echo "❌ 本机不是 Leader"
fi

