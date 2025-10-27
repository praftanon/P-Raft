#!/bin/bash
# tc_netem_82.sh
# 在 192.168.0.82 上配置不同目的 IP 的网络延迟

# 网卡名称
DEV="eth0"

# 先清除已有的 qdisc 配置
sudo tc qdisc del dev $DEV root 2>/dev/null

# 1. 创建基础 prio 队列
sudo tc qdisc add dev $DEV root handle 1: prio

# 2. 为目标 228 创建 15ms 延迟通道 (band 3)
sudo tc qdisc add dev $DEV parent 1:3 handle 30: netem delay 15ms

# 3. 为目标 93 创建 25ms 延迟通道 (band 1)
sudo tc qdisc add dev $DEV parent 1:1 handle 40: netem delay 25ms

# 4. 创建过滤器，将发往 228 的流量导向 band 3
sudo tc filter add dev $DEV protocol ip parent 1:0 prio 1 u32 \
    match ip dst 192.168.0.38/32 flowid 1:3

# 5. 创建过滤器，将发往 93 的流量导向 band 1
sudo tc filter add dev $DEV protocol ip parent 1:0 prio 2 u32 \
    match ip dst 192.168.0.223/32 flowid 1:1

echo "配置完成！"