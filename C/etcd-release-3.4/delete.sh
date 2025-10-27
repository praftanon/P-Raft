#!/bin/bash
rm -rf /etcd/etcd-release-3.4/leader_move_metrics*.csv
rm -rf /etcd/etcd-release-3.4/move_leader_time*.csv
#rm -rf /etcd/etcd-release-3.4/mae_avgC.csv

# 删除 /root/artifacts 目录
rm -rf /root/artifacts

#echo "[DEL] /root/artifacts 已删除"

# 删除 ycsb-logs 目录
rm -rf /etcd/go-ycsb-master/go-ycsb-master/ycsb-logs
echo "[DEL] ycsb-logs 已删除"

rm -rf /etcd/etcd-release-3.4/predicted_leader*.csv


# 删除 times*.csv 文件
rm -f /etcd/etcd-release-3.4/times*.csv
echo "[DEL] times*.csv 已删除"

# 删除 raft_stats.csv 文件（如果存在）
if [ -f /etcd/etcd-release-3.4/raft_stats.csv ]; then
    rm -f /etcd/etcd-release-3.4/raft_stats.csv
    echo "[DEL] raft_stats.csv 已删除"
fi

