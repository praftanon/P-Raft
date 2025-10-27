#!/bin/bash

ETCD_BIN=./bin/etcd

pids=()

./tc_netem82.sh

$ETCD_BIN --config-file=./node2_1.yaml > node2_1.log 2>&1 &
pids+=($!)
echo "Started etcd node2_1 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node2_2.yaml > node2_2.log 2>&1 &
pids+=($!)
echo "Started etcd node2_2 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node2_3.yaml > node2_3.log 2>&1 &
pids+=($!)
echo "Started etcd node2_3 (PID ${pids[-1]})"

# 捕获 Ctrl+C，杀掉所有 etcd
trap "echo 'Stopping etcd...'; kill ${pids[@]}; exit" INT

echo "All etcd nodes started. Press Ctrl+C to stop."

# 等待所有子进程
wait

