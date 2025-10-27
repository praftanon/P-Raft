#!/bin/bash

ETCD_BIN=./bin/etcd

pids=()

./tc_netem223.sh

$ETCD_BIN --config-file=./node3_1.yaml > node3_1.log 2>&1 &
pids+=($!)
echo "Started etcd node3_1 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node3_2.yaml > node3_2.log 2>&1 &
pids+=($!)
echo "Started etcd node3_2 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node3_3.yaml > node3_3.log 2>&1 &
pids+=($!)
echo "Started etcd node3_3 (PID ${pids[-1]})"

# 捕获 Ctrl+C，杀掉所有 etcd
trap "echo 'Stopping etcd...'; kill ${pids[@]}; exit" INT

echo "All etcd nodes started. Press Ctrl+C to stop."

# 等待所有子进程
wait

