#!/bin/bash

ETCD_BIN=./bin/etcd

pids=()

./tc_netem38.sh

$ETCD_BIN --config-file=./node1_1.yaml > node1_1.log 2>&1 &
pids+=($!)
echo "Started etcd node1_1 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node1_2.yaml > node1_2.log 2>&1 &
pids+=($!)
echo "Started etcd node1_2 (PID ${pids[-1]})"

$ETCD_BIN --config-file=./node1_3.yaml > node1_3.log 2>&1 &
pids+=($!)
echo "Started etcd node1_3 (PID ${pids[-1]})"

# 捕获 Ctrl+C，杀掉所有 etcd
trap "echo 'Stopping etcd...'; kill ${pids[@]}; exit" INT

echo "All etcd nodes started. Press Ctrl+C to stop."

# 等待所有子进程
wait

