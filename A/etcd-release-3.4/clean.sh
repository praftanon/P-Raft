#!/bin/bash

# 删除 etcd 临时目录
rm -rf /tmp/etcd1/*
rm -rf /tmp/etcd2/*
rm -rf /tmp/etcd3/*

# 删除当前目录下的 .log 文件
rm -f ./*.log

echo "清理完成 ✅"

