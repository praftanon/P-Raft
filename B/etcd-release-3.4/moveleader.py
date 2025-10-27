#!/usr/bin/env python3
"""
Optimal Raft Leader Calculator
根据延迟和请求负载计算最优的Raft领导者节点
"""

import csv
import math
import sys
import time
from typing import List, Dict, Tuple, Optional
import requests
import json

import os
import subprocess
import socket

class Domain:
    def __init__(self, domain_id: int, address: str, nodes: int, read_requests: int = 0, write_requests: int = 0):
        self.id = domain_id
        self.address = address  # IP地址
        self.nodes = nodes
        self.read_requests = read_requests
        self.write_requests = write_requests

class OptimalLeaderCalculator:
    def __init__(self):
        # 硬编码的延迟矩阵 (RTT in ms)
        # IP映射: 192.168.0.38 -> 0, 192.168.0.82 -> 1, 192.168.0.223 -> 2
        self.ip_to_index = {
            "192.168.0.38": 0,
            "192.168.0.82": 1, 
            "192.168.0.223": 2
        }
        
        self.index_to_ip = {v: k for k, v in self.ip_to_index.items()}
        
        # 延迟矩阵 (双向RTT)
        self.latency_matrix = [
            [0,  30, 40],  # 192.168.0.38 到其他节点的延迟
            [30, 0,  50],  # 192.168.0.82 到其他节点的延迟  
            [40, 50, 0]    # 192.168.0.223 到其他节点的延迟
        ]

        self.ip_groups = {
            "192.168.0.38": ["933ca51d2bb602b8", "6181f76d6668aeb0", "69948e3d245f62b7"],
            "192.168.0.82": ["b92b49a4de72942d", "3cdaf029c87a002", "5b3ba363fb10d52f"],  
            "192.168.0.223": ["69f554c3f7f50a72", "8861d1f6a0217629", "512e070e8eb32959"]
        }

        self.index_to_ip = {v: k for k, v in self.ip_to_index.items()}

        self.IpToId = {
            '192.168.0.38:2379': '933ca51d2bb602b8',
            '192.168.0.38:3379': '6181f76d6668aeb0',
            '192.168.0.38:4379': '69948e3d245f62b7',
            '192.168.0.82:2379': 'b92b49a4de72942d',
            '192.168.0.82:3379': '3cdaf029c87a002',
            '192.168.0.82:4379': '5b3ba363fb10d52f',
            '192.168.0.223:2379': '69f554c3f7f50a72',
            '192.168.0.223:3379': '8861d1f6a0217629',
            '192.168.0.223:4379': '512e070e8eb32959'
        }

        self.node_id_to_ip={
            '933ca51d2bb602b8': '192.168.0.38',
            '6181f76d6668aeb0': '192.168.0.38',
            '69948e3d245f62b7': '192.168.0.38',
            'b92b49a4de72942d': '192.168.0.82',
            '3cdaf029c87a002': '192.168.0.82',
            '5b3ba363fb10d52f': '192.168.0.82',
            '69f554c3f7f50a72': '192.168.0.223',
            '8861d1f6a0217629': '192.168.0.223',
            '512e070e8eb32959': '192.168.0.223'
        }

    def read_raft_stats(self, csv_path: str) -> List[Domain]:
        """
        读取raft_stats.csv文件的最后一行数据
        CSV格式: timestamp, node1_write, node1_read, node2_write, node2_read, ...
        """
        domains = []
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                rows = list(reader)
                
                if len(rows) < 2:
                    print("CSV文件数据不足")
                    return domains
                
                # 获取标题行和最后一行数据
                header = rows[0]
                last_row = rows[-1]
                
                # print(f"CSV标题行: {header[:5]}...")  # 只显示前5列
                # print(f"最后一行数据: {last_row[:5]}...")  # 只显示前5列
                
                # 解析标题行，提取节点ID
                node_ids = []
                for col in header[1:]:  # 跳过timestamp列
                    if col.endswith('_write'):
                        node_id = col.replace('_write', '')
                        node_ids.append(node_id)
                
                print(f"发现节点IDs: {node_ids}")
                
                # 解析数据行
                timestamp = last_row[0]
                col_index = 1
                
                for node_id in node_ids:
                    # 获取该节点的写和读请求数
                    write_requests = int(last_row[col_index])
                    read_requests = int(last_row[col_index + 1])
                    col_index += 2
                    
                    # 跳过没有数据的节点
                    if write_requests == 0 and read_requests == 0:
                        continue
                    
                    # 获取IP地址（需要映射关系）
                    ip_address = self.node_id_to_ip.get(node_id, "unknown")
                    if ip_address == "unknown":
                        print(f"警告: 节点 {node_id} 的IP地址未知，跳过")
                        continue
                    
                    # 假设每个节点只有1个节点（可以根据实际情况调整）
                    nodes = 1
                    
                    domain = Domain(node_id, ip_address, nodes, read_requests, write_requests)
                    domains.append(domain)
                    
                    print(f"解析域信息: ID={node_id[:8]}..., IP={ip_address}, "
                          f"读请求={read_requests}, 写请求={write_requests}")
                    
        except FileNotFoundError:
            print(f"找不到文件: {csv_path}")
        except Exception as e:
            print(f"读取CSV文件时发生错误: {e}")
            
        return domains

    def get_latency(self, from_ip: str, to_ip: str) -> int:
        """获取两个IP之间的延迟"""
        if from_ip not in self.ip_to_index or to_ip not in self.ip_to_index:
            print(f"警告: 未知的IP地址 {from_ip} 或 {to_ip}")
            return 999  # 返回一个较大的延迟值
            
        from_idx = self.ip_to_index[from_ip]
        to_idx = self.ip_to_index[to_ip]
        return self.latency_matrix[from_idx][to_idx]

    def calculate_commit_latency(self, leader_domain: Domain, domains: List[Domain], quorum_size: int) -> int:
        """
        计算提交延迟 W_commit(p)
        领导者需要等待足够的跟随者响应以达到法定人数
        """
        leader_ip = leader_domain.address
        
        # 计算从领导者到所有其他域的延迟
        latencies = []
        for domain in domains:
            if domain.id != leader_domain.id:
                latency = self.get_latency(leader_ip, domain.address)
                latencies.append(latency)
        
        # 按延迟排序
        latencies.sort()
        
        # 法定人数需要包括领导者自身，所以需要 (quorum_size - 1) 个跟随者响应
        followers_needed = quorum_size - 1
        
        if followers_needed > len(latencies):
            print(f"无法达到法定人数: 需要{followers_needed}个跟随者，但只有{len(latencies)}个")
            return math.inf
        
        if followers_needed <= 0:
            return 0
            
        # 提交延迟是达到法定人数所需的最大延迟
        commit_latency = latencies[followers_needed - 1]
        return commit_latency

    def calculate_total_latency(self, leader_domain: Domain, domains: List[Domain], commit_latency: int) -> int:
        """
        计算总加权系统延迟 T(p)
        T(p) = Σ(i=1 to n) R_i * L(p,i) + W_commit(p) * Σ(i=1 to n) W_i
        """
        leader_ip = leader_domain.address
        total_latency = 0
        total_write_requests = 0
        
        for domain in domains:
            # 读请求延迟: R_i * L(p,i)  
            if domain.id == leader_domain.id:
                read_latency = 0  # 领导者处理本地读请求无延迟
            else:
                read_latency = self.get_latency(leader_ip, domain.address)
            
            total_latency += domain.read_requests * read_latency
            total_write_requests += domain.write_requests
            
            print(f"  域 {domain.id}: 读请求={domain.read_requests}, 写请求={domain.write_requests}, 延迟={read_latency}ms")
        
        # 写请求延迟: W_commit(p) * Σ W_i
        write_latency_component = commit_latency * total_write_requests
        total_latency += write_latency_component
        
        print(f"  写延迟组件: {commit_latency} * {total_write_requests} = {write_latency_component}")
        
        return total_latency

    def find_optimal_leader(self, domains: List[Domain]) -> Optional[Domain]:
        """
        找到最优的领导者域
        """
        if not domains:
            print("没有可用的域")
            return None
            
        # 计算总节点数和法定人数
        total_nodes = sum(domain.nodes for domain in domains)
        quorum_size = math.floor(total_nodes / 2) + 1
        
        print(f"总节点数: {total_nodes}")
        print(f"法定人数: {quorum_size}")
        print(f"域数量: {len(domains)}")
        print("=" * 50)
        
        min_total_latency = float('inf')
        optimal_leader = None
        
        # 遍历每个域作为潜在领导者
        for leader_candidate in domains:
            print(f"计算领导者候选域 {leader_candidate.id} (IP: {leader_candidate.address})...")
            
            # 1. 计算提交延迟
            commit_latency = self.calculate_commit_latency(leader_candidate, domains, quorum_size)
            if math.isinf(commit_latency):
                print(f"  跳过领导者 {leader_candidate.id}，无法达到法定人数")
                print("-" * 30)
                continue
                
            print(f"  提交延迟 W_commit({leader_candidate.id}): {commit_latency} ms")
            
            # 2. 计算总系统延迟  
            total_latency = self.calculate_total_latency(leader_candidate, domains, commit_latency)
            print(f"  总加权延迟 T({leader_candidate.id}): {total_latency}")
            print("-" * 30)
            
            # 3. 更新最优领导者
            if total_latency < min_total_latency:
                min_total_latency = total_latency  
                optimal_leader = leader_candidate
                
        return optimal_leader, min_total_latency if optimal_leader else (None, None)

    def scp_to_host(self, local_file, host, remote_path, user="root", port=22, key=None):
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"本地文件不存在: {local_file}")
            return

        scp_cmd = ["scp"]
        if key:
            scp_cmd += ["-i", key]
        if port and port != 22:
            scp_cmd += ["-P", str(port)]  # 注意 scp 使用大写 P 指定端口
        scp_cmd += [local_file, f"{user}@{host}:{remote_path}"]

        print("执行文件传输命令：", " ".join(scp_cmd))
        subprocess.run(scp_cmd, check=True)
        print(f"文件已传到 {host}:{remote_path}")

    def check_and_transfer_leader(self, current_leader_ip: str, optimal_leader_ip: str):
        local_path = "/etcd/etcd-release-3.4/raft_stats.csv"
        remote_path = "/etcd/etcd-release-3.4/raft_stats.csv"

        endpoint = optimal_leader_ip + ":2379"
        self.scp_to_host(local_path, endpoint, remote_path)

        cmd = [
            "./bin/etcdctl",
            f"--endpoints={endpoint}",
            "move-leader",
            self.IpToId.get(endpoint, "unknown")
        ]
        print("执行命令:", " ".join(cmd))

        subprocess.run(cmd, check=True)
        print("success")

        os.remove(local_path)
        print(f"已删除本地文件 {local_path}")
        
        return



def get_current_leader_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 连接一个不存在的地址，只为获取本机IP
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def main():
    calculator = OptimalLeaderCalculator()
    
    # CSV文件路径
    csv_path = "raft_stats.csv"
    
    print("开始计算最优Raft领导者...")
    print("=" * 50)
    
    # 1. 读取域数据（按IP分组）
    domains = calculator.read_raft_stats(csv_path)
    if not domains:
        print("无法读取域数据，程序退出")
        return
    
    # 2. 计算最优领导者
    optimal_leader, min_latency = calculator.find_optimal_leader(domains)
    
    # 3. 输出结果
    if optimal_leader:
        print(f"\n🎯 最优领导者:")
        print(f"   域IP: {optimal_leader.address}")
        print(f"   节点数: {optimal_leader.nodes}")
        print(f"   读请求: {optimal_leader.read_requests}")
        print(f"   写请求: {optimal_leader.write_requests}")
        print(f"   最小总加权延迟: {min_latency}")
        
        # 4. 获取当前领导者并考虑转移
        current_leader_ip = calculator.get_current_leader_ip()



        if current_leader_ip:
            print(f"\n📍 当前领导者IP: {current_leader_ip}")
            print(f"📍 最优领导者IP: {optimal_leader.address}")
            
            if current_leader_ip != optimal_leader.address:
                print("\n🔄 需要进行领导者转移")
                success = calculator.check_and_transfer_leader(current_leader_ip, optimal_leader.address)
                if success:
                    print("✅ 领导者转移完成")
                else:
                    print("❌ 领导者转移失败")
            else:
                print("✅ 当前领导者已经是最优选择")
        else:
            print("⚠️  无法获取当前领导者信息")
            
    else:
        print("\n❌ 无法确定最优领导者（可能无法达到法定人数）")

if __name__ == "__main__":
    main()