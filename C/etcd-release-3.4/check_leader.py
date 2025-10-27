#!/usr/bin/env python3
import os
import subprocess
import socket
import time

IpToId = {
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

ip_group =[]





# ------------------------------
# 配置
# ------------------------------
ETCDCTL = "./bin/etcdctl"
ENDPOINTS = (
    "http://192.168.0.38:2379,http://192.168.0.38:3379,http://192.168.0.38:4379,"
    "http://192.168.0.82:2379,http://192.168.0.82:3379,http://192.168.0.82:4379,"
    "http://192.168.0.223:2379,http://192.168.0.223:3379,http://192.168.0.223:4379"
)

# ------------------------------
# 获取本机 IP (取第一块非 127.0.0.1 的地址)
# ------------------------------
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 连接一个不存在的地址，只为获取本机IP
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

# ------------------------------
# 获取 Leader 地址 (http://ip:port)
# ------------------------------
def get_leader():
    cmd = [
        ETCDCTL,
        f"--endpoints={ENDPOINTS}",
        "endpoint",
        "status",
        "--write-out=table",
    ]
    output = subprocess.check_output(cmd, text=True)
    for line in output.splitlines():
        if "true" in line:  # 这一行包含 Leader
            parts = line.split()
            return parts[1]  # 第二列是 URL
    return None

# ------------------------------
# 提取 Leader IP
# ------------------------------
def extract_ip(url: str):
    if not url:
        return None
    return url.split("//")[1].split(":")[0]
def moveleader_demo(local_ip, leader_url,
                    local_path="/etcd/etcd-release-3.4/raft_stats.csv",
                    remote_path="/etcd/etcd-release-3.4/raft_stats.csv"):

    leader_endpoint = leader_url.split("://")[1]  # 例如 "192.168.0.38:2379"
    target = None

    if leader_endpoint != "192.168.0.38:2379":
         target = "192.168.0.38:2379"

    # 固定轮换逻辑
    # if leader_endpoint == "192.168.0.38:2379":
    #     target = "192.168.0.82:2379"
    # elif leader_endpoint == "192.168.0.82:2379":
    #     target = "192.168.0.223:2379"
    # elif leader_endpoint == "192.168.0.223:2379":
    #     target = "192.168.0.38:2379"
    
    # if leader_endpoint == "192.168.0.38:3379":
    #     target = "192.168.0.82:2379"
    # elif leader_endpoint == "192.168.0.82:3379":
    #     target = "192.168.0.223:2379"
    # elif leader_endpoint == "192.168.0.223:3379":
    #     target = "192.168.0.38:2379"

    # if leader_endpoint == "192.168.0.38:3379":
    #     target = "192.168.0.82:2379"
    # elif leader_endpoint == "192.168.0.82:3379":
    #     target = "192.168.0.223:2379"
    # elif leader_endpoint == "192.168.0.223:3379":
    #     target = "192.168.0.38:2379"

    if not target:
        print("⚠️ 未找到匹配的目标节点")
        return

    target_ip = target.split(":")[0]

    # 先传文件
    scp_to_host(local_path, target_ip, remote_path)

    # 执行 etcdctl move-leader
    cmd = [
        ETCDCTL,
        f"--endpoints={leader_endpoint}",
        "move-leader",
        IpToId[target]
    ]
    print("执行命令:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print("✅ Leader 已转移到", target)

    # 删除本地文件
    os.remove(local_path)
    print(f"已删除本地文件 {local_path}")


def scp_to_host(local_file, host, remote_path, user="root", port=22, key=None, enable_log=True):
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"本地文件不存在: {local_file}")

    scp_cmd = ["scp"]
    if key:
        scp_cmd += ["-i", key]
    if port and port != 22:
        scp_cmd += ["-P", str(port)]  # 注意：scp 用大写 P 指定端口
    scp_cmd += [local_file, f"{user}@{host}:{remote_path}"]

    print("执行文件传输命令：", " ".join(scp_cmd))

    # === 计时开始 ===
    t0 = time.perf_counter()
    subprocess.run(scp_cmd, check=True)
    duration = time.perf_counter() - t0
    # === 计时结束 ===

    print(f"文件已传到 {host}:{remote_path} ，传输耗时：{duration*1000:.1f} ms ({duration:.6f} s)")

    # 可选：记录到 CSV
    
    return duration





def main():
    local_file_path = "/etcd/etcd-release-3.4/raft_stats.csv"
    for ip in IpToId:
        ip_group.append(ip)
    local_ip = get_local_ip()
    leader_url = get_leader()
    leader_ip = extract_ip(leader_url)

    print(f"本机 IP: {local_ip}")
    print(f"Leader IP: {leader_ip}")

    if local_ip == leader_ip:
        print("✅ 本机是 Leader")
        moveleader_demo(local_ip,leader_url,local_file_path)


    else:
        print("❌ 本机不是 Leader")

if __name__ == "__main__":
    
    main()
