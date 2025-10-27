#!/usr/bin/env python3
"""
Optimal Raft Leader Calculator (预测驱动版)
- 调用预测模型获取“未来 horizon 行”（默认模型里写的 horizon），
- 对这段未来窗口做列均值，
- 将均值视作接下来一段时间的负载来计算最优领导者。
"""

import argparse
import csv
import math
import os
import socket
import subprocess
import sys
from typing import List, Optional
import time
from datetime import datetime
import numpy as np
import pandas as pd
from forecaster import Forecaster

import threading               # ← NEW: 你用到了 threading.RLock/Thread
from pathlib import Path
import hashlib 

from leader_logger import BufferedPredictedLeaderWriter


class ModelReloader:
    """
    通过 mtime 或 sha256 监控 model/scaler/meta 是否变化；变化且稳定后触发 reload。
    - use_hash=False: 仅看 mtime，性能好；需训练端原子重命名发布，配合 debounce。
    - use_hash=True : 看文件哈希，更稳但更耗时（大文件会慢）。
    """
    def __init__(self, model_path: str, scaler_path: str, meta_path: str,
                 debounce_sec: int = 5, use_hash: bool = False):
        self.paths = {
            "model": Path(model_path),
            "scaler": Path(scaler_path),
            "meta":  Path(meta_path),
        }
        self.debounce = debounce_sec
        self.use_hash = use_hash
        self.last_sig = self._signature()   # 记录已加载版本的签名（mtime 或 hash）
        self.last_seen_change_ts = 0.0      # 最近一次发现“不同”的时间，用于去抖

    def _hash(self, p: Path, bs: int = 1<<20) -> str:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(bs), b""):
                h.update(chunk)
        return h.hexdigest()

    def _signature(self) -> dict:
        sig = {}
        for k, p in self.paths.items():
            if not p.exists():
                sig[k] = None
            else:
                sig[k] = self._hash(p) if self.use_hash else p.stat().st_mtime
        return sig

    def changed_and_stable(self) -> bool:
        """返回：是否发现新版本且已稳定（不再变化至少 debounce_sec 秒）"""
        cur = self._signature()
        if cur == self.last_sig:                 # 签名完全一致，无变化
            self.last_seen_change_ts = 0.0
            return False

        now = time.time()
        # 首次发现变化，记录时间戳
        if self.last_seen_change_ts == 0.0:
            self.last_seen_change_ts = now
            return False

        # 已经变化，并且持续至少 debounce 秒 —— 认为训练端写入完成
        if (now - self.last_seen_change_ts) >= self.debounce:
            return True

        return False

    def mark_loaded(self):
        """在成功 reload 后调用，更新“已加载签名”并清理去抖状态。"""
        self.last_sig = self._signature()
        self.last_seen_change_ts = 0.0

class ForecasterHolder:
    """线程安全地存放/读取当前 forecaster。"""
    def __init__(self, forecaster):
        self._f = forecaster
        self._lock = threading.RLock()
    def get(self):
        with self._lock:
            return self._f
    def set(self, fnew):
        with self._lock:
            self._f = fnew


class ReloadWorker(threading.Thread):
    """
    后台线程：每 interval_sec 检查一次；若模型更新且稳定，则重建 Forecaster 并无缝替换。
    """
    daemon = True  # 随主进程退出
    def __init__(self, holder: ForecasterHolder,
                 model_path: str, scaler_path: str, meta_path: str,
                 interval_sec: int = 15, debounce_sec: int = 5, use_hash: bool = False,
                 forecaster_ctor=None, on_log=print):
        super().__init__(name="ModelReloadWorker")
        self.holder = holder
        self.interval = int(interval_sec)
        self.on_log = on_log
        self.ctor = forecaster_ctor  # callable(MODEL, SCALER, META) -> Forecaster
        self.guard = ModelReloader(model_path, scaler_path, meta_path,
                                   debounce_sec=debounce_sec, use_hash=use_hash)
        self.stop_evt = threading.Event()
        self.guard.mark_loaded()

    def stop(self):
        self.stop_evt.set()

    def run(self):
        while not self.stop_evt.wait(self.interval):
            try:
                if self.guard.changed_and_stable():
                    self.on_log("[RELOAD] 检测到新模型，开始热更新…")
                    fnew = self.ctor()  # 创建新 forecaster
                    self.holder.set(fnew)  # 原子替换
                    self.guard.mark_loaded()
                    self.on_log("[RELOAD] 热更新完成。")
            except Exception as e:
                self.on_log(f"[WARN] 热更新失败，保持旧模型：{e!r}")

class Domain:
    def __init__(self, domain_id: str, address: str, nodes: int,
                 read_requests: float = 0.0, write_requests: float = 0.0):
        self.id = domain_id
        self.address = address  # IP
        self.nodes = nodes
        self.read_requests = read_requests
        self.write_requests = write_requests


class OptimalLeaderCalculator:
    def __init__(self):
        # IP 映射
        self.ip_to_index = {
            "192.168.0.38": 0,
            "192.168.0.82": 1,
            "192.168.0.223": 2
        }
        self.index_to_ip = {v: k for k, v in self.ip_to_index.items()}

        # RTT 矩阵（ms）
        self.latency_matrix = [
            [0, 30, 40],   # from 192.168.0.38
            [30, 0, 50],   # from 192.168.0.82
            [40, 50, 0]    # from 192.168.0.223
        ]

        self.ip_groups = {
            "192.168.0.38": ["933ca51d2bb602b8", "6181f76d6668aeb0", "69948e3d245f62b7"],
            "192.168.0.82": ["b92b49a4de72942d", "3cdaf029c87a002", "5b3ba363fb10d52f"],
            "192.168.0.223": ["69f554c3f7f50a72", "8861d1f6a0217629", "512e070e8eb32959"]
        }

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

        self.node_id_to_ip = {
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

    # === 新增：用预测均值来构造 domains ===
    def build_domains_from_pred_mean(self, pred_df: pd.DataFrame) -> List[Domain]:
        """
        输入：预测结果的 DataFrame（index=未来时间戳，列名=训练时特征）
        处理：对所有列在时间维度上取平均，然后按 *_write / *_read 成对组合
        输出：Domain 列表
        """
        if pred_df is None or pred_df.empty:
            print("预测结果为空")
            return []

        # 对 horizon 维度求均值（得到一行）
        mean_row = pred_df.mean(axis=0)  # Series, index=列名

        # 从列名自动抽取节点 id
        # 期望列命名形如：{node_id}_write  与  {node_id}_read
        node_ids = set()
        for col in mean_row.index:
            if col.endswith("_write"):
                node_ids.add(col[:-6])
            elif col.endswith("_read"):
                node_ids.add(col[:-5])

        domains: List[Domain] = []
        for node_id in sorted(node_ids):
            write_col = f"{node_id}_write"
            read_col  = f"{node_id}_read"
            write_val = float(mean_row.get(write_col, 0.0))
            read_val  = float(mean_row.get(read_col, 0.0))

            if write_val == 0.0 and read_val == 0.0:
                continue

            ip_addr = self.node_id_to_ip.get(node_id, "unknown")
            if ip_addr == "unknown":
                print(f"警告: 节点 {node_id} 的IP地址未知，跳过")
                continue

            domains.append(Domain(node_id, ip_addr, nodes=1,
                                  read_requests=read_val, write_requests=write_val))
            print(f"预测均值域: {node_id[:8]}..., ip={ip_addr}, read(mean)={read_val:.3f}, write(mean)={write_val:.3f}")
        return domains

    def get_latency(self, from_ip: str, to_ip: str) -> int:
        if from_ip not in self.ip_to_index or to_ip not in self.ip_to_index:
            print(f"警告: 未知的IP地址 {from_ip} 或 {to_ip}")
            return 999
        return self.latency_matrix[self.ip_to_index[from_ip]][self.ip_to_index[to_ip]]

    def calculate_total_latency(self, leader_domain: Domain, domains: List[Domain]) -> int:
        leader_ip = leader_domain.address
        total_latency = 0
        for domain in domains:
            latency = 0 if domain.id == leader_domain.id else self.get_latency(leader_ip, domain.address)
            total_req = domain.read_requests + domain.write_requests
            total_latency += int(total_req * latency)
            print(f"  域 {domain.id}: 总请求={total_req:.3f}, 延迟={latency}ms, 域延迟={int(total_req * latency)}ms")
        print(f"总加权延迟 T({leader_domain.id}) = {total_latency}ms")
        return total_latency

    def find_optimal_leader(self, domains: List[Domain]):
        if not domains:
            print("没有可用的域")
            return None, None
        total_nodes = sum(d.nodes for d in domains)
        quorum_size = math.floor(total_nodes / 2) + 1
        print(f"总节点数: {total_nodes}，法定人数: {quorum_size}，域数量: {len(domains)}")
        print("=" * 50)

        best, best_cost = None, float("inf")
        for cand in domains:
            print(f"评估候选领导者 {cand.id} (IP: {cand.address}) ...")
            cost = self.calculate_total_latency(cand, domains)
            print("-" * 30)
            if cost < best_cost:
                best, best_cost = cand, cost
        return best, best_cost

    def scp_to_host(self, local_file, host, remote_path, user="root", port=22, key=None):
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"本地文件不存在: {local_file}")
        scp_cmd = ["scp"]
        if key:
            scp_cmd += ["-i", key]
        if port and port != 22:
            scp_cmd += ["-P", str(port)]
        scp_cmd += [local_file, f"{user}@{host}:{remote_path}"]
        print("执行文件传输命令：", " ".join(scp_cmd))
        subprocess.run(scp_cmd, check=True)
        print(f"文件已传到 {host}:{remote_path}")

    def check_and_transfer_leader(self, current_leader_ip: str, optimal_leader_ip: str):
        local_path = "/etcd/etcd-release-3.4/raft_stats.csv"
        remote_path = "/etcd/etcd-release-3.4/raft_stats.csv"


        endpoint = optimal_leader_ip + ":2379"
        self.scp_to_host(local_path, optimal_leader_ip, remote_path)

        current_leader_url = current_leader_ip + ":2379"

        cmd = [
            "/etcd/etcd-release-3.4/bin/etcdctl",
            f"--endpoints={current_leader_url}",
            "move-leader",
            self.IpToId.get(endpoint, "unknown")
        ]

        print("执行命令:", " ".join(cmd))

        # subprocess.run(cmd, check=True)
        self.move_leader_with_timing(cmd)
        print("success")
        
        os.remove(local_path)
        print(f"已删除本地文件 {local_path}")
        return 1
        
    def move_leader_with_timing(self, cmd, csv_path="/etcd/etcd-release-3.4/move_leader_timeA.csv"):
        # 记录起始时间
        t_start = time.time()
        try:
            subprocess.run(cmd, check=True)
            status = "success"
        except subprocess.CalledProcessError:
            status = "failed"
        t_end = time.time()

        # 计算耗时（毫秒）
        elapsed_ms = (t_end - t_start) * 1000

        # 当前时间（精确到分钟）
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        # 写入 CSV
        row = {
            "timestamp": timestamp,
            "status": status,
            "elapsed_ms": f"{elapsed_ms:.2f}"
        }

        file_exists = os.path.exists(csv_path)
        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        return elapsed_ms
HISTORY_CSV = "/etcd/etcd-release-3.4/raft_stats.csv"
MODEL_PATH  = "/root/artifacts/lstm_seq2seq.h5"
SCALER_PATH = "/root/artifacts/scaler.pkl"
META_PATH   = "/root/artifacts/model_meta.json"

# 热更新控制
RELOAD_DEBOUNCE_SEC = 5   # 文件更新落盘后需稳定 >= 5s 才切换
RELOAD_BY_HASH      = False  # True=按hash检测（稳但慢），False=按mtime（快）

USE_LAST_ROWS   = 30       # 用最后 N 行作为历史窗口（若 < look_back，会被自动提升到 look_back）
FIXED_STEP_SEC  = None     # 固定步长（秒）；None 表示从时间索引中位数推断

SLEEP_SEC       = 0
INTERVAL_SEC    = 15      # 循环间隔秒
MAX_ITERS       = 0        # 0 表示无限循环直到迁移；>0 则最多迭代 N 次后退出（便于调试）
PREDICT_LOG_CSV = "/etcd/etcd-release-3.4/predicted_leaderA.csv"
METRICS_CSV     = "/etcd/etcd-release-3.4/leader_move_metricsA.csv"


def get_current_leader_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


# ========= 主逻辑 =========
def run_once_and_decide(forecaster: Forecaster, calc: OptimalLeaderCalculator, self_ip: str) -> tuple[bool, Optional[str]]:
    """
    执行一次：预测 -> 取均值 -> 计算最优领导者
    返回：(is_self_optimal, optimal_ip or None)
    """
    # 预测未来 horizon 行（DataFrame；列名与训练时一致）
    pred_df: pd.DataFrame = forecaster.predict(
        data=HISTORY_CSV,
        use_last_rows=USE_LAST_ROWS,
        fixed_step_sec=FIXED_STEP_SEC,
        return_dataframe=True
    )

    if pred_df is None or pred_df.empty:
        print("[WARN] 预测结果为空，默认认为保持当前领导者")
        return True, None

    # 用“未来窗口均值”构造域（你类里已有该方法，接受 DataFrame）
    domains = calc.build_domains_from_pred_mean(pred_df)
    if not domains:
        print("[WARN] 无法从预测构造域，默认保持当前领导者")
        return True, None

    # 计算最优领导者
    optimal, _ = calc.find_optimal_leader(domains)
    if not optimal:
        print("[WAR] 未能选出最优领导者，默认保持当前领导者")
        return True, None

    optimal_ip = optimal.address
    is_self = (optimal_ip == self_ip)
    print(f"[INFO] 本机={self_ip}，最优领导者={optimal_ip} -> {'保持' if is_self else '需要迁移'}")
    return is_self, optimal_ip

def main():
    start_time=time.time()
    print("[BOOT] 启动 predict_leader（无命令行参数版）")
    print(f"[CONF] HISTORY_CSV={HISTORY_CSV}")
    print(f"[CONF] MODEL={MODEL_PATH}")
    print(f"[CONF] SCALER={SCALER_PATH}")
    print(f"[CONF] META={META_PATH}")
    print(f"[CONF] USE_LAST_ROWS={USE_LAST_ROWS}, FIXED_STEP_SEC={FIXED_STEP_SEC}, INTERVAL_SEC={INTERVAL_SEC}, MAX_ITERS={MAX_ITERS}")

    logger = BufferedPredictedLeaderWriter(PREDICT_LOG_CSV)

    # === METRICS: 活动时长与预测次数（不含 sleep）===
    from time import perf_counter
    pred_active_sec = 0.0   # 预测/决策累计耗时（不含 sleep、也不含最终迁移）
    move_sec = 0.0          # move-leader 实际执行耗时
    rounds = 0              # 预测/评估轮数（也即预测调用次数）

    # 初始化预测器与计算器
    forecaster = None
    while forecaster is None:
        try:
            forecaster = Forecaster(MODEL_PATH, SCALER_PATH, META_PATH)
            print("[BOOT] 预测器初始化成功")

            f_holder = ForecasterHolder(forecaster)

            def _make_forecaster():
                return Forecaster(MODEL_PATH, SCALER_PATH, META_PATH)

            reload_worker = ReloadWorker(   # === NEW ===
                holder=f_holder,
                model_path=MODEL_PATH, scaler_path=SCALER_PATH, meta_path=META_PATH,
                interval_sec=15, debounce_sec=5, use_hash=False,
                forecaster_ctor=_make_forecaster, on_log=print
            )
            reload_worker.start()
        except Exception as e:
            print(f"[ERROR] 预测器初始化失败: {e!r}，30s 后重试...")
            time.sleep(30)

    calc = OptimalLeaderCalculator()

    # 本机 IP
    self_ip = get_current_leader_ip()
    print(f"[BOOT] 本机IP: {self_ip}")
    
    time.sleep(SLEEP_SEC)

    it = 0
    try: 
        while True:
            it += 1
            print(f"\n===== 回合 #{it} =====")

            try:
                cur_f = f_holder.get()

                # === METRICS: 预测/决策计时（不含 sleep）===
                t0 = perf_counter()
                is_self, optimal_ip = run_once_and_decide(cur_f, calc, self_ip)
                t1 = perf_counter()
                pred_active_sec += (t1 - t0)
                rounds += 1

            except Exception as e:
                # 兜底：即使某轮预测/构造失败，也不要 crash，按保持自己处理
                print(f"[ERROR] 本轮预测/决策异常：{e!r}，默认保持当前领导者")
                is_self, optimal_ip = True, None
	    
            will_move = not is_self

            if will_move and not optimal_ip:
                print("[WARN] 需要迁移但 optimal_ip 为空，降级为保持当前领导者。")
                will_move = False

            leader_for_log = self_ip if not will_move else optimal_ip
            logger.record(leader_for_log, will_move)


            # 5) 根据决策执行/等待
            if not will_move:
                # 仍为最优领导者，可能迭代退出或继续睡眠
                if MAX_ITERS > 0 and it >= MAX_ITERS:
                    print("[EXIT] 达到最大轮次限制，退出。")
                    break
                print(f"[KEEP] predicted {self_ip}, {INTERVAL_SEC}s 后再次评估...")
                time.sleep(INTERVAL_SEC)
                continue

            # 6) 需要迁移（will_move=True 且 optimal_ip 非空）——执行一次并退出
            print(f"[ACTION] 触发领导者迁移: {self_ip} -> {optimal_ip}")

            # === METRICS: 迁移动作计时 ===
            mv0 = perf_counter()
            ok = calc.check_and_transfer_leader(self_ip, optimal_ip)
            mv1 = perf_counter()
            move_sec = (mv1 - mv0)

            if ok:
                print(f"[DONE] 迁移完成，leader -> {optimal_ip}")
            else:
                print("[ERROR] 迁移失败，退出程序（避免重复操作）。")

            # === METRICS: 汇总并写入 CSV ===
            total_active_ms = (pred_active_sec + move_sec) * 1000.0
            row = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "rounds": rounds,
                "pred_active_ms": f"{pred_active_sec * 1000.0:.2f}",
                "move_ms": f"{move_sec * 1000.0:.2f}",
                "total_active_ms": f"{(pred_active_sec + move_sec) * 1000.0:.2f}",
            }
            try:
                _append_metrics_row(METRICS_CSV, row)
                print(f"[METRICS] 写入 {METRICS_CSV}: {row}")
            except Exception as e:
                print(f"[WARN] 指标写入失败: {e!r}")

            break


    finally:
        # === NEW: 退出前通知后台线程停止（可选，daemon=True 时不强制）===
        try:
            reload_worker.stop()
        except Exception:
            pass


    time_end=time.time()
    print(f"[TIMER] 程序总耗时: {time_end - start_time:.2f} 秒")

def _append_metrics_row(csv_path: str, row: dict):
    """将一次运行的统计指标写入 CSV（若不存在则写 header）。"""
    header = [
        "ts",                 # 记录时间（到分钟）
        "rounds",             # 评估轮数
        "pred_active_ms",     # 迁移前：纯计算累计时长（不含 sleep、不含迁移）
        "move_ms",            # move-leader 执行时长
        "total_active_ms"     # 总活动时长 = pred_active_ms + move_ms
    ]
    need_header = False
    try:
        with open(csv_path, "r", newline="") as _:
            pass
    except FileNotFoundError:
        need_header = True

    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if need_header:
            w.writeheader()
        w.writerow(row)


if __name__ == "__main__":
    main()
