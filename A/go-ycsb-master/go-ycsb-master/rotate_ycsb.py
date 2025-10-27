#!/usr/bin/env python3
"""
rotate_ycsb_matrix.py  (PURE MATRIX VERSION)

只保留“矩阵比例 -> operationcount”的逻辑：
    opcount = round(load_base * p)
    p = M[ slot % load_rows , domain_col ]

- 无 FG/BG 旧逻辑、无 --load-mode 开关。
- 其余行为（按窗口对齐、优雅停止、提取 takes 等）保持不变。
"""

from __future__ import annotations
import argparse
import os
import re
import signal
import subprocess
import sys
import time
import random
from datetime import datetime
from pathlib import Path

import numpy as np

from recordIP import write_hotspot_plan
from datetime import datetime

import subprocess

# -------------------------- 共用工具 --------------------------

def call_delete():
    delete_path = "/etcd/etcd-release-3.4/delete.sh"
    try:
        subprocess.run([delete_path], check=True)
        log(f"Delete script {delete_path} executed successfully.")
    except subprocess.CalledProcessError as e:
        log(f"Delete script failed: {e}")

def log(msg: str) -> None:
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] {msg}")
    sys.stdout.flush()


def ensure_executable(path: Path) -> None:
    if not path.exists():
        sys.exit(f"ERROR: {path} does not exist")
    if not os.access(path, os.X_OK):
        sys.exit(f"ERROR: {path} is not executable")


def start_ycsb(
    goycsb: Path,
    db: str,
    workload: Path,
    self_endpoint: str,
    threads: int | None,
    extra_props: dict[str, str],
    log_file: Path,
    opcount: int,
) -> subprocess.Popen:
    """启动 go-ycsb；在命令末尾加入 -p operationcount=<opcount> 覆盖同名参数。"""
    cmd = [
        str(goycsb),
        "run",
        db,
        "-P",
        str(workload),
        "-p",
        f"etcd.endpoints={self_endpoint}",
    ]
    if threads is not None:
        cmd += ["-p", f"threadcount={threads}"]
    for k, v in extra_props.items():
        cmd += ["-p", f"{k}={v}"]

    cmd += ["-p", f"operationcount={opcount}"]  # 关键：覆盖 opcount

    log(f"START: {' '.join(cmd)}")
    lf = open(log_file, "ab", buffering=0)
    proc = subprocess.Popen(
        cmd,
        stdout=lf,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,  # POSIX: 新进程组
    )
    return proc


def stop_process(proc: subprocess.Popen, grace_s: float = 1.0) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except Exception:
        pass
    t_end = time.time() + grace_s
    while time.time() < t_end:
        if proc.poll() is not None:
            return
        time.sleep(0.1)
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        pass
    t_end = time.time() + grace_s
    while time.time() < t_end:
        if proc.poll() is not None:
            return
        time.sleep(0.1)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        pass


def align_to_boundary(duration: int) -> float:
    """返回下一个墙钟对齐边界的时间戳（秒）。"""
    now = time.time()
    remain = duration - int(now) % duration
    if remain == duration:
        remain = 0
    if remain > 0:
        log(f"Aligning to wallclock boundary in {remain}s…")
        time.sleep(remain)
    t0 = float(int(time.time()))
    return t0

# ---------------------- 从 playload 提取的核心 ----------------------
def generate_abc_matrix(n_rows=100, seed=None):
    """
    生成 n_rows×3 的比例矩阵，每行和为 1。
    第 i 行的“主列”= i % 3；其余两列为小比例随机分配。
    """
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    # 主列 -> 第二列；第三列 = 其余那一列
    second_col_for_dominant = {
        0: 1,  # A 主 -> 第二列取 B
        1: 0,  # B 主 -> 第二列取 A
        2: 1,  # C 主 -> 第二列取 B（若想改为 A：改成 0）
    }

    mat = np.zeros((n_rows, 3), dtype=float)

    for i in range(n_rows):
        dominant = i % 3
        second = second_col_for_dominant[dominant]
        third = 3 - dominant - second

        major_val = random.uniform(0.6, 1.0)
        remaining = max(0.0, 1.0 - major_val)

        upper = min(0.2, remaining)
        if upper <= 0:
            second_val = 0.0
        else:
            second_val = random.uniform(0.0, upper)

        third_val = 1.0 - major_val - second_val

        # 浮点稳定处理
        eps = 1e-12
        if third_val < -eps:
            s = major_val + second_val
            if s > 0:
                major_val *= (1.0 / s)
                second_val *= (1.0 / s)
                third_val = 0.0
            else:
                major_val, second_val, third_val = 1.0, 0.0, 0.0
        elif third_val < 0:
            third_val = 0.0
            adjust = (major_val + second_val) - 1.0
            if adjust > 0:
                take = min(adjust, major_val)
                major_val -= take
                adjust -= take
                if adjust > 0:
                    second_val = max(0.0, second_val - adjust)

        row = np.zeros(3, dtype=float)
        row[dominant] = major_val
        row[second] = second_val
        row[third] = third_val

        s = row.sum()
        if s != 0:
            row /= s

        mat[i] = row

    return mat


# -------------------------- 参数与主流程 --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Per-node coordinated YCSB rotation (operationcount from load matrix only)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--goycsb", default="./go-ycsb", help="Path to go-ycsb binary")
    p.add_argument("--workload", required=True, help="Path to workload file (e.g., myworkload)")
    #p.add_argument("--endpoints", required=True, help="Comma-separated endpoints in rotation order")
    #group = p.add_mutually_exclusive_group(required=True)
    #group.add_argument("--self-endpoint", help="This machine's endpoint; must appear in --endpoints")
    #group.add_argument("--self-index", type=int, help="This machine's index in --endpoints (0-based)")
    p.add_argument("--db", default="etcd", choices=["etcd", "etcdv3"], help="YCSB DB target")
    p.add_argument("--threads", type=int, default=None, help="Override threadcount via -p threadcount=")
    p.add_argument("--duration", type=int, default=10, help="Seconds per window")
    p.add_argument("--rounds", type=int, default=None, help="Number of full rotations; omit for infinite")
    p.add_argument("--log-dir", default="./ycsb-logs", help="Directory to write logs")
    p.add_argument("--prop", action="append", help="Extra -p key=value to forward (may repeat)")
    p.add_argument("--no-align", action="store_true", help="Do not align to wallclock boundary; start immediately")

    # 矩阵负载参数（仅保留这些）
    p.add_argument("--load-rows", type=int, default=300, help="负载矩阵的行数（轮换周期）")
    p.add_argument("--load-base", type=int, default=200, help="基数 base，使 opcount=round(base*p)")
    p.add_argument("--load-seed", type=int, default=42, help="随机种子（整数，可复现实验）")
    p.add_argument("--load-domain-index", type=int, default=None,
                   help="使用矩阵的哪一列（0/1/2）作为‘本机列’；默认等于 self-index")

    return p.parse_args()


def append_takes_from_log(log_path: str, times_path: str = "/etcd/etcd-release-3.4/timesA.csv") -> None:
    """
    从 go-ycsb 日志里提取最后一个 'Run finished, takes XX(ms|s)'，
    以秒为单位，带时间戳写入 CSV。
    """
    takes_sec = None
    with open(log_path, "r", errors="ignore") as f:
        # 反向遍历，找到“最后一次”匹配
        for line in reversed(f.readlines()):
            m = re.search(r"Run finished,\s*takes\s*([\d.]+)\s*(ms|s)", line)
            if m:
                num, unit = m.groups()
                val = float(num) / 1000.0 if unit == "ms" else float(num)
                takes_sec = f"{val:.6f}"
                break
    if takes_sec is None:
        return  # 没匹配到就跳过

    # 当前时间戳
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    first_time = not os.path.exists(times_path)
    with open(times_path, "a") as tf:
        if first_time:
            tf.write("time,takes_seconds\n")  # CSV 表头
        tf.write(f"{now_str},{takes_sec}\n")

def append_total_stats_from_log(
    log_path: str,
    times_path: str = "/etcd/etcd-release-3.4/timesA.csv"
) -> None:
    """
    从 go-ycsb 日志解析三项指标并追加到 CSV:
      - operationcount  (来自 properties: "operationcount"="182")
      - total_count     (来自 TOTAL 行: Count: 68)
      - total_avg_ms    (来自 TOTAL 行: Avg(us): 43211  -->  43.211 ms)

    注意：只取日志中“最后一条” TOTAL 聚合（与原实现一致）。
    """
    opcount = None
    total_count = None
    total_avg_ms = None

    # 预编译正则
    re_opcount = re.compile(r'"operationcount"\s*=\s*"(\d+)"')
    re_total = re.compile(
        r'^TOTAL\s+-.*?Count:\s*(\d+).*?Avg\(us\):\s*([\d.]+)',
        re.IGNORECASE
    )

    with open(log_path, "r", errors="ignore") as f:
        lines = f.readlines()

    # 1) operationcount：从上到下找第一处即可（properties 段）
    for line in lines:
        m = re_opcount.search(line)
        if m:
            opcount = int(m.group(1))
            break

    # 2) TOTAL 的 Count 与 Avg(us)：从下往上找“最后一条”
    for line in reversed(lines):
        m = re_total.search(line)
        if m:
            total_count = int(m.group(1))
            avg_us = float(m.group(2))
            total_avg_ms = avg_us / 1000.0
            break

    # 找不到就不写入
    if opcount is None or total_count is None or total_avg_ms is None:
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    first_time = not os.path.exists(times_path)
    with open(times_path, "a") as tf:
        if first_time:
            tf.write("time,operationcount,total_count,total_avg_ms\n")
        tf.write(f"{now_str},{opcount},{total_count},{total_avg_ms:.3f}\n")


def main() -> None:
    call_delete()

    args = parse_args()

    goycsb = Path(args.goycsb).resolve()
    workload = Path(args.workload).resolve()
    log_dir = Path(args.log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    ensure_executable(goycsb)
    if not workload.exists():
        sys.exit(f"ERROR: workload file not found: {workload}")

    '''
    endpoints = [ep.strip() for ep in args.endpoints.split(',') if ep.strip()]
    if not endpoints:
        sys.exit("ERROR: --endpoints must not be empty")

    if args.self_endpoint:
        if args.self_endpoint not in endpoints:
            sys.exit("ERROR: --self-endpoint not found in --endpoints")
        my_idx = endpoints.index(args.self_endpoint)
        my_ep = args.self_endpoint
    else:
        if args.self_index < 0 or args.self_index >= len(endpoints):
            sys.exit("ERROR: --self-index out of range for --endpoints")
        my_idx = args.self_index
        my_ep = endpoints[my_idx]
    '''
    endpoints = [
        "192.168.0.38:2379",
        "192.168.0.82:2379",
        "192.168.0.223:2379",
    ]
    my_ep = "192.168.0.38:2379"
    my_idx = endpoints.index(my_ep)

    # 组装额外属性
    extra_props = {}
    for kv in args.prop or []:
        if '=' not in kv:
            sys.exit(f"ERROR: --prop expects key=value, got: {kv}")
        k, v = kv.split('=', 1)
        extra_props[k.strip()] = v.strip()

    win = int(args.duration)
    n = len(endpoints)

    # —— 矩阵负载准备 ——
    domain_col = args.load_domain_index if args.load_domain_index is not None else my_idx
    if domain_col not in (0, 1, 2):
        domain_col = my_idx % 3  # 仅 3 列（A/B/C），多节点时按 %3 取列
    M = generate_abc_matrix(n_rows=args.load_rows, seed=args.load_seed)
    log(f"[MATRIX] rows={args.load_rows}, base={args.load_base}, seed={args.load_seed}, domain_col={domain_col}")

    # 对齐边界（或立即开始）
    if args.no_align:
        t0 = float(int(time.time()))
        log("No wallclock alignment ( --no-align ). Starting immediately.")
    else:
        t0 = align_to_boundary(win)
        log(f"Aligned. First window starts at {datetime.fromtimestamp(t0).strftime('%H:%M:%S')} (slot=0)")



    start_time_str = datetime.fromtimestamp(t0).strftime("%Y-%m-%d %H:%M:%S")
    write_hotspot_plan(start_time=start_time_str,)


    proc: subprocess.Popen | None = None
    rounds_completed = 0
    slot = 0

    try:
        while True:
            active_idx = slot % n
            slot_start = t0 + slot * win
            slot_end = slot_start + win

            now = time.time()
            if now < slot_start:
                time.sleep(max(0.0, slot_start - now))

            # === 产生 operationcount（纯矩阵） ===
            row = slot % args.load_rows
            p = float(M[row, domain_col])
            opcount = max(1, int(round(args.load_base * p)))

            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            log(f"=== SLOT {slot} (index {active_idx}) @ {my_ep} for {win}s, opcount={opcount}, p={p:.4f} ===")
            log_file = log_dir / f"slot{slot}_MATRIX_{my_ep.replace(':','_')}_{ts}.log"
            proc = start_ycsb(
                goycsb, args.db, workload, my_ep, args.threads, extra_props, log_file, opcount
            )

            # 等待窗口结束
            now = time.time()
            remaining = slot_end - now
            if remaining > 0:
                time.sleep(remaining)

            # 窗口结束，停止并抽取 takes
            if proc is not None:
                stop_process(proc)
                proc = None
                log(f"END SLOT {slot} (MATRIX done)")
                #append_takes_from_log(str(log_file))
            append_total_stats_from_log(str(log_file))


            slot += 1
            if slot % n == 0:
                rounds_completed += 1
                if args.rounds is not None and rounds_completed >= args.rounds:
                    log(f"Completed {rounds_completed} full rotation(s). Exiting.")
                    break

    except KeyboardInterrupt:
        log("Interrupted by user.")
    finally:
        if proc is not None:
            stop_process(proc)
            log("Cleaned up running go-ycsb.")


if __name__ == "__main__":
    main()
