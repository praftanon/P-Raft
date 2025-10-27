#!/usr/bin/env python3
import subprocess
import time

# 统一的基础参数
BASE_CMD = [
    "python3",
    "change_ycsb.py",   # 或 rotate_ycsb_matrix.py
    "--workload", "./workloads/workloada",
]

# 三个不同间隔及其运行时长（单位秒）
intervals = [
    (10, 20 * 60),
    (20, 20 * 60),
    (30, 20 * 60),
]

for dur, total_sec in intervals:
    print(f"\n=== Running with duration={dur}s for {total_sec}s ===")
    start = time.time()

    proc = subprocess.Popen(
        BASE_CMD + ["--duration", str(dur)],  # 关闭对齐
    )

    # 等待 20 min 或提前退出
    while proc.poll() is None and (time.time() - start < total_sec):
        time.sleep(5)

    # 到达 20 min 或异常时终止
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    print(f"=== Finished duration={dur}s ===\n")
