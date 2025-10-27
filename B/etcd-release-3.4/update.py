#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, shutil, signal, subprocess, urllib.request, re
from datetime import datetime
from pathlib import Path

# ===== 可配置 =====
METRICS_URL = os.environ.get("ETCD_METRICS_URL", "http://192.168.0.82:2379/metrics")
SRC_CSV     = os.environ.get("RAFT_CSV_PATH", "/etcd/etcd-release-3.4/raft_stats.csv")
SNAP_DIR    = os.environ.get("SNAP_DIR", "/tmp/raft-snapshots")
PYTHON_BIN  = os.environ.get("PYTHON_BIN", "python3")

TRAIN_PY    = "/root/train.py"
TRAIN_ARGS  = [
    "--input", None,                # 运行时替换为快照路径
    "--out-dir", "/root/artifacts",
    "--look-back", "30",
    
    "--horizon", "7",
    
    "--epoch", "35",                # 按你给的 --epoch
    "--batch-size", "64",
    "--early-stopping",
    "--reduce-lr",
]



CYCLE_WAIT_SECONDS = 180   # 5min
WINDOW_SECONDS     = 175    # 60s 轻探测窗口
CHECK_INTERVAL     = 3     # 每 3s 探测一次
CSV_STABLE_WINDOW  = 0.5   # CSV 稳定性判断窗口
CSV_SNAPSHOT_RETRY = 10    # 快照重试
LOG_PATH           = "/root/leader_update_loop.log"
# ==================

_stop = False
def _handle_stop(signum, frame):
    global _stop; _stop = True
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, _handle_stop)

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def is_leader() -> bool:
    try:
        with urllib.request.urlopen(METRICS_URL, timeout=2) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
        m = re.search(r"^etcd_server_is_leader\s+([01])\s*$", text, re.M)
        return bool(m and m.group(1) == "1")
    except Exception as e:
        log(f"[WARN] metrics 获取失败: {e}")
        return False

def _stat_tuple(path: str):
    st = os.stat(path)
    return (st.st_size, getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))

def csv_is_stable(path: str, window: float = CSV_STABLE_WINDOW) -> bool:
    try:
        s1 = _stat_tuple(path)
        time.sleep(window)
        s2 = _stat_tuple(path)
        return s1 == s2
    except FileNotFoundError:
        return False

def snapshot_csv(src: str, snap_dir: str) -> str:
    Path(snap_dir).mkdir(parents=True, exist_ok=True)
    src_p = Path(src)
    if not src_p.exists():
        raise FileNotFoundError(f"CSV 不存在: {src}")
    for attempt in range(1, CSV_SNAPSHOT_RETRY + 1):
        try:
            if not csv_is_stable(src):
                raise RuntimeError("CSV 不稳定（写入中或被移动）")
            ts = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            dst_p = Path(snap_dir) / f"{src_p.stem}-{ts}{src_p.suffix}"
            shutil.copy2(src_p, dst_p)
            if src_p.stat().st_size != dst_p.stat().st_size:
                raise RuntimeError("快照大小校验失败")
            return str(dst_p)
        except Exception as e:
            log(f"[WARN] 快照失败(第{attempt}次): {e}")
            time.sleep(0.8)
    raise RuntimeError("CSV 快照反复失败，放弃本轮")

def run_training(snapshot_path: str) -> int:
    args = [PYTHON_BIN, TRAIN_PY]
    patched = TRAIN_ARGS.copy()
    patched[1] = snapshot_path
    args.extend(patched)
    log(f"[INFO] 启动训练: {' '.join(args)}")
    try:
        proc = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=os.environ | {"PYTHONUNBUFFERED": "1"}
        )
        if proc.stdout:
            for line in proc.stdout.splitlines():
                log(f"[TRAIN] {line}")
        return proc.returncode
    except Exception as e:
        log(f"[ERROR] 训练进程异常: {e}")
        return 1

def main_loop():
    log("[BOOT] leader_update_loop 启动")
    a = time.time()
    while not _stop:
        # 等 5 分钟
        log(f"[WAIT] 等待 {CYCLE_WAIT_SECONDS}s 后进入 {WINDOW_SECONDS}s 探测窗口")
        for _ in range(CYCLE_WAIT_SECONDS):
            if _stop: return
            time.sleep(1)

        window_deadline = time.time() + WINDOW_SECONDS
        updated = False

        while time.time() < window_deadline and not _stop:
            # —— 每次“尝试读取/快照”之前，都先判定是否仍是 leader —— #
            if not is_leader():
                time.sleep(CHECK_INTERVAL)
                continue

            log("[INFO] 发现本机为 leader，尝试快照 CSV 并直接训练（不做二次确认）")
            try:
                snap_path = snapshot_csv(SRC_CSV, SNAP_DIR)
            except Exception as e:
                # 可能是正在写、或 leader 已迁移导致 CSV 不在本机
                log(f"[WARN] 本次快照失败：{e}；稍后重试（先再次确认是否仍为 leader）")
                time.sleep(CHECK_INTERVAL)
                continue

            # 快照成功后，直接训练（不再确认 leader）
            rc = run_training(snap_path)
            try:
                Path(snap_path).unlink(missing_ok=True)
            except Exception:
                pass

            updated = True
            a = time.time()  # 刷新时间 a
            if rc == 0:
                log("[OK] 本轮训练完成，已更新时间 a，进入下一轮 5 分钟等待")
            else:
                log("[ERROR] 训练失败(退出码非0)，但仍进入下一轮 5 分钟等待")
            break  # 结束窗口，滚动到下一轮

        if not updated:
            log("[INFO] 窗口内未成功训练（可能一直未成为 leader），直接进入下一轮 5 分钟等待")
            a = time.time()

    log("[EXIT] 收到停止信号，安全退出")

if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        pass
