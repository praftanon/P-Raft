#!/usr/bin/env python3
"""
ycsb_mix_controller.py
----------------------
Control go-ycsb to generate a changing read/write mix for etcd.

Features
1) Run a single initial "load" once.
2) Then every N seconds, stop the previous go-ycsb "run" and start a new one
   with a fresh random write proportion; read proportion is (1 - write).
3) Write proportion is sampled uniformly in [min_write, max_write] (defaults 0..1).

Example:
  python ycsb_mix_controller.py \
    --go-ycsb ./go-ycsb \
    --endpoints http://192.168.0.38:2379,http://192.168.0.62:2379,http://192.168.0.104:2379 \
    --threads 100 \
    --recordcount 1000 \
    --interval 10 \
    --duration 300 \
    --base-props workloads/myworkload

Notes:
- Works best on Linux/macOS. On Windows, process-group termination is best-effort.
- If you provide --base-props, dynamic -p flags here will override the same keys.
- Only read/update ops are used (insert/scan/rmw proportions are forced to 0).

Author: ChatGPT
"""
import argparse
import os
import random
import signal
import sys
import time
import subprocess
from datetime import datetime

def build_p_flags(props: dict):
    """Return a flat list like ['-p','k=v','-p','k2=v2', ...]"""
    flags = []
    for k, v in props.items():
        flags.extend(["-p", f"{k}={v}"])
    return flags

def start_proc(cmd):
    """
    Start a subprocess in its own process group (so we can kill cleanly).
    Returns Popen.
    """
    if os.name == "nt":
        # Windows: CREATE_NEW_PROCESS_GROUP = 0x00000200
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        return subprocess.Popen(cmd, creationflags=CREATE_NEW_PROCESS_GROUP)
    else:
        # POSIX
        return subprocess.Popen(cmd, preexec_fn=os.setsid)

def stop_proc(proc, graceful_seconds=2):
    """Terminate a running Popen proc and its group."""
    if proc is None:
        return
    try:
        if os.name == "nt":
            proc.terminate()
            try:
                proc.wait(timeout=graceful_seconds)
            except subprocess.TimeoutExpired:
                proc.kill()
        else:
            # Send SIGTERM to the whole group
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            # wait a bit
            deadline = time.time() + graceful_seconds
            while time.time() < deadline:
                ret = proc.poll()
                if ret is not None:
                    return
                time.sleep(0.1)
            # then SIGKILL if still alive
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    finally:
        try:
            proc.wait(timeout=1)
        except Exception:
            pass

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def run_once(args):
    # Validate go-ycsb path
    if not os.path.exists(args.go_ycsb):
        print(f"[FATAL] go-ycsb not found at: {args.go_ycsb}", file=sys.stderr)
        sys.exit(2)

    # Common props
    common = {
        "workload": "core",
        "requestdistribution": args.request_distribution,
        "threadcount": args.threads,
        "table": args.table,
        "fieldcount": args.fieldcount,
        "fieldlength": args.fieldlength,
        "readallfields": "true",
        "etcd.endpoints": args.endpoints,
        "etcd.dialtimeout": args.dial_timeout,
        "etcd.reqtimeout": args.req_timeout,
    }

    # === 1) LOAD once ===
    load_props = dict(common)
    load_props["recordcount"] = args.recordcount

    load_cmd = [args.go_ycsb, "load", "etcd"]
    if args.base_props:
        load_cmd.extend(["-P", args.base_props])
    load_cmd += build_p_flags(load_props)

    print("[INFO] Running initial LOAD:")
    print("       ", " ".join(map(str, load_cmd)))
    ret = subprocess.call(load_cmd)
    if ret != 0:
        print(f"[FATAL] LOAD failed with exit code {ret}", file=sys.stderr)
        sys.exit(ret)
    print("[OK] Load complete.")

    # === 2) Repeated RUN with changing mix ===
    run_props_static = dict(common)
    # Ensure RUN uses the same keyspace as LOAD
    run_props_static["recordcount"] = args.recordcount
    # zero-out other ops to ensure pure read/update mix
    run_props_static.update({
        "insertproportion": 0.0,
        "scanproportion": 0.0,
        "readmodifywriteproportion": 0.0,
        # Make sure we are in transaction mode on RUN
        "dotransactions": "true",
        # Keep running; we'll kill by time. You can also set a finite opcount.
        "operationcount": args.operationcount,
    })

    logfile = open(args.log_csv, "a", buffering=1)
    if logfile.tell() == 0:
        logfile.write("ts,iter,write_prop,read_prop,pid\n")

    print(f"[INFO] Starting RUN loop: interval={args.interval}s, duration={args.duration or 'infinite'}s")
    print(f"[INFO] Logging proportions to: {args.log_csv}")
    proc = None

    # Handle Ctrl+C cleanly
    stopping = {"flag": False}
    def on_sigint(signum, frame):
        print("\n[INFO] Caught interrupt, stopping...")
        stopping["flag"] = True
    signal.signal(signal.SIGINT, on_sigint)

    t0 = time.time()
    i = 0
    try:
        while True:
            if args.duration and (time.time() - t0 >= args.duration):
                break

            # Random write proportion in [min_write,max_write]; read = 1 - write
            write_prop = random.uniform(args.min_write, args.max_write)
            write_prop = clamp01(write_prop)
            read_prop = clamp01(1.0 - write_prop)

            # Build per-iteration props
            iter_props = dict(run_props_static)
            iter_props["updateproportion"] = round(write_prop, 6)
            iter_props["readproportion"] = round(read_prop, 6)

            run_cmd = [args.go_ycsb, "run", "etcd"]
            if args.base_props:
                run_cmd.extend(["-P", args.base_props])
            run_cmd += build_p_flags(iter_props)

            # Stop previous and start new
            stop_proc(proc, graceful_seconds=args.graceful_stop_seconds)
            proc = start_proc(run_cmd)

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] iter={i} pid={proc.pid} write={iter_props['updateproportion']} read={iter_props['readproportion']}")
            logfile.write(f"{ts},{i},{iter_props['updateproportion']},{iter_props['readproportion']},{proc.pid}\n")

            # Sleep interval or until stop requested
            slept = 0.0
            while slept < args.interval:
                if stopping["flag"]:
                    break
                time.sleep(min(0.2, args.interval - slept))
                slept += 0.2

            if stopping["flag"]:
                break
            i += 1
    finally:
        stop_proc(proc, graceful_seconds=args.graceful_stop_seconds)
        logfile.close()
        print("[OK] Stopped.")

def parse_args():
    p = argparse.ArgumentParser(description="Control go-ycsb read/write mix for etcd with periodic restarts.")
    p.add_argument("--go-ycsb", required=True, help="Path to go-ycsb binary (e.g., ./go-ycsb)")
    p.add_argument("--endpoints", required=True, help="Comma-separated etcd endpoints, e.g. http://192.168.0.38:2379,http://...")
    p.add_argument("--base-props", default="", help="Optional base properties file passed via -P (e.g., workloads/myworkload)")

    # Dataset / workload basics
    p.add_argument("--table", default="usertable")
    p.add_argument("--recordcount", type=int, default=1000, help="Records to load once")
    p.add_argument("--threads", type=int, default=100)
    p.add_argument("--fieldcount", type=int, default=1)
    p.add_argument("--fieldlength", type=int, default=1)
    p.add_argument("--request-distribution", dest="request_distribution", default="uniform",
                   choices=["uniform","zipfian","latest"])

    # etcd driver timeouts
    p.add_argument("--dial-timeout", dest="dial_timeout", default="20s")
    p.add_argument("--req-timeout", dest="req_timeout", default="30s")

    # Run loop timing
    p.add_argument("--interval", type=int, default=10, help="Seconds between restarts")
    p.add_argument("--duration", type=int, default=0, help="Total seconds to run; 0=until Ctrl+C")
    p.add_argument("--graceful-stop-seconds", type=int, default=2, help="Grace period when stopping go-ycsb")

    # Operation count for each run (we kill by time anyway)
    p.add_argument("--operationcount", type=int, default=10_000_000)

    # Random write proportion range
    p.add_argument("--min-write", type=float, default=0.0, help="Minimum write proportion")
    p.add_argument("--max-write", type=float, default=1.0, help="Maximum write proportion")

    # Logging
    p.add_argument("--log-csv", default="mix_log.csv", help="CSV to record per-iteration ratios")

    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.min_write < 0 or args.max_write > 1 or args.min_write > args.max_write:
        print("[FATAL] --min-write/--max-write must satisfy 0 <= min <= max <= 1", file=sys.stderr)
        sys.exit(2)
    # Normalize endpoints (remove spaces)
    args.endpoints = ",".join([e.strip() for e in args.endpoints.split(",") if e.strip()])
    run_once(args)
