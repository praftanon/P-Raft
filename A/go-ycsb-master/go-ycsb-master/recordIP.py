#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Sequence
import csv
import os


def write_hotspot_plan(
    start_time: str,
    first_ip: str = "192.168.0.38",            # 默认固定为 38 的 IP
    domain_ips: Sequence[str] = ("192.168.0.38", "192.168.0.82", "192.168.0.223"),
    slot_secs: int = 10,
    horizon_minutes: int = 120,
    out_csv: str = "/etcd/etcd-release-3.4/rightIP.csv",
) -> str:
    """
    根据第一条的时间和第一个IP，循环输出未来时间段的高负载IP。
    """
    if len(domain_ips) < 1:
        raise ValueError("domain_ips 必须至少给出 1 个 IP")

    # 解析起始时间
    try:
        t0 = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError("start_time 格式必须是 'YYYY-MM-DD HH:MM:SS'")

    # 确定第一条的索引（默认 38 的 IP）
    try:
        first_idx = list(domain_ips).index(first_ip)
    except ValueError:
        raise ValueError(f"first_ip 不在 domain_ips 中：{first_ip!r}")

    if slot_secs <= 0 or horizon_minutes <= 0:
        raise ValueError("slot_secs 和 horizon_minutes 必须为正整数")

    total_slots = (horizon_minutes * 60) // slot_secs
    if total_slots <= 0:
        raise ValueError("规划时长太短，无法生成任何条目")

    new_file = not os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["time", "ip"])
        for s in range(total_slots):
            when = t0 + timedelta(seconds=s * slot_secs)
            dom_idx = (first_idx + s) % len(domain_ips)
            ip = domain_ips[dom_idx]
            w.writerow([when.strftime("%Y-%m-%d %H:%M:%S"), ip])

    return out_csv

