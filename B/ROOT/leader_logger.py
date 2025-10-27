import os
import csv
import time
from typing import List

class BufferedPredictedLeaderWriter:
    """
    缓冲写入预测leader的日志。
    - 未迁移/预测为本机 → 先放内存
    - 检测到将要迁移 → 把缓冲+当前记录一次性落到CSV
    """

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.buffer: List[List[str]] = []

    def _ensure_file(self):
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["timestamp", "predicted_leader_ip"])

    def _flush_buffer(self):
        if not self.buffer:
            return
        self._ensure_file()
        with open(self.csv_path, "a", newline="") as f:
            w = csv.writer(f)
            w.writerows(self.buffer)
        self.buffer.clear()

    def record(self, leader_ip: str, will_move: bool):
        """
        leader_ip: 本次预测的leader（如果是本机，也要写本机IP）
        will_move: 是否需要迁移
        """
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        row = [now, leader_ip]

        if not will_move:
            # 不迁移 → 只放缓存
            self.buffer.append(row)
        else:
            # 迁移 → 缓存+当前一起落盘
            self.buffer.append(row)
            self._flush_buffer()

    def flush_remaining(self):
        """程序退出前可调用，落盘还没写出的缓存"""
        self._flush_buffer()
