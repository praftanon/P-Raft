# forecaster.py
import numpy as np
import pandas as pd
from joblib import load as joblib_load
from tensorflow.keras.models import load_model

from ts_common import (
    ensure_timestamp,      # 正确名称：没有下划线
    infer_step_delta,      # 正确名称：没有下划线
    safe_inverse_transform # 反缩放并裁负值
)

class Forecaster:
    def __init__(self, model_path: str, scaler_path: str, meta_path: str):
        import json
        self.model  = load_model(model_path)
        self.scaler = joblib_load(scaler_path)
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        self.feature_cols = list(meta["columns"])
        self.look_back    = int(meta["look_back"])
        self.horizon      = int(meta["horizon"])

    def predict(
        self,
        data,                       # 路径或 DataFrame
        use_last_rows: int | None = None,
        fixed_step_sec: int | None = None,
        return_dataframe: bool = True
    ):
        # 读数据
        if isinstance(data, str):
            df = pd.read_csv(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            raise TypeError("data 必须是文件路径或 pandas.DataFrame")

        TRAIN_FEATURES = [
            "69f554c3f7f50a72_write", "69f554c3f7f50a72_read",
            "933ca51d2bb602b8_write", "933ca51d2bb602b8_read",
            "b92b49a4de72942d_write", "b92b49a4de72942d_read",
        ]

        needed = ["timestamp"] + TRAIN_FEATURES
        df = df.loc[:, needed]  # 按列名精确选取

        # 确保全是 float
        for c in TRAIN_FEATURES:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        df = ensure_timestamp(df, col="timestamp")  # ← 正确函数名

        # 对齐列
        missing = [c for c in self.feature_cols if c not in df.columns]
        if missing:
            raise ValueError(f"输入缺少训练时的特征列: {missing}")
        df = df[self.feature_cols].astype("float32").dropna()

        total_len = len(df)
        if total_len < self.look_back:
            raise ValueError(f"数据太短：总行数 {total_len} < look_back {self.look_back}。")

        # 历史窗口（最后 N 行）
        N = self.look_back if use_last_rows is None else int(use_last_rows)
        if N < self.look_back:
            N = self.look_back
        if N > total_len:
            N = total_len

        hist_values = df.iloc[-N:].values
        hist_index  = df.index[-N:]

        # 标准化 & 取最后 look_back
        X = self.scaler.transform(hist_values)[-self.look_back:, :][np.newaxis, ...]

        # 预测（反缩放用 safe_inverse_transform）
        preds_scaled = self.model.predict(X, verbose=0)[0]
        preds = safe_inverse_transform(self.scaler, preds_scaled)

        # 时间索引
        if fixed_step_sec is not None:
            step = pd.Timedelta(seconds=int(fixed_step_sec))
        else:
            step = infer_step_delta(hist_index)  # ← 正确函数名
            # 兜底：推断失败则回退 1s
            if pd.isna(step) or step <= pd.Timedelta(0):
                step = pd.Timedelta(seconds=1)

        start_ts = hist_index[-1] + step
        future_index = pd.date_range(start=start_ts, periods=self.horizon, freq=step)

        if return_dataframe:
            out = pd.DataFrame(preds, index=future_index, columns=self.feature_cols)
            out.index.name = "timestamp"
            return out
        else:
            return future_index, preds
