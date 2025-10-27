# ts_common.py
import json, os, warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from joblib import dump, load as joblib_load
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, RepeatVector, TimeDistributed

# ---------- 工具 ----------
def set_seed(seed: int = 42):
    np.random.seed(seed)
    tf.random.set_seed(seed)

def ensure_timestamp(df: pd.DataFrame, col: str = "timestamp"):
    if col not in df.columns:
        raise ValueError(f"CSV 必须包含 '{col}' 列（单位为秒）。")
    df[col] = pd.to_datetime(df[col], unit="s")
    return df.set_index(col)

def infer_step_delta(ts_index: pd.DatetimeIndex) -> pd.Timedelta:
    diffs = pd.Series(ts_index).diff().dropna()
    return diffs.median() if len(diffs) else pd.Timedelta(seconds=1)

def create_dataset_multi(dataset: np.ndarray, look_back: int, horizon: int):
    X, Y = [], []
    limit = len(dataset) - look_back - horizon + 1
    for i in range(limit):
        X.append(dataset[i:i+look_back, :])
        Y.append(dataset[i+look_back:i+look_back+horizon, :])
    if not X:
        f = dataset.shape[1]
        return np.empty((0, look_back, f)), np.empty((0, horizon, f))
    return np.array(X), np.array(Y)

def build_seq2seq_model(n_features: int, look_back: int, horizon: int, units: int = 64) -> Sequential:
    model = Sequential([
        LSTM(units, input_shape=(look_back, n_features)),
        RepeatVector(horizon),
        LSTM(units, return_sequences=True),
        TimeDistributed(Dense(n_features, activation="relu"))
    ])
    model.compile(loss="mean_squared_error", optimizer="adam")
    return model

def safe_inverse_transform(scaler: MinMaxScaler, arr_2d: np.ndarray) -> np.ndarray:
    out = scaler.inverse_transform(arr_2d)
    out[out < 0] = 0
    return out
