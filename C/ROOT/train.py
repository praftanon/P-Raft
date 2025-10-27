# train_seq2seq.py
import os, json, argparse
import numpy as np
import pandas as pd
from joblib import dump
from sklearn.preprocessing import MinMaxScaler

from ts_common import (
    set_seed, ensure_timestamp, create_dataset_multi,
    build_seq2seq_model, safe_inverse_transform
)
from sklearn.metrics import mean_squared_error
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

def run_train(args):
    set_seed(args.seed)

    df = pd.read_csv(args.input)
    df = ensure_timestamp(df, col="timestamp")

    # 只保留指定的 6 个节点列
    keep_cols = [
        "69f554c3f7f50a72_write", "69f554c3f7f50a72_read",
        "933ca51d2bb602b8_write", "933ca51d2bb602b8_read",
        "b92b49a4de72942d_write", "b92b49a4de72942d_read"
    ]

    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise ValueError(f"缺少需要的列: {missing}")

    df = df[keep_cols].dropna().astype("float32")

        # === 仅使用最后 N 行（默认 1800） ===
    if args.tail_rows and args.tail_rows > 0:
        if len(df) > args.tail_rows:
            df = df.tail(args.tail_rows).copy()
            print(f"[INFO] 使用 CSV 最后 {args.tail_rows} 行作为数据窗口（当前 df 行数={len(df)}）")
        else:
            print(f"[WARN] 数据行数({len(df)}) 少于 tail-rows({args.tail_rows})，将使用全部数据")
    
    feature_cols = keep_cols
    

    data = df.values
    n_features = data.shape[1]

    # 划分 + 归一化
    train_size = int(len(data) * args.train_ratio)
    if train_size <= args.look_back + args.horizon:
        raise ValueError("训练数据太短，请调小 look-back/horizon 或提供更多数据。")
    train_raw, test_raw = data[:train_size], data[train_size:]
    scaler = MinMaxScaler((0, 1))
    scaler.fit(train_raw)
    train_scaled = scaler.transform(train_raw)
    test_scaled  = scaler.transform(test_raw)

    # 构造样本
    X_train, y_train = create_dataset_multi(train_scaled, args.look_back, args.horizon)
    X_val,   y_val   = None, None
    if args.val_split > 0:
        # 直接用 validation_split 由 Keras 切分；这里不手动切
        pass

    # 建模 & 回调
    model = build_seq2seq_model(n_features, args.look_back, args.horizon, units=args.units)
    cbs = []
    if args.early_stopping:
        cbs.append(EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True))
    if args.reduce_lr:
        cbs.append(ReduceLROnPlateau(monitor="val_loss", factor=0.5, patience=3, min_lr=1e-5))

    model.fit(
        X_train, y_train,
        epochs=args.epochs,
        batch_size=args.batch_size,
        validation_split=args.val_split,
        callbacks=cbs,
        verbose=1
    )
    '''
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    import csv

    def mean_absolute_percentage_error(y_true, y_pred):
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        nonzero = y_true != 0
        return np.mean(np.abs((y_true[nonzero] - y_pred[nonzero]) / y_true[nonzero])) * 100

    # 简单评估（测试集）
    from ts_common import safe_inverse_transform
    X_test, y_test = create_dataset_multi(test_scaled, args.look_back, args.horizon)
    if X_test.shape[0] > 0:
        y_pred_scaled = model.predict(X_test, verbose=0)
        y_pred = safe_inverse_transform(scaler, y_pred_scaled.reshape(-1, n_features)).reshape(-1, args.horizon, n_features)
        y_true = safe_inverse_transform(scaler, y_test.reshape(-1, n_features)).reshape(-1, args.horizon, n_features)

        # 累积指标
        mae_list, rmse_list, mape_list, r2_list = [], [], [], []

        for i in range(len(feature_cols)):
            y_t = y_true[..., i].ravel()
            y_p = y_pred[..., i].ravel()

            mae_list.append(mean_absolute_error(y_t, y_p))
            mse = mean_squared_error(y_t, y_p)
            rmse_list.append(np.sqrt(mse))
            mape_list.append(mean_absolute_percentage_error(y_t, y_p))
            r2_list.append(r2_score(y_t, y_p))

        # 平均值
        avg_mae  = np.mean(mae_list)
        avg_rmse = np.mean(rmse_list)
        avg_mape = np.mean(mape_list)
        avg_r2   = np.mean(r2_list)

        # print("\n测试集整体平均指标：")
        # print(f"MAE={avg_mae:.4f}, RMSE={avg_rmse:.4f}, MAPE={avg_mape:.2f}%, R²={avg_r2:.4f}")

        # 保存到 CSV
        out_file = os.path.join(args.out_dir, "/etcd/etcd-release-3.4/mae_avgC.csv")
        file_exists = os.path.exists(out_file)

        with open(out_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:  # 文件不存在时写表头
                writer.writerow(["MAE", "RMSE", "MAPE(%)", "R2"])
            writer.writerow([avg_mae, avg_rmse, avg_mape, avg_r2])

        print(f"\n✅ 已保存平均指标到 {out_file}")
    '''
    # 保存
    os.makedirs(args.out_dir, exist_ok=True)
    model_path  = os.path.join(args.out_dir, args.model_out)
    scaler_path = os.path.join(args.out_dir, args.scaler_out)
    meta_path   = os.path.join(args.out_dir, args.meta_out)

    model.save(model_path)
    dump(scaler, scaler_path)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"columns": feature_cols, "look_back": int(args.look_back), "horizon": int(args.horizon)}, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 训练完成：\n  模型:   {model_path}\n  Scaler: {scaler_path}\n  元数据: {meta_path}")

def build_arg_parser():
    p = argparse.ArgumentParser(description="训练 Seq2Seq LSTM（多步直接预测）")
    p.add_argument("--input", required=True, help="训练 CSV 路径，需含 'timestamp'(秒) 与数值列")
    p.add_argument("--out-dir", default="./artifacts")
    p.add_argument("--model-out", default="lstm_seq2seq.h5")
    p.add_argument("--scaler-out", default="scaler.pkl")
    p.add_argument("--meta-out", default="model_meta.json")

    p.add_argument("--look-back", type=int, default=30)
    p.add_argument("--horizon", type=int, default=5)
    p.add_argument("--train-ratio", type=float, default=0.9)
    p.add_argument("--tail-rows", type=int, default=900,
                   help="仅使用 CSV 的最后 N 行进行训练与评估；0 表示不截取")


    p.add_argument("--epochs", type=int, default=30)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--units", type=int, default=64)
    p.add_argument("--val-split", type=float, default=0.1)

    p.add_argument("--early-stopping", action="store_true")
    p.add_argument("--reduce-lr", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    return p

if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    run_train(args)
