import argparse

import numpy as np
import pandas as pd

# loader 输出的 8 列。下游（explore_churn_states / io_hmm）再 drop near-constant 的
# multiplier_change_count_ratio -> 实际喂模型的是 Golden 7（canonical，见 report_model_selection.md）。
FEATURE_NAMES = [
    "no_bet_streak_days",
    "bet_amount_ratio_today_vs_history",
    "avg_bet_one_time_today_log",
    "rtp_7_bet_days",
    "loss_streak_ratio_today",
    "current_balance_max_to_avg_bet_ratio",
    "target_selection_entropy",
    "multiplier_change_count_ratio",
]


def load_model_data(csv_path):
    """读 DS 导出的 CSV，产出模型直接能用的 (X_raw, lengths, feature_names)。

    - 选 Golden 8 列（缺列直接 raise）
    - drop 每个用户的首投日行（首日历史类特征是 NaN 的唯一来源）
    - 按 [user_id, bet_date] 排序后重建 lengths（HMM 靠它切每个用户的序列）
    - 断言无残留 NaN / sum(lengths)==行数 / 非空，否则 raise（早崩，不静默 fillna）

    X_raw 不标准化：HMM 侧由下游（explore_churn_states / io_hmm）自己 StandardScaler，决策树要用未标准化的 X_raw 出真实阈值。
    """
    df = pd.read_csv(csv_path, parse_dates=["bet_date"])

    missing = [c for c in ["user_id", "bet_date", *FEATURE_NAMES] if c not in df.columns]
    if missing:
        raise ValueError(f"CSV 缺少列: {missing}（来自 {csv_path}）")

    df = df.sort_values(["user_id", "bet_date"], kind="stable")
    df = df[df.groupby("user_id").cumcount() > 0]  # drop 每用户最早 bet_date 那行
    if df.empty:
        raise ValueError("drop 首投日后没有数据了（是不是全是单投注日用户？）")

    lengths = df.groupby("user_id", sort=False).size().tolist()
    X_raw = df[FEATURE_NAMES].to_numpy(dtype=float)

    if np.isnan(X_raw).any():
        bad = [FEATURE_NAMES[i] for i in np.unique(np.where(np.isnan(X_raw))[1])]
        raise ValueError(f"drop 首投日后仍有 NaN，列: {bad} —— NaN 来源不止首日，需排查后再定策略")
    if sum(lengths) != len(X_raw):
        raise ValueError(f"lengths 求和 {sum(lengths)} != 行数 {len(X_raw)}")

    return X_raw, lengths, FEATURE_NAMES


def main():
    parser = argparse.ArgumentParser(description="把 DS 导出的 HMM 特征 CSV 处理成模型可用的 (X_raw, lengths)。")
    parser.add_argument("csv_path", help="DS 导出的 selected_hmm_features_*.csv")
    args = parser.parse_args()

    X_raw, lengths, feature_names = load_model_data(args.csv_path)
    print(f"用户数: {len(lengths)}")
    print(f"样本(行)数: {len(X_raw)}")
    print(f"特征数: {len(feature_names)} -> {feature_names}")
    print(f"序列长度 lengths: min={min(lengths)} max={max(lengths)} sum={sum(lengths)}")


if __name__ == "__main__":
    main()