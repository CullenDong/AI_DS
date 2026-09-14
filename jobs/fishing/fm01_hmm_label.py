"""用 IO-HMM churn 模型给 FM01 每用户每北京日打生命周期状态标签,并出
每日数量分布 + 各状态击打表现总结。

模型:jobs/fishing/hmm_infer/(io_hmm_infer.py + iohmm_model.json,来自
bituslabs/common-ai-research HMM_player_states 的 fm01_churn_delivery)。
状态:k=1→T1(first_day)· 0→S1(Low)· 1→S2(Engaged)· 2→S3(Lapsed)。
输入特征表:data/output/hmm/fm01_hmm_features/(需含每用户完整历史,tenure 才对)。
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "fishing" / "hmm_infer"))
import io_hmm_infer as inf  # noqa: E402

FEATDIR = ROOT / "data" / "output" / "hmm" / "fm01_hmm_features"
OUTDIR = ROOT / "data" / "output" / "hmm"
MODEL = ROOT / "jobs" / "fishing" / "hmm_infer" / "iohmm_model.json"

FEAT7 = ["no_bet_streak_days", "bet_amount_ratio_today_vs_history", "avg_bet_one_time_today_log",
         "rtp_7_bet_days", "loss_streak_ratio_today", "current_balance_max_to_avg_bet_ratio",
         "target_selection_entropy"]
RAW = ["bet_count_today", "bet_amount_today", "payout_today", "profit_today", "rtp_day",
       "avg_bet_one_time_today", "max_consecutive_loss_count_today"]
STATE = {"T1": "T1_first_day", 0: "S1_Low", 1: "S2_Engaged", 2: "S3_Lapsed"}


def main():
    df = pd.read_parquet(FEATDIR, columns=["user_id", "bet_date"] + FEAT7 + RAW)
    df["bet_date"] = pd.to_datetime(df["bet_date"])
    df = df.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    df["k"] = df.groupby("user_id").cumcount() + 1
    print(f"特征表: {len(df):,} 用户-日, {df['user_id'].nunique():,} 用户")

    # k>=2 打模型;k>=2 特征若有 NaN 会被 score 拒绝,先剔(极少)
    k2 = df[df["k"] >= 2]
    nan_mask = k2[FEAT7].isna().any(axis=1)
    if nan_mask.any():
        bad = df.index[df["k"] >= 2][nan_mask.values]
        print(f"  剔除 k>=2 中特征含 NaN 的行: {nan_mask.sum():,}")
        df_score = df.drop(index=bad)
    else:
        df_score = df
    model = inf.load_model(str(MODEL))
    scored = inf.score(df_score[["user_id", "bet_date", "bet_amount_today"] + FEAT7], model)
    # scored: user_id,bet_date,k,stage,p_stop,risk (k>=2 + 单日用户的 first_day)
    s2 = scored[scored["k"] >= 2][["user_id", "bet_date", "stage", "p_stop", "risk"]]

    # 全量标签:k==1 → T1;k>=2 → 模型 stage(纯字符串,避免混型)
    lab = df[["user_id", "bet_date", "k"] + RAW].merge(s2, on=["user_id", "bet_date"], how="left")
    def name_row(k, stage):
        if k == 1:
            return STATE["T1"]
        if pd.isna(stage):
            return "unlabeled"
        return STATE.get(int(stage), "unlabeled")
    lab["state_name"] = [name_row(k, st) for k, st in zip(lab["k"], lab["stage"])]

    # 落地全量标签
    lab.to_parquet(OUTDIR / "fm01_hmm_labels.parquet", index=False)
    order = ["T1_first_day", "S1_Low", "S2_Engaged", "S3_Lapsed"]

    # ---- 每日数量分布 ----
    daily = lab.groupby([lab["bet_date"].dt.date, "state_name"]).size().unstack(fill_value=0)
    for c in order:
        if c not in daily.columns:
            daily[c] = 0
    daily = daily[order]
    daily.to_csv(OUTDIR / "fm01_hmm_daily_state_counts.csv")
    print("\n=== 每日状态数量分布(尾 14 天) ===")
    print(daily.tail(14).to_string())
    print(f"\n(完整每日分布已存 {OUTDIR/'fm01_hmm_daily_state_counts.csv'})")
    print("\n各状态 总用户-日 & 占比:")
    tot = lab["state_name"].value_counts().reindex(order)
    for s in order:
        print(f"  {s:14} {int(tot[s]):>9,}  ({tot[s]/len(lab)*100:5.1f}%)")

    # ---- 各状态击打表现 ----
    def wmed(g, col):
        return g[col].median()
    print("\n=== 各状态 击打表现(每状态跨全部用户-日) ===")
    print(f"{'状态':16}{'用户-日':>9}{'投注额中':>9}{'发数中':>8}{'RTP%':>7}{'日净中':>8}{'连亏比中':>9}")
    for s in order:
        g = lab[lab["state_name"] == s]
        if not len(g):
            continue
        rtp = g["payout_today"].sum() / g["bet_amount_today"].sum() * 100
        loss_ratio = (g["max_consecutive_loss_count_today"] / g["bet_count_today"]).median()
        print(f"{s:16}{len(g):>9,}{g['bet_amount_today'].median():>9.0f}{g['bet_count_today'].median():>8.0f}"
              f"{rtp:>7.1f}{g['profit_today'].median():>8.0f}{loss_ratio:>9.2f}")


if __name__ == "__main__":
    main()
