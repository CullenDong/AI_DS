"""SS03 IO-HMM churn 打标 runner —— 把 delivery(jobs/ss03_analysis/hmm/)接成完整流程:
Redshift spin → 特征(复用 delivery 的 export 函数) → io_hmm_infer 打标(stage/p_stop/risk)。

**以玩家为单位、追溯全历史**（关键，保 tenure 正确）:
  1. 先取 cohort：[cohort-start, cohort-end) 窗口内活跃的所有 user_id。
  2. 再把这些玩家的**全部历史注单**(created_at >= hist-start)整段拉出来。
  3. 建特征 → 打标。这样 k / no_bet_streak 才对（不会像按日期截窗那样把老玩家误判 first_day）。

模型/口径来自 bituslabs/common-ai-research 分支 yvonne01 的 ss03_churn_delivery:
  · 特征(5 发射)：avg_bet_one_time_today / bet_count_today / bet_level_distinct_today /
    deposit_amount_today / no_bet_streak_days（9 分钟 session 切北京日；刨 normal_kakuteiB）。
  · k=1→first_day；k≥2→IO-HMM。标签 stage(行为状态) / p_stop(21日流失概率) / risk(high/med/low)。
  · hist-start 默认模型 window_start 2026-05-21（其前的历史模型未训练、左删失，不追溯更早）。

用法：
  # 给 09-20~09-28 活跃玩家打标（追溯这些人从 05-21 起的全历史）
  python3 jobs/ss03_analysis/ss03_hmm_label.py --cohort-start 2026-09-20 --cohort-end 2026-09-29
  # 单日 cohort 快速验证
  python3 jobs/ss03_analysis/ss03_hmm_label.py --cohort-start 2026-09-27 --cohort-end 2026-09-28
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis" / "hmm"))
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis" / "hmm" / "inference" / "scripts"))
from tools.db import redshift as rs                              # noqa: E402
import export_ss03_features as ex                                # noqa: E402  (特征函数)
import io_hmm_infer as inf                                       # noqa: E402  (打标器)

MODEL = ROOT / "jobs" / "ss03_analysis" / "hmm" / "inference" / "assets" / "ss03_iohmm_model.json"
OUTDIR = ROOT / "data" / "output" / "hmm"; OUTDIR.mkdir(parents=True, exist_ok=True)
# 行为状态按 feature-median 命名（数字索引不稳定，按 profile 引用；见 DELIVERY）
STATE_PROFILE = {0: "s0", 1: "s1", 2: "s2", 3: "s3"}


def pull_spins(cohort_start, cohort_end, hist_start):
    """cohort 追溯：先取 [cohort_start,cohort_end) 活跃 user_id，再拉这些人 >= hist_start 的全历史。"""
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    cols = ", ".join("f." + c for c in ex.NEEDED_COLUMNS)
    n = be.execute(f"""SELECT COUNT(DISTINCT user_id) FROM public.fct_bet_orders
        WHERE game_id='SS03' AND currency_type='CNY' AND status='COMPLETED'
          AND created_at >= '{cohort_start}' AND created_at < '{cohort_end}'""")[0][0]
    print(f"[cohort] {cohort_start}~{cohort_end} 活跃玩家 {n:,} 人 → 追溯其 >= {hist_start} 全历史")
    df = be.query_to_df(f"""
      WITH cohort AS (
        SELECT DISTINCT user_id FROM public.fct_bet_orders
        WHERE game_id='SS03' AND currency_type='CNY' AND status='COMPLETED'
          AND created_at >= '{cohort_start}' AND created_at < '{cohort_end}')
      SELECT {cols} FROM public.fct_bet_orders f
      JOIN cohort c ON f.user_id = c.user_id
      WHERE f.game_id='SS03' AND f.currency_type='CNY' AND f.status='COMPLETED'
        AND f.created_at >= '{hist_start}'""")
    be.close()
    df = df.drop_duplicates("spin_id", ignore_index=True)
    df = df[~df["math_table_id"].isin(ex.EXCLUDE_MATH_TABLES)].copy()   # 刨 kakuteiB
    for c in ex.DECIMAL_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["retrigger"] = df["retrigger"].astype(bool)
    print(f"[load] {len(df):,} spins, {df['user_id'].nunique():,} users, {df['created_at'].min()} .. {df['created_at'].max()}")
    return df


def build_features(df):
    df = ex.add_session_day(df)
    df = ex.add_deposit(df)
    base = ex.fold_free_payout(df)
    day = ex.aggregate_user_day(df, base)
    day = ex.add_history_features(day)
    return day


def label(day, model):
    day = day.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    day["k"] = day.groupby("user_id").cumcount() + 1
    feats = model["features"]
    need = ["user_id", "bet_date", "bet_amount_today"] + feats
    # k≥2 且特征含 NaN 的行会被 score 拒绝 → 先剔(极少)
    k2nan = (day["k"] >= 2) & day[feats].isna().any(axis=1)
    if k2nan.any():
        print(f"[label] 剔除 k≥2 特征含 NaN 行 {int(k2nan.sum()):,}")
    scored = inf.score(day.loc[~k2nan, need], model)
    return day, scored


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cohort-start", default="2026-09-20")   # 定义要打标的玩家(活跃窗口)
    ap.add_argument("--cohort-end", default="2026-09-29")
    ap.add_argument("--hist-start", default="2026-05-21")     # 追溯历史下界(=模型 window_start)
    a = ap.parse_args()
    model = inf.load_model(str(MODEL))
    day = build_features(pull_spins(a.cohort_start, a.cohort_end, a.hist_start))
    day.to_parquet(OUTDIR / "ss03_user_day_features.parquet", index=False)
    day, scored = label(day, model)
    scored.to_parquet(OUTDIR / "ss03_hmm_labels.parquet", index=False)
    print(f"\n[out] 特征 {len(day):,} 用户-日 → ss03_user_day_features.parquet")
    print(f"[out] 标签 {len(scored):,} 行 → ss03_hmm_labels.parquet")
    print("\n=== risk 分层分布(全部标签) ===")
    print(scored["risk"].value_counts().to_string())
    cur = inf.current_label(day[["user_id", "bet_date", "bet_amount_today"] + model["features"]].dropna(subset=model["features"]), model) \
        if False else scored.sort_values("bet_date").groupby("user_id").tail(1)
    print("\n=== 每用户当前(最新bet-day) risk 分布 ===")
    print(cur["risk"].value_counts().to_string())
    print(f"高危(high)用户: {int((cur['risk']=='high').sum()):,} / {cur['user_id'].nunique():,}")


if __name__ == "__main__":
    main()
