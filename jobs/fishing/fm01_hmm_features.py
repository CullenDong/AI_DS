"""FM01 生命周期 HMM 特征表 —— 在 Redshift 复现 aws-example 的
feature_engineer_life_cycle 口径,每用户每【北京日】一行(北京时间 0点-0点 日结),
落地到本地文件。供 HMM 拟合/解码生命周期状态(T1/S1/S2/S3)用。

口径来源:.claude/skills/hmm-lifecycle-features/(reference/feature_engineer_life_cycle.py)。
差异:按用户要求用**北京日**(created_at+8h)::date 直接日结,不做 180s 会话化跨零点归属。

用法:
  python3 jobs/fishing/fm01_hmm_features.py [--start 2026-07-01] [--end 2026-08-31]
  (end 排他上界;不给则取"最后一个完整北京日"+1)
输出:data/output/hmm/fm01_hmm_features_<start>_<end-1>.parquet + .csv
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUTDIR = ROOT / "data" / "output" / "hmm"
OUTDIR.mkdir(parents=True, exist_ok=True)
FILT = "currency_type='CNY' AND game_id='FM01' AND op_code NOT IN ('B26','TST','TSB','TSO')"

# ---- Stage 1: 每(user_id, 北京日) 基础聚合(含日内有序特征) ----
def base_sql(start, end):
    return f"""
WITH base AS (
  SELECT user_id,
    (created_at + interval '8 hours')::date AS bet_date,
    (created_at + interval '8 hours')      AS ts_bj,
    bullet_level, multiplier, fish_value,
    CAST(bet AS DOUBLE PRECISION) bet, CAST(payout AS DOUBLE PRECISION) payout,
    COALESCE(CAST(profit AS DOUBLE PRECISION), CAST(payout AS DOUBLE PRECISION)-CAST(bet AS DOUBLE PRECISION)) profit,
    CAST(curr_balance AS DOUBLE PRECISION) curr_balance,
    COALESCE(event_id, CAST(bullet_id AS VARCHAR), '') ord
  FROM public.bullet
  WHERE {FILT}
    AND (created_at + interval '8 hours')::date >= '{start}'
    AND (created_at + interval '8 hours')::date <  '{end}'
),
enr AS (
  SELECT *,
    LAG(bullet_level) OVER (PARTITION BY user_id, bet_date ORDER BY ts_bj, ord) prev_bl,
    LAG(multiplier)   OVER (PARTITION BY user_id, bet_date ORDER BY ts_bj, ord) prev_mult,
    SUM(CASE WHEN profit < 0 THEN 0 ELSE 1 END)
      OVER (PARTITION BY user_id, bet_date ORDER BY ts_bj, ord
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nlrg
  FROM base
),
loss_runs AS (
  SELECT user_id, bet_date, nlrg, COUNT(*) rl FROM enr WHERE profit < 0 GROUP BY 1,2,3
),
loss_day AS (
  SELECT user_id, bet_date, MAX(rl) max_consec_loss FROM loss_runs GROUP BY 1,2
),
day_base AS (
  SELECT user_id, bet_date,
    COUNT(*) bet_count_today, SUM(bet) bet_amount_today, AVG(bet) avg_bet_one_time_today,
    SUM(payout) payout_today, SUM(profit) profit_today,
    CASE WHEN SUM(bet)>0 THEN SUM(payout)/SUM(bet) END rtp_day,
    MAX(curr_balance) current_balance_day_max,
    SUM(CASE WHEN prev_bl   IS NOT NULL AND bullet_level<>prev_bl   THEN 1 ELSE 0 END) bullet_level_change_count_day,
    SUM(CASE WHEN prev_mult IS NOT NULL AND multiplier  <>prev_mult THEN 1 ELSE 0 END) multiplier_change_count_day,
    CAST(SUM(CASE WHEN fish_value BETWEEN 2   AND 10   THEN 1 ELSE 0 END) AS DOUBLE PRECISION)/COUNT(*) low_ratio,
    CAST(SUM(CASE WHEN fish_value BETWEEN 15  AND 130  THEN 1 ELSE 0 END) AS DOUBLE PRECISION)/COUNT(*) medium_ratio,
    CAST(SUM(CASE WHEN fish_value BETWEEN 150 AND 200  THEN 1 ELSE 0 END) AS DOUBLE PRECISION)/COUNT(*) high_ratio,
    CAST(SUM(CASE WHEN fish_value BETWEEN 500 AND 1000 THEN 1 ELSE 0 END) AS DOUBLE PRECISION)/COUNT(*) ultra_ratio
  FROM enr GROUP BY 1,2
)
SELECT d.*, COALESCE(l.max_consec_loss, 0) AS max_consecutive_loss_count_today
FROM day_base d LEFT JOIN loss_day l ON d.user_id=l.user_id AND d.bet_date=l.bet_date
"""


# ---- Stage 2: 历史/趋势派生特征(per-user 按 bet_date 序列) ----
def add_hmm_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    g = df.groupby("user_id", sort=False)
    cnt_prior = g.cumcount()                                   # 之前投注日数(0起)
    # 历史(不含当天)扩展均值 = 严格之前行 cumsum / 之前行数(向量化,快)
    def hist_mean(col):
        csum_prior = g[col].cumsum() - df[col]
        return np.where(cnt_prior > 0, csum_prior / cnt_prior, np.nan)
    df["prev_bet_date"] = g["bet_date"].shift(1)
    df["no_bet_streak_days"] = (df["bet_date"] - df["prev_bet_date"]).dt.days - 1
    hist_amt = hist_mean("bet_amount_today")
    df["bet_amount_ratio_today_vs_history"] = np.where(hist_amt > 0, df["bet_amount_today"]/hist_amt, np.nan)
    hist_cnt = hist_mean("bet_count_today")
    df["bet_count_ratio_today_vs_history"] = np.where(hist_cnt > 0, df["bet_count_today"]/hist_cnt, np.nan)
    df["avg_bet_one_time_today_log"] = np.log1p(df["avg_bet_one_time_today"].clip(lower=0))
    # 近 7 个投注日滚动 RTP(按行=投注日,不是自然7天)
    pay7 = g["payout_today"].rolling(7, min_periods=1).sum().reset_index(level=0, drop=True)
    amt7 = g["bet_amount_today"].rolling(7, min_periods=1).sum().reset_index(level=0, drop=True)
    df["rtp_7_bet_days"] = np.where(amt7 > 0, pay7/amt7, np.nan)
    df["loss_streak_ratio_today"] = df["max_consecutive_loss_count_today"]/df["bet_count_today"]
    hist_1t = hist_mean("avg_bet_one_time_today")
    df["current_balance_max_to_avg_bet_ratio"] = np.where(hist_1t > 0, df["current_balance_day_max"]/hist_1t, np.nan)
    def ent(c):
        c = c.fillna(0.0).clip(lower=0)
        return np.where(c > 0, c*np.log(c), 0.0)
    df["target_selection_entropy"] = -(ent(df.low_ratio)+ent(df.medium_ratio)+ent(df.high_ratio)+ent(df.ultra_ratio))/np.log(4.0)
    df["multiplier_change_count_ratio"] = df["multiplier_change_count_day"]/df["bet_count_today"]
    df["bullet_level_change_count_ratio"] = df["bullet_level_change_count_day"]/df["bet_count_today"]
    return df.drop(columns=["prev_bet_date"])


HMM_FEATURES = ["no_bet_streak_days", "bet_amount_ratio_today_vs_history", "bet_count_ratio_today_vs_history",
                "avg_bet_one_time_today_log", "rtp_7_bet_days", "loss_streak_ratio_today",
                "current_balance_max_to_avg_bet_ratio", "target_selection_entropy",
                "multiplier_change_count_ratio", "bullet_level_change_count_ratio"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-07-01", help="北京日下界(含)")
    ap.add_argument("--end", default=None, help="北京日上界(排他);默认=最后完整北京日+1")
    a = ap.parse_args()
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5543)
    maxbj = pd.Timestamp(be.execute(
        f"SELECT MAX((created_at+interval '8 hours')::date) FROM public.bullet WHERE {FILT}")[0][0])
    end = a.end or str((maxbj).date())            # maxbj 当天残缺 → 上界排他到 maxbj(即含到 maxbj-1)
    last = pd.Timestamp(end) - pd.Timedelta(days=1)
    print(f"窗口(北京日 0-0 日结): {a.start} ~ {last.date()}  (end 排他 {end})")
    base = be.query_to_df(base_sql(a.start, end))
    be.close()
    base["bet_date"] = pd.to_datetime(base["bet_date"])
    for c in base.columns:
        if c not in ("user_id", "bet_date"):
            base[c] = pd.to_numeric(base[c], errors="coerce")
    print(f"基础表: {len(base):,} 用户-日, {base['user_id'].nunique():,} 用户")
    feat = add_hmm_features(base)
    # 一天一个 parquet,放同一文件夹(文件名=北京日)
    daydir = OUTDIR / "fm01_hmm_features"; daydir.mkdir(parents=True, exist_ok=True)
    n = 0
    for d, g in feat.groupby("bet_date"):
        g.to_parquet(daydir / f"{d.date()}.parquet", index=False, compression="snappy")
        n += 1
    print(f"落地: {daydir}/  ({n} 个每日 parquet,{a.start} ~ {last.date()})")
    print(f"\nHMM 特征列: {HMM_FEATURES}")
    print("\n各 HMM 特征分布(describe):")
    print(feat[HMM_FEATURES].describe().T[["count", "mean", "50%", "std", "min", "max"]].round(3).to_string())


if __name__ == "__main__":
    main()
