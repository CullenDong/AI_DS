"""SS03 HMM 打标 · 第2步：汇总分天特征 → 算 tenure(no_bet_streak/k) → io_hmm_infer 打标 →
交叉 AB 组(MOD100)看每(组×risk)/(组×stage)的玩家数/人均投注/人均GGR/留存。

依赖：第1步产出 data/output/hmm/ss03_ud_daily/dt=*/part.parquet；实验期 ss03_hpt_report_ud.parquet。
⚠ 若第1步只跑了实验周期，tenure(k) 从该窗口首个 bet-day 起算（早于窗口的历史未计入 → k 偏小）。
用法：python3 jobs/ss03_analysis/ss03_hmm_assemble.py
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis" / "hmm" / "inference" / "scripts"))
import io_hmm_infer as inf  # noqa: E402
DAILY = ROOT / "data" / "output" / "hmm" / "ss03_ud_daily"
MODEL = ROOT / "jobs" / "ss03_analysis" / "hmm" / "inference" / "assets" / "ss03_iohmm_model.json"
OUT = ROOT / "data" / "output" / "hmm"


def main():
    day = pd.read_parquet(DAILY)
    day["bet_date"] = pd.to_datetime(day["bet_date"])
    day = day.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    day["k"] = day.groupby("user_id").cumcount() + 1
    prev = day.groupby("user_id")["bet_date"].shift()
    day["no_bet_streak_days"] = (day["bet_date"] - prev).dt.days - 1
    print(f"汇总 {len(day):,} 玩家-天, {day['user_id'].nunique():,} 玩家；首发日范围 {day['bet_date'].min().date()}~{day['bet_date'].max().date()}")
    model = inf.load_model(str(MODEL))
    feats = model["features"]
    need = day[["user_id", "bet_date", "bet_amount_today"] + feats].copy()
    bad = (day["k"] >= 2) & need[feats].isna().any(axis=1)
    if bad.any():
        print(f"剔除 k≥2 特征含 NaN 行 {int(bad.sum()):,}")
    scored = inf.score(need.loc[~bad], model)
    scored.to_parquet(OUT / "ss03_hpt_hmm_labels.parquet", index=False)
    cur = scored.sort_values("bet_date").groupby("user_id").tail(1)[["user_id", "stage", "risk"]]
    cur.to_parquet(OUT / "ss03_hpt_hmm_current.parquet", index=False)
    print("\n=== 每用户当前 risk 分布 ===")
    print(cur["risk"].value_counts().to_string())

    # 交叉 AB 组 × HMM：用实验期 user-day
    ud = pd.read_parquet(OUT.parent / "ss03_hpt_report_ud.parquet"); ud["d"] = pd.to_datetime(ud["d"])
    act = set(map(tuple, ud[["user_id", "d"]].values)); last = ud["d"].max()
    def ret(sub, N):
        num = den = 0
        for u, dd in sub[["user_id", "d"]].itertuples(index=False):
            t = dd + pd.Timedelta(days=N)
            if t > last: continue
            den += 1; num += (u, t) in act
        return num / den * 100 if den else np.nan
    GL = {"A_BGTR97v3": "A", "B_BGTR95HMM": "B", "default_95Kai": "default"}
    sub = ud.merge(cur, on="user_id", how="left")
    RISK = ["high", "med", "low", "first_day"]
    for dim, order in [("risk", RISK), ("stage", None)]:
        print(f"\n=== AB组 × HMM {dim}（实验期留存）===")
        print(f"{'组':<9}{dim:<10}{'玩家':>7}{'人均投注中':>10}{'人均GGR均':>10}{'D1':>7}{'D3':>7}{'D7':>7}")
        for g in ["A_BGTR97v3", "B_BGTR95HMM", "default_95Kai"]:
            gg = sub[sub.grp == g]
            levels = order if order else sorted(gg[dim].dropna().unique())
            for lv in levels:
                s = gg[gg[dim] == lv]
                if not len(s): continue
                usr = s.groupby("user_id").agg(bet=("bet", "sum"), pay=("pay", "sum"))
                nu = usr.shape[0]
                print(f"{GL[g]:<9}{str(lv):<10}{nu:>7,}{usr.bet.median():>10.0f}"
                      f"{(usr.bet.sum()-usr.pay.sum())/nu:>10.0f}{ret(s,1):>7.1f}{ret(s,3):>7.1f}{ret(s,7):>7.1f}")


if __name__ == "__main__":
    main()
