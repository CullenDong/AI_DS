"""SS03 两期数学表 AB 实验时间轴对比 + 各表特性画像（可复现）。

两期（default 两期都是 95kai，作统一锚点；只比"相对 default 提升"）：
  · P1 2026-08-18~2026-09-14：MOD(user_id,10) 分组
        A=尾号4,5 → BGTR97_Saitekika_BGadj_v2 ；B=尾号6,7 → BGTR95_Saitekika_BGadj_v3 ；default=尾号0-3 → 95kai
  · P2 2026-09-15~2026-10-02：MOD(user_id,100) 分组（high_player_test_v1）
        A=尾号00-29 → BGTR97_..._v3 ；B=尾号30-59 → BGTR95_HMM_Highvalue_v1 ；default=尾号60-99 → 95kai

口径：北京日 (created_at+8h)::date、CNY、COMPLETED、刨 op_code 测试单；RTP/特性刨 kakutei 暗保底。
产出：data/output/ss03_2period_ud_nokak.parquet（user-day），控制台打印两期全指标 + 相对提升 + 各表特性。
用法：python3 jobs/ss03_analysis/ss03_two_period_compare.py [--pull]   （--pull 重新从 Redshift 拉数）
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
UD = ROOT / "data" / "output" / "ss03_2period_ud_nokak.parquet"

P1S, P1E = pd.Timestamp("2026-08-18"), pd.Timestamp("2026-09-14")
P2S, P2E = pd.Timestamp("2026-09-15"), pd.Timestamp("2026-10-02")
TAIL = {"P1": {"A": "4,5", "B": "6,7", "default": "0-3"},
        "P2": {"A": "00-29", "B": "30-59", "default": "60-99"}}
TBL = {"P1": {"A": "BGTR97_BGadj_v2", "B": "BGTR95_BGadj_v3", "default": "95kai"},
       "P2": {"A": "BGTR97_BGadj_v3", "B": "BGTR95_HMM_v1", "default": "95kai"}}
CHAR_TABLES = ["normal_zero_95_kai",
               "normal_Zero_BGTR97_Saitekika_BGadj_v2", "normal_Zero_BGTR97_Saitekika_BGadj_v3",
               "normal_Zero_BGTR95_Saitekika_BGadj_v3", "normal_Zero_BGTR95_HMM_Highvalue_v1"]


def pull_ud():
    from tools.db import redshift as rs
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    sql = f"""
    SELECT user_id, (created_at + interval '8 hours')::date AS bj,
      MOD(user_id,10) AS m10, MOD(user_id,100) AS m100,
      SUM(bet_amount) AS bet, SUM(actual_payout) AS pay, COUNT(*) AS nbet
    FROM public.fct_bet_orders
    WHERE game_id='SS03' AND currency_type='CNY' AND status='COMPLETED'
      AND op_code NOT IN ('B26','TST','TSB','TSO')
      AND LOWER(math_table_id) NOT LIKE '%kakutei%'
      AND (created_at+interval '8 hours')::date BETWEEN '{P1S.date()}' AND '{P2E.date()}'
    GROUP BY 1,2,3,4"""
    df = pd.DataFrame(be.execute(sql), columns=["user_id", "bj", "m10", "m100", "bet", "pay", "nbet"])
    be.close()
    for c in ["bet", "pay", "nbet", "m10", "m100"]:
        df[c] = pd.to_numeric(df[c])
    df["bj"] = pd.to_datetime(df["bj"])
    UD.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(UD, index=False)
    print(f"拉取 {len(df):,} user-day, {df.user_id.nunique():,} 玩家 → {UD}")
    return df


def table_characteristics():
    """各表 spin 级特性画像（命中/均倍/波动/中档大奖/FG）。"""
    from tools.db import redshift as rs
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    base = "bet_type='BASE' AND bet_amount>0"
    nb = f"COUNT(CASE WHEN {base} THEN 1 END)"
    tset = ",".join(f"'{t}'" for t in CHAR_TABLES)
    sql = f"""
    SELECT math_table_id AS tbl,
      SUM(actual_payout)/NULLIF(SUM(bet_amount),0)*100 AS rtp,
      SUM(CASE WHEN {base} AND actual_payout>0 THEN 1 ELSE 0 END)*100.0/NULLIF({nb},0) AS hit,
      AVG(CASE WHEN {base} THEN actual_payout*1.0/bet_amount END) AS avg_mult,
      STDDEV(CASE WHEN {base} THEN actual_payout*1.0/bet_amount END) AS sd_mult,
      SUM(CASE WHEN {base} AND actual_payout>=10*bet_amount THEN 1 ELSE 0 END)*100.0/NULLIF({nb},0) AS big10,
      SUM(CASE WHEN {base} AND actual_payout>=50*bet_amount THEN 1 ELSE 0 END)*100.0/NULLIF({nb},0) AS big50,
      SUM(CASE WHEN bet_type!='BASE' THEN actual_payout ELSE 0 END)/NULLIF(SUM(actual_payout),0)*100 AS fg_payshare,
      COUNT(CASE WHEN bet_type!='BASE' THEN 1 END)*1000.0/NULLIF(COUNT(CASE WHEN bet_type='BASE' THEN 1 END),0) AS fg_per_1k
    FROM public.fct_bet_orders
    WHERE game_id='SS03' AND currency_type='CNY' AND status='COMPLETED'
      AND op_code NOT IN ('B26','TST','TSB','TSO')
      AND (created_at+interval '8 hours')::date BETWEEN '{P1S.date()}' AND '{P2E.date()}'
      AND math_table_id IN ({tset})
    GROUP BY math_table_id ORDER BY rtp DESC"""
    rows = be.execute(sql)
    be.close()
    print("\n===== 各表特性画像（BASE spin 级）=====")
    print(f"{'表':<28}{'RTP':>6}{'命中%':>7}{'均倍':>6}{'波动sd':>7}{'≥10x%':>7}{'≥50x%':>7}{'FG赔付%':>8}{'FG/千发':>8}")
    f = lambda x: float(x) if x is not None else 0.0
    for r in rows:
        t = r[0].replace("normal_Zero_", "").replace("normal_zero_", "").replace("_Saitekika", "")
        print(f"{t:<28}{f(r[1]):>6.1f}{f(r[2]):>7.1f}{f(r[3]):>6.2f}{f(r[4]):>7.2f}"
              f"{f(r[5]):>7.3f}{f(r[6]):>7.3f}{f(r[7]):>8.1f}{f(r[8]):>8.1f}")


def g1(m):
    return "default" if m in (0, 1, 2, 3) else "A" if m in (4, 5) else "B" if m in (6, 7) else None


def g2(m):
    return "A" if m < 30 else "B" if m < 60 else "default" if m < 100 else None


def compare(df):
    act = set(map(tuple, df[["user_id", "bj"]].values))
    last = df.bj.max()

    def ret(sub, n):
        num = den = 0
        for u, dd in sub[["user_id", "bj"]].itertuples(index=False):
            t = dd + pd.Timedelta(days=n)
            if t > last:
                continue
            den += 1
            num += (u, t) in act
        return num / den * 100 if den else np.nan

    res = {}
    for pk, (s, e, mc) in {"P1": (P1S, P1E, "m10"), "P2": (P2S, P2E, "m100")}.items():
        p = df[(df.bj >= s) & (df.bj <= e)].copy()
        p["grp"] = p[mc].map(g1 if mc == "m10" else g2)
        res[pk] = {}
        print(f"\n===== {pk} {s.date()}~{e.date()} ({mc}) =====")
        print(f"{'组':<8}{'尾号':<7}{'表':<18}{'玩家':>7}{'人均活天':>8}{'人均投注':>9}{'投注中位':>9}"
              f"{'人均GGR':>8}{'D1':>6}{'D3':>6}{'D5':>6}{'D7':>6}{'D10':>6}")
        for g in ["A", "B", "default"]:
            sb = p[p.grp == g]
            if not len(sb):
                continue
            u = sb.user_id.nunique()
            ud = len(sb)
            tb, tp = sb.bet.sum(), sb.pay.sum()
            row = dict(permean=tb / u, permed=sb.groupby("user_id").bet.sum().median(),
                       ggr=(tb - tp) / u, actdays=ud / u,
                       D1=ret(sb, 1), D3=ret(sb, 3), D5=ret(sb, 5), D7=ret(sb, 7), D10=ret(sb, 10))
            res[pk][g] = row
            print(f"{g:<8}{TAIL[pk][g]:<7}{TBL[pk][g]:<18}{u:>7,}{row['actdays']:>8.2f}"
                  f"{row['permean']:>9.0f}{row['permed']:>9.0f}{row['ggr']:>8.0f}"
                  f"{row['D1']:>6.1f}{row['D3']:>6.1f}{row['D5']:>6.1f}{row['D7']:>6.1f}{row['D10']:>6.1f}")

    print("\n===== 相对 default 提升（留存 Δpp · 投注/GGR 相对%）=====")
    print(f"{'期·组':<10}{'表':<18}{'D1':>6}{'D3':>6}{'D5':>6}{'D7':>6}{'D10':>6}{'投注Δ%':>8}{'GGRΔ%':>8}")
    for pk in ["P1", "P2"]:
        d = res[pk]["default"]
        for g in ["A", "B"]:
            r = res[pk][g]
            print(f"{pk + ' ' + g:<10}{TBL[pk][g]:<18}{r['D1'] - d['D1']:>6.1f}{r['D3'] - d['D3']:>6.1f}"
                  f"{r['D5'] - d['D5']:>6.1f}{r['D7'] - d['D7']:>6.1f}{r['D10'] - d['D10']:>6.1f}"
                  f"{(r['permean'] / d['permean'] - 1) * 100:>8.1f}{(r['ggr'] / d['ggr'] - 1) * 100:>8.1f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pull", action="store_true", help="从 Redshift 重新拉数（否则读本地 parquet）")
    ap.add_argument("--chars", action="store_true", help="同时拉各表 spin 级特性画像")
    a = ap.parse_args()
    df = pull_ud() if (a.pull or not UD.exists()) else pd.read_parquet(UD)
    compare(df)
    if a.chars:
        table_characteristics()


if __name__ == "__main__":
    main()
