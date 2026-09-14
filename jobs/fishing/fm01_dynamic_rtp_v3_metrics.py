"""FM01 dynamic_rtp_v3 vs DEFAULT_FALLBACK —— 从 Redshift 重算核心指标（对齐研究侧报告口径）。

record 级按 strategy_name 切两组：DYNAMIC_RTP_V3(treatment) vs DEFAULT_FALLBACK(control)。
清洗：CNY、剔 B26/TST/TSB/TSO。北京日 = (event_timestamp+8h)::date。
输出：核心指标对比 + 首次击杀轮次 + D1/D3/D7/D10 留存 + 生涯前 200 注理论 rtp_th 曲线。
落地 data/output/fm01_v3/。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output" / "fm01_v3"
OUT.mkdir(parents=True, exist_ok=True)
LO, HI = "2026-08-06", "2026-09-02"          # 北京日窗口（近 4 周，右截断避开最新未完整日）
BASE = "currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"
STRAT = "strategy_name IN ('DYNAMIC_RTP_V3','DEFAULT_FALLBACK')"
WIN = f"(event_timestamp+interval '8 hours')::date BETWEEN '{LO}' AND '{HI}'"


def main():
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)

    # 1) 核心指标（先按 user 聚合，再对玩家求均值的口径用子查询）
    print(f"窗口(北京日) {LO} ~ {HI}\n=== 核心指标 ===")
    agg = be.execute(f"""
      WITH u AS (
        SELECT strategy_name sn, user_id,
               COUNT(*) shots, SUM(bet) bet, SUM(payout) payout, SUM(profit) profit,
               SUM(CASE WHEN killed THEN 1 ELSE 0 END) kills,
               AVG(bullet_level::float) lvl, AVG(fish_value::float) fish,
               AVG(rtp_th::float) rtpth, AVG(multiplier::float) mult
        FROM public.bullet WHERE {BASE} AND {STRAT} AND {WIN}
        GROUP BY 1,2)
      SELECT sn, COUNT(*) users, SUM(shots) shots, SUM(bet) bet, SUM(payout) payout,
             SUM(payout)/NULLIF(SUM(bet),0)*100 rtp,
             SUM(shots)::float/COUNT(*) avg_shots,
             SUM(bet)/COUNT(*) avg_cumbet,
             SUM(bet)/NULLIF(SUM(shots),0) avg_betsize,
             SUM(profit)/COUNT(*) avg_net,
             SUM(kills)::float/NULLIF(SUM(shots),0)*100 hit_rate,
             AVG(lvl) avg_level, AVG(fish) avg_fish, AVG(rtpth) avg_rtpth, AVG(mult) avg_mult
      FROM u GROUP BY 1""")
    cols = ["sn", "users", "shots", "bet", "payout", "rtp", "avg_shots", "avg_cumbet",
            "avg_betsize", "avg_net", "hit_rate", "avg_level", "avg_fish", "avg_rtpth", "avg_mult"]
    ag = pd.DataFrame(agg, columns=cols)
    ag.to_csv(OUT / "core_metrics.csv", index=False)
    for _, r in ag.iterrows():
        print(f"  {r['sn']:16} 玩家{int(r['users']):>7,} 发{int(r['shots']):>12,} RTP{r['rtp']:.2f} "
              f"人均发{r['avg_shots']:.0f} 人均累投{r['avg_cumbet']:.0f} 单笔{r['avg_betsize']:.2f} "
              f"人均净{r['avg_net']:.0f} 命中{r['hit_rate']:.2f} level{r['avg_level']:.2f} 鱼值{r['avg_fish']:.1f} rtp_th{r['avg_rtpth']:.3f}")

    # 2) 首次击杀轮次（per user 首个 killed 的序号，对玩家取均值）
    print("\n=== 首次击杀轮次 ===")
    fk = be.execute(f"""
      WITH r AS (
        SELECT strategy_name sn, user_id, killed,
               ROW_NUMBER() OVER (PARTITION BY strategy_name, user_id ORDER BY event_timestamp) rn
        FROM public.bullet WHERE {BASE} AND {STRAT} AND {WIN}),
      f AS (SELECT sn, user_id, MIN(CASE WHEN killed THEN rn END) fkill FROM r GROUP BY 1,2)
      SELECT sn, AVG(fkill::float) avg_first_kill, COUNT(fkill) n FROM f GROUP BY 1""")
    fkdf = pd.DataFrame(fk, columns=["sn", "avg_first_kill", "n_killed"])
    fkdf.to_csv(OUT / "first_kill.csv", index=False)
    for _, r in fkdf.iterrows():
        print(f"  {r['sn']:16} 平均首杀轮次 {r['avg_first_kill']:.2f}（{int(r['n_killed']):,} 人有击杀）")

    # 3) 留存 D1/D3/D7/D10（cohort=某北京日该策略活跃玩家；回访=任意 FM01 投注）
    print("\n=== 留存 ===")
    coh = pd.DataFrame(be.execute(f"""
      SELECT strategy_name sn, user_id, (event_timestamp+interval '8 hours')::date d
      FROM public.bullet WHERE {BASE} AND {STRAT} AND {WIN} GROUP BY 1,2,3"""), columns=["sn", "user_id", "d"])
    glob = pd.DataFrame(be.execute(f"""
      SELECT user_id, (event_timestamp+interval '8 hours')::date d
      FROM public.bullet WHERE {BASE}
        AND (event_timestamp+interval '8 hours')::date BETWEEN '{LO}' AND DATEADD(day,11,'{HI}')
      GROUP BY 1,2"""), columns=["user_id", "d"])
    be.close()
    coh["d"] = pd.to_datetime(coh["d"]); glob["d"] = pd.to_datetime(glob["d"])
    act = {u: set(x) for u, x in glob.groupby("user_id")["d"]}
    last = glob["d"].max()
    rows = []
    for sn, g in coh.groupby("sn"):
        rec = {"sn": sn}
        for N in [1, 3, 7, 10]:
            num = den = 0
            for d, gg in g.groupby("d"):
                tgt = d + pd.Timedelta(days=N)
                if tgt > last:
                    continue
                us = gg["user_id"].values; den += len(us)
                num += sum(1 for u in us if tgt in act.get(u, ()))
            rec[f"D{N}"] = num / den * 100 if den else float("nan")
        rows.append(rec)
    ret = pd.DataFrame(rows)
    ret.to_csv(OUT / "retention.csv", index=False)
    for _, r in ret.iterrows():
        print(f"  {r['sn']:16} D1 {r['D1']:.1f} / D3 {r['D3']:.1f} / D7 {r['D7']:.1f} / D10 {r['D10']:.1f}")
    print(f"\n落地 {OUT}/*.csv")


if __name__ == "__main__":
    main()
