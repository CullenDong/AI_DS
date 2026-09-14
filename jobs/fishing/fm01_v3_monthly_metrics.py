"""FM01 dynamic_rtp_v3 vs DEFAULT_FALLBACK —— 按月分段总结对比（均值类指标带 中/均/均99 三视图）。

一次拉 (策略,user,北京日) → 按(策略,user,月)聚成每用户月度值 → 每月出：
  计数(用户/总发)、率(RTP%/命中%)、以及均值类(人均发/累投/净/单笔/鱼值/level/rtp_th/首杀轮)
  的「中/均/均99」（中位 / 原始均值 / 0-99%缩尾均值）。
首杀轮次单独按(策略×月×user)算后并入。输出每月对比表 + 跨月趋势(md 片段)。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output" / "fm01_v3"; OUT.mkdir(parents=True, exist_ok=True)
LO = "2026-06-10"
BASE = "currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"
STRAT = "strategy_name IN ('DYNAMIC_RTP_V3','DEFAULT_FALLBACK')"
SN = {"DYNAMIC_RTP_V3": "v3", "DEFAULT_FALLBACK": "default"}
BJ = "(event_timestamp+interval '8 hours')::date"
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean() if len(s) else float("nan")
MON_LABEL = {"2026-06": "6 月（06-10~30）", "2026-07": "7 月", "2026-08": "8 月", "2026-09": "9 月（01-04,部分）"}
# 均值类指标：(键, 显示名, 小数位)
AVG_METRICS = [("shots", "人均发", 0), ("bet", "人均累投", 0), ("profit", "人均净", 0),
               ("betsize", "单笔投注", 2), ("fish", "平均鱼值", 1), ("lvl", "平均level", 2),
               ("rtpth", "平均rtp_th", 3), ("fk", "首杀轮次", 1)]


def main():
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)
    last = pd.Timestamp(be.execute(f"SELECT MAX({BJ}) FROM public.bullet WHERE {BASE}")[0][0])
    ud = pd.DataFrame(be.execute(f"""
      SELECT strategy_name sn, user_id, {BJ} d, COUNT(*) shots, SUM(bet) bet, SUM(payout) payout,
             SUM(profit) profit, SUM(CASE WHEN killed THEN 1 ELSE 0 END) kills,
             SUM(rtp_th::float) rtpth, SUM(fish_value::float) fish, SUM(bullet_level::float) lvl
      FROM public.bullet WHERE {BASE} AND {STRAT} AND {BJ} >= '{LO}' GROUP BY 1,2,3"""),
      columns=["sn", "user_id", "d", "shots", "bet", "payout", "profit", "kills", "rtpth", "fish", "lvl"])
    ud["sn"] = ud["sn"].map(SN); ud["d"] = pd.to_datetime(ud["d"])
    for c in ["shots", "bet", "payout", "profit", "kills", "rtpth", "fish", "lvl"]:
        ud[c] = ud[c].astype(float)
    ud["mon"] = ud["d"].dt.strftime("%Y-%m")
    glob = pd.DataFrame(be.execute(f"SELECT user_id, {BJ} d FROM public.bullet WHERE {BASE} AND {BJ} >= '{LO}' GROUP BY 1,2"),
                        columns=["user_id", "d"]); glob["d"] = pd.to_datetime(glob["d"])
    act = {u: set(x) for u, x in glob.groupby("user_id")["d"]}
    # 首杀轮次 per (策略×月×user)
    fk = pd.DataFrame(be.execute(f"""
      WITH r AS (SELECT strategy_name sn, TO_CHAR({BJ},'YYYY-MM') mon, user_id, killed,
                   ROW_NUMBER() OVER (PARTITION BY strategy_name, TO_CHAR({BJ},'YYYY-MM'), user_id ORDER BY event_timestamp) rn
                 FROM public.bullet WHERE {BASE} AND {STRAT} AND {BJ} >= '{LO}')
      SELECT sn, mon, user_id, MIN(CASE WHEN killed THEN rn END) fk FROM r GROUP BY 1,2,3"""),
      columns=["sn", "mon", "user_id", "fk"]); fk["sn"] = fk["sn"].map(SN)
    be.close()

    # 每用户月度值
    um = ud.groupby(["sn", "mon", "user_id"]).agg(
        shots=("shots", "sum"), bet=("bet", "sum"), payout=("payout", "sum"),
        profit=("profit", "sum"), kills=("kills", "sum"),
        rtpth_s=("rtpth", "sum"), fish_s=("fish", "sum"), lvl_s=("lvl", "sum")).reset_index()
    um["betsize"] = um["bet"] / um["shots"]; um["fish"] = um["fish_s"] / um["shots"]
    um["lvl"] = um["lvl_s"] / um["shots"]; um["rtpth"] = um["rtpth_s"] / um["shots"]
    um = um.merge(fk, on=["sn", "mon", "user_id"], how="left")

    def ret(sub, N):
        num = den = 0
        for d, g in sub.groupby("d"):
            tgt = d + pd.Timedelta(days=N)
            if tgt > last: continue
            us = g["user_id"].unique(); den += len(us)
            num += sum(1 for u in us if tgt in act.get(u, ()))
        return num / den * 100 if den else float("nan")

    def tri(s, dec):
        s = s.dropna()
        return f"{s.median():.{dec}f}/{s.mean():.{dec}f}/{w99(s):.{dec}f}"

    months = sorted(um["mon"].unique())
    frag, trend = [], []
    for mon in months:
        v = um[(um["sn"] == "v3") & (um["mon"] == mon)]; d = um[(um["sn"] == "default") & (um["mon"] == mon)]
        dv = ud[(ud["sn"] == "v3") & (ud["mon"] == mon)]; dd = ud[(ud["sn"] == "default") & (ud["mon"] == mon)]
        rtp_v = dv["payout"].sum()/dv["bet"].sum()*100; rtp_d = dd["payout"].sum()/dd["bet"].sum()*100
        hit_v = dv["kills"].sum()/dv["shots"].sum()*100; hit_d = dd["kills"].sum()/dd["shots"].sum()*100
        frag.append(f"\n**{MON_LABEL.get(mon, mon)}** — v3 RTP {rtp_v:.2f}% vs {rtp_d:.2f}%\n")
        frag.append("| 指标 | v3（中/均/均99） | default（中/均/均99） | v3−对照(中位) |\n|---|---|---|---:|")
        ggr_v = (dv['bet'].sum() - dv['payout'].sum()) / 1e4; ggr_d = (dd['bet'].sum() - dd['payout'].sum()) / 1e4
        frag.append(f"| 用户 | {v['user_id'].nunique():,} | {d['user_id'].nunique():,} | — |")
        frag.append(f"| 总投注量(万发) | {dv['shots'].sum()/1e4:,.1f} | {dd['shots'].sum()/1e4:,.1f} | — |")
        frag.append(f"| 总投注额(万) | {dv['bet'].sum()/1e4:,.1f} | {dd['bet'].sum()/1e4:,.1f} | — |")
        frag.append(f"| 总GGR(万) | {ggr_v:,.1f} | {ggr_d:,.1f} | — |")
        frag.append(f"| RTP% | {rtp_v:.2f} | {rtp_d:.2f} | {rtp_v-rtp_d:+.2f}pp |")
        frag.append(f"| 命中% | {hit_v:.2f} | {hit_d:.2f} | {hit_v-hit_d:+.2f}pp |")
        for key, name, dec in AVG_METRICS:
            dm = v[key].median() - d[key].median()
            frag.append(f"| {name} | {tri(v[key], dec)} | {tri(d[key], dec)} | {dm:+.{dec}f} |")
        for lab, Ns in [("D1/D3/D7/D10", [1, 3, 7, 10])]:
            rv = "/".join(f"{ret(dv, N):.1f}" for N in Ns); rd = "/".join(f"{ret(dd, N):.1f}" for N in Ns)
            frag.append(f"| {lab} | {rv} | {rd} | — |")
        trend.append((mon, v["user_id"].nunique(), rtp_v, rtp_d, v["shots"].median(),
                      v["fk"].median(), ret(dv, 1), ret(dd, 1)))
        print(f"  {mon}: v3 RTP {rtp_v:.2f}/def {rtp_d:.2f} | 人均发中 {v['shots'].median():.0f} | 首杀中 {v['fk'].median():.0f} | D1 {ret(dv,1):.1f}/{ret(dd,1):.1f}")

    frag.append("\n**跨月趋势（v3）**\n\n| 月 | v3 用户 | v3 RTP% | 对照 RTP% | v3 人均发(中) | v3 首杀轮(中) | v3 D1 | 对照 D1 | v3−对照 D1 |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for mon, uu, rv, rd, sm, fkm, d1v, d1d in trend:
        frag.append(f"| {MON_LABEL.get(mon, mon)} | {uu:,} | {rv:.2f} | {rd:.2f} | {sm:,.0f} | {fkm:.0f} | {d1v:.1f} | {d1d:.1f} | {d1v-d1d:+.1f}pp |")
    (OUT / "monthly_fragments.md").write_text("\n".join(frag), encoding="utf-8")
    print("\n落地", OUT / "monthly_fragments.md")


if __name__ == "__main__":
    main()
