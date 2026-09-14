"""FM01 dynamic_rtp_v3 vs default —— 按 HMM 状态 & 日龄分层（均值类带 中/均/均99）。

窗口：清 8 月 北京日 2026-08-01 ~ 08-31（HMM 标签覆盖到 09-01，8 月完整）。
层：① HMM 状态（T1/S1/S2/S3，按 user-day）；② 日龄（new 0-3 / beginner 4-7 / old ≥8）。
每（策略 × 层）：用户、RTP%、命中%，以及 人均发/人均累投/人均净/鱼值/rtp_th 的「中/均/均99」，+ D1/D3/D7。
生成 md 片段。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output" / "fm01_v3"; OUT.mkdir(parents=True, exist_ok=True)
LO, HI = "2026-08-01", "2026-08-31"
BASE = "currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"
STRAT = "strategy_name IN ('DYNAMIC_RTP_V3','DEFAULT_FALLBACK')"
SN = {"DYNAMIC_RTP_V3": "v3", "DEFAULT_FALLBACK": "default"}
BJ = "(event_timestamp+interval '8 hours')::date"
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean() if len(s) else float("nan")
STATES = [("T1 首日", "T1_first_day"), ("S1 低参与", "S1_Low"), ("S2 核心活跃", "S2_Engaged"), ("S3 间歇回流", "S3_Lapsed")]
TENURE = [("new (0-3天)", 0, 3), ("beginner (4-7天)", 4, 7), ("old (≥8天)", 8, 10**9)]
AVG = [("shots", "人均发", 0), ("bet", "人均累投", 0), ("profit", "人均净", 0), ("fish", "鱼值", 1), ("rtpth", "rtp_th", 3)]


def main():
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)
    ud = pd.DataFrame(be.execute(f"""
      SELECT strategy_name sn, user_id, {BJ} d, COUNT(*) shots, SUM(bet) bet, SUM(payout) payout,
             SUM(profit) profit, SUM(CASE WHEN killed THEN 1 ELSE 0 END) kills,
             SUM(rtp_th::float) rtpth, SUM(fish_value::float) fish
      FROM public.bullet WHERE {BASE} AND {STRAT} AND {BJ} BETWEEN '{LO}' AND '{HI}' GROUP BY 1,2,3"""),
      columns=["sn", "user_id", "d", "shots", "bet", "payout", "profit", "kills", "rtpth", "fish"])
    ud["sn"] = ud["sn"].map(SN); ud["d"] = pd.to_datetime(ud["d"])
    for c in ["shots", "bet", "payout", "profit", "kills", "rtpth", "fish"]:
        ud[c] = ud[c].astype(float)
    fb = {u: pd.Timestamp(x) for u, x in be.execute(
        f"SELECT user_id, MIN({BJ}) FROM public.bullet WHERE {BASE} AND bet>0 GROUP BY 1")}
    glob = pd.DataFrame(be.execute(f"SELECT user_id, {BJ} d FROM public.bullet WHERE {BASE} AND {BJ} BETWEEN '{LO}' AND DATEADD(day,11,'{HI}') GROUP BY 1,2"),
                        columns=["user_id", "d"]); glob["d"] = pd.to_datetime(glob["d"])
    be.close()
    act = {u: set(x) for u, x in glob.groupby("user_id")["d"]}; last = glob["d"].max()
    lab = pd.read_parquet(ROOT / "data" / "output" / "hmm" / "fm01_hmm_labels.parquet", columns=["user_id", "bet_date", "state_name"])
    lab["bet_date"] = pd.to_datetime(lab["bet_date"])
    ud = ud.merge(lab, left_on=["user_id", "d"], right_on=["user_id", "bet_date"], how="left")
    ud["tenure"] = [(dd - fb.get(u, dd)).days for u, dd in zip(ud["user_id"], ud["d"])]

    def ret(sub, N):
        num = den = 0
        for d, g in sub.groupby("d"):
            tgt = d + pd.Timedelta(days=N)
            if tgt > last: continue
            us = g["user_id"].unique(); den += len(us)
            num += sum(1 for u in us if tgt in act.get(u, ()))
        return num / den * 100 if den else float("nan")

    def peruser(sub):
        g = sub.groupby("user_id").agg(shots=("shots", "sum"), bet=("bet", "sum"), profit=("profit", "sum"),
                                       rtpth_s=("rtpth", "sum"), fish_s=("fish", "sum")).reset_index()
        g["fish"] = g["fish_s"] / g["shots"]; g["rtpth"] = g["rtpth_s"] / g["shots"]
        return g

    def tri(s, dec): s = s.dropna(); return f"{s.median():.{dec}f}/{s.mean():.{dec}f}/{w99(s):.{dec}f}"

    def emit(title, layers):
        out = [f"\n**{title}**\n", "| 层 | 策略 | 用户 | 总投注量(万) | 总投注额(万) | 总GGR(万) | RTP% | 命中% | 人均发 中/均/均99 | 人均累投 中/均/均99 | 人均净 中/均/均99 | 鱼值 中/均/均99 | rtp_th 中/均/均99 | D1/D3/D7 |",
               "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for lname, mask in layers:
            for sn in ["v3", "default"]:
                sub = ud[mask & (ud["sn"] == sn)]
                if not len(sub): continue
                pu = peruser(sub); rtp = sub["payout"].sum()/sub["bet"].sum()*100; hit = sub["kills"].sum()/sub["shots"].sum()*100
                cells = " | ".join(tri(pu[k], dec) for k, _, dec in AVG)
                out.append(f"| {lname} | {sn} | {pu['user_id'].nunique():,} | {sub['shots'].sum()/1e4:,.1f} | "
                           f"{sub['bet'].sum()/1e4:,.1f} | {(sub['bet'].sum()-sub['payout'].sum())/1e4:,.1f} | "
                           f"{rtp:.2f} | {hit:.2f} | {cells} | "
                           f"{ret(sub,1):.1f}/{ret(sub,3):.1f}/{ret(sub,7):.1f} |")
            print(f"  [{title[:8]}] {lname} 出完")
        return "\n".join(out)

    # 投注额分层（玩家 8 月总投注额；阈值 小<1000 / 中1000-20000 / 大≥20000）
    usr_tot = ud.groupby("user_id")["bet"].sum()
    segc = pd.cut(usr_tot, [-1, 1000, 20000, 1e18], labels=["small", "mid", "large"])
    segu = {s: set(usr_tot.index[segc == s]) for s in ["small", "mid", "large"]}
    BSL = [("小投注额（<1000）", "small"), ("中投注额（1000–20000）", "mid"), ("大投注额（≥20000）", "large")]
    frag = [emit("按 HMM 生命周期状态分层（清 8 月）", [(n, ud["state_name"] == s) for n, s in STATES]),
            emit("按日龄分层（自首发子弹起算，清 8 月）", [(n, (ud["tenure"] >= lo) & (ud["tenure"] <= hi)) for n, lo, hi in TENURE]),
            emit("按投注额分层（玩家 8 月总投注额，清 8 月）", [(n, ud["user_id"].isin(segu[k])) for n, k in BSL])]
    (OUT / "layered_fragments.md").write_text("\n".join(frag), encoding="utf-8")
    print("\n落地", OUT / "layered_fragments.md")
    print("\n".join(frag))


if __name__ == "__main__":
    main()
