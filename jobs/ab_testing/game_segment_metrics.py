"""游戏级「按策略变更点切时段」指标 —— 每段对应一个固定策略配置,看每次变更的影响。

每(游戏×时段)：投注用户、总投注额(万)、总GGR(万)、RTP%、人均投注额/次数/时长(分)中/均/均99、D1/D3/D7。
时段边界取自「AI 活动时间线」的策略变更日(北京日,hi 含端点)。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output" / "multi_game"; OUT.mkdir(parents=True, exist_ok=True)
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean() if len(s) else float("nan")
SLOT = "currency_type='CNY' AND status='COMPLETED' AND op_code NOT IN ('B26','TST','TSB','TSO')"
FM01F = "currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"

# 每游戏：(名, db, port, table, timecol, bet, pay, filter, [ (段名, lo, hi) ... ])
SEG = {
    "FM01": ["transform-agfish-game", 5442, "public.bullet", "event_timestamp", "bet", "payout", FM01F, [
        ("动态RTP v2.x（~06-08）", "2026-03-01", "2026-06-08"),
        ("v3上线·RTP改96.5%（06-09~06-30）", "2026-06-09", "2026-06-30"),
        ("+风控v1.1/v1.2（07-01~07-30）", "2026-07-01", "2026-07-30"),
        ("+个性化挽留（07-31~）", "2026-07-31", "2026-09-05")]],
    "SS03": ["slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", "game_id='SS03' AND " + SLOT, [
        ("上线·活动表（02-11~03-30）", "2026-02-11", "2026-03-30"),
        ("AI+暗保底上线（03-31~06-08）", "2026-03-31", "2026-06-08"),
        ("暗保底全体·A95Kai/B97BG（06-09~07-04）", "2026-06-09", "2026-07-04"),
        ("Def95Kai/Azero/B93Kai（07-05~07-28）", "2026-07-05", "2026-07-28"),
        ("kakuteiC·Ashi/B95bgtr（07-29~08-16）", "2026-07-29", "2026-08-16"),
        ("A97bgtr·尾号分组（08-17~）", "2026-08-17", "2026-09-05")]],
    "SS01": ["slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", "game_id='SS01' AND " + SLOT, [
        ("AI调控·kmeans（~05-10）", "2026-03-01", "2026-05-10"),
        ("AI cluster0→risky2（05-11~07-29）", "2026-05-11", "2026-07-29"),
        ("+个性化挽留（07-30~）", "2026-07-30", "2026-09-05")]],
    "SS02": ["slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", "game_id='SS02' AND " + SLOT, [
        ("AI上线（03-31~05-04）", "2026-03-31", "2026-05-04"),
        ("Default=Fast1（05-05~05-25）", "2026-05-05", "2026-05-25"),
        ("Default=Medium3（05-26~）", "2026-05-26", "2026-09-05")]],
    "SS06": ["slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", "game_id='SS06' AND " + SLOT, [
        ("暗保底前（06-01~07-06）", "2026-06-01", "2026-07-06"),
        ("暗保底上线（07-07~07-15）", "2026-07-07", "2026-07-15"),
        ("SFS换表+AB组（07-16~08-16）", "2026-07-16", "2026-08-16"),
        ("SFS 5000倍上限（08-17~）", "2026-08-17", "2026-09-05")]],
}


def run(name):
    db, port, table, tc, bc, pc, filt, segs = SEG[name]
    start = segs[0][1]
    be = rs.RedshiftBackend(database=db, bastion_ip=rs.DEFAULT_BASTION_IP, local_port=port)
    bj = f"({tc}+interval '8 hours')::date"
    ud = pd.DataFrame(be.execute(f"""
      SELECT user_id, {bj} d, COUNT(*) n, SUM({bc}) bet, SUM({pc}) payout,
             DATEDIFF(second, MIN({tc}), MAX({tc})) span
      FROM {table} WHERE {filt} AND {bj} >= '{start}' GROUP BY 1,2"""),
      columns=["user_id", "d", "n", "bet", "payout", "span"])
    be.close()
    ud["d"] = pd.to_datetime(ud["d"])
    for c in ["n", "bet", "payout", "span"]:
        ud[c] = ud[c].astype(float)
    act = {u: set(x) for u, x in ud.groupby("user_id")["d"]}
    last = ud["d"].max()

    def ret(sub, N):
        num = den = 0
        for d, g in sub.groupby("d"):
            tgt = d + pd.Timedelta(days=N)
            if tgt > last: continue
            us = g["user_id"].unique(); den += len(us)
            num += sum(1 for u in us if tgt in act.get(u, ()))
        return num / den * 100 if den else float("nan")

    tri = lambda s: f"{s.median():.0f}/{s.mean():.0f}/{w99(s):.0f}"
    frag = [f"\n### {name}\n", "| 时段（策略配置） | 投注用户 | 总投注额(万) | 总GGR(万) | RTP% | 人均投注额 中/均/均99 | 人均投注次数 中/均/均99 | 人均时长(分) 中/均/均99 | D1/D3/D7 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for lbl, lo, hi in segs:
        sub = ud[(ud["d"] >= lo) & (ud["d"] <= hi)]
        if not len(sub):
            continue
        usr = sub.groupby("user_id").agg(bet=("bet", "sum"), n=("n", "sum"), dur=("span", "sum"))
        usr["dur"] /= 60.0
        sb, sp = sub["bet"].sum(), sub["payout"].sum()
        frag.append(f"| {lbl} | {sub['user_id'].nunique():,} | {sb/1e4:,.1f} | {(sb-sp)/1e4:,.1f} | {sp/sb*100:.2f} | "
                    f"{tri(usr['bet'])} | {tri(usr['n'])} | {tri(usr['dur'])} | {ret(sub,1):.1f}/{ret(sub,3):.1f}/{ret(sub,7):.1f} |")
        print(f"  [{name}] {lbl}")
    (OUT / f"{name}_seg.md").write_text("\n".join(frag), encoding="utf-8")
    print(f"  {name} 落地")


if __name__ == "__main__":
    for g in (sys.argv[1:] or list(SEG)):
        print(f"=== {g} ==="); run(g)
