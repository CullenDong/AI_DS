"""游戏级时间轴指标 —— 每游戏逐月「整体」指标(不分组),供策略时间轴总结用。

每(游戏×月)：投注用户、总投注额(万)、总payout(万)、总GGR(万)、RTP%(含FG)、
人均投注额/投注次数/游玩时长(分)的 中/均/均99、D1/D3/D7 留存。北京日分月。
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

# (名, db, port, table, timecol, betcol, paycol, filter, 起始北京日)
GAMES = [
    ("FM01", "transform-agfish-game", 5442, "public.bullet", "event_timestamp", "bet", "payout", FM01F, "2026-03-01"),
    ("SS03", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", f"game_id='SS03' AND {SLOT}", "2026-02-01"),
    ("SS02", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", f"game_id='SS02' AND {SLOT}", "2026-03-01"),
    ("SS06", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout", f"game_id='SS06' AND {SLOT}", "2026-06-01"),
]


def run(name, db, port, table, tc, bc, pc, filt, start):
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
    ud["mon"] = ud["d"].dt.strftime("%Y-%m")
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
    frag = [f"\n### {name}\n", "| 月 | 投注用户 | 总投注额(万) | 总GGR(万) | RTP% | 人均投注额 中/均/均99 | 人均投注次数 中/均/均99 | 人均时长(分) 中/均/均99 | D1/D3/D7 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for mon in sorted(ud["mon"].unique()):
        sub = ud[ud["mon"] == mon]
        usr = sub.groupby("user_id").agg(bet=("bet", "sum"), n=("n", "sum"), dur=("span", "sum"))
        usr["dur"] /= 60.0
        sb, sp = sub["bet"].sum(), sub["payout"].sum()
        frag.append(f"| {mon} | {sub['user_id'].nunique():,} | {sb/1e4:,.1f} | {(sb-sp)/1e4:,.1f} | {sp/sb*100:.2f} | "
                    f"{tri(usr['bet'])} | {tri(usr['n'])} | {tri(usr['dur'])} | {ret(sub,1):.1f}/{ret(sub,3):.1f}/{ret(sub,7):.1f} |")
        print(f"  [{name}] {mon}")
    (OUT / f"{name}_timeline.md").write_text("\n".join(frag), encoding="utf-8")
    print(f"  {name} 落地")


def main():
    only = sys.argv[1:] or [g[0] for g in GAMES]
    for cfg in GAMES:
        if cfg[0] in only:
            print(f"=== {cfg[0]} ==="); run(*cfg)


if __name__ == "__main__":
    main()
