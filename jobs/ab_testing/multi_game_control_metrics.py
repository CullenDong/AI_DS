"""综合调控效用报告 —— 四游戏(FM01/SS03/SS02/SS06)各调控机制按月 treatment vs control。

统一核心指标（每 游戏×组×月）：用户、总投注额(万)、总payout(万)、总GGR(万)、RTP%(含FG)、
人均投注(中/均/均99)、D1/D3/D7 留存。北京日分月。config 驱动，每游戏一个片段。

分组口径：
  FM01(bullet)          : strategy_name → dynamic_rtp / retention / risk_control / default
  SS03(fct_bet_orders)  : Default / AB_TEST_A / AB_TEST_B / AI（切换点+MOD10，见 ss03_grouping）
  SS02(fct_bet_orders)  : partition_ab[0]=jojpin-9mokha-rexQug → AI，否则 Default
  SS06(fct_bet_orders)  : MOD10 → holdout(0,1)/planA(2-5)/planB(6-9)
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402
from jobs.fishing.fm01_grouping import GROUP_CASE as FM01_CASE, BASE_FILTER as FM01_FILTER  # noqa: E402
from jobs.ss03_analysis.ss03_grouping import GROUP_CASE as SS03_CASE  # noqa: E402

OUT = ROOT / "data" / "output" / "multi_game"; OUT.mkdir(parents=True, exist_ok=True)
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean() if len(s) else float("nan")

SS02_CASE = "CASE WHEN partition_ab[0]::varchar='jojpin-9mokha-rexQug' THEN 'AI' ELSE 'Default' END"
SS06_CASE = ("CASE WHEN MOD(user_id,10) IN (0,1) THEN 'holdout' "
             "WHEN MOD(user_id,10) IN (2,3,4,5) THEN 'planA' ELSE 'planB' END")
SLOT_FILTER = "currency_type='CNY' AND status='COMPLETED' AND op_code NOT IN ('B26','TST','TSB','TSO')"

# (名, db, port, table, timecol, betcol, paycol, filter, group_case, 组顺序, 起始北京日)
GAMES = [
    ("FM01", "transform-agfish-game", 5442, "public.bullet", "event_timestamp", "bet", "payout",
     FM01_FILTER, FM01_CASE, ["default", "dynamic_rtp", "retention", "risk_control"], "2026-03-01"),
    ("SS03", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout",
     f"game_id='SS03' AND {SLOT_FILTER}", SS03_CASE, ["Default", "AB_TEST_A", "AB_TEST_B", "AI"], "2026-03-01"),
    ("SS02", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout",
     f"game_id='SS02' AND {SLOT_FILTER}", SS02_CASE, ["Default", "AI"], "2026-04-01"),
    ("SS06", "slot-machine", 5443, "public.fct_bet_orders", "created_at", "bet_amount", "actual_payout",
     f"game_id='SS06' AND {SLOT_FILTER}", SS06_CASE, ["holdout", "planA", "planB"], "2026-07-01"),
]


def run_game(name, db, port, table, tc, bc, pc, filt, case, order, start):
    be = rs.RedshiftBackend(database=db, bastion_ip=rs.DEFAULT_BASTION_IP, local_port=port)
    bj = f"({tc}+interval '8 hours')::date"
    ud = pd.DataFrame(be.execute(f"""
      SELECT ({case}) grp, user_id, {bj} d, COUNT(*) n, SUM({bc}) bet, SUM({pc}) payout,
             DATEDIFF(second, MIN({tc}), MAX({tc})) span
      FROM {table} WHERE {filt} AND {bj} >= '{start}' GROUP BY 1,2,3"""),
      columns=["grp", "user_id", "d", "n", "bet", "payout", "span"])
    glob = pd.DataFrame(be.execute(f"SELECT user_id, {bj} d FROM {table} WHERE {filt} AND {bj} >= '{start}' GROUP BY 1,2"),
                        columns=["user_id", "d"])
    be.close()
    ud["d"] = pd.to_datetime(ud["d"])
    for c in ["n", "bet", "payout", "span"]:
        ud[c] = ud[c].astype(float)
    ud["mon"] = ud["d"].dt.strftime("%Y-%m")
    glob["d"] = pd.to_datetime(glob["d"])
    act = {u: set(x) for u, x in glob.groupby("user_id")["d"]}
    last = glob["d"].max()

    def ret(sub, N):
        num = den = 0
        for d, g in sub.groupby("d"):
            tgt = d + pd.Timedelta(days=N)
            if tgt > last: continue
            us = g["user_id"].unique(); den += len(us)
            num += sum(1 for u in us if tgt in act.get(u, ()))
        return num / den * 100 if den else float("nan")

    tri = lambda s: f"{s.median():.0f}/{s.mean():.0f}/{w99(s):.0f}"
    months = sorted(ud["mon"].unique())
    frag = [f"\n### {name}\n"]
    for mon in months:
        frag.append(f"\n**{mon}**\n\n| 组 | 用户 | 总投注额(万) | 总GGR(万) | RTP% | 人均投注额 中/均/均99 | 人均投注次数 中/均/均99 | 人均时长(分) 中/均/均99 | D1/D3/D7 |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for g in order:
            sub = ud[(ud["mon"] == mon) & (ud["grp"] == g)]
            if not len(sub): continue
            usr = sub.groupby("user_id").agg(bet=("bet", "sum"), n=("n", "sum"), dur=("span", "sum"))
            usr["dur"] = usr["dur"] / 60.0                     # 秒->分
            sb, sp = sub["bet"].sum(), sub["payout"].sum()
            frag.append(f"| {g} | {sub['user_id'].nunique():,} | {sb/1e4:,.1f} | {(sb-sp)/1e4:,.1f} | "
                        f"{sp/sb*100:.2f} | {tri(usr['bet'])} | {tri(usr['n'])} | {tri(usr['dur'])} | "
                        f"{ret(sub,1):.1f}/{ret(sub,3):.1f}/{ret(sub,7):.1f} |")
        print(f"  [{name}] {mon} 出完")
    p = OUT / f"{name}.md"; p.write_text("\n".join(frag), encoding="utf-8")
    print(f"  {name} 落地 {p}")


def main():
    only = sys.argv[1:] or [g[0] for g in GAMES]
    for cfg in GAMES:
        if cfg[0] in only:
            print(f"=== {cfg[0]} ===")
            run_game(*cfg)


if __name__ == "__main__":
    main()
