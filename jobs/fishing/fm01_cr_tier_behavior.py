"""挽留组(retention/CR)按 T1–T4 大层（不分 P1/P2）的每日玩家行为 + 留存 详细分析。

分层：玩家×天级，有 CR 加成记录取该 Tk，纯兜底 default 归 T4（见 fm01_cr_tier_daily.py）。
时间：event_timestamp(UTC) +8h 转北京，自然日切割。

行为指标（每 日×层）：
  users 人数 / avg_bets 人均投注次数 / avg_bet 人均投注额 / avg_net 人均净盈亏 /
  bet_size 平均单笔投注 / rtp 整层RTP / hit_rate 命中率 / avg_fish_value 平均鱼价值 /
  avg_level 平均子弹档 / avg_mult 平均子弹倍率 / avg_first_kill 平均首次击杀轮次
留存指标：D1 / D3 / D5 / D7 / D10（末尾数日右截断留空）

用法：python3 jobs/fishing/fm01_cr_tier_behavior.py [--start 2026-07-31] [--end 2026-08-13]
输出：data/output/fm01_cr_tier_behavior.csv
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs           # noqa: E402
from jobs.fishing.fm01_grouping import GROUP_CASE, BASE_FILTER, RETENTION_LAUNCH_UTC  # noqa: E402


def run(start: str, end: str):
    be = rs.RedshiftBackend(database="transform-agfish-game",
                            bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)
    base = f"""
    WITH tagged AS (
      SELECT user_id, bet, payout, killed, fish_value, bullet_level, multiplier,
        strategy_name, event_timestamp,
        (event_timestamp + interval '8 hours')::date AS bj_date,
        {GROUP_CASE} AS gt
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}'),
    ret AS (SELECT * FROM tagged WHERE gt='retention'
            AND bj_date >= '{start}' AND bj_date < '{end}'),
    seq AS (SELECT user_id, bj_date, killed,
              row_number() OVER (PARTITION BY user_id,bj_date ORDER BY event_timestamp) rn
            FROM ret),
    fk AS (SELECT user_id, bj_date, MIN(CASE WHEN killed=1 THEN rn END) first_kill
           FROM seq GROUP BY 1,2),
    pday AS (   -- 玩家×天 定层(大层) + 汇总
      SELECT user_id, bj_date,
        COALESCE(NULLIF(MAX(regexp_substr(strategy_name,'T[1-4]')),''), 'T4') tier,
        SUM(bet) bet, SUM(payout) payout, COUNT(*) shots, SUM(killed) killed,
        SUM(fish_value) fv, SUM(bullet_level) bl, SUM(multiplier) mult
      FROM ret GROUP BY 1,2)
    """
    beh = pd.DataFrame(be.execute(base + """
      SELECT p.bj_date, p.tier,
        COUNT(DISTINCT p.user_id) users, SUM(p.shots) shots, SUM(p.bet) bet, SUM(p.payout) payout,
        SUM(p.killed) killed, SUM(p.fv) fv, SUM(p.bl) bl, SUM(p.mult) mult,
        ROUND(AVG(fk.first_kill),1) avg_first_kill
      FROM pday p JOIN fk ON p.user_id=fk.user_id AND p.bj_date=fk.bj_date
      GROUP BY 1,2 ORDER BY 1,2"""),
      columns=["bj_date","tier","users","shots","bet","payout","killed","fv","bl","mult","avg_first_kill"])
    for c in ["users","shots","bet","payout","killed","fv","bl","mult","avg_first_kill"]:
        beh[c] = beh[c].astype(float)
    beh["avg_bets"]      = (beh.shots/beh.users).round(0)          # 人均投注次数
    beh["avg_bet"]       = (beh.bet/beh.users).round(1)            # 人均投注额
    beh["avg_net"]       = ((beh.payout-beh.bet)/beh.users).round(1)  # 人均净盈亏
    beh["bet_size"]      = (beh.bet/beh.shots).round(3)            # 平均单笔投注
    beh["rtp"]           = (beh.payout/beh.bet*100).round(2)       # 整层RTP
    beh["hit_rate"]      = (beh.killed/beh.shots*100).round(2)     # 命中率%
    beh["avg_fish_value"]= (beh.fv/beh.shots).round(1)             # 平均鱼价值
    beh["avg_level"]     = (beh.bl/beh.shots).round(3)             # 平均子弹档
    beh["avg_mult"]      = (beh.mult/beh.shots).round(3)           # 平均子弹倍率

    # 留存
    ret = pd.DataFrame(be.execute(base + f"""
      , active AS (SELECT DISTINCT user_id, (event_timestamp+interval '8 hours')::date d
                   FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}')
      SELECT p.bj_date, p.tier, N.n,
        ROUND(100.0*COUNT(DISTINCT a.user_id)/NULLIF(COUNT(DISTINCT p.user_id),0),2) ret
      FROM pday p
      CROSS JOIN (SELECT 1 n UNION SELECT 3 UNION SELECT 5 UNION SELECT 7 UNION SELECT 10) N
      LEFT JOIN active a ON a.user_id=p.user_id AND a.d = dateadd(day, N.n, p.bj_date)
      GROUP BY 1,2,3"""),
      columns=["bj_date","tier","n","ret"])
    be.close()

    piv = ret.pivot_table(index=["bj_date","tier"], columns="n", values="ret").reset_index()
    piv.columns = ["bj_date","tier"] + [f"D{c}" for c in piv.columns[2:]]
    out = beh.merge(piv, on=["bj_date","tier"], how="left")
    maxd = pd.to_datetime(out.bj_date).max()
    for n in (1,3,5,7,10):
        c=f"D{n}"
        if c in out:
            out.loc[pd.to_datetime(out.bj_date) > (maxd - pd.Timedelta(days=n)), c] = None
    cols = ["bj_date","tier","users","avg_bets","avg_bet","bet_size","avg_net","rtp",
            "hit_rate","avg_fish_value","avg_level","avg_mult","avg_first_kill",
            "D1","D3","D5","D7","D10"]
    out = out[cols]
    out.to_csv("data/output/fm01_cr_tier_behavior.csv", index=False)
    pd.set_option("display.width",300); pd.set_option("display.max_rows",None); pd.set_option("display.max_columns",None)
    print("=== 玩家行为（每日×层）===")
    print(out[["bj_date","tier","users","avg_bets","avg_bet","bet_size","avg_net","rtp",
               "hit_rate","avg_fish_value","avg_level","avg_mult","avg_first_kill"]].to_string(index=False), flush=True)
    print("\n=== 留存（每日×层）===")
    print(out[["bj_date","tier","users","D1","D3","D5","D7","D10"]].to_string(index=False), flush=True)
    print("\n完整表 -> data/output/fm01_cr_tier_behavior.csv", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-07-31")
    p.add_argument("--end", default="2026-08-13")
    a = p.parse_args()
    run(a.start, a.end)


if __name__ == "__main__":
    main()
