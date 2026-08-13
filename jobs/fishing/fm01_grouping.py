"""FM01 玩家分组（group_tag）—— 唯一取数口径，供所有分析脚本复用。

规则见 docs/FM01_分组政策.md（2026-07-30 版）。记录级（逐发子弹）判定，优先级从上到下：
  1. DYNAMIC_RTP_V3            -> dynamic_rtp
  2. RC_FISHING_*              -> risk_control
  3. *RISK_CONTROL*            -> risk_control
  4. 剩余 + event_timestamp >= 挽留上线 + user_id%10 in (0,1) -> retention
  5. 其余                      -> default

挽留上线：2026-07-30 16:00 PST = 2026-07-31 00:00 UTC（event_timestamp 为 UTC，直接比即可）。

用法：
  from jobs.fishing.fm01_grouping import GROUP_CASE           # 拿到 CASE 片段拼进任意 SQL
  python3 jobs/fishing/fm01_grouping.py [--start 2026-07-31] [--end 2026-08-13]   # 跑各组概览
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

# 挽留系统上线分界（UTC；= 2026-07-30 16:00 PST）
RETENTION_LAUNCH_UTC = "2026-07-31 00:00:00"

# 权威分组判定表达式（引用列：strategy_name, event_timestamp, user_id）
GROUP_CASE = f"""CASE
  WHEN strategy_name = 'DYNAMIC_RTP_V3'    THEN 'dynamic_rtp'
  WHEN strategy_name LIKE 'RC_FISHING%'    THEN 'risk_control'
  WHEN strategy_name LIKE '%RISK_CONTROL%' THEN 'risk_control'
  WHEN event_timestamp >= '{RETENTION_LAUNCH_UTC}'
       AND user_id % 10 IN (0, 1)          THEN 'retention'
  ELSE 'default'
END"""

# 通用清洗过滤（对齐动态RTP报告口径：CNY、剔测试单）
BASE_FILTER = "currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"

# 挽留组内 strategy_name 归一化：T4 各档 + 兜底 DEFAULT_FALLBACK 统一记为 T4:P2（=不加成/停发状态）。
# 仅对 group_tag='retention' 生效；default 组的 DEFAULT_FALLBACK 不受影响。
# 依赖外层已算出的 group_tag 列。
EFF_STRATEGY_CASE = """CASE
  WHEN group_tag = 'retention'
       AND (strategy_name LIKE 'CR_FISHING_V1:T4%' OR strategy_name = 'DEFAULT_FALLBACK')
    THEN 'CR_FISHING_V1:T4:P2'
  ELSE strategy_name
END"""


def summary(start: str, end: str):
    be = rs.RedshiftBackend(database="transform-agfish-game",
                            bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)
    sql = f"""
    SELECT {GROUP_CASE} AS group_tag,
      count(distinct user_id) AS users,
      count(*)                AS shots,
      round(sum(bet),0)       AS bet,
      round(sum(payout),0)    AS payout,
      round(sum(payout)/nullif(sum(bet),0)*100,2) AS rtp,
      round(sum(payout)-sum(bet),0) AS net
    FROM public.bullet
    WHERE {BASE_FILTER}
      AND event_timestamp >= '{start}' AND event_timestamp < '{end}'
    GROUP BY 1 ORDER BY shots DESC
    """
    print(f"FM01 各组概览  {start} ~ {end} (UTC)\n"
          f"{'group_tag':14}{'users':>9}{'shots':>14}{'bet':>16}{'RTP%':>9}{'net':>14}", flush=True)
    tot_bet = tot_pay = 0
    for r in be.execute(sql):
        print(f"  {r[0]:12}{r[1]:>9,}{r[2]:>14,}{float(r[3]):>16,.0f}{float(r[5]):>9}{float(r[6]):>14,.0f}",
              flush=True)
        tot_bet += float(r[3]); tot_pay += float(r[4])
    print(f"  {'TOTAL':12}{'':>9}{'':>14}{tot_bet:>16,.0f}{tot_pay/tot_bet*100:>9.2f}", flush=True)
    be.close()


def strategy_distribution(start: str, end: str):
    """每个 group_tag 内部各 strategy_name 的占比（子弹数 / 投注 / RTP）。"""
    be = rs.RedshiftBackend(database="transform-agfish-game",
                            bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)
    sql = f"""
    WITH g AS (
      SELECT {GROUP_CASE} AS group_tag, strategy_name, bet, payout, user_id
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{start}' AND event_timestamp < '{end}'),
    e AS (SELECT group_tag, {EFF_STRATEGY_CASE} AS strategy_name, bet, payout, user_id FROM g)
    SELECT group_tag, strategy_name,
      count(distinct user_id) users, count(*) shots,
      round(100.0*count(*)/sum(count(*)) OVER (PARTITION BY group_tag),2) shot_pct,
      round(sum(bet),0) bet,
      round(sum(payout)/nullif(sum(bet),0)*100,2) rtp
    FROM e GROUP BY 1,2 ORDER BY group_tag, shots DESC
    """
    print(f"\n各组内部 strategy_name 分布  {start} ~ {end} (UTC)", flush=True)
    cur = None
    for r in be.execute(sql):
        if r[0] != cur:
            cur = r[0]
            print(f"\n[{cur}]  {'strategy_name':28}{'users':>8}{'shots':>13}{'shot%':>8}{'bet':>15}{'RTP%':>8}", flush=True)
        print(f"  {r[1] or '(空)':28}{r[2]:>8,}{r[3]:>13,}{float(r[4]):>8}{float(r[5]):>15,.0f}{float(r[6]):>8}", flush=True)
    be.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-07-31", help="UTC start (default = 挽留上线日)")
    p.add_argument("--end", default="2026-08-13", help="UTC end (exclusive)")
    p.add_argument("--no-strategy", action="store_true", help="skip strategy_name breakdown")
    a = p.parse_args()
    summary(a.start, a.end)
    if not a.no_strategy:
        strategy_distribution(a.start, a.end)


if __name__ == "__main__":
    main()
