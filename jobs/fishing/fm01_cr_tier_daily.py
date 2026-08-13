"""挽留组(retention/CR)按 T1–T4 分层的每日表现 + 留存。

- 组别口径见 jobs/fishing/fm01_grouping.py（GROUP_CASE）。仅取 retention 组记录。
- 分层（玩家级/天级，避免跨层）：某玩家某北京日
    有 CR_FISHING_V1:Tk 记录 -> 取该 Tk；
    当天只有兜底 DEFAULT_FALLBACK（无 CR）-> 归 T4（对齐「T4* + default = T4:P2」归并规则）。
- 时间：event_timestamp(UTC) +8h 转北京，按自然日 0–24 点切割。
- 每（日 × 层）输出：人数 / 投注次数 / 投注额 / 人均投注 / 人均投注次数 / RTP / D1·D3·D7 留存。
  留存 = 该(日,层)玩家在 D0+N 日仍有任意 FM01 投注的比例；末尾数日 D3/D7 因右截断留空。

用法：python3 jobs/fishing/fm01_cr_tier_daily.py [--start 2026-07-31] [--end 2026-08-13]
输出：data/output/fm01_cr_tier_daily.csv
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
    # 1) 每日×层 人数/投注 + 玩家名单(用于留存)
    base = f"""
    WITH tagged AS (
      SELECT user_id, bet, payout, strategy_name,
        (event_timestamp + interval '8 hours')::date AS bj_date,
        {GROUP_CASE} AS gt
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}'),
    ret AS (SELECT * FROM tagged WHERE gt='retention'
            AND bj_date >= '{start}' AND bj_date < '{end}'),
    pday AS (   -- 玩家×天 定桶(Tk:Pj) + 投注汇总（每个 user×bj_date 恰好一行/一桶）
      -- 层 Tk = 当天 CR 记录的 T（否则=兜底 default -> T4）；档 Pj = CR 记录的 P（否则 -> P2）
      -- 归并：T4 一律归 P2（T4:P1/T4:P2/纯default 全部 = T4:P2）
      SELECT user_id, bj_date, bet, payout, shots,
        t_tier || ':' || CASE WHEN t_tier='T4' THEN 'P2' ELSE t_pool END AS tier
      FROM (
        SELECT user_id, bj_date,
          COALESCE(NULLIF(MAX(regexp_substr(strategy_name,'T[1-4]')),''), 'T4') t_tier,
          COALESCE(NULLIF(MAX(regexp_substr(strategy_name,'P[1-2]')),''), 'P2') t_pool,
          SUM(bet) bet, SUM(payout) payout, COUNT(*) shots
        FROM ret GROUP BY 1,2))
    """
    daily = pd.DataFrame(be.execute(base + """
      SELECT bj_date, tier, COUNT(DISTINCT user_id) users, SUM(shots) shots,
        ROUND(SUM(bet),0) bet, ROUND(SUM(payout),0) payout
      FROM pday GROUP BY 1,2 ORDER BY 1,2"""),
      columns=["bj_date","tier","users","shots","bet","payout"])
    for c in ["users","shots","bet","payout"]:
        daily[c] = daily[c].astype(float)
    daily["avg_bet_per_user"] = (daily.bet/daily.users).round(1)
    daily["avg_shots_per_user"] = (daily.shots/daily.users).round(0)
    daily["rtp"] = (daily.payout/daily.bet*100).round(2)

    # 2) 留存：pday(cohort) join 全游戏活跃日
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
    # 对账：挽留组每天总人数（应 == 各层人数之和，因每个 user×天 唯一层）
    tot = pd.DataFrame(be.execute(base + """
      SELECT bj_date, COUNT(DISTINCT user_id) total_users FROM pday GROUP BY 1 ORDER BY 1"""),
      columns=["bj_date","total_users"])
    be.close()

    piv = ret.pivot_table(index=["bj_date","tier"], columns="n", values="ret").reset_index()
    piv.columns = ["bj_date","tier"] + [f"D{c}" for c in piv.columns[2:]]
    out = daily.merge(piv, on=["bj_date","tier"], how="left")
    # 右截断：末日 DN 因未来数据不足置空
    maxd = pd.to_datetime(out.bj_date).max()
    for n in (1,3,5,7,10):
        col=f"D{n}"
        if col in out:
            mask = pd.to_datetime(out.bj_date) > (maxd - pd.Timedelta(days=n))
            out.loc[mask, col] = None
    out.to_csv("data/output/fm01_cr_tier_daily.csv", index=False)
    pd.set_option("display.width",260); pd.set_option("display.max_rows",None)
    show = out[["bj_date","tier","users","avg_bet_per_user","avg_shots_per_user","rtp","D1","D3","D5","D7","D10"]]
    print(show.to_string(index=False), flush=True)
    # 对账
    chk = out.groupby("bj_date").users.sum().reset_index().merge(tot, on="bj_date")
    chk["match"] = (chk.users == chk.total_users)
    print("\n=== 对账：各层人数之和 vs 挽留组当天全体 ===", flush=True)
    print(chk.to_string(index=False), flush=True)
    print(f"全部匹配: {chk.match.all()}", flush=True)
    print(f"\n完整表 -> data/output/fm01_cr_tier_daily.csv (桶=Tk:Pj)", flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-07-31")
    p.add_argument("--end", default="2026-08-13")
    a = p.parse_args()
    run(a.start, a.end)


if __name__ == "__main__":
    main()
