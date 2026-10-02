"""SS03 HMM 打标 · 第1步：分天批次在 Redshift 聚合「玩家×天」特征，存本地 parquet(可续传)。

模型只需 5 发射特征，多为当天聚合 → 在 Redshift SQL 里逐北京日算，只回传小结果(玩家×天)，
全量也不爆内存。no_bet_streak / k(tenure)留到第2步汇总时跨天算。

每天产出列：user_id, bet_date, avg_bet_one_time_today, bet_count_today,
  bet_level_distinct_today, deposit_amount_today, bet_amount_today。
口径对齐 delivery：刨 normal_kakuteiB；deposit = 当日内 balance 残差(跨日边界首发置0,近似)。

输出：data/output/hmm/ss03_ud_daily/dt=<北京日>/part.parquet
用法：python3 jobs/ss03_analysis/ss03_hmm_daybatch.py [--start 2026-05-21] [--end 2026-10-03]
"""
from __future__ import annotations
import argparse, sys
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402
OUT = ROOT / "data" / "output" / "hmm" / "ss03_ud_daily"; OUT.mkdir(parents=True, exist_ok=True)


def day_sql(d):
    bj = "(created_at + interval '8 hours')::date"
    return f"""
    WITH s AS (
      SELECT user_id, created_at, spin_id, bet_type, bet_amount, credit,
        GREATEST(COALESCE(balance_after_bet -
          (LAG(balance_after_payout) OVER (PARTITION BY user_id ORDER BY created_at, spin_id) - bet_amount), 0), 0) AS dep
      FROM public.fct_bet_orders
      WHERE game_id='SS03' AND currency_type='CNY' AND status='COMPLETED'
        AND op_code NOT IN ('B26','TST','TSB','TSO')
        AND math_table_id <> 'normal_kakuteiB'
        AND {bj} = '{d}')
    SELECT user_id,
      AVG(CASE WHEN bet_type='BASE' THEN bet_amount END)                 AS avg_bet_one_time_today,
      COUNT(*)                                                           AS bet_count_today,
      COUNT(DISTINCT CASE WHEN bet_type='BASE' THEN credit END)          AS bet_level_distinct_today,
      SUM(bet_amount)                                                    AS bet_amount_today,
      SUM(CASE WHEN dep > 0.001 THEN dep ELSE 0 END)                     AS deposit_amount_today
    FROM s GROUP BY user_id"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-05-21"); ap.add_argument("--end", default="2026-10-03")
    a = ap.parse_args()
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    d, stop = date.fromisoformat(a.start), date.fromisoformat(a.end)
    cols = ["user_id", "avg_bet_one_time_today", "bet_count_today", "bet_level_distinct_today",
            "bet_amount_today", "deposit_amount_today"]
    while d < stop:
        ds = d.isoformat(); p = OUT / f"dt={ds}" / "part.parquet"
        if p.exists() and p.stat().st_size > 0:
            d += timedelta(days=1); continue
        df = pd.DataFrame(be.execute(day_sql(ds)), columns=cols)
        if len(df):
            df.insert(1, "bet_date", ds)
            for c in cols[1:]:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
            p.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(p, index=False)
        print(f"[{ds}] {len(df):,} 玩家-天", flush=True)
        d += timedelta(days=1)
    be.close()
    print("分天特征全部落地 →", OUT)


if __name__ == "__main__":
    main()
