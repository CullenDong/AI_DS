"""探每玩家(期内)总投注额分布，为大/中/小投注额玩家定阈值。"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs                          # noqa: E402
from jobs.ss03_analysis.ss03_grouping import GROUP_CASE, BASE_FILTER  # noqa: E402

PERIODS = [("P0", "2026-06-15", "2026-07-04"), ("P1", "2026-07-07", "2026-07-28"),
           ("P2", "2026-08-05", "2026-08-17"), ("P3", "2026-08-19", "2026-09-01")]
QS = [.25, .5, .75, .90, .95, .99]


def main():
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    pool = []
    for name, lo, hi in PERIODS:
        rows = be.execute(f"""
          SELECT user_id, SUM(bet_amount) tot FROM public.fct_bet_orders
          WHERE {BASE_FILTER} AND bet_amount>0
            AND (created_at+interval '8 hours')::date BETWEEN '{lo}' AND '{hi}'
          GROUP BY 1""")
        s = pd.Series([float(t) for _, t in rows])
        pool.append(s)
        q = s.quantile(QS)
        print(f"{name} n={len(s):,}  " + "  ".join(f"P{int(p*100)}={q[p]:,.0f}" for p in QS))
    alls = pd.concat(pool, ignore_index=True)
    q = alls.quantile(QS)
    print(f"\n合并 n={len(alls):,}  " + "  ".join(f"P{int(p*100)}={q[p]:,.0f}" for p in QS))
    # 候选绝对阈值下的覆盖（用合并分布）
    print("\n候选阈值覆盖(合并分布)：小=<t1, 中=[t1,t2), 大=>=t2")
    for t1, t2 in [(200, 2000), (300, 3000), (500, 5000), (500, 3000)]:
        sm = (alls < t1).mean() * 100
        md = ((alls >= t1) & (alls < t2)).mean() * 100
        lg = (alls >= t2).mean() * 100
        bshare_lg = alls[alls >= t2].sum() / alls.sum() * 100
        print(f"  t1={t1:>4} t2={t2:>5}: 小 {sm:4.1f}% / 中 {md:4.1f}% / 大 {lg:4.1f}%  (大玩家占总投注 {bshare_lg:4.1f}%)")
    be.close()


if __name__ == "__main__":
    main()
