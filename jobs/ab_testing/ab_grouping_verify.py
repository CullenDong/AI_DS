"""AB 分组人群验证（合并 pop_sizing + 静态40/40/20 + 每期轮换）。

三个子命令，均基于 FM01 挽留上线后数据、复用 jobs.fishing.fm01_grouping 口径：
  pop      —— 可用 eligible 人群（clean default）按尾号测量（严格正交口径）
  static   —— 风控 sticky + 尾号 40/40/20 静态分组人数验证
  rotate   —— 每期 +40 旋转轮换：每期 40/40/20 + 相邻期硬排除（对角线=0）

用法：
  python3 jobs/ab_testing/ab_grouping_verify.py pop
  python3 jobs/ab_testing/ab_grouping_verify.py static
  python3 jobs/ab_testing/ab_grouping_verify.py rotate
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.db import redshift as rs                                    # noqa: E402
from jobs.fishing.fm01_grouping import BASE_FILTER, RETENTION_LAUNCH_UTC  # noqa: E402
import pandas as pd                                                    # noqa: E402

FARM_PIDS = "('MD5','EW3','KV3','JN1','C81','HL2','C16','JR8')"


def _be():
    return rs.RedshiftBackend(database="transform-agfish-game",
                              bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)


def pop():
    """eligible = 尾号2-9 且从未被 dynamic/风控接管 且非农场 —— 严格正交人群测量。"""
    be = _be()
    sql = f"""
    WITH u AS (
      SELECT user_id,
        MAX(CASE WHEN strategy_name='DYNAMIC_RTP_V3'
                 OR strategy_name LIKE 'RC_FISHING%'
                 OR strategy_name LIKE '%RISK_CONTROL%' THEN 1 ELSE 0 END) touched_other,
        MAX(CASE WHEN op_code IN {FARM_PIDS} THEN 1 ELSE 0 END) is_farm,
        COUNT(*) shots
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}'
      GROUP BY 1)
    SELECT user_id % 10 AS tail, COUNT(*) users_all,
      SUM(CASE WHEN touched_other=0 AND is_farm=0 THEN 1 ELSE 0 END) eligible_users
    FROM u GROUP BY 1 ORDER BY 1
    """
    d = pd.DataFrame(be.execute(sql), columns=["尾号", "该尾号总用户", "eligible用户"])
    be.close()
    for c in ["该尾号总用户", "eligible用户"]:
        d[c] = d[c].astype(float)
    print(d.to_string(index=False))
    print("\n尾号2-9 eligible 合计:",
          f"{d[d['尾号'].between(2,9)]['eligible用户'].sum():,.0f} 人")


def static():
    """风控 sticky（ever-风控）+ 其余尾号 40/40/20（0-3 dyn / 4-7 ret / 8-9 def）。"""
    DIGIT_MAP = {0: "dynamic_rtp", 1: "dynamic_rtp", 2: "dynamic_rtp", 3: "dynamic_rtp",
                 4: "retention", 5: "retention", 6: "retention", 7: "retention",
                 8: "default", 9: "default"}
    be = _be()
    rows = be.execute(f"""
      SELECT user_id % 10 AS tail,
        MAX(CASE WHEN strategy_name LIKE 'RC_FISHING%' OR strategy_name LIKE '%RISK_CONTROL%'
                 THEN 1 ELSE 0 END) AS ever_rc
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}'
      GROUP BY user_id
    """)
    be.close()
    df = pd.DataFrame(rows, columns=["tail", "ever_rc"])
    df["tail"] = df["tail"].astype(int)
    df["grp"] = df.apply(
        lambda r: "risk_control" if r["ever_rc"] == 1 else DIGIT_MAP[r["tail"]], axis=1)
    total = len(df)
    g = df.groupby("grp").size().reset_index(name="users")
    g["占比%"] = (g.users / total * 100).round(1)
    print(g.to_string(index=False))
    print(f"\n全体 distinct 用户: {total:,}")


def rotate():
    """每期 +40 旋转：验证每期 40/40/20 与相邻期硬排除（同臂转移=0）。"""
    STEP = 40
    BANDS = [(0, 40, "dynamic_rtp"), (40, 80, "retention"), (80, 100, "default")]

    def band(x):
        for lo, hi, name in BANDS:
            if lo <= x < hi:
                return name
        return "?"

    be = _be()
    rows = be.execute(f"""
      SELECT user_id % 100 AS rank,
        MAX(CASE WHEN strategy_name LIKE 'RC_FISHING%' OR strategy_name LIKE '%RISK_CONTROL%'
                 THEN 1 ELSE 0 END) ever_rc
      FROM public.bullet
      WHERE {BASE_FILTER} AND event_timestamp >= '{RETENTION_LAUNCH_UTC}'
      GROUP BY user_id
    """)
    be.close()
    df = pd.DataFrame(rows, columns=["rank", "ever_rc"])
    df = df[df["ever_rc"] == 0].copy()
    df["rank"] = df["rank"].astype(int)
    N = len(df)
    print(f"参与轮换（非风控）用户: {N:,}\n")
    for p in range(4):
        df[f"W{p}"] = ((df["rank"] + STEP * p) % 100).map(band)
    print("=== 每期各臂占比 ===")
    for p in range(4):
        vc = df[f"W{p}"].value_counts()
        print("W%d: " % p + " | ".join(
            f"{a}:{vc.get(a,0)/N*100:.1f}%" for a in ["dynamic_rtp", "retention", "default"]))
    print("\n=== 相邻期同臂转移（应全 0）===")
    for p in range(3):
        ct = pd.crosstab(df[f"W{p}"], df[f"W{p+1}"]).reindex(
            index=["dynamic_rtp", "retention", "default"],
            columns=["dynamic_rtp", "retention", "default"], fill_value=0)
        print(f"W{p}->W{p+1} 对角线合计 = {sum(ct.loc[a,a] for a in ct.index)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["pop", "static", "rotate"])
    {"pop": pop, "static": static, "rotate": rotate}[ap.parse_args().cmd]()


if __name__ == "__main__":
    main()
