"""验证 CBO 数学表实验各期「每组用哪张主基础表」，对齐 ab_summary_and_plan.md 时间线。

各期窗口按北京日((created_at+8h)::date)。分组用权威 GROUP_CASE（切换点 08-03 23:00 UTC 前
按 partition_ab[0]、之后按 MOD10）。排除 kakutei 暗保底表后，取该(期,组)去重用户最多的表为主基础表。
只做只读探查，输出每(期,组)各 math_table_id 的去重用户/笔数/投注额占比。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs                          # noqa: E402
from jobs.ss03_analysis.ss03_grouping import GROUP_CASE, BASE_FILTER  # noqa: E402

# 各期北京日窗口（含端点）。P2 拆前/后切换点各一段以保同期洁净。
PERIODS = {
    "P0_一期(pre07-05)":  ("2026-06-15", "2026-07-04"),   # Default=zero, TestA=95Kai, TestB=97BG
    "P1(07-05~07-29)":    ("2026-07-07", "2026-07-28"),   # Default=95Kai, TestA=zero, TestB=93Kai
    "P2pre(07-30~08-03)": ("2026-07-31", "2026-08-03"),   # Default=95Kai, TestA=shi, TestB=95bgtr (partition_ab)
    "P2post(08-05~08-17)":("2026-08-05", "2026-08-17"),   # 同上(MOD10)
    "P3(08-19~)":         ("2026-08-19", "2026-09-01"),   # Default=95Kai, TestA=97bgtr+BGadj, TestB=95bgtr, AI=mix
}


def main():
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    # 数据可用范围
    mn, mx = be.execute("SELECT MIN(created_at), MAX(created_at) FROM public.fct_bet_orders WHERE game_id='SS03'")[0]
    print(f"SS03 数据范围(created_at UTC): {mn} ~ {mx}\n")

    for name, (lo, hi) in PERIODS.items():
        rows = be.execute(f"""
          SELECT ({GROUP_CASE}) grp, math_table_id,
                 COUNT(DISTINCT user_id) uu, COUNT(*) nspin,
                 SUM(CASE WHEN bet_amount>0 THEN bet_amount ELSE 0 END) bet
          FROM public.fct_bet_orders
          WHERE {BASE_FILTER}
            AND (created_at + interval '8 hours')::date BETWEEN '{lo}' AND '{hi}'
          GROUP BY 1,2
        """)
        df = pd.DataFrame(rows, columns=["grp", "table", "uu", "nspin", "bet"])
        if not len(df):
            print(f"===== {name} ({lo}~{hi}) : 无数据 =====\n"); continue
        df[["uu", "nspin", "bet"]] = df[["uu", "nspin", "bet"]].astype(float)
        print(f"===== {name} ({lo}~{hi}) =====")
        for g in ["Default", "AB_TEST_A", "AB_TEST_B", "AI"]:
            sub = df[df["grp"] == g].sort_values("uu", ascending=False)
            if not len(sub):
                continue
            tot_u = sub["uu"].sum()
            # 主基础表：排除含 kakutei/確定 的暗保底表
            base = sub[~sub["table"].astype(str).str.contains("kakutei|kakutei|確定|guarantee", case=False, na=False)]
            main = base.iloc[0]["table"] if len(base) else "(全是kakutei?)"
            print(f"  [{g}] 去重用户≈{tot_u:,.0f}  主基础表= {main}")
            for _, r in sub.head(5).iterrows():
                tag = " ←主" if r["table"] == main else ""
                print(f"       {str(r['table'])[:52]:52} uu={r['uu']:>7,.0f}  bet%={r['bet']/sub['bet'].sum()*100:5.1f}{tag}")
        print()
    be.close()


if __name__ == "__main__":
    main()
