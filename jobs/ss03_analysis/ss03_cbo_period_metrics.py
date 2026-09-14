"""CBO 数学表实验：各期「同期对比」真实指标 + 玩家生命周期(日龄)分层
（回填 ab_summary_and_plan.md 的 DKL 占位）。

每期一个北京日窗口（切换点内，保同期洁净）。分组用权威 GROUP_CASE。
玩家分层按「自首发子弹(全历史第一发 bet>0)起算的自然日龄 d」：
  new = 0-3 天 · beginner = 4-7 天 · old = ≥8 天（d<=3→new, d<=7→beginner, else old）。
  日龄按 user-day 计（同一玩家不同天可落入不同层）。

每(期, 层, 组)出：去重用户、num_bet(人均中位+总量)、人均投注(缩99均+中位)、
RTP%(含FG)、人均净中、D1/D3/D7 留存，并给相对同期同层 Default 的差异。

口径依据 memory[ss03-mathtable-report]。结果落地 data/output/ss03_cbo/ + 生成 md 片段。
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs                          # noqa: E402
from jobs.ss03_analysis.ss03_grouping import GROUP_CASE, BASE_FILTER  # noqa: E402

OUT = ROOT / "data" / "output" / "ss03_cbo"
OUT.mkdir(parents=True, exist_ok=True)
FRAG = OUT / "md_fragments.md"
KAK = "AND LOWER(math_table_id) NOT LIKE '%kakutei%'"   # 刨除暗保底 kakuteiA/B/C（整行）

PERIODS = [
    ("P0_一期", "2026-06-15", "2026-07-04", {"Default": "zero", "AB_TEST_A": "95Kai", "AB_TEST_B": "97BG", "AI": "混合"}),
    ("P1",      "2026-07-07", "2026-07-28", {"Default": "95Kai", "AB_TEST_A": "zero", "AB_TEST_B": "93Kai", "AI": "混合"}),
    ("P2_二期", "2026-08-05", "2026-08-17", {"Default": "95Kai", "AB_TEST_A": "shi", "AB_TEST_B": "95bgtr", "AI": "混合"}),
    ("P3_三期", "2026-08-19", "2026-09-01", {"Default": "95Kai", "AB_TEST_A": "97bgtr", "AB_TEST_B": "95bgtr", "AI": "混合"}),
]
GROUPS = ["Default", "AB_TEST_A", "AB_TEST_B", "AI"]
GLABEL = {"Default": "Default", "AB_TEST_A": "TestA", "AB_TEST_B": "TestB", "AI": "AI"}
# 日龄分层：(名, 下界, 上界含, SQL层key)
CLASSES = [("总体", -1, 10**9, "总体"), ("new (0-3天)", 0, 3, "new"),
           ("beginner (4-7天)", 4, 7, "beginner"), ("old (≥8天)", 8, 10**9, "old")]
# 投注额分层（按玩家期内总投注额；阈值取整、跨期通用）
BS_T1, BS_T2 = 200, 2000
BETSIZE = [("小投注额（期内总投注 <200）", "small"), ("中投注额（200–2000）", "mid"), ("大投注额（≥2000）", "large")]
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean() if len(s) else float("nan")
neg = lambda x: f"{x:,.0f}".replace("-", "−")            # 负号统一 U+2212


def pull_userday(be, lo, hi_ret):
    rows = be.execute(f"""
      SELECT ({GROUP_CASE}) grp, user_id, (created_at+interval '8 hours')::date bet_date,
             SUM(CASE WHEN bet_amount>0 THEN 1 ELSE 0 END) num_bet,
             SUM(bet_amount) bet, SUM(actual_payout) payout
      FROM public.fct_bet_orders
      WHERE {BASE_FILTER} {KAK} AND (created_at+interval '8 hours')::date BETWEEN '{lo}' AND '{hi_ret}'
      GROUP BY 1,2,3
    """)
    df = pd.DataFrame(rows, columns=["grp", "user_id", "bet_date", "num_bet", "bet", "payout"])
    df["bet_date"] = pd.to_datetime(df["bet_date"])
    for c in ["num_bet", "bet", "payout"]:
        df[c] = df[c].astype(float)
    return df


def pull_single_bet(be, lo, hi):
    """按(组×日龄层)算单次投注额的 均值 + 中位(行级 PERCENTILE_CONT)。返回 {(grp,cls_key):(mean,med)}。"""
    rows = be.execute(f"""
      WITH fb AS (
        SELECT user_id, MIN((created_at+interval '8 hours')::date) fbd
        FROM public.fct_bet_orders WHERE {BASE_FILTER} {KAK} AND bet_amount>0 GROUP BY 1),
      o AS (
        SELECT ({GROUP_CASE}) grp, user_id, bet_amount,
               (created_at+interval '8 hours')::date bd
        FROM public.fct_bet_orders
        WHERE {BASE_FILTER} {KAK} AND bet_amount>0
          AND (created_at+interval '8 hours')::date BETWEEN '{lo}' AND '{hi}'),
      y AS (
        SELECT o.grp, o.bet_amount,
               CASE WHEN DATEDIFF(day, fb.fbd, o.bd)<=3 THEN 'new'
                    WHEN DATEDIFF(day, fb.fbd, o.bd)<=7 THEN 'beginner' ELSE 'old' END cls
        FROM o JOIN fb ON o.user_id=fb.user_id)
      SELECT grp, cls, AVG(bet_amount), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bet_amount)
      FROM y GROUP BY grp, cls
      UNION ALL
      SELECT grp, '总体', AVG(bet_amount), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bet_amount)
      FROM y GROUP BY grp
    """)
    return {(g, c): (float(m), float(md)) for g, c, m, md in rows}


def pull_single_bet_betsize(be, lo, hi):
    """按(组×投注额层)算单次投注额 均/中。投注额层由玩家期内总投注额定。"""
    rows = be.execute(f"""
      WITH ub AS (
        SELECT user_id, SUM(bet_amount) tot FROM public.fct_bet_orders
        WHERE {BASE_FILTER} {KAK} AND bet_amount>0
          AND (created_at+interval '8 hours')::date BETWEEN '{lo}' AND '{hi}' GROUP BY 1),
      o AS (
        SELECT ({GROUP_CASE}) grp, user_id, bet_amount FROM public.fct_bet_orders
        WHERE {BASE_FILTER} {KAK} AND bet_amount>0
          AND (created_at+interval '8 hours')::date BETWEEN '{lo}' AND '{hi}'),
      y AS (
        SELECT o.grp, o.bet_amount,
               CASE WHEN ub.tot < {BS_T1} THEN 'small' WHEN ub.tot < {BS_T2} THEN 'mid' ELSE 'large' END seg
        FROM o JOIN ub ON o.user_id=ub.user_id)
      SELECT grp, seg, AVG(bet_amount), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bet_amount)
      FROM y GROUP BY grp, seg""")
    return {(g, s): (float(m), float(md)) for g, s, m, md in rows}


def metric_row(sub, ret_fn):
    usr = sub.groupby("user_id").agg(num_bet=("num_bet", "sum"), bet=("bet", "sum"), payout=("payout", "sum"))
    usr["net"] = usr["payout"] - usr["bet"]
    sb, sp = usr["bet"].sum(), usr["payout"].sum()
    return dict(
        uu=len(usr), nb_total=usr["num_bet"].sum(), sum_bet=sb, sum_payout=sp, ggr=sb - sp,  # 汇总额
        rtp=sp / sb * 100 if sb else float("nan"),
        nb_med=usr["num_bet"].median(), nb_mean=usr["num_bet"].mean(), nb_w99=w99(usr["num_bet"]),
        bet_med=usr["bet"].median(), bet_mean=usr["bet"].mean(), bet_w99=w99(usr["bet"]),
        net_med=usr["net"].median(), net_mean=usr["net"].mean(), net_w99=w99(usr["net"]),
        d1=ret_fn(sub, 1), d3=ret_fn(sub, 3), d7=ret_fn(sub, 7))


def md_table(rows, labels):
    """三连值列格式：中/均/均99（均99 = 0-99% 缩尾均值）。汇总额单位=万。"""
    W = lambda v: f"{v/1e4:,.1f}"
    Wsig = lambda v: (f"−{abs(v)/1e4:,.1f}" if v < 0 else f"{v/1e4:,.1f}")   # 万·带负号·1位
    tri = lambda a, b, c, sign=False: (f"{neg(a)}/{neg(b)}/{neg(c)}" if sign else f"{a:.0f}/{b:.0f}/{c:.0f}")
    out = ["| 组（表） | 用户 | 总投注次数(万) | 总投注额(万) | 总payout(万) | 总GGR(万) | RTP% | num_bet 中/均/均99 | 人均投注 中/均/均99 | 单发金额 均/中 | 人均净 中/均/均99 | D1/D3/D7 |",
           "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for g, r in rows:
        out.append(f"| {GLABEL[g]}（{labels.get(g,'')}） | {r['uu']:,} | {W(r['nb_total'])} | {W(r['sum_bet'])} | {W(r['sum_payout'])} | "
                   f"{Wsig(r['ggr'])} | {r['rtp']:.1f} | "
                   f"{tri(r['nb_med'],r['nb_mean'],r['nb_w99'])} | {tri(r['bet_med'],r['bet_mean'],r['bet_w99'])} | "
                   f"{r['sb_mean']:.1f}/{r['sb_med']:.1f} | "
                   f"{tri(r['net_med'],r['net_mean'],r['net_w99'],sign=True)} | {r['d1']:.1f}/{r['d3']:.1f}/{r['d7']:.1f} |")
    return "\n".join(out)


def main():
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    last_bj = (pd.Timestamp(be.execute("SELECT MAX(created_at) FROM public.fct_bet_orders WHERE game_id='SS03'")[0][0])
               + pd.Timedelta(hours=8)).normalize()
    print(f"最后可用北京日: {last_bj.date()}")

    # 全局首发子弹日（全历史 bet>0 的最早北京日）
    fb = be.execute(f"""SELECT user_id, MIN((created_at+interval '8 hours')::date)
                        FROM public.fct_bet_orders WHERE {BASE_FILTER} {KAK} AND bet_amount>0 GROUP BY 1""")
    first_bet = {u: pd.Timestamp(d) for u, d in fb}
    print(f"全局首发日已取：{len(first_bet):,} 玩家\n")

    frag, fragbs = [], []
    for name, lo, hi, labels in PERIODS:
        hi_ret = (pd.Timestamp(hi) + pd.Timedelta(days=8)).strftime("%Y-%m-%d")
        df = pull_userday(be, lo, hi_ret)
        act = {u: set(d) for u, d in df.groupby("user_id")["bet_date"]}
        win = df[(df["bet_date"] >= lo) & (df["bet_date"] <= hi)].copy()
        # 日龄（user-day）
        win["first"] = win["user_id"].map(first_bet)
        win["tenure"] = (win["bet_date"] - win["first"]).dt.days
        win = win[win["tenure"] >= 0]                      # 首发前不应有；防脏

        def ret(sub, N):
            num = den = 0
            for d, g in sub.groupby("bet_date"):
                tgt = d + pd.Timedelta(days=N)
                if tgt > last_bj:
                    continue
                us = g["user_id"].values; den += len(us)
                num += sum(1 for u in us if tgt in act.get(u, ()))
            return num / den * 100 if den else float("nan")

        sb = pull_single_bet(be, lo, hi)                   # 单发金额 均/中（行级）
        frag.append(f"\n<!-- ===== {name} ({lo}~{hi}) ===== -->")
        for cname, lo_d, hi_d, ckey in CLASSES:
            csub = win if cname == "总体" else win[(win["tenure"] >= lo_d) & (win["tenure"] <= hi_d)]
            rows = [(g, metric_row(csub[csub["grp"] == g], ret)) for g in GROUPS if len(csub[csub["grp"] == g])]
            if not rows:
                continue
            for g, r in rows:                              # 注入单发金额
                r["sb_mean"], r["sb_med"] = sb.get((g, ckey), (float("nan"), float("nan")))
            frag.append(f"\n**[{name}] {cname}**\n\n" + md_table(rows, labels))
            # 控制台核对
            print(f"[{name}] {cname:16} " + " | ".join(
                f"{GLABEL[g]}:{r['uu']}u nb{r['nb_med']:.0f} bet{r['bet_med']:.0f} rtp{r['rtp']:.0f} D1{r['d1']:.0f}" for g, r in rows))

        # ---- 大/中/小 投注额玩家分层 ----
        usr_tot = win.groupby("user_id")["bet"].sum()
        seg_of = pd.cut(usr_tot, [-1, BS_T1, BS_T2, 1e18], labels=["small", "mid", "large"])
        seg_users = {s: set(usr_tot.index[seg_of == s]) for s in ["small", "mid", "large"]}
        sbb = pull_single_bet_betsize(be, lo, hi)
        fragbs.append(f"\n<!-- ===== {name} ({lo}~{hi}) 投注额分层 ===== -->")
        for sname, skey in BETSIZE:
            csub = win[win["user_id"].isin(seg_users[skey])]
            rows = [(g, metric_row(csub[csub["grp"] == g], ret)) for g in GROUPS if len(csub[csub["grp"] == g])]
            if not rows:
                continue
            for g, r in rows:
                r["sb_mean"], r["sb_med"] = sbb.get((g, skey), (float("nan"), float("nan")))
            fragbs.append(f"\n**[{name}] {sname}**\n\n" + md_table(rows, labels))
            print(f"[{name}] {sname[:10]:12} " + " | ".join(
                f"{GLABEL[g]}:{r['uu']}u nb{r['nb_med']:.0f} rtp{r['rtp']:.0f} GGR{r['ggr']/1e4:.0f}万 D1{r['d1']:.0f}" for g, r in rows))
        print()
    be.close()
    FRAG.write_text("\n".join(frag), encoding="utf-8")
    (OUT / "md_frag_betsize.md").write_text("\n".join(fragbs), encoding="utf-8")
    print(f"md 片段已写 {FRAG} 和 {OUT/'md_frag_betsize.md'}")


if __name__ == "__main__":
    main()
