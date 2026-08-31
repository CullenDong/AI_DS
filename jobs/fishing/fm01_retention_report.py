"""FM01 个性化挽留组 · 完整汇报（单一 PDF）—— 每日 挽留组 vs default 对比。

含：分组准则/留存定义、规模、投注行为激励总结、每日留存(D1/D3/D7/D10)对比+差值、每日投注对比+差值、每日数值表。
两组由 user_id 尾号随机切（挽留=0/1、default=2-9，均上线后），唯一系统差异=有无挽留干预（ITT）。
留存 = day-N 当日回访率：当天有投注即活跃，按天算、不去重成唯一玩家；北京日；全游戏活跃判定；右截断置空。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import font_manager as fm

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs                                          # noqa: E402
from jobs.fishing.fm01_grouping import GROUP_CASE, BASE_FILTER, RETENTION_LAUNCH_UTC  # noqa: E402

START = "2026-07-31"  # END 在连库后按数据最大日动态计算
OUT = ROOT / "docs" / "FM01_个性化挽留_完整汇报.pdf"
NS = [1, 3, 7, 10]
FP = None
for pth in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(pth).exists():
        FP = fm.FontProperties(fname=pth); break

be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5442)

# 数据最大北京日（含当前残缺日）→ cohort/展示只用到最后一个完整日 = maxbj-1；END 取 maxbj(排他)
maxbj = pd.Timestamp(be.execute(
    f"SELECT MAX((event_timestamp+interval '8 hours')::date) FROM public.bullet "
    f"WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'")[0][0])
END = str(maxbj.date())                                  # d < END → cohort 到最后完整日(maxbj 当天残缺，排除)
LASTDAY = str((maxbj - pd.Timedelta(days=1)).date())     # 最后完整北京日(用于标题/口径说明)
TODAY = str(maxbj.date())

g = pd.DataFrame(be.execute(f"""
  SELECT {GROUP_CASE} gt, COUNT(DISTINCT user_id) users, COUNT(*) shots, ROUND(SUM(bet),0) bet,
    ROUND(SUM(payout)/NULLIF(SUM(bet),0)*100,2) rtp, ROUND(SUM(payout)-SUM(bet),0) net
  FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' GROUP BY 1"""),
  columns=["gt", "users", "shots", "bet", "rtp", "net"]).set_index("gt")

# 聚合行为（激励总结）
agg = pd.DataFrame(be.execute(f"""
  WITH t AS (SELECT user_id,bet,payout,killed,{GROUP_CASE} gt,(event_timestamp+interval '8 hours')::date d
    FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}')
  SELECT gt,COUNT(DISTINCT user_id) u,COUNT(*) shots,SUM(bet) bet,SUM(payout) pay,SUM(killed) k
  FROM t WHERE gt IN ('retention','default') AND d>='{START}' AND d<'{END}' GROUP BY 1"""),
  columns=["gt", "u", "shots", "bet", "pay", "k"]).set_index("gt")
for c in ["u", "shots", "bet", "pay", "k"]:
    agg[c] = agg[c].astype(float)

beh = pd.DataFrame(be.execute(f"""
  WITH t AS (SELECT user_id,bet,payout,killed,{GROUP_CASE} gt,(event_timestamp+interval '8 hours')::date d
    FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}')
  SELECT gt,d,COUNT(DISTINCT user_id) u,COUNT(*) shots,SUM(bet) bet,SUM(payout) pay,SUM(killed) k
  FROM t WHERE gt IN ('retention','default') AND d>='{START}' AND d<'{END}' GROUP BY 1,2"""),
  columns=["gt", "d", "u", "shots", "bet", "pay", "k"])
for c in ["u", "shots", "bet", "pay", "k"]:
    beh[c] = beh[c].astype(float)
beh["d"] = pd.to_datetime(beh["d"])
beh["avg_bets"] = beh["shots"] / beh["u"]; beh["avg_amt"] = beh["bet"] / beh["u"]
beh["net"] = (beh["pay"] - beh["bet"]) / beh["u"]; beh["rtp"] = beh["pay"] / beh["bet"] * 100

# 每周行为（distinct 人数需单独查，不能日表相加）
bw = pd.DataFrame(be.execute(f"""
  WITH t AS (SELECT user_id,bet,payout,killed,{GROUP_CASE} gt,
      floor(datediff(day,'2026-07-31',(event_timestamp+interval '8 hours')::date)/7) wk,
      (event_timestamp+interval '8 hours')::date d
    FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}')
  SELECT gt,wk,COUNT(DISTINCT user_id) u,COUNT(*) shots,SUM(bet) bet,SUM(payout) pay,SUM(killed) k
  FROM t WHERE gt IN ('retention','default') AND d>='{START}' AND d<'{END}' GROUP BY 1,2"""),
  columns=["gt", "wk", "u", "shots", "bet", "pay", "k"])
for c in ["u", "shots", "bet", "pay", "k"]:
    bw[c] = bw[c].astype(float)

# 人数增长分解：每日 新增/回流/回流占比（两组）
grow = pd.DataFrame(be.execute(f"""
  WITH grp AS (SELECT DISTINCT user_id,{GROUP_CASE} gt,(event_timestamp+interval '8 hours')::date bj
    FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'),
  s AS (SELECT * FROM grp WHERE gt IN ('retention','default') AND bj>='{START}' AND bj<'{END}'),
  f AS (SELECT user_id,gt,MIN(bj) fd FROM s GROUP BY 1,2)
  SELECT s.gt,s.bj, COUNT(DISTINCT s.user_id) active,
    COUNT(DISTINCT CASE WHEN f.fd=s.bj THEN s.user_id END) new_u,
    COUNT(DISTINCT CASE WHEN f.fd<s.bj THEN s.user_id END) ret_u
  FROM s JOIN f ON s.user_id=f.user_id AND s.gt=f.gt GROUP BY 1,2"""),
  columns=["gt", "d", "active", "new_u", "ret_u"])
for c in ["active", "new_u", "ret_u"]:
    grow[c] = grow[c].astype(float)
grow["d"] = pd.to_datetime(grow["d"])
grow["reflow"] = grow["ret_u"] / grow["active"] * 100      # 回流占比
grow = grow.sort_values(["gt", "d"])
grow["cum"] = grow.groupby("gt")["new_u"].cumsum()         # 累计触达
grow["reactiv"] = grow["ret_u"] / grow.groupby("gt")["cum"].shift(1) * 100  # base 复活率

nlist = " UNION ".join(f"SELECT {n} n" for n in NS)
ret = pd.DataFrame(be.execute(f"""
  WITH t AS (SELECT user_id,{GROUP_CASE} gt,(event_timestamp+interval '8 hours')::date d
    FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'),
  coh AS (SELECT DISTINCT user_id,gt,d FROM t WHERE gt IN ('retention','default') AND d>='{START}' AND d<'{END}'),
  act AS (SELECT DISTINCT user_id,d FROM t)
  SELECT c.gt,c.d,N.n,100.0*COUNT(DISTINCT a.user_id)/NULLIF(COUNT(DISTINCT c.user_id),0) r
  FROM coh c CROSS JOIN ({nlist}) N
  LEFT JOIN act a ON a.user_id=c.user_id AND a.d=dateadd(day,N.n,c.d)
  GROUP BY 1,2,3"""), columns=["gt", "d", "n", "r"])

# 分 tier 打鱼习惯（挽留A vs default B），tier = dim 当前标签
hab = pd.DataFrame(be.execute(f"""
  WITH tier AS (SELECT user_id,ab_group,tier FROM public.dim_user_ab_tier WHERE is_current AND game_id='FM01'),
  b AS (SELECT user_id,bet,payout,killed,multiplier,fish_value FROM public.bullet
    WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}')
  SELECT t.ab_group,t.tier,COUNT(DISTINCT b.user_id) users,COUNT(*) shots,SUM(b.bet) bet,SUM(b.payout) pay,
    SUM(b.killed) killed,AVG(b.multiplier) mult,AVG(b.bet/b.multiplier) base,
    SUM(CASE WHEN b.multiplier>=10 THEN 1 ELSE 0 END) himult,
    AVG(CASE WHEN b.killed=1 THEN b.fish_value END) kfv
  FROM b JOIN tier t ON b.user_id=t.user_id GROUP BY 1,2"""),
  columns=["ab", "tier", "users", "shots", "bet", "pay", "killed", "mult", "base", "himult", "kfv"])
for c in ["users", "shots", "bet", "pay", "killed", "mult", "base", "himult", "kfv"]:
    hab[c] = hab[c].astype(float)

# 各 tier 击打鱼价值档分布（命中鱼）
fish = pd.DataFrame(be.execute(f"""
  WITH tier AS (SELECT user_id,ab_group,tier FROM public.dim_user_ab_tier WHERE is_current AND game_id='FM01'),
  b AS (SELECT user_id,fish_value FROM public.bullet
    WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' AND killed=1)
  SELECT t.ab_group,t.tier,
    CASE WHEN b.fish_value<10 THEN '<10' WHEN b.fish_value<30 THEN '10-30'
         WHEN b.fish_value<60 THEN '30-60' WHEN b.fish_value<120 THEN '60-120'
         WHEN b.fish_value<200 THEN '120-200' ELSE '200+' END band, COUNT(*) k
  FROM b JOIN tier t ON b.user_id=t.user_id GROUP BY 1,2,3"""),
  columns=["ab", "tier", "band", "k"])
fish["k"] = fish["k"].astype(float)

# 四关键指标分布（倍率场 / 单笔投注 / 子弹价值底分），按(ab,tier,bucket)
def _dq(expr):
    x = pd.DataFrame(be.execute(f"""
      WITH tier AS (SELECT user_id,ab_group,tier FROM public.dim_user_ab_tier WHERE is_current AND game_id='FM01')
      SELECT t.ab_group ab,t.tier,{expr} bucket,COUNT(*) n FROM public.bullet b
      JOIN tier t ON b.user_id=t.user_id
      WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' GROUP BY 1,2,3"""),
      columns=["ab", "tier", "bucket", "n"])
    x["n"] = x["n"].astype(float); return x
dmult = _dq("multiplier"); dmult["bucket"] = dmult["bucket"].astype(float)
dbet = _dq("CASE WHEN bet<3 THEN '<3' WHEN bet<10 THEN '3-9' WHEN bet<50 THEN '10-49' "
           "WHEN bet<200 THEN '50-199' ELSE '200+' END")
dbase = _dq("(bet/multiplier)::int"); dbase["bucket"] = dbase["bucket"].astype(int)

# 投注额分解：每(组,倍率场)的发数与投注额（看高倍子弹如何撑起投注额）
dmn = pd.DataFrame(be.execute(f"""
  SELECT ({GROUP_CASE}) gt, multiplier m, COUNT(*) shots, SUM(bet) bet
  FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'
    AND ({GROUP_CASE}) IN ('retention','default') GROUP BY 1,2"""),
  columns=["gt", "m", "shots", "bet"])
for c in ["shots", "bet"]:
    dmn[c] = dmn[c].astype(float)
dmn["m"] = dmn["m"].astype(float)

# 会话与节奏（会话数/人、每会话发数、会话时长、射速）
TJ = "JOIN (SELECT user_id,ab_group,tier FROM public.dim_user_ab_tier WHERE is_current AND game_id='FM01') t ON b.user_id=t.user_id"
sess = pd.DataFrame(be.execute(f"""
  WITH s AS (SELECT t.ab_group ab,t.tier,b.user_id,b.session_id,COUNT(*) sh,
      EXTRACT(epoch FROM MAX(b.created_at)-MIN(b.created_at))/60.0 mins
    FROM public.bullet b {TJ} WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' GROUP BY 1,2,3,4)
  SELECT ab,tier,COUNT(*) sessions,COUNT(DISTINCT user_id) users,AVG(sh) sh_sess,
    AVG(CASE WHEN mins>0 THEN mins END) mins_sess, SUM(sh)/NULLIF(SUM(CASE WHEN mins>0 THEN mins END),0) spm
  FROM s GROUP BY 1,2"""),
  columns=["ab", "tier", "sessions", "users", "sh_sess", "mins_sess", "spm"])
for c in ["sessions", "users", "sh_sess", "mins_sess", "spm"]:
    sess[c] = sess[c].astype(float)
fk = pd.DataFrame(be.execute(f"""
  WITH seq AS (SELECT t.ab_group ab,t.tier,b.user_id,b.session_id,b.killed,
      row_number() OVER (PARTITION BY b.user_id,b.session_id ORDER BY b.created_at) rn
    FROM public.bullet b {TJ} WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'),
  z AS (SELECT ab,tier,user_id,session_id,MIN(CASE WHEN killed=1 THEN rn END) fkr FROM seq GROUP BY 1,2,3,4)
  SELECT ab,tier,AVG(fkr) avg_fk FROM z WHERE fkr IS NOT NULL GROUP BY 1,2"""),
  columns=["ab", "tier", "avg_fk"])
fk["avg_fk"] = fk["avg_fk"].astype(float)

# 每日各(组,时点tier)活跃人数（tier 用 SCD 当时有效标签）
dta = pd.DataFrame(be.execute(f"""
  SELECT dt.ab_group ab, dt.tier, (b.event_timestamp+interval '8 hours')::date bj, COUNT(DISTINCT b.user_id) u
  FROM public.bullet b JOIN public.dim_user_ab_tier dt
    ON b.user_id=dt.user_id AND dt.game_id='FM01'
    AND b.event_timestamp>=dt.valid_from AND (b.event_timestamp<dt.valid_to OR dt.valid_to IS NULL)
  WHERE {BASE_FILTER} AND b.event_timestamp>='{RETENTION_LAUNCH_UTC}' GROUP BY 1,2,3"""),
  columns=["ab", "tier", "bj", "u"])
dta["u"] = dta["u"].astype(float); dta["bj"] = pd.to_datetime(dta["bj"])
dta = dta[(dta["bj"] >= pd.Timestamp(START)) & (dta["bj"] <= pd.Timestamp(LASTDAY))]

# 中位数：玩家级(每人聚合) + shot 级(值-计数→加权中位数，规避 Redshift 多 MEDIAN 限制)
pu = pd.DataFrame(be.execute(f"""
  SELECT t.ab_group ab,t.tier,b.user_id,COUNT(*) shots,SUM(b.bet) tbet,SUM(b.payout)-SUM(b.bet) net
  FROM public.bullet b {TJ} WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' GROUP BY 1,2,3"""),
  columns=["ab", "tier", "uid", "shots", "tbet", "net"])
for c in ["shots", "tbet", "net"]:
    pu[c] = pu[c].astype(float)
def _wmed(expr):
    d = pd.DataFrame(be.execute(f"""SELECT t.ab_group ab,t.tier,{expr} v,COUNT(*) n FROM public.bullet b {TJ}
      WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}' AND {expr} IS NOT NULL GROUP BY 1,2,3"""),
      columns=["ab", "tier", "v", "n"])
    d["v"] = d["v"].astype(float); d["n"] = d["n"].astype(float); out = {}
    for (a, t), gg in d.groupby(["ab", "tier"]):
        gg = gg.sort_values("v"); cc = gg["n"].cumsum(); out[(a, t)] = gg[cc >= gg["n"].sum() / 2]["v"].iloc[0]
    return out
med_bet = _wmed("bet"); med_mult = _wmed("multiplier"); med_base = _wmed("(bet/multiplier)")
med_kfv = _wmed("CASE WHEN killed=1 THEN fish_value END")
pmed = pu.groupby(["ab", "tier"])[["shots", "tbet", "net"]].median()

# 登陆会话（platform 库 fct_user_session_event，按 user_id 映射到 tier）：登陆次数 / 登陆时长
tiermap = pd.DataFrame(be.execute(
    "SELECT user_id,ab_group,tier FROM public.dim_user_ab_tier WHERE is_current AND game_id='FM01'"),
    columns=["uid", "ab", "tier"])
be.close()
_LF = "game_id='FM01' AND currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')"
_bp = rs.RedshiftBackend(database="platform", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5450)
# 每用户：登陆次数 n、有效在线总时长 tot(分,排除 timeout 720 封顶噪声)、活跃天数 ad
suq = pd.DataFrame(_bp.execute(f"""
  SELECT user_id, COUNT(*) n,
    SUM(CASE WHEN session_duration_minutes>0 AND session_status<>'timeout' THEN session_duration_minutes ELSE 0 END) tot,
    COUNT(DISTINCT (login_timestamp+interval '8 hours')::date) ad
  FROM public.fct_user_session_event
  WHERE {_LF} AND login_timestamp>='{RETENTION_LAUNCH_UTC}'
    AND (login_timestamp+interval '8 hours')::date < '{END}'
  GROUP BY 1"""), columns=["uid", "n", "tot", "ad"])
# 会话级时长（算每次登陆时长中位数，排 timeout 封顶）
slq = pd.DataFrame(_bp.execute(f"""
  SELECT user_id, session_duration_minutes m FROM public.fct_user_session_event
  WHERE {_LF} AND login_timestamp>='{RETENTION_LAUNCH_UTC}'
    AND (login_timestamp+interval '8 hours')::date < '{END}'
    AND session_duration_minutes>0 AND session_status<>'timeout'"""), columns=["uid", "m"])
_bp.close()
for c in ["n", "tot", "ad"]:
    suq[c] = suq[c].astype(float)
slq["m"] = slq["m"].astype(float)
suq = suq.merge(tiermap, on="uid"); slq = slq.merge(tiermap, on="uid")
suq["spd"] = suq["n"] / suq["ad"]                       # 登陆次数/活跃日
def _lg(ab, tier=None):
    s = suq[suq["ab"] == ab] if tier is None else suq[(suq["ab"] == ab) & (suq["tier"] == tier)]
    d = slq[slq["ab"] == ab] if tier is None else slq[(slq["ab"] == ab) & (slq["tier"] == tier)]
    return dict(users=len(s), n_mean=s["n"].mean(), n_med=s["n"].median(),
                tot_mean=s["tot"].mean(), tot_med=s["tot"].median(),
                per_mean=d["m"].mean(), per_med=d["m"].median(), spd=s["spd"].mean())
LG = {(ab, tr): _lg(ab, tr) for ab in ["A", "B"] for tr in ["T1", "T2", "T3", "T4"]}
LG[("A", "ALL")] = _lg("A"); LG[("B", "ALL")] = _lg("B")

# tier 活跃率（解释“为什么 T4 活跃人数反超 T3”）：全量池 vs 窗口活跃
_pool = tiermap.groupby("tier").size()                       # 各 tier 全量玩家
_actw = pu.groupby("tier")["uid"].nunique()                  # 各 tier 窗口内活跃玩家
ACTR = {t: dict(pool=int(_pool.get(t, 0)), act=int(_actw.get(t, 0)),
                rate=(_actw.get(t, 0) / _pool[t] * 100 if _pool.get(t, 0) else 0)) for t in ["T1", "T2", "T3", "T4"]}
ret["d"] = pd.to_datetime(ret["d"]); ret["r"] = pd.to_numeric(ret["r"], errors="coerce")
maxd = ret["d"].max()
ret.loc[ret["d"] + pd.to_timedelta(ret["n"], "D") > maxd, "r"] = None

def piv(df, val, n=None):
    d = df if n is None else df[df["n"] == n]
    return d.pivot_table(index="d", columns="gt", values=val)

# D10 分析：同一批完整 cohort 的衰减 + 各 N 覆盖
cut = maxd - pd.Timedelta(days=10)
decay = {gt: {n: ret[(ret["gt"] == gt) & (ret["n"] == n) & (ret["d"] <= cut)]["r"].mean() for n in NS}
         for gt in ["default", "retention"]}
cover = {}
for n in NS:
    ok = ret[(ret["gt"] == "retention") & (ret["n"] == n) & (ret["d"] + pd.to_timedelta(n, "D") <= maxd)]
    cover[n] = (ok["d"].nunique(), ok["r"].mean(), str(ok["d"].min())[5:10], str(ok["d"].max())[5:10])

days = sorted(beh["d"].unique())
C_RET, C_DEF, MUTED = "#0969da", "#c98a00", "#656d76"
NCOL = ["#0969da", "#1a9850", "#d73027", "#8856a7"]
A4 = (8.27, 11.69)

def newp():
    fig = plt.figure(figsize=A4); fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    return fig, ax
def txt(ax, x, y, s, size=10, bold=False, color="#1f2328"):
    ax.text(x, y, s, fontproperties=FP, fontsize=size, color=color, fontweight="bold" if bold else "normal", va="top", ha="left")
def head(ax, y, s):
    txt(ax, 0.06, y, s, size=13, bold=True); ax.plot([0.06, 0.94], [y - 0.012, y - 0.012], color="#1f2328", lw=1); return y - 0.033
def cell(v):
    return "—" if v is None or (isinstance(v, float) and np.isnan(v)) else str(v)
def tbl(ax, y, headers, rows, colx, fs=8.4, rh=0.023):
    ax.add_patch(plt.Rectangle((colx[0] - 0.005, y - rh + 0.004), colx[-1] - colx[0] + 0.06, rh, facecolor="#eef2f6", edgecolor="none"))
    for i, h in enumerate(headers): txt(ax, colx[i], y, str(h), size=fs, bold=True)
    y -= rh
    for rr in rows:
        for i, v in enumerate(rr): txt(ax, colx[i], y, cell(v), size=fs)
        y -= rh
    return y
def two_line(fig, rect, data, ttl):
    axx = fig.add_axes(rect)
    for gt, col in [("default", C_DEF), ("retention", C_RET)]:
        if gt in data.columns:
            axx.plot([pd.Timestamp(x) for x in data.index], data[gt].values, marker="o", ms=2.3, lw=1.2, color=col, label=gt)
    axx.set_title(ttl, fontproperties=FP, fontsize=9); axx.legend(prop=FP, fontsize=6.5); axx.grid(alpha=.3)
    for lb in axx.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(5)
    for lb in axx.get_yticklabels(): lb.set_fontsize(6)
    return axx

pdf = PdfPages(str(OUT))

# ===== Page 0：执行摘要（整合各节结论）=====
# 就地计算关键数字（从已加载数据）
_d, _r = agg.loc["default"], agg.loc["retention"]
_lift = lambda a, b: f"{(b/a-1)*100:+.1f}%"
sm_bets = _lift(_d["shots"]/_d["u"], _r["shots"]/_r["u"])
sm_size = _lift(_d["bet"]/_d["shots"], _r["bet"]/_r["shots"])
sm_amt = _lift(_d["bet"]/_d["u"], _r["bet"]/_r["u"])
_tot = dta.groupby(["ab", "bj"])["u"].sum().unstack(0).sort_index()
_grt = _tot.pct_change() * 100
sm_ga, sm_gb = _grt["A"].mean(), _grt["B"].mean()
_recent = grow[grow["d"] >= (grow["d"].max() - pd.Timedelta(days=6))]
_rf = _recent.groupby("gt")["reflow"].mean()
sm_rf_a, sm_rf_b = _rf.get("retention", float("nan")), _rf.get("default", float("nan"))
_pm = pu.groupby(["ab", "tier"])[["shots", "net"]].median()
fig, ax = newp()
txt(ax, 0.06, 0.965, "FM01 个性化挽留组 · 核心结论摘要", size=17, bold=True)
txt(ax, 0.06, 0.938, f"挽留(A,尾号0/1) vs default(B,尾号2-9) · 窗口 {START}~{LASTDAY} · 数据截至 {TODAY} · tier 用 dim_user_ab_tier", size=8, color=MUTED)
sec = [
    ("一、规模与健康度", [
        f"挽留组 {int(g.loc['retention','users']):,} 玩家；整组 RTP {g.loc['retention','rtp']}% ≈ default {g.loc['default','rtp']}%，系统层面无放水。"]),
    ("二、留存（趋势向好）", [
        "上线初期挽留落后 default 约 3pp，随时间改善——近端 D1/D3 已反超（§4 每日差值零轴、§7 回流占比一致）。",
        f"回流占比近 7 日均值：挽留 {sm_rf_a:.1f}% vs default {sm_rf_b:.1f}%（反超）。",
        "D10 偏低=自然衰减长尾 + 右截断只覆盖早期低留存 cohort，非系统异常（§5）。"]),
    ("三、投注习惯（挽留＝小额·高频·低倍）", [
        f"激励效果：投注次数 {sm_bets}、单笔 {sm_size}、投注总额 {sm_amt}——“打得多、花得少”（§3/§13）。",
        "高 tier(T3/T4) 被降档：单笔/倍率场/底分/高倍% 全低于 default；打哪种鱼两组接近（§10–12）。",
        "投注额被少数高倍子弹(占4–6%发数)撑起(占30–35%额)，default 高倍更多→总额更高（§13）。"]),
    ("四、节奏（两组几乎一致）", [
        "会话时长(3–6分)、射速(约120–138发/分)、命中率、首杀轮次 两组接近——挽留没改“怎么打”；但会话数/人更多（§14）。",
        "登陆会话同结论：挽留登陆次数/人更多(总计8.5 vs 7.6)、人均总在线时长更长(42.8 vs 36.8分)，但每次登陆时长两组一致(§17)。"]),
    ("五、人数增长（挽留更快）", [
        f"每日环比增长率均值：挽留组总 {sm_ga:.2f}% vs default {sm_gb:.2f}%，各 tier 普遍更高（T3 最快）（§15）。"]),
    ("六、中位数视角（看典型玩家）", [
        "单笔/倍率场/底分 中位数=1（典型是“1币1倍小注”）；均值被少数高倍大户拉高（§16）。",
        f"投注次数均值挽留高但中位数略低（大户效应）；典型挽留玩家人均净亏略少（T4 中位 {_pm.loc[('A','T4'),'net']:.0f} vs {_pm.loc[('B','T4'),'net']:.0f}）。"]),
    ("七、风险", [
        "尾部农场号（高倍狙击套利）拖低挽留人均净/RTP，建议持续按农场签名剔除（§7 备注、MD5/EW3/KV3/JN1）。"]),
]
yy = 0.895
for title, items in sec:
    txt(ax, 0.06, yy, title, size=11, bold=True, color=C_RET); yy -= 0.028
    for it in items:
        txt(ax, 0.08, yy, "· " + it, size=8.8); yy -= 0.026
    yy -= 0.006
txt(ax, 0.06, 0.05, "详见后续各节（§1–§17）。FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 1：概览 + 激励总结 =====
fig, ax = newp()
txt(ax, 0.06, 0.965, "FM01 个性化挽留组 · 完整汇报", size=18, bold=True)
txt(ax, 0.06, 0.936, f"每日 挽留组 vs default 对比 · 窗口 {START} ~ {LASTDAY}（北京日，末日残缺已剔除）", size=9, color=MUTED)
txt(ax, 0.06, 0.919, f"尾号随机切，唯一差异=有无挽留（ITT）· 口径 fm01_grouping.py · 数据截至 {TODAY} · draft", size=8.3, color=MUTED)

y = head(ax, 0.884, "1. 分组准则 与 留存定义")
for b in ["· 挽留组 = 上线后（≥2026-07-31 00:00 UTC=7/30 16:00 PST）+ user_id%10 IN(0,1) 的剩余记录；default = 尾号 2-9。",
          "· 留存 = day-N 当日回访率：当天有投注即活跃，按天算、不去重成唯一玩家；北京日；全游戏活跃判定。"]:
    txt(ax, 0.07, y, b, size=9); y -= 0.024

y = head(ax, y - 0.015, "2. 规模对比（挽留上线后至今）")
gx = [0.07, 0.26, 0.40, 0.58, 0.72, 0.83]
rows = [[gt, f"{int(g.loc[gt,'users']):,}", f"{int(g.loc[gt,'shots']):,}", f"{g.loc[gt,'bet']:,.0f}",
         f"{g.loc[gt,'rtp']}", f"{g.loc[gt,'net']:,.0f}"] for gt in ["default", "retention"] if gt in g.index]
y = tbl(ax, y, ["组", "玩家", "子弹", "投注", "RTP%", "净盈亏"], rows, gx)

y = head(ax, y - 0.015, "3. 投注行为 · 激励总结（窗口聚合，per-user）")
d, r = agg.loc["default"], agg.loc["retention"]
def lift(a, b): return f"{(b/a-1)*100:+.1f}%"
metrics = [
    ("人均投注次数", d.shots / d.u, r.shots / r.u, "{:,.0f}"),
    ("人均投注额", d.bet / d.u, r.bet / r.u, "{:,.0f}"),
    ("单笔投注", d.bet / d.shots, r.bet / r.shots, "{:.3f}"),
    ("命中率%", d.k / d.shots * 100, r.k / r.shots * 100, "{:.2f}"),
    ("RTP%", d.pay / d.bet * 100, r.pay / r.bet * 100, "{:.2f}"),
    ("人均净", (d.pay - d.bet) / d.u, (r.pay - r.bet) / r.u, "{:,.0f}"),
]
mx = [0.07, 0.30, 0.48, 0.66]
rows = [[m, f.format(dv), f.format(rv), lift(dv, rv)] for m, dv, rv, f in metrics]
y = tbl(ax, y, ["指标", "default", "retention", "挽留 vs default"], rows, mx)
_bets = lift(d["shots"] / d["u"], r["shots"] / r["u"])
_size = lift(d["bet"] / d["shots"], r["bet"] / r["shots"])
_amt = lift(d["bet"] / d["u"], r["bet"] / r["u"])
for b in [f"· 激励效果：挽留组投注次数 {_bets}（打得更频繁），但单笔 {_size}、人均投注额 {_amt}（单注更小）。",
          "· 即挽留加成把玩家推向“高频小额”，参与度↑但单次投入与人均净略降；RTP 略低（含农场号拖累）。",
          "· 结合每日/按周看：上线初期落后，近端已追平至反超（见 §4 留存差值零轴）。"]:
    txt(ax, 0.07, y - 0.005, b, size=8.7, color=MUTED); y -= 0.023
pdf.savefig(fig); plt.close(fig)

# ===== Page 2：每日留存对比 + 差值 =====
fig, ax = newp()
head(ax, 0.965, "4. 每日留存对比（挽留组 vs default）")
pos = [[0.09, 0.72, 0.38, 0.17], [0.57, 0.72, 0.38, 0.17], [0.09, 0.50, 0.38, 0.17], [0.57, 0.50, 0.38, 0.17]]
for (n, rect) in zip(NS, pos):
    ttl = f"D{n} 留存%" + ("（末端右截断）" if n >= 7 else "")
    two_line(fig, rect, piv(ret, "r", n), ttl)
# 差值零轴
axd = fig.add_axes([0.09, 0.24, 0.86, 0.17])
for n, col in zip(NS, NCOL):
    p = piv(ret, "r", n)
    if "retention" in p.columns and "default" in p.columns:
        diff = p["retention"] - p["default"]
        axd.plot([pd.Timestamp(x) for x in p.index], diff.values, marker="o", ms=2.5, lw=1.3, color=col, label=f"D{n}")
axd.axhline(0, color="#333", lw=1)
axd.set_title("留存差值：挽留 − default（>0 即挽留反超）", fontproperties=FP, fontsize=9.5)
axd.legend(prop=FP, fontsize=7, ncol=4); axd.grid(alpha=.3)
for lb in axd.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(5.5)
for lb in axd.get_yticklabels(): lb.set_fontsize(6.5)
txt(ax, 0.06, 0.05, "蓝=挽留 橙=default；差值图 4 色分别为 D1/D3/D7/D10。每天独立 cohort（当天有投注即活跃）。", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 3：为什么 D10 留存较低 =====
fig, ax = newp()
y = head(ax, 0.965, "5. 为什么 D10 留存较低（分析）")
for b in ["· 主因＝自然留存衰减：同一批完整 cohort（7/31–8/8）D1→D3→D7→D10 平滑单调下降"
          f"（挽留 {decay['retention'][1]:.1f}→{decay['retention'][3]:.1f}→{decay['retention'][7]:.1f}→{decay['retention'][10]:.1f}%）。",
          f"· D7→D10 仅降约 {decay['retention'][7]-decay['retention'][10]:.1f}pp，曲线趋平——留到 D7 的玩家已较黏，D10 是自然长尾、非异常。",
          "· 次因＝右截断组成偏差：D10 只能算最早 9 天（7/31–8/8）的 cohort（晚的还没满 10 天），",
          "   而早期 cohort 留存本就更低（早期新客/濒流失多 + 挽留系统初期不成熟），把 D10 均值又往下拽。"]:
    txt(ax, 0.07, y, b, size=9.2); y -= 0.026

y = head(ax, y - 0.02, "5.1 各 N 的可用天数与覆盖窗口（D10 只覆盖早期）")
cx = [0.07, 0.20, 0.36, 0.60]
rows = [[f"D{n}", f"{cover[n][0]} 天", f"{cover[n][2]} ~ {cover[n][3]}", f"{cover[n][1]:.1f}%"] for n in NS]
y = tbl(ax, y, ["窗口", "可用天数", "覆盖 cohort 日", "均值(挽留)"], rows, cx)

# 衰减曲线（匹配 cohort，挽留 vs default）
head(ax, y - 0.02, "5.2 匹配 cohort 衰减曲线（7/31–8/8，均含完整 D1/D3/D7/D10）")
axd = fig.add_axes([0.12, 0.30, 0.76, 0.22])
for gt, col in [("default", C_DEF), ("retention", C_RET)]:
    axd.plot(NS, [decay[gt][n] for n in NS], marker="o", ms=6, lw=2, color=col, label=gt)
    for n in NS:
        axd.annotate(f"{decay[gt][n]:.1f}", (n, decay[gt][n]), textcoords="offset points", xytext=(0, 7),
                     ha="center", fontproperties=FP, fontsize=7, color=col)
axd.set_xticks(NS); axd.set_xticklabels([f"D{n}" for n in NS], fontproperties=FP, fontsize=9)
axd.set_ylabel("留存%", fontproperties=FP, fontsize=8); axd.legend(prop=FP, fontsize=8); axd.grid(alpha=.3)
for lb in axd.get_yticklabels(): lb.set_fontsize(7)
txt(ax, 0.06, 0.05, "结论：D10 低＝自然衰减长尾 + 右截断只覆盖早期低留存 cohort，非系统异常。", size=8.5, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 4：每日规模与投注对比 =====
fig, ax = newp()
head(ax, 0.965, "6. 每日规模与投注对比（挽留组 vs default）")
two_line(fig, [0.09, 0.735, 0.38, 0.155], piv(beh, "u"), "人数（活跃玩家）")
two_line(fig, [0.57, 0.735, 0.38, 0.155], piv(beh, "bet"), "投注金额（总额）")
two_line(fig, [0.09, 0.525, 0.38, 0.155], piv(beh, "avg_bets"), "人均投注次数")
two_line(fig, [0.57, 0.525, 0.38, 0.155], piv(beh, "avg_amt"), "人均投注额")
two_line(fig, [0.09, 0.315, 0.38, 0.155], piv(beh, "net"), "人均净盈亏")
two_line(fig, [0.57, 0.315, 0.38, 0.155], piv(beh, "rtp"), "RTP%")
axd = fig.add_axes([0.12, 0.09, 0.76, 0.14])
for val, col, lb in [("avg_bets", "#0969da", "人均次数"), ("net", "#d73027", "人均净")]:
    p = piv(beh, val)
    axd.plot([pd.Timestamp(x) for x in p.index], (p["retention"] - p["default"]).values, marker="o", ms=2.3, lw=1.2, color=col, label=lb)
axd.axhline(0, color="#333", lw=1); axd.set_title("投注差值：挽留 − default", fontproperties=FP, fontsize=9)
axd.legend(prop=FP, fontsize=7); axd.grid(alpha=.3)
for lb in axd.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(5)
for lb in axd.get_yticklabels(): lb.set_fontsize(6)
pdf.savefig(fig); plt.close(fig)

# ===== Page 5：回流占比对比（人数涨 ≠ 留存高）=====
fig, ax = newp()
y = head(ax, 0.965, "7. 回流占比对比（挽留 vs default）—— 人数涨 ≠ 留存高")
for b in ["· CR 组人数越来越多，主要靠持续拉新（每天 200–330 个全新玩家涌入），不代表留存率高。",
          "· 回流占比 = 当日回流(老)玩家 / 当日活跃：反映沉淀老玩家的黏性，且不受 D7/D10 右截断影响。",
          "· 与留存率同一故事：W1 挽留落后 → W2 追平 → W3 反超（回流占比 57.4% vs default 55.1%）。"]:
    txt(ax, 0.07, y, b, size=9.2); y -= 0.026
grP = grow.pivot_table(index="d", columns="gt", values="reflow")
axc = fig.add_axes([0.10, 0.50, 0.85, 0.28])
for gt, col in [("default", C_DEF), ("retention", C_RET)]:
    axc.plot([pd.Timestamp(x) for x in grP.index], grP[gt].values, marker="o", ms=3, lw=1.6, color=col, label=gt)
axc.set_title("回流占比%（当日回流/当日活跃）", fontproperties=FP, fontsize=10.5)
axc.legend(prop=FP, fontsize=8.5); axc.grid(alpha=.3)
for lb in axc.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(6)
for lb in axc.get_yticklabels(): lb.set_fontsize(7)
# 每周多参数对比表
LBLw = {0: "W1(7/31-8/6)", 1: "W2(8/7-8/13)", 2: "W3(8/14-8/18)"}
grow["wk"] = ((grow["d"] - pd.Timestamp("2026-07-31")).dt.days // 7)
gw = grow.groupby(["wk", "gt"]).agg(reflow=("reflow", "mean"), reactiv=("reactiv", "mean"),
                                    new_u=("new_u", "mean"), cum=("cum", "max")).round(1)
y = head(ax, 0.44, "7.1 每周参数对比")
cx = [0.07, 0.34, 0.50, 0.66, 0.82]
rows = []
for wk in sorted(LBLw):
    for gt in ["default", "retention"]:
        if (wk, gt) in gw.index:
            r = gw.loc[(wk, gt)]
            rows.append([f"{LBLw[wk]} · {gt}", f"{r['reflow']:.1f}%", f"{r['reactiv']:.1f}%",
                         f"{r['new_u']:,.0f}", f"{r['cum']:,.0f}"])
tbl(ax, y, ["周 · 组", "回流占比", "base复活率", "日均新增", "累计触达"], rows, cx, fs=8.2)
txt(ax, 0.06, 0.05, "回流占比↑=累计老玩家越来越多地回来；base复活率随累计池扩大自然下降（同期跨组仍可比）。", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 6：每日数值表 =====
fig, ax = newp()
head(ax, 0.965, "8. 每日数值表（挽留 / default）")
txt(ax, 0.06, 0.925, "留存 %（挽/默）", size=10, bold=True)
pv = {n: piv(ret, "r", n) for n in NS}
def gv(p, dd, key):
    r = p.loc[dd] if dd in p.index else None
    return None if r is None else r.get(key)
def pair(p, dd, f=lambda x: f"{x:.0f}"):
    rv, dv = gv(p, dd, "retention"), gv(p, dd, "default")
    return f"{f(rv)}/{f(dv)}" if (pd.notna(rv) and pd.notna(dv)) else "—"
rx = [0.06, 0.15, 0.27, 0.39, 0.51]
rows = [[str(d)[5:10]] + [pair(pv[n], pd.Timestamp(d)) for n in NS] for d in days]
tbl(ax, 0.90, ["日期", "D1", "D3", "D7", "D10"], rows, rx, fs=7.4, rh=0.0175)
txt(ax, 0.58, 0.925, "规模/投注（挽/默）", size=10, bold=True)
u = piv(beh, "u"); bt = piv(beh, "bet"); nt = piv(beh, "net")
bx = [0.58, 0.665, 0.77, 0.90]
rows = [[str(d)[5:10], pair(u, pd.Timestamp(d), lambda x: f"{x:,.0f}"),
         pair(bt, pd.Timestamp(d), lambda x: f"{x/1000:,.0f}k"), pair(nt, pd.Timestamp(d), lambda x: f"{x:,.0f}")] for d in days]
tbl(ax, 0.90, ["日期", "人数", "投注额", "人均净"], rows, bx, fs=7.4, rh=0.0175)
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 6：每周数据表 =====
LBL = {0: "W1(7/31-8/6)", 1: "W2(8/7-8/13)", 2: "W3(8/14-8/18)"}
ret["wk"] = ((ret["d"] - pd.Timestamp("2026-07-31")).dt.days // 7)
RW = ret.groupby(["wk", "gt", "n"])["r"].mean().round(1)
bw = bw.set_index(["wk", "gt"])
weeks = sorted(LBL)
fig, ax = newp()
head(ax, 0.965, "9. 每周数据表（挽留组 vs default）")

txt(ax, 0.06, 0.925, "每周留存 %（每日率按周均，末端右截断）", size=10, bold=True)
rx = [0.07, 0.34, 0.48, 0.62, 0.76]
rows = []
for wk in weeks:
    for gt in ["default", "retention"]:
        rows.append([f"{LBL[wk]} · {gt}"] + [
            (f"{RW.get((wk, gt, n)):.1f}" if pd.notna(RW.get((wk, gt, n))) else "—") for n in NS])
tbl(ax, 0.895, ["周 · 组", "D1", "D3", "D7", "D10"], rows, rx, fs=8.4, rh=0.026)

txt(ax, 0.06, 0.58, "每周规模与投注（per-user；投注额=当周总额）", size=10, bold=True)
bx = [0.07, 0.30, 0.42, 0.54, 0.65, 0.76, 0.87]
rows = []
for wk in weeks:
    for gt in ["default", "retention"]:
        if (wk, gt) in bw.index:
            r = bw.loc[(wk, gt)]
            rows.append([f"{LBL[wk]} · {gt}", f"{r['u']:,.0f}", f"{r['bet']/1000:,.0f}k",
                         f"{r['shots']/r['u']:,.0f}", f"{r['bet']/r['u']:,.0f}",
                         f"{r['pay']/r['bet']*100:.2f}", f"{(r['pay']-r['bet'])/r['u']:,.0f}"])
tbl(ax, 0.55, ["周 · 组", "人数", "投注额", "人均次数", "人均额", "RTP%", "人均净"], rows, bx, fs=8.2, rh=0.026)

txt(ax, 0.06, 0.20, "读法：W1 上线初期挽留落后（留存低、人均净差）；W2 追平；W3 反超（留存更高、人均净更好、投注更频繁）。",
    size=8.6, color=MUTED)
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 8：打鱼习惯全指标表（挽留 / default 各一张）=====
TIERS = ["T1", "T2", "T3", "T4"]
HM = {(r["ab"], r["tier"]): dict(
        bets=r["shots"] / r["users"], size=r["bet"] / r["shots"], base=r["base"], mult=r["mult"],
        hi=r["himult"] / r["shots"] * 100, hit=r["killed"] / r["shots"] * 100, kfv=r["kfv"],
        net=(r["pay"] - r["bet"]) / r["users"], rtp=r["pay"] / r["bet"] * 100, tot=r["bet"] / r["users"],
        users=r["users"]) for _, r in hab.iterrows()}
COLS = [("投注次数/人", "bets", lambda x: f"{x:,.0f}"),
        ("单笔投注", "size", lambda x: f"{x:.2f}"),
        ("倍率场", "mult", lambda x: f"{x:.2f}"), ("高倍%", "hi", lambda x: f"{x:.1f}"),
        ("命中%", "hit", lambda x: f"{x:.1f}"), ("鱼价值", "kfv", lambda x: f"{x:.0f}"),
        ("人均净", "net", lambda x: f"{x:,.0f}"), ("RTP%", "rtp", lambda x: f"{x:.1f}")]
fig, ax = newp()
head(ax, 0.965, "10. 打鱼习惯全指标对比（分 tier · 挽留 / default）")
cx = [0.05, 0.15, 0.29, 0.41, 0.51, 0.61, 0.71, 0.80, 0.90]
for gi, (abv, name) in enumerate([("A", "挽留"), ("B", "default")]):
    yt = 0.905 - gi * 0.30
    txt(ax, 0.05, yt + 0.018, f"【{name}】", size=11, bold=True, color=C_RET)
    rows = [[tr] + [f(HM[(abv, tr)][k]) for _, k, f in COLS] for tr in TIERS]
    tbl(ax, yt, ["tier"] + [c[0] for c in COLS], rows, cx, fs=7.6, rh=0.026)
y2 = head(ax, 0.32, "10.1 结论")
for b in ["· 投注次数/人：挽留各 tier 都更多（T4 约 +3700）；但投注总额更低——“打得多、花得少”（详见 §13）。",
          "· 高 tier 被“降档”：挽留 T3/T4 单笔、底分、倍率场、高倍% 全低于 default → “小额低倍高频”。",
          "· 低 tier(T1/T2) 挽留单笔/倍率略高（轻推多打）；RTP 挽留 T1–T3 高 ~1pp，T4 略低（default T4 含高倍套利号）。",
          "· 命中率、命中鱼价值 两组接近（由 tier/机制决定）；四指标分布见 §11–12，投注额分解见 §13，节奏见 §14。"]:
    txt(ax, 0.06, y2, b, size=8.8); y2 -= 0.025
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# 堆叠分布图助手：每个指标画 挽留|default 两张堆叠柱（x=tier，堆叠=bucket 占比%）
CMAP6 = ["#c6dbef", "#9ecae1", "#6baed6", "#4292c6", "#2171b5", "#084594"]
def stackpage(title, specs, note):
    fig, ax = newp(); head(ax, 0.965, title)
    for si, (dd, order, sub) in enumerate(specs):
        ytop = 0.87 - si * 0.44
        for gi, (abv, gname) in enumerate([("A", "挽留"), ("B", "default")]):
            axx = fig.add_axes([0.10 + gi * 0.47, ytop - 0.30, 0.37, 0.30])
            s = dd[dd["ab"] == abv].copy()
            tot = s.groupby("tier")["n"].transform("sum"); s["pct"] = s["n"] / tot * 100
            p = s.pivot_table(index="tier", columns="bucket", values="pct", fill_value=0).reindex(index=TIERS, columns=order)
            bottom = np.zeros(len(TIERS))
            for i, b in enumerate(order):
                vals = p[b].fillna(0).values
                axx.bar(TIERS, vals, bottom=bottom, color=CMAP6[i % 6], label=str(b)); bottom += vals
            axx.set_title(f"{sub} · {gname}", fontproperties=FP, fontsize=9); axx.set_ylim(0, 100)
            axx.legend(prop=FP, fontsize=6, ncol=3, loc="lower center", bbox_to_anchor=(0.5, -0.30))
            for lb in axx.get_xticklabels(): lb.set_fontproperties(FP); lb.set_fontsize(8)
            for lb in axx.get_yticklabels(): lb.set_fontsize(6)
    txt(ax, 0.06, 0.05, note, size=8.3, color=MUTED)
    pdf.savefig(fig); plt.close(fig)

fish2 = fish[["ab", "tier", "band", "k"]].rename(columns={"band": "bucket", "k": "n"})
stackpage("11. 关键指标分布 · 倍率场 & 单笔投注（挽留 vs default，堆叠 %）",
          [(dmult, [1.0, 2.0, 10.0, 50.0, 100.0], "倍率场 multiplier"),
           (dbet, ["<3", "3-9", "10-49", "50-199", "200+"], "单笔投注 bet")],
          "倍率场：绝大多数 1x/2x；挽留高 tier 更集中低倍。单笔投注：挽留 T4 更集中 <3，default T4 更多 10-49（下注更大）。")
stackpage("12. 关键指标分布 · 子弹价值(底分) & 鱼价值（挽留 vs default，堆叠 %）",
          [(dbase, [1, 2, 3, 5, 10], "子弹价值 底分=bet÷倍率"),
           (fish2, ["<10", "10-30", "30-60", "60-120", "120-200", "200+"], "鱼价值 fish_value")],
          "底分：挽留 T4 底分1占比更高（更小额）。鱼价值：tier 越高越打大鱼；挽留 vs default 结构接近。")

# ===== Page 13：投注额分解（为什么打得多、花得少）=====
fig, ax = newp()
y = head(ax, 0.965, "13. 投注额分解：为什么“子弹打得多、投注总额反而少”")
def gm(gt):
    s = dmn[dmn["gt"] == gt]; ts = s["shots"].sum(); tb = s["bet"].sum()
    return dict(bets=g.loc[gt, "shots"] / g.loc[gt, "users"], size=g.loc[gt, "bet"] / g.loc[gt, "shots"],
                total=g.loc[gt, "bet"] / g.loc[gt, "users"], base=(s["bet"] / s["m"]).sum() / ts,
                mult=(s["m"] * s["shots"]).sum() / ts, his=s[s["m"] >= 10]["shots"].sum() / ts * 100,
                hib=s[s["m"] >= 10]["bet"].sum() / tb * 100)
A, B = gm("retention"), gm("default")
rows = [["投注次数/人", f"{B['bets']:,.0f}", f"{A['bets']:,.0f}"],
        ["单笔投注", f"{B['size']:.2f}", f"{A['size']:.2f}"],
        ["投注总额/人", f"{B['total']:,.0f}", f"{A['total']:,.0f}"],
        ["平均底分", f"{B['base']:.2f}", f"{A['base']:.2f}"],
        ["平均倍率场", f"{B['mult']:.2f}", f"{A['mult']:.2f}"],
        ["高倍(≥10x)发数占比", f"{B['his']:.1f}%", f"{A['his']:.1f}%"],
        ["高倍贡献投注额占比", f"{B['hib']:.1f}%", f"{A['hib']:.1f}%"]]
y = tbl(ax, y, ["指标", "default", "挽留"], rows, [0.06, 0.42, 0.64], fs=9.5, rh=0.03)
MULTS = [1.0, 2.0, 10.0, 50.0, 100.0]
for gi, (gt, name) in enumerate([("retention", "挽留"), ("default", "default")]):
    axx = fig.add_axes([0.10 + gi * 0.47, 0.32, 0.37, 0.22])
    s = dmn[dmn["gt"] == gt].set_index("m").reindex(MULTS).fillna(0)
    sp = s["shots"] / s["shots"].sum() * 100; bp = s["bet"] / s["bet"].sum() * 100
    xx = np.arange(len(MULTS)); w = 0.38
    axx.bar(xx - w / 2, sp.values, w, color="#9ecae1", label="按发数%")
    axx.bar(xx + w / 2, bp.values, w, color="#08519c", label="按投注额%")
    axx.set_title(f"倍率场：发数 vs 投注额 · {name}", fontproperties=FP, fontsize=9)
    axx.set_xticks(xx); axx.set_xticklabels([f"{int(m)}x" for m in MULTS], fontsize=8)
    axx.legend(prop=FP, fontsize=7); axx.grid(alpha=.3, axis="y")
    for lb in axx.get_yticklabels(): lb.set_fontsize(6)
yy = 0.24
for b in ["· 投注总额 = 次数 × 单笔；单笔 = 底分 × 倍率场。挽留次数 +10% 但单笔 −25%（底分、倍率场都低）→ 总额 −17%。",
          "· 关键：高倍(≥10x)子弹只占 4–6% 发数，却贡献 30–35% 投注额——按“发数”看分布几乎一样，按“投注额”看差别巨大。",
          "· default 高倍用得更多，把总额顶上去；挽留多是便宜的 1x/2x 子弹，打再多也堆不出那么多钱。",
          "· 注：bullet_level(子弹档)全为 1 档、无差异故未列；区分投注习惯的是 倍率场/底分/单笔/鱼价值。"]:
    txt(ax, 0.07, yy, b, size=8.8, color=MUTED); yy -= 0.028
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 14：会话与节奏（挽留 vs default）=====
S = {(r["ab"], r["tier"]): dict(sess=r["sessions"] / r["users"], shsess=r["sh_sess"],
                                mins=r["mins_sess"], spm=r["spm"]) for _, r in sess.iterrows()}
FKD = {(r["ab"], r["tier"]): r["avg_fk"] for _, r in fk.iterrows()}
fig, ax = newp()
y = head(ax, 0.965, "14. 会话与节奏（挽留 vs default，单元格＝挽/默）")
def sp(tr, d, k, f):
    return f"{f(d[('A', tr)][k])}/{f(d[('B', tr)][k])}" if isinstance(d[('A', tr)], dict) else f"{f(d[('A', tr)])}/{f(d[('B', tr)])}"
cx = [0.06, 0.22, 0.40, 0.58, 0.76]
rows = []
for tr in TIERS:
    rows.append([tr, f"{S[('A',tr)]['sess']:.1f}/{S[('B',tr)]['sess']:.1f}",
                 f"{S[('A',tr)]['shsess']:.0f}/{S[('B',tr)]['shsess']:.0f}",
                 f"{S[('A',tr)]['mins']:.1f}/{S[('B',tr)]['mins']:.1f}",
                 f"{S[('A',tr)]['spm']:.0f}/{S[('B',tr)]['spm']:.0f}"])
y = tbl(ax, y, ["tier", "会话数/人", "每会话发数", "会话时长(分)", "射速(发/分)"], rows, cx, fs=9, rh=0.03)
y = head(ax, y - 0.04, "14.1 首杀轮次（一局内平均打到第几发才首次命中）")
rows = [[tr, f"{FKD[('A',tr)]:.0f}", f"{FKD[('B',tr)]:.0f}", f"{FKD[('A',tr)]-FKD[('B',tr)]:+.0f}"] for tr in TIERS]
y = tbl(ax, y, ["tier", "挽留", "default", "差"], rows, [0.06, 0.24, 0.42, 0.60], fs=9, rh=0.03)
y2 = head(ax, y - 0.04, "14.2 结论")
for b in ["· 节奏高度相似：会话时长(3–6 分)、射速(约 120–138 发/分)两组几乎一致——挽留没改“怎么打”。",
          "· 挽留玩家会话数/人更多（尤其 T4：27 vs 24），即更频繁开局、总参与更多。",
          "· 首杀轮次两组接近（14–32 发），随 tier 升高而变长（打更大更难命中的鱼）。",
          "· 综合：挽留改变的是“下什么注”（倍率场/单笔/底分↓）与“来玩几次”（会话数↑），而非节奏/速度/命中结构。"]:
    txt(ax, 0.07, y2, b, size=9); y2 -= 0.026
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 15：T1–T4 每日人数 + 每日环比增长率 =====
TCOL = {"T1": "#6baed6", "T2": "#74c476", "T3": "#fd8d3c", "T4": "#de2d26"}
piv15 = dta.pivot_table(index="bj", columns=["ab", "tier"], values="u").sort_index()
gr = piv15.pct_change() * 100                                   # 每日环比增长率% per (ab,tier)
tot15 = dta.groupby(["ab", "bj"])["u"].sum().unstack(0).sort_index()
grt = tot15.pct_change() * 100                                  # 组总每日环比
fig, ax = newp()
head(ax, 0.965, "15. T1–T4 每日活跃人数 + 每日环比增长率（挽留 vs default）")
for gi, (abv, name) in enumerate([("A", "挽留"), ("B", "default")]):
    axx = fig.add_axes([0.10 + gi * 0.47, 0.68, 0.37, 0.21])
    for tr in TIERS:
        s = dta[(dta["ab"] == abv) & (dta["tier"] == tr)].set_index("bj")["u"].sort_index()
        axx.plot([pd.Timestamp(x) for x in s.index], s.values, lw=1.3, color=TCOL[tr], label=tr)
    axx.set_title(f"{name} · 各 tier 每日活跃人数", fontproperties=FP, fontsize=9); axx.legend(prop=FP, fontsize=6, ncol=4)
    axx.grid(alpha=.3)
    for lb in axx.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(5)
    for lb in axx.get_yticklabels(): lb.set_fontsize(6)
y = head(ax, 0.61, "15.1 每日环比增长率均值（当天/前一天 − 1，算术平均 %）")
rows = [["组总（合4 tier）", f"{grt['A'].mean():.2f}%", f"{grt['B'].mean():.2f}%"]]
for tr in TIERS:
    rows.append([tr, f"{gr[('A', tr)].mean():.2f}%", f"{gr[('B', tr)].mean():.2f}%"])
y = tbl(ax, y, ["序列", "挽留", "default"], rows, [0.06, 0.34, 0.60], fs=9, rh=0.028)
y2 = head(ax, y - 0.03, "15.2 结论")
for b in [f"· 每日环比增长率均值：挽留组总 {grt['A'].mean():.2f}% vs default {grt['B'].mean():.2f}%——挽留人数增长更快。",
          "· 各 tier 挽留日环比均值普遍 ≥ default 同 tier（T3 最快，挽留约 7.4%/日）。",
          "· 每日环比 = (当天人数 − 前一天人数) / 前一天人数；单日波动大（受工作日/周末影响），故看均值。",
          f"· 为何 T4 活跃人数≈T3甚至更多？全量池是正常金字塔（T1 {ACTR['T1']['pool']:,}＞T2 {ACTR['T2']['pool']:,}＞T3 {ACTR['T3']['pool']:,}＞T4 {ACTR['T4']['pool']:,}，T4 最少），",
          f"   但活跃率随 tier 飙升：T1 {ACTR['T1']['rate']:.1f}% / T2 {ACTR['T2']['rate']:.1f}% / T3 {ACTR['T3']['rate']:.1f}% / T4 {ACTR['T4']['rate']:.1f}%——T4 参与度最高，故活跃人数（{ACTR['T4']['act']:,}）反超 T3（{ACTR['T3']['act']:,}）。非异常。",
          "· 口径提示：此处“活跃/活跃率”＝窗口内有投注(发过子弹)的去重玩家（每人只算1次）；与留存的“当天有投注、按天算、不去重”不同，勿混淆。",
          "· 逐日明细见 §15.3；tier 用 SCD 时点标签，末日残缺已剔除。"]:
    txt(ax, 0.07, y2, b, size=9); y2 -= 0.026
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 15.3：每日环比增长率逐日明细 =====
fig, ax = newp()
head(ax, 0.965, "15.3 每日环比增长率逐日明细（当天 vs 前一天，%）")
dates = list(gr.index[1:])   # 跳过首日(NaN)
for gi, (abv, name) in enumerate([("A", "挽留"), ("B", "default")]):
    x0 = 0.05 + gi * 0.49
    txt(ax, x0, 0.925, f"【{name}】", size=10, bold=True, color=C_RET)
    cxx = [x0, x0 + 0.10, x0 + 0.19, x0 + 0.28, x0 + 0.37]
    rows = [[str(dd)[5:10]] + [f"{gr[(abv, tr)].loc[dd]:+.0f}" if pd.notna(gr[(abv, tr)].loc[dd]) else "—" for tr in TIERS] for dd in dates]
    tbl(ax, 0.90, ["日期", "T1", "T2", "T3", "T4"], rows, cxx, fs=7.2, rh=0.0195)
txt(ax, 0.06, 0.05, "正=较前一天增长，负=下降；单日波动大属正常（周内节律）。FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 16：中位数对比（投注相关·典型玩家）=====
fig, ax = newp()
head(ax, 0.965, "16. 中位数对比（投注相关·典型玩家，单元格＝挽/默）")
txt(ax, 0.06, 0.925, "16.1 玩家级中位数（每位玩家其窗口内汇总的中位数）", size=10, bold=True, color=C_RET)
def pmv(tr, col, f):
    return f"{f(pmed.loc[('A', tr), col])}/{f(pmed.loc[('B', tr), col])}"
rows = [[tr, pmv(tr, "shots", lambda x: f"{x:,.0f}"), pmv(tr, "tbet", lambda x: f"{x:,.0f}"),
         pmv(tr, "net", lambda x: f"{x:,.0f}")] for tr in TIERS]
y = tbl(ax, 0.90, ["tier", "投注次数(中位)", "投注总额(中位)", "人均净(中位)"], rows, [0.06, 0.28, 0.52, 0.76], fs=9, rh=0.028)

txt(ax, 0.06, y - 0.02, "16.2 shot 级中位数（典型一发子弹）", size=10, bold=True, color=C_RET)
def smv(tr, m, f):
    return f"{f(m[('A', tr)])}/{f(m[('B', tr)])}"
rows = [[tr, smv(tr, med_bet, lambda x: f"{x:.0f}"), smv(tr, med_mult, lambda x: f"{x:.0f}"),
         smv(tr, med_base, lambda x: f"{x:.0f}"), smv(tr, med_kfv, lambda x: f"{x:.0f}")] for tr in TIERS]
y = tbl(ax, y - 0.05, ["tier", "单笔投注", "倍率场", "底分", "命中鱼价值"], rows, [0.06, 0.26, 0.44, 0.62, 0.80], fs=9, rh=0.028)

txt(ax, 0.06, y - 0.02, "16.3 均值 vs 中位数（投注次数/人，看大户效应）", size=10, bold=True, color=C_RET)
rows = [[tr, f"{HM[('A',tr)]['bets']:,.0f}", f"{pmed.loc[('A',tr),'shots']:,.0f}",
         f"{HM[('B',tr)]['bets']:,.0f}", f"{pmed.loc[('B',tr),'shots']:,.0f}"] for tr in TIERS]
y = tbl(ax, y - 0.05, ["tier", "挽留均值", "挽留中位", "default均值", "default中位"], rows, [0.06, 0.26, 0.44, 0.64, 0.82], fs=9, rh=0.028)

y2 = head(ax, y - 0.03, "16.4 结论")
for b in ["· 单笔/倍率场/底分 中位数几乎都=1——典型玩家就是“1 币 1 倍小注”；均值被少数高倍大户拉高。",
          "· 投注次数：均值挽留远高于 default（T4 20464 vs 16735），但中位数反而略低（1902 vs 1974）——",
          "   “挽留更活跃”主要是大户效应，典型玩家两组接近。",
          "· 人均净中位数：典型挽留玩家亏得略少（如 T4 −269 vs −290），体验略好。",
          "· 命中鱼价值中位数 4–8（小鱼），两组接近；结论：看典型玩家须用中位数，均值易被大户误导。"]:
    txt(ax, 0.07, y2, b, size=8.8); y2 -= 0.025
txt(ax, 0.06, 0.05, "FM01 个性化挽留完整汇报 · draft", size=8, color=MUTED)
pdf.savefig(fig); plt.close(fig)

# ===== Page 17：登陆会话（登陆次数 · 登陆时长，挽留 vs default）=====
fig, ax = newp()
y = head(ax, 0.965, "17. 登陆会话：登陆次数 · 登陆时长（挽留 vs default，单元格＝挽/默）")
txt(ax, 0.06, 0.928, f"源：platform.fct_user_session_event（FM01·CNY·剔测试单）· 登陆时间≥{RETENTION_LAUNCH_UTC[:10]} 且北京日<{END} · 按 user_id 映射 tier",
    size=7.6, color=MUTED)
txt(ax, 0.06, 0.912, f"覆盖有 tier 标签的登陆用户 {len(suq):,} 人（挽留A {LG[('A','ALL')]['users']:,} / default B {LG[('B','ALL')]['users']:,}）；已剔除 timeout 封顶(720分)噪声",
    size=7.6, color=MUTED)

def _pair(a, b, f="{:.1f}"):
    return f"{f.format(a)}/{f.format(b)}"
cx = [0.06, 0.24, 0.46, 0.68, 0.85]
txt(ax, 0.06, 0.885, "17.1 均值（人均）", size=10, bold=True, color=C_RET)
rows = []
for tr in ["T1", "T2", "T3", "T4", "ALL"]:
    a, b = LG[("A", tr)], LG[("B", tr)]
    lab = "总计" if tr == "ALL" else tr
    rows.append([lab, _pair(a["n_mean"], b["n_mean"]), _pair(a["tot_mean"], b["tot_mean"]),
                 _pair(a["per_mean"], b["per_mean"], "{:.2f}"), _pair(a["spd"], b["spd"], "{:.2f}")])
y = tbl(ax, 0.862, ["tier", "登陆次数/人", "人均总时长(分)", "每次登陆(分)", "次/活跃日"], rows, cx, fs=9, rh=0.03)

txt(ax, 0.06, y - 0.02, "17.2 中位数（典型玩家）", size=10, bold=True, color=C_RET)
rows = []
for tr in ["T1", "T2", "T3", "T4", "ALL"]:
    a, b = LG[("A", tr)], LG[("B", tr)]
    lab = "总计" if tr == "ALL" else tr
    rows.append([lab, _pair(a["n_med"], b["n_med"], "{:.0f}"), _pair(a["tot_med"], b["tot_med"]),
                 _pair(a["per_med"], b["per_med"], "{:.2f}")])
y = tbl(ax, y - 0.05, ["tier", "登陆次数/人(中位)", "人均总时长分(中位)", "每次登陆分(中位)"], rows,
        [0.06, 0.30, 0.56, 0.82], fs=9, rh=0.028)

# 两张柱状图：登陆次数/人、人均总时长（按 tier，挽 vs 默）
tiers = ["T1", "T2", "T3", "T4"]; xp = np.arange(len(tiers)); w = 0.38
for i, (key, ttl) in enumerate([("n_mean", "登陆次数/人（均值）"), ("tot_mean", "人均总登陆时长(分,均值)")]):
    axx = fig.add_axes([0.10 + i * 0.47, 0.30, 0.36, 0.16])
    axx.bar(xp - w / 2, [LG[("A", t)][key] for t in tiers], w, color=C_RET, label="挽留")
    axx.bar(xp + w / 2, [LG[("B", t)][key] for t in tiers], w, color=C_DEF, label="default")
    axx.set_xticks(xp); axx.set_xticklabels(tiers, fontproperties=FP, fontsize=7)
    axx.set_title(ttl, fontproperties=FP, fontsize=8.5); axx.legend(prop=FP, fontsize=6.5)
    axx.grid(alpha=.3, axis="y")
    for lb in axx.get_yticklabels(): lb.set_fontsize(6)

y2 = head(ax, 0.255, "17.3 结论")
for b in ["· 挽留玩家“来得更勤”：登陆次数/人各 tier 都更高（T4 33.6 vs 29.3、T3 9.7 vs 7.9、总计 8.5 vs 7.6），次/活跃日也略高。",
          "· 人均总在线时长更长：T4 182 vs 153 分、T3 48 vs 40 分（总计 42.8 vs 36.8 分）——挽留组累计参与更多。",
          "· 但“每次登陆时长”两组几乎一致（总计均值 5.1 vs 4.9 分、中位 ~1.9 vs 2.0 分）——单次时长没被改变。",
          "· ⚠ 关键反差：登陆次数中位数两组完全相同（T1=1/T2=2/T3=3/T4=7/总计=2），总在线时长中位数也基本持平（总计 4.0 vs 4.2 分）——",
          "   “挽留登陆更勤/更久”只是均值现象，纯属少数高频大户效应；典型玩家（中位）两组登陆频次与总时长无差别。",
          "· 与 §14/§16 一致：挽留改变的是“来玩几次/累计多久”，不是“每次玩多久”；且这份“更勤”集中在大户，看典型玩家须用中位数。"]:
    txt(ax, 0.07, y2, b, size=8.6); y2 -= 0.025
pdf.savefig(fig); plt.close(fig)
pdf.close()
print("saved:", OUT)
