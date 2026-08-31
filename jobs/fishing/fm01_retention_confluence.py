"""把 FM01 个性化挽留报告发布为 Confluence 页面（正常文档形式：文字/原生表格 + 图表图片）。

复用 jobs.fishing.fm01_retention_report 已算好的数据（import 时会跑查询并生成 PDF）。
- 文字：<h1/h2>、<ul>、原生 <table>
- 图表：单独渲染 PNG，作为附件内嵌（<ac:image><ri:attachment>）
- 附件：完整 16 页 PDF 供下载
凭证取自 .env（CONFLUENCE_URL/EMAIL/TOKEN）。空间 MFS。
用法：python3 jobs/fishing/fm01_retention_confluence.py [--publish]
"""
from __future__ import annotations
import os, re, sys, html, argparse
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import jobs.fishing.fm01_retention_report as R   # noqa: E402  运行查询 + 生成 PDF，拿到所有数据

SPACE = "productsha"          # BL-HUB
PARENT_ID = "122421321"       # 文件夹 fish hunter data (AI Team/data/fish hunter data)
TITLE = "FM01 个性化挽留组 · 分析汇报"
IMGDIR = ROOT / "data" / "output" / "conf_imgs"; IMGDIR.mkdir(parents=True, exist_ok=True)
FP = R.FP
imgs = []   # (filename, path)

def esc(s): return html.escape(str(s))

def htable(headers, rows):
    h = "".join(f"<th>{esc(x)}</th>" for x in headers)
    body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tbody><tr>{h}</tr>{body}</tbody></table>"

def bullets(items): return "<ul>" + "".join(f"<li>{esc(x)}</li>" for x in items) + "</ul>"

def savefig(fig, name):
    p = IMGDIR / name; fig.savefig(p, dpi=130, bbox_inches="tight", facecolor="white"); plt.close(fig)
    imgs.append((name, p)); return f'<ac:image ac:width="760"><ri:attachment ri:filename="{name}"/></ac:image>'

def line(dfpiv, title, ylab=""):
    fig, ax = plt.subplots(figsize=(7, 3))
    for gt, col in [("default", "#c98a00"), ("retention", "#0969da")]:
        if gt in dfpiv.columns:
            ax.plot([pd.Timestamp(x) for x in dfpiv.index], dfpiv[gt].values, marker="o", ms=2.5, lw=1.4, color=col, label=gt)
    ax.set_title(title, fontproperties=FP, fontsize=11); ax.legend(prop=FP, fontsize=8); ax.grid(alpha=.3)
    for lb in ax.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(6)
    return fig

# ---------------- 组装内容 ----------------
parts = []
parts.append(f"<p><strong>窗口</strong> {R.START} ~ {R.LASTDAY}（北京日）· 数据截至 {R.TODAY} · "
             f"挽留(A,尾号0/1) vs default(B,尾号2-9)，尾号随机切、唯一差异=有无挽留（ITT）· tier 用 dim_user_ab_tier。</p>")

# 摘要
parts.append("<h1>一、核心结论摘要</h1>")
_d, _r = R.agg.loc["default"], R.agg.loc["retention"]
lf = lambda a, b: f"{(b/a-1)*100:+.1f}%"
tot = R.dta.groupby(["ab", "bj"])["u"].sum().unstack(0).sort_index(); grt = tot.pct_change() * 100
parts.append(bullets([
    f"规模：挽留 {int(R.g.loc['retention','users']):,} 玩家；RTP {R.g.loc['retention','rtp']}% ≈ default {R.g.loc['default','rtp']}%，无系统放水。",
    "留存：上线初期落后 default 约 3pp，随时间改善——近端 D1/D3 反超，回流占比也反超；D10 偏低是自然衰减+右截断，非异常。",
    f"投注习惯：激励使 投注次数 {lf(_d['shots']/_d['u'],_r['shots']/_r['u'])}、单笔 {lf(_d['bet']/_d['shots'],_r['bet']/_r['shots'])}、总额 {lf(_d['bet']/_d['u'],_r['bet']/_r['u'])}——“打得多、花得少”；高 tier 被降档为“小额低倍高频”；打哪种鱼两组接近。",
    "投注额被少数高倍子弹(占4–6%发数)撑起(占30–35%额)，default 高倍更多→总额更高。",
    f"人数增长：每日环比均值 挽留 {grt['A'].mean():.2f}% vs default {grt['B'].mean():.2f}%，挽留更快。",
    "中位数：单笔/倍率场/底分 中位数=1（典型是1币1倍小注），均值被大户拉高；典型挽留玩家亏得略少。",
    "风险：尾部农场号（高倍套利）拖低挽留人均净/RTP，建议持续剔除。",
]))

# 规模
parts.append("<h1>二、规模对比</h1>")
parts.append(htable(["组", "玩家", "子弹", "投注", "RTP%", "净盈亏"],
    [[gt, f"{int(R.g.loc[gt,'users']):,}", f"{int(R.g.loc[gt,'shots']):,}", f"{R.g.loc[gt,'bet']:,.0f}",
      R.g.loc[gt, "rtp"], f"{R.g.loc[gt,'net']:,.0f}"] for gt in ["retention", "default"]]))

# 激励总结
parts.append("<h1>三、投注行为 · 激励总结（窗口聚合 per-user）</h1>")
mets = [("人均投注次数", _d["shots"]/_d["u"], _r["shots"]/_r["u"], "{:,.0f}"),
        ("人均投注额", _d["bet"]/_d["u"], _r["bet"]/_r["u"], "{:,.0f}"),
        ("单笔投注", _d["bet"]/_d["shots"], _r["bet"]/_r["shots"], "{:.2f}"),
        ("命中率%", _d["k"]/_d["shots"]*100, _r["k"]/_r["shots"]*100, "{:.2f}"),
        ("RTP%", _d["pay"]/_d["bet"]*100, _r["pay"]/_r["bet"]*100, "{:.2f}"),
        ("人均净", (_d["pay"]-_d["bet"])/_d["u"], (_r["pay"]-_r["bet"])/_r["u"], "{:,.0f}")]
parts.append(htable(["指标", "default", "retention", "挽留 vs default"],
    [[m, f.format(dv), f.format(rv), lf(dv, rv)] for m, dv, rv, f in mets]))

# 每日留存差值图
parts.append("<h1>四、每日留存（挽留 vs default）</h1>")
parts.append("<p>上线初期挽留落后，近端反超；下图为 <strong>挽留 − default 差值零轴</strong>（&gt;0 即反超）。</p>")
fig, ax = plt.subplots(figsize=(7.2, 3))
for n, col in zip([1, 3, 7, 10], ["#0969da", "#1a9850", "#d73027", "#8856a7"]):
    p = R.ret[R.ret["n"] == n].pivot_table(index="d", columns="gt", values="r")
    if "retention" in p and "default" in p:
        ax.plot([pd.Timestamp(x) for x in p.index], (p["retention"]-p["default"]).values, marker="o", ms=2.5, lw=1.3, color=col, label=f"D{n}")
ax.axhline(0, color="#333", lw=1); ax.set_title("留存差值 挽留−default（>0=反超）", fontproperties=FP, fontsize=11)
ax.legend(prop=FP, fontsize=8, ncol=4); ax.grid(alpha=.3)
for lb in ax.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(6)
parts.append(savefig(fig, "ret_diff.png"))

# 回流占比
parts.append("<h1>五、回流占比（人数涨≠留存高）</h1>")
parts.append("<p>回流占比=当日回流(老)玩家/当日活跃；反映沉淀老玩家黏性，不受右截断影响。</p>")
grP = R.grow.pivot_table(index="d", columns="gt", values="reflow")
parts.append(savefig(line(grP, "回流占比% 挽留 vs default"), "reflow.png"))

# 每日活跃人数 by tier
parts.append("<h1>六、T1–T4 每日活跃人数 + 增长率</h1>")
TCOL = {"T1": "#6baed6", "T2": "#74c476", "T3": "#fd8d3c", "T4": "#de2d26"}
fig, axes = plt.subplots(1, 2, figsize=(9, 3))
for axx, (abv, name) in zip(axes, [("A", "挽留"), ("B", "default")]):
    for tr in R.TIERS:
        s = R.dta[(R.dta["ab"] == abv) & (R.dta["tier"] == tr)].set_index("bj")["u"].sort_index()
        axx.plot([pd.Timestamp(x) for x in s.index], s.values, lw=1.2, color=TCOL[tr], label=tr)
    axx.set_title(f"{name}·各 tier 每日活跃", fontproperties=FP, fontsize=10); axx.legend(prop=FP, fontsize=6, ncol=4); axx.grid(alpha=.3)
    for lb in axx.get_xticklabels(): lb.set_rotation(45); lb.set_fontsize(5)
parts.append(savefig(fig, "daily_users.png"))
gr = R.dta.pivot_table(index="bj", columns=["ab", "tier"], values="u").sort_index().pct_change() * 100
rows = [["组总", f"{grt['A'].mean():.2f}%", f"{grt['B'].mean():.2f}%"]]
for tr in R.TIERS:
    rows.append([tr, f"{gr[('A',tr)].mean():.2f}%", f"{gr[('B',tr)].mean():.2f}%"])
parts.append("<p><strong>每日环比增长率均值</strong>（当天/前一天−1）：</p>")
parts.append(htable(["序列", "挽留", "default"], rows))
# T4 活跃人数为何反超 T3：全量金字塔 vs 活跃率
parts.append("<p><strong>为何 T4 活跃人数≈T3甚至更多？</strong> 全量池是正常金字塔（T4 最少），但活跃率随 tier 飙升，T4 参与度最高，故活跃人数反超 T3——非异常。</p>")
parts.append(htable(["tier", "全量池人数", "窗口活跃人数", "活跃率%"],
    [[t, f"{R.ACTR[t]['pool']:,}", f"{R.ACTR[t]['act']:,}", f"{R.ACTR[t]['rate']:.1f}"] for t in R.TIERS]))
parts.append("<p><em>口径提示：此处“活跃/活跃率”＝窗口内有投注(发过子弹)的去重玩家（每人只算1次）；与留存的“当天有投注、按天算、不去重”不同，勿混淆。</em></p>")

# 打鱼习惯全指标
parts.append("<h1>七、分 tier 打鱼习惯全指标（挽留 / default）</h1>")
COLS = [("投注次数/人", "bets", "{:,.0f}"), ("单笔投注", "size", "{:.2f}"), ("倍率场", "mult", "{:.2f}"),
        ("高倍%", "hi", "{:.1f}"), ("命中%", "hit", "{:.1f}"), ("鱼价值", "kfv", "{:.0f}"),
        ("人均净", "net", "{:,.0f}"), ("RTP%", "rtp", "{:.1f}")]
for abv, name in [("A", "挽留"), ("B", "default")]:
    parts.append(f"<h2>{name}</h2>")
    rows = [[tr] + [f.format(R.HM[(abv, tr)][k]) for _, k, f in COLS] for tr in R.TIERS]
    parts.append(htable(["tier"] + [c[0] for c in COLS], rows))

# 关键指标分布图（倍率场 & 鱼价值）
parts.append("<h1>八、关键指标分布（倍率场 / 鱼价值，堆叠 %）</h1>")
CMAP6 = ["#c6dbef", "#9ecae1", "#6baed6", "#4292c6", "#2171b5", "#084594"]
def stackfig(dd, order, sub):
    fig, axes = plt.subplots(1, 2, figsize=(9, 3))
    for axx, (abv, gname) in zip(axes, [("A", "挽留"), ("B", "default")]):
        s = dd[dd["ab"] == abv].copy(); t = s.groupby("tier")["n"].transform("sum"); s["pct"] = s["n"]/t*100
        p = s.pivot_table(index="tier", columns="bucket", values="pct", fill_value=0).reindex(index=R.TIERS, columns=order)
        bottom = np.zeros(len(R.TIERS))
        for i, b in enumerate(order):
            v = p[b].fillna(0).values; axx.bar(R.TIERS, v, bottom=bottom, color=CMAP6[i % 6], label=str(b)); bottom += v
        axx.set_title(f"{sub}·{gname}", fontproperties=FP, fontsize=10); axx.set_ylim(0, 100)
        axx.legend(prop=FP, fontsize=6, ncol=3);
        for lb in axx.get_xticklabels(): lb.set_fontproperties(FP); lb.set_fontsize(8)
    return fig
parts.append(savefig(stackfig(R.dmult.assign(bucket=R.dmult["bucket"]), [1.0, 2.0, 10.0, 50.0, 100.0], "倍率场"), "dist_mult.png"))
fish2 = R.fish[["ab", "tier", "band", "k"]].rename(columns={"band": "bucket", "k": "n"})
parts.append(savefig(stackfig(fish2, ["<10", "10-30", "30-60", "60-120", "120-200", "200+"], "鱼价值"), "dist_fish.png"))

# 投注额分解图
parts.append("<h1>九、投注额分解（为什么打得多、花得少）</h1>")
parts.append("<p>高倍子弹只占 4–6% 发数，却贡献 30–35% 投注额——按发数看分布几乎一样，按投注额看差别巨大。</p>")
fig, axes = plt.subplots(1, 2, figsize=(9, 3))
for axx, (gt, name) in zip(axes, [("retention", "挽留"), ("default", "default")]):
    s = R.dmn[R.dmn["gt"] == gt].set_index("m").reindex([1.0, 2.0, 10.0, 50.0, 100.0]).fillna(0)
    sp = s["shots"]/s["shots"].sum()*100; bp = s["bet"]/s["bet"].sum()*100; xx = np.arange(5); w = 0.38
    axx.bar(xx-w/2, sp.values, w, color="#9ecae1", label="按发数%"); axx.bar(xx+w/2, bp.values, w, color="#08519c", label="按投注额%")
    axx.set_title(f"倍率场 发数vs投注额·{name}", fontproperties=FP, fontsize=10); axx.set_xticks(xx); axx.set_xticklabels(["1x","2x","10x","50x","100x"], fontsize=8)
    axx.legend(prop=FP, fontsize=7); axx.grid(alpha=.3, axis="y")
parts.append(savefig(fig, "bet_decomp.png"))

# 中位数
parts.append("<h1>十、中位数视角（典型玩家）</h1>")
parts.append("<p>均值易被少数高倍大户拉高；中位数看典型玩家。单笔/倍率/底分 中位数几乎都=1。</p>")
rows = [[tr, f"{R.pmed.loc[('A',tr),'shots']:,.0f}/{R.pmed.loc[('B',tr),'shots']:,.0f}",
         f"{R.pmed.loc[('A',tr),'net']:,.0f}/{R.pmed.loc[('B',tr),'net']:,.0f}",
         f"{R.med_bet[('A',tr)]:.0f}/{R.med_bet[('B',tr)]:.0f}",
         f"{R.med_mult[('A',tr)]:.0f}/{R.med_mult[('B',tr)]:.0f}"] for tr in R.TIERS]
parts.append(htable(["tier", "投注次数中位(挽/默)", "人均净中位(挽/默)", "单笔中位(挽/默)", "倍率场中位(挽/默)"], rows))

# 登陆会话（登陆次数 · 登陆时长）
parts.append("<h1>十一、登陆会话（登陆次数 · 登陆时长）</h1>")
parts.append("<p>源：platform.fct_user_session_event（FM01·CNY·剔测试单），按 user_id 映射 tier；已剔除 timeout(720分)封顶噪声。</p>")
def _pr(a, b, f="{:.1f}"): return f"{f.format(a)}/{f.format(b)}"
parts.append("<p><strong>均值（人均）</strong></p>")
parts.append(htable(["tier", "登陆次数/人(挽/默)", "人均总时长分(挽/默)", "每次登陆分(挽/默)", "次/活跃日(挽/默)"],
    [["总计" if t == "ALL" else t,
      _pr(R.LG[("A", t)]["n_mean"], R.LG[("B", t)]["n_mean"]),
      _pr(R.LG[("A", t)]["tot_mean"], R.LG[("B", t)]["tot_mean"]),
      _pr(R.LG[("A", t)]["per_mean"], R.LG[("B", t)]["per_mean"], "{:.2f}"),
      _pr(R.LG[("A", t)]["spd"], R.LG[("B", t)]["spd"], "{:.2f}")] for t in ["T1", "T2", "T3", "T4", "ALL"]]))
parts.append("<p><strong>中位数（典型玩家）</strong></p>")
parts.append(htable(["tier", "登陆次数/人中位(挽/默)", "人均总时长分中位(挽/默)", "每次登陆分中位(挽/默)"],
    [["总计" if t == "ALL" else t,
      _pr(R.LG[("A", t)]["n_med"], R.LG[("B", t)]["n_med"], "{:.0f}"),
      _pr(R.LG[("A", t)]["tot_med"], R.LG[("B", t)]["tot_med"]),
      _pr(R.LG[("A", t)]["per_med"], R.LG[("B", t)]["per_med"], "{:.2f}")] for t in ["T1", "T2", "T3", "T4", "ALL"]]))
parts.append(bullets([
    "挽留玩家“来得更勤/累计更久”：登陆次数/人各 tier 都更高（总计 8.5 vs 7.6），人均总在线时长更长（总计 42.8 vs 36.8 分）。",
    "但“每次登陆时长”两组几乎一致（总计均值 5.1 vs 4.9 分、中位 ~2 分）——单次时长没被改变。",
    "关键反差：登陆次数中位数两组完全相同（T1=1/T2=2/T3=3/T4=7/总计=2），总时长中位也基本持平——“更勤/更久”纯属少数高频大户效应，典型玩家两组无差别。",
    "结论：挽留改变的是“来玩几次/累计多久”，不是“每次玩多久”；看典型玩家须用中位数。",
]))

parts.append("<hr/><p><em>完整明细见附件 PDF（17 页）。口径：jobs/fishing/fm01_grouping.py；留存=day-N 当日回访率（当天有投注即活跃、按天算、不去重唯一玩家）。</em></p>")
parts.append('<p><strong>附件：</strong><ac:link><ri:attachment ri:filename="FM01_retention_report.pdf"/></ac:link></p>')

BODY = "\n".join(parts)
print(f"HTML 长度 {len(BODY)}，图片 {len(imgs)} 张，附件 PDF 1 个")

# ---------------- 发布 ----------------
def publish():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"]); H = {"X-Atlassian-Token": "no-check"}
    # 已存在？
    r = requests.get(f"{base}/rest/api/content", auth=auth,
                     params={"spaceKey": SPACE, "title": TITLE, "expand": "version"}, timeout=30)
    existing = r.json().get("results", []) if r.status_code == 200 else []
    if existing:
        pid = existing[0]["id"]; ver = existing[0]["version"]["number"] + 1
        r = requests.put(f"{base}/rest/api/content/{pid}", auth=auth, json={
            "id": pid, "type": "page", "title": TITLE, "space": {"key": SPACE},
            "version": {"number": ver}, "body": {"storage": {"value": BODY, "representation": "storage"}}}, timeout=60)
        print("更新页面:", r.status_code)
    else:
        r = requests.post(f"{base}/rest/api/content", auth=auth, json={
            "type": "page", "title": TITLE, "space": {"key": SPACE},
            "ancestors": [{"id": PARENT_ID}],
            "body": {"storage": {"value": BODY, "representation": "storage"}}}, timeout=60)
        print("创建页面:", r.status_code)
    if r.status_code not in (200, 201):
        print(r.text[:400]); return
    pid = r.json()["id"]
    # 上传附件（图片 + PDF）
    for name, path in imgs:
        with open(path, "rb") as f:
            requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                          files={"file": (name, f, "image/png")}, data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=60)
    with open(R.OUT, "rb") as f:
        requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                      files={"file": ("FM01_retention_report.pdf", f, "application/pdf")},
                      data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=120)
    print("附件已上传。页面:", f"{base}/spaces/{SPACE}/pages/{pid}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    if a.publish: publish()
    else: print("(dry-run；加 --publish 实际发布)")
