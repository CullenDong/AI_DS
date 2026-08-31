"""只追加：在已存在的 Confluence 原生页面末尾追加「§17 登陆会话」+「T4 活跃说明」，并替换 PDF 附件。
不连 Redshift、不重算——数字取自本地 PDF（已人工核对当前版本）。
用法：python3 jobs/fishing/fm01_confluence_append.py [--publish]
"""
from __future__ import annotations
import os, re, sys, html, argparse
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
import requests

ROOT = Path(__file__).resolve().parents[2]
PDF = ROOT / "docs" / "FM01_个性化挽留_完整汇报.pdf"
IMGDIR = ROOT / "data" / "output" / "conf_imgs"; IMGDIR.mkdir(parents=True, exist_ok=True)
SPACE = "productsha"; TITLE = "FM01 个性化挽留组 · 分析汇报"
PDF_ATTNAME = "FM01_retention_report.pdf"   # 与既有页面附件同名 → 原地替换
FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break

# ---- 从当前 PDF 读到的数字（已人工核对，END=08-22）----
TIERS = ["T1", "T2", "T3", "T4"]
# 登陆均值：(挽A, 默B)  n=登陆次数/人  tot=人均总时长分  per=每次登陆分  spd=次/活跃日
LOGIN_MEAN = {
    "T1": dict(n=(1.8, 1.9), tot=(6.0, 6.1), per=(3.26, 3.22), spd=(1.70, 1.72)),
    "T2": dict(n=(4.0, 4.2), tot=(19.4, 19.2), per=(4.85, 4.63), spd=(2.07, 2.15)),
    "T3": dict(n=(9.9, 8.1), tot=(49.7, 41.4), per=(5.03, 5.15), spd=(2.71, 2.41)),
    "T4": dict(n=(34.9, 30.2), tot=(187.6, 158.5), per=(5.41, 5.28), spd=(3.79, 3.41)),
    "ALL": dict(n=(8.6, 7.7), tot=(43.4, 37.6), per=(5.07, 4.92), spd=(2.22, 2.16)),
}
# 登陆中位：n=登陆次数中位  tot=总时长分中位  per=每次登陆分中位
LOGIN_MED = {
    "T1": dict(n=(1, 1), tot=(2.7, 2.8), per=(1.47, 1.52)),
    "T2": dict(n=(2, 2), tot=(6.2, 6.1), per=(1.93, 1.90)),
    "T3": dict(n=(3, 3), tot=(10.6, 9.3), per=(2.07, 2.07)),
    "T4": dict(n=(7, 7), tot=(25.8, 23.5), per=(1.95, 2.13)),
    "ALL": dict(n=(2, 2), tot=(4.0, 4.2), per=(1.88, 2.00)),
}
LOGIN_HDR = "覆盖有 tier 标签的登陆用户 35,414 人（挽留A 6,963 / default B 28,451）"
# T4 活跃说明：全量池 / 窗口活跃 / 活跃率%
ACT = {
    "T1": dict(pool=245727, act=17692, rate=7.2),
    "T2": dict(pool=55074, act=5673, rate=10.3),
    "T3": dict(pool=17979, act=3077, rate=17.1),
    "T4": dict(pool=13848, act=5565, rate=40.2),
}

def esc(s): return html.escape(str(s))
def htable(headers, rows):
    h = "".join(f"<th>{esc(x)}</th>" for x in headers)
    body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tbody><tr>{h}</tr>{body}</tbody></table>"
def bullets(items): return "<ul>" + "".join(f"<li>{esc(x)}</li>" for x in items) + "</ul>"
def pair(t, f="{:.1f}"): return f"{f.format(t[0])}/{f.format(t[1])}"

def make_chart():
    xp = np.arange(len(TIERS)); w = 0.38
    fig, axes = plt.subplots(1, 2, figsize=(9, 3))
    for axx, (key, ttl) in zip(axes, [("n", "登陆次数/人（均值）"), ("tot", "人均总登陆时长(分,均值)")]):
        axx.bar(xp - w / 2, [LOGIN_MEAN[t][key][0] for t in TIERS], w, color="#0969da", label="挽留")
        axx.bar(xp + w / 2, [LOGIN_MEAN[t][key][1] for t in TIERS], w, color="#c98a00", label="default")
        axx.set_xticks(xp); axx.set_xticklabels(TIERS, fontproperties=FP, fontsize=8)
        axx.set_title(ttl, fontproperties=FP, fontsize=10); axx.legend(prop=FP, fontsize=8); axx.grid(alpha=.3, axis="y")
    name = "login_session.png"; fp = IMGDIR / name
    fig.savefig(fp, dpi=130, bbox_inches="tight", facecolor="white"); plt.close(fig)
    return name, fp

def build_append(chart_name):
    parts = ["<hr/><h1>十一、登陆会话（登陆次数 · 登陆时长）</h1>"]
    parts.append(f"<p>源：platform.fct_user_session_event（FM01·CNY·剔测试单），按 user_id 映射 tier；已剔除 timeout(720分)封顶噪声。{esc(LOGIN_HDR)}。</p>")
    parts.append("<p><strong>均值（人均）</strong></p>")
    parts.append(htable(["tier", "登陆次数/人(挽/默)", "人均总时长分(挽/默)", "每次登陆分(挽/默)", "次/活跃日(挽/默)"],
        [["总计" if t == "ALL" else t, pair(LOGIN_MEAN[t]["n"]), pair(LOGIN_MEAN[t]["tot"]),
          pair(LOGIN_MEAN[t]["per"], "{:.2f}"), pair(LOGIN_MEAN[t]["spd"], "{:.2f}")] for t in TIERS + ["ALL"]]))
    parts.append("<p><strong>中位数（典型玩家）</strong></p>")
    parts.append(htable(["tier", "登陆次数/人中位(挽/默)", "人均总时长分中位(挽/默)", "每次登陆分中位(挽/默)"],
        [["总计" if t == "ALL" else t, pair(LOGIN_MED[t]["n"], "{:.0f}"), pair(LOGIN_MED[t]["tot"]),
          pair(LOGIN_MED[t]["per"], "{:.2f}")] for t in TIERS + ["ALL"]]))
    parts.append(f'<p><ac:image ac:width="820"><ri:attachment ri:filename="{chart_name}"/></ac:image></p>')
    parts.append(bullets([
        "挽留玩家“来得更勤/累计更久”：登陆次数/人各 tier 都更高（总计 8.6 vs 7.7），人均总在线时长更长（总计 43.4 vs 37.6 分）。",
        "但“每次登陆时长”两组几乎一致（总计均值 5.1 vs 4.9 分、中位 ~2 分）——单次时长没被改变。",
        "关键反差：登陆次数中位数两组完全相同（T1=1/T2=2/T3=3/T4=7/总计=2），总时长中位也基本持平——“更勤/更久”纯属少数高频大户效应，典型玩家两组无差别。",
        "结论：挽留改变的是“来玩几次/累计多久”，不是“每次玩多久”；看典型玩家须用中位数。",
    ]))
    # T4 活跃说明
    parts.append("<h1>十二、补充：为何 T4 活跃人数反超 T3</h1>")
    parts.append("<p>全量池是正常金字塔（T4 最少），但活跃率随 tier 飙升，T4 参与度最高，故活跃人数反超 T3——非异常。</p>")
    parts.append(htable(["tier", "全量池人数", "窗口活跃人数", "活跃率%"],
        [[t, f"{ACT[t]['pool']:,}", f"{ACT[t]['act']:,}", f"{ACT[t]['rate']:.1f}"] for t in TIERS]))
    parts.append("<p><em>口径提示：此处“活跃/活跃率”＝窗口内有投注(发过子弹)的去重玩家（每人只算1次）；与留存的“当天有投注、按天算、不去重”不同，勿混淆。</em></p>")
    return "\n".join(parts)

def load_env():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env

def main(do_publish):
    chart_name, chart_fp = make_chart()
    add = build_append(chart_name)
    print(f"追加 HTML 长度 {len(add)}；图表 {chart_name}")
    env = load_env()
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"]); H = {"X-Atlassian-Token": "no-check"}
    r = requests.get(f"{base}/rest/api/content", auth=auth,
                     params={"spaceKey": SPACE, "title": TITLE, "expand": "version,body.storage"}, timeout=30)
    res = r.json().get("results", []) if r.status_code == 200 else []
    if not res:
        print("未找到页面：", r.status_code, r.text[:200]); return
    pid = res[0]["id"]; ver = res[0]["version"]["number"]; cur = res[0]["body"]["storage"]["value"]
    print(f"页面 id={pid} 当前版本 v{ver} 正文长度 {len(cur)}")
    # 去掉旧的追加块（若已追加过，避免重复）——从「十一、登陆会话」起截断
    marker = "<hr/><h1>十一、登陆会话"
    base_body = cur.split(marker)[0] if marker in cur else cur
    new_body = base_body + "\n" + add
    print(f"新正文长度 {len(new_body)}（原 {len(cur)}）")
    if not do_publish:
        print("(dry-run；加 --publish 实际更新)"); return
    r = requests.put(f"{base}/rest/api/content/{pid}", auth=auth, json={
        "id": pid, "type": "page", "title": TITLE, "space": {"key": SPACE},
        "version": {"number": ver + 1}, "body": {"storage": {"value": new_body, "representation": "storage"}}}, timeout=60)
    print("更新页面:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400]); return
    with open(chart_fp, "rb") as f:
        requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                      files={"file": (chart_name, f, "image/png")}, data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=60)
    with open(PDF, "rb") as f:
        requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                      files={"file": (PDF_ATTNAME, f, "application/pdf")}, data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=120)
    print("完成。页面:", f"{base}/spaces/{SPACE}/pages/{pid}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
