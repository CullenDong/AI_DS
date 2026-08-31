"""把 SS07 AB 方案发布为 Confluence 原生页面（文字+原生表格+分组结构图），放入 slot machine data 文件夹。
不连 Redshift。用法：python3 jobs/ab_testing/ss07_ab_confluence.py [--publish]
"""
from __future__ import annotations
import os, re, sys, html, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
DIAG = ROOT / "prd" / "ab_testing" / "SS07_分组结构图.png"
SPACE = "productsha"
PARENT_ID = "122421319"          # slot machine data (BL-HUB > AI Team > Data)
TITLE = "SS07 AB组方案"
DIAG_NAME = "SS07_分组结构图.png"

def esc(s): return html.escape(str(s))
def h(n, t): return f"<h{n}>{esc(t)}</h{n}>"
def p(t): return f"<p>{t}</p>"
def ul(items): return "<ul>" + "".join(f"<li>{x}</li>" for x in items) + "</ul>"
def tbl(headers, rows):
    th = "".join(f"<th>{esc(x)}</th>" for x in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tbody><tr>{th}</tr>{body}</tbody></table>"
def code(s, lang="sql"):
    return (f'<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">{lang}</ac:parameter>'
            f'<ac:plain-text-body><![CDATA[{s}]]></ac:plain-text-body></ac:structured-macro>')
def b(s): return f"<strong>{esc(s)}</strong>"

P = []
P.append(p(f"{b('适用游戏')} SS07（老虎机，新游戏，尚未上线）· 数据表（上线后）slot-machine.public.fct_bet_orders · "
           f"起草 2026-08-21 · 状态 draft（样本量用 SS03 代理流量测算，SS07 上线后须用真实流量复核）。"))

# 分组结构图
P.append(h(2, "分组结构图"))
P.append(f'<p><ac:image ac:width="900"><ri:attachment ri:filename="{DIAG_NAME}"/></ac:image></p>')

# 0 决策
P.append(h(2, "0. 已确认的设计决策"))
P.append(tbl(["决策点", "结论"], [
    ["试点游戏", "SS07（老虎机）"],
    ["实验目的", f"在{b('同 RTP 96.5%')}下，测 {b('payout 分布 + 命中率')} 对投注行为与留存的影响"],
    ["实验臂", "三臂：default / testA / testB（均 96.5% RTP，仅表形不同）"],
    ["分流哈希", "复用 user_id，静态分组（不轮换、不引设备/OneID）"],
    ["分流比例", f"{b('default 40% / testA 30% / testB 30%')}（Dunnett 对照配比）"],
    ["主指标", f"{b('人均投注额')}（缩尾均值 + 中位数双口径，不用 log）"],
    ["判定阈(MDE)", f"{b('相对差异 20%')}（受 14 天周期约束）"],
    ["实验周期", f"{b('14 天一期')}"],
    ["分析人群", "ITT（assignment 人群）"],
    ["holdout", "无（三臂，default 即基准）"],
]))

# 1 假设
P.append(h(2, "1. 假设"))
P.append(p("三张表 RTP 全部 = 96.5%，唯一差异是命中率 + payout 倍率分布（波动性表形）。本实验剥离 RTP 变量，纯粹检验表形的影响。"))
P.append(tbl(["表", "RTP", "命中率(payout>0)", "倍率分布调整", "表形定性"], [
    [b("default"), "96.5%", "现行", "现行线上分布（基准）", "基准"],
    [b("testA"), "96.5%", b("20%"), "加 2x/3x 符号；加 1–5x 占比；砍 50–100x", b("高命中 · 低波动 · 小奖密集")],
    [b("testB"), "96.5%", b("15%"), "加 3x/5x 符号；加 2–10x 占比；砍 20–100x", b("中命中 · 中波动 · 砍大奖尾")],
]))
P.append(ul([
    f"{b('H1（testA vs default）')}：高命中低波动，正反馈更密 → 提升投注频次/活跃天与人均投注额。",
    f"{b('H2（testB vs default）')}：中命中中波动、砍大奖尾 → 影响人均投注额（可能提升单次投注），但高波动对低资金玩家留存有风险。",
    f"{b('H3（testA vs testB，次要）')}：命中率与波动结构差异对投注行为的净影响方向。",
    f"{b('预期方向（先验）')}：testA 更可能拉高投注次数/活跃天；testB 更可能拉高单次投注额但留存风险更高。主指标孰高需实验判定。",
]))

# 2 分流
P.append(h(2, "2. 实验臂与分流"))
P.append(p(f"按 {b('MOD(user_id, 10)')} 十进制末位静态分组，每人终身固定一臂、不随时间/会话变化："))
P.append(tbl(["臂", "尾号 MOD(user_id,10)", "流量占比", "角色"], [
    [b("default"), "0、1、2、3", "40%", "对照基准（现行表形）"],
    [b("testA"), "4、5、6", "30%", "处理臂（高命中低波动）"],
    [b("testB"), "7、8、9", "30%", "处理臂（中命中中波动）"],
]))
P.append(ul([
    f"{b('为何 40/30/30')}：两个关键对比都是新表 vs default（Dunnett 结构），给对照臂 ~√2 倍流量提升 H1/H2 精度；复用尾号机制、与 SS03 一致。",
    f"{b('为何静态不轮换')}：数学表实验需在同一批人上累积 D1/D3/D7 留存与投注行为，保 cohort 连续。",
]))
P.append(p(b("分组判定 SQL（上线后落 jobs/ss07_analysis/ss07_grouping.py）")))
P.append(code("CASE\n  WHEN MOD(user_id, 10) IN (0,1,2,3) THEN 'default'\n  WHEN MOD(user_id, 10) IN (4,5,6)   THEN 'testA'\n  ELSE 'testB'                                  -- 7,8,9\nEND AS arm"))
P.append(p(b("通用清洗（对齐 SS03 口径）")))
P.append(code("game_id='SS07' AND currency_type='CNY' AND status='COMPLETED'\nAND op_code NOT IN ('B26','TST','TSB','TSO')"))

# 3 指标
P.append(h(2, "3. 主指标 · 护栏 · 观测口径"))
P.append(p("投注行为为主；所有指标都观测，判定用缩尾均值 + 中位数双口径，不用 log 变换（保原单位可解释）。"))
P.append(tbl(["层级", "指标", "口径"], [
    [b("主判定"), "人均投注额", "窗口内每人总投注额；报 缩尾95%均值 + 中位数(Mann-Whitney)"],
    ["主观测", "人均投注次数", "每人 bet 笔数；缩尾均值 + 中位"],
    ["观测", "人均活跃天", "每人有投注的去重天数"],
    ["观测", "单笔投注额", "每笔 bet_amount 中位"],
    [b("表形校验"), "实测命中率、命中间隔", "验证 testA≈20% / testB≈15% 确实生效"],
    [b("护栏"), "实测 RTP", "三臂都须 ≈96.5%（否则 RTP 混淆，结论无效）"],
    ["护栏", "D1/D3/D7 留存", "day-N 当日回访率（当天有投注即活跃、按天算、不去重、北京日、右截断置空）"],
    ["护栏", "人均净亏", "缩尾均值 + 中位"],
]))
P.append(ul([
    "主指标（人均投注额）原始均值被鲸鱼撑爆（CV≈8）→ 不作统计判定，只作描述；判定用缩尾95%均值（CV≈1.8）+ 中位数。",
    "每个指标必同时报缩尾均值 + 中位数，鲸鱼效应一眼可辨。",
    "实测 RTP 是硬护栏：三臂 RTP 偏离 96.5% 或彼此不齐，先排查再谈结论。",
]))

# 4 样本量
P.append(h(2, "4. 样本量与时间线"))
P.append(p("SS07 无流量 → 用 SS03 近 14 天 CNY 作代理（同 slot、同人群）。日 DAU ≈ 1,240，14 天去重 UV ≈ 12,246。"
           " 公式 n/臂 = 2(z_a/2+z_b)²·CV²/rel²，α=0.05 双侧、power=0.8 → 系数 15.70。"))
P.append(tbl(["指标 / 口径", "CV", "检测 10%", "15%", "20%"], [
    ["人均投注额 · 原始均值", "8.09", "102,626", "45,611", "25,656 ✗"],
    ["人均投注额 · 缩尾99%", "3.17", "15,814", "7,028", "3,953"],
    [b("人均投注额 · 缩尾95%（主）"), "1.80", "5,101", "2,267", b("1,275")],
    ["人均投注次数 · 缩尾99%", "2.11", "7,017", "3,119", "1,754"],
    ["人均活跃天", "0.91", "1,309", "581", "327"],
]))
P.append(p(b("14 天可行性（代理流量 40/30/30 → default ≈ 4,900 / testA ≈ testB ≈ 3,670 人/臂）")))
P.append(ul([
    "主指标（人均投注额·缩尾95%）@ MDE 20% 需 1,275/臂 → 三臂均充分 powered，14 天可结论。",
    "缩尾99% @20% 需 3,953/臂 → 新表臂(3,670)略欠，需 ~16–18 天或等 SS07 实际流量更高。",
    "检测 15% 差异需 3–4 周，超出 14 天 → 本期只对 20% 及以上差异有把握，15% 属欠功效作探索。",
    "⚠ 以上基于 SS03 代理。SS07 上线后第一件事：用真实 DAU 复核样本量。",
]))

# 5 时间线
P.append(h(2, "5. 时间线与流程"))
P.append(tbl(["阶段", "内容"], [
    ["T0 上线", "SS07 三表按 §2 分流上线；确认 math_table_id 与 arm 映射落 ss07_grouping.py"],
    ["T0+1d", "SRM + 表形校验：尾号均匀性、40/30/30 比例；实测命中率 testA≈20%/testB≈15%、三臂 RTP≈96.5%"],
    ["T0 ~ T0+14d", "一期 = 14 天，累积投注行为 + D1/D3/D7 留存"],
    ["T0+14d", "出报告：主指标缩尾均值+中位双口径、护栏、H1/H2/H3 判定（ship/extend/stop/investigate）"],
]))

# 6 验证
P.append(h(2, "6. 验证与 SRM"))
P.append(ul([
    "SRM：预设 40/30/30，实测长期比例偏离即暂停结论，排查分桶/曝光/数据链路。尾号均匀性作基线（各尾号 ≈10%）。",
    "表形校验：实测命中率必须复现设计值（testA≈20%、testB≈15%）；实测倍率分布对齐设计的加/砍档位。",
    "RTP 护栏：三臂实测 RTP 都须 ≈96.5%，彼此无显著差 → 确认唯一差异=表形。",
    "ITT：回到 assignment 人群分析，曝光完整性作诊断而非筛样本条件。",
    "上线后建 jobs/ss07_analysis/ss07_grouping.py（对齐 ss03_grouping.py），跑分组验证。",
]))

# 7 开放问题
P.append(h(2, "7. 开放问题"))
P.append(tbl(["编号", "问题"], [
    ["OQ-S1", "三张表的真实 math_table_id（上线后填入，用于取数分组）"],
    ["OQ-S2", "default 表形（命中率/分布）具体参数——目前只知 RTP 96.5%"],
    ["OQ-S3", "SS07 实际流量是否达到代理水平；若偏低，14 天是否够"],
    ["OQ-S4", "是否需要第 4 臂常驻 holdout 支持纵向（LTV/D30）测量（PRD Phase 2）"],
    ["OQ-S5", "主指标缩尾档位定 95%（powered）还是 99%（更保守但欠功效）——本方案默认 95%"],
]))

BODY = "\n".join(P)

def load_env():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env

def main(do_publish):
    print(f"HTML 长度 {len(BODY)}；结构图 {DIAG.name}（存在 {DIAG.exists()}）")
    if not do_publish:
        print("(dry-run；加 --publish 实际创建页面)"); return
    env = load_env()
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"]); H = {"X-Atlassian-Token": "no-check"}
    # 已存在则更新，否则创建
    r = requests.get(f"{base}/rest/api/content", auth=auth,
                     params={"spaceKey": SPACE, "title": TITLE, "expand": "version"}, timeout=30)
    ex = r.json().get("results", []) if r.status_code == 200 else []
    if ex:
        pid = ex[0]["id"]; ver = ex[0]["version"]["number"] + 1
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
        print(r.text[:500]); return
    pid = r.json()["id"]
    with open(DIAG, "rb") as f:
        requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                      files={"file": (DIAG_NAME, f, "image/png")}, data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=60)
    print("完成。页面:", f"{base}/spaces/{SPACE}/pages/{pid}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
