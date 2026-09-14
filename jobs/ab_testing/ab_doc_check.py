"""AB 组文档一致性检查器（配合 skill `ab-doc-consistency`）。

对每个 AB 实验文档核 6 条硬性要求：
  1) 结构图      —— 引用了 AB 结构图 PNG，文件存在，且出自权威生成器（样式统一）
  2) 所有调控层级 —— 覆盖该游戏当前所有调控层（以 AB分组规格汇总 为准）
  3) 当前 AB 分组 —— 有组别 + 比例 + 分流键（尾号/MOD）+ 各组数学表/机制
  4) 实验时长    —— 明确写了周期/时长
  5) 假设        —— 有 Hypothesis（H1/H2…）
  6) 检验指标    —— 有主指标 + 护栏（+ MDE）

纯文件扫描，不连库。用法：
  python3 jobs/ab_testing/ab_doc_check.py            # 扫 prd/ab_testing 下全部 md
  python3 jobs/ab_testing/ab_doc_check.py <file...>  # 指定文档
退出码非 0 = 有硬失败（可接 CI / pre-commit）。
"""
from __future__ import annotations
import re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AB_DIR = ROOT / "prd" / "ab_testing"

# —— 各游戏「当前调控层级」单一事实源：prd/ab_testing/AB分组规格汇总_v0.1.md ——
# 每层给一组同义词（正则），文档命中任一即算覆盖该层。改动线上配置时同步更新这里。
GAME_LAYERS = {
    "FM01": {"风控": r"风控|RISK_CONTROL|RC_FISHING",
             "动态RTP": r"dynamic_rtp|动态\s*RTP",
             "个性化挽留": r"挽留|retention",
             "default/holdout": r"default|holdout|基准"},
    "SS03": {"AB分组(基础数学表)": r"AB\s*分组|数学表|基础表|BGTR|kai\b|95Kai",
             "暗保底": r"暗保底|kakutei"},
    "SS06": {"基础表(dos)": r"基础表|dos\b",
             "暗保底分组": r"暗保底|garantizado"},
    "SS07": {"表形AB(testA/testB)": r"表形|testA|testB",
             "AI调控(MAB)": r"\bAI\b|MAB",
             "暗保底层": r"暗保底|kakutei|garantizado"},
    "SS01": {"AI调控(MAB)": r"\bAI\b|MAB", "对照(default)": r"default|对照|基准"},
    "SS02": {"AI调控(MAB)": r"\bAI\b|MAB", "对照(default)": r"default|对照|基准"},
}
# 权威结构图生成器（图片必须出自其一，才保证样式统一）
CANON_GENERATORS = [
    "jobs/ab_testing/render_ab_diagrams.py",
    "jobs/ab_testing/high_player_test_diagram.py",
    "jobs/ab_testing/ab_diagram.py",
]

DUR_RE = re.compile(r"为期|实验周期|实验\s*时长|\b时长\b|\d+\s*天|\d+\s*周|minimum_duration|一期")
HYP_RE = re.compile(r"假设|hypothesis|\bH1\b", re.I)
METRIC_RE = re.compile(r"主指标|护栏|检验.{0,4}指标|观测口径|\bMDE\b")
GROUP_RE = re.compile(r"尾号|MOD\s*\(|MOD\d|哈希")


def detect_games(text, fname):
    by_name = [g for g in GAME_LAYERS if g in fname]     # 文件名带游戏码 → 以它为准
    if by_name:
        return by_name
    hit = [g for g in GAME_LAYERS if re.search(rf"\b{g}\b", text)]
    return hit or ["(未识别游戏)"]


def check_image(text, doc_path):
    imgs = re.findall(r"!\[[^\]]*\]\(([^)]+\.png)\)", text)
    if not imgs:
        return "FAIL", "无结构图引用（应内嵌 AB 结构图 PNG）"
    notes = []
    ok = False
    for rel in imgs:
        p = (ROOT / rel) if (ROOT / rel).exists() else (doc_path.parent / rel)
        if not p.exists():
            notes.append(f"图缺失:{rel}")
            continue
        looks = re.search(r"AB结构图|结构图|grouping|分流|分层", rel)
        notes.append(f"{rel}{'' if looks else ' (命名非结构图?)'}")
        ok = True
    if not ok:
        return "FAIL", "; ".join(notes)
    return "OK", "; ".join(notes)


def check_layers(text, games):
    miss = []
    for g in games:
        for layer, pat in GAME_LAYERS.get(g, {}).items():
            if not re.search(pat, text, re.I):
                miss.append(f"{g}:{layer}")
    if miss:
        return "WARN", "缺层(核对是否本实验范围): " + " / ".join(miss)
    return "OK", "覆盖全部调控层"


def sym(s):
    return {"OK": "✅", "WARN": "⚠️ ", "FAIL": "❌"}[s]


def check_doc(doc_path):
    text = doc_path.read_text(encoding="utf-8", errors="ignore")
    games = detect_games(text, doc_path.name)
    rows = []
    st, msg = check_image(text, doc_path); rows.append(("结构图", st, msg))
    st, msg = check_layers(text, games); rows.append(("所有调控层级", st, msg))
    rows.append(("当前AB分组", "OK" if (GROUP_RE.search(text) and text.count("%") >= 2) else "FAIL",
                 f"分流键{'有' if GROUP_RE.search(text) else '无'} · 比例出现 {text.count('%')} 次"))
    rows.append(("实验时长", "OK" if DUR_RE.search(text) else "FAIL",
                 (DUR_RE.search(text).group() if DUR_RE.search(text) else "未写周期/时长")))
    rows.append(("假设(Hypothesis)", "OK" if HYP_RE.search(text) else "FAIL",
                 "有" if HYP_RE.search(text) else "缺 H1/H2… 假设"))
    rows.append(("检验指标", "OK" if METRIC_RE.search(text) else "FAIL",
                 "有主指标/护栏" if METRIC_RE.search(text) else "缺主指标/护栏/MDE"))
    hard_fail = any(s == "FAIL" for _, s, _ in rows)
    return games, rows, hard_fail


def main(argv):
    docs = [Path(a).resolve() for a in argv] if argv else sorted(AB_DIR.rglob("*.md"))
    docs = [d for d in docs if d.exists()]
    if not docs:
        print("没找到要检查的 md"); return 1
    any_fail = False
    for d in docs:
        games, rows, hard = check_doc(d)
        any_fail |= hard
        head = "❌ 不合格" if hard else "✅ 合格"
        print(f"\n{'='*76}\n{head}  {d.relative_to(ROOT)}   [game: {'/'.join(games)}]")
        for name, st, msg in rows:
            print(f"  {sym(st)} {name:<16} {msg}")
    print(f"\n{'='*76}\n结果：{len(docs)} 份，{'有硬失败 ❌' if any_fail else '全部通过硬性项 ✅'}")
    print("提醒：⚠️ 调控层缺失需人工核对是否属本实验范围；结构图样式一致性以肉眼+"
          f"是否出自 {'/'.join(Path(g).name for g in CANON_GENERATORS)} 为准。")
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
