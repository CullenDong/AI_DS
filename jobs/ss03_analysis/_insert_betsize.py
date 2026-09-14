"""把 md_frag_betsize.md 的 4 个期(各3张投注额分层表)插入 ab_summary_and_plan.md，
放在每期「设计初衷 → 真实作用」块之前。标签由 **[期] 小投注额（..）** 改成斜体子标题。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MD = ROOT / "prd" / "ab_testing" / "math_table" / "ab_summary_and_plan.md"
FRAGBS = ROOT / "data" / "output" / "ss03_cbo" / "md_frag_betsize.md"
MARK = "**设计初衷 → 真实作用**"
HDR = "**按玩家投注额分层（小 <200 / 中 200–2000 / 大 ≥2000 元，按玩家期内总投注额）**"

frag = FRAGBS.read_text(encoding="utf-8")
blocks = re.split(r"<!-- ===== .*?投注额分层 ===== -->", frag)[1:]   # 4 段
assert len(blocks) == 4, f"期数不对: {len(blocks)}"
# 标签：**[..] 小投注额（..）** -> *小投注额（..）*
blocks = [re.sub(r"\*\*\[[^\]]+\]\s*(.*?)\*\*", lambda m: f"*{m.group(1)}*", b).strip() for b in blocks]

md_lines = MD.read_text(encoding="utf-8").splitlines()
marks = [i for i, l in enumerate(md_lines) if l.strip() == MARK]
assert len(marks) == 4, f"设计初衷标记数不对: {len(marks)}"

for i, blk in zip(reversed(marks), reversed(blocks)):
    ins = [HDR, ""] + blk.splitlines() + [""]
    md_lines[i:i] = ins

MD.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
print(f"已插入 4 期投注额分层表（各3张）到 {MD}")
