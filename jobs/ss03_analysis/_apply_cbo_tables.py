"""把 ab_summary_and_plan.md 里的 16 张 CBO 表按文档顺序 1:1 替换为 md_fragments.md 生成的新版。
表块 = 连续以 '|' 开头的行。两文件表块顺序一致（P0总体/new/beg/old, P1..., P2..., P3...）。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MD = ROOT / "prd" / "ab_testing" / "math_table" / "ab_summary_and_plan.md"
FRAG = ROOT / "data" / "output" / "ss03_cbo" / "md_fragments.md"


def table_blocks(lines):
    """返回 [(start_idx, end_idx_exclusive, block_lines)]，块=极大连续 '|' 行。"""
    blocks, i, n = [], 0, len(lines)
    while i < n:
        if lines[i].lstrip().startswith("|"):
            j = i
            while j < n and lines[j].lstrip().startswith("|"):
                j += 1
            blocks.append((i, j, lines[i:j]))
            i = j
        else:
            i += 1
    return blocks


md_lines = MD.read_text(encoding="utf-8").splitlines()
frag_lines = FRAG.read_text(encoding="utf-8").splitlines()

HDR = "| 组（表）"   # 只认 CBO 真实 A/B 表，跳过时间线表/模拟数据表
FRAGBS = ROOT / "data" / "output" / "ss03_cbo" / "md_frag_betsize.md"
bs_lines = FRAGBS.read_text(encoding="utf-8").splitlines()

md_blocks = [b for b in table_blocks(md_lines) if b[2][0].startswith(HDR)]
tot = [b[2] for b in table_blocks(frag_lines) if b[2][0].startswith(HDR)]      # 16: 4期×(总体+3日龄)
bs = [b[2] for b in table_blocks(bs_lines) if b[2][0].startswith(HDR)]         # 12: 4期×3投注额
assert len(tot) == 16 and len(bs) == 12, f"片段表数不对 tot={len(tot)} bs={len(bs)}"
# 按 md 文档顺序交织：每期 [总体,new,beg,old, 小,中,大]
new_tables = []
for p in range(4):
    new_tables += tot[p*4:(p+1)*4] + bs[p*3:(p+1)*3]
print(f"md 表块 {len(md_blocks)}；新表 {len(new_tables)}")
assert len(md_blocks) == len(new_tables) == 28, "表块数不匹配，中止"

# 从后往前替换，避免下标漂移
out = md_lines[:]
for (s, e, _), new in zip(reversed(md_blocks), reversed(new_tables)):
    out[s:e] = new

MD.write_text("\n".join(out) + "\n", encoding="utf-8")
print(f"已替换 {len(new_tables)} 张表并写回", MD)
