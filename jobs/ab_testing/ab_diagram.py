"""渲染挽留 AB 分流分层图（PNG，中文），供 Word/HTML 文档内嵌。

全体 → 风控?(含农场) → 出局 / 实验人群 → dynamic_rtp臂 / 挽留AB → HMM 4状态 × OFF|ON。
输出：prd/ab_testing/分流分层图.png
"""
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib import font_manager as fm

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "prd" / "ab_testing" / "分流分层图.png"

# 中文字体（macOS PingFang）
FP = None
for p in ["/System/Library/Fonts/PingFang.ttc",
          "/System/Library/Fonts/STHeiti Medium.ttc",
          "/Library/Fonts/Arial Unicode.ttf"]:
    if Path(p).exists():
        FP = fm.FontProperties(fname=p); break

C = {"pop": "#DCE9FB", "risk": "#F8D7DA", "out": "#EDEDED",
     "dyn": "#FFE8CC", "rab": "#D6F0DD", "hmm": "#E7DBF5", "cell": "#FFF3B0"}

fig, ax = plt.subplots(figsize=(12, 8.8))
ax.set_xlim(0, 12); ax.set_ylim(0, 11.3); ax.axis("off")

def box(x, y, w, h, text, fill, fs=11, bold=False):
    ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h,
                 boxstyle="round,pad=0.03,rounding_size=0.12",
                 fc=fill, ec="#444", lw=1.2))
    ax.text(x, y, text, ha="center", va="center", fontproperties=FP,
            fontsize=fs, fontweight="bold" if bold else "normal", color="#1f2328")

def arrow(x1, y1, x2, y2, label=None, lx=0, ly=0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
                 arrowstyle="-|>", mutation_scale=15, lw=1.3, color="#555"))
    if label:
        ax.text((x1 + x2) / 2 + lx, (y1 + y2) / 2 + ly, label, ha="center",
                va="center", fontproperties=FP, fontsize=9.5, color="#b02a37")

# 节点
box(6, 10.6, 3.0, 0.7, "全体活跃玩家", C["pop"], fs=12, bold=True)
box(6, 9.3, 3.4, 0.7, "风控?（含套利农场）", C["risk"], fs=11)
box(10.3, 9.3, 3.0, 0.7, "风控组\nsticky 锁定 · 出局", C["out"], fs=10)
box(6, 7.9, 3.0, 0.7, "实验人群", C["pop"], fs=12, bold=True)

# 价值干预层：三臂 dynamic_rtp / 个性化挽留 / default
box(2.2, 6.3, 3.2, 0.95, "dynamic_rtp 臂\n单独 RTP 实验（排除）", C["dyn"], fs=10)
box(6.0, 6.3, 3.0, 0.95, "个性化挽留 retention\n挽留 ON（Variant）", C["rab"], fs=10, bold=True)
box(9.8, 6.3, 3.0, 0.95, "default\nholdout · 挽留 OFF（Control）", C["out"], fs=10, bold=True)

box(7.9, 4.9, 4.8, 0.65, "HMM 状态分层（retention + default 两臂，处理无关）", C["hmm"], fs=9.5)

# 四状态 + OFF|ON
states = [("T1\n首日", 1.5), ("S1\nlow", 4.5), ("S2\nengaged", 7.5), ("S3\nescaped", 10.5)]
for name, x in states:
    box(x, 3.4, 2.2, 0.8, name, C["hmm"], fs=10, bold=True)
    box(x, 2.1, 2.2, 0.7, "OFF  |  ON", C["cell"], fs=10)

# 箭头
arrow(6, 10.25, 6, 9.68)
arrow(7.7, 9.3, 8.8, 9.3, "是", ly=0.28)
arrow(6, 8.92, 6, 8.28, "否", lx=0.28)
arrow(5.2, 7.58, 2.6, 6.82)            # 实验人群 -> dynamic
arrow(6, 7.55, 6, 6.82)                # 实验人群 -> retention
arrow(6.8, 7.58, 9.4, 6.82)            # 实验人群 -> default
arrow(6.0, 5.82, 7.3, 5.28)            # retention -> HMM (ON)
arrow(9.8, 5.82, 8.5, 5.28)            # default -> HMM (OFF)
for name, x in states:
    arrow(7.9, 4.55, x, 3.82)          # HMM -> 各状态
    arrow(x, 2.98, x, 2.48)            # 状态 -> OFF|ON

ax.text(6, 0.85, "挽留 AB = retention(挽留 ON / Variant) vs default(holdout · 挽留 OFF / Control)，两臂均不吃 RTP；"
        "按 HMM 状态分层后每个状态内比 ON vs OFF。dynamic_rtp 吃 RTP，单独实验、不进挽留 AB。",
        ha="center", va="center", fontproperties=FP, fontsize=9, color="#656d76")

plt.tight_layout()
fig.savefig(OUT, dpi=150, bbox_inches="tight", facecolor="white")
print("saved:", OUT)
