"""SS03 high_player_test_v1 分组结构图（沿用 render_ab_diagrams.py 的流程图样式）。
两层：第1层 AB分组(MOD user_id 尾号后两位换基础数学表) + 第2层 暗保底(三臂共用)。
输出 data/output/ab_testing/high_player_test_grouping.png（供 Confluence 内嵌）。
"""
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break
OUT = Path(__file__).resolve().parents[2] / "data" / "output" / "ab_testing"; OUT.mkdir(parents=True, exist_ok=True)

C = dict(gray="#eef2f6", grayE="#57606a", gold="#fff3cd", goldE="#d4a72c",
         green="#d1e7dd", greenE="#0f7b3f", blue="#cfe2ff", blueE="#0969da",
         orange="#ffe0cc", orangeE="#d1611a", purple="#f3e8ff", purpleE="#8250df",
         red="#ffd6d6", redE="#cf222e")


def box(ax, x, y, w, h, text, fc, ec, fs=10.5, bold=False, tc="#1f2328"):
    ax.add_patch(FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle="round,pad=0.5,rounding_size=2", fc=fc, ec=ec, lw=1.6))
    ax.text(x, y, text, ha="center", va="center", fontproperties=FP, fontsize=fs, color=tc,
            fontweight="bold" if bold else "normal", linespacing=1.45)


def arr(ax, x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=15, lw=1.3, color="#57606a"))


fig, ax = plt.subplots(figsize=(11, 7)); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
ax.text(50, 97, "SS03 · high_player_test_v1 分组结构（两层）", ha="center", fontproperties=FP,
        fontsize=15.5, fontweight="bold", color="#1f2328")
ax.text(50, 91.5, "第1层 AB分组：按 user_id 尾号后两位(MOD100) 换基础数学表 · 第2层 暗保底：三臂共用 · 为期 21 天",
        ha="center", fontproperties=FP, fontsize=9.5, color="#656d76")

# 全体
box(ax, 50, 85, 42, 6, "SS03 全体 CNY 玩家", C["gray"], C["grayE"], 11, True)
# 第1层 AB 分组标签
box(ax, 50, 75.5, 52, 5.5, "第1层 · AB 分组 —— MOD(user_id,100) 尾号后两位 静态分流", C["gold"], C["goldE"], 10, True, "#8a5a00")
arr(ax, 50, 82, 50, 78.3)
# 三组
box(ax, 24, 60, 27, 14, "A · AB_TEST\n尾号 00-29 · 30%\nBGTR97_Saitekika_v3\nRTP 96.9%（高RTP·高BG）",
    C["blue"], C["blueE"], 9, True, "#084298")
box(ax, 50, 60, 27, 14, "B · AB_TEST\n尾号 30-59 · 30%\nBGTR95_HMM_Highvalue_v1\nRTP 95.2%（低波动·高价值训练）",
    C["orange"], C["orangeE"], 8.6, True, "#8a3b0a")
box(ax, 76, 60, 27, 14, "default\n尾号 60-99 · 40%\n95Kai（现行）\nRTP 94.9%（对照）",
    C["green"], C["greenE"], 9, True, "#0f5132")
for tx in [24, 50, 76]:
    arr(ax, 50-((50-tx)*0.33), 72.7, tx, 67.2)
# 第2层 暗保底
box(ax, 50, 40, 72, 12, "第2层 · 暗保底（三臂共用同一套）\n累计达档 → 应用確定表 normal_kakuteiC\n触发保底赔付",
    C["red"], C["redE"], 10, True, "#a40e26")
for tx in [24, 50, 76]:
    arr(ax, tx, 53, 50-((50-tx)*0.55), 46.2)
# 对比
box(ax, 50, 19, 78, 8, "对比（核心人群=高价值前10%）：各臂基础表玩家向表现 · p90 total_bet/num_bet · D1/D3/D7 + 暗保底触发情况",
    C["purple"], C["purpleE"], 9, False, "#5a32a3")
arr(ax, 50, 34, 50, 23.2)

p = OUT / "high_player_test_grouping.png"
fig.savefig(p, dpi=150, bbox_inches="tight", facecolor="white"); plt.close(fig)
print("saved", p)
