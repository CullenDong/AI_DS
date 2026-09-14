"""渲染 SS07 新版分组结构图(暗保底方案A全体 + MOD100 + default/testA/testB/AI(主+2 Micro-MAB))。
同时输出 SS07_分组结构图.png(独立文档用) 和 AB结构图_SS07.png(汇总用)。
"""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from pathlib import Path

FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break
OUT = Path(__file__).resolve().parents[2] / "prd" / "ab_testing"
C = dict(gray="#eef2f6", grayE="#57606a", gold="#fff3cd", goldE="#d4a72c", green="#d1e7dd", greenE="#0f7b3f",
         blue="#cfe2ff", blueE="#0969da", orange="#ffe0cc", orangeE="#d1611a", purple="#f3e8ff", purpleE="#8250df",
         red="#ffd6d6", redE="#cf222e")

def box(ax, x, y, w, h, t, fc, ec, fs=9, bold=True, tc="#1f2328"):
    ax.add_patch(FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle="round,pad=0.5,rounding_size=2", fc=fc, ec=ec, lw=1.5))
    ax.text(x, y, t, ha="center", va="center", fontproperties=FP, fontsize=fs, color=tc, fontweight="bold" if bold else "normal", linespacing=1.4)
def arr(ax, x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.3, color="#57606a"))

fig, ax = plt.subplots(figsize=(11, 7.6)); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
ax.text(50, 97, "SS07 · AB 分组结构（v0.2）", ha="center", fontproperties=FP, fontsize=15.5, fontweight="bold", color="#1f2328")
ax.text(50, 92.5, "暗保底方案A覆盖全体(非分组) · MOD(user_id,100) 按比例随机分配 · 静态终身稳定", ha="center", fontproperties=FP, fontsize=8.5, color="#656d76")
# 暗保底 banner(全体覆盖)
box(ax, 50, 87, 92, 5, "暗保底 · 方案A（全体覆盖 · 常开背景 · 非分组）", C["red"], C["redE"], 9.5, True, "#a40e26")
box(ax, 50, 79, 66, 5, "SS07 全体 CNY 玩家 · MOD(user_id,100) 按比例随机分配", C["gray"], C["grayE"], 9.5, True)
arr(ax, 50, 84.5, 50, 81.5)
# 4 臂
box(ax, 15, 68, 20, 8, "default\n30%\n现行表形·基准", C["green"], C["greenE"], 9, True, "#0f5132")
box(ax, 37, 68, 20, 8, "testA\n20%\n命中20%·低波动", C["blue"], C["blueE"], 8.8, True, "#084298")
box(ax, 59, 68, 20, 8, "testB\n20%\n命中15%·中波动", C["orange"], C["orangeE"], 8.8, True, "#8a3b0a")
box(ax, 83, 68, 22, 8, "AI 调控\n30%\n(MAB)", C["purple"], C["purpleE"], 9, True, "#5a32a3")
for tx in [15, 37, 59, 83]:
    arr(ax, 50-((50-tx)*0.32), 76.5, tx, 72.2)
# AI 三子组(AI 下方纵向)
box(ax, 83, 55, 30, 6, "AI 主组 20% · 主 MAB", C["purple"], C["purpleE"], 8.6, True, "#5a32a3")
box(ax, 83, 47, 30, 6, "Micro-MAB #1 · 5%\nFirst-Arm causal", C["purple"], "#a978e0", 8, True, "#5a32a3")
box(ax, 83, 39, 30, 6, "Micro-MAB #2 · 5%\nReward Function", C["purple"], "#a978e0", 8, True, "#5a32a3")
arr(ax, 83, 64, 83, 58); arr(ax, 83, 52, 83, 50); arr(ax, 83, 44, 83, 42)
# 说明
box(ax, 35, 30, 60, 12, "静态数学表实验(default/testA/testB):\n同 96.5% RTP,唯一差异=表形(命中率+倍率分布)\n主对比 H1 testA vs default · H2 testB vs default",
    C["gold"], C["goldE"], 8.4, False, "#8a5a00")
box(ax, 50, 14, 92, 6, "暗保底方案A = 所有臂共同背景(非分组、不设对照);各臂唯一差异 = 数学表表形 / AI 调控。AI(MAB)是动态调控,评估口径与静态表不同。",
    C["gray"], C["grayE"], 8, False)
plt.savefig(OUT / "SS07_分组结构图.png", dpi=150, bbox_inches="tight", facecolor="white")
plt.savefig(OUT / "AB结构图_SS07.png", dpi=150, bbox_inches="tight", facecolor="white")
plt.close(fig)
print("saved SS07_分组结构图.png + AB结构图_SS07.png")
