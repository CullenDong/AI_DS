"""数学表体感测试与优化流程图（沿用 render_ab_diagrams.py 的流程图样式）。
五阶段自顶向下：市场定位 → 体感操作化 → 数学设计 → 盲测验证 → 判定/上线，
右侧标各阶段产出，含未达标回退（第五步→第三/二步）与上线 A/B 出口。
输出 data/output/math_table/feel_test_flow.png（供文档/Confluence 内嵌）。
"""
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break
OUT = Path(__file__).resolve().parents[2] / "data" / "output" / "math_table"; OUT.mkdir(parents=True, exist_ok=True)

C = dict(gray="#eef2f6", grayE="#57606a", gold="#fff3cd", goldE="#d4a72c",
         green="#d1e7dd", greenE="#0f7b3f", blue="#cfe2ff", blueE="#0969da",
         orange="#ffe0cc", orangeE="#d1611a", purple="#f3e8ff", purpleE="#8250df",
         red="#ffd6d6", redE="#cf222e")


def box(ax, x, y, w, h, text, fc, ec, fs=10, bold=False, tc="#1f2328"):
    ax.add_patch(FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle="round,pad=0.5,rounding_size=2", fc=fc, ec=ec, lw=1.6))
    ax.text(x, y, text, ha="center", va="center", fontproperties=FP, fontsize=fs, color=tc,
            fontweight="bold" if bold else "normal", linespacing=1.4)


def arr(ax, x1, y1, x2, y2, color="#57606a", ls="-"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=15, lw=1.4, color=color, linestyle=ls))


def note(ax, x, y, text, tc="#656d76", fs=8.4):
    ax.text(x, y, text, ha="left", va="center", fontproperties=FP, fontsize=fs, color=tc, linespacing=1.35)


fig, ax = plt.subplots(figsize=(12, 9.2)); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
ax.text(50, 97.5, "数学表体感测试与优化流程", ha="center", fontproperties=FP, fontsize=16, fontweight="bold", color="#1f2328")
ax.text(50, 93.2, "方法论范式：构念 → 操作化 → 测量 → 验证（construct → operationalization → measurement → validation）",
        ha="center", fontproperties=FP, fontsize=9.5, color="#656d76")

CX = 38                       # 主流程中轴（左移，右侧留给产出注解）
W = 46
steps = [
    (85, C["gray"],   C["grayE"],   "① 市场调研 · 用户画像 · 产品定位",
     "产出：目标受众界定 + 产品定位报告\n· 竞品体感基线　· 分层画像(HMM/日龄/投注额)　· 定位陈述+波动取向"),
    (68, C["gold"],   C["goldE"],   "② 体感需求操作化",
     "产出：分受众《体感需求操作化表》\n构念→数学指标→目标区间：命中率 / 存活发数 / dry streak /\n大奖接触频率 / 波动 …（受 RTP 护栏约束，标 P0/P1/P2）"),
    (51, C["blue"],   C["blueE"],   "③ 数学设计（体感约束 + RTP 护栏）",
     "产出：候选数学表 ×N + 各表《设计说明》\n· CBO / Esscher / 蒙特卡洛模拟　· 一表=一组体感假设　· 模拟自检达标方进盲测"),
    (34, C["orange"], C["orangeE"], "④ 盲测验证（跨职能受试 · 双测量层）",
     "产出：主观评分 + 客观指标 + 主客观效度\n· 盲法+顺序配平　· 标准化投注剖面 P1/P2/P3　· 主观量表 Q1–Q8　· 逐发埋点"),
    (17, C["purple"], C["purpleE"], "⑤ 判定 · 迭代 · 上线真实 A/B",
     "四准则：客观达标 · 主观达标 · 主客观效度 · RTP 护栏\n达标 → 线上同期 A/B（ab-experiment-design）"),
]
ys = [s[0] for s in steps]
for i, (y, fc, ec, title, sub) in enumerate(steps):
    box(ax, CX, y, W, 8.4, title, fc, ec, 11, True,
        {"#d4a72c": "#8a5a00", "#0969da": "#084298", "#d1611a": "#8a3b0a", "#8250df": "#5a32a3"}.get(ec, "#1f2328"))
    note(ax, CX + W/2 + 2.5, y, sub)
    if i < len(steps) - 1:
        arr(ax, CX, y - 4.2, CX, ys[i+1] + 4.2)

# 出口：上线 A/B
box(ax, CX, 5.5, 30, 5.5, "线上真实 A/B 实验\n（业务效度确认）", C["green"], C["greenE"], 9.5, True, "#0f5132")
arr(ax, CX, 12.8, CX, 8.3, color="#0f7b3f")

# 回退环：⑤ → ③（重设计）、⑤ → ②（修目标区间）
arr(ax, CX - W/2 - 0.5, 18.5, 8, 51, color="#cf222e", ls=(0, (5, 3)))
ax.text(6.2, 36, "未达标\n回退重设计", ha="center", va="center", fontproperties=FP, fontsize=8, color="#a40e26", linespacing=1.3)
arr(ax, 8, 55, CX - W/2 - 0.5, 66.5, color="#cf222e", ls=(0, (5, 3)))
ax.text(6.2, 70, "修正\n目标区间", ha="center", va="center", fontproperties=FP, fontsize=8, color="#a40e26", linespacing=1.3)

ax.text(50, 1.2, "盲测验证 = 上线真实 A/B 前的低成本前置筛选与量表校准，不替代线上因果验证",
        ha="center", fontproperties=FP, fontsize=8.2, color="#8250df")

p = OUT / "feel_test_flow.png"
fig.savefig(p, dpi=150, bbox_inches="tight", facecolor="white"); plt.close(fig)
print("saved", p)
