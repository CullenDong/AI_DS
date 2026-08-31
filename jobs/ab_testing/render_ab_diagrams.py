"""渲染四个游戏(FM01/SS03/SS06/SS07)的 AB 分组结构图 PNG，供汇总文档内嵌。
输出 prd/ab_testing/AB结构图_<game>.png。不连库，纯绘图。
"""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from pathlib import Path

FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break
OUT = Path(__file__).resolve().parents[2] / "prd" / "ab_testing"

C = dict(gray="#eef2f6", grayE="#57606a", gold="#fff3cd", goldE="#d4a72c",
         green="#d1e7dd", greenE="#0f7b3f", blue="#cfe2ff", blueE="#0969da",
         orange="#ffe0cc", orangeE="#d1611a", purple="#f3e8ff", purpleE="#8250df",
         red="#ffd6d6", redE="#cf222e")

def canvas():
    fig, ax = plt.subplots(figsize=(11, 6.8)); ax.set_xlim(0, 100); ax.set_ylim(0, 100); ax.axis("off")
    return fig, ax
def box(ax, x, y, w, h, text, fc, ec, fs=10.5, bold=False, tc="#1f2328"):
    ax.add_patch(FancyBboxPatch((x-w/2, y-h/2), w, h, boxstyle="round,pad=0.5,rounding_size=2", fc=fc, ec=ec, lw=1.6))
    ax.text(x, y, text, ha="center", va="center", fontproperties=FP, fontsize=fs, color=tc,
            fontweight="bold" if bold else "normal", linespacing=1.45)
def arr(ax, x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=15, lw=1.3, color="#57606a"))
def title(ax, t, sub):
    ax.text(50, 97, t, ha="center", fontproperties=FP, fontsize=15.5, fontweight="bold", color="#1f2328")
    ax.text(50, 91.5, sub, ha="center", fontproperties=FP, fontsize=9.5, color="#656d76")
def save(fig, name):
    fig.savefig(OUT / name, dpi=150, bbox_inches="tight", facecolor="white"); plt.close(fig)
    print("saved", name)

# ---------------- FM01 捕鱼 ----------------
fig, ax = canvas()
title(ax, "FM01 捕鱼 · AB 分组结构", "风控管理优先剔除(含反欺诈/多开/套利) · 其余按 策略/user_id尾号 分组")
box(ax, 50, 85, 40, 6.5, "全体活跃玩家 (by user_id)", C["gray"], C["grayE"], 11, True)
box(ax, 20, 69, 30, 13, "风控管理 (sticky)\n反欺诈·多开·套利\n人工可拉入·锁定\n→ 剔除出局", C["red"], C["redE"], 9, True, "#a40e26")
box(ax, 62, 69, 28, 13, "其余玩家\n→ 实验人群", C["gray"], C["grayE"], 10.5, True)
arr(ax, 42, 81.5, 24, 76); arr(ax, 56, 81.5, 62, 76)
box(ax, 22, 50, 26, 11, "dynamic_rtp\n尾号(策略)\nRTP 动态优化", C["blue"], C["blueE"], 9.5, True, "#084298")
box(ax, 50, 50, 26, 11, "retention 挽留\n尾号 0/1\n挽留 ON", C["green"], C["greenE"], 9.5, True, "#0f5132")
box(ax, 78, 50, 26, 11, "default\n尾号 2-9\nholdout / 基准", C["gray"], C["grayE"], 9.5, True)
arr(ax, 55, 64.5, 30, 56); arr(ax, 62, 64.5, 52, 56); arr(ax, 69, 64.5, 76, 56)
box(ax, 50, 29, 80, 9, "挽留 AB：retention(ON) vs default(OFF·holdout)，均不吃 RTP\n按 HMM 状态分层(T1/S1/S2/S3) 比 D1/D3/D7 留存 (ITT)",
    C["purple"], C["purpleE"], 9.5, False, "#5a32a3")
arr(ax, 50, 44.5, 50, 33.8)
box(ax, 50, 13, 62, 6, "注：dynamic_rtp 吃 RTP 会混淆挽留效果，排除在挽留 AB 之外", C["gold"], C["goldE"], 8.5, False)
arr(ax, 50, 24.5, 50, 16.2)
save(fig, "AB结构图_FM01.png")

# ---------------- SS03 (两层：AB分组 + 暗保底；层标签写进框内，无侧浮字) ----------------
fig, ax = canvas()
title(ax, "SS03 · 分组结构（两层）", "第1层 AB分组：换基础数学表 · 第2层 暗保底：所有臂共用")
box(ax, 50, 86, 40, 6, "SS03 全体 CNY 玩家", C["gray"], C["grayE"], 11, True)
box(ax, 50, 76, 46, 5.5, "第1层 · AB 分组 —— MOD(user_id,10) 静态分流", C["gold"], C["goldE"], 10, True, "#8a5a00")
arr(ax, 50, 82.8, 50, 79)
box(ax, 20, 62, 21, 11, "Default\n尾号 0-3 · 40%\n用 95kai", C["green"], C["greenE"], 9.5, True, "#0f5132")
box(ax, 44, 62, 21, 11, "AB_TEST_A\n尾号 4-5 · 20%\n用 BGTR97_v2", C["blue"], C["blueE"], 9, True, "#084298")
box(ax, 68, 62, 21, 11, "AB_TEST_B\n尾号 6-7 · 20%\n用 BGTR95_v3", C["orange"], C["orangeE"], 9, True, "#8a3b0a")
box(ax, 88, 62, 18, 11, "AI\n尾号 8-9 · 20%\n用 混合表", C["purple"], C["purpleE"], 9, True, "#5a32a3")
for tx in [20, 44, 68, 88]:
    arr(ax, 50-((50-tx)*0.33), 73, tx, 68)
box(ax, 50, 42, 70, 12, "第2层 · 暗保底（所有臂共用同一套）\n累计达档 → 应用確定表 normal_kakuteiC\n触发保底赔付", C["red"], C["redE"], 10, True, "#a40e26")
for tx in [20, 44, 68, 88]:
    arr(ax, tx, 56, 50-((50-tx)*0.55), 48.3)
box(ax, 50, 20, 62, 7, "对比：各臂基础表玩家向表现 + 暗保底触发情况", C["purple"], C["purpleE"], 9.5, False, "#5a32a3")
arr(ax, 50, 36, 50, 23.8)
save(fig, "AB结构图_SS03.png")

# ---------------- SS06 (暗保底分组 A/B) ----------------
fig, ax = canvas()
title(ax, "SS06 · 暗保底分组", "暗保底(garantizado)方案 A/B · 按 MOD(user_id,10) 切 · dos 为全员基础表")
box(ax, 50, 85, 46, 6.5, "SS06 全体 CNY 玩家 (基础表 dos)", C["gray"], C["grayE"], 11, True)
box(ax, 50, 73, 42, 7, "暗保底分组 —— MOD(user_id,10) 分流", C["gold"], C["goldE"], 10.5, True)
arr(ax, 50, 81.5, 50, 76.8)
box(ax, 22, 49, 26, 14, "holdout · 尾号 0-1\n20%\n无暗保底\n(仅玩 dos)", C["gray"], C["grayE"], 9.5, True)
box(ax, 50, 49, 26, 14, "暗保底方案 A\n尾号 2-5 · 40%\ngarantizado_a", C["blue"], C["blueE"], 9.5, True, "#084298")
box(ax, 78, 49, 26, 14, "暗保底方案 B\n尾号 6-9 · 40%\ngarantizado_b", C["orange"], C["orangeE"], 9.5, True, "#8a3b0a")
arr(ax, 42, 69.5, 26, 56.5); arr(ax, 50, 69.5, 50, 56.5); arr(ax, 58, 69.5, 74, 56.5)
box(ax, 50, 28, 76, 8, "对比 暗保底 A vs B（及 vs holdout）：保底触发体验 · 投注行为 · 留存",
    C["purple"], C["purpleE"], 9.5, False, "#5a32a3")
arr(ax, 50, 42, 50, 32)
box(ax, 50, 13, 66, 7, "注：a∩b=0 干净分流；暗保底叠加在基础表 dos 上（功能层，非独立游戏）", C["gold"], C["goldE"], 8.5, False)
arr(ax, 50, 24, 50, 16.5)
save(fig, "AB结构图_SS06.png")

# ---------------- SS07 ----------------
fig, ax = canvas()
title(ax, "SS07 · AB 分组结构 (方案 v0.1)", "同 RTP 96.5% · 仅表形(命中率+payout分布)不同 · MOD(user_id,10) 静态分流")
box(ax, 50, 84, 44, 8, "SS07 全体 CNY 玩家 (上线后)", C["gray"], C["grayE"], 11, True)
box(ax, 50, 70, 40, 8, "MOD(user_id,10) 静态哈希", C["gold"], C["goldE"], 10.5, True)
arr(ax, 50, 80, 50, 74)
box(ax, 22, 49, 26, 15, "default 40%\n尾号 0-3\n现行表形\n基准", C["green"], C["greenE"], 10, True, "#0f5132")
box(ax, 50, 49, 26, 15, "testA 30%\n尾号 4-6\n命中20%·低波动\n小奖密集(1-5x)", C["blue"], C["blueE"], 9.5, True, "#084298")
box(ax, 78, 49, 26, 15, "testB 30%\n尾号 7-9\n命中15%·中波动\n中奖为主(2-10x)", C["orange"], C["orangeE"], 9.5, True, "#8a3b0a")
arr(ax, 42, 66, 26, 57); arr(ax, 50, 66, 50, 57); arr(ax, 58, 66, 74, 57)
box(ax, 50, 28, 78, 8, "主对比(ITT·缩尾均值+中位)：H1 testA vs default · H2 testB vs default · H3 testA vs testB",
    C["purple"], C["purpleE"], 9.5, False, "#5a32a3")
arr(ax, 50, 44, 50, 32)
box(ax, 50, 13, 66, 7, "护栏：三臂实测RTP≈96.5% · 实测命中率复现 · D1/D3/D7 留存 · MDE 20% · 14天", C["gold"], C["goldE"], 8.5, False)
arr(ax, 50, 24, 50, 16.5)
save(fig, "AB结构图_SS07.png")

# ---------------- SS03 Future（调控层 FTUE×数学表 factorial ⊥时序⊥ 暗保底）----------------
fig = plt.figure(figsize=(11, 10.2)); fig.patch.set_facecolor("white")
ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off"); ax.set_xlim(0, 100); ax.set_ylim(0, 100)
ax.text(50, 98, "SS03 · Future 结构（提案 v0.2）", ha="center", fontproperties=FP, fontsize=15, fontweight="bold", color="#1f2328")
ax.text(50, 94.7, "明保底=全体活动(常开·非实验) · 调控层(FTUE×数学表 factorial) 与 暗保底 分时序（不同时跑·互相冻结）",
        ha="center", fontproperties=FP, fontsize=8, color="#656d76")
box(ax, 50, 90, 42, 4, "SS03 全体 CNY 玩家", C["gray"], C["grayE"], 10, True)
box(ax, 50, 84, 86, 4.5, "明保底 · 全体活动（常开 · 非实验 · 所有人都有，不设对照）", C["orange"], C["orangeE"], 9.5, True, "#8a3b0a")
arr(ax, 50, 88, 50, 86.3)
ax.text(50, 78.5, "▼ 调控层 与 暗保底 分时序（不同时跑）", ha="center", fontproperties=FP, fontsize=8.5, fontweight="bold", color="#5a32a3")
# —— 期① 调控层：FTUE × 数学表 factorial ——
box(ax, 50, 74.5, 88, 4, "期① · 调控层实验 = FTUE × 数学表（factorial，合在一起）　❄ 冻结：暗保底", C["purple"], C["purpleE"], 8.6, True, "#5a32a3")
cols = [38, 52, 66, 80]; colhead = ["Default\n95kai", "A · 97%", "B · 95%", "AI · 混合"]
for cx, ch in zip(cols, colhead):
    box(ax, cx, 68.5, 13, 4, ch, "#eef2f6", "#8a94a0", 8, True, "#3a4149")
ax.text(15, 63.5, "FTUE\nvariant 80%", ha="center", va="center", fontproperties=FP, fontsize=7.6, fontweight="bold", color="#084298", linespacing=1.3)
ax.text(15, 58.5, "FTUE\ncontrol 20%", ha="center", va="center", fontproperties=FP, fontsize=7.6, fontweight="bold", color="#3a4149", linespacing=1.3)
for cx, tag in zip(cols, ["V×Def", "V×A", "V×B", "V×AI"]):
    box(ax, cx, 63.5, 13, 4.6, tag, C["blue"], C["blueE"], 8.2, True, "#084298")
for cx, tag in zip(cols, ["C×Def", "C×A", "C×B", "C×AI"]):
    box(ax, cx, 58.5, 13, 4, tag, C["gray"], C["grayE"], 8, True)
arr(ax, 50, 72.4, 50, 70.7)
# —— 期② 暗保底 ——
box(ax, 50, 48, 88, 4, "期② · 暗保底实验（十位切对照）　❄ 冻结：调控层（FTUE + 数学表 保持默认）", C["red"], "#cf6a6a", 8.6, True, "#a40e26")
box(ax, 34, 41.5, 48, 5, "有暗保底 · 80%　累计达档 → normal_kakuteiC", C["red"], C["redE"], 8.3, True, "#a40e26")
box(ax, 81, 41.5, 24, 5, "无暗保底 对照 · 20%", C["gray"], C["grayE"], 8.3, True)
arr(ax, 40, 46, 34, 44); arr(ax, 60, 46, 81, 44)
# 时间轴（期①→期②）
ax.annotate("", xy=(6, 46), xytext=(6, 66), arrowprops=dict(arrowstyle="-|>", lw=1.5, color="#8a94a0"))
ax.text(3.3, 56, "时\n间", ha="center", va="center", fontproperties=FP, fontsize=8, color="#57606a", linespacing=1.2)
# 分析
box(ax, 50, 29, 88, 7,
    "分析：\n"
    "期① 调控层 —— FTUE×数学表 factorial：FTUE主效应(行)·数学表主效应(列)·交互·carryover\n"
    "期② 暗保底 —— 独立 AB：有 vs 无对照。两期分时序 → 互不混淆",
    C["purple"], C["purpleE"], 8, False, "#5a32a3")
arr(ax, 50, 39, 50, 32.7)
box(ax, 50, 15, 90, 6.5, "明保底全体常开为背景；user_id 正交哈希（数学表=末位·FTUE=独立位·暗保底=十位）；\n对照各 20%；风控独立前置（命中剔除，见现状口径）",
    C["gold"], C["goldE"], 7.9, False)
arr(ax, 50, 25.4, 50, 18.4)
save(fig, "AB结构图_SS03_future.png")

# ---------------- FM01 捕鱼 Future（FTUE × 调控 factorial；风控脱离分组，仅影响子弹策略）----------------
fig = plt.figure(figsize=(11, 9.8)); fig.patch.set_facecolor("white")
ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off"); ax.set_xlim(0, 100); ax.set_ylim(0, 100)
ax.text(50, 98, "FM01 捕鱼 · Future 结构（提案 v0.2）", ha="center", fontproperties=FP, fontsize=15, fontweight="bold", color="#1f2328")
ax.text(50, 95, "分组 = FTUE × 调控(dynamic_rtp/个性化挽留/default) factorial；风控脱离分组、只影响子弹策略",
        ha="center", fontproperties=FP, fontsize=8, color="#656d76")
# 分组主流程（无风控 gate）
box(ax, 40, 91, 40, 4, "全体活跃玩家 (by user_id)", C["gray"], C["grayE"], 9.5, True)
box(ax, 24, 84, 28, 4, "纯新玩家 · 首登", C["blue"], C["blueE"], 9, True, "#084298")
box(ax, 56, 84, 26, 4, "老玩家（无 FTUE）", C["gray"], C["grayE"], 8.8, True)
arr(ax, 34, 89, 26, 86.2); arr(ax, 46, 89, 55, 86.2)
box(ax, 24, 77.5, 46, 4, "两个正交分配：FTUE 臂(V 80%/C 20%) × 调控臂(user_id)", C["gold"], C["goldE"], 8, True, "#8a5a00")
arr(ax, 24, 82, 24, 79.6)
# —— FTUE × 调控 网格 2×3 ——
ax.text(46, 71.5, "调控（价值干预）＝ 网格列", ha="center", fontproperties=FP, fontsize=8.2, fontweight="bold", color="#3a4149")
cols = [30, 47, 64]; colhead = ["dynamic_rtp\nRTP 动态优化", "个性化挽留\n挽留 ON", "default\nholdout/基准"]
for cx, ch in zip(cols, colhead):
    box(ax, cx, 68, 16, 4.2, ch, "#eef2f6", "#8a94a0", 7.8, True, "#3a4149")
ax.text(11, 62.5, "FTUE\nvariant 80%", ha="center", va="center", fontproperties=FP, fontsize=7.5, fontweight="bold", color="#084298", linespacing=1.3)
ax.text(11, 57, "FTUE\ncontrol 20%", ha="center", va="center", fontproperties=FP, fontsize=7.5, fontweight="bold", color="#3a4149", linespacing=1.3)
for cx, tag in zip(cols, ["V×Dyn", "V×挽留", "V×Def"]):
    box(ax, cx, 62.5, 16, 5, tag, C["blue"], C["blueE"], 8.2, True, "#084298")
for cx, tag in zip(cols, ["C×Dyn", "C×挽留", "C×Def"]):
    box(ax, cx, 57, 16, 4, tag, C["gray"], C["grayE"], 7.8, True)
arr(ax, 26, 75.4, 42, 70.5)
# —— 风控：脱离分组的独立块（右侧）——
box(ax, 87, 76, 22, 15, "风控（独立于分组）\n\n· 不以分组形式存在\n· 不改玩家 AB 组\n· 只影响子弹策略\n  (命中→改子弹策略)\n\n含反欺诈/多开/套利\n·人工，sticky",
    C["red"], C["redE"], 7.6, True, "#a40e26")
ax.annotate("", xy=(72, 60), xytext=(87, 68), arrowprops=dict(arrowstyle="-|>", lw=1.3, color="#cf6a6a", linestyle=(0, (4, 3))))
ax.text(83, 63.5, "只改子弹策略\n不改分组", ha="center", va="center", fontproperties=FP, fontsize=7, color="#a40e26", linespacing=1.3)
# 分析
box(ax, 50, 42, 90, 8.5,
    "分析（factorial 拆解 · 全 ITT）：\n"
    "FTUE 主效应(行) · 调控主效应(列) · FTUE×调控 交互 · carryover(毕业后 D7/D14/LTV)\n"
    "个性化挽留臂内按 HMM 状态(T1/S1/S2/S3) 分层比 D1/D3/D7 留存；dynamic_rtp 吃 RTP、挽留不吃\n"
    "调控干净读数 = 老玩家(无 FTUE 基线)",
    C["purple"], C["purpleE"], 7.9, False, "#5a32a3")
arr(ax, 40, 54.9, 45, 46.4)
box(ax, 50, 24, 92, 8,
    "关键：① 分组与策略完全独立——风控不以分组形式存在，任何玩家命中风控只改其子弹策略，仍属其原 (FTUE×调控) 组。\n"
    "② FTUE 与 dynamic_rtp、个性化挽留 非互斥（新人先经 FTUE→毕业→进调控，效果 carryover，类比 SS03）。\n"
    "③ 正交哈希：FTUE(独立位) · 调控臂(user_id)；FTUE 20% holdout。风控为独立判定，与分组哈希无关。",
    C["gold"], C["goldE"], 7.7, False)
arr(ax, 50, 37.7, 50, 28.2)
save(fig, "AB结构图_FM01_future.png")

print("全部完成 →", OUT)
