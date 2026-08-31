"""把 AB 分组规格汇总渲染成 PDF（原生文字表格 + 四张结构图），供预览。不连库。
输出 prd/ab_testing/AB分组规格汇总_v0.1.pdf
"""
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm, matplotlib.image as mpimg
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path

FP = None
for p in ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP = fm.FontProperties(fname=p); break
D = Path(__file__).resolve().parents[2] / "prd" / "ab_testing"
OUT = D / "AB分组规格汇总_v0.1.pdf"
A4 = (8.27, 11.69)
MUTED, ACC = "#656d76", "#0969da"

def newp():
    fig = plt.figure(figsize=A4); fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off"); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    return fig, ax
def txt(ax, x, y, s, size=10, bold=False, color="#1f2328"):
    ax.text(x, y, s, fontproperties=FP, fontsize=size, color=color,
            fontweight="bold" if bold else "normal", va="top", ha="left")
def head(ax, y, s, size=13):
    txt(ax, 0.06, y, s, size=size, bold=True)
    ax.plot([0.06, 0.94], [y - 0.011, y - 0.011], color="#1f2328", lw=1); return y - 0.03
def wrap(ax, x, y, s, size=9, color="#1f2328", w=88, lh=0.019, bullet=False):
    import textwrap
    pre = "· " if bullet else ""
    for i, ln in enumerate(textwrap.wrap(s, w)):
        txt(ax, x, y, (pre if i == 0 else "  ") + ln, size=size, color=color); y -= lh
    return y - 0.004
def table(ax, y, headers, rows, colx, fs=7.6, rh=0.02, wrapw=None):
    ax.add_patch(plt.Rectangle((colx[0]-0.006, y-rh+0.004), colx[-1]-colx[0]+0.10, rh, facecolor="#eef2f6", edgecolor="none"))
    for i, h in enumerate(headers): txt(ax, colx[i], y, h, size=fs, bold=True)
    y -= rh
    import textwrap
    for r in rows:
        maxl = 1
        cells = []
        for i, c in enumerate(r):
            ww = (wrapw[i] if wrapw else 22)
            lines = textwrap.wrap(str(c), ww) or [""]
            cells.append(lines); maxl = max(maxl, len(lines))
        for li in range(maxl):
            for i, lines in enumerate(cells):
                if li < len(lines): txt(ax, colx[i], y - li*0.016, lines[li], size=fs)
        y -= rh * 0.5 + 0.016 * maxl
    return y
def img_page(pdf, imgpath, caption):
    fig, ax = newp()
    txt(ax, 0.06, 0.965, caption, size=13, bold=True)
    ax.plot([0.06, 0.94], [0.955, 0.955], color="#1f2328", lw=1)
    im = mpimg.imread(str(imgpath)); h, w = im.shape[0], im.shape[1]
    ar = w / h
    boxw, boxh = 0.88, 0.82
    if boxw / ar <= boxh:
        iw, ih = boxw, boxw / ar
    else:
        ih, iw = boxh, boxh * ar
    x0 = 0.06 + (boxw - iw) / 2; y0 = 0.08
    axi = fig.add_axes([x0, y0, iw, ih]); axi.axis("off"); axi.imshow(im)
    pdf.savefig(fig); plt.close(fig)

def exp_table(ax, y, title, rows):
    y = head(ax, y, title, size=11)
    y = table(ax, y, ["项", "内容"], rows, [0.06, 0.24], fs=7.8, rh=0.019, wrapw=[9, 62])
    return y - 0.008

pdf = PdfPages(str(OUT))

# ---- 封面 + §0 总览 ----
fig, ax = newp()
txt(ax, 0.06, 0.965, "AB 分组规格汇总 · v0.1", size=18, bold=True)
txt(ax, 0.06, 0.935, "覆盖 FM01 捕鱼 · SS03 · SS06 · SS07 · 汇总日 2026-08-24 · draft", size=9, color=MUTED)
txt(ax, 0.06, 0.918, "SS03/SS06 为线上现状实测；SS07 为方案 v0.1（尚未上线）", size=8.5, color=MUTED)
y = head(ax, 0.888, "0. 总览对照")
rows = [
    ["FM01 捕鱼", "线上", "风控/农场剔除+策略/尾号", "风控·dynamic_rtp·retention(尾0/1)·default(尾2-9)", "D1/D3/D7留存(HMM)"],
    ["SS03", "线上", "两层:AB分组MOD10 + 暗保底(共用)", "第1层 Default(95kai)·A(BGTR97v2)·B(BGTR95v3)·AI(混合); 第2层 暗保底三档→kakuteiC", "RTP·留存·投注"],
    ["SS06", "线上", "MOD10(暗保底分组)", "holdout0-1(20%)·暗保底方案A 2-5(40%)·方案B 6-9(40%)", "保底体验·投注·留存"],
    ["SS07", "方案v0.1", "MOD10 静态", "default0-3(40%)·testA4-6(30%)·testB7-9(30%)", "人均投注额(缩尾+中位)"],
]
y = table(ax, y, ["游戏", "状态", "分流机制", "臂（占比）", "主指标"], rows,
          [0.06, 0.19, 0.28, 0.50, 0.80], fs=7, rh=0.02, wrapw=[8, 6, 13, 26, 13])
y -= 0.01
y = head(ax, y - 0.005, "共性 与 差异", size=11)
y = wrap(ax, 0.07, y, "共性：均以 user_id 尾号做确定性静态哈希（终身稳定、可复算、不引设备/OneID）；分析用 ITT 人群；投注类指标一律缩尾均值+中位数双口径（规避鲸鱼偏斜）；上线后先查 SRM。", size=8.6, bullet=True)
y = wrap(ax, 0.07, y, "差异：FM01 是“有无干预（挽留 ON/OFF）”的价值干预实验，带风控/农场 sticky 剔除；SS03/SS06/SS07 是“换数学表/换功能方案”的机制实验，静态分臂、保 cohort 连续。", size=8.6, bullet=True)
y -= 0.01
y = head(ax, y - 0.005, "开放问题", size=11)
for q in [
    "Q1 SS07：三表真实 math_table_id、default 表形参数（上线后填）",
    "Q2 SS07：真实流量复核样本量（现用 SS03 代理）",
    "Q3 SS06：garantizado A/B 是否已出结论；DAU 偏小是否延长周期",
    "Q4 SS03：切换前 partition_ab 的 A/B 映射待最终确认",
    "Q5 FM01：每期是否重洗轮换（提案 40/40/20 与现状静态尾号并存待定）",
    "Q6 全部：是否需要跨游戏统一常驻 holdout 支持纵向（LTV/D30）测量",
]:
    y = wrap(ax, 0.07, y, q, size=8.4, bullet=True, lh=0.017)
pdf.savefig(fig); plt.close(fig)

# ==== §1 FM01 现状：结构图 + 各实验流程 ====
img_page(pdf, D / "AB结构图_FM01.png", "1. FM01 捕鱼 · 分组结构（现状）")
fig, ax = newp()
txt(ax, 0.06, 0.965, "1. FM01 捕鱼 · 各实验流程（现状）", size=14, bold=True)
y = exp_table(ax, 0.925, "1.1 风控 sticky（优先剔除）", [
    ["目标", "剔除欺诈/多开/套利（反欺诈封禁 ~1,105 人），保证实验人群干净"],
    ["人群", "全体"],
    ["判定", "strategy RC_FISHING/%RISK_CONTROL% 或人工；命中 sticky 锁定"],
    ["处理", "命中 → 剔除出局（不进任何实验臂）"],
])
y = exp_table(ax, y, "1.2 dynamic_rtp（RTP 动态优化）", [
    ["目标", "RTP 动态优化对玩家表现的影响"],
    ["分流", "strategy_name = DYNAMIC_RTP_V3"],
    ["主指标", "RTP · 净亏 · 留存 · 投注（缩尾+中位）"],
    ["口径", "吃 RTP，故排除在挽留 AB 对比之外"],
])
y = exp_table(ax, y, "1.3 个性化挽留（HMM 分层）", [
    ["目标", "挽留策略 ON vs OFF 对 D1/D3/D7 留存的影响"],
    ["人群/分流", "user_id 尾号 0/1（上线≥07-31）=挽留 ON；尾号 2-9 = default(OFF·holdout)"],
    ["处理vs对照", "挽留 ON vs default OFF，两臂均不吃 RTP → 唯一差异=挽留 ON/OFF"],
    ["分层", "HMM 状态 T1/S1(low)/S2(engaged)/S3(escaped)，处理无关·冻结"],
    ["主指标", "各 HMM 状态内 D1/D3/D7 当日回访留存（ITT）"],
    ["分析", "各 HMM 状态内 ON vs OFF；dynamic_rtp 吃RTP 排除在挽留对比外"],
])
pdf.savefig(fig); plt.close(fig)

# ==== §2 SS03 现状：结构图 + 各实验流程 ====
img_page(pdf, D / "AB结构图_SS03.png", "2. SS03 · 分组结构（现状·两层）")
fig, ax = newp()
txt(ax, 0.06, 0.965, "2. SS03 · 各实验流程（现状）", size=14, bold=True)
txt(ax, 0.06, 0.935, "两层：第1层 AB分组换基础表 · 第2层 暗保底(所有臂共用)", size=8.5, color=MUTED)
y = exp_table(ax, 0.90, "2.1 第1层 · 数学表 AB", [
    ["目标", "不同基础数学表（RTP / 表形）对玩家向表现的影响"],
    ["人群", "全体（切换点 08-03 23:00 UTC 后）"],
    ["分流", "MOD10 末位：Default 0-3(40%)/A 4-5(20%)/B 6-7(20%)/AI 8-9(20%)"],
    ["处理", "Default=95kai(95%)/A=BGTR97_v2(97%)/B=BGTR95_v3(95%)/AI=混合"],
    ["主指标", "RTP · 人均净亏 · D1/D3/D7 留存 · 投注（缩尾+中位）"],
    ["分析", "各臂两两对比；切换前按 partition_ab 映射"],
])
y = exp_table(ax, y, "2.2 第2层 · 暗保底（所有臂共用）", [
    ["目标", "暗保底（隐性累计保底）对玩家表现的效果"],
    ["人群", "全体（四臂共用同一套暗保底）"],
    ["处理", "累计达档 → 確定表 normal_kakuteiC 触发保底赔付（近7天 2,582 人触发）"],
    ["主指标", "暗保底触发情况 · 对 RTP/留存/投注 的贡献"],
    ["口径", "具体门槛/档位见暗保底定稿文档，此处不展开"],
])
pdf.savefig(fig); plt.close(fig)

# ==== §3 SS06：结构图 + 实验流程 ====
img_page(pdf, D / "AB结构图_SS06.png", "3. SS06 · 暗保底分组")
fig, ax = newp()
txt(ax, 0.06, 0.965, "3. SS06 · 实验流程（暗保底分组 A/B）", size=14, bold=True)
y = exp_table(ax, 0.925, "3.1 暗保底（garantizado）方案 A/B", [
    ["目标", "两套暗保底（garantizado）方案哪个更好，及 vs 无保底"],
    ["人群", "全体（dos 为全员基础表；暗保底叠加在其上，非独立游戏）"],
    ["分流", "MOD10：holdout 尾0-1(20%) / 方案A 尾2-5(40%) / 方案B 尾6-9(40%)"],
    ["处理vs对照", "方案A=garantizado_a；方案B=garantizado_b；holdout=无暗保底"],
    ["主指标", "保底触发体验 · 投注行为 · 留存"],
    ["实测", "a∩b=0 干净分流；方案A 630 人 / 方案B 380 人（2026-07-08 起）"],
    ["注意", "SS06 日 DAU ~150，样本偏小；A/B 判定需较长周期累积"],
])
pdf.savefig(fig); plt.close(fig)

# ==== §4 SS07：结构图 + 实验流程 ====
img_page(pdf, D / "AB结构图_SS07.png", "4. SS07 · 分组结构（方案 v0.1）")
fig, ax = newp()
txt(ax, 0.06, 0.965, "4. SS07 · 实验流程（数学表 AB · 方案 v0.1）", size=14, bold=True)
txt(ax, 0.06, 0.935, "同 96.5% RTP，测 payout 分布 + 命中率（表形），尚未上线", size=8.5, color=MUTED)
y = exp_table(ax, 0.90, "4.1 数学表 AB（default / testA / testB）", [
    ["目标", "同 RTP 96.5% 下，表形（命中率+payout分布）对投注行为/留存的影响"],
    ["人群", "SS07 全体 CNY 玩家（上线后）"],
    ["分流", "MOD10：default 0-3(40%) / testA 4-6(30%) / testB 7-9(30%)（Dunnett 对照配比）"],
    ["处理", "default=现行；testA=命中20%·低波动；testB=命中15%·中波动"],
    ["主指标", "人均投注额（缩尾95%均值+中位，不用 log）；投注次数/活跃天"],
    ["护栏", "三臂实测 RTP≈96.5% · 实测命中率复现 testA≈20%/testB≈15% · D1/D3/D7 留存"],
    ["样本/时长", "SS03 代理：缩尾95% @20% MDE 需 ~1,275/臂；14 天一期可测 20% 差异"],
    ["分析", "H1 testA vs default · H2 testB vs default · H3 testA vs testB（次要）；全 ITT"],
])
pdf.savefig(fig); plt.close(fig)

# ---- SS03 Future：结构图 + 期①调控层(factorial)/期②暗保底 详细流程 ----
img_page(pdf, D / "AB结构图_SS03_future.png", "5. SS03 · Future 结构图（调控层 FTUE×数学表 factorial ⊥时序⊥ 暗保底）")

fig, ax = newp()
txt(ax, 0.06, 0.965, "5. SS03 · Future（调控层 factorial · 与暗保底分时序）", size=14, bold=True)
txt(ax, 0.06, 0.935, "明保底=全体活动(常开·非实验) · 调控层(FTUE×数学表 factorial) 与 暗保底 分时序(不同时跑·互相冻结)", size=7.6, color=MUTED)
y = head(ax, 0.90, "5.0 分流总览（两块分时序）")
y = table(ax, y, ["块", "何时跑", "内容", "哈希位"], [
    ["调控层实验", "调控层期", "FTUE(V80%/C20%) × 数学表(Def/A/B/AI) factorial", "FTUE=独立位、数学表=末位"],
    ["暗保底实验", "暗保底期", "有暗保底 80% / 无对照 20%", "十位 /10%10"],
    ["明保底", "—(常开)", "全体活动·非实验", "—"],
], [0.06, 0.18, 0.30, 0.68], fs=7.5, rh=0.019, wrapw=[6, 7, 30, 16])
y -= 0.006
y = wrap(ax, 0.07, y, "调控层 与 暗保底 分时序、互相冻结 → 两块互不混淆；明保底常开为背景。", size=8.3, bullet=True, w=58)
y -= 0.004
y = exp_table(ax, y, "5.1 期① · 调控层实验（FTUE × 数学表 factorial）", [
    ["目标", "数学表(表形/RTP)、FTUE(新手引导) 各自及交互 对表现/留存的影响"],
    ["人群", "全体(数学表) + 首登新玩家(FTUE;老玩家=FTUE-none 基线)"],
    ["分流", "数学表 MOD10 末位(Def0-3/A4-5/B6-7/AI8-9)；FTUE 独立位(V80%/C20%)"],
    ["处理", "数学表 Def=95kai/A=BGTR97_v2/B=BGTR95_v3/AI=混合；FTUE variant=表替代/control=现状"],
    ["冻结", "该期内 暗保底保持默认，不做变动"],
    ["主指标", "RTP·净亏·D1/D3/D7留存·投注(缩尾+中位)；FTUE 另看早期留存/完成率+carryover"],
    ["分析", "FTUE×数学表 factorial：FTUE主效应(行)·数学表主效应(列)·交互·carryover；干净读数用老玩家"],
    ["样本", "数学表 UV~12k/2周；FTUE 新玩家≈666/天→2周~9,300(V~7,440/C~1,860)"],
])
pdf.savefig(fig); plt.close(fig)

fig, ax = newp()
y = exp_table(ax, 0.955, "5.2 期② · 暗保底实验（独立 AB）", [
    ["目标", "暗保底（隐性累计保底）对留存 / 投注 / RTP 的净效果"],
    ["人群", "全体"],
    ["分流", "十位 MOD(user_id/10,10)：有暗保底 80% / 无对照 20%"],
    ["处理vs对照", "有=累计达档触发 normal_kakuteiC 保底赔付；无=不触发"],
    ["冻结", "该期内 调控层(FTUE + 数学表)保持默认，不做变动"],
    ["主指标", "RTP · 净亏 · 留存 · 投注（缩尾+中位）；暗保底触发率/成本"],
    ["分析", "有 vs 无对照（干净独立 AB）；对照 20%~2,400/2周，够测 20% 差异"],
])
y = head(ax, y, "5.3 明保底（全体活动·非实验） + 5.4 口径")
for s in ["明保底=面向全体的常开活动，所有人都有、不设对照、不做 AB；两期实验期间常开为共同背景。",
          "调控层内部=factorial(FTUE×数学表 同时跑、拆主效应+交互)；调控层 与 暗保底=分时序(互相冻结、不混淆)。",
          "各维 user_id 正交哈希（数学表=末位·FTUE=独立位·暗保底=十位）；风控独立前置（命中剔除）。",
          "对照统一 20%（CV≈8 须缩尾95%+中位，每组~1,300 测 20% 差异）；全 ITT，上线查 SRM。"]:
    y = wrap(ax, 0.07, y, s, size=8.3, bullet=True, w=58)
pdf.savefig(fig); plt.close(fig)

# ---- FM01 捕鱼 Future：整体结构图 + 每个实验详细流程 ----
img_page(pdf, D / "AB结构图_FM01_future.png", "6. FM01 捕鱼 · Future 结构图（FTUE × 调控 factorial；风控脱离分组）")

# 页：intro + 分流总览 + 实验A FTUE
fig, ax = newp()
txt(ax, 0.06, 0.965, "6. FM01 捕鱼 · Future（FTUE × 调控 factorial · 风控脱离分组）", size=13.5, bold=True)
txt(ax, 0.06, 0.935, "分组=FTUE×调控 factorial；风控脱离分组、不以分组形式存在，只影响子弹策略（见 6.4）", size=7.8, color=MUTED)
y = head(ax, 0.90, "6.0 分流总览（分组=两维正交哈希；风控独立于分组）")
y = table(ax, y, ["维度", "实验/角色", "判定/哈希", "分组"], [
    ["FTUE", "新玩家引导", "独立哈希位(与调控臂正交)", "variant 80% / control 20%（新玩家·窗口内）"],
    ["调控", "dyn/挽留/default", "user_id(沿用AB口径)", "dynamic_rtp / 个性化挽留 / default(holdout)"],
    ["风控", "不参与分组", "独立判定(与分组哈希无关)", "不产生组，只改子弹策略（见 6.4）"],
], [0.06, 0.17, 0.33, 0.56], fs=7.4, rh=0.019, wrapw=[5, 10, 13, 30])
y -= 0.008
y = exp_table(ax, y, "6.1 实验 A · FTUE 新玩家引导", [
    ["目标", "FTUE 是否提升新玩家早期留存/转化/LTV（效果类比 SS03）"],
    ["人群", "首登纯新玩家（老玩家不参与，作 FTUE-none 基线）"],
    ["分流", "与调控臂正交的独立哈希位；variant 80% / control 20%"],
    ["处理vs对照", "variant=FTUE 策略；control=现状新手体验"],
    ["生效窗口", "首登→毕业(须短)；毕业后进调控"],
    ["非互斥", "毕业后仍经历 dynamic_rtp/挽留 → FTUE carryover"],
    ["主指标", "窗口内早期留存/转化；毕业后 carryover D7/D14/LTV"],
    ["样本/时长", "新玩家≈1,072/天→2周~15,000(V~12,000/C~3,000)"],
])
y = exp_table(ax, y, "6.2 实验 B · dynamic_rtp（调控 · RTP 优化）", [
    ["目标", "RTP 动态优化对玩家表现的影响"],
    ["人群", "全体玩家（调控臂之一）"],
    ["分流", "user_id（调控臂，沿用 AB 口径）"],
    ["处理", "RTP 动态优化 ON（strategy_name=DYNAMIC_RTP_V3）"],
    ["分析", "vs default holdout；FTUE×dynamic_rtp 交互；吃RTP→排除在挽留AB外"],
])
pdf.savefig(fig); plt.close(fig)

# 页：实验C 挽留 + 风控(脱离分组) + 联合分析
fig, ax = newp()
y = exp_table(ax, 0.955, "6.3 实验 C · 个性化挽留（调控 · HMM 分层）", [
    ["目标", "挽留策略 ON vs OFF 对 D1/D3/D7 留存的影响"],
    ["人群", "全体玩家（调控臂之一）"],
    ["分流", "user_id（调控臂）"],
    ["处理vs对照", "挽留 ON vs default(OFF·holdout)，两臂均不吃 RTP"],
    ["分层", "HMM 状态 T1/S1(low)/S2(engaged)/S3(escaped)，处理无关·冻结"],
    ["分析", "各 HMM 状态内 ON vs OFF(ITT)；dynamic_rtp 排除在挽留对比外"],
])
y = exp_table(ax, y, "6.4 风控（脱离分组 · 只影响子弹策略 · 非实验）", [
    ["独立性", "分组与策略完全独立：风控不以分组形式存在、不产生 AB 组、不改玩家分组"],
    ["判定", "独立判定（反欺诈/多开/套利或人工，sticky），与分组哈希无关"],
    ["作用", "命中后只改该玩家的子弹策略；其 FTUE/调控 分组保持不变"],
    ["分析", "视为策略侧扰动，可作协变量/诊断，不作分组维度"],
])
y = head(ax, y, "6.5 联合分析与口径")
for s in ["分组与策略独立：风控只改子弹策略、不改分组；各实验按其 FTUE×调控 组分析，风控作扰动(协变量/诊断)。",
          "factorial：FTUE 主效应(行)·调控主效应(列)·FTUE×调控 交互·carryover(毕业后)。",
          "挽留 HMM 分层比留存；dynamic_rtp 吃RTP会混淆挽留→排除在挽留 AB 外。",
          "干净读数=老玩家(无FTUE)；新玩家带 FTUE block。全 ITT，上线查 SRM。"]:
    y = wrap(ax, 0.07, y, s, size=8.4, bullet=True, w=58)
pdf.savefig(fig); plt.close(fig)

# ---- §7 group_name 编码定义与要求 ----
fig, ax = newp()
txt(ax, 0.06, 0.965, "7. group_name 编码定义与要求", size=14, bold=True)
y = head(ax, 0.925, "7.1 定义")
y = wrap(ax, 0.07, y, "group_name = 数组，每个元素 = 该玩家参与的一个 AB 实验，形如 experiment=group（key=value，全英文）；每个 group 都有对应数字编码，可等价用文字数组或数字数组。整份带整体版本号。", size=8.5, bullet=True, w=58)
y = wrap(ax, 0.07, y, "文字: [ \"<experiment>=<group>\", ... ]   数字: [ <n>, ... ]   · version: v<N>", size=8.2, w=60)
y -= 0.006
y = head(ax, y, "7.2 各游戏 实验名 + 组别（文字=数字）")
y = table(ax, y, ["游戏", "实验名(key)", "组别 value = 编码"], [
    ["SS03", "math_table", "default=0 / TestA=1 / TestB=2 / AI=3"],
    ["SS03", "ftue", "variant=0 / control=1"],
    ["SS03", "dark_guarantee", "on=0 / off=1"],
    ["FM01", "ftue", "variant=0 / control=1"],
    ["FM01", "strategy", "default=0 / dynamic_rtp=1 / customized_retention=2"],
    ["SS06", "dark_guarantee", "default=0 / TestA=1 / TestB=2"],
    ["SS07", "math_table", "default=0 / TestA=1 / TestB=2"],
], [0.06, 0.19, 0.44], fs=7.8, rh=0.02, wrapw=[6, 14, 44])
y = wrap(ax, 0.07, y-0.002, "编码=组别固定枚举序(从0起)；default/variant/on 均为 0。", size=7.8, bullet=True, w=58)
y -= 0.004
y = head(ax, y, "7.3 示例")
for s in ['SS03 新玩家 [math_table, ftue, dark_guarantee]：["math_table=default","ftue=variant","dark_guarantee=on"] = [0,0,0]',
          'SS03 老玩家(无 ftue)：["math_table=TestA","dark_guarantee=off"] = [1,1]',
          'FM01：["ftue=variant","strategy=customized_retention"] = [0,2]',
          'SS06：["dark_guarantee=TestA"] = [1]　SS07：["math_table=TestB"] = [2]']:
    y = wrap(ax, 0.08, y, s, size=7.8, w=64)
y -= 0.004
y = head(ax, y, "7.4 要求")
for s in ["数组：一个玩家可同时在多个实验，每实验一个元素；实验顺序固定(数字数组按此对齐)。",
          "key=value 全英文：key=实验名，value=组别，并有对应数字编码；文字/数字数组等价。",
          "版本：整份带一个整体 v<N>，方案任一改动 +1；各实验定义查 ab_config。",
          "只含“分组”实验：风控不进（脱离分组·只改子弹策略）；明保底不进（全体活动·非实验）。",
          "不参与/不适用则该元素不出现（老玩家无 ftue 元素）；分时序冻结的实验取默认组别。",
          "确定性可复算：由 user_id 各正交哈希位（math_table=末位·ftue=独立位·dark_guarantee=十位）+ 配置版本派生。"]:
    y = wrap(ax, 0.07, y, s, size=8.3, bullet=True, w=58)
pdf.savefig(fig); plt.close(fig)

pdf.close()
print("saved:", OUT)
