---
name: ab-experiment-design
description: >
  Design an A/B experiment for the Bitus games (FM01 fishing, SS03/SS07 slots):
  hypothesis, population split, traffic allocation, HMM-state stratification,
  metrics, sample size, timeline, and validation — then emit the design as md /
  Mermaid diagram / Word / PDF. Use when the user says they want to design an AB
  group / 分组方案 / AB 实验, iterate the retention-AB or dynamic-RTP arms, pick
  traffic ratios, or reconcile a new experiment against the A/B platform PRD.
---

# A/B 实验设计（Bitus games）

复用既有的一套方法论与脚本，别重造。核心资料：
- **`prd/ab_testing/AB分组方案设计_v0.1.md`** — 权威人群划分方案（本 skill 的口径来源）。
- `prd/ab_testing/2026-08-11 A-B 实验平台 PRD v0.2.html` — 跨产品 A/B 平台 PRD（术语/统计规则/2×2 试点的上位契约）；`prd_text.txt` 是它的纯文本。
- `prd/ab_testing/挽留策略AB实验设计_v1.0.docx`、`分流分层图.png` — 已成稿的挽留 AB 文档与图。

脚本（都在 `jobs/ab_testing/`，复用/扩展，勿另起炉灶）：
- `ab_grouping_verify.py` — 分组人群验证，三子命令 `pop` / `static` / `rotate`，复用 `jobs.fishing.fm01_grouping` 口径。**任何新分组方案先跑它验证 SRM/重洗成立再定稿。**
- `ab_sample_size.py` — 两比例样本量测算（每臂每 HMM 状态，主指标 D7 留存，α=0.05 双侧、power=0.8）。
- `ab_diagram.py` — 渲染分流分层图 PNG（中文）→ `prd/ab_testing/分流分层图.png`。
- `ab_doc_gen.py` — 生成 Word 设计文档 → `prd/ab_testing/挽留策略AB实验设计_v1.0.docx`。
- `extract_prd.py` — 把 PRD html 抽成纯文本。

## 何时用

用户说「设计 AB 组 / 分组方案 / AB 实验」「迭代挽留臂或 dynamic_rtp 臂」「定流量比例」「新实验怎么对齐 A/B 平台」时进入本流程。

## 已拍板的设计决策（默认遵守，除非用户改）

| 决策点 | 结论 |
|---|---|
| 试点游戏 | FM01 捕鱼（SS03/SS07 同法可迁移） |
| 分流哈希 | 复用 `user_id`，不引设备/OneID（对齐 PRD §9.3） |
| 分组粒度 | **user 级**判定，非记录级；同一人终身稳定归组 |
| 分组架构 | 风控优先 sticky + 其余按 user_id 切 **dynamic_rtp 40% / retention 40% / default 20%** |
| 农场处理 | 分组**前**剔除出实验人群（不进任何臂，防污染 RTP/盈亏） |
| 每期换人 | 整个 40/40/20 每期**重洗**，且**硬排除上期参与者**（同臂不连任） |
| 挽留 AB 分层 | 用 **HMM 状态**（T1/S1 low/S2 engaged/S3 escaped），处理无关、分组时冻结 |
| 分析人群 | 默认 **ITT**（assignment 人群），不用 triggered，避免 post-treatment 偏差 |

## 分组优先级（user 级，从高到低）

1. **风控（sticky，最高）**：曾命中 `RC_FISHING*` / `%RISK_CONTROL%` 或人工拉入 → 永久锁定，分组不再动 TA，只有人工可移出；风控可强制拉任意人入组，不参与轮换。
2. **农场剔除**：命中已知农场 PID（`MD5/EW3/KV3/JN1/C81/HL2/C16/JR8` 及关联小号）或高倍狙击套利签名 → 剔除出实验，不进任何臂。
3. **其余 user 按 rank 轮换分三臂**（见下）。

## 每期旋转轮换（确定性，可复算）

```
rank = user_id % 100                      # 0-99，均匀哈希，终身稳定
p    = 期号 wave_id（从 0 递增）
arm  = band( (rank + 40*p) mod 100 )
band: [0,40)->dynamic_rtp(40%) · [40,80)->retention(40%) · [80,100)->default(20%)
```
性质（已实测，`ab_grouping_verify.py rotate`）：每期严格 40/40/20；`STEP=40` → 相邻期同臂人群完全 disjoint（转移矩阵对角线=0）；5 期一循环，长期人人轮遍三臂。一期长度建议对齐实验 `minimum_duration`（默认 2 周），开新期 `p+=1`。

## 挽留 AB · HMM 状态分层

- 状态：`T1`首日 · `S1` low · `S2` engaged · `S3` escaped（HMM 从行为序列算，与是否被挽留无关，两臂统一口径、分组时**冻结**）。
- 处理臂：Control(挽留 OFF，来自 holdout，**不吃 RTP**) vs Variant(挽留 ON，**不吃 RTP**) → 两臂唯一差异 = 挽留 ON/OFF，主效应无混淆。dynamic_rtp 臂吃 RTP，会混淆，**排除**在挽留 AB 外。
- 每个状态内比 ON vs OFF 的 D1/D3/D7 留存、转化（ITT）。
- **为何不用系统 `CR_FISHING:Tk` 标签分层**：`Tk` 是挽留系统产出，只有被处理者才有，对照组没有 → 无法分层且引入 post-treatment 偏差。HMM 状态两臂都能算。

## 标准流程

1. **读口径**：先读 `prd/ab_testing/AB分组方案设计_v0.1.md`（+ 必要时 `prd_text.txt` 查 PRD 术语/统计规则）。有变动只改这份 md，保持单一事实源。
2. **明确实验四要素**（缺则问用户，别臆造）：假设(hypothesis) · 受众/分层 · 机制变量(RTP/挽留/美术…) · 主指标+护栏指标+MDE。
3. **定分组**：套用上面的优先级 + 40/40/20 轮换；如需 2×2 叠加，机制层在 dynamic_rtp 臂内切 C/V、美术层用第二个正交哈希（如 `(user_id/10)%2`）做 50/50，联合查 SRM。
4. **验证**：跑 `ab_grouping_verify.py static` 和 `rotate`，确认比例 40/40/20、尾号均匀（SRM 基线）、相邻期对角线=0。
5. **样本量**：跑 `ab_sample_size.py`，按各 HMM 状态基线留存 + MDE 得每臂 n，估实验时长是否够。
6. **产出文档**：md 为主；要图跑 `ab_diagram.py`，要 Word/PDF 跑 `ab_doc_gen.py`（PingFang SC 中文字体）。文件都落 `prd/ab_testing/`。

## 口径要点与坑

- **ITT 优先**：回到 assignment 人群分析；把曝光完整性列为诊断而非筛样本条件（否则把 Variant 效果筛没）。
- **SRM**：预设比例 vs 实测长期比例出现非随机偏差 → 暂停结论、查分桶/曝光/数据链路。定稿前必查。
- **holdout**：default 20% 是常驻 holdout / 绝对基准；纵向累计影响（长期 holdout、稳定 cohort 测 LTV/D30）属 PRD Phase 2。
- **每期重洗的代价（已知情接受）**：破坏 cohort continuity，无法在同一批人上累积 D7/D30/LTV；需纵向测量时另设永不轮换的稳定 holdout（开放问题 OQ-A5）。
- FM01 分组底层口径在 `jobs/fishing/fm01_grouping.py`（`GROUP_CASE`/`BASE_FILTER`/`RETENTION_LAUNCH_UTC`）；SS03 在 `jobs/ss03_analysis/ss03_grouping.py`（有切换点，见 `docs/SS03_分组政策.md`）。
- 开放问题（每次设计时确认是否已解）：OQ-A1 一期周期长度 · OQ-A2 2×2 机制 C/V 切法 · OQ-A3 农场是否每期动态更新 · OQ-A4 风控 sticky 是否设复评冷静期 · OQ-A5 是否要稳定 holdout 支持纵向测量。
