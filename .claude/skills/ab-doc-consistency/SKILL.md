---
name: ab-doc-consistency
description: >
  Audit & enforce consistency across all A/B-experiment documents for the Bitus
  games (FM01 fishing, SS03/SS06/SS07/SS01/SS02 slots). Guarantees every AB doc
  has (1) a structure diagram in the ONE canonical flowchart style, (2) all
  current control layers of that game shown, (3) the current AB grouping (arms +
  ratios + split key + tables), (4) an explicit experiment duration, (5) stated
  hypotheses, and (6) the metrics under test (primary + guardrail + MDE). Use when
  the user says 检查/核对所有 AB 组文档、保证图片格式一致、AB 文档合规/一致性、
  补齐实验时长/假设/指标, or after creating/editing any AB experiment doc.
---

# AB 组文档一致性审计（Bitus games）

**职责**：让每一份 AB 实验文档满足同一套硬性规格，且图统一成同一种结构图样式。不是重新设计实验（那是 `ab-experiment-design`），是**核对既有文档 + 补齐缺项 + 统一图**。

## 六条硬性要求（每份 AB 文档都必须有）

| # | 要求 | 判定标准 | 缺了怎么补 |
|---|---|---|---|
| 1 | **结构图** | 用 `![](…png)` 内嵌一张 AB 结构图，PNG 存在，且**出自权威生成器**（样式统一，见下） | 用 `render_ab_diagrams.py` 风格生成后 `![]()` 内嵌 |
| 2 | **所有调控层级** | 覆盖该游戏**当前所有调控层**（以 `AB分组规格汇总_v0.1.md` 为准）——如 SS03 = AB分组层 + 暗保底层 | 结构图与正文补上缺的层；不在本实验范围的层也要在图里点明"共用/常开" |
| 3 | **当前 AB 分组** | 每个臂：组名 + 比例% + 分流键(尾号/MOD) + 各组数学表/机制 | 补分组表 + 分流 SQL |
| 4 | **实验时长** | 明确写"为期 N 天 / 一期 = N 天 / 实验周期" | 补周期（对齐 `minimum_duration`，默认 2 周；样本量不足则延长） |
| 5 | **假设 (Hypothesis)** | 有 H1/H2/H3 式假设（各臂 vs 对照、及两两） | 按 `SS07_数学表AB方案` §1 格式补 |
| 6 | **检验指标** | 有**主指标 + 护栏指标**（+ MDE/判定阈） | 按 §3「主指标·护栏·观测口径」表补 |

**标杆文档**：`prd/ab_testing/SS07_数学表AB方案_v0.1.md`（六项俱全，照它的章节骨架补其它文档）。
**单一事实源（SoT）**：`prd/ab_testing/AB分组规格汇总_v0.1.md`——各游戏"当前有哪些调控层 + 分组比例"以它为准；线上配置变了先改它，再改各专项文档。

## 结构图样式标准（唯一，不许各画各的）

所有 AB 结构图必须由 **`jobs/ab_testing/render_ab_diagrams.py`**（或同风格的 `high_player_test_diagram.py`）生成，特征固定：

- **中文字体 PingFang**（`/System/Library/Fonts/PingFang.ttc`），`figsize≈(11, 7)`，坐标系 0–100，`axis off`。
- **圆角框** `FancyBboxPatch(boxstyle="round,pad=0.5,rounding_size=2")` + **箭头** `FancyArrowPatch(arrowstyle="-|>")`；配色用脚本里的 `C` 调色板（gray/gold/green/blue/orange/purple/red 七色，各带描边色）。
- **自顶向下分层**：标题(15.5pt 粗) + 副标题(9.5pt 灰) → 全体玩家框 → **每一调控层一排框**（层标签用 gold 框）→ 底部"对比/口径"框（purple）。
- **一张图必须能一眼看全该游戏所有调控层**（如 SS03 两层：第1层 AB分组、第2层 暗保底共用；SS07 三层：表形AB + AI-MAB + 暗保底）。
- 用固定色义：对照/holdout=gray/green、测试臂=blue/orange、暗保底=red、机制层标签=gold、对比口径=purple、风控=red。

> 判定"样式是否一致"目前靠**肉眼 + 是否出自上述生成器**。新游戏加图时，在 `render_ab_diagrams.py` 里加一个 figure 块（复用 `box/arr/title/save` 帮手），不要另写新画法。

## 检查脚本

```bash
python3 jobs/ab_testing/ab_doc_check.py                 # 扫 prd/ab_testing/**.md
python3 jobs/ab_testing/ab_doc_check.py <file...>       # 指定文档
```

逐份输出 6 项 ✅/⚠️/❌ 与缺项明细；有硬失败(❌)则退出码=1（可接 pre-commit / CI）。

- **✅ 合格 / ❌ 不合格** 只看 6 项里有无 `FAIL`。
- **⚠️ 调控层缺失** 是软告警——需**人工判断**该层是否属本实验范围（专项文档可只覆盖子集，但按要求 2「所有层」应至少在图/正文点明其存在）。
- 脚本内 `GAME_LAYERS` = 各游戏当前调控层的**事实源镜像**；**线上配置变更时，同步改 `GAME_LAYERS` 和 `AB分组规格汇总`**，否则检查会漏/误报。
- `CANON_GENERATORS` = 允许的结构图生成器白名单。

坑：`python3 … | head` 会用 `head` 的退出码盖住脚本的——要看真实退出码别接管道。传相对路径已在脚本内 `resolve()`，可直接用仓库相对路径。

## 标准审计流程

1. **跑全量**：`python3 jobs/ab_testing/ab_doc_check.py`，拿到不合格清单。
2. **逐份补缺**（按上表"缺了怎么补"）：
   - 缺**结构图** → 在 `render_ab_diagrams.py` 加/改该游戏 figure（含全部调控层）→ 生成 PNG → md 里 `![](相对路径.png)` 内嵌。
   - 缺**假设/指标/时长** → 照 `SS07_数学表AB方案_v0.1.md` §1/§3/§4 的章节骨架补齐。
   - 缺**调控层** → 对照 `AB分组规格汇总` 补上，图与正文都要有。
3. **复跑**确认转绿。
4. **发布**（若该文档已上 Confluence）：用对应发布器（`math_table_confluence.py` / `high_player_test_confluence.py` / `multi_game_confluence.py`）。图片经 `upload_images()` 作附件上传，md 里 `![](path)` 会被渲染成 `<ac:image>`（附件必须与页面同名；先建页后传附件）。
5. **更新 SoT**：分组/层级有实质变化时，回改 `AB分组规格汇总_v0.1.md`（+ 脚本 `GAME_LAYERS`）。

## 各游戏当前调控层级（事实源镜像，改动线上须同步）

| 游戏 | 调控层（自上而下） | 分流键 |
|---|---|---|
| **FM01** | 风控(sticky前置剔除) · dynamic_rtp · 个性化挽留 · default/holdout | user_id 尾号 + 行为标签（**非纯随机**，含选择偏差） |
| **SS03** | 第1层 AB分组(基础数学表:Default/A/B/AI) · 第2层 暗保底(`normal_kakuteiC` 全体共用) | MOD(user_id,10)；`high_player_test_v1` 用 MOD(user_id,100) A/B/default |
| **SS06** | 基础表 dos · 暗保底分组(holdout/方案A/方案B) | MOD(user_id,10) |
| **SS07** | 表形AB(default/testA/testB) · AI-MAB(AI组) · 暗保底层(全体·方案A) | MOD(user_id,100) |
| **SS01/SS02** | AI-MAB(AI vs Default) | partition_ab / user_id 随机 |

> 老虎机 user_id 随机分桶 = 干净 A/B 可归因；FM01 dynamic_rtp/挽留是行为贴标签、含选择偏差，文档需注明"相关非因果"。

## 在册 AB 文档（审计范围）

- `prd/ab_testing/AB分组规格汇总_v0.1.md`（**SoT**，覆盖全部游戏）
- `prd/ab_testing/AB分组方案设计_v0.1.md`（FM01 早期方案——已知缺图/假设/指标，属历史设计稿）
- `prd/ab_testing/SS07_数学表AB方案_v0.1.md`（标杆；注意结构图目前未用 `![]()` 内嵌）
- `prd/ab_testing/math_table/SS03_high_player_test_v1.md`
- `prd/ab_testing/math_table/ab_summary_and_plan.md`（CBO 总结，§十 含 high_player_test）
- `挽留策略AB实验设计_v1.0.docx`（docx，脚本不扫；人工对照六项）

## 关联

- 设计新实验 → skill `ab-experiment-design`（本 skill 只审计既有文档）。
- 结构图生成器：`jobs/ab_testing/render_ab_diagrams.py`、`high_player_test_diagram.py`。
- Confluence 发布 + 图附件：`jobs/ss03_analysis/math_table_confluence.py`（`md_to_storage` 支持 `![]()`→`<ac:image>`、`upload_images()`）、`jobs/ab_testing/high_player_test_confluence.py`。
