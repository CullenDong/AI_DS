# SS03 · AB 分组方案 `high_player_test_v1`（上线）· v0.1

> **独立新方案**，不覆盖既有 SS03 分组结构（既有 4 臂 MOD10 结构见 `AB分组规格汇总_v0.1.md` §2 / `docs/SS03_分组政策.md`）。本方案为 `high_player_test_v1`（高价值玩家数学表 A/B）**上线配置**，衔接设计文档 [SS03 · high_player_test_v1](https://bituslabs.atlassian.net/wiki/x/CAANUg)。
> **上线时间**：2026-09-14 16:00 美西（PDT）= **2026-09-14 23:00 UTC** = 2026-09-15 07:00 北京。

## 一、分组配比（`MOD(user_id,100)` 尾号后两位）

| 组 | 尾号后两位 | 占比 | 数学表（数据库实测确认） | RTP |
|---|---|---:|---|---|
| **A** | [0, 30) | 30% | `normal_Zero_BGTR97_Saitekika_BGadj_v3` | 97% |
| **B** | [30, 60) | 30% | `normal_Zero_BGTR95_HMM_Highvalue_v1` | 95% |
| **default** | [60, 100) | 40% | `normal_zero_95_kai`（95Kai） | 95% |

> 无 AI 组。**已用数据库核对（>=2026-09-15）**：uid[0,30)→BGTR97_v3、[30,60)→BGTR95_HMM_v1、[60,100)→95Kai，SRM 实测 29.7/30.2/40.1%，分流干净。
> ⚠ 用户初稿把 A 组写成 `BGTR95_Saitekika_BGadj_v3`，与库实测（`BGTR97_Saitekika_BGadj_v3`）不符——**以数据库为准，A = BGTR97**。

## 二、分组判定 SQL（只按 user_id；暗保底不进分组）

```sql
-- created_at >= '2026-09-14 23:00:00' (UTC) 起生效
CASE
  WHEN MOD(user_id, 100) < 30 THEN 'A'        -- [0,30)  BGTR97_Saitekika_BGadj_v3
  WHEN MOD(user_id, 100) < 60 THEN 'B'        -- [30,60) BGTR95_HMM_Highvalue_v1
  ELSE 'default'                               -- [60,100) 95Kai
END
```

## 三、暗保底 = 第 2 层策略，不是分组

- `normal_kakuteiC` 是**所有臂共用的第 2 层策略**（累计达档触发保底赔付），**不参与分组**、不单列组；命中者仍属其 uid 所定臂（同 FM01「风控=策略非分组」原则）。

## 四、对照逻辑与假设

- **A vs default**：高 RTP（97）+ BG 结构改造是否提升高价值玩家投注/留存。
- **B vs default**：同 RTP（95）换 HMM 高价值训练的低波动形态是否更适配高价值玩家（干净形态对照）。
- **A vs B**：「高 RTP」vs「低波动·高价值训练」哪条路线对高价值更优。

## 五、检验指标

- **核心人群**：高价值玩家（终身下注前 10%）。
- **主指标**：p90 total_bet / p90 num_bet、D1/D3/D7 留存（同期组间，中/均/均99，刨 kakutei，CNY）。
- **护栏**：各组实测 RTP 贴近设计（A~97、B~95、default~95）；全体留存不滑；无异常大额赔付集中。

## 六、时长与验证

- 周期建议 21 天（高价值样本偏薄，保每组有效样本量）。
- 定稿/复核跑 `ss03_grouping` 同类 SRM 校验，确认 30/30/40 与尾号后两位均匀。

## 七、结构图

![SS03 high_player_test_v1 上线 分组结构](data/output/ab_testing/AB结构图_SS03_high_player_test_v1.png)

## 八、与既有 SS03 结构的关系

- 既有 SS03 4 臂（MOD10：Default/A/B/AI，见汇总 §2）**保留不改**；本方案为其之后的新一期上线配置，单独立档。
- 若需把本期并入分析口径（`jobs/ss03_analysis/ss03_grouping.py`），请单独确认后再改（本方案默认不改既有口径文件）。
