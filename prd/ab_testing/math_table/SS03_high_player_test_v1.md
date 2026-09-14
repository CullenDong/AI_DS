# SS03 · high_player_test_v1：高价值玩家数学表真实 A/B 方案

> 首个把「高价值玩家专属表」推上真实同期 A/B 的实验，衔接《Fancy Stuff（CBO）数学表优化》第八节（高价值专属表与 reward shaping）。

## 一、背景与目的

第八节已提出给高价值玩家分配专属表（HMM day-state / 静态终身前 10%），表已训练但未上真实 A/B。本方案把两张面向高价值玩家的 CBO 表推上线，与现行 default（95Kai）同期对照，验证「高 RTP」与「低波动·高价值训练」两条路线哪条更适配高价值玩家。

- **default = 95Kai**：现行表，Esscher FGadj，RTP 94.9%。
- **A = BGTR97_Saitekika_v3**：RTP 97、CBO 训练、BG 占比高——把多出的 RTP 放进 base game / trigger，弱化对 FG 大奖尾部的依赖。
- **B = BGTR95_HMM_Highvalue_v1**：RTP 95（与 default 同档）、HMM 高价值 day-state 训练、**波动最低（SD 93 vs 95Kai 127）**——面向高价值玩家的稳态、少暴涨暴跌体验。

三条对照逻辑：

- **A vs default**：「加 2pp RTP 且改 BG 结构」是否提升高价值玩家投注/留存。
- **B vs default**：「同 RTP、换成 HMM 高价值训练的低波动形态」是否更适配高价值玩家——**干净的形态对照**（RTP 基本不变，只换分布形状）。
- **A vs B**：「高 RTP」 vs 「低波动·高价值训练」哪个方向对高价值更优，为后续选表策略 / 高价值定向 default 定调。

## 二、分组设计

- **分流键**：`MOD(user_id, 100)`（user_id 尾号后两位，100 个桶，随机均匀分流）。
- **桶分配**：

| 组 | 数学表 | 占比 | user_id 尾号（后两位） |
|---|---|---:|---|
| A | BGTR97_Saitekika_v3 | 30% | 00 – 29 |
| B | BGTR95_HMM_Highvalue_v1 | 30% | 30 – 59 |
| default | 95Kai | 40% | 60 – 99 |

![SS03 high_player_test_v1 AB 实验分组图](data/output/ab_testing/high_player_test_grouping.png)

- **周期**：21 天（3 个完整周，覆盖工作日/周末节律；高价值玩家样本偏薄，21 天保证每组高价值有效样本量）。
- 三组**同期并行** → 干净随机 A/B，全程用同期组间对比（沿用数据组既有口径）。

## 三、三张表设计参数（设计方 Excel）

| 组（表） | RTP | BG_RTP | FG 贡献 | BG 命中 | trigger | 波动 SD | p95 单发 | p99 单发 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| default 95Kai | 94.9% | 70.4% | 24.5% | 29.6% | 0.46% | 127.3 | 90.0 | 357.0 |
| A BGTR97_v3 | 96.9% | 78.2% | 18.7% | 32.9% | 0.33% | 124.0 | 93.2 | 305.7 |
| B BGTR95_HMM_HV | 95.2% | 78.8% | 16.3% | 30.4% | 0.40% | 92.6 | 95.1 | 318.7 |

**设计改动点（相对 95Kai）**：

- **A · BGTR97_v3**：RTP 95→97，多出的 2pp 主要走 **BG（BG_RTP 70.4%→78.2%）** 与 trigger 微调；**FG 贡献 24.5%→18.7%**（降低对 FG 大奖尾部的依赖）；**BG 命中 29.6%→32.9%**（base game 更常中小奖）。→ 高价值玩家在 base game 的「摸奖感」更稳、整体 RTP 更高。
- **B · BGTR95_HMM_HV**：RTP 与 default 基本同档（95.2 vs 94.9），但结构完全不同——**BG 主导（BG_RTP 78.8%）**、**FG 贡献压到 16.3%**、**波动 SD 92.6（三张最低，vs 127.3）**、p99 单发赔付 318.7（vs 357，尾部更收敛）。→ HMM 高价值训练目标：给高价值玩家更平滑、少暴涨暴跌的曲线，降低「一波大亏离场」。

## 四、假设与主指标

**核心人群 = 高价值玩家**（沿第八节静态定义：终身下注前 10%；HMM 高价值 day-state 作交叉验证）。同时看全体做护栏。

**主指标**（同期组间，中/均/均99 三视图，刨 kakutei，CNY 口径）：

- **人均投注额、人均投注次数**——高价值验收主口径用 **p90 total_bet / p90 num_bet**（沿第九节支撑项，避免均值/中位被头部掩盖）
- **留存 D1 / D3 / D7**
- GGR、实测 RTP（护栏）、单次投注金额、人均时长

**假设**：

- **H1（A vs default）**：高价值玩家人均投注/留存↑（RTP +2pp 且 BG 结构改善）。
- **H2（B vs default）**：高价值玩家留存↑、离场率↓（低波动、HMM 训练），投注额至少不降。
- **H3（A vs B）**：判定「高 RTP」还是「低波动·高价值训练」对高价值更优。

**护栏**：各组实测 RTP 接近设计（A~97、B~95、default~95）；全体留存不下滑；无异常大额赔付集中。

## 五、分组 SQL（口径）

```sql
CASE
  WHEN MOD(user_id, 100) BETWEEN 0  AND 29 THEN 'A_BGTR97_v3'
  WHEN MOD(user_id, 100) BETWEEN 30 AND 59 THEN 'B_BGTR95_HMM_HV'
  ELSE 'default_95Kai'
END AS hp_group
```

北京日 `(created_at + interval '8 hours')::date`；刨暗保底 `AND LOWER(math_table_id) NOT LIKE '%kakutei%'`；`currency_type='CNY' AND status='COMPLETED'`。

**group_name 编码**（新增实验键 `high_player_test`）：`default_95Kai=0` / `A_BGTR97_v3=1` / `B_BGTR95_HMM_HV=2`。

## 六、样本量与时长

- SS03 CNY 日活约 ~850/天，高价值（前 10%）日活约 ~85/天。每测试臂 30% → 高价值每臂约 ~25/天 × 21 天，够看投注/D1-D3 趋势；**D7 分层留存样本偏薄**，若不足则延长至 28 天或合并周口径。
- 建议**中期（第 10–11 天）看一次护栏**（RTP / 大额赔付），确认无异常再走满 21 天。

## 七、验收标准

- **主判定**：高价值玩家 p90 total_bet / p90 num_bet + D1/D3/D7，A、B 相对 default 的同期差。
- A 或 B 在高价值段显著正向且护栏不破 → 候选纳入选表策略 / 高价值定向 default。
- B 留存正向且投注不降 → 验证「低波动·高价值训练」路线；A 正向 → 验证「加 RTP + BG 结构」路线。

## 八、开放问题

- 高价值人群定义用**静态（终身前 10%）**还是 **HMM day-state**？建议主口径用静态（稳定可复现），HMM 作交叉验证。
- BGTR97_v3 历史实测 RTP 偏高（97bgtr 实测曾 ~101%），需在中期护栏点确认营收影响。
- default(40%) 即为对照，无需额外 holdout。
