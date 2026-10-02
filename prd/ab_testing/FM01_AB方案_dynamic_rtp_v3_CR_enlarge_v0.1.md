# FM01 捕鱼 · AB 分组方案 `dynamic_rtp_v3_CR_enlarge` · v0.1

> 实验名：**`dynamic_rtp_v3_CR_enlarge`**。本期只改**分组配比逻辑**：`dynamic_rtp_v3` 40% / `CR 挽留` 30% / `default` 30%（P0 基准/holdout）。分流键、group_name 编码、各组机制内容均沿用上期，不在本期改动。
>
> ✅ **已上线（DB 精确定位）**：变更在 **2026-09-21 的 ~80 分钟停服部署窗口**内完成——停服前最后一条 bullet = **2026-09-21 20:57:18 UTC（13:57:18 PDT）**，恢复后第一条 = **2026-09-21 22:17:20 UTC（15:17:20 PDT）**，恢复时新 40/30/30 已生效（dynamic_rtp 落 [40,100)=0）。即**美西 09-21 约 13:57~15:17 PDT**。此后 dynamic_rtp 全部落 MOD(user_id,100) [0,40)；近 3 天 SRM 实测 **dynamic_rtp 41.0% / retention 30.3% / default 28.7%**，与 40/30/30 一致。权威口径 `jobs/fishing/fm01_grouping.py`（`FM01_ENLARGE_UTC = 2026-09-21 21:00 UTC` = 14:00 PDT，取断档区内、不误分 + `GROUP_CASE`）已更新。

## 一、变更点（唯一）

| 组 | 上期配比 | **本期配比** | 说明 |
|---|---|---|---|
| dynamic_rtp_v3 | 行为分配（非随机） | **40%** | 改为按 user_id 尾号后两位随机分配（同时消除原选择偏差） |
| CR 挽留（retention） | 尾号 0/1 = 20% | **30%** | |
| default（P0 基准/holdout） | 其余 | **30%** | 常驻 holdout / 绝对基准 |

> 只调"多少比例的人进哪个组"，**不新增/不修改 group_name**，不改各组机制参数。

## 二、分流键与配比（user_id 尾号**后两位** MOD 100）

| 组 | user_id 尾号后两位 | 占比 |
|---|---|---:|
| **dynamic_rtp_v3** | 00 – 39 | 40% |
| **CR 挽留** | 40 – 69 | 30% |
| **default（P0）** | 70 – 99 | 30% |

- **user 级判定**，同一人终身稳定归组；user_id 尾号后两位均匀 → SRM 基线成立。

## 三、风控/封控 = 策略，**不是分组**（重要）

- **风控/封控（`RC_FISHING*` / `%RISK_CONTROL%`）是一层"策略"，不是一个 AB 组。** 它只改玩家的**子弹策略（RTP）**，**不参与 AB 分流**。
- 命中风控的玩家**仍属其 user_id 尾号所定的组**（dynamic_rtp / CR / default），不单列 `risk_control` 组。
- 风控与分组**正交**：分组只看 user_id 尾号；风控作为策略叠加在任意组之上（分析时作为协变量/注记，不当组）。
- 对比：**农场剔除**不同——农场是**前置剔除出实验人群**（不进任何臂），风控不剔除、只改策略。

## 四、分组判定逻辑（SQL CASE，只按 user_id；不含 group_name、不含风控组）

```sql
-- 农场 PID 命中者在此之前已剔除出实验人群
CASE
  WHEN user_id % 100 BETWEEN 0  AND 39   THEN 'dynamic_rtp'   -- 40%
  WHEN user_id % 100 BETWEEN 40 AND 69   THEN 'retention'     -- 30% (CR 挽留)
  ELSE 'default'                                               -- 30% (尾号后两位 70-99) P0 基准
END
```

- **不再有 `risk_control` 分支**——风控是策略、不进分组 CASE（更正旧 `GROUP_CASE` 口径）。
- 取数口径沿用 `BASE_FILTER`（CNY、剔测试 op_code）、`event_timestamp`（UTC）。
- 落地：改 `jobs/fishing/fm01_grouping.py` 的 `GROUP_CASE` 为上表（去掉 risk_control 分支），线上路由按尾号后两位 00-39→dynamic_rtp_v3、40-69→CR、70-99→default；风控策略照常独立生效、不改玩家所在组。

## 五、实验时长

- **一期 = 14 天**（对齐 `minimum_duration`，覆盖工作日/周末节律）；样本量不足则顺延至 28 天或并周。

## 六、假设（Hypothesis）

- **H1（dynamic_rtp_v3 40% vs default）**：动态 RTP 冷启动加成 → 新玩家/冷启动段留存与参与提升（RTP/GGR 可能略降，作护栏）。
- **H2（CR 挽留 30% vs default）**：挽留 ON vs OFF（default=OFF，均不吃 RTP）→ D1/D3/D7 留存提升；按 HMM 状态（T1/S1/S2/S3）分层比。
- **H3（dynamic_rtp_v3 vs CR 挽留，次要）**：两种调控对留存的相对效果与适用人群差异。

## 七、检验指标

| 层级 | 指标 | 口径 |
|---|---|---|
| **主指标** | D1 / D3 / D7 留存 | 当天有投注即活跃、按天算、不去重、右截断置空；ITT（assignment 人群） |
| 主指标（挽留臂） | 分 HMM 状态 D1/D3/D7 | 状态两臂统一口径、分组时冻结 |
| 观测 | 人均投注额 / 投注次数 / 游玩时长 | 缩尾均值 + 中位双口径 |
| **护栏** | 实测 RTP、人均净亏、GGR | dynamic_rtp 吃 RTP；挽留不吃 RTP。**风控命中占比作协变量监控**（防其 RTP 影响混淆组间比较） |
| SRM | 三组实测占比 vs 40/30/30 | 偏离即查分桶/曝光 |

## 八、结构图

![FM01 dynamic_rtp_v3_CR_enlarge AB 分组结构](data/output/ab_testing/AB结构图_FM01_dynamic_rtp_v3_CR_enlarge.png)

## 九、注意

- **default 30% = 常驻 holdout / 绝对基准**（P0），用于两臂对照。
- **ITT 优先**：回 assignment 人群分析，不用 triggered（避免 post-treatment 偏差）。
- dynamic_rtp 改随机分配后，本期起其效果**可作因果解读**（上期为行为标签、含选择偏差，仅相关）。
- 定稿前跑 `ab_grouping_verify.py` 确认实测 40/30/30 与尾号后两位均匀（SRM）。
