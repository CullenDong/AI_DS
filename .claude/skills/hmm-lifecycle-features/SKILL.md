---
name: hmm-lifecycle-features
description: >
  FM01 (fish_hunter) user-lifecycle HMM feature engineering + labeling. Builds a
  per-user-per-day feature table from bullet-level data (sessionize, aggregate,
  derive history/trend features) that feeds an HMM producing lifecycle-state
  labels (T1 initial / S1 low / S2 engaged / S3 escaped). Use when the user wants
  to compute HMM lifecycle features, fit/apply the HMM, label players by lifecycle
  state, or add a lifecycle tag to the player label library (alongside new/old).
---

# HMM 生命周期特征工程 + 打标签（FM01 fish_hunter）

给玩家贴**生命周期状态标签**的完整链路:子弹级数据 → 每用户每天特征表 → HMM → 状态标签 **T1(初始) / S1(low) / S2(engaged) / S3(escaped)**。这套标签用于个性化挽留 AB 的分层(HMM 状态处理无关、两臂统一口径,见 `prd/ab_testing/AB分组规格汇总_v0.1.md` 挽留部分)。

## 权威脚本（参考,勿重写口径）

源自 `NxNiki/aws-example`(dev 分支)`jobs/fish_hunter/`,本地副本在本 skill `reference/`:
- **`feature_engineer_life_cycle.py`** — PySpark ETL 主作业(SageMaker Spark 集群跑)。产出 HMM 特征表。
- **`feature_engineer_life_cycle_submit.py`** — 从本地提交上面这个作业为 SageMaker PySpark processing job。

> 这两个是**特征工程**(feature engineering)那一步;**HMM 拟合/打状态**是下游(不在这两个脚本里)。这套跑在 **SageMaker/Spark 读 S3 parquet**,不是本项目的 Redshift 直连。

## 关键口径（必须一致）

- **时区**:`created_at + 8 小时` = 北京时间;`natural_bet_date` = 北京自然日。
- **会话化(sessionize)**:相邻两发间隔 **> 180 秒 = 新会话**;会话按**首发所在日**归属 → **跨零点的会话整段算在起始日**。为此每月扫描窗口两端各**多扫 1 天**(保会话完整),输出只留本月 bet_date。
- **输入**:`fish_bullets_group_tag` bullets 数据集(S3,分区 `year=/month=/day=` 或 `period=`);先用 `etl_fish_bullets_group_tag_submit.py` 刷新。过滤 `currency_type='CNY' AND game_id='FM01'`。
- **输出**(S3 `OUTPUT_ROOT` 下):`user_day_base_fm01_cny/month=*`(月度基础聚合)、`..._dedup`(跨月去重)、`selected_hmm_features_fm01_cny/month=*`(最终特征)+ `csv/` 单文件导出。

## 四阶段流程（主脚本）

1. **月度 user-day 基础聚合**:逐月读 bullet parquet(分区裁剪) → 会话化 → 聚合到 (user_id, bet_date)。基础字段:`bet_count_today / bet_amount_today / avg_bet_one_time_today / payout_today / profit_today / rtp_day / current_balance_day_max / max_consecutive_loss_count_today`(当日最长连亏)`/ bullet_level_change_count_day / multiplier_change_count_day`,以及**鱼价值档位占比** `low_ratio(2-10) / medium_ratio(15-130) / high_ratio(150-200) / ultra_ratio(500-1000)`。
2. **跨月去重**:(user_id,bet_date) 只保各自月窗口内的行,平手按活跃量(bet_count/bet_amount)破。
3. **HMM 特征派生**(per-user 时间窗:`w_hist`=历史全部前一天、`w_7`=近 7 个投注日)。
4. **单文件 CSV 导出**。

## 喂给 HMM 的最终特征(stage 3)

| 特征 | 含义 |
|---|---|
| `no_bet_streak_days` | 距上个投注日的空档天数(datediff−1) |
| `bet_amount_ratio_today_vs_history` | 今日投注额 / 历史日均(w_hist) |
| `bet_count_ratio_today_vs_history` | 今日发数 / 历史日均 |
| `avg_bet_one_time_today_log` | log1p(今日单发均额) |
| `rtp_7_bet_days` | 近 7 投注日滚动 RTP(Σpayout/Σbet) |
| `loss_streak_ratio_today` | 当日最长连亏 / 当日发数 |
| `current_balance_max_to_avg_bet_ratio` | 当日最高余额 / 历史单发均额 |
| `target_selection_entropy` | 打鱼档位(low/med/high/ultra)选择熵,除以 log(4) 归一 |
| `multiplier_change_count_ratio` | 倍率切换次数 / 发数 |
| `bullet_level_change_count_ratio` | 子弹档切换次数 / 发数 |

(另带一批 raw/debug 原始列供核对。)

## HMM 状态标签

特征表 → HMM(下游拟合/解码)→ 每 (user, 时段) 一个状态:
- **T1** 初始(首日/首次登录的第一个自然日)· **S1** low(低活跃/低投入)· **S2** engaged(活跃投入)· **S3** escaped(流失/逃逸)。
- 状态由**行为序列**算出、与"是否被挽留"无关 → 挽留 AB 两臂都能算,消除 post-treatment 分层偏差。

## 用来打标签(用户目标)

- 现有标签库:**新老玩家**;新增维度:**HMM 生命周期状态 T1/S1/S2/S3**。
- 打标签 = 跑特征工程(SageMaker) → 对特征跑 HMM 出状态 → 把状态写回玩家标签(与 [[ss03-mathtable-report]] 之类分析或挽留 AB 分层对齐)。
- 若要在**本项目 Redshift** 侧复现(不走 SageMaker):严格按上面「关键口径」(180s 会话、北京日首发归属、月边界±1天、鱼价值档位、w_hist/w_7 窗口)重写等价 SQL/pandas;口径以 `reference/feature_engineer_life_cycle.py` 的 `USER_DAY_BASE_SQL` 与 stage-3 派生为准。

## 坑

- 会话跨零点、月边界必须±1天扫描,否则会话被切断、首日归属错。
- `rtp_7_bet_days` 是"近 7 个**投注日**"不是自然 7 天(跳过空档日)。
- 熵除以 log(4) 归一到 [0,1];4 = 档位数(low/med/high/ultra)。
- 特征是**每用户每天**粒度;HMM 是在**用户的日序列**上解码状态,不是单点分类。
