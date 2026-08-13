# 动态 RTP 分鱼种报告 —— 完整流程记录

> 目录：`SageMaker/9.24FM01_cold_data/`
> 本文把「动态 RTP 分鱼种」这份报告从**取数 → 切割 → 读取 → 分析 → 出图 → 写报告**的整条链路完整记录下来，
> 作为后续复现、交接与迭代的唯一索引文件。

---

## 0. 一句话概括

从 S3 拉 FM01 冷数据（bullet 明细）→ 按**策略 `strategy_name`** 切成 5 组、转北京时间分日存 parquet
→ 按日期+组别读取 → 计算日度指标 / 留存 / 单组分布 / 生涯 RTP 曲线并出图
→ 人工把图和指标贴进 docx 报告。默认对比 **DYNAMIC_RTP_V3（新版分鱼种）vs DEFAULT_FALLBACK（原始对照组）**。

---

## 1. 文件地图（谁负责哪一步）

| 阶段 | 文件 | 说明 |
|------|------|------|
| **取数 fetch** | `get_current_fm_data.ipynb` | `aws s3 sync` 拉 bullet 原始明细 + 玩家维表 |
| **切割 split** | `run_report_6.9_7.9.py`（`split` 命令）| 清洗 + 按策略切 5 组 + 转北京时间分日写 parquet |
| **读取 load** | `get_dynamic_rtp_player_data.ipynb` / 各脚本内 `load_group()` | 按日期范围+组别并行读 parquet 合并成一个 DataFrame |
| **分析 analyze** | `dynamic_rtp_player_analysis.ipynb`（口径基准）| 全部分析函数的源头（日度、单组、生涯、倍率场、单玩家…）|
| **分析（脚本版）** | `run_report_6.9_7.9.py`（`analyze` 命令）| 把 notebook 分析逻辑抽成可跑脚本，硬编码 6.9–7.9 |
| **分析（参数化版）** | `dynamic_rtp_report/pipeline/run_pipeline.py` | 上面的**参数化 + 补齐 fetch** 版本，日期可传参，一键跑 |
| **方法论文档** | `dynamic_rtp_report/动态RTP分鱼种_数据处理方法与公式说明.docx` | 字段定义 + 全部公式 |
| **结果报告** | `dynamic_rtp_report/动态RTP分鱼种_结果报告_6.9-6.24.docx` | 最近一版成稿报告（6.9–6.24）|
| **历史底稿** | `动态RTP分鱼种2.1版本1.29-4.19.doc` | 更早的 2.1 版本报告 |

### 目录产物
```
FM01bullet/year=/month=/day=/*.parquet        # fetch 落地的原始 bullet 明细
output_parquet/<date>/<date>_<group>_<idx>.parquet   # split 产物（5 组 × 每天）
FM01player/                                    # 玩家维表快照
figs_<start>_<end>/                            # analyze 产物：图 + metrics.json + daily_*.csv
```

---

## 2. 整条流程（4 个阶段）

```
        ┌─────────┐        ┌─────────┐         ┌───────────┐        ┌──────────────┐
 S3  ──▶│  fetch  │──▶ FM01│  split  │──▶ out  │  analyze  │──▶ figs│  写报告 docx  │
raw     └─────────┘ bullet └─────────┘ parquet └───────────┘        └──────────────┘
                                                                       (人工，非脚本)
```

### 2.1 fetch —— 取数
```bash
# bullet 明细（核心数据源）
aws s3 sync s3://oceanhunter-production-data-warehouse/transformed_data/cold_data/bullet/ ./FM01bullet
# 玩家维表快照
aws s3 sync s3://platform-production-data-warehouse/transformed_data/cold_data/dim_game_auth_user_snapshot/<date>/ ./FM01player
```
增量拉取，已有的天不会重拉。

### 2.2 split —— 清洗 + 按策略分组 + 转北京时间分日
函数：`split_dataframe_final_logic(df)`（`run_report_6.9_7.9.py`）

1. **清洗**：只留 `currency_type == 'CNY'`；剔除 `op_code ∈ {B26, TST, TSB, TSO}`（测试/后台单）；数值列缺失填 0。
2. **北京时间**：`event_timestamp(UTC) → tz_convert(Asia/Shanghai) → 去时区`，再取自然日得 `activity_date`。
3. **按 `strategy_name` 切 5 组**：

| 显示名 | group_key | 含义 |
|--------|-----------|------|
| BOOST_POOL | `boost_pool` | 定向奖池分配（大额玩家）|
| DYNAMIC_RTP | `dynamic_rtp` | 动态调控 RTP v1 |
| DYNAMIC_RTP_V2 | `dynamic_rtp_v2` | 动态调控 RTP v2（分鱼种）|
| DYNAMIC_RTP_V3 | `dynamic_rtp_v3` | 动态调控 RTP v3（分鱼种，新版）|
| DEFAULT_FALLBACK | `default` | 原始对照组 |

   > BOOST 逻辑特殊：先找出当天进过 `BOOST_POOL` 的 `(date, user_id)`，这些玩家当天的
   > `BOOST_POOL` + `DEFAULT_FALLBACK` 记录都归入 boost_pool 组，其余 default 记录才留给 default 组。

4. 每个原始文件按天、按组写出 `output_parquet/<date>/<date>_<group>_<idx>.parquet`，多线程（默认 16 并发），已处理日期自动跳过。

### 2.3 load —— 读取
函数：`load_group(start, end, group, columns=…)`
- 按日期范围扫 `output_parquet/<date>/` 下匹配 `<date>_<group>_\d+.parquet` 的文件
- 用 `pyarrow.dataset` 并行读、`unify_schemas` 兼容列差异、`self_destruct=True` 省内存合并成一个 DataFrame
- `columns=` 只读需要的列，大幅提速省内存

### 2.4 analyze —— 指标 + 出图
对 v3 与 default 两组分别跑：
- **日度对比**：`analyze_fish_game()` → 每日 RTP / 人均投注 / 人均利润 / 1·3·7 日留存，画在一起对比
- **单组分布**：`analyze_strategy()` → 子弹等级分布、鱼价值分布、各等级击杀率、首次击杀轮次分布 + 玩家行为均值
- **生涯曲线**：`get_career_stats_by_top_loyal_users(top=100)` → 取生涯最长前 100 名玩家，按 `bet_index` 对齐，画生涯平均投注 / 累积 RTP / 前 200 注理论 rtp_th
- 汇总写 `metrics.json` + `daily_<tag>.csv`

### 2.5 写报告（人工）
打开 `figs_<start>_<end>/`，把图和 `metrics.json` 的指标贴进结果 docx。方法论固定引用说明 docx。

---

## 3. 核心口径 / 公式（速查）

| 指标 | 公式 |
|------|------|
| **北京时间** | `event_timestamp(UTC) → Asia/Shanghai → 去时区`，normalize 得 `activity_date`（当日 00:00）|
| **清洗** | 只留 `currency_type=='CNY'`，剔 `op_code∈{B26,TST,TSB,TSO}`，数值列填 0 |
| **整组 RTP** | `Σpayout / Σbet`（分母 0 取 0）；日度版 `daily_group_rtp`，全局版即「总 RTP」|
| **N 日留存** | `|D0 ∩ D_{0+N}| / |D0|`，N=1/3/7；活跃 = 当日≥1 条投注；分母统一 `num_users_day0` |
| **人均投注/利润** | `Σbet / num_users_day0`、`Σprofit / num_users_day0` |
| **回流玩家** | `(activity_date − 上次投注日).days > 7` |
| **子弹击杀率** | `Σkilled / 子弹总数`；分 level：`kills(level)/total(level)` |
| **首次击杀轮次** | 玩家按时间排序，首个 `killed=1` 的序号；再对玩家取均值 |
| **玩家行为均值** | 先按 `user_id` 聚合，再对玩家求平均（投注次数/累计投注/单笔投注/子弹level/鱼价值）|
| **生涯瞬时 RTP(i)** | `Σ_users payout(i) / Σ_users bet(i)`（按 `bet_index` 对齐）|
| **生涯累积 RTP(i)** | `Σ_{k≤i}payout / Σ_{k≤i}bet`；着色 >1.0 红 / ≥target 绿 / <target 紫，target=0.96 |
| **理论 RTP(i)** | `mean(rtp_th(i))`，rtp_th = 系统每注下发的理论 RTP；动态策略随生涯变化，对照组近似常数 |

> 完整字段定义与公式见 `动态RTP分鱼种_数据处理方法与公式说明.docx`。

---

## 4. 怎么跑（参数化 pipeline，推荐）

```bash
cd /home/ec2-user/SageMaker/9.24FM01_cold_data/dynamic_rtp_report/pipeline
chmod +x run.sh          # 首次

# 出一份完整新报告的标准顺序
./run.sh fetch                                          # 1. 同步最新原始数据（需 AWS 凭证）
./run.sh split   --start 2026-06-09 --end 2026-07-09    # 2. 切割（已处理日期自动跳过）
./run.sh analyze --start 2026-06-09 --end 2026-07-09    # 3. 产出图表指标到 figs_<start>_<end>/
# 4. 打开 figs_2026-06-09_2026-07-09/ 把图贴进结果 docx
```

- 环境：需 `pandas/numpy/pyarrow/matplotlib/seaborn`；`run.sh` 默认走 conda 环境 `pytorch_p310`（已自带）。
- 常用参数：`--groups`（默认 v3+default）、`--target-rtp 0.96`、`--top-users 100`、`--zoom-rounds 200`、`--workers 16`、`--force-rerun`。
- 直接跑原始脚本（日期硬编码 6.9–7.9）：`python run_report_6.9_7.9.py split|analyze`。

### 产物（`figs_<start>_<end>/`）
- 对比图：`cmp_daily_rtp.png`、`cmp_total_bet_per_user.png`、`cmp_profit_per_user.png`、`cmp_retention_day{1,3,7}.png`
- 单组图（前缀 = tag，如 `v3_`/`default_`）：`*_bullet_level_distribution` `*_fish_value_distribution` `*_kill_ratio_by_level` `*_rounds_to_first_kill` `*_career_avg_bet` `*_career_cum_rtp` `*_career_rtp_th_zoom`
- 数据：`daily_<tag>.csv`（每日指标）、`metrics.json`（每组汇总）

---

## 5. 报告结构与结论（以 6.9–6.24 成稿为例）

**数据范围**：FM01 全量，2026-06-09 ~ 2026-06-24，对比 DYNAMIC_RTP_V3 vs DEFAULT_FALLBACK。
（原计划取到 06-25，但取数时仅落库到 06-24。）

**报告章节顺序**：
1. 数据选取范围
2. 效果对比总结（3 条要点）
3. 两组综合数据对比（日度 RTP / 投注 / 利润 / 1·3·7 留存 4 组对比图）
4. 两组核心指标对比表
5. 各分组分鱼种、子弹与击杀分布（每组 4 张分布图）
6. 每个玩家按生涯走（top100，生涯投注 / 累积 RTP / 前 200 注理论 rtp_th）

**核心指标对比（6.9–6.24）**：

| 核心汇总 | 动态 RTP v3 | 原始对照 default |
|----------|-------------|------------------|
| 总订单量 | 27,175,240 | 62,219,676 |
| 总 RTP | 97.56% | 96.05% |
| 唯一玩家数 | 9,424 | 22,016 |
| 玩家平均投注次数 | 2883.62 | 2826.11 |
| 玩家平均累计投注额 | 9560.90 | 9963.79 |
| 玩家平均单笔投注额 | 3.32 | 3.53 |
| **玩家平均首次击杀轮次** | **17.05 轮** | **24.72 轮** |
| 玩家平均子弹 level | 1.31 | 1.28 |
| 玩家平均打击鱼价值 | 144.06 | 139.89 |

**结论**：
- v3 在维持接近目标的整组 RTP（97.56%）的同时，把首次击杀轮次从 24.72 压缩到 17.05，明显加速早期击杀反馈。
- v3 的理论 rtp_th 在生涯前期被抬升（前 200 注约 1.62 → 1.0 附近逐步回落），原始组则全程稳定在约 0.9627 —— 体现「冷启动加成 + 逐步回落」的分鱼种设计。
- v3 整组 RTP（97.56%）仍略高于目标 0.96，后续可下调参数使其更快回归目标；且未以牺牲投注规模换早期体验（人均累计投注与原始组基本持平）。

---

## 6. 与原文件的关系

| pipeline 文件 | 对应原文件 |
|---------------|-----------|
| `pipeline/run_pipeline.py` | `run_report_6.9_7.9.py`（日期硬编码）+ notebook 取数步骤 |
| `fetch` 阶段 | `get_current_fm_data.ipynb` 的 `aws s3 sync` |
| `split` / `analyze` | `run_report_6.9_7.9.py` 的对应逻辑（一字不改）|
| 分析函数口径基准 | `dynamic_rtp_player_analysis.ipynb` |

`run_report_6.9_7.9.py` 保留不动；`pipeline/` 是它的**参数化 + 补齐取数**版本。
更细的 pipeline 运行说明见 `pipeline/README.md`。

---

## 7. 一次性辅助脚本（历史，非主线）

`get_dynamic_rtp_player_data.ipynb` 里还有一段把 v2 数据按 `before/after` 阈值桶 + `user_id`
拆成单玩家 parquet 并打包 zip 的逻辑（用 `5.6player_threshold.csv` 做 bucket 映射），
用于早期 5.6–5.12 的 A/B 抽样交付，不属于当前报告主流程。
