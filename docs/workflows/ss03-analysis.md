# SS03 分析(日报 / before-after / AB 策略与数学表对比)

对本地 `data/ss03/dt=*/part.parquet`(见 redshift-s3-sync)用 duckdb 做 SS03 运营与
AB 实验分析。**SS03 数据是真·回合级**:`bet_type ∈ {BASE, FREE}`,FREE 行 `bet_amount=0`,
通过 `root_spin_id` 关联触发它的 BASE 旋转 —— 无需推断即可精确拆分 base/free。

## 通用口径(所有脚本一致)

- 过滤:`game_id='SS03' AND status='COMPLETED' AND currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')`
- **free RTP 贡献** = `SUM(free_payout) / SUM(base_bet)`(行业口径,FREE 行自身 bet=0)
- **trigger rate** = 是某 FREE 行 root_spin_id 的 BASE 行数 / 全部 BASE 行数(含 DIRECT_PURCHASED 购买触发)
- **free_rtp_per_spin** = FREE 派彩 / 触发 BASE 的 bet(经 root_spin_id 逐行匹配)
- 跨期对比一律**总量重算比率**(aggregate ratio),不是日比率求平均
- `partition_ab` 是 JSON 数组,策略分组用 `json_extract_string(partition_ab,'$[0]')`

## 文件

| 文件 | 用途 | 输出 |
|---|---|---|
| `jobs/ss03_daily_report.py` | 单日汇总:overall/base/free 指标 + 按 math_table_id 细分。`--date --out` | stdout(+可选 CSV) |
| `jobs/ss03_daily_series.py` | 逐日序列 + 以 `--cut`(默认 2026-06-10)分界的 before/after 差异;`--drop-last` 丢弃残日 | `data/ss03_daily_summary.csv`, `ss03_before_after_diff.csv` |
| `jobs/ss03_math_table_diff.py` | **按 math_table_id** 的 before/after(RTP/命中/触发率 pp 差 + 日均投注量差) | `data/ss03_math_table_before_after.csv` |
| `jobs/ss03_strategy_diff.py` | **按策略(partition_ab 桶)**的 before/after,指标同上 | `data/ss03_strategy_before_after.csv` |
| `jobs/ss03_strategy_math_timeline.py` | 每策略逐日使用的 math_table(≥`--min-share` 5% 视为 active),压缩成连续区间时间线 | `data/ss03_strategy_math_daily.csv`, `ss03_strategy_math_timeline.csv` |

## 典型用法

```bash
python3 jobs/ss03_daily_report.py --date 2026-06-11
python3 jobs/ss03_daily_series.py --cut 2026-06-10 --drop-last
python3 jobs/ss03_math_table_diff.py --start 2026-05-01 --cut 2026-06-10 --end 2026-07-08
python3 jobs/ss03_strategy_math_timeline.py            # 策略→数学表路由变更史
```

## 注意

- 这些脚本读的是 `data/ss03/dt=*`(全表日同步的布局);cohort 子目录(`data/ss03/<folder>/dt=*`)
  需要自行改 glob 或传路径。
- `n_users` 跨天不能直接求和(同一用户多天),`_agg_period` 里用 user-day 和作代理并已标注。
- `--end` 是开区间,常用于跳过最后一个不完整日。
