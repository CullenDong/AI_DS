# Super Ace BG/FG 推断(倍率 + 停顿法)

针对**没有** base/free 标志的旧投注级导出(`bituslabs-team-ai/superace/2024-01-*`,PHP 平台,
59 列,1 行 = 1 笔付费旋转,免费游戏收益折叠进触发笔),用行为信号推断哪些旋转触发了免费游戏。
> 若数据源是 SS03 fct_bet_orders(有 `bet_type`/`root_spin_id`),**不要用本方法**,直接精确拆分
> (见 ss03-analysis.md)。本方法只服务无标志的历史数据。

## 原理(双数据集验证)

- 免费旋转动画会推迟玩家下一笔投注:未中奖旋转的到下一笔中位间隔 ~2s(自动旋转基线),
  随中奖倍率单调上升,m>50 时中位 ~70s。该签名是客户端固有的,跨平台/年份稳定。
- 标签(m = payout/bet;gap = 同玩家下一笔时间差):
  - **FG-strong**:`m>=25 AND gap>=45s` → 触发率落在官方 scatter 区间(实测 **1/202~1/207**,
    10 天 × ~1.1 亿行/天;2026 CNY 平台 686K 行为 1/285)
  - FG-mid:`m>=10 AND gap>=30s`(偏松,混入纯级联,单独报告)
  - BG-big-win:m>=25 但间隔短 = 纯 base 级联大奖(数量 ≈ FG-strong,只看倍率会误判一半)
- 关键业务数:**FG-strong 承载全部派彩的 ~25.2%**(即 97% RTP ≈ 73% BG + 24% FG);
  纯 BG 行 RTP ≈ 0.729,FG 触发笔平均总价值 ≈ 61x(中位 46x,P90 120x)。

## 文件与产物

| 文件 | 用途 | 输出 |
|---|---|---|
| `tools/superace/sa_fg_gap_analysis.py [YYYY-MM-DD ...]` | 按天跑:倍率组×间隔统计 + FG 触发率/派彩占比(默认 2024-01-01..10,断点续跑) | `data/output/superace_fg_gap_2024-01.json` |
| `tools/superace/sa_fg_mult_distribution.py` | 10 天 BG vs FG-trigger 的 m=payout/bet 分箱分布(分级箱宽) | `data/output/superace_fg_bg_multiplier_distribution.json` + `..._payout_distribution.png` |

## 工程要点(必守)

- **玩家旋转散在当日 30 个 Trino 分片里**——间隔必须跨全部分片计算;单文件算会把基线虚高 ~30 倍,信号全毁。
- 1.19 亿行/天的窗口函数会把 duckdb OOM(window 不能完全溢写):先物化
  `(hash(cust), epoch(billtime), bet, payout)` 到 duckdb 文件,再按 `c % 32` 分桶逐桶开窗,
  用整数秒间隔直方图跨桶合并(中位数精确),6GB 内存跑完一天。
- 已知偏差:总值 <25x 的"失败免费游戏"留在 BG(BG 20~26x 尖峰含此残留),FG 占比是下界。

## 分布形状备忘

BG:76% m=0 + 幂律衰减小奖(50% 派彩 ≤8x);FG:旋转峰 28~34x、派彩峰 55x(50% 派彩 ≤70x),
~1500x 处有断口后接 2000~5000x 重触发尾。另见 skill `.claude/skills/game-bet-eda`。
