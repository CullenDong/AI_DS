# Fortune Gems 倍率分布(Normal / Extra Bet 分离)

对 Fortune Gems(JILI,无 free spin 的 3×3+倍率轮游戏)的投注级数据
(`bituslabs-tsplayerai/fortune_gems/year=/month=/day=`,59 列,同 superace 旧导出结构)
计算全月 m=payout/bet 离散分布,并把 Normal 与 Extra Bet(加押 1.5x)两种模式分开。

## 游戏机制 ↔ 数据的对应(2024-01 全月 25.6 亿行验证)

- `W_final = W_base × M`,M∈{1,2,3,5,10,15}:中奖倍率 **100% 落在理论格点**上
- 最大派彩 375x:max m **精确=375**,无超出(防御性封顶)
- RTP 0.9675 / 命中率 12.04%,与官方 96.65%/12% 口径吻合
- **Extra Bet 指纹**:总押=底注×1.5 但按底注赔付 → Extra 局 m = 格点值÷1.5,
  约 1/3 变循环小数(1.333…, 2.1333…),正常局绝不产生循环小数

## 押注档判定(数据驱动,勿硬编码菜单)

对每档统计**循环小数率**(m 非 0.2 整数倍占比):≈0 → 纯 Normal 档;≈5.1% → 纯 Extra 档;
中间 → 歧义档(既是底注按钮又=另一底注×1.5,实测有 **3 和 300** 两个)。

## 歧义档分配(v6 最终口径)

约束"Normal 绝不含循环小数" + 目标"两模式 RTP 相等":
1. 循环小数行 → 100% Extra(铁律)
2. 非循环小数行按单一残余率 r 分配,**r 解方程使 Normal RTP = Extra RTP**
   (2024-01 解得 r\*=0.90677,两侧 RTP 均=0.967504,Extra 占投注 42.7%)
> 取舍:①指纹占比守恒(~0.346) ②两 RTP 相等 ③Normal 无循环小数 —— 三者只能同时满足两个,
> v6 选 ②③。要①的口径改回指纹比例即可(脚本历史版本有实现)。

## 文件与产物

| 文件 | 输出 |
|---|---|
| `tools/fortune_gems/fg_month_distribution.py` | `data/output/fortune_gems_2024-01_distribution.json`(normal/extra 两段完整分布,extra 带 `base_multiplier`=m×1.5)+ `_report.md` |
| (分析产物)玩家/时段聚合、单日分布 | `data/output/fg_player_agg.parquet`, `fg_hour_agg.parquet`, `fortune_gems_multiplier_distribution.json`(单日合并版), `fortune_gems_player_analysis.md`, `fortune_gems_2024-01_distribution.png` |

## 读取坑

分区列 `year` 在不同 shard 是 int16/dictionary 混合类型 → `pq.read_table(列表)`/dataset API 抛
ArrowTypeError,必须逐文件 `pq.ParquetFile(f).read()`。丢 `bet_amount<=0` 行(产生 inf)。
