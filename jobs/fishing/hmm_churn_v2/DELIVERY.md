# Decision Tree + IO HMM 玩家流失打标（FM01 / CNY）

从投注日志产出每个玩家的流失风险标签,面向**干预/留存**用途,不追求高精度预测:目标是给出可用的高危玩家标记,边界见下文。

## 线上服务流程

收数据 → 过模型 → 出标签,**有状态、逐投注日**:

1. **收数据** —— 上游特征管线在每个投注日产出该玩家的当日特征(以 DS 当前导出为准)。粒度是**投注日**。
2. **过模型** —— 每个用户持久化一小份状态(前向 message + tenure + 累计投注)。新投注日到来 → **O(1) 增量更新** → 出标签;不必每次重算全历史。按 tenure 路由:首投日(k=1)走首日模型(标 `first_day`),第 2 个投注日起走 IO-HMM。
3. **出标签** —— `stage / p_stop / risk`(见下)。

推理是纯 numpy/pandas,不依赖 hmmlearn,可直接嵌进服务;每用户只需存几个浮点。

## 交付物

| 文件 | 是什么                                                   |
|---|-------------------------------------------------------|
| `<csv>_iohmm_labels.csv` | 逐 (user, 投注日) 的流失标签:`stage` / `p_stop` / `risk`(离线批量) |
| `inference/` 包 | 在线打分包:`scripts/io_hmm_infer.py`(纯 numpy/pandas,无 library 依赖)+ `assets/iohmm_model.json`(训练好的模型 artifact:参数 + 标准化常数 + 分档门槛)+ `tests/` |
| `first_day_model.py` | 首投日(k=1)模型                                            |
| `io_hmm.py` / `explore_churn_states.py` | 训练 / 分析代码                                             |
| `report_*.md` | 各模型背后的分析                                              |

## 模型说明

流失定义为:玩家在某投注日之后 46 天内没再投注,即"离开"。两个模型效用通过玩游戏时长（tenure）拆分。

- **回流玩家(第 2 个投注日起)**:Input-Output HMM。行为定状态(Low / Engaged / Lapsed);tenure 和累计投注驱动转移和流失 hazard `P(离开 | 状态, tenure)`。
- **首投日(约占全部用户 75%)**:单独用首日当日信号建模(首日没有历史)。74.1% 首投后再不回;模型读首场的投注量与输赢。

## 标签说明

- `stage` —— 当天行为状态(Low / Engaged / Lapsed)。当前行为的分类/刻画。编号固定:Low=0、Engaged=1、Lapsed=2,重训不变。
- `p_stop` —— 模型的一步流失 hazard(下一步离开的概率),∈ [0, 1]。
- `risk` —— 校准到**真实** 46 天流失率的档位:
  - **high** —— 当前标签的 top 15%;约 **70% 真的不回来**。最紧、最高确信的干预目标。
  - **med** —— 流失率约 30–70%。
  - **low** —— 流失率低于 30%。
  - **first_day** —— 首投日玩家(由首日模型打分,不走 IO-HMM)。

一个玩家的**当前标签** = 他最新投注日的一行。
**注：这里的risk阈值为手动设定的比例。可以调整。**

## 使用方法

**线上(每个新投注日调一次):**

```python
import io_hmm_infer as inf
model = inf.load_model("<csv>_iohmm_model.json")   # 服务启动时载入

state = inf.init_state()                            # 每个用户一份,持久化(几个浮点)
# 该用户来了一个新投注日:
state, label = inf.update(state, bet_day_features, model)
# label = {k, stage, p_stop, risk};把 state 存回该用户,等下一个投注日
```

**批量回填 / 分析(给一批玩家历史一次性打标):**

```python
labels = inf.score(player_df, model)               # 逐 (user, 投注日)
current = inf.current_label(player_df, model)       # 每人最新标签
```

`bet_day_features` / `player_df` 需带和训练导出相同的上游特征。两条路径标签一致(增量 == 批量,已测)。`<csv>_iohmm_labels.csv` 是批量路径的离线产物,供回填/分析。

调 high 档阈值只需改 `io_hmm.py` 的 `HIGH_SHARE`,跑 `python io_hmm.py <csv> calibrate` 重算。

## 必读注意

- **干预前先过滤到近期活跃的玩家。**
- **这里的流失的准确定义为可逆的空白期（46天）** 约 26.6% 的回流玩家曾从 >46 天的沉默中回来过。老玩家回流概率高所以重点是在周期2-4日干预。
- **状态编号固定**——`stage` 按画像对齐为 Low=0 / Engaged=1 / Lapsed=2,重训不换号。
- **在线 vs 离线**:在线打分是 filtered,离线 labels 文件是 smoothed,两者 `stage` 一致约 98.3%、`risk` 一致约 99.8%,但不完全相同。
- **首日路由**:IO-HMM 不参与首投日玩家打分。首日玩家统一标 `first_day`,由首日模型处理。

## 精度

用户钟标签下,行为预测离开的 AUC:state-only 0.70,行为 0.82,行为 + state 0.82,GBM 0.85;首日模型 0.72(树)/ 0.76(HGB),校准良好。`risk` 档是**校准过的概率**,不是完美分类器——"high" 意味着约 70% 会走,不是 100%。

## 运维 / 重训

训练是**离线、定期**做的,和在线打分分开;重训只换 artifact,线上热加载新 JSON 即可。

- 训练侧需要 `hmmlearn`(anaconda base;`/opt/anaconda3/bin/python3`);**在线侧 `io_hmm_infer.py` 纯 numpy/pandas**。
- 重训:`python io_hmm.py <csv> fit 3` → 产出新 `<csv>_iohmm_model.json`(在线用)+ 离线 labels CSV。重训首日模型:`python first_day_model.py <csv>`。
- 状态编号按画像固定(Low=0 / Engaged=1 / Lapsed=2);artifact 自带门槛和标准化常数,换 artifact 时一起换,不要跨版本混用。
- 测试:`python -m pytest`(覆盖数据处理、批量推理、增量 == 批量一致性)。

## 未含 / 待定

- 线上特征管线(在生产环境实时算上游特征)——服务时需要和导出相同的那套特征。
- `状态 -> 业务动作` 映射,以及生产部署口径。
- 完整分析见 `report_lifecycle_model.md`、`report_first_day_model.md`、`report_churn.md`。

## 本版数据与变更

- 训练数据:DS 导出的 (玩家, 投注日) 特征表,2025-05-22 至 2026-09-17(2026-09-18 导出;投注日切分由 DS 聚合侧定义)。357,251 用户 / 844,690 投注日;IO-HMM 训练域(k≥2)92,596 用户 / 487,439 投注日,46 天基础流失率 21.4%(可观测投注日 394,964)。
- `stage` 编号固定为 Low=0 / Engaged=1 / Lapsed=2,由 `io_hmm.py` 按发射均值自动对齐;上一版按 fit 结果编号,重训后可能换号。
- risk 分档门槛(随 artifact):med `p_stop >= 0.264`,high `p_stop >= 0.628`(上一版 0.312 / 0.620)。当前标签占比 high 15.0% / med 52.2% / low 32.8%,对应真实 46 天流失率 70.0% / 44.1% / 9.5%。
- 流失 hazard `P(stage -> STOP | tenure)`,低 / 中 / 高 tenure:Low 36.3% / 6.7% / 0.8%,Engaged 17.6% / 4.9% / 1.1%,Lapsed 38.7% / 9.7% / 1.7%。
- 首日模型:可观测首投日 308,740,基础流失 77.1%,AUC 树 0.724 / HGB 0.757。
- 交付包不再包含 DS 导出脚本;特征口径以 DS 当前导出为准。
