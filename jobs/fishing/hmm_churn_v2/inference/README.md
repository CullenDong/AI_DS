# inference — IO-HMM 在线打分包

自包含的线上打分包:加载训练好的 artifact,对玩家逐投注日打 churn 标签。**纯 numpy / pandas,不依赖 hmmlearn 或训练代码**。

## 结构

```
inference/
  scripts/io_hmm_infer.py     在线打分器(load_model / init_state / update / score / current_label)
  assets/iohmm_model.json     训练好的模型 artifact(参数 + 标准化常数 + 分档门槛)
  tests/test_io_hmm_infer.py  数据处理 + 推理 + 增量==批量 一致性测试
  conftest.py                 让 tests 找到 scripts/
```

## 用法

把 `scripts/` 放进 import 路径(或从该目录运行),然后:

```python
import io_hmm_infer as inf
model = inf.load_model("inference/assets/iohmm_model.json")   # 服务启动时载入一次

# 线上(有状态,每个新投注日调一次):
state = inf.init_state()                       # 每个用户一份,持久化(几个浮点)
state, label = inf.update(state, bet_day_features, model)
# label = {k, stage, p_stop, risk};把 state 存回该用户

# 批量(给一批玩家历史一次性打标):
labels = inf.score(player_df, model)
current = inf.current_label(player_df, model)  # 每人最新标签
```

`bet_day_features` / `player_df` 需带和训练导出相同的上游特征(Golden 7 + `bet_amount_today` + `user_id` + `bet_date`)。打分是 filtered(只看过去),可增量;k=1(首投日)标 `risk="first_day"`,由首日模型单独处理(不在本包)。

依赖:`numpy`、`pandas`。测试:在本目录跑 `python -m pytest`。

## artifact 来源

`assets/iohmm_model.json` 由训练侧 `io_hmm.py fit`(在上级目录)产出,复制到此。重训后用新 artifact 替换本文件即可,无需改代码;artifact 自带门槛和标准化常数,换时整体替换、不要跨版本混用。标签字段含义见 `../report_cn.md` §2 / §1.5。
