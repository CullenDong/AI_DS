# 游戏运营 Top20 Excel 报告(含联网研究)

把游戏维度运营 CSV(供应商/游戏聚合表)加工成格式化 Excel:按总投注取 Top20,
联网查各游戏官网得主力市场/波动/上线年,按「运营分(55%)×市场契合分(45%)→最终分(归一化100)」
重排,Δ=相对总投注排序的升降。输出 3 个 sheet:Top20重排 / 评分说明 / 全量数据。

## 文件

| 文件 | 数据源 | 特点 |
|---|---|---|
| `tools/reports/build_top20_report.py` | CasinoPlus(菲律宾,PHP,`data/input/casinoplus_2024_01-06.csv`) | 波动=数据档(最大倍率分档)+官网双口径;月均玩家=玩家数/6 |
| `tools/reports/build_game_report.py` | CNY 大中华/亚洲向(`data/input/ivi_game_all.csv`,历史上依次处理过 game_id_filter / filter_together) | 供应商=沙巴体育/AG真人/PG/JDB/EVO;非 slot 用官网波动口径;RTP=100−莊家優勢% |

运行:`python3 tools/reports/build_top20_report.py`(改数据源=改文件顶部 SRC/OUT 常量;
两脚本的 RESEARCH dict 内置了已查过的游戏官网结论,新游戏需补条目)。

## 约定

- 评分公式与所有假设(供应商接受度/波动偏好/题材契合权重)写在「评分说明」sheet,可调。
- 数据算波动分档:最大倍率 <50 Low /<200 ML /<1000 M /<3000 MH /<8000 High /≥8000 VeryHigh;
  官网与数据不一致显示为 `数据(官网X)`。
- 输入放 `data/input/`,产出放 `data/output/`(均 gitignored)。
- 曾处理过的口径坑:`orders_` 前缀=订单口径与供应商口径并存(重复计量,需用户定夺);
  游戏名可能整列为空(只有代码);详见 git 历史与对话记录。
