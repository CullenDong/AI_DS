# Claude 数据科学 Agent 脚手架

自然语言驱动的 DS agent:用户描述任务,Claude 通过 tool-use 循环选择并执行数据工具。
状态:可用脚手架,工具面较小(v0)。

## 文件

| 文件 | 角色 |
|---|---|
| `agent/main.py` | CLI 入口:`python -m agent.main "Summarize data/sample.csv"`(需 `ANTHROPIC_API_KEY`) |
| `agent/loop.py` | 核心循环:发 `{system, tools, messages}` → `stop_reason=="tool_use"` 则执行全部工具块、结果作为 user turn 回填 → 否则打印文本退出。`MAX_TURNS=20` |
| `tools/agent_tools/registry.py` | 工具 schema(发给 Claude)+ dispatch(异常转字符串回传给模型自愈) |
| `tools/agent_tools/state.py` | `FRAMES` dict:按名字持有内存 DataFrame,跨工具调用复用 |
| `tools/agent_tools/data_tools.py` | `load_dataframe`(CSV/Parquet/Excel/JSON → FRAMES) |
| `tools/agent_tools/eda_tools.py` | `describe` / `value_counts` / `correlation` |
| `tools/agent_tools/viz_tools.py` | `plot_histogram`(存 outputs/) |
| `prompts/system_prompt.md` | 系统提示词 |
| `configs/config.yaml` | 模型/运行配置 |
| `examples/run_sample.py` | 端到端示例 |
| `tests/test_data_tools.py` | 工具单测 |

## 注意

- `agent/loop.py` 的 `MODEL` 常量需随模型版本更新(写过 `claude-opus-4-7` 之类的旧 ID)。
- 扩展工具 = 在 registry `_TOOLS` 加 (schema, fn) 对;设计笔记见 `docs/design.md`
  (Next: group_agg / fit_baseline / profile_report / DuckDB / BigQuery 源)。
- `tools/` 下的独立分析脚本(sa_fg_* / fg_month_* / build_*_report / redshift)不在
  registry 内,是直接运行的流水线,见各自 workflow 文档。
