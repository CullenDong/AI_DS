# Notebook 知识库(notebook_kb)

扫描本地 `.ipynb` 建 SQLite 索引(imports / cell 数 / LOC / 数据源信号如 S3 URI、Athena 调用),
并用 LLM 生成摘要,用于沉淀历史 notebook 的经验与数据源线索。

## 文件

| 文件 | 角色 |
|---|---|
| `notebook_kb/cli.py` | CLI 入口(`python -m notebook_kb.cli <子命令>`) |
| `notebook_kb/config.py` | `configs/notebook_kb.yaml` 加载(notebooks_root / db_path / model) |
| `notebook_kb/ingest.py` | 扫描目录 → 解析 → 入库(sha256 去重) |
| `notebook_kb/parse.py` | ipynb 解析(cells / imports / LOC) |
| `notebook_kb/static.py` | 静态信号提取(S3 URI、Athena/SQL 调用等) |
| `notebook_kb/store.py` | SQLite 存取层 |
| `notebook_kb/summarize.py` | 对缺摘要的 notebook 跑 LLM 摘要(prompt 在 `prompts/notebook_summarize.md`) |
| `tests/test_ingest.py` + `tests/fixtures/sample.ipynb` | 单测 |

## 子命令

```bash
python -m notebook_kb.cli ingest --path <dir> [--db <path>]   # 扫描索引
python -m notebook_kb.cli summarize [--limit N] [--model M]   # LLM 摘要(需 API key)
python -m notebook_kb.cli show <id|rel_path>                  # 单个 notebook 摘要 JSON
python -m notebook_kb.cli stats                               # top imports / top 数据源信号
```
