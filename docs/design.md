# Design notes

## Agent loop

Single-turn-per-tool loop:

1. Send `{system, tools, messages}` to Claude.
2. If `stop_reason == "tool_use"`: execute every tool_use block, append results as a user turn, loop.
3. Otherwise: print the final text and exit.

In-memory DataFrames are held in `tools.state.FRAMES`, keyed by a user-chosen `name`, so one run can carry intermediate tables across tool calls without re-loading.

## Tool surface (v0)

| Tool             | Purpose                                   |
|------------------|-------------------------------------------|
| load_dataframe   | CSV / Parquet / Excel / JSON → FRAMES     |
| describe         | dtypes + summary stats + missingness      |
| value_counts     | top-k counts for a column                 |
| correlation      | Pearson on numeric cols                   |
| plot_histogram   | save PNG to outputs/                      |

## Next

- `group_agg`, `filter_rows`, `join_frames`
- `fit_baseline` (sklearn logistic / linear)
- `profile_report` (ydata-profiling) as a one-shot EDA
- SQL source (DuckDB / BigQuery) tools
