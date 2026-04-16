# AI_DS

An AI agent for data science tasks — basic analysis, data processing, and exploration powered by Claude.

## Goals

- Load and clean tabular datasets (CSV, Parquet, Excel, SQL)
- Run exploratory data analysis (summary stats, distributions, correlations, missingness)
- Produce visualizations (histograms, scatter, time series, heatmaps)
- Feature engineering helpers (encoding, scaling, date parts, binning)
- Lightweight modeling (baseline regression/classification, CV, metrics)
- Natural-language interface: user describes the task, agent picks and runs the right tool

## Structure

```
agent/      # Agent core: loop, planning, tool dispatch
tools/      # Callable DS tools (data loading, EDA, viz, modeling)
prompts/    # System prompts and task templates
configs/    # Model / runtime config
data/       # Sample / working datasets (gitignored except samples)
notebooks/  # Jupyter exploration
tests/      # Unit tests for tools
examples/   # End-to-end usage scripts
docs/       # Design notes
```

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=...
python -m agent.main "Summarize data/sample.csv"
```

## Status

Scaffolding only — agent loop and tools are stubs.
