"""Exploratory data analysis tools."""
from __future__ import annotations

from tools.state import FRAMES


def _get(name: str):
    if name not in FRAMES:
        raise KeyError(f"no DataFrame named {name!r}; call load_dataframe first")
    return FRAMES[name]


def describe(name: str) -> str:
    df = _get(name)
    desc = df.describe(include="all").to_string()
    missing = df.isna().sum().to_dict()
    return f"describe:\n{desc}\n\nmissing_per_column:\n{missing}"


def value_counts(name: str, column: str, top: int = 20) -> str:
    df = _get(name)
    if column not in df.columns:
        return f"error: column {column!r} not in DataFrame"
    return df[column].value_counts(dropna=False).head(top).to_string()


def correlation(name: str) -> str:
    df = _get(name)
    numeric = df.select_dtypes(include="number")
    if numeric.shape[1] < 2:
        return "error: need at least 2 numeric columns for correlation"
    return numeric.corr(method="pearson").round(3).to_string()
