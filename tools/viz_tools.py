"""Visualization tools — save plots to outputs/ and return the path."""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from tools.state import FRAMES

OUTPUT_DIR = Path("outputs")


def _get(name: str):
    if name not in FRAMES:
        raise KeyError(f"no DataFrame named {name!r}; call load_dataframe first")
    return FRAMES[name]


def plot_histogram(name: str, column: str, bins: int = 30) -> str:
    df = _get(name)
    if column not in df.columns:
        return f"error: column {column!r} not in DataFrame"

    OUTPUT_DIR.mkdir(exist_ok=True)
    out = OUTPUT_DIR / f"{name}__{column}__hist.png"

    fig, ax = plt.subplots(figsize=(7, 4))
    df[column].dropna().plot.hist(bins=bins, ax=ax)
    ax.set_title(f"{name}.{column} histogram")
    ax.set_xlabel(column)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)

    return f"saved {out}"
