"""Data loading / IO tools."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.state import FRAMES


def load_dataframe(path: str, name: str) -> str:
    p = Path(path)
    if not p.exists():
        return f"error: file not found: {path}"

    suffix = p.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(p)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(p)
    elif suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(p)
    elif suffix == ".json":
        df = pd.read_json(p)
    else:
        return f"error: unsupported file type: {suffix}"

    FRAMES[name] = df
    dtypes = {c: str(t) for c, t in df.dtypes.items()}
    return f"loaded '{name}' shape={df.shape} dtypes={dtypes}"
