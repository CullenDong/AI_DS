"""Data loading / IO tools."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from tools.db import redshift as rs
from tools.agent_tools.state import FRAMES


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


def query_redshift(
    sql: str,
    database: str,
    bastion_ip: Optional[str] = None,
    preview_rows: int = 5,
) -> str:
    """Run a read-only SQL query against Redshift and return a text preview.

    Use ``load_redshift_to_frame`` if you need to keep the result for later tool calls.
    """
    if bastion_ip is None:
        bastion_ip = rs.DEFAULT_BASTION_IP
    backend = rs._get_backend(database=database, bastion_ip=bastion_ip)
    df = backend.query_to_df(sql)
    preview = df.head(preview_rows).to_string(index=False)
    return f"rows={len(df)} cols={list(df.columns)}\n{preview}"


def load_redshift_to_frame(
    sql: str,
    name: str,
    database: str,
    bastion_ip: Optional[str] = None,
) -> str:
    """Run a read-only Redshift query and store the result as a named DataFrame."""
    if bastion_ip is None:
        bastion_ip = rs.DEFAULT_BASTION_IP
    backend = rs._get_backend(database=database, bastion_ip=bastion_ip)
    df = backend.query_to_df(sql)
    FRAMES[name] = df
    dtypes = {c: str(t) for c, t in df.dtypes.items()}
    return f"loaded '{name}' from redshift shape={df.shape} dtypes={dtypes}"
