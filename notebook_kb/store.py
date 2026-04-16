from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS notebooks (
  id INTEGER PRIMARY KEY,
  path TEXT UNIQUE NOT NULL,
  rel_path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size_bytes INTEGER,
  n_code_cells INTEGER,
  n_md_cells INTEGER,
  loc INTEGER,
  imports_json TEXT,
  signals_json TEXT,
  parsed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notebooks_sha ON notebooks(sha256);

CREATE TABLE IF NOT EXISTS summaries (
  notebook_id INTEGER PRIMARY KEY REFERENCES notebooks(id) ON DELETE CASCADE,
  purpose TEXT,
  inputs_json TEXT,
  outputs_json TEXT,
  key_steps_json TEXT,
  data_sources_json TEXT,
  domain_tags_json TEXT,
  notable_functions_json TEXT,
  confidence TEXT,
  model TEXT,
  tokens_in INTEGER,
  tokens_out INTEGER,
  raw_json TEXT,
  based_on_sha TEXT NOT NULL,
  summarized_at TEXT NOT NULL
);
"""


@dataclass
class NotebookRow:
    id: int
    path: str
    rel_path: str
    sha256: str
    n_code_cells: int
    n_md_cells: int
    loc: int
    imports: list[str]
    signals: dict


@contextmanager
def connect(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


def get_sha_for_path(conn: sqlite3.Connection, path: str) -> str | None:
    row = conn.execute(
        "SELECT sha256 FROM notebooks WHERE path = ?", (path,)
    ).fetchone()
    return row["sha256"] if row else None


def upsert_notebook(
    conn: sqlite3.Connection,
    *,
    path: str,
    rel_path: str,
    sha256: str,
    size_bytes: int,
    n_code_cells: int,
    n_md_cells: int,
    loc: int,
    imports: list[str],
    signals: dict,
) -> int:
    now = _utcnow()
    conn.execute(
        """
        INSERT INTO notebooks (path, rel_path, sha256, size_bytes, n_code_cells,
                               n_md_cells, loc, imports_json, signals_json, parsed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
          rel_path=excluded.rel_path,
          sha256=excluded.sha256,
          size_bytes=excluded.size_bytes,
          n_code_cells=excluded.n_code_cells,
          n_md_cells=excluded.n_md_cells,
          loc=excluded.loc,
          imports_json=excluded.imports_json,
          signals_json=excluded.signals_json,
          parsed_at=excluded.parsed_at
        """,
        (
            path,
            rel_path,
            sha256,
            size_bytes,
            n_code_cells,
            n_md_cells,
            loc,
            json.dumps(imports),
            json.dumps(signals),
            now,
        ),
    )
    row = conn.execute("SELECT id FROM notebooks WHERE path = ?", (path,)).fetchone()
    return row["id"]


def notebooks_needing_summary(conn: sqlite3.Connection, limit: int | None = None):
    # A notebook needs a summary if:
    #   (a) it has no summary, OR
    #   (b) its summary was based on a different sha256 than the current ingest.
    sql = """
    SELECT n.id, n.path, n.rel_path, n.sha256
    FROM notebooks n
    LEFT JOIN summaries s ON s.notebook_id = n.id
    WHERE s.notebook_id IS NULL OR s.based_on_sha != n.sha256
    ORDER BY n.rel_path
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in conn.execute(sql).fetchall()]


def get_notebook(
    conn: sqlite3.Connection, *, id: int | None = None, rel_path: str | None = None
) -> NotebookRow | None:
    if id is not None:
        row = conn.execute("SELECT * FROM notebooks WHERE id = ?", (id,)).fetchone()
    elif rel_path is not None:
        row = conn.execute(
            "SELECT * FROM notebooks WHERE rel_path = ?", (rel_path,)
        ).fetchone()
    else:
        raise ValueError("must pass id or rel_path")
    if not row:
        return None
    return NotebookRow(
        id=row["id"],
        path=row["path"],
        rel_path=row["rel_path"],
        sha256=row["sha256"],
        n_code_cells=row["n_code_cells"],
        n_md_cells=row["n_md_cells"],
        loc=row["loc"],
        imports=json.loads(row["imports_json"] or "[]"),
        signals=json.loads(row["signals_json"] or "{}"),
    )


def upsert_summary(
    conn: sqlite3.Connection,
    *,
    notebook_id: int,
    parsed: dict,
    raw_json: str,
    based_on_sha: str,
    model: str,
    tokens_in: int,
    tokens_out: int,
    confidence: str,
) -> None:
    now = _utcnow()
    conn.execute(
        """
        INSERT INTO summaries (notebook_id, purpose, inputs_json, outputs_json,
                               key_steps_json, data_sources_json, domain_tags_json,
                               notable_functions_json, confidence, model,
                               tokens_in, tokens_out, raw_json, based_on_sha, summarized_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(notebook_id) DO UPDATE SET
          purpose=excluded.purpose,
          inputs_json=excluded.inputs_json,
          outputs_json=excluded.outputs_json,
          key_steps_json=excluded.key_steps_json,
          data_sources_json=excluded.data_sources_json,
          domain_tags_json=excluded.domain_tags_json,
          notable_functions_json=excluded.notable_functions_json,
          confidence=excluded.confidence,
          model=excluded.model,
          tokens_in=excluded.tokens_in,
          tokens_out=excluded.tokens_out,
          raw_json=excluded.raw_json,
          based_on_sha=excluded.based_on_sha,
          summarized_at=excluded.summarized_at
        """,
        (
            notebook_id,
            parsed.get("purpose"),
            json.dumps(parsed.get("inputs", [])),
            json.dumps(parsed.get("outputs", [])),
            json.dumps(parsed.get("key_steps", [])),
            json.dumps(parsed.get("data_sources", [])),
            json.dumps(parsed.get("domain_tags", [])),
            json.dumps(parsed.get("notable_functions", [])),
            confidence,
            model,
            tokens_in,
            tokens_out,
            raw_json,
            based_on_sha,
            now,
        ),
    )


def get_summary(conn: sqlite3.Connection, notebook_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM summaries WHERE notebook_id = ?", (notebook_id,)
    ).fetchone()
    if not row:
        return None
    return {
        "purpose": row["purpose"],
        "inputs": json.loads(row["inputs_json"] or "[]"),
        "outputs": json.loads(row["outputs_json"] or "[]"),
        "key_steps": json.loads(row["key_steps_json"] or "[]"),
        "data_sources": json.loads(row["data_sources_json"] or "[]"),
        "domain_tags": json.loads(row["domain_tags_json"] or "[]"),
        "notable_functions": json.loads(row["notable_functions_json"] or "[]"),
        "confidence": row["confidence"],
        "model": row["model"],
        "tokens_in": row["tokens_in"],
        "tokens_out": row["tokens_out"],
        "summarized_at": row["summarized_at"],
    }


def counts(conn: sqlite3.Connection) -> dict:
    n = conn.execute("SELECT COUNT(*) c FROM notebooks").fetchone()["c"]
    s = conn.execute("SELECT COUNT(*) c FROM summaries").fetchone()["c"]
    return {"notebooks": n, "summaries": s}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
