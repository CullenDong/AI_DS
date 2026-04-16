from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from notebook_kb import store
from notebook_kb.ingest import ingest

FIXTURE = Path(__file__).parent / "fixtures" / "sample.ipynb"


@pytest.fixture
def nb_dir(tmp_path: Path) -> Path:
    d = tmp_path / "notebooks"
    d.mkdir()
    shutil.copy(FIXTURE, d / "sample.ipynb")
    return d


def test_ingest_populates_row(nb_dir: Path, tmp_path: Path):
    db = tmp_path / "kb.sqlite"
    report = ingest(nb_dir, db)
    assert report.scanned == 1
    assert report.new == 1
    assert report.unchanged == 0

    with store.connect(db) as conn:
        rows = list(conn.execute("SELECT * FROM notebooks").fetchall())
    assert len(rows) == 1
    row = rows[0]
    imports = json.loads(row["imports_json"])
    signals = json.loads(row["signals_json"])

    assert "pandas" in imports
    assert "awswrangler" in imports
    assert any("s3://analytics-bucket" in u for u in signals["s3_uris"])
    assert any("analytics.retention" in q for q in signals["sql_snippets"])
    assert "weekly_mean" in signals["fn_defs"]
    assert row["n_code_cells"] == 4
    assert row["n_md_cells"] == 1


def test_ingest_is_idempotent(nb_dir: Path, tmp_path: Path):
    db = tmp_path / "kb.sqlite"
    ingest(nb_dir, db)
    second = ingest(nb_dir, db)
    assert second.scanned == 1
    assert second.new == 0
    assert second.updated == 0
    assert second.unchanged == 1


def test_ingest_detects_changes(nb_dir: Path, tmp_path: Path):
    db = tmp_path / "kb.sqlite"
    ingest(nb_dir, db)

    nb_path = nb_dir / "sample.ipynb"
    content = json.loads(nb_path.read_text())
    content["cells"].append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": ["x = 1\n"],
        }
    )
    nb_path.write_text(json.dumps(content))

    report = ingest(nb_dir, db)
    assert report.updated == 1
    assert report.unchanged == 0
