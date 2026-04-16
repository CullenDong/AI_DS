from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from . import store
from .parse import parse_notebook
from .static import extract_imports, extract_signals


@dataclass
class IngestReport:
    scanned: int = 0
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    parse_errors: int = 0

    def __str__(self) -> str:
        return (
            f"scanned={self.scanned} new={self.new} updated={self.updated} "
            f"unchanged={self.unchanged} parse_errors={self.parse_errors}"
        )


def ingest(root: Path, db_path: Path) -> IngestReport:
    root = root.resolve()
    if not root.exists():
        raise FileNotFoundError(f"notebooks root not found: {root}")

    store.init_db(db_path)
    report = IngestReport()

    files = [p for p in root.rglob("*.ipynb") if ".ipynb_checkpoints" not in p.parts]
    report.scanned = len(files)

    with store.connect(db_path) as conn:
        for path in sorted(files):
            path = path.resolve()
            rel = str(path.relative_to(root))
            raw = path.read_bytes()
            sha = hashlib.sha256(raw).hexdigest()

            existing_sha = store.get_sha_for_path(conn, str(path))
            if existing_sha == sha:
                report.unchanged += 1
                continue

            try:
                parsed = parse_notebook(path)
            except Exception:
                report.parse_errors += 1
                continue

            code_cells = [c.source for c in parsed.cells if c.cell_type == "code"]
            imports = extract_imports(parsed.code_text)
            signals = extract_signals(code_cells).to_dict()

            store.upsert_notebook(
                conn,
                path=str(path),
                rel_path=rel,
                sha256=sha,
                size_bytes=len(raw),
                n_code_cells=parsed.n_code,
                n_md_cells=parsed.n_md,
                loc=parsed.loc(),
                imports=imports,
                signals=signals,
            )
            if existing_sha is None:
                report.new += 1
            else:
                report.updated += 1

    return report
