from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from . import store
from .config import Config
from .ingest import ingest as run_ingest
from .summarize import summarize as run_summarize


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="notebook_kb")
    parser.add_argument("--config", type=Path, default=None, help="path to notebook_kb.yaml")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="scan a local dir for .ipynb and index them")
    p_ing.add_argument("--path", type=Path, default=None)
    p_ing.add_argument("--db", type=Path, default=None)

    p_sum = sub.add_parser("summarize", help="run LLM summaries for notebooks missing one")
    p_sum.add_argument("--limit", type=int, default=None)
    p_sum.add_argument("--model", type=str, default=None)
    p_sum.add_argument("--db", type=Path, default=None)

    p_show = sub.add_parser("show", help="print summary JSON for one notebook")
    p_show.add_argument("target", help="notebook id or rel_path")
    p_show.add_argument("--db", type=Path, default=None)

    p_stats = sub.add_parser("stats", help="print index stats")
    p_stats.add_argument("--db", type=Path, default=None)

    args = parser.parse_args(argv)
    cfg = Config.load(args.config)

    if args.cmd == "ingest":
        root = args.path or cfg.notebooks_root
        db = args.db or cfg.db_path
        report = run_ingest(root, db)
        print(report)
        return 0

    if args.cmd == "summarize":
        if args.db:
            cfg.db_path = args.db
        report = run_summarize(cfg, limit=args.limit, model=args.model)
        print(report)
        return 0

    if args.cmd == "show":
        db = args.db or cfg.db_path
        with store.connect(db) as conn:
            nb = _resolve_target(conn, args.target)
            if not nb:
                print(f"not found: {args.target}", file=sys.stderr)
                return 1
            summary = store.get_summary(conn, nb.id)
        out = {
            "id": nb.id,
            "rel_path": nb.rel_path,
            "sha256": nb.sha256,
            "n_code_cells": nb.n_code_cells,
            "n_md_cells": nb.n_md_cells,
            "loc": nb.loc,
            "imports": nb.imports,
            "signals": nb.signals,
            "summary": summary,
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "stats":
        db = args.db or cfg.db_path
        with store.connect(db) as conn:
            base = store.counts(conn)
            top_imports = _top_column_json(conn, "imports_json", top=15)
            all_signals = [
                json.loads(r["signals_json"] or "{}")
                for r in conn.execute("SELECT signals_json FROM notebooks")
            ]
            ds_counter: Counter = Counter()
            for s in all_signals:
                for uri in s.get("s3_uris", []):
                    ds_counter[uri] += 1
                for a in s.get("athena_calls", []):
                    ds_counter[a] += 1
        print(json.dumps(base, indent=2))
        print("top_imports:")
        for k, v in top_imports:
            print(f"  {v:4d}  {k}")
        print("top_data_signals:")
        for k, v in ds_counter.most_common(15):
            print(f"  {v:4d}  {k}")
        return 0

    return 2


def _resolve_target(conn, target: str):
    try:
        nid = int(target)
        nb = store.get_notebook(conn, id=nid)
        if nb:
            return nb
    except ValueError:
        pass
    return store.get_notebook(conn, rel_path=target)


def _top_column_json(conn, column: str, top: int):
    c: Counter = Counter()
    for row in conn.execute(f"SELECT {column} FROM notebooks"):
        vals = json.loads(row[column] or "[]")
        for v in vals:
            c[v] += 1
    return c.most_common(top)


if __name__ == "__main__":
    raise SystemExit(main())
