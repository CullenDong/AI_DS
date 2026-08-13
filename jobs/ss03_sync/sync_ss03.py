"""Sync public.fct_bet_orders (SS03) from Redshift to day-partitioned parquet.

Layout: data/ss03/dt=YYYY-MM-DD/part.parquet
Resumable: existing day files are skipped.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.db import redshift as rs  # noqa: E402

DATABASE = "slot-machine"
TABLE = "public.fct_bet_orders"
DATE_COL = "created_at"
OUT_DIR = ROOT / "data" / "ss03"


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def sync_day(be, d: date) -> tuple[int, float, bool]:
    out = OUT_DIR / f"dt={d.isoformat()}" / "part.parquet"
    if out.exists():
        return 0, 0.0, True  # skipped
    out.parent.mkdir(parents=True, exist_ok=True)
    next_d = d + timedelta(days=1)
    sql = (
        f"SELECT * FROM {TABLE} "
        f"WHERE {DATE_COL} >= '{d.isoformat()}' AND {DATE_COL} < '{next_d.isoformat()}'"
    )
    t0 = time.time()
    df = be.query_to_df(sql)
    df.to_parquet(out, index=False, compression="snappy")
    return len(df), time.time() - t0, False


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-05-01")
    p.add_argument("--end", default=date.today().isoformat())
    args = p.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = list(daterange(start, end))
    print(f"syncing {TABLE} {start}..{end} ({len(days)} days) -> {OUT_DIR}")

    be = rs._get_backend(database=DATABASE, bastion_ip=rs.DEFAULT_BASTION_IP)
    total_rows = 0
    total_secs = 0.0
    for i, d in enumerate(days, 1):
        rows, secs, skipped = sync_day(be, d)
        if skipped:
            print(f"[{i}/{len(days)}] {d} skipped (exists)")
            continue
        total_rows += rows
        total_secs += secs
        print(f"[{i}/{len(days)}] {d} rows={rows:>8d} took={secs:6.1f}s")

    print(f"done. new_rows={total_rows} elapsed={total_secs:.1f}s out={OUT_DIR}")


if __name__ == "__main__":
    main()
