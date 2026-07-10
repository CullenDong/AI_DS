"""Timeline of which math_table_id(s) each strategy (partition_ab) used, day by day.

Produces two outputs:
  data/ss03_strategy_math_daily.csv    — one row per (strategy, dt, math_table_id) with bet share
  data/ss03_strategy_math_timeline.csv — collapsed contiguous ranges per (strategy, math_table_set)

For each (strategy, dt), math_tables with >= threshold share of that day's bet are considered
"active". A run of consecutive days with the same active-set is compressed to one range row.
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SS03_DIR = ROOT / "data" / "ss03"


def dates_in(lo: str, hi_exclusive: str) -> list[str]:
    d = date.fromisoformat(lo)
    stop = date.fromisoformat(hi_exclusive)
    out = []
    while d < stop:
        p = SS03_DIR / f"dt={d.isoformat()}"
        if p.exists():
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def globs_for(days: list[str]) -> str:
    paths = [str(SS03_DIR / f"dt={d}" / "*.parquet") for d in days]
    quoted = ", ".join(f"'{p}'" for p in paths)
    return f"read_parquet([{quoted}])"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--game-id", default="SS03")
    p.add_argument("--start", default="2026-05-01")
    p.add_argument("--end", default="2026-07-08")
    p.add_argument("--min-share", type=float, default=0.05,
                   help="a math_table is 'active' for a day if its bet share within the strategy >= this (default 5%)")
    args = p.parse_args()

    days = dates_in(args.start, args.end)
    paths = globs_for(days)
    print(f"scanning {len(days)} days: {days[0]}..{days[-1]}")

    con = duckdb.connect()
    daily = con.execute(f"""
    WITH d AS (
      SELECT dt::VARCHAR AS dt,
             json_extract_string(partition_ab, '$[0]') AS strategy,
             math_table_id,
             bet_amount
      FROM {paths} WHERE game_id = '{args.game_id}'
        AND status='COMPLETED' AND currency_type='CNY'
        AND op_code NOT IN ('B26','TST','TSB','TSO')
    ),
    per_cell AS (
      SELECT strategy, dt, math_table_id, SUM(bet_amount) AS cell_bet
      FROM d GROUP BY 1,2,3
    ),
    per_day AS (
      SELECT strategy, dt, SUM(cell_bet) AS day_bet
      FROM per_cell GROUP BY 1,2
    )
    SELECT c.strategy, c.dt, c.math_table_id,
           c.cell_bet, p.day_bet,
           c.cell_bet / NULLIF(p.day_bet, 0) AS share
    FROM per_cell c JOIN per_day p ON c.strategy=p.strategy AND c.dt=p.dt
    ORDER BY c.strategy, c.dt, share DESC
    """).df()

    daily_out = ROOT / "data" / "ss03_strategy_math_daily.csv"
    daily["share"] = daily["share"].round(4)
    daily["cell_bet"] = daily["cell_bet"].round(2)
    daily["day_bet"] = daily["day_bet"].round(2)
    daily.to_csv(daily_out, index=False)
    print(f"wrote {daily_out}  ({len(daily)} rows)")

    # collapse: for each (strategy, dt) compute the set of "active" math_tables (share >= min-share)
    active = daily[daily["share"] >= args.min_share].copy()
    active_sets = (
        active.sort_values(["strategy", "dt", "share"], ascending=[True, True, False])
              .groupby(["strategy", "dt"])
              .agg(active_tables=("math_table_id", lambda s: "+".join(sorted(s))),
                   n_tables=("math_table_id", "count"),
                   day_bet=("day_bet", "first"))
              .reset_index()
    )

    # runs of consecutive days with the same active_tables set (per strategy)
    active_sets = active_sets.sort_values(["strategy", "dt"]).reset_index(drop=True)
    active_sets["prev_set"] = active_sets.groupby("strategy")["active_tables"].shift(1)
    active_sets["change"] = (active_sets["active_tables"] != active_sets["prev_set"]).astype(int)
    active_sets["run_id"] = active_sets.groupby("strategy")["change"].cumsum()

    runs = (
        active_sets.groupby(["strategy", "run_id"])
                   .agg(dt_start=("dt", "min"),
                        dt_end=("dt", "max"),
                        n_days=("dt", "count"),
                        active_tables=("active_tables", "first"),
                        avg_daily_bet=("day_bet", "mean"))
                   .reset_index()
                   .drop(columns="run_id")
    )
    runs["avg_daily_bet"] = runs["avg_daily_bet"].round(0)
    runs = runs.sort_values(["strategy", "dt_start"]).reset_index(drop=True)

    tl_out = ROOT / "data" / "ss03_strategy_math_timeline.csv"
    runs.to_csv(tl_out, index=False)
    print(f"wrote {tl_out}  ({len(runs)} ranges)")

    with pd.option_context("display.max_rows", None, "display.max_columns", None,
                           "display.width", 220, "display.expand_frame_repr", False):
        print("\n=== TIMELINE (active math_tables per strategy, min_share={:.0%}) ===".format(args.min_share))
        print(runs.to_string(index=False))


if __name__ == "__main__":
    main()
