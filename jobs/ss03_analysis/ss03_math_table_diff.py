"""Per-math_table before/after comparison across the update boundary.

For each math_table_id, aggregate the raw totals across the 'before' and 'after'
periods (aggregate ratios, not mean-of-daily-ratios) and show the delta.

Output: data/ss03_math_table_before_after.csv
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
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
    """Build a single duckdb read_parquet call with an explicit list."""
    paths = [str(SS03_DIR / f"dt={d}" / "*.parquet") for d in days]
    quoted = ", ".join(f"'{p}'" for p in paths)
    return f"read_parquet([{quoted}])"


def aggregate(paths_expr: str, game_id: str) -> pd.DataFrame:
    con = duckdb.connect()
    # main aggregate (grouped by math_table_id of the row itself)
    sql_main = f"""
    WITH d AS (SELECT * FROM {paths_expr} WHERE game_id = '{game_id}' AND status='COMPLETED' AND currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')),
    free_roots AS (SELECT DISTINCT root_spin_id FROM d WHERE bet_type = 'FREE')
    SELECT
      b.math_table_id                                                             AS math_table_id,
      COUNT(*)                                                                    AS n_spins,
      COUNT(DISTINCT b.user_id)                                                   AS n_users,
      SUM(b.bet_amount)                                                           AS sum_bet,
      SUM(b.actual_payout)                                                        AS sum_payout,
      SUM(CASE WHEN b.bet_type='BASE' THEN 1 ELSE 0 END)                          AS base_n,
      SUM(CASE WHEN b.bet_type='BASE' THEN b.bet_amount ELSE 0 END)               AS base_bet,
      SUM(CASE WHEN b.bet_type='BASE' THEN b.actual_payout ELSE 0 END)            AS base_payout,
      SUM(CASE WHEN b.bet_type='BASE' AND b.actual_payout > 0 THEN 1 ELSE 0 END)  AS base_hits,
      SUM(CASE WHEN b.bet_type='BASE' AND f.root_spin_id IS NOT NULL
                THEN 1 ELSE 0 END)                                                AS base_triggers,
      SUM(CASE WHEN b.bet_type='FREE' THEN 1 ELSE 0 END)                          AS free_n,
      SUM(CASE WHEN b.bet_type='FREE' THEN b.actual_payout ELSE 0 END)            AS free_payout,
      SUM(CASE WHEN b.bet_type='FREE' AND b.actual_payout > 0 THEN 1 ELSE 0 END)  AS free_hits
    FROM d b
    LEFT JOIN free_roots f ON b.spin_id = f.root_spin_id AND b.bet_type='BASE'
    GROUP BY b.math_table_id
    """
    df = con.execute(sql_main).df()

    # per-free-spin RTP: for each FREE spin, look up triggering BASE.bet_amount via root_spin_id.
    # free_rtp_per_spin (of the FREE spin's math_table) = SUM(free.actual_payout) / SUM(triggering base.bet_amount)
    sql_free = f"""
    WITH d AS (SELECT * FROM {paths_expr} WHERE game_id = '{game_id}' AND status='COMPLETED' AND currency_type='CNY' AND op_code NOT IN ('B26','TST','TSB','TSO')),
    free_spins AS (
      SELECT spin_id, root_spin_id, math_table_id, actual_payout
      FROM d WHERE bet_type = 'FREE'
    ),
    base_ref AS (
      SELECT spin_id, bet_amount FROM d WHERE bet_type = 'BASE'
    )
    SELECT
      f.math_table_id                     AS math_table_id,
      SUM(f.actual_payout)                AS free_payout_matched,
      SUM(br.bet_amount)                  AS free_notional_bet,
      COUNT(*)                            AS free_n_matched
    FROM free_spins f
    LEFT JOIN base_ref br ON f.root_spin_id = br.spin_id
    WHERE br.bet_amount IS NOT NULL
    GROUP BY f.math_table_id
    """
    fdf = con.execute(sql_free).df()
    df = df.merge(fdf, on="math_table_id", how="left")

    # derive rates
    df["rtp"] = df["sum_payout"] / df["sum_bet"].where(df["sum_bet"] > 0)
    df["base_rtp"] = df["base_payout"] / df["base_bet"].where(df["base_bet"] > 0)
    df["base_hit_rate"] = df["base_hits"] / df["base_n"].where(df["base_n"] > 0)
    df["base_trigger_rate"] = df["base_triggers"] / df["base_n"].where(df["base_n"] > 0)
    df["free_rtp_contrib"] = df["free_payout"] / df["base_bet"].where(df["base_bet"] > 0)
    df["free_hit_rate"] = df["free_hits"] / df["free_n"].where(df["free_n"] > 0)
    # NEW: per-free-spin RTP (free payout / triggering base bet, summed across matched free spins)
    df["free_rtp_per_spin"] = df["free_payout_matched"] / df["free_notional_bet"].where(df["free_notional_bet"] > 0)
    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--game-id", default="SS03")
    p.add_argument("--cut", default="2026-06-10")
    p.add_argument("--start", default="2026-05-01")
    p.add_argument("--end", default="2026-07-08", help="exclusive upper bound (skips partial last day)")
    args = p.parse_args()

    before_days = dates_in(args.start, args.cut)
    after_days = dates_in(args.cut, args.end)
    print(f"before: {len(before_days)} days  {before_days[0]}..{before_days[-1]}")
    print(f"after:  {len(after_days)} days  {after_days[0]}..{after_days[-1]}")

    before = aggregate(globs_for(before_days), args.game_id).add_prefix("b_")
    after = aggregate(globs_for(after_days), args.game_id).add_prefix("a_")
    before = before.rename(columns={"b_math_table_id": "math_table_id"})
    after = after.rename(columns={"a_math_table_id": "math_table_id"})

    m = before.merge(after, on="math_table_id", how="outer")

    RATE_METRICS = ["rtp", "base_rtp", "base_hit_rate", "base_trigger_rate",
                    "free_rtp_contrib", "free_rtp_per_spin", "free_hit_rate"]
    VOL_METRICS = ["n_spins", "sum_bet", "sum_payout", "base_n", "free_n"]

    for r in RATE_METRICS:
        m[f"d_{r}_pp"] = (m[f"a_{r}"] - m[f"b_{r}"]) * 100   # pp = percentage points
    for v in VOL_METRICS:
        b = m[f"b_{v}"]
        m[f"d_{v}_pct"] = (m[f"a_{v}"] - b) / b.where(b > 0) * 100

    # normalize per-day volume for fair comparison (before=len(before_days), after=len(after_days))
    m["b_bet_per_day"] = m["b_sum_bet"] / len(before_days)
    m["a_bet_per_day"] = m["a_sum_bet"] / len(after_days)
    m["d_bet_per_day_pct"] = (m["a_bet_per_day"] - m["b_bet_per_day"]) / m["b_bet_per_day"].where(m["b_bet_per_day"] > 0) * 100

    # sort by total bet across both periods
    m["total_bet"] = m["b_sum_bet"].fillna(0) + m["a_sum_bet"].fillna(0)
    m = m.sort_values("total_bet", ascending=False).drop(columns="total_bet").reset_index(drop=True)

    out_cols = [
        "math_table_id",
        # volume
        "b_bet_per_day", "a_bet_per_day", "d_bet_per_day_pct",
        # RTP
        "b_rtp", "a_rtp", "d_rtp_pp",
        "b_base_rtp", "a_base_rtp", "d_base_rtp_pp",
        "b_free_rtp_contrib", "a_free_rtp_contrib", "d_free_rtp_contrib_pp",
        "b_free_rtp_per_spin", "a_free_rtp_per_spin", "d_free_rtp_per_spin_pp",
        # hit rate
        "b_base_hit_rate", "a_base_hit_rate", "d_base_hit_rate_pp",
        "b_free_hit_rate", "a_free_hit_rate", "d_free_hit_rate_pp",
        # trigger rate
        "b_base_trigger_rate", "a_base_trigger_rate", "d_base_trigger_rate_pp",
    ]

    disp = m[out_cols].copy()
    # display in %
    for c in disp.columns:
        if c == "math_table_id":
            continue
        if c.startswith("b_") and c not in ("b_bet_per_day",):
            disp[c] = disp[c] * 100
        if c.startswith("a_") and c not in ("a_bet_per_day",):
            disp[c] = disp[c] * 100
    disp = disp.round(3)

    out_path = ROOT / "data" / "ss03_math_table_before_after.csv"
    disp.to_csv(out_path, index=False)
    print(f"\nwrote {out_path}\n")

    with pd.option_context("display.max_columns", None, "display.width", 260, "display.expand_frame_repr", False):
        print(disp.to_string(index=False))


if __name__ == "__main__":
    main()
