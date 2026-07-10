"""Daily summary report for SS03 (public.fct_bet_orders) over local parquet.

Metrics (per day):
  overall:   n_spins, n_users, sum_bet, sum_payout, rtp, avg_bet, avg_payout
  base:      n, sum_bet, sum_payout, rtp, hit_rate, trigger_rate
  free:      n, sum_payout, rtp_contrib (=free_payout/base_bet), hit_rate

Then the same metrics broken down by math_table_id.

Convention notes:
  - FREE spins have bet_amount=0. "free RTP" is reported as
    contribution = SUM(free_payout) / SUM(base_bet)  (industry standard).
  - trigger rate = base spins with non-empty trigger_type / all base spins.
    Includes DIRECT_PURCHASED (feature buy). Exclude those separately if needed.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def summary_sql(parquet_glob: str, groupby: str | None = None, game_id: str = "SS03") -> str:
    # trigger rate defined via join: BASE rows whose spin_id is the root_spin_id of some FREE row
    group_col = f"b.{groupby}" if groupby else "NULL"
    group_clause = f"GROUP BY {group_col}" if groupby else ""
    order_clause = "ORDER BY sum_bet DESC" if groupby else ""
    select_group = f"{group_col} AS {groupby}," if groupby else ""
    return f"""
    WITH d AS (
      SELECT * FROM read_parquet('{parquet_glob}')
      WHERE game_id = '{game_id}'
        AND status = 'COMPLETED'
        AND currency_type = 'CNY'
        AND op_code NOT IN ('B26','TST','TSB','TSO')
    ),
    free_roots AS (SELECT DISTINCT root_spin_id FROM d WHERE bet_type = 'FREE')
    SELECT
      {select_group}
      COUNT(*)                                            AS n_spins,
      COUNT(DISTINCT b.user_id)                           AS n_users,
      SUM(b.bet_amount)                                   AS sum_bet,
      SUM(b.actual_payout)                                AS sum_payout,
      SUM(b.actual_payout) / NULLIF(SUM(b.bet_amount), 0) AS rtp,
      AVG(b.bet_amount)                                   AS avg_bet,
      AVG(b.actual_payout)                                AS avg_payout,

      SUM(CASE WHEN b.bet_type='BASE' THEN 1 ELSE 0 END)                            AS base_n,
      SUM(CASE WHEN b.bet_type='BASE' THEN b.bet_amount ELSE 0 END)                 AS base_bet,
      SUM(CASE WHEN b.bet_type='BASE' THEN b.actual_payout ELSE 0 END)              AS base_payout,
      SUM(CASE WHEN b.bet_type='BASE' THEN b.actual_payout ELSE 0 END)
        / NULLIF(SUM(CASE WHEN b.bet_type='BASE' THEN b.bet_amount ELSE 0 END), 0)  AS base_rtp,
      SUM(CASE WHEN b.bet_type='BASE' AND b.actual_payout > 0 THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN b.bet_type='BASE' THEN 1 ELSE 0 END), 0)             AS base_hit_rate,
      SUM(CASE WHEN b.bet_type='BASE' AND f.root_spin_id IS NOT NULL
                THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN b.bet_type='BASE' THEN 1 ELSE 0 END), 0)             AS base_trigger_rate,

      SUM(CASE WHEN b.bet_type='FREE' THEN 1 ELSE 0 END)                            AS free_n,
      SUM(CASE WHEN b.bet_type='FREE' THEN b.actual_payout ELSE 0 END)              AS free_payout,
      SUM(CASE WHEN b.bet_type='FREE' THEN b.actual_payout ELSE 0 END)
        / NULLIF(SUM(CASE WHEN b.bet_type='BASE' THEN b.bet_amount ELSE 0 END), 0)  AS free_rtp_contrib,
      SUM(CASE WHEN b.bet_type='FREE' AND b.actual_payout > 0 THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN b.bet_type='FREE' THEN 1 ELSE 0 END), 0)             AS free_hit_rate
    FROM d b
    LEFT JOIN free_roots f ON b.spin_id = f.root_spin_id AND b.bet_type='BASE'
    {group_clause}
    {order_clause}
    """


def fmt_overall(row: pd.Series) -> str:
    lines = [
        "=== OVERALL ===",
        f"  spins           {int(row.n_spins):>14,d}",
        f"  users           {int(row.n_users):>14,d}",
        f"  sum_bet         {row.sum_bet:>14,.2f}",
        f"  sum_payout      {row.sum_payout:>14,.2f}",
        f"  RTP             {row.rtp*100:>13,.3f}%",
        f"  avg_bet/spin    {row.avg_bet:>14,.4f}",
        f"  avg_payout/spin {row.avg_payout:>14,.4f}",
        "",
        "=== BASE game ===",
        f"  spins           {int(row.base_n):>14,d}",
        f"  sum_bet         {row.base_bet:>14,.2f}",
        f"  sum_payout      {row.base_payout:>14,.2f}",
        f"  RTP             {row.base_rtp*100:>13,.3f}%",
        f"  hit_rate        {row.base_hit_rate*100:>13,.3f}%   (payout>0 / all base)",
        f"  trigger_rate    {row.base_trigger_rate*100:>13,.3f}%   (base whose spin_id = some FREE root_spin_id / all base)",
        "",
        "=== FREE game ===",
        f"  spins           {int(row.free_n):>14,d}",
        f"  sum_payout      {row.free_payout:>14,.2f}",
        f"  RTP (contrib)   {row.free_rtp_contrib*100:>13,.3f}%   (free_payout / base_bet)",
        f"  hit_rate        {row.free_hit_rate*100:>13,.3f}%   (payout>0 / all free)",
    ]
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default="2026-06-11")
    p.add_argument("--game-id", default="SS03")
    p.add_argument("--out", default=None, help="optional CSV path for per-math_table table")
    args = p.parse_args()

    parquet_glob = str(ROOT / f"data/ss03/dt={args.date}/*.parquet")
    con = duckdb.connect()

    print(f"date: {args.date}  game_id: {args.game_id}\nsource: {parquet_glob}\n")

    overall = con.execute(summary_sql(parquet_glob, game_id=args.game_id)).df().iloc[0]
    print(fmt_overall(overall))

    by_mt = con.execute(summary_sql(parquet_glob, groupby="math_table_id", game_id=args.game_id)).df()
    # tidy numeric formatting for display
    show_cols = [
        "math_table_id", "n_spins", "n_users", "sum_bet", "sum_payout", "rtp",
        "avg_bet", "avg_payout",
        "base_n", "base_bet", "base_payout", "base_rtp", "base_hit_rate", "base_trigger_rate",
        "free_n", "free_payout", "free_rtp_contrib", "free_hit_rate",
    ]
    disp = by_mt[show_cols].copy()
    for c in ("rtp", "base_rtp", "base_hit_rate", "base_trigger_rate", "free_rtp_contrib", "free_hit_rate"):
        disp[c] = (disp[c] * 100).round(3)
    for c in ("sum_bet", "sum_payout", "base_bet", "base_payout", "free_payout"):
        disp[c] = disp[c].round(2)
    for c in ("avg_bet", "avg_payout"):
        disp[c] = disp[c].round(4)

    print("\n=== BY math_table_id (sorted by sum_bet desc) ===")
    with pd.option_context("display.max_rows", None, "display.max_columns", None,
                           "display.width", 220, "display.expand_frame_repr", False):
        print(disp.to_string(index=False))

    if args.out:
        by_mt.to_csv(args.out, index=False)
        print(f"\nwrote csv: {args.out}")


if __name__ == "__main__":
    main()
