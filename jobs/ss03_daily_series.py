"""Per-day SS03 summary series + before/after comparison across an update boundary.

Emits:
  data/ss03_daily_summary.csv       one row per day, all metrics
  data/ss03_before_after_diff.csv   metric-level delta across cut date

Metrics per day (same as jobs/ss03_daily_report.py OVERALL block):
  n_spins, n_users, sum_bet, sum_payout, rtp, avg_bet, avg_payout,
  base_n, base_bet, base_payout, base_rtp, base_hit_rate, base_trigger_rate,
  free_n, free_payout, free_rtp_contrib, free_hit_rate
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SS03_DIR = ROOT / "data" / "ss03"

RATE_COLS = [
    "rtp", "base_rtp", "base_hit_rate", "base_trigger_rate",
    "free_rtp_contrib", "free_hit_rate",
]
METRIC_COLS = [
    "n_spins", "n_users", "sum_bet", "sum_payout", "rtp", "avg_bet", "avg_payout",
    "base_n", "base_bet", "base_payout", "base_rtp", "base_hit_rate", "base_trigger_rate",
    "free_n", "free_payout", "free_rtp_contrib", "free_hit_rate",
]


def daily_sql(parquet_glob: str, game_id: str = "SS03") -> str:
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
    """


def build_series(game_id: str) -> pd.DataFrame:
    con = duckdb.connect()
    days = sorted(p.name.removeprefix("dt=") for p in SS03_DIR.iterdir()
                  if p.is_dir() and p.name.startswith("dt="))
    rows = []
    for day in days:
        glob = str(SS03_DIR / f"dt={day}" / "*.parquet")
        r = con.execute(daily_sql(glob, game_id=game_id)).df().iloc[0].to_dict()
        r["dt"] = day
        rows.append(r)
        print(f"  {day}  spins={int(r['n_spins']):>8,d}  users={int(r['n_users']):>5,d}  "
              f"rtp={(r['rtp'] or 0)*100:6.3f}%  trig={(r['base_trigger_rate'] or 0)*100:5.3f}%")
    df = pd.DataFrame(rows)[["dt"] + METRIC_COLS]
    return df


def _agg_period(df: pd.DataFrame) -> dict:
    """Aggregate a set of days back into raw totals and true ratios (not mean-of-ratios)."""
    # totals we can just sum straight from daily rows
    n_spins = df["n_spins"].sum()
    sum_bet = df["sum_bet"].sum()
    sum_payout = df["sum_payout"].sum()
    base_n = df["base_n"].sum()
    base_bet = df["base_bet"].sum()
    base_payout = df["base_payout"].sum()
    free_n = df["free_n"].sum()
    free_payout = df["free_payout"].sum()

    # n_users cannot be exactly summed across days (a user can play multiple days) — but the
    # user asked for aggregate metrics; report total distinct-user-days as a proxy. Also expose
    # mean-per-day n_users so it's not confusing.
    users_daydays = df["n_users"].sum()

    # rate/derived metrics rebuilt from totals
    base_hit_num = (df["base_n"] * df["base_hit_rate"]).sum()          # count of base hits
    free_hit_num = (df["free_n"] * df["free_hit_rate"]).sum()          # count of free hits
    base_trig_num = (df["base_n"] * df["base_trigger_rate"]).sum()     # count of base triggers

    return {
        "n_spins": n_spins,
        "n_users_day_sum": users_daydays,
        "sum_bet": sum_bet,
        "sum_payout": sum_payout,
        "rtp": sum_payout / sum_bet if sum_bet else float("nan"),
        "avg_bet": sum_bet / n_spins if n_spins else float("nan"),
        "avg_payout": sum_payout / n_spins if n_spins else float("nan"),
        "base_n": base_n,
        "base_bet": base_bet,
        "base_payout": base_payout,
        "base_rtp": base_payout / base_bet if base_bet else float("nan"),
        "base_hit_rate": base_hit_num / base_n if base_n else float("nan"),
        "base_trigger_rate": base_trig_num / base_n if base_n else float("nan"),
        "free_n": free_n,
        "free_payout": free_payout,
        "free_rtp_contrib": free_payout / base_bet if base_bet else float("nan"),
        "free_hit_rate": free_hit_num / free_n if free_n else float("nan"),
        "n_days": len(df),
    }


def compare(daily: pd.DataFrame, cut: str) -> pd.DataFrame:
    before = daily[daily["dt"] < cut]
    after = daily[daily["dt"] >= cut]
    print(f"\nbefore rows={len(before)} ({before['dt'].min()}..{before['dt'].max()})")
    print(f"after  rows={len(after)}  ({after['dt'].min()}..{after['dt'].max()})")

    agg_before = _agg_period(before)
    agg_after = _agg_period(after)

    metrics = [
        "n_spins", "n_users_day_sum", "sum_bet", "sum_payout", "rtp", "avg_bet", "avg_payout",
        "base_n", "base_bet", "base_payout", "base_rtp", "base_hit_rate", "base_trigger_rate",
        "free_n", "free_payout", "free_rtp_contrib", "free_hit_rate",
    ]

    rows = []
    for m in metrics:
        b, a = agg_before[m], agg_after[m]
        delta = a - b
        pct = (delta / b * 100) if b else float("nan")
        rows.append({"metric": m, "before": b, "after": a, "abs_delta": delta, "pct_change": pct})
    out = pd.DataFrame(rows)

    # sort by |pct_change| desc, but keep count/sum metrics at bottom and rates at top
    RATES = {"rtp", "base_rtp", "base_hit_rate", "base_trigger_rate",
             "free_rtp_contrib", "free_hit_rate", "avg_bet", "avg_payout"}
    out["is_rate"] = out["metric"].isin(RATES)
    out = out.sort_values(["is_rate", "pct_change"], key=lambda s: s.abs() if s.name == "pct_change" else s,
                          ascending=[False, False]).drop(columns="is_rate").reset_index(drop=True)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--game-id", default="SS03")
    p.add_argument("--cut", default="2026-06-10",
                   help="first day of 'after' period (inclusive). before = < cut, after >= cut.")
    p.add_argument("--drop-last", action="store_true",
                   help="drop final day (often partial data). Default off.")
    args = p.parse_args()

    print(f"building per-day series for game_id={args.game_id} ...")
    daily = build_series(args.game_id)

    if args.drop_last:
        last = daily["dt"].max()
        print(f"dropping last day {last} (--drop-last set)")
        daily = daily[daily["dt"] < last]

    daily_out = ROOT / "data" / f"ss03_daily_summary.csv"
    daily.to_csv(daily_out, index=False)
    print(f"\nwrote {daily_out}  ({len(daily)} days)")

    diff = compare(daily, args.cut)
    diff_out = ROOT / "data" / f"ss03_before_after_diff.csv"
    diff.to_csv(diff_out, index=False)

    print("\n=== before-vs-after (aggregate ratios, rates on top) ===")
    disp = diff.copy()
    for c in ("before", "after", "abs_delta", "pct_change"):
        disp[c] = disp[c].astype(float)
    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(disp.to_string(index=False, float_format=lambda x: f"{x:,.4f}"))
    print(f"\nwrote {diff_out}")


if __name__ == "__main__":
    main()
