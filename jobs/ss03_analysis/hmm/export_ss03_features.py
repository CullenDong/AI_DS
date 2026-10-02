"""Build the SS03 slot user-day feature table directly from raw spin parquet.

Port of the FM01 fishing feature export (data/FM01/export_selected_hmm_features.py) to SS03 slots,
computed locally from math_table_convex_bandit/asset/raw_zero_dat instead of a DS Redshift export.
One output row = (user_id, bet_date). No churn label is emitted (window pending longer data); a
no_bet_streak_days gap distribution is written instead as the first look for choosing the window.

Feature families:
  A. Core ported from fishing (money / tempo / gap)
  B. Slot game-play: free-game engagement, big-win/volatility, bet-level dynamics
  C. Raw passthrough for EDA

Decisions baked in (see data_request_SS03.md):
  - bet-level features use `credit` (== bet_amount on BASE, defined on FREE too); money aggregates use
    `bet_amount` (0 on FREE, correct: a free spin costs nothing).
  - FREE payout is folded back onto the paying BASE spin via root_spin_id before per-spin outcomes
    (RTP, loss streak, win multiple).
  - No op_code filter yet (no SS03 test-code list); session-day rule = 9min gap, SS03's own median
    bet-day play duration (see report_ss03_iohmm_full.md Sec 2.5) -- not fishing's 180s (blindly ported,
    never re-derived for this game) and not the 12h used elsewhere for math-table training (that value
    merges real day-boundary crossings for ~17% of the k>=2 domain and isn't motivated for churn tagging).
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_RAW = Path(
    "/Users/yvonneliu/PycharmProjects/SLOT_gai/common-ai-research/"
    "math_table_convex_bandit/asset/raw_zero_dat"
)
SESSION_GAP_SECONDS = 9 * 60
BIG_WIN_THRESHOLDS = [10, 20, 50]

NEEDED_COLUMNS = [
    "user_id", "spin_id", "root_spin_id", "bet_type",
    "bet_amount", "credit", "actual_payout",
    "balance_after_bet", "balance_after_payout",
    "retrigger", "created_at", "op_code", "status", "currency_type", "game_id", "math_table_id",
]

EXCLUDE_MATH_TABLES = ["normal_kakuteiB"]  # guaranteed-bonus regime (RTP ~4.7, 91% free spins); zero + 95kai kept as one baseline
DECIMAL_COLUMNS = ["bet_amount", "credit", "actual_payout", "balance_after_bet", "balance_after_payout"]

FINAL_FEATURES = [
    "no_bet_streak_days",
    "bet_amount_ratio_today_vs_history",
    "avg_bet_one_time_today_log",
    "rtp_7_bet_days",
    "loss_streak_ratio_today",
    "current_balance_max_to_avg_bet_ratio",
    "deposit_amount_ratio_today_vs_history",
    # slot game-play
    "fg_trigger_rate_today",
    "free_spin_ratio_today",
    "retrigger_rate_today",
    "fg_payout_share_today",
    "hit_rate_today",
    "max_win_multiple_today",
    "big_win_count_10x_today",
    "big_win_count_20x_today",
    "big_win_count_50x_today",
    "bet_level_change_ratio_today",
    "bet_level_max_today",
    "bet_level_distinct_today",
]
RAW_PASSTHROUGH = [
    "bet_count_today", "base_spin_count_today", "free_spin_count_today",
    "fg_trigger_count_today", "retrigger_count_today", "hit_count_today",
    "bet_amount_today", "payout_today", "rtp_day",
    "avg_bet_one_time_today", "current_balance_day_max", "deposit_amount_today",
    "max_consecutive_loss_count_today",
]


def load_raw(raw_dirs):
    if isinstance(raw_dirs, (str, Path)):
        raw_dirs = [raw_dirs]
    files = [f for d in raw_dirs for f in sorted(Path(d).glob("*.parquet"))]
    if not files:
        raise FileNotFoundError(f"No parquet files under {raw_dirs}")
    parts = [pd.read_parquet(f, columns=NEEDED_COLUMNS) for f in files]
    df = pd.concat(parts, ignore_index=True)
    n0 = len(df)
    df = df.drop_duplicates("spin_id", ignore_index=True)
    if len(df) < n0:
        print(f"[load] dropped {n0 - len(df):,} duplicate spin_id rows across dirs")
    excl = df["math_table_id"].isin(EXCLUDE_MATH_TABLES)
    if excl.any():
        print(f"[load] excluded {int(excl.sum()):,} rows on {EXCLUDE_MATH_TABLES} (different economy)")
    df = df[~excl].copy()
    for c in DECIMAL_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="raise").astype(float)
    df["created_at"] = pd.to_datetime(df["created_at"])
    if df["retrigger"].isna().any():
        raise ValueError("NaN in retrigger flag; upstream must send a boolean, never null.")
    df["retrigger"] = df["retrigger"].astype(bool)

    # The pull is pre-filtered, so these should already be uniform; drop-and-log any row that is not
    # (an explicit filter with a count, not a silent keep).
    keep = (df["status"] == "COMPLETED") & (df["currency_type"] == "CNY") & (df["game_id"] == "SS03")
    dropped = int((~keep).sum())
    if dropped:
        print(f"[load] dropped {dropped} rows failing status/currency/game filter")
    df = df[keep].copy()

    if not (df.loc[df["bet_type"] == "BASE", "bet_amount"] > 0).all():
        raise ValueError("Found BASE spins with bet_amount <= 0; win_mult division would be unsafe.")
    if not (df.loc[df["bet_type"] == "FREE", "bet_amount"] == 0).all():
        raise ValueError("Found FREE spins with nonzero bet_amount; free-spin assumption broken.")
    print(f"[load] {len(files)} files, {len(df):,} spins, {df['user_id'].nunique():,} users, "
          f"{df['created_at'].min()} .. {df['created_at'].max()}")
    return df


def add_session_day(df):
    df = df.sort_values(["user_id", "created_at", "spin_id"]).reset_index(drop=True)
    prev_ts = df.groupby("user_id")["created_at"].shift()
    gap_s = (df["created_at"] - prev_ts).dt.total_seconds()
    new_session = prev_ts.isna() | (gap_s > SESSION_GAP_SECONDS)
    df["session_seq"] = new_session.groupby(df["user_id"]).cumsum()
    natural_date = df["created_at"].dt.normalize()
    # a session that crosses midnight keeps its opening day, matching the fishing export
    df["bet_date"] = natural_date.groupby([df["user_id"], df["session_seq"]]).transform("min")
    return df


def add_deposit(df):
    # Fishing balance-delta logic: with no external money, balance_after_bet[N] equals
    # balance_after_payout[N-1] - bet_amount[N]. The positive residual is a deposit.
    # First spin per user has no prior balance -> deposit unknown, set 0 (intentional boundary).
    prev_bap = df.groupby("user_id")["balance_after_payout"].shift()
    deposit = df["balance_after_bet"] - (prev_bap - df["bet_amount"])
    deposit = deposit.where(prev_bap.notna(), 0.0)
    df["deposit_pos"] = deposit.clip(lower=0).where(deposit.abs() > 1e-3, 0.0)
    return df


def fold_free_payout(df):
    free = df[df["bet_type"] == "FREE"]
    free_by_root = free.groupby("root_spin_id")["actual_payout"].sum()
    base = df[df["bet_type"] == "BASE"].copy()
    base["folded_payout"] = base["actual_payout"] + base["spin_id"].map(free_by_root).fillna(0.0)
    base["win_mult"] = base["folded_payout"] / base["bet_amount"]
    base["is_loss"] = (base["folded_payout"] < base["bet_amount"]).astype(int)
    return base


def max_loss_run(base):
    # longest run of consecutive losing BASE spins within each (user, bet_date)
    b = base.sort_values(["user_id", "bet_date", "created_at", "spin_id"])
    block = (b["is_loss"] == 0).groupby([b["user_id"], b["bet_date"]]).cumsum()
    loss_rows = b[b["is_loss"] == 1]
    if loss_rows.empty:
        return pd.Series(dtype=float)
    run_len = loss_rows.groupby([loss_rows["user_id"], loss_rows["bet_date"], block[b["is_loss"] == 1]]).size()
    return run_len.groupby(level=[0, 1]).max()


def aggregate_user_day(df, base):
    gkey = ["user_id", "bet_date"]

    # all-spin day aggregates (FREE payouts are their own rows -> summed into payout_today)
    allg = df.groupby(gkey)
    day = pd.DataFrame({
        "bet_count_today": allg.size(),
        "free_spin_count_today": allg["bet_type"].apply(lambda s: (s == "FREE").sum()),
        "payout_today": allg["actual_payout"].sum(),
        "hit_count_today": allg["actual_payout"].apply(lambda s: (s > 0).sum()),
        "retrigger_count_today": allg["retrigger"].sum(),
        "current_balance_day_max": allg["balance_after_payout"].max(),
        "deposit_amount_today": allg["deposit_pos"].sum(),
    })

    # BASE-only day aggregates
    baseg = base.groupby(gkey)
    change_ratio = baseg.apply(
        lambda g: (g.sort_values(["created_at", "spin_id"])["credit"].diff().fillna(0) != 0).sum() / len(g),
        include_groups=False,
    )
    base_day = pd.DataFrame({
        "base_spin_count_today": baseg.size(),
        "bet_amount_today": baseg["bet_amount"].sum(),
        "avg_bet_one_time_today": baseg["bet_amount"].mean(),
        "max_win_multiple_today": baseg["win_mult"].max(),
        "big_win_count_10x_today": baseg["win_mult"].apply(lambda s: (s >= 10).sum()),
        "big_win_count_20x_today": baseg["win_mult"].apply(lambda s: (s >= 20).sum()),
        "big_win_count_50x_today": baseg["win_mult"].apply(lambda s: (s >= 50).sum()),
        "bet_level_max_today": baseg["credit"].max(),
        "bet_level_distinct_today": baseg["credit"].nunique(),
        "bet_level_change_ratio_today": change_ratio,
    })

    # free-game trigger counts + free payout share
    free = df[df["bet_type"] == "FREE"]
    fg_trigger = free.groupby(gkey)["root_spin_id"].nunique().rename("fg_trigger_count_today")
    free_payout = free.groupby(gkey)["actual_payout"].sum().rename("free_payout_today")

    loss = max_loss_run(base).rename("max_consecutive_loss_count_today")

    day = day.join(base_day, how="left").join(fg_trigger, how="left").join(free_payout, how="left").join(loss, how="left")
    day["fg_trigger_count_today"] = day["fg_trigger_count_today"].fillna(0).astype(int)
    day["free_payout_today"] = day["free_payout_today"].fillna(0.0)
    day["max_consecutive_loss_count_today"] = day["max_consecutive_loss_count_today"].fillna(0).astype(int)
    day = day.reset_index()

    # derived rates
    day["rtp_day"] = day["payout_today"] / day["bet_amount_today"]
    day["hit_rate_today"] = day["hit_count_today"] / day["bet_count_today"]
    day["fg_trigger_rate_today"] = day["fg_trigger_count_today"] / day["base_spin_count_today"]
    day["free_spin_ratio_today"] = day["free_spin_count_today"] / day["bet_count_today"]
    # 0 free-game triggers today -> retrigger rate is undefined -> 0 (intentional/definitional, not a missing-value fill)
    day["retrigger_rate_today"] = day["retrigger_count_today"] / day["fg_trigger_count_today"].replace(0, np.nan)
    day["retrigger_rate_today"] = day["retrigger_rate_today"].fillna(0.0)
    # 0 payout today -> free-payout share is undefined -> 0 (intentional/definitional)
    day["fg_payout_share_today"] = (day["free_payout_today"] / day["payout_today"].replace(0, np.nan)).fillna(0.0)
    day["loss_streak_ratio_today"] = day["max_consecutive_loss_count_today"] / day["base_spin_count_today"]
    return day


def add_history_features(day):
    day = day.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    g = day.groupby("user_id")

    prev_bet_date = g["bet_date"].shift()
    day["no_bet_streak_days"] = (day["bet_date"] - prev_bet_date).dt.days - 1

    def hist_mean(col):  # expanding mean over strictly prior bet-days
        return g[col].apply(lambda s: s.shift().expanding().mean()).reset_index(level=0, drop=True)

    hist_bet = hist_mean("bet_amount_today")
    hist_bet_one = hist_mean("avg_bet_one_time_today")
    hist_dep = hist_mean("deposit_amount_today")

    day["bet_amount_ratio_today_vs_history"] = np.where(
        hist_bet > 0, day["bet_amount_today"] / hist_bet, np.nan)
    day["current_balance_max_to_avg_bet_ratio"] = np.where(
        hist_bet_one > 0, day["current_balance_day_max"] / hist_bet_one, np.nan)
    day["deposit_amount_ratio_today_vs_history"] = np.where(
        hist_dep > 0, day["deposit_amount_today"] / hist_dep, np.nan)

    # rolling last-7 bet-days (incl current), folded payout / bet
    roll_pay = g["payout_today"].apply(lambda s: s.rolling(7, min_periods=1).sum()).reset_index(level=0, drop=True)
    roll_bet = g["bet_amount_today"].apply(lambda s: s.rolling(7, min_periods=1).sum()).reset_index(level=0, drop=True)
    day["rtp_7_bet_days"] = np.where(roll_bet > 0, roll_pay / roll_bet, np.nan)

    day["avg_bet_one_time_today_log"] = np.log1p(day["avg_bet_one_time_today"].clip(lower=0))
    return day


def write_gap_distribution(day, out_dir):
    gaps = day["no_bet_streak_days"].dropna()
    summary = gaps.describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    bins = [-0.5, 0.5, 1.5, 2.5, 3.5, 6.5, 13.5, 20.5, np.inf]
    labels = ["0", "1", "2", "3", "4-6", "7-13", "14-20", "21+"]
    hist = pd.cut(gaps, bins=bins, labels=labels).value_counts().sort_index()
    hist_df = hist.rename("count").reset_index().rename(columns={"index": "gap_days"})
    hist_df["frac"] = (hist_df["count"] / hist_df["count"].sum()).round(4)
    hist_df.to_csv(out_dir / "ss03_gap_distribution.csv", index=False)
    print("\n[gap] no_bet_streak_days (days between consecutive bet-days; RIGHT-CENSORED by observation window):")
    print(summary.round(2).to_string())
    print(hist_df.to_string(index=False))
    print("[gap] NOTE: window is short -> long gaps and true churn are undercounted; window TBD on longer data.")


def main():
    ap = argparse.ArgumentParser(description="Build SS03 user-day features from raw spin parquet.")
    ap.add_argument("--raw-dir", nargs="+", default=[str(DEFAULT_RAW)])
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parent))
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_raw(args.raw_dir)
    df = add_session_day(df)
    df = add_deposit(df)
    base = fold_free_payout(df)
    day = aggregate_user_day(df, base)
    day = add_history_features(day)

    ordered = ["user_id", "bet_date"] + FINAL_FEATURES + RAW_PASSTHROUGH
    out = day[ordered]
    out_csv = out_dir / "ss03_user_day_features.csv"
    out.to_csv(out_csv, index=False)
    print(f"\n[out] {len(out):,} user-days, {out['user_id'].nunique():,} users -> {out_csv}")
    print("[out] feature summary:")
    print(out[FINAL_FEATURES].describe().T[["mean", "std", "min", "50%", "max"]].round(3).to_string())

    write_gap_distribution(day, out_dir)


if __name__ == "__main__":
    main()
