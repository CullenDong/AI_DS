"""First-day churn model (separate from the IO-HMM).

74.9% of users never return after their first bet -- the dominant, single-largest churn. These users have
no betting history (the IO-HMM and its history features can't score them), so they are modeled separately
from same-day signals only.

Target: for each user's FIRST bet-day, leave = no bet within H=46 days (the user-clock churn label).
Trained only on observable first-days (first bet >= 46 days before cutoff). Base leave rate ~77%.

Features: same-day signals available on the very first bet-day -- NO history features (gap, ratio-vs-history,
balance-vs-history are all undefined on day 1 and are excluded). Trees use raw values (deployable thresholds).

Models: depth-3 decision tree (interpretable, deployable baseline) and HistGradientBoosting (stronger ceiling).

Run:  python first_day_model.py <csv>
"""
import sys

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text

H = 46
# Same-day features defined on a user's first bet-day. History features (no_bet_streak_days,
# *_ratio_today_vs_history, current_balance_max_to_avg_bet_ratio) are NaN on day 1 and are excluded.
FIRST_DAY_FEATURES = [
    "bet_count_today", "bet_amount_today", "avg_bet_one_time_today", "payout_today", "profit_today",
    "rtp_day", "rtp_7_bet_days", "loss_streak_ratio_today", "max_consecutive_loss_count_today",
    "target_selection_entropy", "low_ratio", "medium_ratio", "high_ratio", "ultra_ratio",
    "current_balance_day_max", "bullet_level_change_count_day", "multiplier_change_count_day",
    "multiplier_change_count_ratio", "bullet_level_change_count_ratio",
]


def prepare_first_day(df, H=H):
    """Each user's first bet-day with the churn label. Returns a frame with FIRST_DAY_FEATURES plus
    `leave` (no 2nd bet within H days) and `obs` (enough look-ahead to evaluate). Raises if a feature
    column is missing or any feature is NaN on a first-day row (would mean a history feature slipped in)."""
    missing = [c for c in ["user_id", "bet_date", *FIRST_DAY_FEATURES] if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    df = df.sort_values(["user_id", "bet_date"], kind="stable")
    cutoff = df["bet_date"].max()
    order = df.groupby("user_id").cumcount()
    first = df[order == 0].copy()
    second_date = df[order == 1].set_index("user_id")["bet_date"]      # the 2nd bet-day, if any
    first["second_date"] = first["user_id"].map(second_date)
    gap_next = (first["second_date"] - first["bet_date"]).dt.days
    first["leave"] = (first["second_date"].isna() | (gap_next > H)).astype(int)
    first["obs"] = first["bet_date"] <= (cutoff - pd.Timedelta(days=H))

    nan_cols = [c for c in FIRST_DAY_FEATURES if first[c].isna().any()]
    if nan_cols:
        raise ValueError(f"NaN in first-day features {nan_cols} -- a history feature slipped in or upstream changed")
    return first


def load(csv_path, H=H):
    return prepare_first_day(pd.read_csv(csv_path, parse_dates=["bet_date"]), H)


def train(data, H=H, seed=42):
    """Fit the depth-3 tree (deployable baseline) and HistGradientBoosting on observable first-days, on a
    held-out split. Reports test AUC, the tree rules, top feature importance, and the predicted-probability
    calibration (predicted P(leave) decile -> actual leave rate). `data` is a csv path or a raw frame."""
    first = prepare_first_day(pd.read_csv(data, parse_dates=["bet_date"]) if isinstance(data, str) else data, H)
    o = first[first["obs"]]
    X, y = o[FIRST_DAY_FEATURES].to_numpy(float), o["leave"].to_numpy()
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.3, random_state=0, stratify=y)
    print(f"observable first-days: {len(o)} | base leave {100*y.mean():.1f}% | train {len(Xtr)} / test {len(Xte)}\n")

    tree = DecisionTreeClassifier(max_depth=3, random_state=seed).fit(Xtr, ytr)
    hgb = HistGradientBoostingClassifier(random_state=seed).fit(Xtr, ytr)
    p_tree = tree.predict_proba(Xte)[:, 1]
    p_hgb = hgb.predict_proba(Xte)[:, 1]
    auc_tree, auc_hgb = roc_auc_score(yte, p_tree), roc_auc_score(yte, p_hgb)
    print(f"test AUC: depth-3 tree {auc_tree:.3f} | HGB {auc_hgb:.3f}")

    print("\n=== top features (depth-3 tree importance) ===")
    print(pd.Series(tree.feature_importances_, index=FIRST_DAY_FEATURES).sort_values(ascending=False).head(8).round(3).to_string())
    print("\n=== depth-3 tree rules (raw thresholds; value=[stay,leave]) ===")
    print(export_text(tree, feature_names=FIRST_DAY_FEATURES))

    print("=== HGB predicted P(leave) decile -> actual leave rate (calibration) ===")
    cal = pd.DataFrame({"p": p_hgb, "leave": yte})
    cal["bin"] = pd.qcut(cal["p"], 10, duplicates="drop")
    g = cal.groupby("bin", observed=True).agg(n=("leave", "size"), p_mean=("p", "mean"), actual_leave=("leave", "mean"))
    print(g.assign(p_mean=g["p_mean"].round(3), actual_leave=(100 * g["actual_leave"]).round(1)).to_string())
    return {"tree": tree, "hgb": hgb, "auc_tree": auc_tree, "auc_hgb": auc_hgb, "base_rate": float(y.mean())}


if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "data/selected_hmm_features_fm01_cny.csv"
    train(csv)
