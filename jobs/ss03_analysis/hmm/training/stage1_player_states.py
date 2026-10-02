"""SS03 stage-1 behavioral emission layer: cluster user-days into player-type states.

This is the emission layer of the planned FM01-style IO-HMM (behavior states emit; tenure/cumulative
bet will drive transitions + an absorbing STOP in stage 2, once longer data lands). GaussianHMM over
within-day INTENT features (bet size / volume / bet-level diversity / deposit) — luck features (rtp,
free-game, hit-rate) deliberately excluded so states are player behavior, not RNG outcome.

Input: ss03_user_day_features.csv (from export_ss03_features.py).
Output: model joblib, per-user-day state labels, and a profile report.
"""

import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler

N_STATES = 4
SEED = 42
MIN_COVAR = 0.01            # floor diag covariance: distinct is integer / deposit is zero-inflated -> prevents variance collapse
CLIP_Q = (0.001, 0.999)
FEATS = ["avg_bet_one_time_today_log", "bet_count_today_log", "bet_level_distinct_today", "deposit_amount_today_log"]
CLIP_COLS = ["avg_bet_one_time_today_log", "bet_count_today_log", "deposit_amount_today_log"]
# names assigned AFTER relabeling states 0..3 by ascending daily spin volume
STATE_NAMES = {0: "casual_nodep", 1: "grinder_small", 2: "depositor_mid", 3: "whale"}
DESC = ["avg_bet_one_time_today", "bet_count_today", "bet_amount_today", "bet_level_distinct_today",
        "deposit_amount_today", "rtp_day", "free_spin_ratio_today", "hit_rate_today", "max_win_multiple_today"]


def build_features(df):
    df = df.sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    df["avg_bet_one_time_today_log"] = np.log1p(df["avg_bet_one_time_today"].clip(lower=0))
    df["bet_count_today_log"] = np.log1p(df["bet_count_today"])
    df["deposit_amount_today_log"] = np.log1p(df["deposit_amount_today"].clip(lower=0))
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)

    df = build_features(pd.read_csv(args.features, parse_dates=["bet_date"]))
    clip_bounds = {c: tuple(map(float, df[c].quantile(list(CLIP_Q)))) for c in CLIP_COLS}
    for c, (lo, hi) in clip_bounds.items():
        df[c] = df[c].clip(lo, hi)
    if df[FEATS].isna().any().any():
        raise ValueError(f"NaN in emission features: {df[FEATS].isna().sum().to_dict()}")

    X = df[FEATS].to_numpy(float)
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)
    lengths = df.groupby("user_id", sort=False).size().to_numpy()

    hmm = GaussianHMM(n_components=N_STATES, covariance_type="diag", min_covar=MIN_COVAR,
                      n_iter=300, random_state=SEED, tol=1e-4).fit(Xs, lengths)
    if not hmm.monitor_.converged:
        raise RuntimeError("HMM did not converge")
    raw = hmm.predict(Xs, lengths)

    order = pd.DataFrame({"raw": raw, "bc": df["bet_count_today"].to_numpy()}).groupby("raw")["bc"].mean().sort_values().index.tolist()
    remap = {int(old): i for i, old in enumerate(order)}
    df["state"] = pd.Series(raw).map(remap).to_numpy()
    df["state_name"] = df["state"].map(STATE_NAMES)

    joblib.dump({"hmm": hmm, "scaler": scaler, "feats": FEATS, "state_remap": remap,
                 "state_names": STATE_NAMES, "clip_bounds": clip_bounds, "min_covar": MIN_COVAR,
                 "seed": SEED, "logL": float(hmm.score(Xs, lengths))}, out / "ss03_stage1_model.joblib")
    df[["user_id", "bet_date", "state", "state_name"] + FEATS].to_csv(out / "ss03_player_states.csv", index=False)

    # profile
    df["nd"] = df.groupby("user_id")["bet_date"].shift(-1)
    df["fwd_gap"] = (df["nd"] - df["bet_date"]).dt.days
    df["is_last"] = df["nd"].isna()
    g = df.groupby(["state", "state_name"])
    prof = g[DESC].mean()
    prof["n_userdays"] = g.size()
    prof["share"] = (g.size() / len(df))
    prof["share_is_last"] = g["is_last"].mean()
    prof["n_users_present"] = g["user_id"].nunique()
    print(f"converged={hmm.monitor_.converged} iters={len(hmm.monitor_.history)} logL={hmm.score(Xs,lengths):,.0f}")
    pd.set_option("display.width", 220); pd.set_option("display.max_columns", 30)
    print(prof.round(3).T.to_string())
    prof.round(4).to_csv(out / "ss03_stage1_profile.csv")
    print(f"\nwrote: ss03_stage1_model.joblib, ss03_player_states.csv, ss03_stage1_profile.csv -> {out}")


if __name__ == "__main__":
    main()
