"""Online scoring for the IO-HMM churn model -- standalone (numpy / pandas only, no hmmlearn or training code).

Loads the artifact written by io_hmm.fit_iohmm and scores a player's bet-day history with a FILTERED
(forward-only, no future) recursion, so it can run live / incrementally. Returns, per (user, bet-day):
stage (behavioral state), p_stop (one-step churn hazard) and risk tier.

k=1 (a player's first bet-day) has no history -- the IO-HMM does not score it; such users are flagged
risk="first_day" (handled by first_day_model). All normalization constants come from the artifact and are
never re-fit at serve time (no train/serve skew). Filtered != the offline smoothed labels, so online labels
differ slightly from <csv>_iohmm_labels.csv; filtered is the correct online quantity.
"""
import json

import numpy as np
import pandas as pd

_ARRAY_KEYS = ["feat_mean", "feat_scale", "u_mean", "u_std", "mu", "var", "W", "pi"]


def load_model(path):
    """Load the JSON artifact and turn its numeric fields into numpy arrays."""
    with open(path) as fh:
        m = json.load(fh)
    for k in _ARRAY_KEYS:
        m[k] = np.asarray(m[k], float)
    return m


def prepare(df, model):
    """Player bet-day rows -> (rows[k>=2], Xs scaled behavior, U inputs, lengths, first_day_users).
    Replicates training feature prep with the artifact's saved constants (no re-fit). Raises on a missing
    column or a NaN in a k>=2 feature row (a history feature must never be NaN there)."""
    feat = model["features"]
    missing = [c for c in ["user_id", "bet_date", "bet_amount_today", *feat] if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    df = df.sort_values(["user_id", "bet_date"], kind="stable").reset_index(drop=True)
    df["k"] = df.groupby("user_id").cumcount() + 1
    df["cum_bet"] = df.groupby("user_id")["bet_amount_today"].cumsum()
    first_day_users = df.loc[df.groupby("user_id")["k"].transform("max") == 1, "user_id"].unique()

    rows = df[df["k"] >= 2].copy()
    if rows.empty:
        return rows, np.empty((0, len(feat))), np.empty((0, 3)), [], first_day_users
    X = rows[feat].to_numpy(float)
    if np.isnan(X).any():
        bad = [feat[i] for i in np.unique(np.where(np.isnan(X))[1])]
        raise ValueError(f"NaN in k>=2 feature rows: {bad}")
    li = [feat.index(f) for f in model["log1p_features"]]
    X[:, li] = np.log1p(X[:, li])
    Xs = (X - model["feat_mean"]) / model["feat_scale"]
    logk, logcb = np.log1p(rows["k"].to_numpy(float)), np.log1p(rows["cum_bet"].to_numpy(float))
    uz = (np.column_stack([logk, logcb]) - model["u_mean"]) / model["u_std"]
    U = np.column_stack([np.ones(len(rows)), uz])
    lengths = rows.groupby("user_id", sort=False).size().tolist()
    return rows, Xs, U, lengths, first_day_users


def _tier(p_stop, model):
    """Risk tier from p_stop. `>` semantics match the offline pd.cut(right=True) so online == offline."""
    if p_stop > model["high_cut"]:
        return "high"
    return "med" if p_stop > model["med_cut"] else "low"


def _emission(Xs, mu, var):
    """Transient-state likelihood per row (n, nb); per-row max subtracted (cancels in filtering normalization)."""
    logb = np.empty((len(Xs), len(mu)))
    for j in range(len(mu)):
        d = Xs - mu[j]
        logb[:, j] = -0.5 * (np.log(2 * np.pi * var[j]).sum() + (d * d / var[j]).sum(1))
    logb -= logb.max(1, keepdims=True)
    return np.exp(logb)


def _transition(u, W):
    """A(i,j) = softmax_j(W[i] . u) for one input vector u; full K x K including STOP."""
    logits = W @ u
    logits -= logits.max(1, keepdims=True)
    A = np.exp(logits)
    return A / A.sum(1, keepdims=True)


def infer(Xs, U, lengths, model):
    """Filtered forward over the transient states, per sequence. Returns (stage, p_stop) per row:
    stage = MAP transient state; p_stop = P(next = STOP | stage, that row's input) = A[stage, STOP]."""
    mu, var, W, pi = model["mu"], model["var"], model["W"], model["pi"]
    nb, stop = len(pi) - 1, model["stop_state"]
    B = _emission(Xs, mu, var)
    pi_t = pi[:nb] / pi[:nb].sum()
    stage = np.empty(len(Xs), int)
    p_stop = np.empty(len(Xs))
    pos = 0
    for L in lengths:
        a = None
        for t in range(L):
            i = pos + t
            A = _transition(U[i], W)
            a = pi_t * B[i] if t == 0 else (a @ A[:nb, :nb]) * B[i]
            a = a / a.sum()
            s = int(a.argmax())
            stage[i], p_stop[i] = s, A[s, stop]
        pos += L
    return stage, p_stop


def score(df, model):
    """Per (user, bet-day) churn label: stage / p_stop / risk for k>=2 rows, plus one risk='first_day'
    row per first-bet-day-only user (not scored by the IO-HMM)."""
    rows, Xs, U, lengths, first_users = prepare(df, model)
    parts = []
    if len(rows):
        stage, p_stop = infer(Xs, U, lengths, model)
        r = rows[["user_id", "bet_date", "k"]].copy()
        r["stage"], r["p_stop"] = stage, p_stop
        r["risk"] = [_tier(p, model) for p in p_stop]
        parts.append(r)
    if len(first_users):
        fd = df[df["user_id"].isin(first_users)][["user_id", "bet_date"]].copy()
        fd["k"], fd["stage"], fd["p_stop"], fd["risk"] = 1, -1, np.nan, "first_day"
        parts.append(fd)
    cols = ["user_id", "bet_date", "k", "stage", "p_stop", "risk"]
    res = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=cols)
    return res[cols].sort_values(["user_id", "bet_date"]).reset_index(drop=True)


def current_label(df, model):
    """Each user's CURRENT churn label = their latest bet-day row."""
    return score(df, model).sort_values("bet_date").groupby("user_id").tail(1).reset_index(drop=True)


# --------------------------------------------------------------------------- stateful online API
#
# For live serving: keep one small state per user (forward message + tenure + cumulative bet) and update it
# O(1) per new bet-day, instead of re-scoring the whole history each time. Produces the same labels as the
# batch path (score). On a player's first bet-day (k=1) the label is "first_day"; the IO-HMM starts at k=2.

def init_state():
    """Empty per-user state. Persist this between a player's bet-days."""
    return {"k": 0, "cum_bet": 0.0, "alpha": None}


def update(state, row, model):
    """Advance one bet-day. `state` is from init_state() (or a prior update); `row` is that bet-day's features
    (dict/Series) with the model's `features` + `bet_amount_today`. Returns (new_state, label) where label is
    {k, stage, p_stop, risk}. Raises if a feature is missing or NaN. k=1 -> risk='first_day' (not IO-HMM scored)."""
    k = state["k"] + 1
    cum_bet = state["cum_bet"] + float(row["bet_amount_today"])
    if k == 1:                                                        # first bet-day: no history, not IO-HMM scored
        return {"k": k, "cum_bet": cum_bet, "alpha": None}, {"k": 1, "stage": -1, "p_stop": float("nan"), "risk": "first_day"}

    feat = model["features"]
    try:
        x = np.array([float(row[f]) for f in feat])
    except KeyError as e:
        raise ValueError(f"missing feature {e} in row")
    if np.isnan(x).any():
        raise ValueError(f"NaN in features {[feat[i] for i in np.where(np.isnan(x))[0]]}")
    li = [feat.index(f) for f in model["log1p_features"]]
    x[li] = np.log1p(x[li])
    xs = (x - model["feat_mean"]) / model["feat_scale"]
    u = np.array([1.0,
                  (np.log1p(k) - model["u_mean"][0]) / model["u_std"][0],
                  (np.log1p(cum_bet) - model["u_mean"][1]) / model["u_std"][1]])

    nb, stop = len(model["pi"]) - 1, model["stop_state"]
    b = _emission(xs[None, :], model["mu"], model["var"])[0]
    A = _transition(u, model["W"])
    if state["alpha"] is None:                                       # first IO-HMM day (k==2): forward init
        pi_t = model["pi"][:nb] / model["pi"][:nb].sum()
        a = pi_t * b
    else:
        a = (state["alpha"] @ A[:nb, :nb]) * b
    a = a / a.sum()
    s = int(a.argmax())
    p_stop = float(A[s, stop])
    return {"k": k, "cum_bet": cum_bet, "alpha": a}, {"k": k, "stage": s, "p_stop": p_stop, "risk": _tier(p_stop, model)}
