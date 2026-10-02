"""SS03 stage-2 Input-Output HMM: player lifecycle + absorbing churn (STOP).

Port of the FM01 io_hmm.py to SS03 (math identical; data/feature layer swapped). Markov-valid:
  - Emissions behavior-only, input-independent: N(x; mu_i, Sigma_i) over the 4 stage-1 INTENT features.
  - Transitions input-driven: P(z_t=j | z_{t-1}=i, u_t) = softmax_j(W[i,j] . u_t), u=[1, log tenure, log cum_bet].
  - One absorbing STOP, appended for churned users (trailing silence > H days), observed via emission clamp.

SS03 specifics: drop the window-start (left-censored) cohort, k>=2 only (k=1 = first-day model), H=21.
Emission features log1p+clip'd like stage-1; min_covar/VAR_FLOOR = 0.01 (distinct integer + deposit zero-inflated).

Run: python stage2_iohmm.py --features <ss03_user_day_features.csv> --out-dir <dir> [--n-behavior 4] [--H 21]
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from sklearn.isotonic import IsotonicRegression
from sklearn.preprocessing import StandardScaler

FEATS = ["avg_bet_one_time_today", "bet_count_today", "bet_level_distinct_today", "deposit_amount_today", "no_bet_streak_days"]
LOG_IDX = [0, 1, 3, 4]         # log1p+clip; distinct(2) raw; idx4=no_bet_streak_days (pause/recency signal)
INPUT_COLS = ["log_k", "log_cum_bet"]
VAR_FLOOR = 0.01
MIN_COVAR = 0.01
HIGH_SHARE = 0.45
MED_RATE = 0.30
WINDOW_START = pd.Timestamp("2026-05-21")


def build_emission_raw(d, clip_bounds=None):
    X = np.column_stack([
        np.log1p(d["avg_bet_one_time_today"].to_numpy(float).clip(0)),
        np.log1p(d["bet_count_today"].to_numpy(float).clip(0)),
        d["bet_level_distinct_today"].to_numpy(float),
        np.log1p(d["deposit_amount_today"].to_numpy(float).clip(0)),
        np.log1p(d["no_bet_streak_days"].to_numpy(float).clip(0)),
    ])
    if clip_bounds is None:                                            # fit-time: derive tail-clip bounds (log space)
        clip_bounds = {FEATS[j]: [float(np.quantile(X[:, j], 0.001)), float(np.quantile(X[:, j], 0.999))] for j in LOG_IDX}
    for j in LOG_IDX:
        lo, hi = clip_bounds[FEATS[j]]; X[:, j] = np.clip(X[:, j], lo, hi)
    return X, clip_bounds


def load_ss03(csv_path, H):
    df = pd.read_csv(csv_path, parse_dates=["bet_date"]).sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    cutoff = df["bet_date"].max()
    first = df.groupby("user_id")["bet_date"].transform("min")
    n_lc = df.loc[first == WINDOW_START, "user_id"].nunique()
    df = df[first != WINDOW_START].copy().reset_index(drop=True)     # drop left-censored window-start cohort
    print(f"[load] dropped {n_lc} left-censored (first-seen {WINDOW_START.date()}) users; {df['user_id'].nunique()} remain")
    df["k"] = df.groupby("user_id").cumcount() + 1
    df["cum_bet"] = df.groupby("user_id")["bet_amount_today"].cumsum()
    nd = df.groupby("user_id")["bet_date"].shift(-1)
    gap = (nd - df["bet_date"]).dt.days
    df["leave"] = (nd.isna() | (gap > H)).astype(int)               # user-clock: no return within H days
    df["obs"] = (cutoff - df["bet_date"]).dt.days >= H              # only these have a determinable label
    return df, cutoff


def fit_warm(Xs, lengths, n, seed):
    return GaussianHMM(n_components=n, covariance_type="diag", min_covar=MIN_COVAR,
                       n_iter=300, random_state=seed, tol=1e-4).fit(Xs, lengths)


def _build_data(csv_path, n_behavior, H, seed):
    df, cutoff = load_ss03(csv_path, H)
    d = df[df["k"] >= 2].copy().reset_index(drop=True)
    if d[FEATS].isna().any().any():                                  # a free-only bet-day -> NaN avg_bet/distinct; must not propagate into cum_bet/EM
        raise ValueError(f"NaN in stage2 emission features: {d[FEATS].isna().sum().to_dict()}")
    Xraw, clip_bounds = build_emission_raw(d)
    scaler = StandardScaler().fit(Xraw)
    sb = scaler.transform(Xraw)
    lengths_real = d.groupby("user_id", sort=False).size().tolist()
    m0 = fit_warm(sb, lengths_real, n_behavior, seed)
    mu0 = m0.means_.copy()
    var0 = np.maximum(np.array([np.diag(c) for c in m0.covars_]), VAR_FLOOR)

    d["log_k"] = np.log1p(d["k"]); d["log_cum_bet"] = np.log1p(d["cum_bet"])
    uraw = d[INPUT_COLS].to_numpy(float)
    u_mean, u_std = uraw.mean(0), uraw.std(0)
    uz = (uraw - u_mean) / u_std
    last_date = d.groupby("user_id")["bet_date"].transform("max")
    churned = ((cutoff - last_date).dt.days >= H).to_numpy()               # >=H matches obs (a bet-day is observable/leaver at exactly H days)

    uids = d["user_id"].to_numpy()
    _, first, counts = np.unique(uids, return_index=True, return_counts=True)
    order = np.argsort(first); first, counts = first[order], counts[order]
    D, K = sb.shape[1], n_behavior + 1
    N = len(d) + int(churned[first].sum())
    Xs = np.zeros((N, D)); U = np.zeros((N, 3)); is_stop = np.zeros(N, bool)
    real_idx = np.empty(len(d), int); starts, lengths = [], []; pos = 0
    for s, c in zip(first, counts):
        rows = slice(s, s + c); starts.append(pos)
        Xs[pos:pos + c] = sb[rows]
        U[pos:pos + c, 0] = 1.0; U[pos:pos + c, 1:] = uz[rows]
        real_idx[s:s + c] = np.arange(pos, pos + c); pos += c
        if churned[s]:
            is_stop[pos] = True; U[pos, 0] = 1.0; U[pos, 1:] = uz[s + c - 1]; pos += 1
        lengths.append(c + (1 if churned[s] else 0))

    W0 = np.zeros((K, K, 3))
    W0[:n_behavior, :n_behavior, 0] = np.log(m0.transmat_ + 1e-6)
    W0[:n_behavior, K - 1, 0] = np.log(0.05); W0[:n_behavior, K - 1, 1] = 0.5
    pi = np.zeros(K); pi[:n_behavior] = m0.get_stationary_distribution(); pi = pi / pi.sum()
    return dict(Xs=Xs, U=U, is_stop=is_stop, starts=np.array(starts), lengths=np.array(lengths),
                mu=mu0, var=var0, W=W0, pi=pi, K=K, D=D, d=d, real_idx=real_idx,
                feat_mean=scaler.mean_, feat_scale=scaler.scale_, u_mean=u_mean, u_std=u_std,
                clip_bounds=clip_bounds)


def _emission_B(Xs, mu, var, is_stop, K):
    N = len(Xs); nb = K - 1; B = np.zeros((N, K)); real = ~is_stop; Xr = Xs[real]
    logB = np.empty((Xr.shape[0], nb))
    for j in range(nb):
        diff = Xr - mu[j]; logB[:, j] = -0.5 * (np.log(2 * np.pi * var[j]).sum() + (diff * diff / var[j]).sum(1))
    logB -= logB.max(1, keepdims=True); B[real, :nb] = np.exp(logB); B[is_stop, K - 1] = 1.0
    return B


def _transition_A(U, W, K):
    logits = np.einsum("tp,ijp->tij", U, W); logits -= logits.max(2, keepdims=True)
    A = np.exp(logits); A /= A.sum(2, keepdims=True)
    A[:, K - 1, :] = 0.0; A[:, K - 1, K - 1] = 1.0
    return A


def _row_hazard(U_rows, stages, W, K):
    logits = np.einsum("nkp,np->nk", W[stages], U_rows); logits -= logits.max(1, keepdims=True)
    p = np.exp(logits); p /= p.sum(1, keepdims=True)
    return p[:, K - 1]


def _forward_backward(B, A, pi, starts, lengths):
    N, K = B.shape; alpha = np.zeros((N, K)); beta = np.zeros((N, K)); c = np.zeros(N); loglik = 0.0
    for s, L in zip(starts, lengths):
        a = pi * B[s]; c[s] = a.sum(); alpha[s] = a / c[s]
        for t in range(1, L):
            idx = s + t; a = (alpha[idx - 1] @ A[idx]) * B[idx]; c[idx] = a.sum(); alpha[idx] = a / c[idx]
        beta[s + L - 1] = 1.0
        for t in range(L - 2, -1, -1):
            idx = s + t; beta[idx] = (A[idx + 1] @ (B[idx + 1] * beta[idx + 1])) / c[idx + 1]
        loglik += np.log(c[s:s + L]).sum()
    gamma = alpha * beta; gamma /= gamma.sum(1, keepdims=True)
    xi = np.zeros((N, K, K)); is_start = np.zeros(N, bool); is_start[starts] = True
    idx = np.where(~is_start)[0]
    xi[idx] = (alpha[idx - 1][:, :, None] * A[idx] * (B[idx] * beta[idx])[:, None, :]) / c[idx][:, None, None]
    return gamma, xi, loglik


def _tier_cuts(obs_p, obs_leave, cur_p, med_rate=MED_RATE, high_share=HIGH_SHARE):
    p, y, cur = np.asarray(obs_p, float), np.asarray(obs_leave, float), np.asarray(cur_p, float)
    tab = pd.DataFrame({"p_stop": p, "leave": y}); tab["bin"] = pd.qcut(tab["p_stop"], 10, duplicates="drop")
    g = tab.groupby("bin", observed=True).agg(n=("leave", "size"), p_stop_mean=("p_stop", "mean"), leave_rate=("leave", "mean"))
    print("calibration (p_stop decile -> actual %dd leave rate):" % H_GLOBAL)
    print(g.assign(p_stop_mean=g["p_stop_mean"].round(3), leave_rate=(100 * g["leave_rate"]).round(1)).to_string())
    iso = IsotonicRegression(out_of_bounds="clip").fit(p, y)
    grid = np.linspace(p.min(), p.max(), 2000); r = iso.predict(grid)
    med_cut = float(grid[np.searchsorted(r, med_rate)]) if r.max() >= med_rate else float(grid[-1])
    high_cut = max(float(np.quantile(cur, 1 - high_share)), med_cut + 1e-6)
    print(f"\ncuts: med (leave>={med_rate:.0%}) p_stop>={med_cut:.3f} | high (top {high_share:.0%} current) p_stop>={high_cut:.3f}")
    for name, lo, hi in [("high", high_cut, 2.0), ("med", med_cut, high_cut), ("low", -1.0, med_cut)]:
        share = 100 * np.mean((cur >= lo) & (cur < hi)); oseg = (p >= lo) & (p < hi)
        lr = 100 * y[oseg].mean() if oseg.any() else float("nan")
        print(f"  {name:<4} current-label {share:>4.1f}% | actual {H_GLOBAL}d leave {lr:>4.1f}%")
    return med_cut, high_cut


# seed=1: this data has two EM local optima (see report_ss03_confluence_cn.md Sec 1.2.1) -- seed=42
# lands on the higher-log-likelihood "gradient" solution, seed=1 lands on "bimodal". Bimodal is the
# one shipped: silhouette 0.136 vs 0.090, min pairwise centroid distance 2.575 vs 2.368 -- better
# player-typing separation, even though gradient fits the raw joint likelihood better (+883).
def fit_iohmm(csv_path, out_dir, n_behavior=4, H=21, seed=1, em_iters=20, inner=20, lr=1.0, reg=1e-3):
    global H_GLOBAL; H_GLOBAL = H
    dat = _build_data(csv_path, n_behavior, H, seed)
    Xs, U, is_stop, starts, lengths = dat["Xs"], dat["U"], dat["is_stop"], dat["starts"], dat["lengths"]
    mu, var, W, pi, K = dat["mu"], dat["var"], dat["W"], dat["pi"], dat["K"]
    nb = K - 1; real = ~is_stop
    print(f"IO-HMM EM: {len(starts)} users, {len(Xs)} obs ({is_stop.sum()} STOP), K={K} (nb={nb}+STOP)\n")
    prev_ll = None
    for it in range(em_iters):
        B = _emission_B(Xs, mu, var, is_stop, K); A = _transition_A(U, W, K)
        gamma, xi, ll = _forward_backward(B, A, pi, starts, lengths)
        print(f"  EM {it:2d}  loglik={ll:.1f}" + ("" if prev_ll is None else f"  d={ll-prev_ll:+.1f}")); prev_ll = ll
        for j in range(nb):
            w = gamma[real, j]; sw = w.sum()
            mu[j] = (w[:, None] * Xs[real]).sum(0) / sw
            var[j] = np.maximum((w[:, None] * (Xs[real] - mu[j]) ** 2).sum(0) / sw, VAR_FLOOR)
        gamma_prev = xi.sum(2); ntr = (~np.isin(np.arange(len(Xs)), starts)).sum()
        for _ in range(inner):
            A = _transition_A(U, W, K)
            grad = np.einsum("tij,tp->ijp", xi - gamma_prev[:, :, None] * A, U) / ntr
            W[:nb] += lr * grad[:nb] - lr * reg * W[:nb]
        pi = gamma[starts].mean(0); pi = pi / pi.sum()

    A = _transition_A(U, W, K)
    gamma, _, _ = _forward_backward(_emission_B(Xs, mu, var, is_stop, K), A, pi, starts, lengths)
    d = dat["d"]; d = d.assign(stage=gamma[dat["real_idx"]].argmax(1))
    pc = ["k", "bet_count_today", "bet_amount_today", "deposit_amount_today", "avg_bet_one_time_today",
          "bet_level_distinct_today", "no_bet_streak_days"]
    prof = d.groupby("stage")[pc].median()
    prof.insert(0, "size", d.groupby("stage").size())
    prof.insert(1, "leave%", (100 * d[d["obs"]].groupby("stage")["leave"].mean()).round(1))
    print("\n=== stages (transient), by tenure ==="); print(prof.sort_values("k").round(2).to_string())

    print("\n=== input-driven churn hazard  P(stage -> STOP | tenure)  at log_cum_bet=median ===")
    haz = {}
    for name, lk in {"low_tenure": -1.0, "med_tenure": 0.0, "high_tenure": 1.0}.items():
        Aq = _transition_A(np.array([[1.0, lk, 0.0]]), W, K)[0]
        haz[name] = [round(100 * Aq[i, K - 1], 1) for i in range(nb)]
    print(pd.DataFrame(haz, index=[f"s{i}" for i in range(nb)]).to_string())

    Amed = _transition_A(np.array([[1.0, 0.0, 0.0]]), W, K)[0]
    print("\n=== transition A at median input (transient + ->STOP) % ===")
    print(pd.DataFrame((100 * Amed).round(1), index=[f"s{i}" for i in range(K - 1)] + ["STOP"],
                       columns=[f"s{i}" for i in range(K - 1)] + ["STOP"]).iloc[:nb].to_string())

    Ur = U[dat["real_idx"]]; d["p_stop"] = _row_hazard(Ur, d["stage"].to_numpy(), W, K)
    print("\n=== risk tier calibration ===")
    o = d[d["obs"]]; cur = d.sort_values("bet_date").groupby("user_id").tail(1)
    print(f"calibrating on {len(o)} observable bet-days (of {len(d)}); base {H}d leave {100*o['leave'].mean():.1f}%\n")
    cut_med, cut_high = _tier_cuts(o["p_stop"], o["leave"], cur["p_stop"])
    d["risk"] = pd.cut(d["p_stop"], [-1, cut_med, cut_high, 2.0], labels=["low", "med", "high"])
    labels = d[["user_id", "bet_date", "k", "stage", "p_stop", "risk"]].copy()
    latest = labels.sort_values("bet_date").groupby("user_id").tail(1)
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    labels.to_csv(out / "ss03_iohmm_labels.csv", index=False)
    print(f"\n=== labels -> ss03_iohmm_labels.csv  ({len(labels)} bet-day rows, {len(latest)} users) ===")
    print("current-label tier mix (each user's latest bet-day):")
    print((100 * latest["risk"].value_counts(normalize=True).reindex(["high", "med", "low"])).round(1).to_string())

    artifact = {"n_behavior": n_behavior, "H": H, "window_start": str(WINDOW_START.date()), "seed": seed, "stop_state": nb, "features": FEATS,
                "feat_mean": dat["feat_mean"].tolist(), "feat_scale": dat["feat_scale"].tolist(),
                "u_mean": dat["u_mean"].tolist(), "u_std": dat["u_std"].tolist(),
                "mu": mu.tolist(), "var": var.tolist(), "W": W.tolist(), "pi": pi.tolist(),
                "log1p_features": [FEATS[j] for j in LOG_IDX], "clip_bounds": dat["clip_bounds"],
                "med_cut": float(cut_med), "high_cut": float(cut_high)}
    with open(out / "ss03_iohmm_model.json", "w") as fh:
        json.dump(artifact, fh)
    print("model artifact -> ss03_iohmm_model.json")
    return dict(mu=mu, var=var, W=W, pi=pi, labels=labels, artifact=artifact)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--n-behavior", type=int, default=4)
    ap.add_argument("--H", type=int, default=21)
    a = ap.parse_args()
    fit_iohmm(a.features, a.out_dir, n_behavior=a.n_behavior, H=a.H)
