"""Offline exploration for the HMM player-state module.

Two halves:
  - Segmentation: BIC sweep, feature importance, per-state metrics, trajectories.
  - Churn / retention on a per-user-clock label (leave = no bet within H days of a bet-day).

Canonical model: n_states=3 (Low / Engaged / Lapsed), Golden 7 features
(Golden 8 minus the near-constant multiplier_change_count_ratio), with the three
heavy-tailed features log1p'd on the HMM path only. Findings live in
report_model_selection.md (segmentation) and report_churn.md (churn).

Run:  python explore_churn_states.py <csv> <mode> [n_states]
modes: sweep | analyze | metrics | traj | materials | oneday | leavestay | statechurn | leavesweep
       | describe | lifecycle    (see FUNCTIONS.md for the full index)
"""
import sys

import numpy as np
import pandas as pd
from hmmlearn import hmm
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text

from prepare_model_data import FEATURE_NAMES

# log1p only on the HMM path (these three are skew 227/144/96); the decision tree
# keeps raw X so its thresholds stay in real business units.
LOG1P_FEATURES = ["bet_amount_ratio_today_vs_history", "rtp_7_bet_days", "current_balance_max_to_avg_bet_ratio"]
# multiplier_change_count_ratio is 0 for 56%+ of rows -> covariance collapses to ~1e-7 and
# inflates the likelihood (BIC never elbows). It carries ~no signal, so drop it -> Golden 7.
DROP_FEATURES = ["multiplier_change_count_ratio"]

# State labels are tied to a seed=42 fit (state ids are not stable across refits).
STATE_LABELS = {3: {0: "Low", 1: "Engaged", 2: "Lapsed"},
                4: {0: "Distress", 1: "Healthy", 2: "Lapsed", 3: "Explorer"}}

# Same-day features that exist on a user's first bet-day (the 3 history features are NaN there).
DAY1_FEATURES = ["avg_bet_one_time_today_log", "rtp_7_bet_days", "loss_streak_ratio_today", "target_selection_entropy"]
# Richer same-day signals available at the first bet-day, for the k=1 leave/stay classifier.
DAY1_X = ["bet_count_today", "bet_amount_today", "profit_today", "avg_bet_one_time_today_log", "rtp_7_bet_days",
          "loss_streak_ratio_today", "target_selection_entropy", "current_balance_day_max", "max_consecutive_loss_count_today"]
# Compact per-state profile (real business units).
PROFILE_COLS = ["no_bet_streak_days", "bet_amount_today", "loss_streak_ratio_today", "rtp_7_bet_days", "current_balance_day_max"]


# --------------------------------------------------------------------------- helpers

def features():
    """The Golden 7 feature names fed to the HMM (Golden 8 minus DROP_FEATURES)."""
    return [f for f in FEATURE_NAMES if f not in DROP_FEATURES]


def label(k, n_states, prefix=True):
    """Human-readable state label: 's0 Low' (prefix) or 'Low' (prefix=False); 's<k>' when unlabeled."""
    name = STATE_LABELS.get(n_states, {}).get(k)
    if name is None:
        return f"s{k}"
    return f"s{k} {name}" if prefix else name


def load_bet_days(csv_path):
    """Read the DS export (one row per (user, bet-day)), sorted by user then date.
    Adds per-user tenure `k` (1-based bet-day index) and `gap_next` (days to the next bet-day,
    NaN on the last). Returns (df, cutoff)."""
    df = pd.read_csv(csv_path, parse_dates=["bet_date"]).sort_values(["user_id", "bet_date"]).reset_index(drop=True)
    df["k"] = df.groupby("user_id").cumcount() + 1
    df["gap_next"] = df.groupby("user_id")["bet_date"].shift(-1).sub(df["bet_date"]).dt.days
    return df, df["bet_date"].max()


def _log1p_golden(X):
    """log1p the heavy-tailed Golden-7 columns in-place-safe; raises on NaN or negatives."""
    feat = features()
    if np.isnan(X).any():
        raise ValueError(f"NaN in HMM feature rows: {[feat[i] for i in np.unique(np.where(np.isnan(X))[1])]}")
    log_idx = [feat.index(f) for f in LOG1P_FEATURES]
    if (X[:, log_idx] < 0).any():
        raise ValueError(f"negative value passed to log1p in {LOG1P_FEATURES}")
    X = X.copy()
    X[:, log_idx] = np.log1p(X[:, log_idx])
    return X


def fit_hmm(X, lengths, n_states, seed=42, n_iter=200):
    m = hmm.GaussianHMM(n_components=n_states, covariance_type="diag", n_iter=n_iter, random_state=seed)
    m.fit(X, lengths)
    return m


def hmm_features(rows):
    """(X_raw, X_scaled, lengths, scaler) for a set of k>=2 rows. X_raw is untransformed (for the
    surrogate tree), X_scaled is log1p'd + standardized (for the HMM)."""
    feat = features()
    X_raw = rows[feat].to_numpy(float)
    scaler = StandardScaler().fit(_log1p_golden(X_raw))
    X_scaled = scaler.transform(_log1p_golden(X_raw))
    lengths = rows.groupby("user_id", sort=False).size().tolist()
    return X_raw, X_scaled, lengths, scaler


def decode_states(df, n_states, seed=42, train_mask=None):
    """Fit a GaussianHMM on the k>=2 rows (or, if train_mask given, the train subset of them) and
    write df['state'] for all k>=2 rows (NaN on first bet-days). Returns (model, scaler)."""
    feat = features()
    k2 = (df["k"] >= 2).to_numpy()
    rows = df.loc[k2]
    X = _log1p_golden(rows[feat].to_numpy(float))
    fit_sel = np.ones(len(rows), bool) if train_mask is None else train_mask[k2]
    scaler = StandardScaler().fit(X[fit_sel])
    Xs = scaler.transform(X)
    model = fit_hmm(Xs[fit_sel], rows[fit_sel].groupby("user_id", sort=False).size().tolist(), n_states, seed)
    df.loc[k2, "state"] = model.predict(Xs, rows.groupby("user_id", sort=False).size().tolist())
    return model, scaler


def add_leave(df, H=46, rel_mult=None):
    """Per-bet-day churn label on the user clock. leave=1 if no bet within the threshold of this bet-day;
    obs=1 if there is enough look-ahead to evaluate it. Threshold is the fixed H, unless rel_mult is set,
    then it is cadence-relative: max(H, rel_mult * the user's median inter-bet gap) -- so a naturally
    low-frequency player is not flagged for a normal-length gap."""
    cutoff = df["bet_date"].max()
    if rel_mult is None:
        thr = float(H)
    else:
        umed = df[df["k"] >= 2].groupby("user_id")["no_bet_streak_days"].median()
        thr = (rel_mult * df["user_id"].map(umed)).clip(lower=H).fillna(float(H))
    df["leave"] = (df["gap_next"].isna() | (df["gap_next"] > thr)).astype(int)
    df["obs"] = df["bet_date"] <= (cutoff - pd.to_timedelta(thr, unit="D"))
    return df


def add_cummeans(df, cols):
    """Add cumulative (expanding) per-user means `cm_<col>` = behavior as known up to each bet-day."""
    for f in cols:
        df["cm_" + f] = df.groupby("user_id")[f].expanding().mean().reset_index(level=0, drop=True)
    return df


def user_test_mask(users, frac=0.3, seed=0):
    """Boolean array over the rows (True=test), split by user_id so a user's whole sequence stays on
    one side (no leakage)."""
    rng = np.random.default_rng(seed)
    uq = users.unique()
    test = set(rng.choice(uq, size=int(frac * len(uq)), replace=False))
    return users.isin(test).to_numpy()


def eta_squared(X, labels):
    """Per-feature SS_between / SS_total -- how much of each feature's variance the state labels
    explain (0..1). X should be standardized so features are comparable."""
    grand = X.mean(0)
    sst = ((X - grand) ** 2).sum(0)
    ssb = np.zeros(X.shape[1])
    for k in np.unique(labels):
        Xk = X[labels == k]
        ssb += len(Xk) * (Xk.mean(0) - grand) ** 2
    return ssb / sst


def marg_loglik(Xs, means, var):
    """Diagonal-Gaussian log-likelihood marginalized to the given dims. Xs(N,D), means(K,D), var(K,D) -> (N,K)."""
    ll = np.empty((len(Xs), len(means)))
    for k in range(len(means)):
        d = Xs - means[k]
        ll[:, k] = -0.5 * (np.log(2 * np.pi * var[k]).sum() + (d * d / var[k]).sum(1))
    return ll


def stationary(transmat):
    """Stationary distribution (normalized left eigenvector for eigenvalue 1) of a transition matrix."""
    vals, vecs = np.linalg.eig(transmat.T)
    pi = np.real(vecs[:, np.argmin(np.abs(vals - 1))])
    return pi / pi.sum()


# --------------------------------------------------------------------------- segmentation

def sweep_bic(csv_path, n_min=2, n_max=8, seed=42):
    """Sweep n_states and report log-lik / AIC / BIC / convergence. BIC barely penalizes complexity
    at this sample size, so read the marginal-gain knee, not the minimum."""
    df, _ = load_bet_days(csv_path)
    rows = df[df["k"] >= 2]
    _, Xs, lengths, _ = hmm_features(rows)
    print(f"train set: {len(Xs)} rows, {len(lengths)} users, {len(features())} features\n")
    print(f"{'n_states':>8} {'loglik':>14} {'AIC':>14} {'BIC':>14} {'converged':>10} {'iters':>6}")
    rows_out = []
    for n in range(n_min, n_max + 1):
        m = fit_hmm(Xs, lengths, n, seed)
        ll, aic, bic = m.score(Xs, lengths), m.aic(Xs, lengths), m.bic(Xs, lengths)
        rows_out.append((n, ll, aic, bic, m.monitor_.converged, m.monitor_.iter))
        print(f"{n:>8} {ll:>14.1f} {aic:>14.1f} {bic:>14.1f} {str(m.monitor_.converged):>10} {m.monitor_.iter:>6}")
    if not all(r[4] for r in rows_out):
        print("\nNote: some models did not converge -- their BIC is not comparable.")
    best = min(rows_out, key=lambda r: r[3])
    print(f"\nBIC-min: n_states={best[0]} (BIC={best[3]:.1f})")
    return rows_out


def analyze(csv_path, n_states=3, seed=42):
    """Feature importance for the segmentation: eta^2 (univariate state separation) and the
    surrogate depth-3 tree (incremental importance + fidelity + rules), plus per-state z-means."""
    df, _ = load_bet_days(csv_path)
    rows = df[df["k"] >= 2]
    X_raw, Xs, lengths, _ = hmm_features(rows)
    m = fit_hmm(Xs, lengths, n_states, seed)
    labels = m.predict(Xs, lengths)
    feat = features()
    sizes = np.bincount(labels, minlength=n_states)
    eta = eta_squared(Xs, labels)
    zmeans = np.array([Xs[labels == k].mean(0) for k in range(n_states)])
    clf = DecisionTreeClassifier(max_depth=3, random_state=seed).fit(X_raw, labels)

    print(f"=== n_states={n_states} | converged={m.monitor_.converged} iters={m.monitor_.iter} ===")
    print("state sizes:", {k: int(sizes[k]) for k in range(n_states)})
    order = np.argsort(eta)[::-1]
    print("\n=== feature importance ===")
    print(f"{'feature':<38}{'eta^2(HMM sep)':>16}{'tree_importance':>18}")
    for i in order:
        print(f"{feat[i]:<38}{eta[i]:>16.3f}{clf.feature_importances_[i]:>18.3f}")
    print(f"\ndepth-3 surrogate tree fidelity (accuracy): {clf.score(X_raw, labels):.3f}")
    print("\n=== per-state z-means (standardized space; + = above population mean) ===")
    print(f"{'state(rows)':<14}" + "".join(f"{feat[i][:11]:>13}" for i in order))
    for k in range(n_states):
        print(f"s{k}({sizes[k]:>6})  " + "".join(f"{zmeans[k][i]:>13.2f}" for i in order))
    print("\n=== surrogate tree rules (thresholds in raw business units) ===")
    print(export_text(clf, feature_names=list(feat)))


def state_metrics(csv_path, n_states=3, seed=42):
    """Full per-state metrics: overview, raw-unit distributions, HMM emission params,
    transition matrix + stationary distribution, and multi-lens feature importance."""
    df, _ = load_bet_days(csv_path)
    m, _ = decode_states(df, n_states, seed)
    dd = df[df["k"] >= 2].copy()
    dd["state"] = dd["state"].astype(int)
    feat = features()
    name = lambda k: label(k, n_states, prefix=False)
    g = dd.groupby("state")

    sizes = g.size()
    modal = dd.groupby("user_id")["state"].agg(lambda s: s.mode().iloc[0])
    ov = pd.DataFrame({
        "rows": sizes, "row%": (100 * sizes / len(dd)).round(1),
        "modal_users": modal.value_counts().reindex(range(n_states)).fillna(0).astype(int),
        "modal_user%": (100 * modal.value_counts(normalize=True).reindex(range(n_states))).round(1)})
    ov.index = [name(k) for k in ov.index]
    print("=== 1. state overview ===")
    print(ov.to_string())

    print("\n=== 2. per-state distribution (raw units: mean / std / p10 p25 p50 p75 p90) ===")
    for c in feat + ["bet_count_today", "bet_amount_today", "profit_today", "current_balance_day_max", "rtp_day"]:
        d = g[c]
        tbl = pd.DataFrame({"mean": d.mean(), "std": d.std(), "p10": d.quantile(.1), "p25": d.quantile(.25),
                            "p50": d.quantile(.5), "p75": d.quantile(.75), "p90": d.quantile(.9)})
        tbl.index = [name(k) for k in tbl.index]
        print(f"\n[{c}]")
        print(tbl.round(3).to_string())

    idx = [name(k) for k in range(n_states)]
    sd = np.sqrt(np.array([np.diag(c) for c in m.covars_]))
    print("\n=== 3. HMM emission params (standardized space) ===")
    print("means_ (z centroids):")
    print(pd.DataFrame(m.means_, columns=feat, index=idx).round(2).to_string())
    print("sd (emission std):")
    print(pd.DataFrame(sd, columns=feat, index=idx).round(2).to_string())

    print("\n=== 4. transition matrix P(next|cur) % + stationary distribution ===")
    print(pd.DataFrame(100 * m.transmat_, index=idx, columns=idx).round(1).to_string())
    pi = stationary(m.transmat_)
    print("stationary pi:", {name(k): round(float(pi[k]), 3) for k in range(n_states)})

    log_idx = [feat.index(f) for f in LOG1P_FEATURES]
    Xlog = dd[feat].to_numpy(float); Xlog[:, log_idx] = np.log1p(Xlog[:, log_idx])  # eta^2 is affine-invariant
    clf = DecisionTreeClassifier(max_depth=3, random_state=seed).fit(dd[feat].to_numpy(float), dd["state"])
    fi = pd.DataFrame({"eta2_HMM_sep": eta_squared(Xlog, dd["state"].to_numpy()),
                       "tree_importance": clf.feature_importances_,
                       "z_centroid_span": m.means_.max(0) - m.means_.min(0)},
                      index=feat).sort_values("eta2_HMM_sep", ascending=False)
    print("\n=== 5. feature importance (eta^2 / tree / z-centroid span) ===")
    print(fi.round(3).to_string())
    print(f"depth-3 tree fidelity: {clf.score(dd[feat].to_numpy(float), dd['state']):.3f}")


def sample_trajectories(csv_path, n_states=3, silence=46, seed=42, per_stratum=4, max_days=50):
    """Print decoded state trajectories for randomly sampled users across strata (long/medium/short
    churned and still-active). 'churned' here just means silent > `silence` days at the cutoff."""
    df, cutoff = load_bet_days(csv_path)
    decode_states(df, n_states, seed)
    dd = df[df["k"] >= 2].copy()
    dd["state"] = dd["state"].astype(int)
    lens = dd.groupby("user_id").size()
    trailing = (cutoff - dd.groupby("user_id")["bet_date"].max()).dt.days
    churned = trailing > silence
    name = lambda k: label(k, n_states, prefix=False)

    rng = np.random.default_rng(0)
    pick = lambda mask, k: rng.choice(mask[mask].index.to_numpy(), size=min(k, int(mask.sum())), replace=False)
    strata = [("churned, long (>=12 bet-days)", churned & (lens >= 12)),
              ("churned, medium (6-11)", churned & lens.between(6, 11)),
              ("churned, short (3-5)", churned & lens.between(3, 5)),
              ("still active (>=8)", (~churned) & (lens >= 8))]
    print(f"cutoff={cutoff.date()} | silence threshold >{silence}d | states: " +
          ", ".join(f"s{k}={name(k)}" for k in range(n_states)))
    for title, mask in strata:
        print(f"\n########## {title} ##########")
        for u in pick(mask, per_stratum):
            d = dd[dd.user_id == u]
            tag = f"churned, silent {int(trailing[u])}d after" if bool(churned[u]) else f"active, last bet {int(trailing[u])}d ago"
            extra = f"  (+{len(d) - max_days} earlier bet-days not shown)" if len(d) > max_days else ""
            print(f"\nuser {u} -- {len(d)} bet-days, {tag}{extra}")
            for r in d.tail(max_days).itertuples():
                print(f"   {r.bet_date.date()}  gap{int(r.no_bet_streak_days):>3}d  {name(r.state):<8}"
                      f" ls{r.loss_streak_ratio_today:.2f} rtp{r.rtp_7_bet_days:.2f}"
                      f"  bet{r.bet_amount_today:>8.0f}  bal{r.current_balance_day_max:>9.0f}")


def dump_oneday_examples(csv_path, k=18, seed=0):
    """Print raw first-day rows of randomly sampled single-bet-day (filtered-out) users."""
    df, _ = load_bet_days(csv_path)
    sz = df.groupby("user_id")["bet_date"].transform("size")
    cols = ["user_id", "bet_date", "bet_count_today", "bet_amount_today", "avg_bet_one_time_today", "payout_today",
            "profit_today", "rtp_day", "rtp_7_bet_days", "loss_streak_ratio_today", "target_selection_entropy",
            "current_balance_day_max"]
    print(f"\n########## filtered single-bet-day users (random {k}) -- first-day raw ##########")
    print("(reference: returning users' first-day median bet_count~472 / rtp_7~0.96 / loss_streak~0.18)\n")
    print(df[sz == 1].sample(k, random_state=seed)[cols].to_string(index=False))


def dump_materials(csv_path, n_states=3, seed=42):
    """Raw materials dump: rich trajectory sample + filtered single-day raw rows."""
    sample_trajectories(csv_path, n_states=n_states, seed=seed)
    dump_oneday_examples(csv_path)


def fit_filtered_one_day(csv_path, n_states=3, seed=42):
    """How well do the filtered single-bet-day users fit the states? Their 3 history features are NaN
    on day 1, so score them with the diagonal-Gaussian emission marginalized to the 4 day-1 features,
    and compare their best-state fit to training rows on the same 4 dims."""
    df, _ = load_bet_days(csv_path)
    feat, ai = features(), [features().index(f) for f in DAY1_FEATURES]
    train = df[df["k"] >= 2]
    _, _, lengths, scaler = hmm_features(train)
    m = fit_hmm(scaler.transform(_log1p_golden(train[feat].to_numpy(float))), lengths, n_states, seed)
    means = m.means_[:, ai]
    var = np.array([np.diag(c) for c in m.covars_])[:, ai]

    def scale_day1(raw):
        x = raw.copy()
        for j, f in enumerate(DAY1_FEATURES):
            if f in LOG1P_FEATURES:
                x[:, j] = np.log1p(x[:, j])
        return (x - scaler.mean_[ai]) / scaler.scale_[ai]

    tr_s = scale_day1(train[DAY1_FEATURES].to_numpy(float))
    tr_ll = marg_loglik(tr_s, means, var)
    sz = df.groupby("user_id")["bet_date"].transform("size")
    oneday = df[sz == 1]
    od_raw = oneday[DAY1_FEATURES].to_numpy(float)
    if np.isnan(od_raw).any():
        raise ValueError("single-day users still have NaN in the 4 day-1 features")
    od_ll = marg_loglik(scale_day1(od_raw), means, var)

    print(f"train rows: {len(train)} | single-day users: {len(oneday)} | day-1 dims: {DAY1_FEATURES}\n")
    print("=== which state single-day users map to (4-dim marginal argmax) vs training rows ===")
    od_assign, tr_assign = od_ll.argmax(1), tr_ll.argmax(1)
    for k in range(n_states):
        print(f"  {label(k, n_states):<12} 1day {100*np.mean(od_assign==k):>5.1f}%   |  train {100*np.mean(tr_assign==k):>5.1f}%")
    print("\n=== fit quality: best-state 4-dim loglik (higher = more like the training manifold) ===")
    tr_best, od_best = tr_ll.max(1), od_ll.max(1)
    print(f"{'pctile':>8}" + "".join(f"{f'p{q}':>10}" for q in (1, 5, 25, 50)))
    print(f"{'train':>8}" + "".join(f"{np.percentile(tr_best, q):>10.2f}" for q in (1, 5, 25, 50)))
    print(f"{'1day':>8}" + "".join(f"{np.percentile(od_best, q):>10.2f}" for q in (1, 5, 25, 50)))
    thr = np.percentile(tr_best, 5)
    print(f"\ntrain 5th-pctile loglik = {thr:.2f}; single-day users below it (poor fit / outliers): {100*np.mean(od_best < thr):.1f}%")
    print("\n=== single-day vs training: 4-dim raw median (where they differ) ===")
    print(f"{'feature':<32}{'1day':>10}{'train':>10}")
    for f in DAY1_FEATURES:
        print(f"{f:<32}{oneday[f].median():>10.3f}{train[f].median():>10.3f}")


# --------------------------------------------------------------------------- churn / retention (user-clock label)

def leave_stay_analysis(csv_path, H=46, seed=42):
    """Leave-vs-stay decision boundary for the k=1 and k=2-4 cohorts under the user-clock label
    (leave = no bet within H days). Reports depth-3 tree rules + feature importance + AUC."""
    df, _ = load_bet_days(csv_path)
    add_leave(df, H)
    add_cummeans(df, features() + ["bet_amount_today", "bet_count_today"])

    def fit_report(X, y, users, title):
        if X.isna().any().any():
            raise ValueError(f"{title}: NaN in X {X.columns[X.isna().any()].tolist()}")
        te = user_test_mask(users, 0.3, 0); tr = ~te
        print(f"\n########## {title} ##########")
        print(f"samples {len(X)} | leave base rate {100*y.mean():.1f}%")
        sc = StandardScaler().fit(X[tr])
        lr = LogisticRegression(max_iter=2000).fit(sc.transform(X[tr]), y[tr])
        gb = HistGradientBoostingClassifier(random_state=0).fit(X[tr], y[tr])
        t3 = DecisionTreeClassifier(max_depth=3, random_state=0).fit(X[tr], y[tr])
        a = lambda p: roc_auc_score(y[te], p)
        print("test AUC: logistic %.3f | GBM(ceiling) %.3f | depth3 tree %.3f" % (
            a(lr.predict_proba(sc.transform(X[te]))[:, 1]), a(gb.predict_proba(X[te])[:, 1]), a(t3.predict_proba(X[te])[:, 1])))
        fi = pd.DataFrame({"tree_imp": pd.Series(t3.feature_importances_, index=X.columns).round(3),
                           "logit_coef(+=leave)": pd.Series(lr.coef_[0], index=X.columns).round(2)})
        print("feature importance:")
        print(fi.sort_values("tree_imp", ascending=False).to_string())
        print("decision boundary (depth3, value=[stay,leave]):")
        print(export_text(t3, feature_names=list(X.columns)))

    k1 = df[(df.k == 1) & df.obs]
    fit_report(k1[DAY1_X].reset_index(drop=True), k1["leave"].reset_index(drop=True),
               k1["user_id"].reset_index(drop=True), "k=1 first day: leave vs stay (features = first-day signals)")
    k24 = df[df.k.isin([2, 3, 4]) & df.obs]
    cols = ["cm_" + f for f in features()] + ["k", "cm_bet_amount_today", "cm_bet_count_today"]
    fit_report(k24[cols].reset_index(drop=True), k24["leave"].reset_index(drop=True),
               k24["user_id"].reset_index(drop=True), "k=2-4 fence-sitters: leave vs stay (features = cumulative behavior)")


def describe_states(csv_path, n_list=(6, 8), H=46, seed=42):
    """Detailed per-state characterization under the user-clock leave label (same train/test setup as
    leave_state_sweep): for each n, every state's leave rate, size, and median profile in real units."""
    df, _ = load_bet_days(csv_path)
    add_leave(df, H)
    test = user_test_mask(df["user_id"], 0.3, 0)
    prof = {"no_bet_streak_days": "gap", "bet_count_today": "n_bets", "bet_amount_today": "bet", "avg_bet_one_time_today": "avgbet",
            "rtp_7_bet_days": "rtp7", "rtp_day": "rtp_day", "loss_streak_ratio_today": "loss_strk",
            "current_balance_day_max": "bal", "target_selection_entropy": "entropy",
            "bet_amount_ratio_today_vs_history": "bet/hist", "profit_today": "profit"}
    for n in n_list:
        m, _ = decode_states(df, n, seed, train_mask=~test)
        dd = df[df["k"] >= 2].copy()
        dd["state"] = dd["state"].astype(int)
        L = dd[dd["obs"]]
        Lt = L[test[L.index.to_numpy()]]
        base = Lt["leave"].mean()
        leave_pct = (100 * Lt.groupby("state")["leave"].mean()).round(1)
        med = dd.groupby("state")[list(prof)].median().rename(columns=prof)
        med.insert(0, "leave%", leave_pct)
        med.insert(1, "lift", (Lt.groupby("state")["leave"].mean() / base).round(2))
        med.insert(2, "n_test", Lt.groupby("state").size())
        med = med.sort_values("leave%", ascending=False)
        print(f"\n===== n={n} | base leave {100*base:.1f}% | per-state median profile (real units) =====")
        print(med.round(2).to_string())

        idx = [f"s{k}({leave_pct.get(k, float('nan'))}%)" for k in range(n)]
        tm = pd.DataFrame(100 * m.transmat_, index=idx, columns=[f"s{k}" for k in range(n)])
        print("transition P(next bet-day state | current) %, rows sorted by leave%:")
        print(tm.loc[[f"s{k}({leave_pct.get(k, float('nan'))}%)" for k in med.index]].round(1).to_string())
        pi = stationary(m.transmat_)
        print("stationary pi:", {f"s{k}": round(float(pi[k]), 3) for k in range(n)})


def state_vs_leave(csv_path, n_states=3, H=46, seed=42, rel_mult=None):
    """Does the HMM state predict the user-clock churn label? For each k>=2 bet-day with a full
    look-ahead: P(leave | the day's state), and state-only vs behavior vs behavior+state AUC.
    rel_mult enables the cadence-relative leave label (see add_leave)."""
    df, _ = load_bet_days(csv_path)
    add_leave(df, H, rel_mult)
    decode_states(df, n_states, seed)
    add_cummeans(df, features() + ["bet_amount_today", "bet_count_today"])
    L = df[(df["k"] >= 2) & df["obs"]].copy()
    L["state"] = L["state"].astype(int)
    base = L["leave"].mean()
    print(f"landmarks (k>=2 with full {H}d look-ahead): {len(L)} | leave base {100*base:.1f}%\n")

    print(f"=== P(leave within {H}d | the bet-day's state) ===")
    g = L.groupby("state")["leave"]
    for k in sorted(L["state"].unique()):
        print(f"  {label(k, n_states):<12} n={int(g.size()[k]):>7}  leave {100*g.mean()[k]:>5.1f}%  lift {g.mean()[k]/base:.2f}")

    y, te = L["leave"].to_numpy(), user_test_mask(L["user_id"], 0.3, 0); tr = ~te
    onehot = pd.get_dummies(L["state"], prefix="st").to_numpy(float)
    beh = L[["cm_" + f for f in features()] + ["k", "cm_bet_amount_today", "cm_bet_count_today"]].to_numpy(float)
    allf = np.hstack([beh, onehot])

    def auc_of(M):
        sc = StandardScaler().fit(M[tr])
        return roc_auc_score(y[te], LogisticRegression(max_iter=2000).fit(sc.transform(M[tr]), y[tr]).predict_proba(sc.transform(M[te]))[:, 1])
    gb = HistGradientBoostingClassifier(random_state=0).fit(allf[tr], y[tr])
    print("\n=== leave-prediction test AUC (correct label) ===")
    print(f"  state one-hot only : {auc_of(onehot):.3f}")
    print(f"  behavior only      : {auc_of(beh):.3f}")
    print(f"  behavior + state   : {auc_of(allf):.3f}")
    print(f"  GBM behavior+state : {roc_auc_score(y[te], gb.predict_proba(allf[te])[:, 1]):.3f}")


def leave_state_sweep(csv_path, n_list=(3, 4, 5, 6, 7, 8), H=46, seed=42, rel_mult=None):
    """Sweep n_states: under the user-clock label, which state has the highest leave rate, and does it
    reproduce on a held-out 30% of users? Reports per-state train/test leave and state-only AUC.
    rel_mult enables the cadence-relative leave label (see add_leave)."""
    df, _ = load_bet_days(csv_path)
    add_leave(df, H, rel_mult)
    test = user_test_mask(df["user_id"], 0.3, 0)
    for n in n_list:
        m, _ = decode_states(df, n, seed, train_mask=~test)
        L = df[(df["k"] >= 2) & df["obs"]].copy()
        L["state"] = L["state"].astype(int)
        L["test"] = test[L.index.to_numpy()]
        Lt, Ltr = L[L.test], L[~L.test]
        base = 100 * Lt["leave"].mean()
        tab = pd.DataFrame({"size_test": Lt.groupby("state").size(),
                            "leave_train%": (100 * Ltr.groupby("state")["leave"].mean()).round(1),
                            "leave_test%": (100 * Lt.groupby("state")["leave"].mean()).round(1)})
        tab["lift_test"] = (tab["leave_test%"] / base).round(2)
        tab = tab.sort_values("leave_test%", ascending=False)
        oh_tr = pd.get_dummies(Ltr["state"])
        oh_te = pd.get_dummies(Lt["state"]).reindex(columns=oh_tr.columns, fill_value=0)
        auc = roc_auc_score(Lt["leave"], LogisticRegression(max_iter=1000).fit(oh_tr, Ltr["leave"]).predict_proba(oh_te)[:, 1])
        print(f"\n===== n={n} | conv={m.monitor_.converged} it={m.monitor_.iter} | test leave base {base:.1f}% | state-only AUC {auc:.3f} =====")
        print(tab.to_string())
        top = tab.index[0]
        med = Lt[Lt.state == top][PROFILE_COLS].median()
        print(f"  highest-leave state s{top} test profile (median): "
              f"gap{med[PROFILE_COLS[0]]:.0f}d  bet{med[PROFILE_COLS[1]]:.0f}  ls{med[PROFILE_COLS[2]]:.2f}  "
              f"rtp{med[PROFILE_COLS[3]]:.2f}  bal{med[PROFILE_COLS[4]]:.0f}")


# --------------------------------------------------------------------------- entry point

def lifecycle(csv_path, n_states=3, seed=42):
    """Descriptive player life cycle along the user's own clock (bet-day index k):
    (1) retention funnel by tenure, (2) behavior evolution by tenure, (3) HMM state-mix by tenure.
    High-k rows are survivors only -- survivorship bias is part of the lifecycle, not a defect."""
    df, _ = load_bet_days(csv_path)
    sz = df.groupby("user_id").size()
    N = len(sz)
    print("=== 1. retention funnel by tenure (bet-day index k) ===")
    print(f"{'reach k>=':>10}{'users':>10}{'% all':>8}{'step ret%':>11}")
    prev = None
    for k in [1, 2, 3, 4, 5, 10, 20, 50, 100]:
        n = int((sz >= k).sum())
        step = f"{100 * n / prev:.1f}" if prev else "-"
        print(f"{k:>10}{n:>10}{100 * n / N:>7.1f}%{step:>11}")
        prev = n

    cols = ["bet_count_today", "bet_amount_today", "avg_bet_one_time_today", "rtp_7_bet_days",
            "loss_streak_ratio_today", "no_bet_streak_days", "current_balance_day_max", "profit_today",
            "target_selection_entropy"]
    tb = pd.cut(df["k"], [0, 1, 2, 4, 9, 19, 49, 10**9], labels=["1", "2", "3-4", "5-9", "10-19", "20-49", "50+"])
    prof = df.groupby(tb, observed=True)[cols].median()
    prof.insert(0, "bet_days", df.groupby(tb, observed=True).size())
    print("\n=== 2. behavior by tenure (median per bet-day; survivorship-biased at high k) ===")
    print(prof.round(2).to_string())

    decode_states(df, n_states, seed)
    dd = df[df["k"] >= 2].copy()
    dd["state"] = dd["state"].astype(int)
    tb2 = pd.cut(dd["k"], [1, 2, 4, 9, 19, 49, 10**9], labels=["2", "3-4", "5-9", "10-19", "20-49", "50+"])
    name = lambda s: label(s, n_states, prefix=False)
    mix = pd.crosstab(tb2, dd["state"], normalize="index") * 100
    mix.columns = [name(c) for c in mix.columns]
    print(f"\n=== 3. state-mix by tenure (% of bet-days, n={n_states}) ===")
    print(mix.round(1).to_string())


MODES = {"sweep": sweep_bic, "analyze": analyze, "metrics": state_metrics, "traj": sample_trajectories,
         "materials": dump_materials, "oneday": fit_filtered_one_day, "leavestay": leave_stay_analysis,
         "statechurn": state_vs_leave, "leavesweep": leave_state_sweep, "describe": describe_states,
         "lifecycle": lifecycle}

if __name__ == "__main__":
    csv = sys.argv[1] if len(sys.argv) > 1 else "data/selected_hmm_features_fm01_cny.csv"
    mode = sys.argv[2] if len(sys.argv) > 2 else "sweep"
    fn = MODES.get(mode)
    if fn is None:
        raise SystemExit(f"unknown mode {mode!r}; choose from {list(MODES)}")
    if len(sys.argv) > 3 and mode in ("analyze", "metrics", "traj", "materials", "oneday", "statechurn", "lifecycle"):
        fn(csv, n_states=int(sys.argv[3]))
    else:
        fn(csv)
