"""Tests for io_hmm_infer: data processing (prepare) and filtered inference / labeling (infer, score).

Run:  /opt/anaconda3/bin/python3 -m pytest test_io_hmm_infer.py -q
"""
import json

import numpy as np
import pandas as pd
import pytest

import io_hmm_infer as inf

BASE = pd.Timestamp("2025-01-01")


def tiny_model(med_cut=0.2, high_cut=0.5, W=None):
    """A hand-built artifact: 2 transient states (mu at [0,0] and [3,3]) + STOP; identity normalization;
    W defaults to zeros -> uniform transitions -> p_stop = 1/3 for any state/input (hand-checkable)."""
    return {
        "n_behavior": 2, "H": 46, "stop_state": 2,
        "features": ["f1", "f2"], "log1p_features": [],
        "feat_mean": np.array([0.0, 0.0]), "feat_scale": np.array([1.0, 1.0]),
        "u_mean": np.array([0.0, 0.0]), "u_std": np.array([1.0, 1.0]),
        "mu": np.array([[0.0, 0.0], [3.0, 3.0]]), "var": np.ones((2, 2)),
        "W": np.zeros((3, 3, 3)) if W is None else W, "pi": np.array([0.5, 0.5, 0.0]),
        "med_cut": med_cut, "high_cut": high_cut,
    }


def player(uid, n_days, f1=0.0, f2=0.0, start=0):
    return pd.DataFrame({
        "user_id": uid,
        "bet_date": [BASE + pd.Timedelta(days=start + d) for d in range(n_days)],
        "bet_amount_today": 10.0, "f1": f1, "f2": f2,
    })


# ----------------------------------------------------------------- load / round-trip

def test_load_model_roundtrip(tmp_path):
    art = {**tiny_model(), "mu": [[0, 0], [3, 3]], "var": [[1, 1], [1, 1]],
           "W": np.zeros((3, 3, 3)).tolist(), "pi": [0.5, 0.5, 0.0],
           "feat_mean": [0, 0], "feat_scale": [1, 1], "u_mean": [0, 0], "u_std": [1, 1]}
    p = tmp_path / "m.json"
    p.write_text(json.dumps(art))
    m = inf.load_model(str(p))
    assert isinstance(m["W"], np.ndarray) and m["W"].shape == (3, 3, 3)
    assert np.allclose(m["mu"], [[0, 0], [3, 3]])


# ----------------------------------------------------------------- prepare (data processing)

def test_prepare_k_and_cumbet():
    rows, Xs, U, lengths, first = inf.prepare(player(1, 3), tiny_model())
    assert rows["k"].tolist() == [2, 3]                 # k=1 dropped
    assert rows["cum_bet"].tolist() == [20.0, 30.0]     # cumulative incl day 1
    assert lengths == [2]


def test_prepare_standardization_uses_model_constants():
    m = tiny_model()
    m["feat_mean"], m["feat_scale"] = np.array([1.0, 1.0]), np.array([2.0, 2.0])
    rows, Xs, U, lengths, first = inf.prepare(player(1, 2, f1=5.0, f2=9.0), m)
    assert np.allclose(Xs[0], [(5 - 1) / 2, (9 - 1) / 2])   # uses saved mean/scale, not refit


def test_prepare_first_day_only_flagged():
    df = pd.concat([player(1, 1), player(2, 3)], ignore_index=True)
    rows, Xs, U, lengths, first = inf.prepare(df, tiny_model())
    assert list(first) == [1]
    assert set(rows["user_id"]) == {2}


def test_prepare_raises_missing_column():
    with pytest.raises(ValueError, match="missing columns"):
        inf.prepare(player(1, 2).drop(columns=["f2"]), tiny_model())


def test_prepare_raises_nan_feature():
    df = player(1, 3)
    df.loc[2, "f1"] = np.nan               # a k>=2 row
    with pytest.raises(ValueError, match="NaN in k>=2"):
        inf.prepare(df, tiny_model())


# ----------------------------------------------------------------- infer / score

def test_infer_emission_picks_nearest_state():
    m = tiny_model()
    rows, Xs, U, lengths, _ = inf.prepare(player(1, 2, f1=3.0, f2=3.0), m)   # near mu[1]
    stage, p_stop = inf.infer(Xs, U, lengths, m)
    assert stage[-1] == 1


def test_infer_p_stop_uniform_W_is_one_third():
    m = tiny_model()                                    # W=0 -> uniform transitions -> p_stop = 1/3
    rows, Xs, U, lengths, _ = inf.prepare(player(1, 2), m)
    _, p_stop = inf.infer(Xs, U, lengths, m)
    assert np.allclose(p_stop, 1 / 3)


def test_infer_p_stop_responds_to_input():
    W = np.zeros((3, 3, 3)); W[:, 2, 1] = 5.0           # STOP logit grows with u[1] (log tenure)
    m = tiny_model(W=W)
    rows, Xs, U, lengths, _ = inf.prepare(player(1, 6), m)   # k=2..6, increasing tenure
    _, p_stop = inf.infer(Xs, U, lengths, m)
    assert np.all(np.diff(p_stop) > 0)                  # monotonically rising with tenure


def test_score_tiers_and_first_day():
    df = pd.concat([player(1, 3), player(2, 1)], ignore_index=True)
    out = inf.score(df, tiny_model(med_cut=0.2, high_cut=0.5))   # p_stop=1/3 -> "med"
    assert set(out.loc[out.user_id == 1, "risk"]) == {"med"}
    assert out.loc[out.user_id == 2, "risk"].iloc[0] == "first_day"


def test_score_tier_boundary_low():
    out = inf.score(player(1, 3), tiny_model(med_cut=0.4, high_cut=0.6))   # 1/3 < 0.4 -> "low"
    assert set(out["risk"]) == {"low"}


def test_score_reproducible():
    df = player(1, 4)
    a, b = inf.score(df, tiny_model()), inf.score(df, tiny_model())
    pd.testing.assert_frame_equal(a, b)


# ----------------------------------------------------------------- stateful online API

def test_update_matches_batch():
    # feeding bet-days one at a time through update() must reproduce the batch score() (k>=2 rows).
    W = np.zeros((3, 3, 3)); W[:, 2, 1] = 2.0; W[1, 1, 0] = 1.0      # non-trivial dynamics
    m = tiny_model(W=W)
    df = player(1, 5, f1=2.0, f2=1.0).sort_values("bet_date").reset_index(drop=True)
    state = inf.init_state()
    online = []
    for _, r in df.iterrows():
        state, lab = inf.update(state, r, m)
        online.append(lab)
    batch = inf.score(df, m)
    # compare the k>=2 portion (k=1 is first_day online; dropped in batch for a returning user)
    on = pd.DataFrame(online)
    on = on[on["k"] >= 2].reset_index(drop=True)
    assert on["stage"].tolist() == batch["stage"].tolist()
    assert np.allclose(on["p_stop"].to_numpy(float), batch["p_stop"].to_numpy(float))
    assert on["risk"].tolist() == batch["risk"].tolist()


def test_update_first_day():
    state = inf.init_state()
    state, lab = inf.update(state, player(1, 1).iloc[0], tiny_model())
    assert lab["risk"] == "first_day" and lab["k"] == 1


def test_update_raises_on_nan():
    m = tiny_model()
    state = inf.init_state()
    state, _ = inf.update(state, player(1, 2).iloc[0], m)            # k=1 ok
    row = player(1, 2).iloc[1].copy(); row["f1"] = np.nan
    with pytest.raises(ValueError, match="NaN in features"):
        inf.update(state, row, m)
