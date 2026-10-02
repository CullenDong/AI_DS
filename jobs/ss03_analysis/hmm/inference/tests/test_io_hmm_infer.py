"""Tests for io_hmm_infer (SS03 online churn scorer).

Two layers:
  1. Unit tests on a hand-built artifact + synthetic players (properties are checkable by hand).
  2. Integration tests on a small real-data sample (fixtures/sample_bet_days.csv) scored by the deployed
     artifact (../assets/ss03_iohmm_model.json), plus a full-data check that self-skips if output/ is absent.

Run:  cd inference && /opt/anaconda3/bin/python3 -m pytest -q      (conftest.py puts scripts/ on the path)
"""
import json
import os

import numpy as np
import pandas as pd
import pytest

import io_hmm_infer as inf

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(HERE, "fixtures", "sample_bet_days.csv")
MODEL = os.path.join(HERE, "..", "assets", "ss03_iohmm_model.json")
FULL_FEATURES = os.path.join(HERE, "..", "..", "output", "ss03_user_day_features.csv")
BASE = pd.Timestamp("2026-06-01")


# ------------------------------------------------------------------ hand-built artifact + synthetic players

def tiny_model(med_cut=0.2, high_cut=0.5, W=None, window_start=None):
    """2 transient states (mu at [0,0] and [3,3]) + STOP; identity normalization; no log1p/clip.
    W defaults to zeros -> uniform transitions -> p_stop = 1/3 for any state/input (hand-checkable)."""
    m = {
        "n_behavior": 2, "H": 21, "stop_state": 2,
        "features": ["f1", "f2"], "log1p_features": [], "clip_bounds": {},
        "feat_mean": np.array([0.0, 0.0]), "feat_scale": np.array([1.0, 1.0]),
        "u_mean": np.array([0.0, 0.0]), "u_std": np.array([1.0, 1.0]),
        "mu": np.array([[0.0, 0.0], [3.0, 3.0]]), "var": np.ones((2, 2)),
        "W": np.zeros((3, 3, 3)) if W is None else W, "pi": np.array([0.5, 0.5, 0.0]),
        "med_cut": med_cut, "high_cut": high_cut,
    }
    if window_start is not None:
        m["window_start"] = window_start
    return m


def player(uid, n_days, f1=0.0, f2=0.0, start=0, bet=10.0):
    return pd.DataFrame({
        "user_id": uid,
        "bet_date": [BASE + pd.Timedelta(days=start + d) for d in range(n_days)],
        "bet_amount_today": bet, "f1": f1, "f2": f2,
    })


def test_load_model_roundtrip(tmp_path):
    art = {"n_behavior": 2, "H": 21, "stop_state": 2, "features": ["f1", "f2"], "log1p_features": [],
           "clip_bounds": {}, "mu": [[0, 0], [3, 3]], "var": [[1, 1], [1, 1]],
           "W": np.zeros((3, 3, 3)).tolist(), "pi": [0.5, 0.5, 0.0], "feat_mean": [0, 0], "feat_scale": [1, 1],
           "u_mean": [0, 0], "u_std": [1, 1], "med_cut": 0.2, "high_cut": 0.5, "window_start": "2026-05-21"}
    p = tmp_path / "m.json"; p.write_text(json.dumps(art))
    m = inf.load_model(str(p))
    assert isinstance(m["W"], np.ndarray) and m["W"].shape == (3, 3, 3)
    assert np.allclose(m["mu"], [[0, 0], [3, 3]])
    assert m["window_start"] == "2026-05-21"          # string metadata, not turned into an array


def test_prepare_k_and_cumbet():
    rows, Xs, U, lengths, fd = inf.prepare(player(1, 3), tiny_model())
    assert rows["k"].tolist() == [2, 3]                # k=1 excluded from the scored rows
    assert rows["cum_bet"].tolist() == [20.0, 30.0]    # cumulative bet includes day 1
    assert lengths == [2]
    assert len(fd) == 1                                # exactly the user's k=1 row for first_day


def test_transform_is_identity_under_trivial_constants():
    m = tiny_model()                                   # zero mean, unit scale, no log1p, no clip
    out = inf._transform(np.array([[1.0, 2.0], [5.0, -3.0]]), m)
    assert np.allclose(out, [[1.0, 2.0], [5.0, -3.0]])


def test_tier_boundaries_match_offline_pd_cut():
    m = tiny_model(med_cut=0.2, high_cut=0.5)
    assert inf._tier(0.6, m) == "high"
    assert inf._tier(0.5, m) == "med"                  # not > high_cut (right-closed cut)
    assert inf._tier(0.3, m) == "med"
    assert inf._tier(0.2, m) == "low"                  # not > med_cut
    assert inf._tier(0.05, m) == "low"


def test_zero_W_gives_uniform_hazard():
    m = tiny_model()                                   # W=0 -> softmax over 3 states -> p_stop = 1/3
    rows, Xs, U, lengths, fd = inf.prepare(player(1, 4), m)
    stage, p_stop = inf.infer(Xs, U, lengths, m)
    assert np.allclose(p_stop, 1.0 / 3.0)


def test_first_day_label_streaming_and_batch_agree():
    m = tiny_model()
    st, lab = inf.update(inf.init_state(), player(1, 3).iloc[0], m)   # streaming, first call
    assert lab["k"] == 1 and lab["stage"] == -1 and lab["risk"] == "first_day" and np.isnan(lab["p_stop"])
    sc = inf.score(player(1, 3), m)                                   # batch: one first_day row for the multi-day user's day 1
    fd = sc[sc["risk"] == "first_day"]
    assert len(fd) == 1 and fd["k"].iloc[0] == 1


def test_window_start_drops_left_censored_cohort():
    ws = "2026-06-01"                                   # == BASE
    m = tiny_model(window_start=ws)
    df = pd.concat([player(1, 3, start=0),              # first-seen 2026-06-01 -> dropped
                    player(2, 3, start=5)], ignore_index=True)  # first-seen 2026-06-06 -> kept
    assert set(inf.score(df, m)["user_id"]) == {2}
    assert set(inf.score(df, tiny_model())["user_id"]) == {1, 2}     # no window_start -> both kept


def test_missing_column_raises():
    with pytest.raises(ValueError, match="missing columns"):
        inf.prepare(player(1, 2).drop(columns=["f2"]), tiny_model())


def test_nan_in_scored_feature_raises():
    p = player(1, 3); p.loc[1, "f1"] = np.nan           # NaN on a k>=2 row
    with pytest.raises(ValueError, match="NaN"):
        inf.prepare(p, tiny_model())


# ------------------------------------------------------------------ integration: real sample + deployed artifact

@pytest.fixture(scope="module")
def real():
    return inf.load_model(MODEL), pd.read_csv(FIXTURE, parse_dates=["bet_date"])


def test_real_score_schema_and_ranges(real):
    model, df = real
    sc = inf.score(df, model)
    assert list(sc.columns) == ["user_id", "bet_date", "k", "stage", "p_stop", "risk"]
    assert sc.loc[sc["k"] >= 2, "p_stop"].between(0, 1).all()
    assert set(sc["risk"]) <= {"low", "med", "high", "first_day"}
    assert sc.loc[sc["k"] >= 2, "stage"].isin(range(model["stop_state"])).all()


def test_real_first_day_one_per_kept_user(real):
    model, df = real
    sc = inf.score(df, model)
    fd = sc[sc["risk"] == "first_day"]
    assert set(fd["user_id"]) == set(sc["user_id"].unique())          # every kept user has a first_day row
    assert (fd.groupby("user_id").size() == 1).all()


def test_real_online_equals_batch(real):
    model, df = real
    batch = inf.score(df, model).set_index(["user_id", "bet_date"])
    recs = []
    for uid, g in df.sort_values(["user_id", "bet_date"]).groupby("user_id", sort=False):
        st = inf.init_state()
        for _, row in g.iterrows():
            st, lab = inf.update(st, row, model)
            recs.append((uid, row["bet_date"], lab["k"], lab["stage"], lab["p_stop"], lab["risk"]))
    on = (pd.DataFrame(recs, columns=["user_id", "bet_date", "k", "stage", "p_stop", "risk"])
          .set_index(["user_id", "bet_date"]).reindex(batch.index))
    assert (on["stage"].to_numpy() == batch["stage"].to_numpy()).all()
    assert (on["risk"].to_numpy() == batch["risk"].to_numpy()).all()
    k2 = batch["k"].to_numpy() >= 2                                   # p_stop is NaN on first_day rows
    assert np.allclose(on["p_stop"].to_numpy(float)[k2], batch["p_stop"].to_numpy(float)[k2], atol=1e-9)


def test_real_current_label_is_each_users_latest(real):
    model, df = real
    cur = inf.current_label(df, model)
    assert (cur.groupby("user_id").size() == 1).all()
    latest = df.groupby("user_id")["bet_date"].max()
    for _, r in cur.iterrows():
        assert r["bet_date"] == latest[r["user_id"]]


def test_real_window_start_drops_05_21_cohort(real):
    model, df = real
    assert model.get("window_start") == "2026-05-21"                 # artifact carries the training window start
    first = df.groupby("user_id")["bet_date"].transform("min")
    censored = set(df.loc[first == pd.Timestamp("2026-05-21"), "user_id"])
    assert censored                                                  # the fixture deliberately includes some
    assert censored.isdisjoint(set(inf.score(df, model)["user_id"]))


@pytest.mark.skipif(not os.path.exists(FULL_FEATURES), reason="full feature table (output/) not generated")
def test_full_data_online_equals_batch():
    model = inf.load_model(MODEL)
    df = pd.read_csv(FULL_FEATURES, parse_dates=["bet_date"])
    batch = inf.score(df, model)
    b2 = batch[batch["k"] >= 2].set_index(["user_id", "bet_date"])
    recs = []
    for uid, g in df.sort_values(["user_id", "bet_date"]).groupby("user_id", sort=False):
        st = inf.init_state()
        for _, row in g.iterrows():
            st, lab = inf.update(st, row, model)
            if lab["k"] >= 2:
                recs.append((uid, row["bet_date"], lab["stage"], lab["p_stop"], lab["risk"]))
    on = (pd.DataFrame(recs, columns=["user_id", "bet_date", "stage", "p_stop", "risk"])
          .set_index(["user_id", "bet_date"]).reindex(b2.index))
    assert (on["stage"].to_numpy() == b2["stage"].to_numpy()).all()
    assert (on["risk"].to_numpy() == b2["risk"].to_numpy()).all()
    assert np.allclose(on["p_stop"].to_numpy(float), b2["p_stop"].to_numpy(float), atol=1e-9)
