"""Tests for first_day_model: data prep (label / observability / NaN guard) and the model (signal, reproducible).

Run:  /opt/anaconda3/bin/python3 -m pytest test_first_day_model.py -q
"""
import numpy as np
import pandas as pd
import pytest

import first_day_model as fdm

BASE = pd.Timestamp("2025-01-01")


def _row(user_id, day_offset, **overrides):
    """One (user, bet-day) row with all FIRST_DAY_FEATURES filled with valid defaults; override as needed."""
    row = {f: 1.0 for f in fdm.FIRST_DAY_FEATURES}
    row.update(user_id=user_id, bet_date=BASE + pd.Timedelta(days=day_offset))
    row.update(overrides)
    return row


def _frame(rows):
    return pd.DataFrame(rows)


# ----------------------------------------------------------------- data prep

def test_leave_label_three_cases():
    # u1 never returns -> leave; u2 returns within 46d -> stay; u3 returns after >46d -> leave.
    df = _frame([
        _row(1, 0),
        _row(2, 0), _row(2, 10),
        _row(3, 0), _row(3, 60),
    ])
    first = fdm.prepare_first_day(df).set_index("user_id")
    assert first.loc[1, "leave"] == 1
    assert first.loc[2, "leave"] == 0
    assert first.loc[3, "leave"] == 1


def test_leave_boundary_exactly_H_is_stay():
    # gap == H (46) is NOT churn (matches the > H convention used everywhere).
    df = _frame([_row(1, 0), _row(1, fdm.H)])
    assert fdm.prepare_first_day(df).iloc[0]["leave"] == 0


def test_first_day_is_first_row_per_user():
    df = _frame([_row(1, 5, bet_count_today=99), _row(1, 0, bet_count_today=7)])
    first = fdm.prepare_first_day(df)
    assert len(first) == 1
    assert first.iloc[0]["bet_count_today"] == 7      # the earliest date, not row order


def test_observability_flag():
    # cutoff is the latest bet_date; a first-day within H of cutoff is not observable.
    df = _frame([_row(1, 0), _row(2, 100)])           # cutoff = day 100
    first = fdm.prepare_first_day(df).set_index("user_id")
    assert bool(first.loc[1, "obs"]) is True          # day 0 <= 100 - 46
    assert bool(first.loc[2, "obs"]) is False          # day 100 > 100 - 46


def test_raises_on_missing_column():
    df = _frame([_row(1, 0)]).drop(columns=["rtp_day"])
    with pytest.raises(ValueError, match="missing columns"):
        fdm.prepare_first_day(df)


def test_raises_on_nan_feature():
    # a NaN in a feature on a first-day row means a history feature slipped in -> must raise, not impute.
    df = _frame([_row(1, 0), _row(2, 0)])
    df.loc[0, "rtp_day"] = np.nan
    with pytest.raises(ValueError, match="NaN in first-day features"):
        fdm.prepare_first_day(df)


# ----------------------------------------------------------------- model

def _signal_frame(n=400, seed=0):
    """First-day-only users where low bet_count -> leaves, high -> returns (with overlap, so AUC < 1).
    One late anchor user pushes the cutoff out so the day-0 first-days are observable."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        leave = i % 2
        bet_count = max((60 if leave else 240) + rng.normal(0, 60), 1.0)
        rows.append(_row(i, 0, bet_count_today=bet_count, bet_amount_today=bet_count))
        if not leave:
            rows.append(_row(i, 12, bet_count_today=bet_count))      # returns within 46d
    rows.append(_row(99999, 100))                                    # anchor -> cutoff = day 100
    return _frame(rows)


def test_model_learns_signal():
    out = fdm.train(_signal_frame())
    assert out["auc_hgb"] > 0.65          # well above chance on the planted signal
    assert out["auc_tree"] > 0.6
    assert 0.0 <= out["base_rate"] <= 1.0


def test_reproducible():
    a = fdm.train(_signal_frame())
    b = fdm.train(_signal_frame())
    assert a["auc_hgb"] == b["auc_hgb"]
    assert a["auc_tree"] == b["auc_tree"]
