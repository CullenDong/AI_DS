# SS03 inference — online churn scoring

Self-contained online scorer: loads the trained artifact and labels a player's betting days with a churn
risk. **Pure numpy / pandas; no hmmlearn and no training code.**

## Structure
```
inference/
  scripts/io_hmm_infer.py       scorer: load_model / init_state / update / score / current_label
  assets/ss03_iohmm_model.json  the deployed model (emission + transition params, normalization, tier cuts)
  tests/test_io_hmm_infer.py    online==batch, first-day routing, filtered-vs-smoothed agreement
  conftest.py                   puts scripts/ on the import path for tests
```

## Usage
Put `scripts/` on the import path (or run from that directory), then:
```python
import io_hmm_infer as inf
model = inf.load_model("inference/assets/ss03_iohmm_model.json")   # load once at startup

# Online (stateful, one call per new betting day):
state = inf.init_state()                            # one per user; persist (a few floats)
state, label = inf.update(state, bet_day_features, model)
# label = {k, stage, p_stop, risk}; store state back for that user

# Batch (score a set of players' histories at once):
labels  = inf.score(player_df, model)               # per (user, betting day)
current = inf.current_label(player_df, model)        # each user's latest label
```
`bet_day_features` / `player_df` must carry the same upstream features the training export produced
(the model's `features` list plus `bet_amount_today`). Tenure routing: k=1 (first betting day) -> label
`first_day` (handled by the separate first-day model); k>=2 -> the IO-HMM. Filtered (online) and smoothed
(offline) risk agree ~99.7%.

## Test
pytest suite (`conftest.py` puts `scripts/` on the path). Two layers: unit tests on a hand-built artifact
+ synthetic players, and integration tests on a committed real-data sample (`tests/fixtures/sample_bet_days.csv`,
14 users / 136 bet-days covering first-day-only, 2-day, multi-day, whale, and left-censored cohorts) scored
by the deployed artifact. A full-data `online==batch` check self-skips if `../output/` is absent.
```
cd inference && /opt/anaconda3/bin/python3 -m pytest -q
```
