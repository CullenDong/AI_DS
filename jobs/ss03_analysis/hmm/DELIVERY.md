# IO-HMM player-churn labeling (SS03 slot)

Parallels FM01 `../../../DELIVERY.md`. From bet logs, produces a per-bet-day churn-risk label used to select
bet-days for an in-client intervention. It is a risk-ranking model (a calibrated one-step churn probability),
not a point predictor of individual outcomes; the boundary is stated below. Visual summary: artifact
"SS03 Slot Churn — Lifecycle & Labeling".

## Online serving flow

Data -> model -> label, **stateful, per betting day**:

1. **Data** — the upstream feature pipeline emits that player's within-day features on each betting day
   (grain = betting day). Same features as `export_ss03_features.py` produces. A betting day is assigned by
   sessionizing spins with a **9-minute gap** (SS03's own median bet-day play duration; see
   `report_ss03_confluence_cn.md` Sec 1.2.1 for the full derivation and for a separate finding worth noting
   here: this 4-state fit has two distinct EM local optima on this feature set — a "bimodal" solution
   (states split mainly on `no_bet_streak_days`) and a "gradient" solution (states split mainly on
   spend magnitude, higher joint log-likelihood but worse player-type separation). The shipped artifact
   uses the bimodal solution (`stage2_iohmm.py` default seed = 1); this is a real bistability in the fit,
   not a settled non-issue — re-check on retrain, don't assume the default seed stays correct forever.
2. **Model** — each user persists a few floats (forward message + tenure k + cumulative bet). A new betting
   day arrives -> **O(1) incremental update** -> label; no full-history recompute. Routed by tenure: first
   betting day (k=1) -> first-day model (label `first_day`); k>=2 -> IO-HMM.
3. **Label** — `stage / p_stop / risk` (below).

Inference is pure numpy/pandas loaded from the JSON artifact (no hmmlearn at serve time); each user stores
only a handful of floats.

## Deliverables

| file | what |
|---|---|
| `export_ss03_features.py` | upstream feature build: multi-dir raw spin parquet -> one row per (user, betting day) |
| `inference/scripts/io_hmm_infer.py` | online scorer — pure numpy/pandas, no hmmlearn. `score`/`current_label` (batch), `init_state`/`update` (O(1) live) |
| `inference/assets/ss03_iohmm_model.json` | deployable artifact: emission params + transition W + normalization constants + tier cuts |
| `inference/tests/` | pytest suite: unit tests (hand-built artifact + synthetic players) + integration on a committed real sample (`fixtures/sample_bet_days.csv`); covers online==batch, first_day routing, window_start drop, tier/NaN guards |
| `ss03_iohmm_labels.csv` | per (user, betting day) label `stage / p_stop / risk` (offline batch product of `stage2_iohmm.py`) |
| `stage1_player_states.py` / `stage2_iohmm.py` | behavior-state emission layer / IO-HMM training + calibration (k>=2). Retrain only; not needed at serve time |
| `report_stage1/2_*.md`, `report_ss03_iohmm_full.md` | model analysis |
| first-day (k=1) model | **out of scope** — owned elsewhere; k=1 routes out as `first_day` |

## Model

Churn = a player does not bet again within **21 days** of a betting day ("left"). Two models split by tenure:

- **Returning players (k>=2)**: Input-Output HMM. Behavior sets the state (4 states over money-tier x recency,
  from 5 emission features incl `no_bet_streak_days`); tenure and cumulative bet drive the transitions and the
  churn hazard `P(leave | state, tenure)`. Emissions are Gaussian (diagonal covariance), input-independent;
  transitions are `softmax(W . u)` with input `u = [1, log(1+k), log(1+cum_bet)]`; one absorbing STOP state.
- **First day (~75% of users)**: separate first-day model (no history on day 1). PENDING (owned elsewhere).

## Labels

- `stage` — the maximum-a-posteriori behavioral state for the bet-day (numeric index). States are
  profile-labeled by their feature medians (daily-regular / high-value / low-value-sporadic / post-gap); the
  numeric index is **not stable** across re-estimation — reference states by profile, not number.
- `p_stop` — modeled one-step churn hazard (`P(leave within 21d | state, tenure)`), in [0, 1].
- `risk` — `p_stop` mapped to a tier calibrated to the **actual** 21-day leave rate:
  - **high** — `p_stop >= 0.601` (top 45% of most-recent-bet-day labels by `p_stop`); observed 21-day leave
    rate 0.584; ~9.5% of all users. This is the tier selected for the intervention.
  - **med** — `0.312 <= p_stop < 0.601`; observed leave ~30-58% (measured 0.378).
  - **low** — `p_stop < 0.312`; observed 21-day leave rate 0.147.
  - **first_day** — k=1 players, scored by the first-day model (not the IO-HMM).
- A player's **current label** = their latest betting day's row.

Base 21-day leave = 32% (0.3198 measured). `HIGH_SHARE` = 0.45, chosen at the reach/precision knee (the
recency feature keeps added players ~50% leavers out to a 45% cut). Tunable — recalibrate to move the cut.

## Tier -> action

The tier decides whether an in-client action fires on that bet-day. The action itself (e.g. an in-client
free-game grant) is a product decision; the model supplies the eligibility signal.

- **high** — eligible for the intervention. The hazard is largest at low tenure, so eligibility concentrates
  on early-tenure bet-days.
- **med** — no default action; re-evaluated as `p_stop` updates on subsequent bet-days.
- **low** — no action. Note high-value and daily-regular states fall predominantly here.

## Usage

Put `inference/scripts/` on the import path (or run from that directory), then:

**Online (one call per new betting day):**

```python
import io_hmm_infer as inf
model = inf.load_model("inference/assets/ss03_iohmm_model.json")   # load once at startup

state = inf.init_state()                             # one per user; persist (a few floats)
# a new betting day arrives for this user:
state, label = inf.update(state, bet_day_features, model)
# label = {k, stage, p_stop, risk}; store state back for that user, wait for the next betting day
```

**Batch backfill / analysis (label a set of players' histories at once):**

```python
labels  = inf.score(player_df, model)                # per (user, betting day)
current = inf.current_label(player_df, model)         # each user's latest label
```

`bet_day_features` / `player_df` must carry the same upstream features the training export produced
(the model's `features` list plus `bet_amount_today`). The two paths agree (online filtered == batch,
verified); `ss03_iohmm_labels.csv` is the batch path's offline product for backfill/analysis.

To move the high cut, change `HIGH_SHARE` in `stage2_iohmm.py` and recalibrate (retrain, below).

## Read before acting

- **Filter to recently-active players before firing anything.**
- The 21-day label is a **reversible** gap, not permanent departure: ~14% of bet-days labeled leave return
  within 45 days. A `high` assignment corresponds to an observed leave rate of ~0.585, not 1.0.
- **State indices are not stable across re-estimation** — reference states by their feature profile, not the
  numeric index.
- The intervention targets high-tier (elevated-hazard) bet-days; high-value / high-retention states have low
  `p_stop` and are not selected.
- **Baseline economy only** (`normal_zero` + `normal_zero_95_kai`; `normal_kakuteiB` excluded). Refit if the
  math table changes (e.g. a bandit ships a new table).
- **Online vs offline**: online scoring is filtered, the offline labels file is smoothed; they agree ~99.7%
  on `risk` but are not identical.
- **First-day routing**: the IO-HMM does not score first betting days. k=1 players are labeled `first_day` and
  routed to the (separate) first-day model.
- **Left-censoring**: the artifact carries `window_start` (2026-05-21); bet-days from the cohort first seen on
  the window start are dropped so tenure is not undercounted.

## Accuracy

Risk tiers are **calibrated probabilities**, not a perfect classifier: `p_stop` deciles track the actual
21-day leave rate monotonically (3.5% -> 62.2%); tiers separate high 58.4% / med 37.8% / low 14.7%. The
hazard drops ~14-77x from low to high tenure depending on state (high-value is the least tenure-sensitive
state at ~14x; post-gap the most, ~77x). `risk = high` means ~58.4% leave, not 100%.

## Ops / Retrain

Training is **offline and periodic**, separate from online scoring; a retrain only swaps the artifact —
the service hot-loads the new JSON.

- Training side needs `hmmlearn` (anaconda base; `/opt/anaconda3/bin/python3`). The **online side
  (`io_hmm_infer.py`) is pure numpy/pandas.**
- Rebuild features: `python export_ss03_features.py --raw-dir <parquet dirs...> --out-dir <dir>`
  -> `ss03_user_day_features.csv`.
- Retrain: `python stage2_iohmm.py --features <ss03_user_day_features.csv> --out-dir <dir> [--n-behavior 4] [--H 21]`
  -> new `ss03_iohmm_model.json` (online) + `ss03_iohmm_labels.csv` (offline). Default seed is 1 (the
  bimodal solution, see above) — check the EM log's final loglik after any retrain (bimodal ≈ -21,728 on
  this dataset, gradient ≈ -20,845); don't assume the default seed keeps landing on the right solution.
- State indices are not stable across retrains; the artifact carries its own tier cuts and normalization
  constants — swap the artifact as a unit, never mix constants across versions.
- Tests: `cd inference && /opt/anaconda3/bin/python3 -m pytest -q` (covers feature transform, batch inference,
  and online == batch consistency).

## Not included / Pending

- The **online feature pipeline** (computing the upstream features in production, live) — serving needs the
  same feature set the export produces.
- The **first-day (k=1) model** — owned elsewhere; k=1 rows are labeled `first_day` here.
- The `state -> business action` mapping and the production deployment contract.
- Full analysis: `report_stage1_player_states.md`, `report_stage2_iohmm.md`, `report_ss03_iohmm_full.md`.
