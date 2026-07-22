---
name: game-bet-eda
description: >
  Sync a JILI/Bitus slot game's bet-level Parquet data from S3 and run EDA that
  maps the data to the game's mechanics (RTP, hit rate, win-multiplier lattice,
  Extra Bet detection, max-win cap, volatility). Use when the user wants to pull
  a game (e.g. superace, fortune_gems) from S3 into data/ and analyze it, or
  connect bet-level rows to documented game rules.
---

# Game bet-level data: sync + EDA + rule mapping

Repeatable workflow for the Bitus player-AI slot datasets. Two S3 buckets seen so far,
both in **us-west-2**:
- `bituslabs-team-ai` → flat prefix e.g. `superace/2024-01-01/`
- `bituslabs-tsplayerai` → Hive partitions e.g. `fortune_gems/year=2024/month=01/day=01/`

Auth: an IAM identity is configured in `~/.aws/credentials` (`[default]`). If missing,
ask the user; prefer SSO temp creds over pasting long-term `AKIA…` keys (those leak into
the transcript — tell the user to rotate any that get pasted).

## 1. Sync from S3 (mirror exactly, then verify)

Goal: local layout mirrors the S3 prefix; object names are kept **verbatim (no `.parquet`
suffix)**; verify by name **and** byte size.

- List + get region first: `get_bucket_location` (us-west-2 here). One day-partition ≈ 30
  objects, 3–7 GB. These are Trino/Athena exports named `<ts>_<hash>_<uuid>`, no extension,
  but content is Parquet (`PAR1` magic).
- **TCC gotcha:** files freshly downloaded to `~/Downloads` carry `com.apple.quarantine` +
  `com.apple.provenance`; the sandboxed shell gets `Operation not permitted` on `cp` even
  with sandbox disabled. Fix: ask the user to move it via Finder drag, or a **real Terminal**
  `mv` (the `!`-prefix only works if `!` is the very first char of their message, no leading
  spaces). S3 downloads via boto3 are unaffected.
- Download concurrently (8 threads), into the mirrored dest dir, keeping `key.split('/')[-1]`.
  Run in the background for multi-GB pulls.

Verify (must print all-zero diffs):
```python
import boto3, os
b, prefix = 'bituslabs-tsplayerai', 'fortune_gems/year=2024/month=01/day=01/'
dest = 'data/fortune_gems/year=2024/month=01/day=01'
s3 = boto3.client('s3', region_name='us-west-2')
remote = {o['Key'].split('/')[-1]: o['Size']
          for p in s3.get_paginator('list_objects_v2').paginate(Bucket=b, Prefix=prefix)
          for o in p.get('Contents', []) if o['Size'] > 0}
local = {f: os.path.getsize(os.path.join(dest, f)) for f in os.listdir(dest)}
assert set(remote) == set(local) and all(remote[k] == local[k] for k in remote)
```
`data/` is gitignored — never commit the parquet.

## 2. Read the Parquet (per-file, not dataset API)

**Gotcha:** the partition col `year` is `int16` in some shards, `dictionary` in others →
`pq.read_table([files])` / dataset API throws `ArrowTypeError`. Read **per file**:
```python
import pyarrow.parquet as pq, glob, pandas as pd
files = sorted(glob.glob('data/fortune_gems/year=2024/month=01/day=01/*'))
df = pq.ParquetFile(files[0]).read().to_pandas()      # ~3M rows/file
```

## 3. Decode the bet-level schema (59 cols, mostly noise)

One row = one settled spin (`billno` is unique). Core fields + identities (hold exactly):
- `bet_amount` == `valid_account` == `remain_amount` = **total stake** (Extra Bet already folded in)
- `cus_account` = player net (win − stake);  `payout = bet_amount + cus_account`
- `bingoggr` = operator GGR = `-cus_account`
- `billtime` timestamp; `branch_name`/`city`/`platform`/`biztype` are real dims
- **Drop:** `gmcode`, `bill_attribute` (always null); `won`, `jackpot*`, `bonus_amount`,
  `ggr`(string "0") — all 0/unused. ~28 of 59 cols are constant/null per day.

Derive: `payout = bet_amount + cus_account`; **win multiplier `m = payout / bet_amount`**
(drop `bet_amount<=0` first — a few rows give inf).

## 4. EDA metrics to always compute

- **RTP** = Σpayout / Σbet  (Super Ace ≈0.968, Fortune Gems ≈0.971 — matches official ~97%)
- **Hit rate** = share of `payout>0` (Fortune Gems ≈12%, Super Ace ≈24%)
- `m` distribution: median, P90/P99/P99.9/P99.99/max; log10(m) histogram for tail/bimodality
- Dims: bet-tier counts, platform/branch/hour-of-day activity, distinct customers

## 5. Map data → game mechanics

Get the rules first (web + any user-provided spec, which is authoritative). Then:

- **Win-multiplier lattice** (games where `W_final = W_base × M`, e.g. Fortune Gems):
  build all products of {payline payouts summed over up to N lines} × {reel multipliers
  M∈set}. Check that winning `m` values land on this lattice (Fortune Gems: 100% on-lattice
  → confirms the `×M` model). Lattice membership also lets you flag likely high-M spins.
- **Extra Bet detection** (stake = BET×1.5, win still paid on base BET): Extra-Bet spins show
  `m` **off** the integer lattice but `m×1.5` **on** it (e.g. m=2.6667=4/1.5). Flag per row;
  recover `base_multiplier = m×1.5`. Cross-check: `.5`-ending bet tiers (1.5/4.5/7.5/12/30)
  carry the unambiguous Extra-Bet wins. Then compare Normal vs Extra: high-M rate should rise.
- **Max-win cap:** confirm `max(m)` hits the documented ceiling exactly with nothing above
  (Fortune Gems: max m = 375.0 exactly → defensive cap confirmed).
- **Volatility:** compare tail (CV, high quantiles) across games (Fortune Gems thin tail = Low;
  Super Ace 4500x tail = higher).

## Base game vs Free spins — row flags missing, but INFERABLE via gap signal

These bet-level tables do **not** carry a usable base/free flag (`is_free_spin` absent or
all-0; `is_special_game` all '0'; `result`/`card_list`/`bonus_amount` null/0; gmcode round
suffix always `-1`; no bet=0 rows). Free-spin winnings are **folded into the triggering paid
spin**. Games with no free spins (Fortune Gems) have no ambiguity. For games WITH free spins
(Super Ace), use the **multiplier + inter-spin-gap method** (validated 2024 PHP & 2026
CNY/USDT platforms):

- **Signal**: the client's free-spin animation delays the player's next bet. Median gap to
  next spin (same player) is ~2s for losing spins (autoplay baseline) and rises monotonically
  with win multiplier — ~70s median for m>50. The signature is intrinsic to the game client,
  stable across platforms/years.
- **Labels** (m = payout/bet; gap_s = next spin ts − this spin ts, per player):
  - `FG-strong`: m>=25 AND gap_s>=45 → trigger rate lands at official scatter ~1/200–300
    (measured 1/204 on 119M spins 2024-01-01; 1/285 on 686k spins 2026-03)
  - `FG-mid`: m>=10 AND gap_s>=30 (looser; mixes in pure cascades — report separately)
  - `BG-big-win`: m>=25 with short gap = pure base-game cascade (≈ as common as FG-strong;
    multiplier alone would misclassify ~half of big wins)
- **Key finding**: FG-strong spins carry ~25% of total payout (2024-01-01) — the free-game
  feature is ~1/4 of Super Ace's RTP.
- **Engineering (must-dos)**: a player's spins are scattered across the day's 30 Trino
  shards — computing gaps within one file inflates the baseline ~30× and destroys the
  signal; compute across ALL shards. A duckdb window over 119M rows OOMs (window can't fully
  spill): materialize `(hash(cust) c, epoch(billtime) ts, bet, payout)` into a duckdb file
  once, then window per `c % 32` bucket and merge stats with integer-second gap histograms
  (exact medians, tiny memory). ~119M rows/day fits in 6GB this way.
- Runner: `tools/superace/sa_fg_gap_analysis.py [YYYY-MM-DD ...]` → appends per-day metrics to
  `data/output/superace_fg_gap_2024-01.json`.

The exact per-row truth still needs upstream export (`is_special_game` exists in orders_2026
schema but is never populated — worth requesting).

## Extra Bet classification (Fortune Gems) + 比例反推法

Fortune Gems has an **Extra Bet** toggle: stake = base_bet × 1.5, win still paid on the base
bet. The observed bet menu = **base ∪ (1.5 × base)** where
`base = {0.1,0.5,1,2,3,5,8,10,20,50,100,200,400,500,700,1000}`.

Classify each spin by stake:
- stake ∈ base and stake/1.5 ∉ base  → **Normal** (e.g. 1,2,5,8,10,20,50,100,200,…)
- stake = 1.5×base and stake ∉ base   → **Extra** (1.5,4.5,7.5,12,15,30,75,150,300,450,600,750,1050,1500)
- **stake == 3 is the only ambiguous tier** (3∈base AND 3=2×1.5).

**Per-spin proof:** a winning spin is provably Extra iff its multiplier `m=payout/bet` is NOT
on the normal lattice but `m×1.5` IS (i.e. m = L/1.5, a fractional value like 1.333, 2.6667).
Losses (no multiplier) and on-lattice wins are unprovable.

**比例反推法 (fingerprint deconvolution) — for the ambiguous tier:** a *pure-Extra* tier shows
a fixed ~**33%** "fractional-multiplier" rate (1/3 of lattice values L give a non-integer L/1.5;
2/3 coincide with the integer lattice). So if an ambiguous tier shows an observed fractional
rate `f`, its **Extra share ≈ f / 0.33**. Fortune Gems stake=3 shows f≈11% → **Extra share ≈ 1/3**
(so stake=3 ≈ 1/3 Extra-of-2, 2/3 normal base-3). Apply the 1/3 ÷ 2/3 weight to split stake=3
rows between the Normal and Extra distributions (the other tiers are assigned 100% by stake).
This is a *population-level* estimate, not per-spin truth. The clean fix is an upstream
`extra_bet` flag (the toggle state is known server-side but not exported, like the null
`bill_attribute`).

## Output conventions

Write derived artifacts to `data/output/` (gitignored), e.g. a labeled sample with
`m / extra_bet_flag / base_multiplier / m_on_lattice`. Report in 中文 by default for this user.
