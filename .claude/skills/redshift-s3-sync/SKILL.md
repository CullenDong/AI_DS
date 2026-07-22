---
name: redshift-s3-sync
description: >
  Extract SS03 (or other fct_bet_orders) cohorts from Redshift and sync them to
  s3://bituslabs-team-ai/SS03_raw_data/ as day-partitioned parquet, one folder per
  cohort. Use when the user asks to pull a player group / AB-test / math-table
  cohort from Redshift, upload SS03 data to S3, or add a new cohort folder.
---

# Redshift → S3 cohort sync (SS03 fct_bet_orders)

Existing, tested stack — extend it, don't rewrite:
- `tools/db/redshift.py` — read-only Redshift backend (blocks INSERT/UPDATE/…), SSH bastion
  tunnel via paramiko. Config from `.env` (auto-loaded at import).
- `jobs/sync_ss03_s3.py` — whole-table day sync (`--upload-only`, `--purge-local`).
- `jobs/sync_ss03_cohorts.py` — **config-driven cohort sync; add new cohorts to its
  `COHORTS` list** (folder / start / end / extra_where).
- `jobs/sync_ss03_ai_groups.py` — example of a second job running in PARALLEL on its own
  tunnel port.

## Prerequisites
`.env` in repo root (gitignored): `REDSHIFT_USER`, `REDSHIFT_PASSWORD`,
`BASTION_KEY_PATH=~/.ssh/oceanhunter-prod-bastion-ec2.pem`. Defaults in tools/db/redshift.py:
host `production-redshift-cluster...ap-southeast-1:5439`, bastion `13.215.212.244`.
Deps: `redshift_connector paramiko python-dotenv boto3 pyarrow`. AWS creds in
`~/.aws/credentials` must write `bituslabs-team-ai` (us-west-2).

## Workflow for a new cohort

1. **Discovery query first — never trust remembered IDs.** Users describe groups fuzzily
   ("wy开头的那个", "AI组"). Resolve the exact `partition_ab[0]` and `math_table_id` with a
   quick aggregate before syncing, e.g.:
   ```sql
   SELECT t.partition_ab[0]::varchar AS grp, t.math_table_id, count(*) AS n
   FROM public.fct_bet_orders t
   WHERE t.game_id='SS03' AND t.status='COMPLETED' AND t.currency_type IN ('CNY')
     AND t.op_code NOT IN ('B26','TST','TSB','TSO')
     AND t.created_at >= '<start>' AND t.created_at < '<end+1>'
     AND t.partition_ab[0]::varchar LIKE 'wy%'
   GROUP BY 1,2 ORDER BY 3 DESC
   ```
   Gotcha: `partition_ab` is SUPER — equality works bare (`t.partition_ab[0] = '...'`),
   but `LIKE` needs `::varchar`. Show the user what tables exist (e.g. `normal_zero` vs
   `normal_kakuteiB`) so exclusions are explicit.

2. **Add to `COHORTS` in jobs/sync_ss03_cohorts.py**:
   - `folder`: descriptive summary the user can read back — group + table + date span,
     e.g. `AI_group_4.2-4.22`, `AB_TEST_B_normal_zero_95_kai_6.10-7.8`,
     `default_normal_zero_6.10-7.5`.
   - `extra_where`: the cohort filters ON TOP of the base filters (game_id/COMPLETED/
     CNY/op_code exclusions live in `BASE_WHERE`). Keep the user's SQL semantics verbatim.
   - Dates are inclusive; day slicing is `created_at >= d AND < d+1` (UTC).

3. **Run** `python3 jobs/sync_ss03_cohorts.py` in the background. It is resumable: any
   day already on S3 (size>0) is skipped, so re-running after edits/crashes is safe and
   previously-finished cohorts cost only fast head-object checks. Redshift connects
   lazily (no tunnel if everything is already synced). Each day: pull → local parquet →
   upload → **byte-size verify** (raises on mismatch).

4. **Parallel jobs**: the SSH tunnel binds a fixed local port. Two sync processes must
   use different ports — construct `RedshiftBackend(..., local_port=5434/5435)` directly
   (see sync_ss03_ai_groups.py). Same cluster/S3 are fine concurrently.

5. **Verify at the end** (day counts + local-vs-S3 byte equality + row totals per folder)
   and report a table to the user.

## Layout & naming

```
s3://bituslabs-team-ai/SS03_raw_data/<cohort_folder>/dt=YYYY-MM-DD/part.parquet
local mirror: data/ss03/<cohort_folder>/dt=YYYY-MM-DD/part.parquet   (data/ gitignored)
```

## Known IDs (verify with discovery before reuse — routing changes over time)
- AI 组: `jojpin-9mokha-rexQug`
- AB_TEST_A: `4a04df21-c749-4808-8e55-3a0b74c084d2` (math `normal_Zero_BG97_Saitekika_BGadj`)
- AB_TEST_B: `4f1a46ca-7baa-4452-9a40-ef21d9b33b57` (math `normal_zero_95_kai`)
- default 组 (wy开头): `wytsuj-fothap-5Qixda` (math `normal_zero`; 同组还有 `normal_kakuteiB`)

## Performance & schema notes
- Filtered cohort day-queries run seconds-to-minutes; a full unfiltered day of
  fct_bet_orders was ~670k rows / ~6 min through the tunnel. Expect hours for
  multi-cohort backfills — always background the run.
- fct_bet_orders is spin-level with `spin_id / root_spin_id / parent_spin_id / bet_type /
  expected_payout / actual_payout` — unlike the older bet-level exports, this parent/child
  structure likely supports a TRUE base-game vs free-spin split (cf. game-bet-eda skill).
- Never echo `.env` secrets into the chat; don't commit `.env` (gitignored).
