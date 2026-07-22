"""Sync two AI-group SS03 cohorts (Pre-v4 / Post-v4) from Redshift to S3.

Runs in PARALLEL with sync_ss03_cohorts.py: uses its own SSH tunnel on local
port 5434 (the other job holds 5433). Same layout & resume semantics:
  s3://bituslabs-team-ai/SS03_raw_data/<folder>/dt=YYYY-MM-DD/part.parquet
  local mirror data/ss03/<folder>/dt=.../part.parquet

Cohorts (AI 组 partition, all math tables):
  AI_group_v3.2_5.22-6.16 : Pre v4 (v3.2), 2026-05-22..2026-06-16 (26 天)
  AI_group_v4_6.18-7.9    : Post v4,       2026-06-18..2026-07-09
"""
from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.db import redshift as rs  # noqa: E402

BUCKET = "bituslabs-team-ai"
PREFIX = "SS03_raw_data"
REGION = "us-west-2"
DATABASE = "slot-machine"
LOCAL_ROOT = ROOT / "data" / "ss03"
LOCAL_PORT = 5434  # avoid clashing with the cohort job's 5433 tunnel

BASE_WHERE = (
    "t.game_id = 'SS03' AND t.status = 'COMPLETED' "
    "AND t.currency_type IN ('CNY') "
    "AND t.op_code NOT IN ('B26', 'TST', 'TSB', 'TSO')"
)
AI_WHERE = "t.partition_ab[0] = 'jojpin-9mokha-rexQug'"

COHORTS = [
    {"folder": "AI_group_v3.2_5.22-6.16", "start": date(2026, 5, 22), "end": date(2026, 6, 16)},
    {"folder": "AI_group_v4_6.18-7.9", "start": date(2026, 6, 18), "end": date(2026, 7, 9)},
]


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def s3_size(s3, key: str):
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def main() -> None:
    s3 = boto3.client("s3", region_name=REGION,
                      config=Config(retries={"max_attempts": 5, "mode": "adaptive"}))
    be = None
    for c in COHORTS:
        days = list(daterange(c["start"], c["end"]))
        print(f"=== {c['folder']}: {c['start']}..{c['end']} ({len(days)} days) ===", flush=True)
        for i, d in enumerate(days, 1):
            key = f"{PREFIX}/{c['folder']}/dt={d.isoformat()}/part.parquet"
            if s3_size(s3, key):
                print(f"[{i}/{len(days)}] {d} on S3, skip", flush=True)
                continue
            local = LOCAL_ROOT / c["folder"] / f"dt={d.isoformat()}" / "part.parquet"
            if not local.exists():
                if be is None:
                    be = rs.RedshiftBackend(database=DATABASE,
                                            bastion_ip=rs.DEFAULT_BASTION_IP,
                                            local_port=LOCAL_PORT)
                nd = d + timedelta(days=1)
                sql = (
                    f"SELECT * FROM public.fct_bet_orders AS t "
                    f"WHERE {BASE_WHERE} AND {AI_WHERE} "
                    f"AND t.created_at >= '{d.isoformat()} 00:00:00' "
                    f"AND t.created_at < '{nd.isoformat()} 00:00:00' "
                    f"ORDER BY t.user_id, t.spin_id, t.created_at"
                )
                t0 = time.time()
                df = be.query_to_df(sql)
                local.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(local, index=False, compression="snappy")
                print(f"[{i}/{len(days)}] {d} pulled rows={len(df):,} ({time.time()-t0:.0f}s)", flush=True)
            sz = local.stat().st_size
            s3.upload_file(str(local), BUCKET, key)
            up = s3_size(s3, key)
            if up != sz:
                raise RuntimeError(f"{c['folder']}/{d}: size mismatch local={sz} s3={up}")
            print(f"[{i}/{len(days)}] {d} uploaded {sz:,} B", flush=True)
    if be is not None:
        be.close()
    print("AI GROUPS DONE", flush=True)


if __name__ == "__main__":
    main()
