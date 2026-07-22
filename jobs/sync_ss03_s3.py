"""Sync SS03 (public.fct_bet_orders) from Redshift to S3, day-partitioned.

Pipeline per day (resumable at both levels):
  1. If s3://{bucket}/{prefix}/dt=YYYY-MM-DD/part.parquet exists (size>0) -> skip.
  2. Else if local data/ss03/dt=YYYY-MM-DD/part.parquet missing -> pull that day
     from Redshift (via tools.redshift, SSH bastion) and write it locally.
  3. Upload the local parquet to S3 and verify the uploaded size matches.

Usage:
  python3 jobs/sync_ss03_s3.py --start 2026-05-01 --end 2026-07-09
  python3 jobs/sync_ss03_s3.py --upload-only          # only push existing local days
  python3 jobs/sync_ss03_s3.py --keep-local           # don't delete local file after upload
                                                      # (default keeps local; --purge-local removes)
Requires: .env with REDSHIFT_USER/PASSWORD + BASTION_KEY_PATH (see .env.example),
and AWS credentials for the bucket (~/.aws/credentials).
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from jobs.sync_ss03 import OUT_DIR, sync_day  # noqa: E402  (reuse Redshift->local logic)
from tools.db import redshift as rs  # noqa: E402

BUCKET = "bituslabs-team-ai"
PREFIX = "SS03_raw_data"
REGION = "us-west-2"
DATABASE = "slot-machine"


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def s3_size(s3, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-05-01")
    p.add_argument("--end", default=date.today().isoformat())
    p.add_argument("--upload-only", action="store_true",
                   help="only upload existing local days; never touch Redshift")
    p.add_argument("--purge-local", action="store_true",
                   help="delete the local parquet after a verified upload")
    args = p.parse_args()

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    days = list(daterange(start, end))
    s3 = boto3.client("s3", region_name=REGION,
                      config=Config(retries={"max_attempts": 5, "mode": "adaptive"}))
    be = None  # lazy: only connect to Redshift if a day actually needs pulling

    print(f"sync {DATABASE}.fct_bet_orders {start}..{end} ({len(days)} days) "
          f"-> s3://{BUCKET}/{PREFIX}/", flush=True)
    n_skip = n_pull = n_up = 0
    for i, d in enumerate(days, 1):
        key = f"{PREFIX}/dt={d.isoformat()}/part.parquet"
        local = OUT_DIR / f"dt={d.isoformat()}" / "part.parquet"

        remote_sz = s3_size(s3, key)
        if remote_sz:
            n_skip += 1
            print(f"[{i}/{len(days)}] {d} on S3 already ({remote_sz:,} B), skip", flush=True)
            continue

        if not local.exists():
            if args.upload_only:
                print(f"[{i}/{len(days)}] {d} no local file, skip (--upload-only)", flush=True)
                continue
            if be is None:
                be = rs._get_backend(database=DATABASE, bastion_ip=rs.DEFAULT_BASTION_IP)
            t0 = time.time()
            rows, secs, _ = sync_day(be, d)
            print(f"[{i}/{len(days)}] {d} pulled from Redshift rows={rows:,} "
                  f"({time.time()-t0:.1f}s)", flush=True)
            n_pull += 1

        local_sz = local.stat().st_size
        s3.upload_file(str(local), BUCKET, key)
        up_sz = s3_size(s3, key)
        if up_sz != local_sz:
            raise RuntimeError(f"{d}: upload size mismatch local={local_sz} s3={up_sz}")
        n_up += 1
        print(f"[{i}/{len(days)}] {d} uploaded {local_sz:,} B -> s3://{BUCKET}/{key}", flush=True)
        if args.purge_local:
            local.unlink()

    if be is not None:
        be.close()
    print(f"done. uploaded={n_up} pulled_from_redshift={n_pull} skipped_on_s3={n_skip}", flush=True)


if __name__ == "__main__":
    main()
