"""Sync three SS03 cohort extracts from Redshift to S3, one folder per cohort.

Layout: s3://bituslabs-team-ai/SS03_raw_data/<cohort_folder>/dt=YYYY-MM-DD/part.parquet
Local mirror: data/ss03/<cohort_folder>/dt=.../part.parquet
Resumable: days already on S3 (size>0) are skipped.

Cohorts (user-specified):
  1. AI_group_4.2-4.22                      — AI 组, 2026-04-02..04-22, 全部数学表
  2. AB_TEST_B_normal_zero_95_kai_6.10-7.8  — AB_TEST_B, 2026-06-10..07-08, 只 normal_zero_95_kai
  3. AB_TEST_A_normal_Zero_BG97_Saitekika_BGadj_6.10-7.8 — AB_TEST_A, 同期, 只 BG97 表

Common filters: game_id='SS03', status='COMPLETED', currency CNY,
op_code NOT IN ('B26','TST','TSB','TSO'). Day slicing on created_at (UTC).
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

BASE_WHERE = (
    "t.game_id = 'SS03' AND t.status = 'COMPLETED' "
    "AND t.currency_type IN ('CNY') "
    "AND t.op_code NOT IN ('B26', 'TST', 'TSB', 'TSO')"
)

COHORTS = [
    {
        "folder": "AI_group_4.2-4.22",
        "start": date(2026, 4, 2), "end": date(2026, 4, 22),
        "extra_where": "t.partition_ab[0] = 'jojpin-9mokha-rexQug'",
    },
    {
        "folder": "AB_TEST_B_normal_zero_95_kai_6.10-7.8",
        "start": date(2026, 6, 10), "end": date(2026, 7, 8),
        "extra_where": ("t.partition_ab[0] = '4f1a46ca-7baa-4452-9a40-ef21d9b33b57' "
                        "AND t.math_table_id = 'normal_zero_95_kai'"),
    },
    {
        "folder": "AB_TEST_A_normal_Zero_BG97_Saitekika_BGadj_6.10-7.8",
        "start": date(2026, 6, 10), "end": date(2026, 7, 8),
        "extra_where": ("t.partition_ab[0] = '4a04df21-c749-4808-8e55-3a0b74c084d2' "
                        "AND t.math_table_id = 'normal_Zero_BG97_Saitekika_BGadj'"),
    },
    {   # default 组(wy开头), 只要 normal_zero (排除 normal_kakuteiB)
        "folder": "default_normal_zero_6.10-7.5",
        "start": date(2026, 6, 10), "end": date(2026, 7, 5),
        "extra_where": ("t.partition_ab[0] = 'wytsuj-fothap-5Qixda' "
                        "AND t.math_table_id = 'normal_zero'"),
    },
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
                    be = rs._get_backend(database=DATABASE, bastion_ip=rs.DEFAULT_BASTION_IP)
                nd = d + timedelta(days=1)
                sql = (
                    f"SELECT * FROM public.fct_bet_orders AS t "
                    f"WHERE {BASE_WHERE} AND {c['extra_where']} "
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
    print("ALL COHORTS DONE", flush=True)


if __name__ == "__main__":
    main()
