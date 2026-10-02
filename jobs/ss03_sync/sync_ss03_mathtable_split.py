"""SS03 原始数据按数学表拆分 + 版面字段打标 → S3。

对 6/9 起每个北京日的 SS03 CNY 全量 spin:
  - 保留 fct_bet_orders 除 snapshot 外全部原始列(SUPER 列 json_serialize 成字符串);
    从 snapshot 抽出 table_id / script_id 两列(snapshot 本体太大,丢弃)。
  - 用 reelset_layout_lookup 索引把 script_id → 版面字段(scatter_count/steps/total_wild/
    used_wild/total_golden/used_golden/reelset_payout + payout_bucket/side/record_uuid)。
  - 按 snapshot.table_id 分区,每(表×天)一个 parquet 传 S3。

口径(标准):game_id='SS03' AND status='COMPLETED' AND currency_type='CNY'
            AND op_code NOT IN ('B26','TST','TSB','TSO')。北京日 = (created_at+8h)::date。
S3: s3://bituslabs-team-ai/SS03_mathtable_split/table=<table_id>/dt=YYYY-MM-DD/part.parquet
本地镜像: data/ss03_mathtable_split/table=<table_id>/dt=.../part.parquet
可续传: 每天完成后写 _done/dt=<date>.done 标记,重跑跳过已完成的天。

用法:
  python3 jobs/ss03_sync/sync_ss03_mathtable_split.py                 # 6/9→9/14 全量, 传S3
  python3 jobs/ss03_sync/sync_ss03_mathtable_split.py --start 2026-09-14 --end 2026-09-14 --local-only
"""
from __future__ import annotations
import argparse, sys, io
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402
from jobs.ss03_analysis.reelset_layout_lookup import load_index, LAYOUT_FIELDS  # noqa: E402

BUCKET = "bituslabs-team-ai"; PREFIX = "SS03_mathtable_split"; REGION = "us-west-2"
DATABASE = "slot-machine"
LOCAL_ROOT = ROOT / "data" / "ss03_mathtable_split"
BASE_WHERE = ("t.game_id='SS03' AND t.status='COMPLETED' AND t.currency_type='CNY' "
              "AND t.op_code NOT IN ('B26','TST','TSB','TSO')")
BJ = "(t.created_at + interval '8 hours')::date"
PAYOUT_BINS = [-1, 0, 5, 20, 100, 500, 10**18]
PAYOUT_LABELS = ["0", "1-5", "6-20", "21-100", "101-500", "500+"]


def build_projection(be) -> str:
    """除 snapshot 外全部列;SUPER 列 json_serialize;附抽取的 table_id/script_id。"""
    cols = be.execute("""SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fct_bet_orders' ORDER BY ordinal_position""")
    parts = []
    for name, dtype in cols:
        if name == "snapshot":
            continue
        if dtype == "super":
            parts.append(f'json_serialize(t."{name}")::varchar AS "{name}"')
        else:
            parts.append(f't."{name}"')
    parts.append('t."snapshot"."table_id"::varchar AS snap_table_id')
    parts.append('t."snapshot"."script_id"::varchar AS snap_script_id')
    return ",\n  ".join(parts)


def enrich(df: pd.DataFrame, idx: pd.DataFrame) -> pd.DataFrame:
    take = ["side", "record_uuid"] + LAYOUT_FIELDS      # payout 在 LAYOUT_FIELDS 里
    j = df.join(idx[take], on="snap_script_id")
    j = j.rename(columns={"side": "reelset_side", "payout": "reelset_payout"})
    j["layout_hit"] = j["reelset_side"].notna()
    j["payout_bucket"] = pd.cut(j["reelset_payout"], PAYOUT_BINS, labels=PAYOUT_LABELS)
    return j


def s3():
    return boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 5, "mode": "standard"}))


def exists(cli, key) -> bool:
    try:
        cli.head_object(Bucket=BUCKET, Key=key); return True
    except ClientError:
        return False


def run(start: date, end: date, local_only: bool):
    idx = load_index(); print(f"版面索引 {len(idx):,} 行")
    cli = None if local_only else s3()
    be = rs.RedshiftBackend(database=DATABASE, bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)
    proj = build_projection(be)
    d = start
    while d <= end:
        ds = d.isoformat()
        done_key = f"{PREFIX}/_done/dt={ds}.done"
        if not local_only and exists(cli, done_key):
            print(f"[{ds}] 已完成,跳过"); d += timedelta(days=1); continue
        sql = f"SELECT\n  {proj}\nFROM public.fct_bet_orders t\nWHERE {BASE_WHERE} AND {BJ} = '{ds}'"
        df = be.query_to_df(sql)
        if df is None or not len(df):
            print(f"[{ds}] 无数据"); d += timedelta(days=1); continue
        df["bet_date"] = ds
        j = enrich(df, idx)
        j["snap_table_id"] = j["snap_table_id"].fillna("unknown")
        hit = j["layout_hit"].mean() * 100
        ntab = 0
        for tbl, g in j.groupby("snap_table_id"):
            safe = str(tbl).replace("/", "_")
            key = f"{PREFIX}/table={safe}/dt={ds}/part.parquet"
            lp = LOCAL_ROOT / f"table={safe}" / f"dt={ds}" / "part.parquet"
            lp.parent.mkdir(parents=True, exist_ok=True)
            g.drop(columns=["layout_hit"]).to_parquet(lp, index=False, compression="snappy")
            if not local_only:
                cli.upload_file(str(lp), BUCKET, key)
                sz = cli.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
                if sz != lp.stat().st_size:
                    raise RuntimeError(f"字节不符 {key}: s3={sz} local={lp.stat().st_size}")
            ntab += 1
        if not local_only:
            cli.put_object(Bucket=BUCKET, Key=done_key, Body=b"")
        print(f"[{ds}] {len(j):,} 行 → {ntab} 表分区, 打标命中 {hit:.3f}%{' (仅本地)' if local_only else ' → S3'}")
        d += timedelta(days=1)
    be.close()
    print("完成。")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-09"); ap.add_argument("--end", default="2026-09-14")
    ap.add_argument("--local-only", action="store_true")
    a = ap.parse_args()
    run(date.fromisoformat(a.start), date.fromisoformat(a.end), a.local_only)
