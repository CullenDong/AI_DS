"""SS03 剧本版面查表 —— 把 snapshot.script_id 对应到剧本库 v2_6 的版面属性。

查表逻辑(详见 docs/SS03_剧本版面查表逻辑_v0.1.md):
  script_id --(script_id_map.json: by_uuid=raw / by_script_id=uuid5)--> 记录uuid
            --(catalog: 记录uuid → 版面属性)--> side/payout/steps/scatter_count/
              free_triggered/total_wild/used_wild/total_golden/used_golden

关键:catalog 的 uuid ≠ Redshift 的 script_id(uuid5 方案表对不上),**必须走 map**,别直接
拿 catalog uuid 等于 script_id、也别自己重算 uuid5(以官方 map 为准)。

一次性把「script_id → 版面属性」烘成 parquet 索引,之后分析直接 join 它。

用法:
  # 建/刷新索引(读 catalog + map,约几秒):
  python3 jobs/ss03_analysis/reelset_layout_lookup.py --build
  # 代码里用:
  from jobs.ss03_analysis.reelset_layout_lookup import load_index, enrich
  idx = load_index()                       # DataFrame,以 script_id 为索引
  spins = enrich(spins, idx, sid_col="sid")# 给 spins 表按 script_id 贴版面属性列
"""
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DB_DIR = ROOT / "data" / "input" / "SS03_MathTable_Database_v2_6"
CATALOGS = {"BG": DB_DIR / "BG_catalog.json", "FG": DB_DIR / "FG_catalog.json"}
MAP_FILE = DB_DIR / "script_id_map.json"
INDEX_PARQUET = ROOT / "data" / "output" / "ss03_reelset_layout_index.parquet"

# catalog 里要取的版面字段(顺序固定)
LAYOUT_FIELDS = ["payout", "steps", "scatter_count", "free_triggered",
                 "total_wild", "used_wild", "total_golden", "used_golden"]


def _meta_by_uuid() -> dict:
    """catalog: 记录uuid -> (side,)+版面字段元组。"""
    meta = {}
    for side, path in CATALOGS.items():
        cat = json.load(open(path))
        for _payout, bucket in cat["payouts"].items():
            for e in bucket["reelsets"]:
                meta[e["uuid"]] = (side,) + tuple(e.get(f) for f in LAYOUT_FIELDS)
        del cat
    return meta


def build_index(save: bool = True) -> pd.DataFrame:
    """走官方 map + catalog,建 script_id -> 版面属性 索引(raw 与 uuid5 两方案都收)。"""
    meta = _meta_by_uuid()
    m = json.load(open(MAP_FILE))
    by_uuid, by_sid = m["by_uuid"], m["by_script_id"]
    rows, miss = [], 0
    for u in by_uuid:                          # raw 方案:记录uuid 本身即合法 script_id
        md = meta.get(u)
        if md is None:
            miss += 1; continue
        rows.append((u, "raw", u) + md)
    for sid, uus in by_sid.items():            # uuid5 方案:script_id -> 记录uuid
        md = meta.get(uus[0])
        if md is None:
            miss += 1; continue
        rows.append((sid, "uuid5", uus[0]) + md)
    del m, by_uuid, by_sid
    cols = ["script_id", "scheme", "record_uuid", "side"] + LAYOUT_FIELDS
    df = pd.DataFrame(rows, columns=cols)
    for c in LAYOUT_FIELDS:
        df[c] = pd.to_numeric(df[c], downcast="integer")
    if save:
        INDEX_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(INDEX_PARQUET, index=False)
    print(f"索引 {len(df):,} 行(raw+uuid5);catalog 缺元数据 {miss};→ {INDEX_PARQUET if save else '(未落盘)'}")
    return df


def load_index() -> pd.DataFrame:
    """载入烘好的索引,以 script_id 为 index。缺失则自动 build。"""
    if not INDEX_PARQUET.exists():
        build_index(save=True)
    return pd.read_parquet(INDEX_PARQUET).set_index("script_id")


def enrich(spins: pd.DataFrame, idx: pd.DataFrame | None = None, sid_col: str = "script_id",
           cols: list[str] | None = None) -> pd.DataFrame:
    """给 spins 表(含 script_id 列)左连版面属性列。未命中的行属性为 NaN。"""
    if idx is None:
        idx = load_index()
    take = cols or (["side", "record_uuid"] + LAYOUT_FIELDS)
    return spins.join(idx[take], on=sid_col)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="读 catalog+map 重建索引 parquet")
    a = ap.parse_args()
    if a.build:
        build_index(save=True)
    else:
        idx = load_index()
        print(f"载入索引 {len(idx):,} 行;列: {list(idx.columns)}")
        print(idx.head(3).to_string())
