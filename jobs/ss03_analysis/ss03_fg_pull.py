"""SS03 近 14 天全体 CNY 玩家 order 数据拉取（FG 三档门槛重核用）。

过滤：game_id='SS03'、bet_amount>0（正常投注，排除 FREE/免费轮）、CNY、status COMPLETED、剔测试单。
导出：data/output/ss03_orders_14d.parquet —— 保留 user_id, created_at(时间), bet_amount, math_table_id(table_id), trigger_type。
同时计算：DAU（每日活跃）、BET 分位(P25/50/75/90)、自然 FG 触发频率、UV14（14 天去重用户）。
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output" / "ss03_orders_14d.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5443)

# 1) 探列名（currency vs currency_type、op_code/status/trigger_type 是否存在）
cols = {r[0] for r in be.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='fct_bet_orders'")}
cur_col = "currency_type" if "currency_type" in cols else ("currency" if "currency" in cols else None)
has_status = "status" in cols
has_op = "op_code" in cols
has_trig = "trigger_type" in cols
print("检测列: currency=", cur_col, "status=", has_status, "op_code=", has_op, "trigger_type=", has_trig)

# 2) 数据最大日 → 近 14 天窗口
maxd = pd.Timestamp(be.execute("SELECT MAX(created_at) FROM public.fct_bet_orders WHERE game_id='SS03'")[0][0])
end = maxd.normalize() + pd.Timedelta(days=1)          # 排他上界（含 maxd 当天）
start = end - pd.Timedelta(days=14)                     # 近 14 天
print(f"窗口(created_at): {start} ~ {end}（14 天）")

# 3) 正常投注过滤
W = [f"game_id='SS03'", "bet_amount>0",
     f"created_at>='{start}'", f"created_at<'{end}'"]
if cur_col: W.append(f"{cur_col}='CNY'")
if has_status: W.append("status='COMPLETED'")
if has_op: W.append("op_code NOT IN ('B26','TST','TSB','TSO')")
WHERE = " AND ".join(W)

# 4) 导出 order 表（精简列：时间 + table_id + 计算所需）
sel = "user_id, created_at, bet_amount, math_table_id" + (", trigger_type" if has_trig else "")
df = be.query_to_df(f"SELECT {sel} FROM public.fct_bet_orders WHERE {WHERE}")
df.to_parquet(OUT, index=False, compression="snappy")
print(f"\n导出 {len(df):,} 行 -> {OUT}")

# 5) 指标
df["created_at"] = pd.to_datetime(df["created_at"])
df["d"] = df["created_at"].dt.date
df["bet_amount"] = df["bet_amount"].astype(float)
dau = df.groupby("d")["user_id"].nunique()
print("\n=== DAU（每日活跃玩家，bet>0）===")
print(dau.to_string())
print(f"  DAU 均值 {dau.mean():.0f}")
print(f"\n=== UV14（14 天去重用户）= {df['user_id'].nunique():,} ===")
q = df["bet_amount"].quantile([.25, .5, .75, .90])
print("\n=== BET 分布（每笔投注额 bet_amount 分位）===")
for p in [.25, .5, .75, .90]:
    print(f"  P{int(p*100)} = {q[p]:.2f}")
print(f"  均值 {df['bet_amount'].mean():.3f}")
if has_trig:
    tv = df["trigger_type"].fillna("(空)").replace("", "(空)").value_counts()
    print("\n=== trigger_type 取值分布（判定自然 FG）===")
    print(tv.to_string())
    trig_all = df["trigger_type"].notna() & (df["trigger_type"].astype(str).str.strip() != "")
    natural = trig_all & (df["trigger_type"].astype(str).str.upper() != "DIRECT_PURCHASED")
    print(f"\n=== FG 触发频率（占 bet>0 spin）===")
    print(f"  含触发(任意 trigger_type 非空): {trig_all.mean()*100:.3f}%")
    print(f"  自然触发(排除 DIRECT_PURCHASED): {natural.mean()*100:.3f}%")
be.close()
