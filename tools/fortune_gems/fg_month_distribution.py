"""Fortune Gems Jan multiplier distribution, Normal vs Extra (v6: RTP-balanced).

Constraint: Normal must contain NO recurring-decimal multipliers. So every ambiguous-tier
row whose multiplier is a recurring decimal (m not a multiple of 0.2 ⇒ provably Extra) is
forced 100% into Extra. The free lever is r = the fraction of the NON-recurring ambiguous
rows (losses + on-lattice wins, all multiples of 0.2) sent to Extra; the rest go to Normal.

We then SOLVE r so that Normal RTP = Extra RTP (as close as possible). As r:0→1 Extra RTP
falls (diluted by non-recurring losses) and Normal RTP rises, so a balancing r exists in
[0,1] (else we clamp to the closest endpoint).

Single pass accumulates four per-multiplier pools (pure-normal, pure-extra, ambiguous-
recurring, ambiguous-nonrecurring) so r can be solved and the distributions assembled
without re-reading the 2.5B rows.

m = payout/bet; payout = bet_amount + cus_account; m=0 (losses) included.
Outputs: data/output/fortune_gems_2024-01_distribution.json (+ _report.md)
"""
from __future__ import annotations
import glob, json, warnings
from collections import defaultdict
import numpy as np, pyarrow.parquet as pq
warnings.filterwarnings("ignore")

ROOT = "data/fortune_gems/year=2024/month=01"
OUT_JSON = "data/output/fortune_gems_2024-01_distribution.json"
OUT_MD = "data/output/fortune_gems_2024-01_distribution_report.md"


def recur_mask(m):
    return (m > 0) & (np.abs(m * 5 - np.round(m * 5)) > 1e-4)  # not a multiple of 0.2


def classify_tiers():
    sample = [sorted(glob.glob(f"{ROOT}/day={d:02d}/*"))[0] for d in range(1, 32)]
    agg = defaultdict(lambda: [0, 0])
    for f in sample:
        d = pq.ParquetFile(f).read(columns=["bet_amount", "cus_account"]).to_pandas()
        d = d[d.bet_amount > 0]
        d["r"] = recur_mask(((d.bet_amount + d.cus_account) / d.bet_amount).values)
        for stake, gg in d.groupby("bet_amount"):
            a = agg[round(float(stake), 4)]; a[0] += len(gg); a[1] += int(gg.r.sum())
    rate = {s: (v[1] / v[0] if v[0] else 0) for s, v in agg.items()}
    pe = float(np.mean([r for r in rate.values() if r > 0.04]))
    role = {}
    for s, r in rate.items():
        role[s] = "normal" if r < 0.01 else ("extra" if r > 0.85 * pe else "amb")
    return role, rate, pe


def add(acc, tot, df, key):
    if not len(df):
        return
    g = df.groupby("m").agg(c=("m", "size"), sb=("bet_amount", "sum"), sp=("pay", "sum"))
    for mv, r in g.iterrows():
        a = acc[key][mv]; a[0] += r.c; a[1] += r.sb; a[2] += r.sp
    tot[key][0] += len(df); tot[key][1] += df.bet_amount.sum(); tot[key][2] += df.pay.sum()


def main():
    role, rate, pe = classify_tiers()
    amb_tiers = sorted([s for s in role if role[s] == "amb"])
    print(f"pure-Extra recur≈{pe:.4f} | amb tiers={amb_tiers}", flush=True)

    files = sorted(glob.glob(f"{ROOT}/day=*/*"))
    keys = ("pn", "pe", "rec", "nr")  # pure-normal, pure-extra, amb-recurring, amb-nonrecurring
    acc = {k: defaultdict(lambda: [0.0, 0.0, 0.0]) for k in keys}
    tot = {k: [0.0, 0.0, 0.0] for k in keys}
    n_rows = 0
    for i, f in enumerate(files, 1):
        d = pq.ParquetFile(f).read(columns=["bet_amount", "cus_account"]).to_pandas()
        d = d[d.bet_amount > 0]
        d["pay"] = d.bet_amount + d.cus_account
        d["m"] = np.round(d.pay / d.bet_amount, 6)
        d["role"] = d.bet_amount.map(lambda s: role.get(round(float(s), 4), "normal"))
        n_rows += len(d)
        add(acc, tot, d[d.role == "normal"], "pn")
        add(acc, tot, d[d.role == "extra"], "pe")
        amb = d[d.role == "amb"]
        if len(amb):
            rec = recur_mask(amb.m.values)
            add(acc, tot, amb[rec], "rec")
            add(acc, tot, amb[~rec], "nr")
        if i % 60 == 0 or i == len(files):
            print(f"[{i}/{len(files)}] rows={n_rows:,}", flush=True)

    # ----- solve r so Normal RTP == Extra RTP -----
    PNb, PNp = tot["pn"][1], tot["pn"][2]
    PEb, PEp = tot["pe"][1], tot["pe"][2]
    RECb, RECp = tot["rec"][1], tot["rec"][2]
    NRb, NRp = tot["nr"][1], tot["nr"][2]

    def n_rtp(r): return (PNp + (1 - r) * NRp) / (PNb + (1 - r) * NRb)
    def e_rtp(r): return (PEp + RECp + r * NRp) / (PEb + RECb + r * NRb)
    def gap(r): return n_rtp(r) - e_rtp(r)

    lo, hi = 0.0, 1.0
    if gap(lo) * gap(hi) > 0:  # no sign change -> clamp to closest
        r_star = lo if abs(gap(lo)) < abs(gap(hi)) else hi
    else:
        for _ in range(80):
            mid = (lo + hi) / 2
            if gap(lo) * gap(mid) <= 0: hi = mid
            else: lo = mid
        r_star = (lo + hi) / 2
    print(f"solved r*={r_star:.5f} | normal_rtp={n_rtp(r_star):.5f} extra_rtp={e_rtp(r_star):.5f}", flush=True)

    # ----- assemble distributions with r_star -----
    def merge(*pairs):
        out = defaultdict(lambda: [0.0, 0.0, 0.0])
        for ad, w in pairs:
            for mv, (c, sb, sp) in ad.items():
                o = out[mv]; o[0] += c * w; o[1] += sb * w; o[2] += sp * w
        return out
    norm_acc = merge((acc["pn"], 1.0), (acc["nr"], 1 - r_star))
    extr_acc = merge((acc["pe"], 1.0), (acc["rec"], 1.0), (acc["nr"], r_star))

    def section(a, with_base=False):
        spins = sum(v[0] for v in a.values()); bet = sum(v[1] for v in a.values()); pay = sum(v[2] for v in a.values())
        dist = []
        for mv in sorted(a):
            c, sb, sp = a[mv]
            row = {"multiplier": round(float(mv), 6), "count": round(c, 2),
                   "pct_of_spins": round(c / spins * 100, 6) if spins else 0,
                   "pct_of_payout": round(sp / pay * 100, 6) if pay else 0}
            if with_base:
                row["base_multiplier"] = round(float(mv) * 1.5, 6)
            dist.append(row)
        loss = a.get(0.0, [0, 0, 0])
        rec = sum(1 for mv in a if abs(mv * 5 - round(mv * 5)) > 1e-4)
        return {"spins": round(spins, 1), "total_bet": round(bet, 2), "total_payout": round(pay, 2),
                "rtp": round(pay / bet, 6) if bet else None,
                "hit_rate_pct": round((spins - loss[0]) / spins * 100, 4) if spins else 0,
                "distinct_multipliers": len(a), "recurring_decimal_multipliers": rec,
                "max_multiplier": round(float(max(a)), 6) if a else None, "distribution": dist}

    normal, extra = section(norm_acc), section(extr_acc, with_base=True)
    gb = normal["total_bet"] + extra["total_bet"]; gp = normal["total_payout"] + extra["total_payout"]
    out = {
        "dataset": "fortune_gems year=2024 month=01 (day=01..31)",
        "total_spins": n_rows, "total_bet": round(gb, 2), "total_payout": round(gp, 2), "rtp": round(gp / gb, 6),
        "multiplier_def": "m = payout/bet_amount; payout = bet_amount + cus_account; 6dp; m=0=losses included.",
        "method": ("Ambiguous tiers (data-detected by recurring-decimal rate): recurring-decimal rows → 100%% "
                   "Extra (so Normal has NONE); non-recurring rows split with a single residual rate r SOLVED so "
                   "Normal RTP = Extra RTP. r is no longer the fingerprint share — it is the RTP-balancing rate."),
        "ambiguous_tiers": [str(t) for t in amb_tiers],
        "solved_residual_rate_r": round(r_star, 5),
        "rtp_gap": round(normal["rtp"] - extra["rtp"], 6),
        "extra_share_of_bet_pct": round(extra["total_bet"] / gb * 100, 4),
        "extra_share_of_payout_pct": round(extra["total_payout"] / gp * 100, 4),
        "normal": normal, "extra": extra,
    }
    with open(OUT_JSON, "w") as fp:
        json.dump(out, fp, ensure_ascii=False, indent=2)

    def topp(sec, k=8):
        return sorted(sec["distribution"], key=lambda x: -x["pct_of_payout"])[:k]
    md = [f"# Fortune Gems 2024-01 倍率分布 (Normal vs Extra, v6 RTP-balanced)\n",
          f"全月 {n_rows:,} 旋转 | 总投注 {gb:,.0f} | 整体RTP {out['rtp']}\n",
          f"\n## 方法\n循环小数行→100%Extra(保证Normal无循环小数);非循环小数行按单一残余率 r 分配,**r 解出使 Normal RTP = Extra RTP**。",
          f"解得 **r* = {r_star:.5f}**,RTP 差 = {out['rtp_gap']}。\n",
          "\n## 两段概览",
          "| 段 | 旋转 | RTP | 命中率 | 不同倍率 | 含循环小数倍率 | 最大倍率 |",
          "|---|---|---|---|---|---|---|",
          f"| Normal | {normal['spins']:,.0f} | {normal['rtp']} | {normal['hit_rate_pct']}% | {normal['distinct_multipliers']} | **{normal['recurring_decimal_multipliers']}** | {normal['max_multiplier']} |",
          f"| Extra | {extra['spins']:,.0f} | {extra['rtp']} | {extra['hit_rate_pct']}% | {extra['distinct_multipliers']} | {extra['recurring_decimal_multipliers']} | {extra['max_multiplier']} |",
          f"\n→ **Normal 循环小数倍率 = {normal['recurring_decimal_multipliers']}(应为0)** | **Normal RTP {normal['rtp']} ≈ Extra RTP {extra['rtp']}**",
          f"Extra 占总投注 {out['extra_share_of_bet_pct']}% / 总派彩 {out['extra_share_of_payout_pct']}%。",
          "\n## Normal 对派彩贡献Top", "| m | 占派彩% | 占旋转% |", "|---|---|---|"]
    md += [f"| {r['multiplier']} | {r['pct_of_payout']:.3f} | {r['pct_of_spins']:.4f} |" for r in topp(normal)]
    md += ["\n## Extra 对派彩贡献Top(含基础倍率=m×1.5)", "| m | base(m×1.5) | 占派彩% | 占旋转% |", "|---|---|---|---|"]
    md += [f"| {r['multiplier']} | {r['base_multiplier']} | {r['pct_of_payout']:.3f} | {r['pct_of_spins']:.4f} |" for r in topp(extra)]
    md += [f"\n> r* 由 RTP 平衡解出(非指纹占比);循环小数恒在Extra故Normal绝无循环小数。逐笔真相需上游 extra_bet 字段。"]
    with open(OUT_MD, "w") as fp:
        fp.write("\n".join(md))
    print(f"DONE r*={r_star:.5f} normal_rtp={normal['rtp']} extra_rtp={extra['rtp']} "
          f"normal_recurring={normal['recurring_decimal_multipliers']} gap={out['rtp_gap']}", flush=True)


if __name__ == "__main__":
    main()
