"""Super Ace multiplier (payout/bet) distribution, split BG vs FG-trigger, 10 days.

Classes per spin (validated method, see .claude/skills/game-bet-eda):
  FG-trigger : m>=25 AND gap_to_next>=45s  (folded row = trigger spin + all free-spin wins)
  BG         : everything else (incl. big cascade wins with short gap; gap NULL -> BG)

Super Ace multipliers are quasi-continuous (1024-ways cascades), so the distribution is
reported in tiered bins (fine at low m, coarser at high m). Bin value = upper edge.

Output: data/output/superace_fg_bg_multiplier_distribution.json
"""
from __future__ import annotations
import glob, json, os, warnings
import duckdb
warnings.filterwarnings("ignore")

ROOT = "data/superace"
DAYS = [f"2024-01-{d:02d}" for d in range(1, 11)]
OUT = "data/output/superace_fg_bg_multiplier_distribution.json"
NB = 32

BIN_SQL = """CASE
  WHEN m=0 THEN 0
  WHEN m<2 THEN round(ceil(m/0.1)*0.1, 1)
  WHEN m<5 THEN round(ceil(m/0.25)*0.25, 2)
  WHEN m<10 THEN round(ceil(m/0.5)*0.5, 1)
  WHEN m<20 THEN ceil(m)
  WHEN m<50 THEN ceil(m/2)*2
  WHEN m<100 THEN ceil(m/5)*5
  WHEN m<200 THEN ceil(m/10)*10
  WHEN m<500 THEN ceil(m/25)*25
  WHEN m<1000 THEN ceil(m/50)*50
  ELSE ceil(m/100)*100 END"""

LABEL_SQL = "CASE WHEN m>=25 AND gap_s>=45 THEN 'fg' ELSE 'bg' END"


def main():
    acc = {}  # (label, bin) -> [count, sum_bet, sum_payout]
    for day in DAYS:
        dbf = f"/tmp/sa_{day}.duckdb"
        if os.path.exists(dbf):
            os.remove(dbf)
        con = duckdb.connect(dbf)
        con.execute("SET memory_limit='6GB'; SET threads=4; "
                    "SET temp_directory='/tmp/duck_spill'; SET preserve_insertion_order=false;")
        con.execute(f'''CREATE TABLE s AS
            SELECT hash(sha_customer_id) AS c, epoch(billtime)::BIGINT AS ts,
                   bet_amount::DOUBLE AS bet, (bet_amount+cus_account)::DOUBLE AS payout
            FROM read_parquet('{ROOT}/{day}/*') WHERE bet_amount > 0''')
        for b in range(NB):
            q = f'''
            WITH t AS (
              SELECT payout/bet AS m, bet, payout,
                     lead(ts) OVER (PARTITION BY c ORDER BY ts) - ts AS gap_s
              FROM s WHERE c % {NB} = {b})
            SELECT {LABEL_SQL} AS lab, {BIN_SQL} AS bin,
                   count(*) AS n, sum(bet) AS sb, sum(payout) AS sp
            FROM t GROUP BY 1, 2'''
            for lab, bn, n, sb, sp in con.execute(q).fetchall():
                k = (lab, float(bn))
                a = acc.get(k)
                if a:
                    a[0] += n; a[1] += sb; a[2] += sp
                else:
                    acc[k] = [n, float(sb), float(sp)]
        con.close(); os.remove(dbf)
        print(f"{day} done", flush=True)

    def section(lab):
        keys = sorted(k[1] for k in acc if k[0] == lab)
        spins = sum(acc[(lab, k)][0] for k in keys)
        bet = sum(acc[(lab, k)][1] for k in keys)
        pay = sum(acc[(lab, k)][2] for k in keys)
        dist = []
        for k in keys:
            c, sb, sp = acc[(lab, k)]
            dist.append({"m_bin_upper": k, "count": c,
                         "pct_of_spins": round(c / spins * 100, 6),
                         "pct_of_payout": round(sp / pay * 100, 6) if pay else 0})
        loss = acc.get((lab, 0.0), [0, 0, 0])[0]
        # quantiles of m from bins (upper-edge approximation), win-conditional
        wins = [(k, acc[(lab, k)][0]) for k in keys if k > 0]
        wtot = sum(c for _, c in wins)
        qs = {}
        cum = 0
        targets = {"p50": .5, "p90": .9, "p99": .99, "p999": .999}
        for k, c in wins:
            cum += c
            for name, t in list(targets.items()):
                if cum >= wtot * t:
                    qs[name] = k; targets.pop(name)
        return {"spins": spins, "total_bet": round(bet, 2), "total_payout": round(pay, 2),
                "rtp": round(pay / bet, 6) if bet else None,
                "hit_rate_pct": round((spins - loss) / spins * 100, 4),
                "win_multiplier_quantiles_binned": qs,
                "max_bin": keys[-1], "n_bins": len(keys), "distribution": dist}

    bg, fg = section("bg"), section("fg")
    gs = bg["spins"] + fg["spins"]; gp = bg["total_payout"] + fg["total_payout"]
    out = {
        "dataset": "superace 2024-01-01..10 (PHP platform), 10 days",
        "method": ("FG-trigger = spin with m>=25 AND gap_to_next>=45s (folded row: trigger + free spins; "
                   "gap NULL treated as BG). BG = all other spins. m = payout/bet. Tiered bins, value = "
                   "upper edge (0.1 wide below 2x ... 100 wide above 1000x)."),
        "total_spins": gs,
        "fg_share_of_spins_pct": round(fg["spins"] / gs * 100, 4),
        "fg_share_of_payout_pct": round(fg["total_payout"] / gp * 100, 4),
        "bg": bg, "fg_trigger": fg,
    }
    json.dump(out, open(OUT, "w"), ensure_ascii=False, indent=2)
    print(f"DONE spins={gs:,} | BG rtp={bg['rtp']} hit={bg['hit_rate_pct']}% | "
          f"FG spins={fg['spins']:,} (1/{gs//fg['spins']}) rtp_row={fg['rtp']} | "
          f"FG payout share={out['fg_share_of_payout_pct']}%", flush=True)


if __name__ == "__main__":
    main()
