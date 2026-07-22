"""Super Ace BG/FG inference via multiplier + inter-spin gap (per-day pipeline).

Method (validated on 2024-01-01 PHP data & 2026 orders CNY/USDT data):
  - The bet-level tables fold free-game winnings into the triggering paid spin; no row-level
    flag exists. But the client's free-spin animation leaves a TIME signature: the gap from a
    triggering spin to the player's next spin is long (median ~70s for m>50) vs the ~2s
    autoplay baseline for losing spins.
  - Label per spin, m = payout/bet, gap_s = next spin (same player) - this spin:
      FG-strong   : m>=25 AND gap_s>=45   (trigger rate lands at official ~1/200-300)
      FG-mid      : m>=10 AND gap_s>=30   (looser, mixes in some pure cascades)
      BG big win  : m>=25 AND gap short   (pure base-game cascade)
      base game   : everything else
  - Engineering: spins of one player are scattered across the 30 Trino shards, so gaps MUST
    be computed across all files of the day. 119M rows/day OOMs a duckdb window; solution is
    hash-bucketing: materialize (hash(cust), epoch_ts, bet, payout) once, then run the window
    per (c % NB) bucket and merge stats via integer-second gap histograms (exact medians).

Usage: python3 tools/sa_fg_gap_analysis.py [day ...]   (day like 2024-01-02; default 01..10)
Appends per-day metrics to data/output/superace_fg_gap_2024-01.json and prints a summary.
"""
from __future__ import annotations
import glob, json, os, sys, warnings
import duckdb
warnings.filterwarnings("ignore")

ROOT = "data/superace"
OUT = "data/output/superace_fg_gap_2024-01.json"
NB = 32
GRP_SQL = ("CASE WHEN m=0 THEN 'a. m=0' WHEN m<=2 THEN 'b. <=2' WHEN m<=10 THEN 'c. 2-10' "
           "WHEN m<=25 THEN 'd. 10-25' WHEN m<=50 THEN 'e. 25-50' ELSE 'f. >50' END")


def analyze_day(day: str) -> dict:
    files = sorted(glob.glob(f"{ROOT}/{day}/*"))
    assert files, f"no files for {day}"
    dbf = f"/tmp/sa_{day}.duckdb"
    if os.path.exists(dbf):
        os.remove(dbf)
    con = duckdb.connect(dbf)
    con.execute("SET memory_limit='6GB'; SET threads=4; "
                "SET temp_directory='/tmp/duck_spill'; SET preserve_insertion_order=false;")
    con.execute(f'''
        CREATE TABLE s AS
        SELECT hash(sha_customer_id) AS c, epoch(billtime)::BIGINT AS ts,
               bet_amount::DOUBLE AS bet, (bet_amount+cus_account)::DOUBLE AS payout
        FROM read_parquet('{ROOT}/{day}/*')
        WHERE bet_amount > 0''')
    hist: dict = {}
    agg = dict(total=0, fg_strong=0, fg_mid=0, big_no_pause=0,
               fg_strong_payout=0.0, all_payout=0.0, all_bet=0.0)
    for b in range(NB):
        base = f'''WITH t AS (
            SELECT payout/bet AS m, payout, bet,
                   lead(ts) OVER (PARTITION BY c ORDER BY ts) - ts AS gap_s
            FROM s WHERE c % {NB} = {b})'''
        for grp, gap, n in con.execute(
                f"{base} SELECT {GRP_SQL} AS g, gap_s, count(*) FROM t "
                f"WHERE gap_s IS NOT NULL AND gap_s BETWEEN 0 AND 600 GROUP BY 1,2").fetchall():
            hist[(grp, int(gap))] = hist.get((grp, int(gap)), 0) + n
        r = con.execute(f'''{base} SELECT count(*),
            sum(CASE WHEN m>=25 AND gap_s>=45 THEN 1 ELSE 0 END),
            sum(CASE WHEN m>=10 AND gap_s>=30 THEN 1 ELSE 0 END),
            sum(CASE WHEN m>=25 AND (gap_s<45 OR gap_s IS NULL) THEN 1 ELSE 0 END),
            sum(CASE WHEN m>=25 AND gap_s>=45 THEN payout ELSE 0 END),
            sum(payout), sum(bet) FROM t''').fetchone()
        for k, v in zip(list(agg), r):
            agg[k] += float(v or 0)
        print(f"  {day} bucket {b+1}/{NB}", flush=True)
    con.close(); os.remove(dbf)

    def med(grp):
        items = sorted((s, c) for (g, s), c in hist.items() if g == grp)
        tot = sum(c for _, c in items); cum = 0
        for s, c in items:
            cum += c
            if cum >= tot * 0.5:
                return s
        return None
    tt, st = int(agg["total"]), int(agg["fg_strong"])
    return {
        "day": day, "spins": tt, "rtp": round(agg["all_payout"] / agg["all_bet"], 6),
        "gap_median_by_grp": {g: med(g) for g in ["a. m=0", "b. <=2", "c. 2-10", "d. 10-25", "e. 25-50", "f. >50"]},
        "fg_strong": st, "fg_strong_rate_1_in": tt // max(st, 1),
        "fg_mid_plus": int(agg["fg_mid"]),
        "big_win_no_pause": int(agg["big_no_pause"]),
        "fg_strong_payout_pct": round(agg["fg_strong_payout"] / agg["all_payout"] * 100, 2),
    }


def main():
    days = sys.argv[1:] or [f"2024-01-{d:02d}" for d in range(1, 11)]
    results = []
    if os.path.exists(OUT):
        results = json.load(open(OUT))
    done = {r["day"] for r in results}
    for day in days:
        if day in done:
            print(f"{day} already done, skip", flush=True)
            continue
        print(f"=== {day} ===", flush=True)
        results.append(analyze_day(day))
        results.sort(key=lambda r: r["day"])
        json.dump(results, open(OUT, "w"), ensure_ascii=False, indent=2)
        r = results[-1]
        print(f"{day}: spins={r['spins']:,} fg_strong=1/{r['fg_strong_rate_1_in']} "
              f"payout_pct={r['fg_strong_payout_pct']}%", flush=True)
    print("ALL DONE", flush=True)
    for r in results:
        print(f"{r['day']}  spins={r['spins']:>12,}  RTP={r['rtp']}  FG强=1/{r['fg_strong_rate_1_in']}"
              f"  FG派彩占比={r['fg_strong_payout_pct']}%  gap(m>50)={r['gap_median_by_grp']['f. >50']}s", flush=True)


if __name__ == "__main__":
    main()
