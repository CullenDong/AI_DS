"""Game growth series (bet / GGR / RTP / D1-retention) since launch, with the
control-mechanism timeline overlaid, for FM01 / SS01 / SS02 / SS03 / SS06.

Sources (all CNY):
  slots  : slot-machine.fct_bet_orders_summary_hourly  (daily agg of bet/payout)
  FM01   : transform-agfish-game.fct_op_summary_daily
  retain : platform.fct_retention_summary_daily (retention_type='point',
           retention_day=1, category='betting'; D1 = returners/players)

Outputs:
  data/output/game_growth_daily.csv          (game, dt, bet, ggr, rtp, d1_retention)
  data/output/game_growth_events.csv         (game, date, event)
  data/output/game_growth_<GAME>.png         (per-game chart with event markers)
  data/output/game_growth_report.md
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402

OUT = ROOT / "data" / "output"
GAMES = ["FM01", "SS01", "SS02", "SS03", "SS06"]

# ---- control-mechanism timeline (from ops log; key events for chart, all in CSV) ----
EVENTS = {
 "FM01": [
  ("2025-10-09","上线定向奖池分配"),("2025-10-23","上线捕鱼动态RTP调控"),
  ("2025-10-29","接入捕鱼大厅,玩家激增"),("2025-11-05","奖池人数100→200"),
  ("2025-11-06","奖池引入2倍场,下限80%→95%"),("2025-11-13","红利钱包上线,奖池500人"),
  ("2025-12-10","奖池扩至1000人"),("2025-12-17","奖池新增新玩家池"),
  ("2025-12-19","筛选门槛1万→650RMB"),("2025-12-23","进组逻辑改a/b组(105%/110%)"),
  ("2025-12-30","两组上限450人"),("2026-01-29","动态RTP分鱼种v2上线"),
  ("2026-03-04","定向奖池v2分桶RTP(BOOST_POOL)"),("2026-03-10","v2桶2万投注上限+动态RTPv2.1"),
  ("2026-03-11","修改退组逻辑"),("2026-03-17","分鱼种v2人数20%→30%"),
  ("2026-03-26","分鱼种v2只用于1.0/2.0"),("2026-03-30","奖池v2进组异常修复"),
  ("2026-04-08","PID封禁A58+IP子弹级风控"),("2026-04-10","筛650RMB玩家进奖池"),
  ("2026-04-11","新玩家封禁策略"),("2026-04-27","PID封禁解除"),
  ("2026-05-05","动态RTPv2.2"),("2026-05-21","动态RTPv2.3"),
  ("2026-06-01","动态调控策略风控模型上线"),("2026-06-09","动态RTPv3,整体RTP改96.5%"),
  ("2026-07-13","风控v1.1(L1-3,BQ1 IP封禁)"),("2026-07-22","风控v1.2(增L0)"),
 ],
 "SS01": [
  ("2025-11-24","SS01上线"),("2025-12-04","AI调控上线(giftshop/newbee/carousels)"),
  ("2026-01-07","AI表换newbee系"),("2026-01-09","AI组扩至50%玩家"),
  ("2026-01-13","Default换carousels"),("2026-02-02","AI表换(carousels/rollerCoaster/Risky)"),
  ("2026-02-03","Risky双scatter bug,临时切表"),("2026-02-04","cluster2换risky2"),
  ("2026-02-18","新kmeans模型+配对"),("2026-02-25","新kmeans(default giftshop)"),
  ("2026-03-23","新kmeans配对"),("2026-03-31","AI数据读取框架更新"),
  ("2026-04-01","Default换carousels"),("2026-04-03","AI进组异常"),("2026-04-07","AI恢复进组"),
  ("2026-05-11","cluster0换risky2"),
 ],
 "SS02": [
  ("2026-03-31","上线AI调控(原计划3.17)"),("2026-04-01","AI无人进组"),
  ("2026-04-07","AI恢复进组"),("2026-04-28","MAB调alpha"),
  ("2026-05-01","MAB重置"),("2026-05-05","Default换Fast1"),("2026-05-26","Default换Medium3"),
 ],
 "SS03": [
  ("2026-02-11","游戏上线"),("2026-03-05","上线活动数学表,整体98%"),
  ("2026-03-09","PA平台活动开始(3.9-3.13)"),("2026-03-14","PA平台活动结束"),
  ("2026-03-31","default 97% normal_zero + AI调控 + 暗保底上线"),
  ("2026-04-01","AI/暗保底进组异常修复"),("2026-04-10","暗保底数值修改(70轮/70%/60%)"),
  ("2026-04-28","MAB调alpha"),("2026-05-21","MAB v3"),("2026-05-28","MAB v2算力bug修复"),
  ("2026-06-09","暗保底全体上线 + A组95Kai/B组97BGadj"),
  ("2026-06-18","MAB v4(GaiL模拟数据)"),
  ("2026-07-05","Default→95Kai, A组→normal_zero, B组→93Kai"),
 ],
 "SS06": [
  ("2026-07-07","暗保底一期上线"),("2026-07-16","换Super Free Spin数学表降波动"),
 ],
}


def fetch() -> pd.DataFrame:
    rows = []
    # slots
    be = rs.RedshiftBackend(database="slot-machine", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5436)
    for r in be.execute("""
        SELECT game_id, utc_date_key, sum(total_bet_amount), sum(total_actual_payout)
        FROM public.fct_bet_orders_summary_hourly
        WHERE game_id IN ('SS01','SS02','SS03','SS06') AND currency_type='CNY'
        GROUP BY 1,2 ORDER BY 1,2"""):
        rows.append((r[0], r[1], float(r[2] or 0), float(r[3] or 0)))
    be.close()
    # FM01
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5437)
    for r in be.execute("""
        SELECT 'FM01', bj_date_key, sum(total_bet), sum(total_payout)
        FROM public.fct_op_summary_daily
        WHERE game_id='FM01' AND currency_type='CNY'
        GROUP BY 2 ORDER BY 2"""):
        rows.append((r[0], r[1], float(r[2] or 0), float(r[3] or 0)))
    be.close()
    df = pd.DataFrame(rows, columns=["game", "dt", "bet", "payout"])
    df["ggr"] = df.bet - df.payout
    df["rtp"] = df.payout / df.bet.where(df.bet > 0)
    # retention D1 (betting category, point)
    be = rs.RedshiftBackend(database="platform", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5438)
    ret = []
    for r in be.execute("""
        SELECT game_id, bj_date_key, sum(player_count), sum(num_return_player)
        FROM public.fct_retention_summary_daily
        WHERE game_id IN ('FM01','SS01','SS02','SS03','SS06')
          AND retention_type='point' AND retention_day=1 AND category='betting'
          AND currency_type='CNY'
        GROUP BY 1,2 ORDER BY 1,2"""):
        ret.append((r[0], r[1], int(r[2] or 0), int(r[3] or 0)))
    be.close()
    rdf = pd.DataFrame(ret, columns=["game", "dt", "players", "returners"])
    rdf["d1_retention"] = rdf.returners / rdf.players.where(rdf.players > 0)
    df = df.merge(rdf[["game", "dt", "players", "d1_retention"]], on=["game", "dt"], how="left")
    return df


def chart(df: pd.DataFrame):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    for g in GAMES:
        d = df[df.game == g].sort_values("dt")
        if d.empty:
            continue
        d = d[d.bet > 0]
        fig, ax = plt.subplots(3, 1, figsize=(16, 11), sharex=True,
                               gridspec_kw={"height_ratios": [2, 1, 1]})
        ax[0].bar(d.dt, d.bet / 1e4, width=1, color="C0", alpha=.55, label="bet (wan)")
        ax0b = ax[0].twinx()
        ax0b.plot(d.dt, d.ggr.rolling(7, min_periods=1).mean() / 1e4, "C3-", lw=1.8, label="GGR 7d-avg (wan)")
        ax0b.axhline(0, color="gray", lw=.6)
        ax[0].set_ylabel("bet (wan CNY)"); ax0b.set_ylabel("GGR (wan)")
        ax[0].legend(loc="upper left"); ax0b.legend(loc="upper right")
        ax[0].set_title(f"{g}: daily bet / GGR / RTP / D1-retention with control events")
        ax[1].plot(d.dt, d.rtp * 100, "C2-", lw=1)
        ax[1].plot(d.dt, (d.rtp * 100).rolling(7, min_periods=1).mean(), "k-", lw=1.6)
        ax[1].axhline(100, color="r", ls=":", lw=.8)
        ax[1].set_ylabel("RTP %"); ax[1].set_ylim(max(50, d.rtp.min()*100-5), min(150, d.rtp.max()*100+5))
        ax[2].plot(d.dt, d.d1_retention * 100, "C4.", ms=3)
        ax[2].plot(d.dt, (d.d1_retention * 100).rolling(7, min_periods=1).mean(), "k-", lw=1.6)
        ax[2].set_ylabel("D1 retention %"); ax[2].set_xlabel("date")
        y_top = ax[0].get_ylim()[1]
        for idx, (dt_s, label) in enumerate(EVENTS.get(g, []), 1):
            x = pd.Timestamp(dt_s)
            if x < pd.Timestamp(d.dt.min()) or x > pd.Timestamp(d.dt.max()):
                continue
            for a in ax:
                a.axvline(x, color="orange", lw=.7, alpha=.7)
            # 编号标注(两层错位防重叠), 与分段表“事件#”一致
            y = y_top * (0.99 if idx % 2 else 0.93)
            ax[0].annotate(str(idx), xy=(x, y), fontsize=7.5, ha="center", va="top",
                           color="white", weight="bold",
                           bbox=dict(boxstyle="circle,pad=0.18", fc="darkorange", ec="none", alpha=.9))
        ax[2].xaxis.set_major_formatter(mdates.DateFormatter("%y-%m"))
        plt.tight_layout()
        p = OUT / f"game_growth_{g}.png"
        plt.savefig(p, dpi=110, bbox_inches="tight")
        plt.close()
        print(f"chart -> {p}", flush=True)


def build_periods(df: pd.DataFrame) -> pd.DataFrame:
    """Per-control-period daily averages; event # matches chart annotations (0 = baseline)."""
    out = []
    for g in GAMES:
        d = df[(df.game == g) & (df.bet > 0)].sort_values("dt")
        if d.empty:
            continue
        bounds = [(0, d.dt.min(), "(上线基线,无调控)")] + \
                 [(i, pd.Timestamp(dt_s), label) for i, (dt_s, label) in enumerate(EVENTS.get(g, []), 1)]
        bounds.append((None, d.dt.max() + pd.Timedelta(days=1), None))
        for i in range(len(bounds) - 1):
            num, s, label = bounds[i]
            e = bounds[i + 1][1]
            seg = d[(d.dt >= s) & (d.dt < e)]
            if seg.empty:
                continue
            pl = seg.players.mean()
            out.append({
                "game": g, "事件#": num, "起": s.date(), "止": (e - pd.Timedelta(days=1)).date(),
                "天数": len(seg), "调控": label,
                "日均投注万": round(seg.bet.mean() / 1e4, 1),
                "日均GGR万": round(seg.ggr.mean() / 1e4, 2),
                "RTP": round(seg.payout.sum() / seg.bet.sum() * 100, 2),
                "D1留存": round(seg.d1_retention.mean() * 100, 1) if seg.d1_retention.notna().any() else None,
                "日均玩家": int(pl) if pd.notna(pl) else None,
            })
    return pd.DataFrame(out)


def main():
    no_fetch = "--no-fetch" in sys.argv
    daily_p = OUT / "game_growth_daily.csv"
    if no_fetch and daily_p.exists():
        df = pd.read_csv(daily_p, parse_dates=["dt"])
        print("(--no-fetch: reuse existing daily csv)", flush=True)
    else:
        df = fetch()
    OUT.mkdir(parents=True, exist_ok=True)
    df.sort_values(["game", "dt"]).to_csv(OUT / "game_growth_daily.csv", index=False)
    ev = pd.DataFrame([(g, i, d, e) for g, lst in EVENTS.items() for i, (d, e) in enumerate(lst, 1)],
                      columns=["game", "事件#", "date", "event"])
    ev.to_csv(OUT / "game_growth_events.csv", index=False)
    periods = build_periods(df)
    periods.to_csv(OUT / "game_growth_by_period.csv", index=False)
    print(f"periods rows={len(periods)} -> game_growth_by_period.csv", flush=True)
    print(f"daily rows={len(df)} -> game_growth_daily.csv ; events={len(ev)}", flush=True)
    for g in GAMES:
        d = df[(df.game == g) & (df.bet > 0)]
        if d.empty:
            continue
        print(f"{g}: {d.dt.min()}..{d.dt.max()} days={len(d)} "
              f"total_bet={d.bet.sum()/1e8:.2f}亿 total_GGR={d.ggr.sum()/1e4:,.0f}万 "
              f"RTP={d.payout.sum()/d.bet.sum()*100:.2f}% "
              f"D1中位={d.d1_retention.median()*100:.1f}%", flush=True)
    chart(df)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
