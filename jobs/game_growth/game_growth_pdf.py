"""Compose the game-growth analysis into a single PDF report.

Inputs (produced by jobs/game_growth_timeline.py + period aggregation):
  data/output/game_growth_daily.csv
  data/output/game_growth_by_period.csv
  data/output/game_growth_events.csv
  data/output/game_growth_<GAME>.png
Output:
  data/output/game_growth_report.pdf
"""
from __future__ import annotations
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import font_manager
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "output"
GAMES = ["FM01", "SS03", "SS01", "SS02", "SS06"]

# CJK font
for cand in ["/System/Library/Fonts/Hiragino Sans GB.ttc", "/Library/Fonts/Arial Unicode.ttf"]:
    if Path(cand).exists():
        font_manager.fontManager.addfont(cand)
        fname = font_manager.FontProperties(fname=cand).get_name()
        plt.rcParams["font.family"] = fname
        break
plt.rcParams["axes.unicode_minus"] = False

daily = pd.read_csv(OUT / "game_growth_daily.csv", parse_dates=["dt"])
period = pd.read_csv(OUT / "game_growth_by_period.csv")

READS = {
    "FM01": ["接入大厅(10/29)为量级奇点: 日均158万→654万(×4), 奖池迭代期推至1000万+",
             "动态RTP分鱼种v2(1/29): 投注1,100→1,350万/日, D1留存 25.5%→27.7%",
             "4月风控期(A58/新玩家封禁): 量不降反升(1,653→1,811万) — 清农场不伤大盘",
             "动态RTPv3+96.5%(6/9)单段最大跳: 日均投注+43%至2,150万, GGR 71万/日全期最佳",
             "7/13风控v1.1段回落至1,927万 — BQ1农场流水被剔的对照证据"],
    "SS03": ["PA活动期(3/9-3/13)日均64万; 活动结束后不降反升至118万 — 活动完成拉新沉淀",
             "3/31三件套(default97%+AI调控+暗保底)后爬坡: 73→130→207万/日(MAB调alpha段)",
             "GGR日均一路爬: 1.2→6.2(暗保底改值)→11.2(6/9全体暗保底)→13.3万(MAB v4峰值)",
             "代价可见: RTP从97%压至93.5%(v4段); 7/5换表段量回落至159万 — 量利权衡点在6/18-7/5"],
    "SS01": ["2/25新kmeans后 36→127万/日(3/23段), 4/7后衰减至24万",
             "2/2段RTP 114.9%: Risky双scatter bug当天净亏, 表中清晰可见"],
    "SS02": ["3/31 AI上线当天18.5万即峰值, 之后一路<6万 — 调控未能救量"],
    "SS06": ["7/7暗保底段RTP 103.6%、GGR为负(上线首周被打穿)",
             "7/16换表止血: RTP 94.1%, GGR转正 — “降波动稳RTP”直接兑现"],
}


def overview_tables():
    rows_all, rows_30 = [], []
    for g in GAMES:
        d = daily[(daily.game == g) & (daily.bet > 0)].sort_values("dt")
        if d.empty:
            continue
        rows_all.append([g, f"{d.dt.min().date()}", len(d), f"{d.bet.mean()/1e4:,.1f}",
                         f"{d.ggr.mean()/1e4:,.2f}", f"{d.payout.sum()/d.bet.sum()*100:.2f}%",
                         f"{d.d1_retention.median()*100:.1f}%", f"{d.players.mean():,.0f}"])
        t = d.tail(30)
        rows_30.append([g, f"{t.bet.mean()/1e4:,.1f}", f"{t.ggr.mean()/1e4:,.2f}",
                        f"{t.payout.sum()/t.bet.sum()*100:.2f}%",
                        f"{t.d1_retention.median()*100:.1f}%", f"{t.players.mean():,.0f}"])
    return rows_all, rows_30


def draw_table(ax, col_labels, rows, fontsize=7.5, col_widths=None, left_align_col=None):
    ax.axis("off")
    tb = ax.table(cellText=rows, colLabels=col_labels, loc="upper center",
                  cellLoc="center", colWidths=col_widths)
    tb.auto_set_font_size(False)
    tb.set_fontsize(fontsize)
    tb.scale(1, 1.25)
    for (r, c), cell in tb.get_celld().items():
        if r == 0:
            cell.set_facecolor("#1F4E78"); cell.set_text_props(color="white", weight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#EAF1FB")
        if left_align_col is not None and c == left_align_col and r > 0:
            cell.set_text_props(ha="left")
            cell.PAD = 0.02
        cell.set_edgecolor("#CCCCCC")


def main():
    pdf_path = OUT / "game_growth_report.pdf"
    with PdfPages(pdf_path) as pdf:
        # ---- Page 1: title + overview ----
        fig = plt.figure(figsize=(11.7, 8.3))
        fig.text(0.5, 0.95, "游戏增长与调控机制复盘报告", ha="center", fontsize=20, weight="bold")
        fig.text(0.5, 0.905, "FM01 / SS03 / SS01 / SS02 / SS06 · 日均口径 · CNY · 数据截至 2026-07-27",
                 ha="center", fontsize=10, color="gray")
        rows_all, rows_30 = overview_tables()
        ax1 = fig.add_axes([0.05, 0.52, 0.9, 0.33])
        fig.text(0.07, 0.865, "① 上线至今 · 日均指标", fontsize=12, weight="bold")
        draw_table(ax1, ["游戏", "数据起点", "天数", "日均投注(万)", "日均GGR(万)", "整体RTP", "D1留存中位", "日均投注玩家"], rows_all, fontsize=9)
        ax2 = fig.add_axes([0.05, 0.17, 0.9, 0.26])
        fig.text(0.07, 0.455, "② 近30天 · 日均指标(现状)", fontsize=12, weight="bold")
        draw_table(ax2, ["游戏", "日均投注(万)", "日均GGR(万)", "RTP", "D1留存中位", "日均投注玩家"], rows_30, fontsize=9)
        fig.text(0.07, 0.10, "口径: GGR=bet−payout; RTP=Σpayout/Σbet; D1留存=当日投注玩家次日仍投注比例(betting/point);\n"
                             "数据源: slot-machine.fct_bet_orders_summary_hourly / agfish.fct_op_summary_daily / platform.fct_retention_summary_daily",
                 fontsize=8, color="gray")
        pdf.savefig(fig); plt.close(fig)

        # ---- per game: chart page + period table page ----
        for g in GAMES:
            img = OUT / f"game_growth_{g}.png"
            if img.exists():
                fig = plt.figure(figsize=(11.7, 8.3))
                fig.text(0.5, 0.96, f"{g} · 日级趋势与调控时点", ha="center", fontsize=15, weight="bold")
                ax = fig.add_axes([0.03, 0.30, 0.94, 0.62]); ax.axis("off")
                ax.imshow(plt.imread(img))
                y = 0.26
                fig.text(0.05, y, "关键读点:", fontsize=11, weight="bold"); y -= 0.032
                for line in READS.get(g, []):
                    fig.text(0.06, y, "· " + line, fontsize=9.5); y -= 0.028
                pdf.savefig(fig); plt.close(fig)

            seg = period[period.game == g]
            if seg.empty:
                continue
            cols = ["事件#", "起", "止", "天数", "调控", "日均投注万", "日均GGR万", "RTP", "D1留存", "日均玩家"]
            rows = seg[cols].fillna("-").astype(str).values.tolist()
            for r in rows:
                r[0] = r[0].split(".")[0]  # 事件# 去浮点尾巴
            per_page = 24
            for pi in range(0, len(rows), per_page):
                fig = plt.figure(figsize=(11.7, 8.3))
                sub = f"(续{pi//per_page})" if pi else ""
                fig.text(0.5, 0.95, f"{g} · 调控分段日均总览{sub}", ha="center", fontsize=14, weight="bold")
                fig.text(0.5, 0.915, "事件#与趋势图橙色圆圈编号对应(0=上线基线); 每段=该调控生效起至下一调控前; RTP=段内Σpayout/Σbet; 1天段仅作方向参考",
                         ha="center", fontsize=8.5, color="gray")
                ax = fig.add_axes([0.02, 0.05, 0.96, 0.83])
                draw_table(ax, cols, rows[pi:pi+per_page], fontsize=7,
                           col_widths=[0.045, 0.075, 0.075, 0.04, 0.40, 0.07, 0.07, 0.055, 0.05, 0.055],
                           left_align_col=4)
                pdf.savefig(fig); plt.close(fig)

    print(f"PDF -> {pdf_path}")


if __name__ == "__main__":
    main()
