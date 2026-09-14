"""HMM 状态 × FM01 分组(default / dynamic_rtp / 个性化挽留)分层表现。
用本地 HMM 标签(fm01_hmm_labels.parquet)+ 每(用户,北京日)主导组,
按(组 × 状态)出:投注额/发数/日净(缩99均+中位) · RTP · 连亏比 · D1/D3/D7 留存。
窗口默认 8 月(挽留组 07-31 上线后,三组齐全)。
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from tools.db import redshift as rs  # noqa: E402
from jobs.fishing.fm01_grouping import GROUP_CASE, BASE_FILTER, RETENTION_LAUNCH_UTC  # noqa: E402

LABELS = ROOT / "data" / "output" / "hmm" / "fm01_hmm_labels.parquet"
FEATDIR = ROOT / "data" / "output" / "hmm" / "fm01_hmm_features"
W_START, W_END = "2026-08-01", "2026-08-31"          # 北京日窗口(含)
STATES = ["T1_first_day", "S1_Low", "S2_Engaged", "S3_Lapsed"]
GROUPS = ["default", "dynamic_rtp", "retention"]
w99 = lambda s: s.clip(upper=s.quantile(0.99)).mean()
# HMM 特征工程的 10 个特征(缩写: 展示用)
HMM_FEATS = [
    ("no_bet_streak_days", "回流间隔"), ("bet_amount_ratio_today_vs_history", "额比史"),
    ("bet_count_ratio_today_vs_history", "发数比史"), ("avg_bet_one_time_today_log", "单发log"),
    ("rtp_7_bet_days", "近7日RTP"), ("loss_streak_ratio_today", "连亏比"),
    ("current_balance_max_to_avg_bet_ratio", "余额比注"), ("target_selection_entropy", "打鱼熵"),
    ("multiplier_change_count_ratio", "倍率切换比"), ("bullet_level_change_count_ratio", "子弹切换比"),
]


def main():
    # 1) 每(用户,北京日)主导组
    be = rs.RedshiftBackend(database="transform-agfish-game", bastion_ip=rs.DEFAULT_BASTION_IP, local_port=5545)
    gd = pd.DataFrame(be.execute(f"""
      SELECT user_id, (created_at+interval '8 hours')::date bet_date, ({GROUP_CASE}) grp, COUNT(*) n
      FROM public.bullet WHERE {BASE_FILTER} AND event_timestamp>='{RETENTION_LAUNCH_UTC}'
        AND (created_at+interval '8 hours')::date BETWEEN '{W_START}' AND '{W_END}'
      GROUP BY 1,2,3"""), columns=["user_id", "bet_date", "grp", "n"])
    be.close()
    gd["n"] = gd["n"].astype(int); gd["bet_date"] = pd.to_datetime(gd["bet_date"])
    gd = gd.sort_values("n").groupby(["user_id", "bet_date"]).tail(1)[["user_id", "bet_date", "grp"]]  # 主导组

    # 2) 标签(全量,用于留存的活跃集)+ 窗口切片
    lab = pd.read_parquet(LABELS)
    lab["bet_date"] = pd.to_datetime(lab["bet_date"])
    act = {u: set(d) for u, d in lab.groupby("user_id")["bet_date"]}          # 活跃(用户→有投注日集)
    last_complete = lab["bet_date"].max()                                     # 右截断
    win = lab[(lab["bet_date"] >= W_START) & (lab["bet_date"] <= W_END)].merge(gd, on=["user_id", "bet_date"], how="inner")
    win = win[win["grp"].isin(GROUPS)]
    win["loss_ratio"] = win["max_consecutive_loss_count_today"] / win["bet_count_today"]
    # 2b) join HMM 特征工程的 10 个特征(只读窗口内每日文件)
    ff = [FEATDIR / f"{d.date()}.parquet" for d in pd.date_range(W_START, W_END)]
    feats = pd.concat([pd.read_parquet(f, columns=["user_id", "bet_date"] + [c for c, _ in HMM_FEATS])
                       for f in ff if f.exists()], ignore_index=True)
    feats["bet_date"] = pd.to_datetime(feats["bet_date"])
    win = win.merge(feats, on=["user_id", "bet_date"], how="left")
    print(f"窗口 {W_START}~{W_END}:{len(win):,} 用户-日(已合并组+状态+HMM特征)")

    # 3) 留存:cohort=(grp,state)某日活跃用户,DN=该批人 d+N 当日是否仍有投注(任意组)
    def ret(sub, N):
        num = den = 0
        for d, g in sub.groupby("bet_date"):
            tgt = d + pd.Timedelta(days=N)
            if tgt > last_complete:
                continue
            users = g["user_id"].values; den += len(users)
            num += sum(1 for u in users if tgt in act.get(u, ()))
        return num / den * 100 if den else float("nan")

    # 4) 汇总成一份 md 报告(表现表 + HMM特征表 + 指标解释)
    def mdtable(headers, rows):
        h = "| " + " | ".join(headers) + " |\n| " + " | ".join(["---"]*len(headers)) + " |\n"
        return h + "".join("| " + " | ".join(str(c) for c in r) + " |\n" for r in rows)
    gname = {"default": "default", "dynamic_rtp": "dynamic_rtp", "retention": "个性化挽留"}
    out = ["# FM01 · HMM 状态 × 分组 分层表现汇总\n",
           f"> 窗口(北京日):{W_START} ~ {W_END}(挽留组 07-31 上线后,三组齐全)· 状态由 IO-HMM 打标 · 组=每(用户,北京日)当天主导组\n",
           f"> 用户-日 {len(win):,}(default/dynamic_rtp/个性化挽留,已并状态与 HMM 特征)· 投注额/日净报缩99%均+中位,其余中位\n"]
    for grp in GROUPS:
        out.append(f"\n## {gname[grp]} 组\n\n**击打表现**\n")
        rows = []
        for st in STATES:
            sub = win[(win["grp"] == grp) & (win["state_name"] == st)]
            if not len(sub): continue
            rtp = sub["payout_today"].sum() / sub["bet_amount_today"].sum() * 100
            rows.append([st, f"{len(sub):,}", f"{w99(sub['bet_amount_today']):.0f}", f"{sub['bet_amount_today'].median():.0f}",
                         f"{sub['bet_count_today'].median():.0f}", f"{w99(sub['profit_today']):.0f}", f"{sub['profit_today'].median():.0f}",
                         f"{rtp:.1f}", f"{sub['loss_ratio'].median():.2f}"] + [f"{ret(sub, N):.1f}" for N in [1, 3, 7]])
        out.append(mdtable(["状态", "用户-日", "投注额缩99", "投注额中", "发数中", "日净缩99", "日净中", "RTP%", "连亏比中", "D1", "D3", "D7"], rows))
        out.append("\n**HMM 特征工程指标(中位)**\n")
        rows = []
        for st in STATES:
            sub = win[(win["grp"] == grp) & (win["state_name"] == st)]
            if not len(sub): continue
            rows.append([st] + [("—" if sub[c].isna().all() else f"{sub[c].median():.2f}") for c, _ in HMM_FEATS])
        out.append(mdtable(["状态"] + [lbl for _, lbl in HMM_FEATS], rows))

    out.append("\n---\n\n## 指标解释\n\n### 击打表现\n")
    for line in [
        "- **用户-日**:该(组,状态)在窗口内的行数 = 有多少个「某用户的某天」落在此格(不是唯一用户数)。",
        "- **投注额缩99 / 投注额中**:每用户每日总投注额,分别取「99% 缩尾均值」(削掉最高 1% 大户,防鲸鱼拉高)和「中位数」(典型玩家)。",
        "- **发数中**:每日打了多少发子弹的中位数。",
        "- **日净缩99 / 日净中**:每日净盈亏 = 当日赔付(含免费游戏 FG)− 当日投注,取缩99均值/中位;负 = 玩家净亏。",
        "- **RTP%**:该格总赔付 ÷ 总投注 ×100(含 FG);放水程度,>100 玩家占便宜。",
        "- **连亏比中**:当日「最长连续不中(亏损)发数 ÷ 当日发数」的中位;越高越「背」(体验差)。",
        "- **D1 / D3 / D7**:day-N 当日回访留存 —— 该状态某日活跃的玩家,N 天后当天是否仍有投注(任意组)。当天有投注即活跃、按天算、不去重、右截断置空。",
    ]:
        out.append(line + "\n")
    out.append("\n### HMM 特征工程指标(定义状态的 10 个特征,取中位)\n")
    for line in [
        "- **回流间隔** `no_bet_streak_days`:距上一个投注日的空档天数;越大越「间歇回流」。首日无历史 = 空。",
        "- **额比史** `bet_amount_ratio_today_vs_history`:今日投注额 ÷ 该玩家历史(今天之前所有投注日)日均投注额;=1 与平时持平,<1 相对自己萎缩,>1 放大。",
        "- **发数比史** `bet_count_ratio_today_vs_history`:今日发数 ÷ 历史日均发数;同上,看打的「量」相对自己涨缩。",
        "- **单发log** `avg_bet_one_time_today_log`:log(1+今日单发均额);反映单发下注档位(取 log 压缩)。",
        "- **近7日RTP** `rtp_7_bet_days`:最近 7 个「投注日」(非自然7天)的滚动 RTP(Σ赔付/Σ投注);近期手气/放水。",
        "- **连亏比** `loss_streak_ratio_today`:同「连亏比中」——它本身就是 HMM 的一个特征(当日最长连亏 ÷ 发数)。",
        "- **余额比注** `current_balance_max_to_avg_bet_ratio`:当日最高余额 ÷ 历史单发均额;越大 = 相对下注,兜里越有钱。",
        "- **打鱼熵** `target_selection_entropy`:打鱼档位(低2-10 / 中15-130 / 高150-200 / 超500-1000)选择的熵,除以 log(4) 归一到 0-1;越高 = 什么鱼都打(不挑),越低 = 集中打某档。",
        "- **倍率切换比** `multiplier_change_count_ratio`:当日倍率切换次数 ÷ 发数;调倍率的频繁度(多数玩家=0)。",
        "- **子弹切换比** `bullet_level_change_count_ratio`:当日子弹档切换次数 ÷ 发数;同上。",
    ]:
        out.append(line + "\n")
    out.append("\n> 状态:T1=首投日(k=1,首日模型)· S1=Low(低参与萎缩)· S2=Engaged(核心活跃)· S3=Lapsed(间歇回流)。tenure/累计投注从数据起点 2026-03-05 累计,老玩家不会被误判首日。\n")

    rp = ROOT / "docs" / "FM01_HMM状态分层表现_v0.1.md"
    rp.write_text("".join(out), encoding="utf-8")
    print(f"报告已生成: {rp}")


if __name__ == "__main__":
    main()
