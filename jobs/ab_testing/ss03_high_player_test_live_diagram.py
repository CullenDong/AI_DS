"""SS03 high_player_test_v1 上线 分组结构图（独立,不覆盖 AB结构图_SS03.png）。
两层:第1层 AB(MOD100 尾号后两位 A/B/default) + 第2层 暗保底(所有臂共用,策略非分组)。
输出 data/output/ab_testing/AB结构图_SS03_high_player_test_v1.png。
"""
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.font_manager as fm
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

FP=None
for p in ["/System/Library/Fonts/PingFang.ttc","/System/Library/Fonts/STHeiti Medium.ttc"]:
    if Path(p).exists(): FP=fm.FontProperties(fname=p); break
OUT=Path(__file__).resolve().parents[2]/"data"/"output"/"ab_testing"; OUT.mkdir(parents=True,exist_ok=True)
C=dict(gray="#eef2f6",grayE="#57606a",gold="#fff3cd",goldE="#d4a72c",green="#d1e7dd",greenE="#0f7b3f",
       blue="#cfe2ff",blueE="#0969da",orange="#ffe0cc",orangeE="#d1611a",purple="#f3e8ff",purpleE="#8250df",
       red="#ffd6d6",redE="#cf222e")
def box(ax,x,y,w,h,t,fc,ec,fs=10,bold=False,tc="#1f2328"):
    ax.add_patch(FancyBboxPatch((x-w/2,y-h/2),w,h,boxstyle="round,pad=0.5,rounding_size=2",fc=fc,ec=ec,lw=1.6))
    ax.text(x,y,t,ha="center",va="center",fontproperties=FP,fontsize=fs,color=tc,fontweight="bold" if bold else "normal",linespacing=1.4)
def arr(ax,x1,y1,x2,y2):
    ax.add_patch(FancyArrowPatch((x1,y1),(x2,y2),arrowstyle="-|>",mutation_scale=15,lw=1.3,color="#57606a"))
fig,ax=plt.subplots(figsize=(11,7)); ax.set_xlim(0,100); ax.set_ylim(0,100); ax.axis("off")
ax.text(50,97,"SS03 · high_player_test_v1 上线 分组结构（两层）",ha="center",fontproperties=FP,fontsize=14.5,fontweight="bold",color="#1f2328")
ax.text(50,91.5,"2026-09-14 23:00 UTC 起 · MOD(user_id,100) 尾号后两位 · A/B/default 30/30/40（无 AI）· 独立新方案,不覆盖既有结构",
        ha="center",fontproperties=FP,fontsize=8.6,color="#656d76")
box(ax,50,84,42,6,"SS03 全体 CNY 玩家",C["gray"],C["grayE"],11,True)
box(ax,50,74,54,5.5,"第1层 · AB 分组 —— MOD(user_id,100) 尾号后两位 静态分流",C["gold"],C["goldE"],9.4,True,"#8a5a00")
arr(ax,50,81,50,76.8)
box(ax,24,59,26,12,"A\n尾号后两位 00-29 · 30%\nBGTR97_Saitekika_BGadj_v3\nRTP 97",C["blue"],C["blueE"],8.4,True,"#084298")
box(ax,52,59,26,12,"B\n尾号后两位 30-59 · 30%\nBGTR95_HMM_Highvalue_v1\nRTP 95",C["orange"],C["orangeE"],8.2,True,"#8a3b0a")
box(ax,80,59,26,12,"default\n尾号后两位 60-99 · 40%\n95Kai\nRTP 95",C["green"],C["greenE"],8.6,True,"#0f5132")
for tx in [24,52,80]: arr(ax,50-((50-tx)*0.33),71.2,tx,65.2)
box(ax,50,40,72,11,"第2层 · 暗保底（所有臂共用,策略非分组）\n累计达档 → 应用確定表 normal_kakuteiC 触发保底赔付",C["red"],C["redE"],9.5,True,"#a40e26")
for tx in [24,52,80]: arr(ax,tx,53,50-((50-tx)*0.55),45.7)
box(ax,50,20,78,8,"对比(核心=高价值前10%,同期组间):p90 total_bet/num_bet · D1/D3/D7\nA vs default(高RTP) · B vs default(低波动形态) · A vs B;护栏 各组实测RTP贴设计",
    C["purple"],C["purpleE"],8.4,False,"#5a32a3")
arr(ax,50,34.5,50,24.2)
p=OUT/"AB结构图_SS03_high_player_test_v1.png"
fig.savefig(p,dpi=150,bbox_inches="tight",facecolor="white"); plt.close(fig); print("saved",p)
