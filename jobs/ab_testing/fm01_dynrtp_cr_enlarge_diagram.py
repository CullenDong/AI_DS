"""FM01 dynamic_rtp_v3_CR_enlarge AB 分组结构图(沿用 render_ab_diagrams.py 样式)。
农场前置剔除 → 其余按 user_id 尾号后两位 MOD100 切 40/30/30(dynamic_rtp_v3 / CR挽留 / default)。
风控/封控 = 策略(只改子弹策略),不参与分组,命中者仍属原组(右侧正交块,虚线)。
输出 data/output/ab_testing/AB结构图_FM01_dynamic_rtp_v3_CR_enlarge.png。
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
def arr(ax,x1,y1,x2,y2,color="#57606a",ls="-"):
    ax.add_patch(FancyArrowPatch((x1,y1),(x2,y2),arrowstyle="-|>",mutation_scale=15,lw=1.3,color=color,linestyle=ls))
fig,ax=plt.subplots(figsize=(11.5,7)); ax.set_xlim(0,100); ax.set_ylim(0,100); ax.axis("off")
ax.text(50,97,"FM01 捕鱼 · dynamic_rtp_v3_CR_enlarge AB 分组结构",ha="center",fontproperties=FP,fontsize=15,fontweight="bold",color="#1f2328")
ax.text(50,91.5,"配比 dynamic_rtp_v3 40% / CR挽留 30% / default 30%(P0) · user_id 尾号后两位 MOD100 分流 · 一期 14 天",
        ha="center",fontproperties=FP,fontsize=9,color="#656d76")
box(ax,42,84,40,6,"全体活跃玩家 (by user_id)",C["gray"],C["grayE"],11,True)
box(ax,42,71,34,7,"农场剔除(前置)\n命中农场PID → 不进任何臂",C["gold"],C["goldE"],8.8,True,"#8a5a00")
arr(ax,42,81,42,74.6)
box(ax,42,58,50,5.5,"其余玩家 → 按 user_id 尾号后两位 MOD100 分流",C["gold"],C["goldE"],9.6,True,"#8a5a00")
arr(ax,42,67.5,42,60.8)
box(ax,17,40,25,13,"dynamic_rtp_v3\n尾号后两位 00-39 · 40%\nRTP 动态优化",C["blue"],C["blueE"],8.8,True,"#084298")
box(ax,44,40,25,13,"CR 挽留\n尾号后两位 40-69 · 30%\n挽留 ON",C["green"],C["greenE"],8.8,True,"#0f5132")
box(ax,71,40,25,13,"default (P0)\n尾号后两位 70-99 · 30%\n基准 / holdout",C["gray"],C["grayE"],8.8,True)
for tx in [17,44,71]: arr(ax,42-((42-tx)*0.3),55.2,tx,46.7)
# 风控:正交策略,不参与分组(右侧,虚线指向三个组)
box(ax,90,71,17,15,"风控/封控\n= 策略,非分组\n\n只改子弹策略(RTP)\n命中者仍属原组\n不单列组",
    C["red"],C["redE"],7.6,True,"#a40e26")
ax.annotate("",xy=(84,44),xytext=(90,63.5),arrowprops=dict(arrowstyle="-|>",lw=1.3,color="#cf6a6a",linestyle=(0,(4,3))))
ax.text(88,54,"叠加策略\n不改分组",ha="center",va="center",fontproperties=FP,fontsize=7,color="#a40e26",linespacing=1.3)
box(ax,42,20,80,8,"对比(ITT·缩尾均值+中位):H1 dynamic_rtp vs default · H2 挽留 vs default(按HMM状态分层) · H3 两者相对\n主指标 D1/D3/D7 留存;护栏 实测RTP/GGR/人均净亏(风控占比作协变量);SRM 校验 40/30/30",
    C["purple"],C["purpleE"],8.2,False,"#5a32a3")
arr(ax,42,33.5,42,24.2)
p=OUT/"AB结构图_FM01_dynamic_rtp_v3_CR_enlarge.png"
fig.savefig(p,dpi=150,bbox_inches="tight",facecolor="white"); plt.close(fig); print("saved",p)
