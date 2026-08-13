"""挽留 AB · 两比例样本量测算（每臂每 HMM 状态）。

n/arm = (z_a/2 + z_b)^2 * [p1(1-p1)+p2(1-p2)] / (p1-p2)^2
主指标 = D7 留存（二值）。alpha=0.05 双侧, power=0.8。
基线 p1 按 HMM 状态取代表值（escaped 低、engaged 高），p2 = p1 + MDE。
"""
Z_A = 1.959964   # z_{0.025}
Z_B = 0.841621   # z_{0.20}

def n_per_arm(p1, mde):
    p2 = p1 + mde
    return (Z_A + Z_B) ** 2 * (p1 * (1 - p1) + p2 * (1 - p2)) / (mde ** 2)

# HMM 状态代表基线（D7 留存，待真实标定）
states = [("T1 初始", 0.15), ("S1 low", 0.10), ("S2 engaged", 0.30), ("S3 escaped", 0.05)]
mdes = [0.03, 0.05, 0.08]

print("每臂样本量需求（alpha=.05, power=.8, 主指标=D7留存）\n")
print(f"{'HMM状态':14}{'基线D7':>8}", end="")
for m in mdes:
    print(f"{'MDE+'+str(int(m*100))+'pp':>12}", end="")
print()
for name, p1 in states:
    print(f"{name:14}{p1*100:>7.0f}%", end="")
    for m in mdes:
        print(f"{n_per_arm(p1, m):>12,.0f}", end="")
    print()

print("\n注：需 × 2 臂（ON/OFF）× 该状态人数占比后与可用人群对照；")
print("单波 control(holdout) 较小，可能需跨波累计到目标样本。")
