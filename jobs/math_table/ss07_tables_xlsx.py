"""按 SS07 summary 格式输出两张目标数学表（Excel）。

v3 (95%)：低中 payout、命中率 15%、x2/x3 多。
v2 (97%)：低中 payout、命中率 20%（更高触发）、x3/x5 多。
设计杠杆（RTP/Hit%/Applied Multiplier）为精确目标；波动/Top-5/Max/正奖 bin 为 reel 仿真输出，标注待定。
"""
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "math_table_design" / "SS07_数学表_v3-95_v2-97.xlsx"
MT = [1, 2, 3, 5, 10, 15]

# 设计（分布已验证自洽，RTP 精确命中）
TABLES = {
    "v3": {"rtp": 0.95, "hit": 0.15,
           "Normal": [.27, .30, .22, .13, .05, .03], "Extra": [0, .42, .28, .18, .08, .04]},
    "v2": {"rtp": 0.97, "hit": 0.20,
           "Normal": [.20, .22, .26, .20, .08, .04], "Extra": [0, .26, .30, .24, .12, .08]},
}
def norm(w): s = sum(w); return [x / s for x in w]
for t in TABLES.values():
    for lv in ("Normal", "Extra"): t[lv] = norm(t[lv])

TBD = "待reel仿真"
def mult_rows(idx):
    return [TABLES["v3"]["Normal"][idx], TABLES["v3"]["Extra"][idx],
            TABLES["v2"]["Normal"][idx], TABLES["v2"]["Extra"][idx]]

PCT = "0.00%"; NUM = "0.00"
# (category, metric, v3N, v3E, v2N, v2E, numfmt)
ROWS = [
    ("Basic Index", "", None, None, None, None, None),
    ("", "Bet", 10, 15, 10, 15, "0"),
    ("RTP", "Mean", 0.95, 0.95, 0.97, 0.97, PCT),
    ("Hit %", "Base Game", 0.15, 0.15, 0.20, 0.20, PCT),
    ("", "", None, None, None, None, None),
    ("Volatility Index", "", None, None, None, None, None),
    ("Overall Volatility", "Standard Deviation", TBD, TBD, TBD, TBD, None),
    ("", "Skewness", TBD, TBD, TBD, TBD, None),
    ("", "Kurtosis", TBD, TBD, TBD, TBD, None),
    ("", "95 Percentile Win", TBD, TBD, TBD, TBD, None),
    ("", "99 Percentile Win", TBD, TBD, TBD, TBD, None),
    ("Top 5 wins", "First~Fifth", TBD, TBD, TBD, TBD, None),
    ("", "", None, None, None, None, None),
    ("Applied Multiplier", "", None, None, None, None, None),
    ("", "x1", *mult_rows(0), PCT),
    ("", "x2", *mult_rows(1), PCT),
    ("", "x3", *mult_rows(2), PCT),
    ("", "x5", *mult_rows(3), PCT),
    ("", "x10", *mult_rows(4), PCT),
    ("", "x15", *mult_rows(5), PCT),
    ("", "", None, None, None, None, None),
    ("Multiplier Stats", "", None, None, None, None, None),
    ("", "Max Multiplier", TBD, TBD, TBD, TBD, None),
    ("", "", None, None, None, None, None),
    ("Bin Range", "", None, None, None, None, None),
    ("Overall", "0 （零奖=1−命中率）", 0.85, 0.85, 0.80, 0.80, PCT),
    ("", "[1,10) ~ [1000,10000)", TBD, TBD, TBD, TBD, None),
]

wb = Workbook(); ws = wb.active; ws.title = "Summary"
HDR = PatternFill("solid", fgColor="1F4E79"); SUB = PatternFill("solid", fgColor="DDEBF7")
CATF = PatternFill("solid", fgColor="F2F2F2"); WHITE = Font(color="FFFFFF", bold=True)
BOLD = Font(bold=True); THIN = Side(style="thin", color="BFBFBF")
BORD = Border(left=THIN, right=THIN, top=THIN, bottom=THIN); CT = Alignment("center", "center")

# 表头两行
ws.merge_cells("C1:D1"); ws.merge_cells("E1:F1")
ws["C1"] = "SS07_Saitekika_v3 (95%)  低倍·高命中"; ws["E1"] = "SS07_Saitekika_v2 (97%)  中倍·大奖"
for c in ("C1", "E1"): ws[c].fill = HDR; ws[c].font = WHITE; ws[c].alignment = CT
ws["A2"] = "类别"; ws["B2"] = "指标"
for i, h in enumerate(["Normal", "Extra", "Normal", "Extra"]):
    ws.cell(2, 3 + i, h)
for c in range(1, 7):
    cell = ws.cell(2, c); cell.fill = SUB; cell.font = BOLD; cell.alignment = CT; cell.border = BORD

r = 3
for cat, met, a, b, c, d, fmt in ROWS:
    ws.cell(r, 1, cat); ws.cell(r, 2, met)
    if cat and not met:                       # 分类标题行
        ws.cell(r, 1).font = BOLD; ws.cell(r, 1).fill = CATF
    for i, v in enumerate([a, b, c, d]):
        cell = ws.cell(r, 3 + i, v)
        cell.alignment = CT; cell.border = BORD
        if fmt and isinstance(v, (int, float)): cell.number_format = fmt
    ws.cell(r, 1).border = BORD; ws.cell(r, 2).border = BORD
    r += 1

# 备注
r += 1
ws.cell(r, 1, "备注：Bet / RTP / Hit% / Applied Multiplier 为精确设计目标（已验证自洽）；"
              "波动/Top-5/Max/正奖 bin 为 reel 仿真输出，搭表后回验。底层 paytable 随命中率调整（新表命中率更高、单奖更低）。")
ws.cell(r, 1).font = Font(italic=True, size=9, color="808080")

ws.column_dimensions["A"].width = 20; ws.column_dimensions["B"].width = 24
for col in "CDEF": ws.column_dimensions[col].width = 12
ws.freeze_panes = "C3"
wb.save(OUT)
print("saved:", OUT)
