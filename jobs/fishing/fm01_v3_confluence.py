"""把 FM01 动态RTP v3 总结与展望(md)发布为 Confluence 原生页面，放入 fish hunter data。
自带 md→storage 转换器（全宽表格）；对每张对比表按「指标方向」把更优的一格标黄。
标黄方向：留存/RTP/命中/人均发/累投/净/鱼值/rtp_th 越高越好；首杀轮次越低越好；计数/单笔/level 不标。
三视值(中/均/均99)、D1/D3/D7 等取第一个数比较。
用法：python3 jobs/fishing/fm01_v3_confluence.py [--publish]
"""
from __future__ import annotations
import re, sys, html, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis"))
from math_table_confluence import load_env   # 复用 .env 读取  # noqa: E402

MD = ROOT / "prd" / "dynamic_rtp" / "FM01_dynamic_rtp_v3_总结与展望.md"
SPACE = "productsha"; PARENT_ID = "122421321"; TITLE = "FM01 动态RTP v3（分鱼种）总结与未来展望"
HL = "#fff0b3"

# 指标方向：+1 越高越好 / -1 越低越好 / 0 不标
MONTHLY_DIR = {"RTP%": 1, "命中%": 1, "人均发": 1, "人均累投": 1, "人均净": 1, "平均鱼值": 1,
               "平均rtp_th": 1, "首杀轮次": -1, "D1/D3/D7/D10": 1}   # 用户/总发/单笔/level 不标
LAYERED_DIR = {"RTP%": 1, "命中%": 1, "人均发 中/均/均99": 1, "人均累投 中/均/均99": 1,
               "人均净 中/均/均99": 1, "鱼值 中/均/均99": 1, "rtp_th 中/均/均99": 1, "D1/D3/D7": 1}


def esc(s): return html.escape(s)
def inline(s):
    s = esc(s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<em>\1</em>", s)
    s = re.sub(r"`([^`]+?)`", r"<code>\1</code>", s)
    return s.replace("&lt;br/&gt;", "<br/>")


def fnum(cell):
    m = re.search(r"-?\d+(?:\.\d+)?", cell.replace("−", "-").replace(",", ""))
    return float(m.group()) if m else None


def better(a, b, d):
    """返回哪个更优：0=左, 1=右, -1=不标(缺失/平)。"""
    if a is None or b is None or d == 0 or a == b:
        return -1
    return 0 if (a - b) * d > 0 else 1


def hl_cells(head, rows):
    """返回需标黄的 (row_idx, col_idx) 集合。"""
    out = set()
    h0 = head[0]
    if h0 == "指标":                                  # 月度：每行 v3(1) vs default(2)
        for i, r in enumerate(rows):
            d = MONTHLY_DIR.get(r[0].strip(), 0)
            w = better(fnum(r[1]), fnum(r[2]), d)
            if w >= 0:
                out.add((i, 1 + w))
    elif h0 == "层":                                  # 分层：每(层)相邻 v3/default 两行，逐列比
        for i in range(0, len(rows) - 1, 2):
            for c in range(3, len(head)):
                d = LAYERED_DIR.get(head[c].strip(), 0)
                if c >= len(rows[i]) or c >= len(rows[i + 1]):
                    continue
                w = better(fnum(rows[i][c]), fnum(rows[i + 1][c]), d)
                if w >= 0:
                    out.add((i + w, c))
    elif h0 == "月":                                  # 趋势：v3RTP(2)vs对照RTP(3)、v3D1(6)vs对照D1(7)
        for i, r in enumerate(rows):
            for (ca, cb) in [(2, 3), (6, 7)]:
                w = better(fnum(r[ca]), fnum(r[cb]), 1)
                if w >= 0:
                    out.add((i, ca + w))
    return out


def render_table(head, rows):
    hl = hl_cells(head, rows)
    th = "".join(f"<th>{inline(c)}</th>" for c in head)
    body = ""
    for i, r in enumerate(rows):
        tds = ""
        for c, cell in enumerate(r):
            tds += (f'<td data-highlight-colour="{HL}">{inline(cell)}</td>' if (i, c) in hl
                    else f"<td>{inline(cell)}</td>")
        body += f"<tr>{tds}</tr>"
    return f'<table data-layout="full-width"><tbody><tr>{th}</tr>{body}</tbody></table>'


def md_to_storage(md):
    lines = md.split("\n"); out, i, para = [], 0, []
    def flush():
        if para:
            out.append("<p>" + "<br/>".join(inline(x) for x in para) + "</p>"); para.clear()
    while i < len(lines):
        st = lines[i].strip()
        m = re.match(r"(#{1,4})\s+(.*)", st)
        if m:
            flush(); out.append(f"<h{len(m.group(1))}>{inline(m.group(2))}</h{len(m.group(1))}>"); i += 1; continue
        if st == "---":
            flush(); out.append("<hr/>"); i += 1; continue
        if st.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s:|-]+\|?$", lines[i + 1].strip()):
            flush()
            cells = lambda row: [c.strip() for c in row.strip().strip("|").split("|")]
            head = cells(st); i += 2; rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(cells(lines[i].strip())); i += 1
            out.append(render_table(head, rows)); continue
        if re.match(r"^[-*]\s+", st):
            flush(); items = []
            while i < len(lines) and re.match(r"^[-*]\s+", lines[i].strip()):
                items.append(inline(re.sub(r"^[-*]\s+", "", lines[i].strip()))); i += 1
            out.append("<ul>" + "".join(f"<li>{x}</li>" for x in items) + "</ul>"); continue
        if st.startswith(">"):
            flush(); out.append(f'<p><em>{inline(st.lstrip(">").strip())}</em></p>'); i += 1; continue
        if st == "":
            flush()
        else:
            para.append(st)
        i += 1
    flush()
    return "\n".join(out)


def main(do_publish):
    body = md_to_storage(MD.read_text(encoding="utf-8"))
    print(f"HTML 长度 {len(body)}；标黄格 {body.count('data-highlight-colour')}")
    if not do_publish:
        print("(dry-run)"); return
    env = load_env()
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"])
    r = requests.get(f"{base}/rest/api/content", auth=auth,
                     params={"spaceKey": SPACE, "title": TITLE, "expand": "version"}, timeout=30)
    ex = r.json().get("results", []) if r.status_code == 200 else []
    if ex:
        pid = ex[0]["id"]; ver = ex[0]["version"]["number"] + 1
        r = requests.put(f"{base}/rest/api/content/{pid}", auth=auth, json={
            "id": pid, "type": "page", "title": TITLE, "space": {"key": SPACE},
            "version": {"number": ver}, "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=60)
        print("更新页面:", r.status_code)
    else:
        r = requests.post(f"{base}/rest/api/content", auth=auth, json={
            "type": "page", "title": TITLE, "space": {"key": SPACE}, "ancestors": [{"id": PARENT_ID}],
            "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=60)
        print("创建页面:", r.status_code)
    if r.status_code not in (200, 201):
        print(r.text[:800]); return
    print(f"完成。页面：{base}/spaces/{SPACE}/pages/{r.json()['id']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
