"""把「游戏调控效用总结」(md)发布为 Confluence 原生页面，放入 BL-HUB > AI Team。
月度指标表按列把最高值单元格背景标黄（不加粗）；其他表保持原样。全宽表格。
用法：python3 jobs/ab_testing/multi_game_confluence.py [--publish]
"""
from __future__ import annotations
import re, sys, html, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis"))
from math_table_confluence import load_env   # noqa: E402

MD = ROOT / "prd" / "multi_game" / "综合调控效用报告_v0.1.md"
SPACE = "productsha"; PARENT_ID = "50233350"; TITLE = "游戏调控效用总结 —— FM01/SS03/SS01/SS02/SS06（分时段）"
HL = "#fff0b3"


def esc(s): return html.escape(s)
def inline(s):
    s = esc(s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<em>\1</em>", s)
    s = re.sub(r"`([^`]+?)`", r"<code>\1</code>", s)
    return s


def first_num(cell):
    m = re.search(r"-?\d+(?:\.\d+)?", cell.replace("−", "-").replace(",", "").replace("*", ""))
    return float(m.group()) if m else None


def render_table(head, rows):
    h0 = head[0].strip() if head else ""
    is_traj = h0 == "月" and (len(head) < 2 or head[1].strip() != "日龄层")   # 月度趋势表:按列全局最高
    is_ten = h0 == "月" and len(head) > 1 and head[1].strip() == "日龄层"      # 日龄表:每层跨月最高
    hl = set()

    def mark(idxs, c):
        vals = [(i, first_num(rows[i][c])) for i in idxs if c < len(rows[i]) and first_num(rows[i][c]) is not None]
        if vals:
            mx = max(v for _, v in vals)
            for i, v in vals:
                if abs(v - mx) < 1e-9:
                    hl.add((i, c))

    if is_traj:
        for c in range(1, len(head)):                             # 跳"月"列,全体行取最高
            mark(range(len(rows)), c)
    elif is_ten:
        from collections import defaultdict
        grp = defaultdict(list)
        for i, r in enumerate(rows):
            grp[r[1].strip()].append(i)                           # 按日龄层分组
        for c in range(2, len(head)):                             # 跳"月""日龄层"两列
            for idxs in grp.values():
                mark(idxs, c)                                     # 每层各自跨月取最高

    strip = is_traj or is_ten
    th = "".join(f"<th><p>{inline(c)}</p></th>" for c in head)
    body = ""
    for i, r in enumerate(rows):
        tds = ""
        for c, cell in enumerate(r):
            cc = cell.replace("**", "") if strip else cell        # 去加粗（改用标黄）
            # Confluence 单元格内容需包在 <p> 里，背景色才渲染
            tds += (f'<td data-highlight-colour="{HL}"><p>{inline(cc)}</p></td>' if (i, c) in hl else f"<td><p>{inline(cc)}</p></td>")
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
