"""发布 SS03 high_player_test_v1 实验总结到 Confluence，每张表每列最高值标黄。
- 组表(表头首列='组'):每列(跳组名/斜杠列)跨行取最高 → 黄底。
- 投注额档表(表头首列='档'):按"档"分组(非空档起新组),每档内每列取最高 → 黄底。
复用 math_table_confluence 的 md→storage（monkeypatch render_table）。
用法: python3 jobs/ss03_analysis/ss03_hpt_report_confluence.py [--publish]
"""
from __future__ import annotations
import sys, argparse, requests
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis"))
import math_table_confluence as mt  # noqa: E402

MD = ROOT / "prd" / "ab_testing" / "math_table" / "SS03_high_player_test_v1_实验总结_v0.1.md"
SPACE = "productsha"; PARENT = "312574166"  # AI Team > Math
TITLE = "SS03 · high_player_test_v1 高价值玩家数学表 A/B · 实验总结 v0.1"


def render_hl(head, rows):
    """每列最高值标黄；'档'表按档分组取组内最高。"""
    h0 = head[0].strip() if head else ""
    tier = h0 == "档"
    # 行分组
    if tier:
        groups, cur = [], []
        for i, r in enumerate(rows):
            if r and r[0].strip() and cur:
                groups.append(cur); cur = [i]
            else:
                cur.append(i)
        if cur:
            groups.append(cur)
    else:
        groups = [list(range(len(rows)))]
    hl = set()
    for gi in groups:
        for c in range(1, len(head)):
            vals = []
            for i in gi:
                if c < len(rows[i]):
                    cell = rows[i][c]
                    if "/" in cell:           # 斜杠列(中/均)不标
                        continue
                    v = mt.first_num(cell)
                    if v is not None:
                        vals.append((i, v))
            if vals:
                mx = max(v for _, v in vals)
                for i, v in vals:
                    if abs(v - mx) < 1e-9:
                        hl.add((i, c))
    th = "".join(f"<th><p>{mt.inline(c)}</p></th>" for c in head)
    body = ""
    for i, r in enumerate(rows):
        tds = ""
        for c, cell in enumerate(r):
            cc = cell.replace("**", "")
            tds += (f'<td data-highlight-colour="{mt.HL}"><p>{mt.inline(cc)}</p></td>'
                    if (i, c) in hl else f"<td><p>{mt.inline(cc)}</p></td>")
        body += f"<tr>{tds}</tr>"
    return f'<table data-layout="full-width"><tbody><tr>{th}</tr>{body}</tbody></table>'


def main(do_publish):
    mt.render_table = render_hl                       # monkeypatch：全表标黄
    body = mt.md_to_storage(MD.read_text(encoding="utf-8"))
    print(f"HTML 长度 {len(body)}；标黄格 {body.count('data-highlight-colour')}；ac:image {body.count('ac:image')}")
    if not do_publish:
        print("(dry-run)"); return
    env = mt.load_env(); base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"])
    import time
    for i in range(4):
        try:
            r = requests.get(f"{base}/rest/api/content", auth=auth, params={"spaceKey": SPACE, "title": TITLE, "expand": "version"}, timeout=60)
            ex = r.json().get("results", [])
            if ex:
                pid = ex[0]["id"]; ver = ex[0]["version"]["number"] + 1
                r = requests.put(f"{base}/rest/api/content/{pid}", auth=auth, json={"id": pid, "type": "page", "title": TITLE, "space": {"key": SPACE}, "version": {"number": ver}, "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=90)
            else:
                r = requests.post(f"{base}/rest/api/content", auth=auth, json={"type": "page", "title": TITLE, "space": {"key": SPACE}, "ancestors": [{"id": PARENT}], "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=90)
                pid = r.json().get("id")
            if r.status_code in (200, 201):
                mt.upload_images(base, auth, pid, MD)
                print("发布", r.status_code, "→", f"{base}/spaces/{SPACE}/pages/{pid}"); return
            print(r.status_code, r.text[:300]); return
        except Exception as e:
            print("重试", i, type(e).__name__); time.sleep(5)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
