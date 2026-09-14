"""把数学表(Fancy Stuff / CBO)A/B 总结报告(md)发布为 Confluence 原生页面，
放入 BL-HUB > AI Team > Data > slot machine data。
CBO 数据表(表头首列='组（表）')按列把最高值单元格背景标黄（data-highlight-colour）。
标黄规则：每列取「首要数值」的最大——三连值(中/均/均99、均/中、D1/D3/D7)取第一个数；
首列组名不参与；只对 CBO 表标黄，其余表(时间线/模拟/编码/总览)不动。
用法：python3 jobs/ss03_analysis/math_table_confluence.py [--publish]
"""
from __future__ import annotations
import re, sys, html, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
MD = ROOT / "prd" / "ab_testing" / "math_table" / "ab_summary_and_plan.md"
SPACE = "productsha"
PARENT_ID = "122421319"          # slot machine data (BL-HUB > AI Team > Data)
TITLE = "数学表(Fancy Stuff/CBO) A/B 实验总结与后续计划"
HL = "#fff0b3"                   # 标黄色


def esc(s): return html.escape(s)


def inline(s):
    s = esc(s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"<em>\1</em>", s)     # 单星斜体
    s = re.sub(r"`([^`]+?)`", r"<code>\1</code>", s)
    s = s.replace("&lt;br/&gt;", "<br/>")                           # 还原手写换行
    return s


def first_num(cell: str):
    """取单元格里第一个数值（处理 − 负号、千分位逗号、三连值取首个）。无数字返回 None。"""
    s = cell.replace("−", "-").replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


def render_table(head, rows):
    hl = head and head[0].startswith("组（表）")          # 只对 CBO 表标黄
    maxset = {}                                            # 列idx -> 该列最大值
    if hl:
        for c in range(1, len(head)):                     # 跳过首列组名
            vals = [first_num(r[c]) for r in rows if c < len(r)]
            nums = [v for v in vals if v is not None]
            if nums:
                maxset[c] = max(nums)
    th = "".join(f"<th>{inline(c)}</th>" for c in head)
    body = ""
    for r in rows:
        tds = ""
        for c, cell in enumerate(r):
            v = first_num(cell) if c in maxset else None
            if v is not None and abs(v - maxset[c]) < 1e-9:
                tds += f'<td data-highlight-colour="{HL}">{inline(cell)}</td>'
            else:
                tds += f"<td>{inline(cell)}</td>"
        body += f"<tr>{tds}</tr>"
    # data-layout=full-width：让表格铺满页面全宽（拉宽横向空间）
    return f'<table data-layout="full-width"><tbody><tr>{th}</tr>{body}</tbody></table>'


def md_to_storage(md: str):
    lines = md.split("\n"); out, i = [], 0
    para = []
    def flush():
        if para:
            out.append("<p>" + "<br/>".join(inline(x) for x in para) + "</p>"); para.clear()
    while i < len(lines):
        st = lines[i].strip()
        m = re.match(r"(#{1,4})\s+(.*)", st)
        if m:
            flush(); lvl = len(m.group(1)); out.append(f"<h{lvl}>{inline(m.group(2))}</h{lvl}>"); i += 1; continue
        if st == "---":
            flush(); out.append("<hr/>"); i += 1; continue
        mi = re.match(r"!\[.*?\]\((.+?)\)", st)                     # 图片：![alt](path) → 附件图
        if mi:
            flush(); fn = Path(mi.group(1)).name
            out.append(f'<ac:image ac:align="center" ac:width="820"><ri:attachment ri:filename="{esc(fn)}"/></ac:image>')
            i += 1; continue
        if st.startswith("```"):
            flush(); lang = st[3:].strip() or "text"; code = []; i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code.append(lines[i]); i += 1
            i += 1
            out.append(f'<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">{esc(lang)}</ac:parameter>'
                       f"<ac:plain-text-body><![CDATA[{chr(10).join(code)}]]></ac:plain-text-body></ac:structured-macro>")
            continue
        if st.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s:|-]+\|?$", lines[i+1].strip()):
            flush()
            def cells(row): return [c.strip() for c in row.strip().strip("|").split("|")]
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


def load_env():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env


def upload_images(base, auth, page_id, md_path):
    """把 md 里 ![](path) 引用的图片作为附件上传/更新到页面（path 相对仓库根）。"""
    md = Path(md_path).read_text(encoding="utf-8")
    hdr = {"X-Atlassian-Token": "nocheck"}
    ex = requests.get(f"{base}/rest/api/content/{page_id}/child/attachment", auth=auth,
                      params={"limit": 200}, timeout=30)
    have = {a["title"]: a["id"] for a in (ex.json().get("results", []) if ex.status_code == 200 else [])}
    for rel in re.findall(r"!\[.*?\]\((.+?)\)", md):
        fp = (ROOT / rel).resolve()
        if not fp.exists():
            print(f"  [图缺失] {rel}"); continue
        fn = fp.name
        with open(fp, "rb") as fh:
            files = {"file": (fn, fh, "image/png")}
            if fn in have:
                r = requests.post(f"{base}/rest/api/content/{page_id}/child/attachment/{have[fn]}/data",
                                  auth=auth, headers=hdr, files=files, timeout=60)
            else:
                r = requests.post(f"{base}/rest/api/content/{page_id}/child/attachment",
                                  auth=auth, headers=hdr, files=files, timeout=60)
        print(f"  附件 {fn}: {r.status_code}")


def main(do_publish):
    body = md_to_storage(MD.read_text(encoding="utf-8"))
    nhl = body.count("data-highlight-colour")
    print(f"HTML 长度 {len(body)}；标黄单元格 {nhl} 个")
    if not do_publish:
        print("(dry-run；加 --publish 实际发布)"); return
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
    pid = r.json()["id"]
    upload_images(base, auth, pid, MD)
    print(f"完成。页面：{base}/spaces/{SPACE}/pages/{pid}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
