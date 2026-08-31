"""把 AB 分组规格汇总（md）发布为 Confluence 原生页面（文字=文字·表格=原生·图=图片），
放入 BL-HUB > AI Team > Data > ab testing 文件夹。不连 Redshift。
用法：python3 jobs/ab_testing/ab_summary_confluence.py [--publish]
"""
from __future__ import annotations
import os, re, sys, html, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
MD = ROOT / "prd" / "ab_testing" / "AB分组规格汇总_v0.1.md"
PDF = ROOT / "prd" / "ab_testing" / "AB分组规格汇总_v0.1.pdf"
IMGDIR = ROOT / "prd" / "ab_testing"
SPACE = "productsha"
PARENT_ID = "1335066652"        # ab testing (BL-HUB > AI Team > Data)
TITLE = "AB 分组规格汇总 v0.1"

def esc(s): return html.escape(s)

def inline(s):
    s = esc(s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"`([^`]+?)`", r"<code>\1</code>", s)
    return s

def md_to_storage(md: str):
    """返回 (storage_html, [用到的图片文件名])。"""
    lines = md.split("\n")
    out, imgs, i = [], [], 0
    def flush_para(buf):
        if buf:
            out.append("<p>" + "<br/>".join(inline(x) for x in buf) + "</p>")
            buf.clear()
    para = []
    while i < len(lines):
        ln = lines[i].rstrip("\n")
        st = ln.strip()
        # 图片
        m = re.match(r"!\[[^\]]*\]\(([^)]+)\)", st)
        if m:
            flush_para(para)
            fn = m.group(1)
            imgs.append(fn)
            out.append(f'<p><ac:image ac:width="820"><ri:attachment ri:filename="{esc(fn)}"/></ac:image></p>')
            i += 1; continue
        # 标题
        m = re.match(r"(#{1,4})\s+(.*)", st)
        if m:
            flush_para(para)
            lvl = len(m.group(1)); out.append(f"<h{lvl}>{inline(m.group(2))}</h{lvl}>")
            i += 1; continue
        # 分隔线
        if st == "---":
            flush_para(para); out.append("<hr/>"); i += 1; continue
        # 代码围栏
        if st.startswith("```"):
            flush_para(para)
            lang = st[3:].strip() or "text"; code = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code.append(lines[i]); i += 1
            i += 1
            body = "\n".join(code)
            out.append(f'<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">{esc(lang)}</ac:parameter>'
                       f"<ac:plain-text-body><![CDATA[{body}]]></ac:plain-text-body></ac:structured-macro>")
            continue
        # 表格
        if st.startswith("|") and i + 1 < len(lines) and re.match(r"^\|[\s:|-]+\|?$", lines[i+1].strip()):
            flush_para(para)
            def cells(row): return [c.strip() for c in row.strip().strip("|").split("|")]
            head = cells(st); i += 2; rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(cells(lines[i].strip())); i += 1
            th = "".join(f"<th>{inline(c)}</th>" for c in head)
            body = ""
            for r in rows:
                body += "<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>"
            out.append(f"<table><tbody><tr>{th}</tr>{body}</tbody></table>")
            continue
        # 列表
        if re.match(r"^[-*]\s+", st):
            flush_para(para); items = []
            while i < len(lines) and re.match(r"^[-*]\s+", lines[i].strip()):
                items.append(inline(re.sub(r"^[-*]\s+", "", lines[i].strip()))); i += 1
            out.append("<ul>" + "".join(f"<li>{x}</li>" for x in items) + "</ul>")
            continue
        # 引用
        if st.startswith(">"):
            flush_para(para)
            out.append(f'<p><em>{inline(st.lstrip(">").strip())}</em></p>'); i += 1; continue
        # 空行 / 段落
        if st == "":
            flush_para(para)
        else:
            para.append(st)
        i += 1
    flush_para(para)
    return "\n".join(out), imgs

def load_env():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env

def main(do_publish):
    body, imgs = md_to_storage(MD.read_text(encoding="utf-8"))
    body += ('<hr/><p><strong>附件：</strong>'
             '<ac:link><ri:attachment ri:filename="AB分组规格汇总_v0.1.pdf"/></ac:link>（完整 16 页 PDF）</p>')
    present = [fn for fn in imgs if (IMGDIR / fn).exists()]
    missing = [fn for fn in imgs if not (IMGDIR / fn).exists()]
    print(f"HTML 长度 {len(body)}；图片引用 {len(imgs)}（存在 {len(present)}，缺 {missing}）")
    if not do_publish:
        print("(dry-run；加 --publish 实际发布)"); return
    env = load_env()
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"]); H = {"X-Atlassian-Token": "no-check"}
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
            "type": "page", "title": TITLE, "space": {"key": SPACE},
            "ancestors": [{"id": PARENT_ID}],
            "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=60)
        print("创建页面:", r.status_code)
    if r.status_code not in (200, 201):
        print(r.text[:500]); return
    pid = r.json()["id"]
    # 现有附件 filename -> id（用于更新已存在的附件数据，避免 allowDuplicated 不更新版本的坑）
    ea = requests.get(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth,
                      params={"limit": 100}, timeout=30).json().get("results", [])
    id_by_name = {a["title"]: a["id"] for a in ea}

    def upsert(fn, path, mime):
        with open(path, "rb") as f:
            files = {"file": (fn, f, mime)}
            if fn in id_by_name:
                # 更新已存在附件的数据 → 新版本（正确姿势）
                u = f"{base}/rest/api/content/{pid}/child/attachment/{id_by_name[fn]}/data"
                rr = requests.post(u, auth=auth, headers=H, files=files, data={"minorEdit": "true"}, timeout=120)
            else:
                u = f"{base}/rest/api/content/{pid}/child/attachment"
                rr = requests.post(u, auth=auth, headers=H, files=files, data={"minorEdit": "true"}, timeout=120)
        return rr.status_code

    for fn in present:
        sc = upsert(fn, IMGDIR / fn, "image/png")
        print(f"  附件 {fn}: {sc}{'（更新）' if fn in id_by_name else '（新建）'}")
    sc = upsert("AB分组规格汇总_v0.1.pdf", PDF, "application/pdf")
    print(f"  附件 PDF: {sc}")
    print(f"完成（{len(present)} 图 + 1 PDF）。页面：{base}/spaces/{SPACE}/pages/{pid}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
