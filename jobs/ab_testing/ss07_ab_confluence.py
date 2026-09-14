"""把 SS07 数学表 AB 方案(md)发布/更新为 Confluence 原生页面(文字+原生表格+结构图),
放在 slot machine data 文件夹。从 md 转原生(自动同步),嵌入渲染好的结构图 PNG。
用法：python3 jobs/ab_testing/ss07_ab_confluence.py [--publish]
"""
from __future__ import annotations
import re, sys, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ab_testing"))
from ab_summary_confluence import md_to_storage, load_env  # 复用 md→storage 转换

MD = ROOT / "prd" / "ab_testing" / "SS07_数学表AB方案_v0.1.md"
DIAG = ROOT / "prd" / "ab_testing" / "SS07_分组结构图.png"
DIAG_NAME = "SS07_分组结构图.png"
SPACE = "productsha"
PARENT_ID = "122421319"          # slot machine data (BL-HUB > AI Team > Data)
TITLE = "SS07 AB组方案"


def build_body():
    md = MD.read_text(encoding="utf-8")
    md = re.sub(r"```mermaid.*?```", "", md, flags=re.S)      # 去 mermaid,用渲染好的 PNG 代替
    body, _ = md_to_storage(md)
    img = f'<p><ac:image ac:width="920"><ri:attachment ri:filename="{DIAG_NAME}"/></ac:image></p>\n'
    return img + body


def main(do_publish):
    body = build_body()
    print(f"HTML 长度 {len(body)}；结构图 {DIAG.name}（存在 {DIAG.exists()}）")
    if not do_publish:
        print("(dry-run；加 --publish 实际更新)"); return
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
            "type": "page", "title": TITLE, "space": {"key": SPACE}, "ancestors": [{"id": PARENT_ID}],
            "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=60)
        print("创建页面:", r.status_code)
    if r.status_code not in (200, 201):
        print(r.text[:500]); return
    pid = r.json()["id"]
    # 附件按 ID 更新(避免 allowDuplicated 不更新版本的坑)
    ea = requests.get(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, params={"limit": 100}, timeout=30).json().get("results", [])
    aid = {a["title"]: a["id"] for a in ea}
    with open(DIAG, "rb") as f:
        files = {"file": (DIAG_NAME, f, "image/png")}
        if DIAG_NAME in aid:
            u = f"{base}/rest/api/content/{pid}/child/attachment/{aid[DIAG_NAME]}/data"
        else:
            u = f"{base}/rest/api/content/{pid}/child/attachment"
        sc = requests.post(u, auth=auth, headers=H, files=files, data={"minorEdit": "true"}, timeout=60).status_code
    print(f"结构图附件: {sc}。页面: {base}/spaces/{SPACE}/pages/{pid}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
