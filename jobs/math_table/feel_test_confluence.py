"""把《数学表体感测试与优化流程方案》(md)发布为 Confluence 页面。
放入 BL-HUB > AI Team > Math（parent id 312574166）。
复用 math_table_confluence 的 md→storage（支持表格标黄/围栏代码/全宽表/图片附件）。
用法：python3 jobs/math_table/feel_test_confluence.py [--publish]
"""
from __future__ import annotations
import sys, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis"))
import math_table_confluence as mt  # noqa: E402

MD = ROOT / "prd" / "math_table" / "数学表体感测试与优化流程方案_v0.1.md"
SPACE = "productsha"
PARENT_ID = "312574166"   # BL-HUB > AI Team > Math
TITLE = "数学表体感测试与优化流程方案 v0.1"


def main(do_publish):
    body = mt.md_to_storage(MD.read_text(encoding="utf-8"))
    print(f"HTML 长度 {len(body)}；ac:image {body.count('ac:image')} 个")
    if not do_publish:
        print("(dry-run；加 --publish 实际发布)"); return
    env = mt.load_env()
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
    mt.upload_images(base, auth, pid, MD)
    print(f"完成。页面：{base}/spaces/{SPACE}/pages/{pid}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    main(a.publish)
