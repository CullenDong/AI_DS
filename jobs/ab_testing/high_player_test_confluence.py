"""把 SS03 high_player_test_v1 方案(md)发布为独立 Confluence 页面。
复用 math_table_confluence 的 md→storage 转换(支持表格标黄/围栏代码/全宽表)。
默认作为「Fancy Stuff（CBO）数学表优化」页的子页。
用法：python3 jobs/ab_testing/high_player_test_confluence.py [--publish]
"""
from __future__ import annotations
import sys, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs" / "ss03_analysis"))
import math_table_confluence as mt  # noqa: E402

MD = ROOT / "prd" / "ab_testing" / "math_table" / "SS03_high_player_test_v1.md"
SPACE = "productsha"
PARENT_ID = "1359249416"   # Fancy Stuff（CBO）数学表优化 页
TITLE = "SS03 · high_player_test_v1 高价值玩家数学表 A/B 方案"


def main(do_publish):
    body = mt.md_to_storage(MD.read_text(encoding="utf-8"))
    print(f"HTML 长度 {len(body)}；标黄单元格 {body.count('data-highlight-colour')} 个")
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
