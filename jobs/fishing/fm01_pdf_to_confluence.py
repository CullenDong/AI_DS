"""把本地完整汇报 PDF 逐页渲染成图片，替换到已存在的 Confluence 页面（不连 Redshift、不重算）。

- 逐页 PDF→PNG（macOS CoreGraphics via ctypes）
- 每页图片作为附件内嵌，页面正文按顺序展示 17 页
- PDF 本身作为附件供下载
- 更新已存在页面（PUT，version+1）；凭证取自 .env
用法：python3 jobs/fishing/fm01_pdf_to_confluence.py [--publish]
"""
from __future__ import annotations
import ctypes, ctypes.util, os, re, sys, argparse
from ctypes import c_void_p, c_char_p, c_int, c_size_t, c_double, c_uint32, Structure
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[2]
PDF = ROOT / "docs" / "FM01_个性化挽留_完整汇报.pdf"
IMGDIR = ROOT / "data" / "output" / "conf_pdf_pages"; IMGDIR.mkdir(parents=True, exist_ok=True)
SPACE = "productsha"          # BL-HUB
TITLE = "FM01 个性化挽留组 · 分析汇报"

# ---------------- PDF → PNG（CoreGraphics） ----------------
cf = ctypes.cdll.LoadLibrary(ctypes.util.find_library("CoreFoundation"))
cg = ctypes.cdll.LoadLibrary(ctypes.util.find_library("CoreGraphics"))
io = ctypes.cdll.LoadLibrary(ctypes.util.find_library("ImageIO"))
cf.CFStringCreateWithCString.restype = c_void_p; cf.CFStringCreateWithCString.argtypes = [c_void_p, c_char_p, c_uint32]
cf.CFURLCreateWithFileSystemPath.restype = c_void_p; cf.CFURLCreateWithFileSystemPath.argtypes = [c_void_p, c_void_p, c_int, c_int]
cg.CGPDFDocumentCreateWithURL.restype = c_void_p; cg.CGPDFDocumentCreateWithURL.argtypes = [c_void_p]
cg.CGPDFDocumentGetNumberOfPages.restype = c_size_t; cg.CGPDFDocumentGetNumberOfPages.argtypes = [c_void_p]
cg.CGPDFDocumentGetPage.restype = c_void_p; cg.CGPDFDocumentGetPage.argtypes = [c_void_p, c_size_t]
class CGRect(Structure): _fields_ = [("x", c_double), ("y", c_double), ("w", c_double), ("h", c_double)]
cg.CGPDFPageGetBoxRect.restype = CGRect; cg.CGPDFPageGetBoxRect.argtypes = [c_void_p, c_int]
cg.CGColorSpaceCreateDeviceRGB.restype = c_void_p
cg.CGBitmapContextCreate.restype = c_void_p; cg.CGBitmapContextCreate.argtypes = [c_void_p, c_size_t, c_size_t, c_size_t, c_size_t, c_void_p, c_uint32]
cg.CGContextDrawPDFPage.argtypes = [c_void_p, c_void_p]
cg.CGContextScaleCTM.argtypes = [c_void_p, c_double, c_double]
cg.CGContextSetRGBFillColor.argtypes = [c_void_p, c_double, c_double, c_double, c_double]
cg.CGContextFillRect.argtypes = [c_void_p, CGRect]
cg.CGBitmapContextCreateImage.restype = c_void_p; cg.CGBitmapContextCreateImage.argtypes = [c_void_p]
io.CGImageDestinationCreateWithURL.restype = c_void_p; io.CGImageDestinationCreateWithURL.argtypes = [c_void_p, c_void_p, c_size_t, c_void_p]
io.CGImageDestinationAddImage.argtypes = [c_void_p, c_void_p, c_void_p]
io.CGImageDestinationFinalize.restype = c_int; io.CGImageDestinationFinalize.argtypes = [c_void_p]
UTF8 = 0x08000100
def _cfstr(s): return cf.CFStringCreateWithCString(None, s.encode(), UTF8)

def render_pages(pdf_path: Path, scale: float = 2.0):
    doc = cg.CGPDFDocumentCreateWithURL(cf.CFURLCreateWithFileSystemPath(None, _cfstr(str(pdf_path)), 0, False))
    n = cg.CGPDFDocumentGetNumberOfPages(doc)
    out = []
    for p in range(1, n + 1):
        page = cg.CGPDFDocumentGetPage(doc, p)
        r = cg.CGPDFPageGetBoxRect(page, 0)
        W, H = int(r.w * scale), int(r.h * scale)
        ctx = cg.CGBitmapContextCreate(None, W, H, 8, 0, cg.CGColorSpaceCreateDeviceRGB(), 1 | (2 << 12))
        cg.CGContextSetRGBFillColor(ctx, 1, 1, 1, 1); cg.CGContextFillRect(ctx, CGRect(0, 0, W, H))
        cg.CGContextScaleCTM(ctx, scale, scale); cg.CGContextDrawPDFPage(ctx, page)
        img = cg.CGBitmapContextCreateImage(ctx)
        name = f"page_{p:02d}.png"; fp = IMGDIR / name
        dest = io.CGImageDestinationCreateWithURL(cf.CFURLCreateWithFileSystemPath(None, _cfstr(str(fp)), 0, False), _cfstr("public.png"), 1, None)
        io.CGImageDestinationAddImage(dest, img, None); io.CGImageDestinationFinalize(dest)
        out.append((name, fp))
    return n, out

# ---------------- 组装正文 ----------------
def build_body(pages):
    parts = ["<p>本页为 FM01 个性化挽留组完整汇报（共 %d 页），逐页如下；完整 PDF 见页尾附件。</p>" % len(pages)]
    for name, _ in pages:
        parts.append(f'<p><ac:image ac:width="900"><ri:attachment ri:filename="{name}"/></ac:image></p>')
    parts.append('<hr/><p><strong>附件：</strong><ac:link><ri:attachment ri:filename="FM01_个性化挽留_完整汇报.pdf"/></ac:link></p>')
    return "\n".join(parts)

# ---------------- 发布 ----------------
def load_env():
    env = {}
    for ln in open(ROOT / ".env"):
        m = re.match(r'\s*export\s+(\w+)=(.*)', ln.strip())
        if m: env[m.group(1)] = m.group(2).strip().strip('"').strip("'")
    return env

def publish(pages, body):
    env = load_env()
    base = env["CONFLUENCE_URL"].rstrip("/"); base = base if base.endswith("/wiki") else base + "/wiki"
    auth = (env["CONFLUENCE_EMAIL"], env["CONFLUENCE_TOKEN"]); H = {"X-Atlassian-Token": "no-check"}
    r = requests.get(f"{base}/rest/api/content", auth=auth,
                     params={"spaceKey": SPACE, "title": TITLE, "expand": "version"}, timeout=30)
    existing = r.json().get("results", []) if r.status_code == 200 else []
    if not existing:
        print("未找到已存在页面（space/title 不匹配），中止。空间", SPACE, "标题", TITLE); return
    pid = existing[0]["id"]; ver = existing[0]["version"]["number"] + 1
    r = requests.put(f"{base}/rest/api/content/{pid}", auth=auth, json={
        "id": pid, "type": "page", "title": TITLE, "space": {"key": SPACE},
        "version": {"number": ver}, "body": {"storage": {"value": body, "representation": "storage"}}}, timeout=60)
    print("更新页面:", r.status_code)
    if r.status_code != 200:
        print(r.text[:400]); return
    for name, path in pages:
        with open(path, "rb") as f:
            requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                          files={"file": (name, f, "image/png")}, data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=60)
    with open(PDF, "rb") as f:
        requests.post(f"{base}/rest/api/content/{pid}/child/attachment", auth=auth, headers=H,
                      files={"file": ("FM01_个性化挽留_完整汇报.pdf", f, "application/pdf")},
                      data={"minorEdit": "true", "allowDuplicated": "true"}, timeout=120)
    print("附件已上传（%d 图 + 1 PDF）。页面: %s" % (len(pages), f"{base}/spaces/{SPACE}/pages/{pid}"))

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--publish", action="store_true"); a = ap.parse_args()
    n, pages = render_pages(PDF)
    body = build_body(pages)
    print(f"渲染 {n} 页 -> {IMGDIR}；正文长度 {len(body)}")
    if a.publish: publish(pages, body)
    else: print("(dry-run；加 --publish 实际更新页面)")
