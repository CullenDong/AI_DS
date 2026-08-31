"""把 PDF 指定页渲染成 PNG（macOS CoreGraphics，via ctypes）。用法：
    python3 jobs/fishing/_render_pdf_page.py <pdf> <page,page,...> <out_prefix>
仅用于本地核对报告版式，不修改原 PDF。
"""
import ctypes, ctypes.util, sys
from ctypes import c_void_p, c_char_p, c_int, c_size_t, c_double, c_uint32, Structure

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
def cfstr(s): return cf.CFStringCreateWithCString(None, s.encode(), UTF8)

pdf_path, pages, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
doc = cg.CGPDFDocumentCreateWithURL(cf.CFURLCreateWithFileSystemPath(None, cfstr(pdf_path), 0, False))
n = cg.CGPDFDocumentGetNumberOfPages(doc)
print("页数", n)
for p in [int(x) for x in pages.split(",")]:
    page = cg.CGPDFDocumentGetPage(doc, p)
    r = cg.CGPDFPageGetBoxRect(page, 0); scale = 2.0
    W, H = int(r.w * scale), int(r.h * scale)
    ctx = cg.CGBitmapContextCreate(None, W, H, 8, 0, cg.CGColorSpaceCreateDeviceRGB(), 1 | (2 << 12))
    cg.CGContextSetRGBFillColor(ctx, 1, 1, 1, 1); cg.CGContextFillRect(ctx, CGRect(0, 0, W, H))
    cg.CGContextScaleCTM(ctx, scale, scale); cg.CGContextDrawPDFPage(ctx, page)
    img = cg.CGBitmapContextCreateImage(ctx)
    out = f"{prefix}_p{p}.png"
    dest = io.CGImageDestinationCreateWithURL(cf.CFURLCreateWithFileSystemPath(None, cfstr(out), 0, False), cfstr("public.png"), 1, None)
    io.CGImageDestinationAddImage(dest, img, None)
    print("写", out, "ok", io.CGImageDestinationFinalize(dest))
