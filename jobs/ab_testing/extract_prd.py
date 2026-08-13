import re, html, sys

src = 'prd/ab_testing/2026-08-11 A-B 实验平台 PRD v0.2.html'
t = open(src, encoding='utf-8', errors='ignore').read()
t = re.sub(r'<script.*?</script>', ' ', t, flags=re.S)
t = re.sub(r'<style.*?</style>', ' ', t, flags=re.S)
t = re.sub(r'data:image[^"\')\s]+', '[IMG]', t)
t = re.sub(r'<[^>]+>', ' ', t)
t = html.unescape(t)
t = re.sub(r'[ \t\xa0]+', ' ', t)
t = re.sub(r'(\s*\n\s*)+', '\n', t).strip()
with open('prd/ab_testing/prd_text.txt', 'w', encoding='utf-8') as f:
    f.write(t)
print('OK chars:', len(t))
