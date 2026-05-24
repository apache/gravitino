#!/usr/bin/env python3
"""Find Markdown files where the first body H2 duplicates the frontmatter title."""
import re
import sys
from pathlib import Path

TITLE_RE = re.compile(r'^title:\s*["\']?(.*?)["\']?\s*$', re.MULTILINE)
H2_RE = re.compile(r'^##\s+(.+)$', re.MULTILINE)
FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---\n', re.DOTALL)


def normalize(s):
    s = s.lower().strip()
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def get_title_and_first_h2(path):
    text = path.read_text(encoding='utf-8')
    fm = FRONTMATTER_RE.match(text)
    if not fm:
        return None, None, None
    title_match = TITLE_RE.search(fm.group(1))
    if not title_match:
        return None, None, None
    title = title_match.group(1).strip().strip('"').strip("'")

    body = text[fm.end():]
    h2 = H2_RE.search(body)
    if not h2:
        return title, None, None

    # Detect if a later "## Introduction" exists (skipping the first H2 we found)
    later_intro = False
    for m in H2_RE.finditer(body):
        if m.start() == h2.start():
            continue
        if normalize(m.group(1)) == 'introduction':
            later_intro = True
            break

    return title, h2.group(1).strip(), later_intro


def main():
    root = Path('docs') if Path('docs').exists() else Path('.')
    matches = []
    for md in sorted(root.rglob('*.md')):
        if md.name == 'STYLE.md':
            continue
        title, h2, later_intro = get_title_and_first_h2(md)
        if not title or not h2:
            continue
        n_title = normalize(title)
        n_h2 = normalize(h2)
        if n_title == n_h2 or n_title in n_h2 or n_h2 in n_title:
            matches.append((md, title, h2, later_intro))

    if not matches:
        print("No duplicate-title H2s found.")
        return 0

    print(f"Found {len(matches)} candidate(s):\n")
    for md, title, h2, later_intro in matches:
        flag = " [HAS LATER ## Introduction — REVIEW]" if later_intro else ""
        print(f"  {md}")
        print(f"    title: {title!r}")
        print(f"    first H2: {h2!r}{flag}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
