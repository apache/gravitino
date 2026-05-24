#!/usr/bin/env python3
"""Find Markdown headings (H2-H6) that lack a blank line before or after."""
import re
import sys
from pathlib import Path

FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---\n', re.DOTALL)
HEADING_RE = re.compile(r'^(#{2,6})\s+(.+)$')
CODE_FENCE_RE = re.compile(r'^```')


def audit(path):
    text = path.read_text(encoding='utf-8')
    fm = FRONTMATTER_RE.match(text)
    if fm:
        body_start_line = fm.group(0).count('\n')
        body = text[fm.end():]
    else:
        body_start_line = 0
        body = text

    lines = body.splitlines()
    issues = []
    in_code_fence = False

    for i, line in enumerate(lines):
        if CODE_FENCE_RE.match(line):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence:
            continue

        m = HEADING_RE.match(line)
        if not m:
            continue

        file_line_no = body_start_line + i + 1

        # Check line before: skip if heading is at the very top of body
        if i > 0:
            prev = lines[i - 1]
            if prev.strip():
                issues.append((file_line_no, line.rstrip(), 'no blank line before'))

        # Check line after: skip if heading is the last line of body
        if i + 1 < len(lines):
            nxt = lines[i + 1]
            if nxt.strip():
                issues.append((file_line_no, line.rstrip(), 'no blank line after'))

    return issues


def main():
    root = Path('docs') if Path('docs').exists() else Path('.')
    total = 0
    for md in sorted(root.rglob('*.md')):
        if md.name == 'STYLE.md':
            continue
        issues = audit(md)
        if not issues:
            continue
        print()
        print(str(md))
        for line_no, heading, msg in issues:
            print(f"  L{line_no} ({msg}): {heading}")
            total += 1

    print()
    print(f"Total issues: {total}")
    return 0 if total == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
