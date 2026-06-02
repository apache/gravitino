#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Audit Markdown heading hierarchy.

Reports two issue classes:
  1. First body heading is not H2 (file starts at H3+ with no H2 anywhere above).
  2. Heading level jumps (e.g., H2 -> H4 with no H3 between).

Skips headings inside fenced code blocks.
"""
import re
import sys
from pathlib import Path

FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---\n', re.DOTALL)
HEADING_RE = re.compile(r'^(#{1,6})\s+(.+?)\s*$')


def headings_in(path):
    """Yield (line_no, level, text) for each heading outside fenced code blocks."""
    text = path.read_text(encoding='utf-8')
    fm = FRONTMATTER_RE.match(text)
    body_offset = fm.end() if fm else 0
    body = text[body_offset:]
    fm_lines = text[:body_offset].count('\n')

    in_code = False
    for i, line in enumerate(body.splitlines()):
        s = line.lstrip()
        if s.startswith('```'):
            in_code = not in_code
            continue
        if in_code:
            continue
        m = HEADING_RE.match(line)
        if m:
            yield fm_lines + i + 1, len(m.group(1)), m.group(2).strip()


def audit_file(path):
    issues = []
    hs = list(headings_in(path))
    if not hs:
        return issues

    first_line, first_level, first_text = hs[0]
    if first_level != 2:
        issues.append({
            'class': 'first-heading-not-h2',
            'line': first_line,
            'msg': f"First body heading is H{first_level}, expected H2",
            'detail': f"{'#' * first_level} {first_text}",
        })

    prev_level = hs[0][1]
    for line, level, text in hs[1:]:
        if level > prev_level + 1:
            issues.append({
                'class': 'level-jump',
                'line': line,
                'msg': f"Heading level jumps from H{prev_level} to H{level}",
                'detail': f"{'#' * level} {text}",
            })
        prev_level = level

    return issues


def main():
    root = Path('docs') if Path('docs').exists() else Path('.')
    by_file = {}
    total = 0
    for md in sorted(root.rglob('*.md')):
        if md.name == 'STYLE.md':
            continue
        issues = audit_file(md)
        if issues:
            by_file[md] = issues
            total += len(issues)

    if total == 0:
        print("Total issues: 0")
        return 0

    print(f"Total issues: {total}\n")
    for md, issues in by_file.items():
        print(f"{md}")
        for it in issues:
            print(f"  L{it['line']} [{it['class']}]: {it['msg']}")
            print(f"    {it['detail']}")
    return 1


if __name__ == '__main__':
    sys.exit(main())
