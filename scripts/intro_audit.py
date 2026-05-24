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

"""Find Markdown files where body content appears before the first H2 heading."""
import re
import sys
from pathlib import Path

FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---\n', re.DOTALL)


def audit(path):
    text = path.read_text(encoding='utf-8')
    fm = FRONTMATTER_RE.match(text)
    if not fm:
        return None, None
    body = text[fm.end():]

    body_lines = body.splitlines()
    fm_line_count = fm.group(0).count('\n')

    in_html_comment = False
    for i, line in enumerate(body_lines):
        stripped = line.strip()
        if not stripped:
            continue
        # Multi-line HTML comment (license header)
        if in_html_comment:
            if '-->' in stripped:
                in_html_comment = False
            continue
        if stripped.startswith('<!--') and '-->' not in stripped:
            in_html_comment = True
            continue
        if stripped.startswith('<!--') and stripped.endswith('-->'):
            continue
        # Single-line MDX import/export
        if stripped.startswith('import ') or stripped.startswith('export '):
            continue
        if line.startswith('## ') and not line.startswith('### '):
            return None, None  # already starts with an H2, good
        # Found a non-blank, non-boilerplate line that isn't an H2
        return fm_line_count + i + 1, line


def main():
    root = Path('docs') if Path('docs').exists() else Path('.')
    matches = []
    for md in sorted(root.rglob('*.md')):
        if md.name == 'STYLE.md':
            continue
        line_no, first_line = audit(md)
        if line_no is None:
            continue
        matches.append((md, line_no, first_line))

    if not matches:
        print("All docs open with an H2 after frontmatter.")
        return 0

    print(f"Found {len(matches)} file(s) where body content precedes the first H2:\n")
    for md, line_no, first_line in matches:
        kind = "H1"            if first_line.startswith('# ') and not first_line.startswith('## ') \
               else "H3"        if first_line.startswith('### ') \
               else "H4+"       if first_line.startswith('####') \
               else "prose"
        preview = first_line[:80] + ('...' if len(first_line) > 80 else '')
        print(f"  {md}")
        print(f"    L{line_no} ({kind}): {preview}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
