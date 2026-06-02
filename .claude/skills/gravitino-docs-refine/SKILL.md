<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

---
name: gravitino-docs-refine
description: Use when refining Gravitino Markdown docs to match docs/STYLE.md and pass docs audit scripts
allowed-tools: Bash
---

# Gravitino Docs Style Refinement

## Overview

Refine Markdown files under `docs/` with focused, reviewable edits that follow `docs/STYLE.md`.
Always validate with the audit scripts under `scripts/` before finishing.

## Refinement Workflow

1. Read `docs/STYLE.md` and the target file end to end.
2. Apply focused style and structure edits only.
3. Run the audit scripts in the required order.
4. Fix issues by file and line number.
5. Re-run the full script set until all checks pass.

## Audit Scripts (Required)

Run from the repository root.

| Script | What it checks | When to run |
|---|---|---|
| `scripts/title_h2_audit.py` | First body H2 should not duplicate frontmatter `title` | After title or opening-section edits |
| `scripts/intro_audit.py` | Body content should not appear before the first H2 | After restructuring page openings |
| `scripts/heading_audit.py` | Heading hierarchy: first heading is H2 and no level jumps (`H2 -> H4`) | After heading edits |
| `scripts/heading_spacing_audit.py` | One blank line before and after each `H2`-`H6` heading | After formatting sweeps |
| `scripts/check_links.py` | Internal Markdown links and anchors under `docs/` | Final quality gate before PR |

## Script Usage

```bash
python3 scripts/title_h2_audit.py
python3 scripts/intro_audit.py
python3 scripts/heading_audit.py
python3 scripts/heading_spacing_audit.py
python3 scripts/check_links.py docs/
```

Run as a single gate when finishing:

```bash
python3 scripts/title_h2_audit.py \
  && python3 scripts/intro_audit.py \
  && python3 scripts/heading_audit.py \
  && python3 scripts/heading_spacing_audit.py \
  && python3 scripts/check_links.py docs/
```

## Completion Criteria

- Target docs comply with `docs/STYLE.md`.
- All five scripts exit with code `0`.
- No unrelated files are changed.
