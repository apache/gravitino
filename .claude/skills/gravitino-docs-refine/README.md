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

# gravitino-docs-refine skill

A practical skill for refining Gravitino documentation so edits follow `STYLE.md` and pass the docs audit scripts.

## What it does

The skill guides a concise docs refinement workflow:

1. Apply focused style and heading-structure edits.
2. Run required audits from `.claude/skills/gravitino-docs-refine/scripts/`.
3. Fix reported issues and re-run until clean.

### Required script checks

| Script | Purpose | Command |
|---|---|---|
| `.claude/skills/gravitino-docs-refine/scripts/title_h2_audit.py` | Detect duplicate page-title H2 headings | `python3 .claude/skills/gravitino-docs-refine/scripts/title_h2_audit.py` |
| `.claude/skills/gravitino-docs-refine/scripts/intro_audit.py` | Find content that appears before the first H2 | `python3 .claude/skills/gravitino-docs-refine/scripts/intro_audit.py` |
| `.claude/skills/gravitino-docs-refine/scripts/heading_audit.py` | Detect heading-level order problems and level jumps | `python3 .claude/skills/gravitino-docs-refine/scripts/heading_audit.py` |
| `.claude/skills/gravitino-docs-refine/scripts/heading_spacing_audit.py` | Enforce blank-line spacing around headings | `python3 .claude/skills/gravitino-docs-refine/scripts/heading_spacing_audit.py` |
| `.claude/skills/gravitino-docs-refine/scripts/check_links.py` | Validate internal links and anchors in `docs/` | `python3 .claude/skills/gravitino-docs-refine/scripts/check_links.py docs/` |

## Installation

Project-local (already in this repository):

```bash
.claude/skills/gravitino-docs-refine
```

Optional global install:

```bash
cp -r .claude/skills/gravitino-docs-refine ~/.claude/skills/
```

## Usage

Ask Claude Code to apply the skill while editing docs pages:

> "Refine this docs page using the gravitino-docs-refine skill"

Recommended final gate:

```bash
python3 .claude/skills/gravitino-docs-refine/scripts/title_h2_audit.py \
  && python3 .claude/skills/gravitino-docs-refine/scripts/intro_audit.py \
  && python3 .claude/skills/gravitino-docs-refine/scripts/heading_audit.py \
  && python3 .claude/skills/gravitino-docs-refine/scripts/heading_spacing_audit.py \
  && python3 .claude/skills/gravitino-docs-refine/scripts/check_links.py docs/
```
