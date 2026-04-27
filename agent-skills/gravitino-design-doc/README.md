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

# gravitino-design-doc skill

A Claude Code skill that enforces consistent structure and content quality for Apache Gravitino design documents.

## What it does

When writing or reviewing a design doc, the skill ensures the document covers all required sections in the correct order:

| # | Section | Required |
|---|---------|----------|
| 1 | **Apache License Header** | Yes |
| 2 | **Title** | Yes |
| 3 | **Background** | Yes |
| 4 | **Goals** | Yes |
| 5 | **Non-Goals** | Strongly recommended |
| 6 | **Solution Investigations** | Yes |
| 7 | **Proposal** | Yes |
| 8 | **Task Breakdown** | Yes |

For each section, the skill checks content quality — not just presence. For example, Goals must be concrete and verifiable, and Solution Investigations must document rejected alternatives with specific reasons.

## Installation

Copy the skill directory to Claude Code's skills folder:

```bash
# Global — available in all projects
cp -r agent-skills/gravitino-design-doc ~/.claude/skills/

# Project-local — available only inside this repo
cp -r agent-skills/gravitino-design-doc .claude/skills/
```

## Usage

### Writing a new design doc

Invoke the skill before you start writing:

```
/gravitino-design-doc
```

Claude will guide you through each section, prompting for the right content and flagging weak or missing parts before you open a PR.

### Reviewing a design doc PR

Ask Claude to review an existing doc against the skill's checklist:

> "Review this design doc using the gravitino-design-doc skill"

Claude will check each section against the quality checklist and report what needs to be fixed before the PR is ready to merge.

### Auto-trigger

The skill also activates automatically when you mention writing or reviewing a Gravitino design document, without needing an explicit invocation.
