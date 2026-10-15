<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Markdown formatting for Gravitino design documents

This is the formatting standard for `design-docs/`. It is not a second design-doc
template. For section structure (Background, Goals, Proposal, and so on), start
at [README.md](README.md) or the
[gravitino-design-doc skill](../agent-skills/gravitino-design-doc/SKILL.md).

The machine config is `.rumdl.toml`. That file starts from markdownlint defaults
and records only the overrides below.

## What we accept

| Layer         | Choice                                                                          |
| ------------- | ------------------------------------------------------------------------------- |
| Syntax        | GitHub Flavored Markdown                                                        |
| Default rules | markdownlint `default: true`                                                    |
| Tables        | Aligned columns, a pipe at both ends of every row, blank lines around the table |
| Runner        | rumdl, pinned in Gradle                                                         |

Do not invent extra house style. Change the standard by editing this document and
the matching override in `.rumdl.toml`.

## Overrides

| Rule  | Override                                 | Why                                                      |
| ----- | ---------------------------------------- | -------------------------------------------------------- |
| MD013 | 120 columns; skip tables and code blocks | GFM tables cannot wrap; design-doc samples are wide      |
| MD040 | off                                      | ASCII diagrams use unlabeled fences                      |
| MD055 | `leading_and_trailing`                   | Every table row starts and ends with a pipe              |
| MD060 | `aligned`                                | Reviewers keep asking for lined-up columns               |

## Enforcement

| Environment   | Command                            | Until the baseline format PR |
| ------------- | ---------------------------------- | ---------------------------- |
| Local         | `./gradlew markdownlint`           | Blocking on `design-docs/`   |
| Local format  | `./gradlew markdownlintFormat`     | Rewrites `design-docs/`      |
| Local/CI test | `./gradlew markdownlintSelfCheck`  | Blocking fixture tests       |
| CI            | GitHub Actions on `design-docs/`   | Warns; does not fail the job |

`./gradlew check` does not run Markdown lint yet. A follow-up PR will format
existing design docs, attach this task to `check`, and make CI blocking.

## Tables

Use a table for scanable comparisons. Keep cells short. Do not wrap a table row.

```markdown
| Approach | Decision |
| -------- | -------- |
| Option A | Rejected |
| Option B | Chosen |
```
