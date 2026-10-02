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

# Design documents

This directory holds Gravitino design documents. Structure and formatting are
separate: write the argument first, then match the Markdown standard.

| Need                                            | Follow                                                                           | Check                    |
| ----------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------ |
| Section structure (Background, Goals, Proposal) | [gravitino-design-doc skill](../agent-skills/gravitino-design-doc/SKILL.md)      | Skill quality checklist  |
| Markdown formatting (tables, line length)       | [Markdown formatting](markdown-formatting.md)                                    | `./gradlew markdownlint` |

## People

1. Use the skill's section order. Do not invent a second template.
2. Format tables and prose as in [markdown-formatting.md](markdown-formatting.md).
3. Run `./gradlew markdownlint` before opening a design-doc PR.

## Agents

When writing or reviewing a design document:

1. Load `agent-skills/gravitino-design-doc/SKILL.md` for section structure.
2. Follow `design-docs/markdown-formatting.md`. Do not invent extra house style.
3. Run `./gradlew markdownlint`.

Prompt example: `Write a design doc for <feature> using the gravitino-design-doc skill.`
