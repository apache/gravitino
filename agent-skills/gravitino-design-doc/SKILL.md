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
name: gravitino-design-doc
description: Use when writing or reviewing a design document PR for the Apache Gravitino project
---

# Gravitino Design Document

## Overview

All Gravitino design documents must cover the problem, scope, and solution in a consistent structure so reviewers can evaluate them efficiently.

## Required Sections (in order)

### 1. Apache License Header
HTML comment block — always the very first block in the file.

```html
<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  ...
-->
```

### 2. Title (H1)
- `# Design of <Feature> in Gravitino`, or
- `# Design: <Feature> for Apache Gravitino`

### 3. Background
Describe **current state** and **what's wrong with it** — not the solution.
- What does Gravitino currently do (or not do)?
- Why is this a problem now?
- Use tables, code blocks, or ASCII diagrams for architecture/current-state illustration.

### 4. Goals
Numbered list. Each goal must be **concrete and verifiable**. Bold the goal name.

```markdown
1. **Multi-Engine Compatibility**: Views managed by Gravitino are visible and manageable across engines.
```

Avoid vague goals like "improve performance" or "better support X."

### 5. Non-Goals (strongly recommended)
Numbered list. Each non-goal must state **what is out of scope and briefly why**.

```markdown
1. **SQL Transpilation**: Gravitino will not convert SQL between dialects. Users must provide correct SQL per dialect.
```

Non-Goals prevent scope creep during review and implementation.

### 6. Solution Investigations
Document the alternative approaches considered and why they were accepted or rejected. This section helps reviewers understand the design space and validates that the proposal is the best fit.

Each alternative should cover:
- **What it is**: a brief description of the approach
- **Pros**: why it is attractive
- **Cons / why rejected**: the concrete reason it was not chosen

**Table format (for 2–4 short alternatives):**

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Option A | Fast, simple | Breaks backward compat | Rejected |
| Option B (chosen) | Backward compatible, extensible | Slightly more complex | **Chosen** |

**Sub-section format (for deep alternatives):**

```markdown
### Option A: <Name>
<Description>
**Pros:** ...
**Cons:** ...
**Decision:** Rejected because ...

### Option B: <Name> (Chosen)
...
```

If only one approach was seriously considered, explain briefly why alternatives were dismissed early.

### 7. Proposal
The actual design for the chosen approach. Use sub-sections. Should cover at minimum:
- API changes (new interfaces, REST endpoints, or CLI commands)
- Data/metadata model (new entities, fields, schemas)
- Implementation approach or algorithm
- Backward compatibility impact

### 8. Task Breakdown
A flat checklist of the concrete implementation tasks required to deliver this feature. Each task should map to one GitHub issue or PR.

Rules:
- Each task must be **actionable** — a developer should be able to pick it up and start without further clarification
- Order tasks so that dependencies come first
- Mark tasks that can be parallelized with `(parallel)` if helpful
- Do not bundle unrelated changes into one task

**Format:**

```markdown
- [ ] Define `<Interface>` API in `api/` module
- [ ] Implement `<Class>` in `core/` module
- [ ] Add REST endpoint `POST /api/metalakes/{metalake}/...` in `server/`
- [ ] Add Java client support in `clients/client-java/`
- [ ] Add Python client support in `clients/client-python/`
- [ ] Write unit tests for `<Class>`
- [ ] Write integration tests (Docker) for end-to-end flow
- [ ] Update OpenAPI spec (`docs/open-api/*.yaml`) and validate with `./gradlew :docs:build`
- [ ] Update user-facing documentation in `docs/`
```

Group tasks by phase if the feature spans multiple releases:

```markdown
### Phase 1: Core API and Storage
- [ ] ...

### Phase 2: Engine Connector Support
- [ ] ...
```

## Quality Checklist

Before submitting a design doc PR, verify:

- [ ] Apache License HTML comment is the first block in the file
- [ ] Background describes the current state **and** the problem with it (not just the desired future)
- [ ] Goals are concrete — each one is verifiable when the feature ships
- [ ] Non-Goals explicitly carve out scope with a one-line rationale each
- [ ] Solution Investigations documents at least two approaches (or explains why only one was viable)
- [ ] Each rejected alternative has a concrete reason for rejection, not just "Option B is better"
- [ ] Proposal has at least one concrete sub-section (API, data model, or algorithm)
- [ ] Task Breakdown has one task per GitHub issue/PR, ordered by dependency
- [ ] Complex structures use tables, code blocks, or diagrams — no wall-of-text paragraphs
- [ ] Section separators (`---`) between major sections for readability

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Missing license header | Add the Apache License HTML comment block at top |
| Background describes the solution instead of the problem | Rewrite to describe current state and its shortcomings |
| Vague goals ("improve X", "support Y better") | Rewrite as concrete, verifiable statements |
| Missing Non-Goals | Add explicit Non-Goals with brief rationale for each |
| Solution Investigations only lists the chosen approach | Document rejected alternatives with specific reasons |
| Rejected alternatives say "too complex" with no detail | Explain the specific complexity or constraint that disqualifies it |
| Proposal skips API/data model and jumps to implementation | Add API design and data model before implementation details |
| Goals have no bold label | Use `**Label**: Description` format |
| Task Breakdown bundles multiple concerns into one task | Split into one task per issue/PR |
| Task Breakdown is unordered with no dependency awareness | Order so dependencies come first |

