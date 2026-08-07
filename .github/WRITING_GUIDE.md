<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# GitHub Writing Guide

Use this guide when drafting or revising pull request titles and descriptions,
GitHub Issues, and GitHub Discussions for Gravitino. It governs the metadata for
a design-document-only pull request, not the design document itself. It gives
contributors and coding agents one tool-neutral standard for producing readable,
reviewable artifacts.

Existing templates and forms define the required structure. Preserve their
headings, fields, title prefixes, and requested content. This guide defines how
to frame that content for the reader.

## Shared Style

- **Lead with the bottom line.** State what the reader needs to understand,
  review, answer, or decide, then explain why it matters.
- **Write top down.** Move from the outcome, problem, or question to context,
  boundaries, evidence, and supporting detail. Prefer causality to a chronology
  of work performed.
- **Cover what matters.** Use who, what, when, where, why, and how as a coverage
  check, not as required headings.
- **Practice smart brevity.** Use a clear title, direct opening, short
  paragraphs, and only enough detail for the artifact's purpose. Brief does not
  mean incomplete.
- **Support claims.** Link relevant issues, decisions, examples, measurements,
  and observed results. Distinguish facts from assumptions and proposals.
- **Put procedure last.** Leave source-obvious detail to the code. Put useful
  file, class, or step-by-step detail near the end in an optional `Deep dive`.
- **Stay proportional.** A small correction may need only a few sentences. A
  broad, risky, or layered change needs enough context and evidence to make its
  boundaries clear.

### Optional Callout Bullets

Callout bullets improve scanning; they are not fields to fill. Use them when two
or more parallel facts are easier to compare, after a short prose bottom line.
Testing or evaluation may begin directly with evidence bullets. Format each as
`- **Label**: explanation.` Keep labels short and items parallel.

Useful labels include:

- **Rationale**: `Problem`, `Why it matters`, `Evidence`, or `Why now`.
- **Change**: `New`, `Reused`, `This layer`, or `Deferred`.
- **Impact**: `Affected users`, `Visible change`, `Action required`, or
  `Unchanged`.
- **Verification**: `Unit tests`, `Integration tests`, `End-to-end tests`,
  `Static checks`, `Manual verification`, `Not run`, or `Coverage gap`.
- **Design**: `Direction`, `Scope`, `Non-goals`, `Alternative`, `Tradeoff`, or
  `Risk`.
- **Issues and Discussions**: `Problem`, `Desired outcome`, `Option`, or
  `Open question`.

Adapt this vocabulary to the content. Do not emit every label, repeat the
section heading, or use a list for one fact.

## Conciseness Pass

After completing a fact-grounded draft and before sharing or publishing it:

1. Identify the single bottom line the reader should retain.
2. Remove or merge repeated facts, reasons, and boundaries.
3. Delete details the diff, template, or source already makes clear. Move useful
   procedure to an optional `Deep dive` when it still adds value.
4. Tighten sentences, remove throat-clearing, then restore any required context,
   boundary, evidence, or template content lost during compression.

Repeat only while the draft becomes clearer. Stop when another pass would remove
meaning. Keep the effort proportional, preserve the established facts, and do
not invent information to bridge a shortened explanation.

## Pull Requests

Preserve the repository pull request template's title syntax, required headings,
and heading order. This guide controls the story within that structure.

Use the design-document narrative below only when the primary purpose is to
propose or materially revise a design decision and substantive changes are
limited to a document under `design-docs/` and its supporting assets. Code,
tests, build, configuration, ordinary documentation, minor design-doc
corrections, and mixed changes use the default narrative.

Treat the Conventional Commit type and scope as classification. Make the subject
a compact narrative: state the concrete outcome, then include the goal, impact,
or condition only when it distinguishes the pull request. Prefer what the change
fixes, enables, or clarifies over the procedure used to implement it.

For a design-document-only pull request, the outcome is the decision, contract,
or boundary the document defines, not the act of editing a document.

### Default Pull Requests

Follow a problem-first reviewer arc: need and why now, proposed outcome and
scope, user-facing boundary, then verification. Keep new behavior distinct from
reused or inherited behavior. For stacked work, identify the predecessor, this
layer's responsibility, and work deliberately left to a later layer.

Report only verification actually performed, with exact commands or checks,
observed results, and relevant environment details.

For material behavior changes, consider applicable unit, integration, and
end-to-end tests plus happy, negative, boundary, expected error or exception,
regression, and compatibility paths. These are coverage prompts, not a fixed
checklist. Never invent evidence. Name a material gap and why it remains when a
reviewer would reasonably expect coverage.

### Design Document Pull Requests

Follow a decision arc: need, shortest material background, proposed direction,
impact if adopted, then evaluation and document validation. Keep proposals
distinct from shipped behavior and do not claim consensus or implementation
tests that do not exist. Link relevant document sections instead of reproducing
its outline or technical detail.

For either route, put formal issue and stack relationships in the optional final
`Metadata` section, after any Deep dive. Use `Fixes:` only when the pull request
fully resolves the linked issue; otherwise use `Part of:` or `Related:`. Use
`Previous:` and `Next:` only when stack navigation helps. Link evidence and
context inline in the narrative instead of duplicating it in Metadata.

## Issues

Select the appropriate [Issue form](ISSUE_TEMPLATE/) and preserve its title
prefix and required fields.

Make the title state the problem or desired outcome. Lead with the problem and
the evidence that establishes it, including affected users or conditions when
relevant. Then state the desired outcome. Keep implementation ideas secondary
unless the approach has already been decided, and do not present a proposal as
approved direction. Put reproduction steps, task lists, and other required
procedural content in the fields where the selected form requests them.

## GitHub Discussions

Use a Discussion for an open question, request for input, or community
conversation, subject to the repository's communication and decision-making
practices.

Make the title state the question or requested decision. Open with the input
needed and why it matters, then present the relevant context, constraints,
options, tradeoffs, and evidence. Label proposals as proposals. Do not describe
an idea as consensus until the community has reached it. As the conversation
develops, keep a concise summary of material conclusions and unresolved points.

## Keep Published Writing Current

Treat titles and bodies as living descriptions. After significant code changes
or any material change to scope, behavior, boundaries, issue relationships,
decisions, or verification evidence, compare the published text with the latest
state and refresh it before requesting review or updating the published
artifact.
