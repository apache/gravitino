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

# Gravitino OpenAPI tooling

Lint, bundle, and codegen-check the OpenAPI spec under
[`docs/open-api`](../../docs/open-api). Run in CI by
[`.github/workflows/openapi.yml`](../../.github/workflows/openapi.yml).

Tool versions live in [`package.json`](./package.json) so Dependabot/Renovate
keep them current — validation always runs on up-to-date linters.

For the full picture — objectives, how it works, and how to publish the
artifact — see
[`docs/openapi-validation-and-publishing.md`](../../docs/openapi-validation-and-publishing.md).

## Layout

| File | Purpose |
|---|---|
| `package.json` | Pinned tool versions (`@redocly/cli`, `@stoplight/spectral-cli`, rulesets). |
| `redocly.yaml` | The `apis` Redocly lints/bundles (strictness is passed on the CLI). |
| `.spectral.yaml` | Governance ruleset: OWASP + documentation + Gravitino house rules. |

## Usage

```bash
cd dev/openapi
npm ci

npm run lint            # Redocly (recommended-strict) + Spectral (governance)
npm run lint:redocly    # structural validation; add --extends=minimal|recommended
npm run lint:spectral   # governance kitchen sink
npm run bundle          # -> build/openapi.json and build/openapi.yaml
```

Requires Node `20.19+` / `22.12+` / `23+` (Redocly v2).

## Design: expose-first, and crashes are signal

The CI pipeline is deliberately **warn-only** for now — every check is
non-blocking so the existing backlog surfaces as PR annotations without turning
`main` red. Stages:

1. **Redocly** at three strictness levels (`minimal`, `recommended`,
   `recommended-strict`) as a matrix, so the finding gradient is visible.
2. **Spectral** kitchen sink — `spectral:oas` + OWASP API-security +
   documentation completeness + Gravitino house rules.
3. **Bundle** → single `openapi.json`/`openapi.yaml` artifact.
4. **progenitor** Rust-client codegen smoke test (strictest consumer).
5. **oasdiff** breaking-change diff vs the PR base branch.

**Nothing is suppressed to force a pass.** In particular, `spectral:oas`'s
`duplicated-entry-in-enum` rule throws on any `null` node in the document
(its `@.enum` JSONPath filter is evaluated against every node). We do **not**
disable it. If Spectral aborts, the workflow detects the crash by its error
signature and raises a "investigate the spec" warning — a linter crash means the
document is malformed enough to break the tool, which is a stronger signal than
any single finding. Fix the document; don't mute the tool.

## Promoting to enforcing

Once the backlog is burned down:

- Drop `continue-on-error` from the Redocly step to make structural validation
  block merges.
- Remove the `exit 0` guard from the Spectral step (and promote individual
  house/security/docs rules from `warn` to `error`).
- Drop `continue-on-error` from the progenitor and oasdiff steps.

## Artifact

`npm run bundle` resolves the ~30 cross-referenced spec files into a single
`build/openapi.json` (and `.yaml`). CI uploads it as the `gravitino-openapi-spec`
artifact — the clean, single-file input SDK generators and mock servers should
consume. Versioning is at `HEAD` only for now; per-release bundles can be added
later via `workflow_dispatch`/tags, and the artifact can be published to a stable
URL on the website (see the doc linked above).
