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

# Business Glossary: OSS and Enterprise Responsibilities

| Updated | Status |
| --- | --- |
| 2026-11-13 | Draft; explicit term-domain membership selected |

OSS 2.0 owns the term model, live storage, metalake-scoped authorization, relations, bindings,
public APIs and basic agent lookup. Enterprise owns domain-scoped authorization, always-on review,
certification, history and the product UI. Enterprise applies approved changes through the shared
core rather than maintaining a second live term model.

The [OSS design](glossary-design.md) gives the core implementation proposal. The allocation below
combines the existing feature allocation with the product clarifications. **Term-domain
membership uses explicit `TermDomain` relations (Option A)**, selected in the
[November 9 review](https://github.com/apache/gravitino/pull/14239#discussion_r4512415347).
The earlier backing-tag proposal is retained as an alternative, not a second membership store
or an implementation prerequisite.

## Scope decisions

| Area | OSS 2.0 | Enterprise 2.0 |
| --- | --- | --- |
| Initial status | `APPROVED` | `DRAFT`, unless an audited metalake setting selects `APPROVED` |
| Content and status changes | Apply immediately with the ordinary edit privilege; all three status values are supported | Review required; a direct status write cannot bypass workflow |
| Definition privileges | `VIEW_DEFINITION`, `CREATE_DEFINITION`, `MANAGE_DEFINITIONS` on the metalake | Same capabilities, with domain-scoped grants and inheritance |
| Ownership | CREATE permits authors to maintain their own terms, including status; MANAGE reads, edits and deletes any term in any status, with no approval role | Contributors propose changes; authorized reviewers decide them, with no self-approval |
| Draft visibility | Only the current owner with CREATE, or MANAGE; CREATE cannot read other owners' drafts | Step 2 defines visibility with domain-scoped review |
| Domain cardinality | Zero or more, including multiple domains; optional at creation | Same core membership, plus governed scope and review |
| Revision and history | Technical concurrency revision and audit; no snapshots/history API | Proposed content, snapshots for approved changes, certified versions and readable history |
| Bindings | All eight asset types plus semantic-model metrics; COLUMN means table columns only, while views have their own bindings | UI and review for asset and metric binding changes |

`APPROVED` in OSS makes a visible term eligible for agent lookup; it does not assert human
certification. New terms default to `APPROVED`, and content and status edits take effect without
review. CREATE never permits deletion, even for an owned draft; MANAGE can delete an approved
term without deprecating it first. Enterprise's creation setting is not a workflow-off switch:
subsequent edits, bindings and domain changes always go through review. Term status and proposal
status are distinct.

## Feature allocation

Feature IDs preserve the existing allocation for discussion and implementation tracking.

| ID | Capability | OSS 2.0 deliverable | Enterprise 2.0 deliverable | Enterprise 2.1 deliverable |
| --- | --- | --- | --- | --- |
| G1 | Term object and storage | Term CRUD, name, definition, synonyms, owner, status, properties, documentation links, source reference and descriptive tag associations | Reuse core identity and live model | Reuse core |
| G2 | Term relations | Multi-parent broader/narrower and related links; cycle prevention, concurrency protection and cleanup | Hierarchy/relationship UI; edits use review | — |
| G3 | Asset bindings | Catalog, schema, table, view, table-column, fileset, topic and model bindings; authorization, atomic writes, bidirectional reads and cleanup | Binding UI and approval of changes through every write path | Connector synchronization through G18 |
| G4 | Metric bindings | Semantic-model identity plus metric name; reverse lookup and removal/rename cleanup | Metric-binding UI and review | — |
| G5 | Domain membership | Zero-or-more domain membership, domain filters and cleanup through selected explicit relations (Option A) | Domain-scoped governance and approval of membership changes | — |
| G6 | Status | Default APPROVED; ordinary edits can set DRAFT, APPROVED or DEPRECATED; no transition endpoint or state machine | Always-on lifecycle workflow; audited UI/REST setting for initial status | — |
| G7 | Authorization | Three definition privileges at metalake scope, owner checks, deny semantics and filtered results | Domain grants/inheritance, governed writes, reviewer authorization and no self-approval | Reuse authorization |
| G8 | Audit and events | Core create/edit/status/relation/membership/binding/delete events and audit; reuse owner/tag audit | Proposal, approval, rejection, certification and initial-status-setting audit | Workflow notifications through G13a |
| G9 | API and clients | REST, Java/Python clients and OpenAPI for core operations; existing tag APIs extended to terms | APIs for proposals, decisions, certification/history, KB and CSV | APIs for resolution, SKOS and connectors |
| G10 | Basic MCP | Read-only exact lookup, accessible bindings, match provenance, ambiguity and unmatched inputs; approved roots and nested terms authorized for each tool call's effective principal | Reuse basic lookup against current effective term state | Keep basic lookup alongside ranked resolution |
| G11 | KB articles | Term definition and documentation links; no separate KB object | Separate rich-text KB article model, storage, API and UI, linked to terms/assets | — |
| G12 | Certification and versions | Technical revision only; no content snapshots, pending content or readable history | Proposed content, certified versions, snapshots on approved changes, history and differences | Certified-version/steward citations through G14 |
| G13 | Approval workflow | Lightweight core write extension with immediate default behavior; no workflow storage, queue or approval rules | Always-on submission/review/decision flow, pending-content isolation and no self-approval | — |
| G13a | Approval notifications | Core events only | Workflow audit remains part of G8 | Submission and decision notifications using the general notification mechanism |
| G14 | Ranked MCP and citations | No ranked/fuzzy/semantic resolver implementation | — | Rank authorized candidates; return certified version and steward for citations |
| G15 | Glossary UI | Programmatic access through API, SDK and MCP; no Glossary UI | Term/KB management, hierarchy, asset/metric bindings and approval queue | UI for corresponding 2.1 features |
| G16 | CSV import/export | Source reference and public APIs usable by importers; no CSV delivery | CSV import/export and repeat-import updates, subject to authorization and review; tracked as OSS-design task S2.6 until a GitHub issue is filed | — |
| G17 | SKOS export | Core relation semantics only | — | Permission-filtered Turtle or JSON-LD export |
| G18 | External connectors | Stable core identities and APIs; no dedicated connectors | CSV onboarding via G16 | DataHub, Collibra and Atlan connectors, including source mapping and supported synchronization |

The UI, KB, CSV and Enterprise 2.1 entries retain the existing allocation. Mark's individual
comments directly clarify tags/membership, status, privilege scope, versioning, full asset coverage
and always-on workflow; they should not be read as new implementation specifications for every
other feature in the table.

## Membership, tags and bindings are different relationships

| Relationship | Purpose | Current technical proposal |
| --- | --- | --- |
| Term to Domain | Organize definitions; establish Enterprise governance scope | `TermDomain` relation, changed through the term API |
| Term to term | Express broader/narrower and related meanings | Typed term relations |
| Term to asset/metric | Locate data or computations relevant to a definition | `TermBinding`, with separate authorization on the target |
| Tag on a term | Classify the term using existing tag facilities | Existing tag association APIs and authorization |

### TermDomain versus backing tags

The explicit relation preserves a single term-controlled membership write path. It is easier to
validate, audit and submit for Enterprise review without making ordinary tag operations change
governance. Tag deletion or backing-tag replacement cannot remove a term from its domains.

The trade-off is a dedicated membership table and cleanup, and different member sources for
terms and assets. `GET /terms?domain=` reads term relations; a unified Domain object list would
need explicit integration. Under this proposal, tagging a term with a Domain's backing tag does
not also assign the term to that Domain.

The earlier backing-tag alternative would reuse storage and listing, but would require review
of all paths that can change term membership, including shared tag and backing-tag operations.
It was not selected. G5 implements explicit relations; do not implement automatic two-way
synchronization or make tags and relations independently authoritative.

### Why the asset list includes eight types

The original design emphasized SQL lookup and limited asset bindings to tables, views and columns.
Mark's response to G3 was: “Ship the full object list in OSS 2.0, not phased.” This sets release
scope; it is not a claim that the storage model cannot support a smaller initial subset.

| Target | Example |
| --- | --- |
| Catalog / schema | A business collection or subject area associated with a term |
| Table / view / table column | Customer records, a reporting view itself, or the customer-ID field of a table |
| Fileset | Files representing a training dataset or document collection |
| Topic | A customer-change or order-event stream |
| Model | An ML model associated with churn prediction |
| Semantic-model metric | A net-revenue metric implementing its business definition |

Bindings remain explicit and direct. A catalog/schema binding neither propagates to descendants
nor grants access. `MODEL` means an ML model; a semantic-model metric is the separate G4 capability.
The existing COLUMN identity, authorization and endpoint fence assume a table parent. OSS 2.0
therefore supports table columns and whole views, but no view-column bindings or
`children=true` on views. View-column support needs a separate identity, authorization, locking
and cleanup contract before it can be added.
Supporting all types requires resolver/authorization coverage, reverse queries, clients, tests
and cleanup for each type. Keeping only table/view/column is possible, but requires an explicit
scope agreement rather than treating the other five types as Enterprise features.

## Delivery responsibilities

### OSS 2.0

1. Deliver the live term model and database migrations for H2, MySQL and PostgreSQL, including
   normalized identity/synonyms, relations, membership, bindings and a technical revision.
2. Implement metalake definition privileges and owner behavior consistently across REST, clients,
   nested results and MCP. Owners can edit their terms in any status. Hide unauthorized domains
   in term responses, authorize targets before reverse binding reads, and apply binding limits
   after target authorization. Basic MCP filters roots and nested terms to approved in the same
   server read, including hierarchy traversal and term-binding requests.
3. Extend existing tag facilities for terms. Keep tag classification and domain membership
   consistent with the membership decision, without two independent membership stores.
4. Deliver all asset and metric bindings, atomic updates, both authorization paths, direct/reverse
   lookups, and cleanup on deletion or loss of a referenced column/metric.
5. Deliver APIs, SDKs, OpenAPI, core operation events/audit and deterministic read-only MCP lookup.
   HTTP tools forward supplied user credentials; stdio and allowed no-header HTTP calls use
   the configured service identity, or Gravitino's unauthenticated behavior when none is set.
   Document rejection when service fallback is disabled, and apply authorization to the effective
   principal rather than promising end-user identity in every mode.
6. Provide the write extension needed by Enterprise while keeping the OSS default immediate and
   free of proposal, certification or snapshot storage.

Completion includes targeted authorization, concurrency, rollback and lifecycle tests on the
relevant database backends, plus coverage of every supported binding type and agent visibility.

### Enterprise 2.0

1. Add domain-scoped definition grants, inherited scope resolution and deny handling. Membership
   identifies applicable scopes but does not itself confer permission to terms or bound data.
2. Implement the always-on workflow, reviewer assignment, no self-approval, pending-content
   isolation and explicit proposal responses. Review edits, bindings and domain changes even
   when creation is configured to start approved.
3. Own the proposal store, approved-change snapshots, certified-version counter and readable
   history/diff APIs. Keep these separate from the OSS technical revision.
4. Revalidate authorization, base revision, domains, relations and binding targets when approving;
   apply through the shared core transaction rather than modifying core tables out of band.
5. Deliver the audited UI/REST creation-status setting, Glossary UI, KB article object/UI and CSV
   import/export. CSV and object-side binding APIs must not become alternate approval bypasses.
6. Audit workflow and certification actions. Integrate any shared operations that change governed
   term state; the membership implementation determines which Domain/tag paths require this.

### Enterprise 2.1

1. Add ranked term resolution and certified-version/steward citations while preserving exact
   basic lookup and authorization filtering.
2. Add permission-filtered SKOS export and the planned external catalog/glossary connectors.
3. Add approval notifications through the general notification mechanism and UI/API support for
   these capabilities. Connector writes remain subject to the same governed write contract.

## Shared core and Enterprise boundary

| Concern | Core responsibility | Enterprise responsibility |
| --- | --- | --- |
| Authorization | Metalake checks, owner semantics, target access checks and reusable authorization integration | Domain scope resolution, review privileges and no-self-approval enforcement |
| Write handling | Validate an operation; apply immediately by default; expose the extension boundary | Retain a proposal and return its pending state; decide when approved application is permitted |
| Application | Locks, constraints, transaction, revision update, cleanup and audit | Revalidate the proposal and invoke the same core application path; block public bypasses |
| Storage | Live rows and technical concurrency metadata | Pending content, approved snapshots, certified versions and history |
| Reads | Current authorized live state and deterministic lookup | Authorized proposal/history views, UI and richer agent features |

A pending proposal must not appear as a successful live mutation. Approval must not silently
replace a newer term revision. The extension contract must cover term-side and object-side writes;
its precise interface and error/response mapping remain implementation-design work.

## Decisions and remaining alignment

- **Domain membership (decided):** explicit relations (Option A), as recorded in the November 9
  review. Backing tags remain an alternative and do not block G5.
- **Default public read access:** define the shared PUBLIC-role behavior or require administrator
  grants; no implicit authorization bypass.
- **Deletion (decided):** MANAGE can delete any term in any status, without first deprecating it.
  CREATE never permits deletion; changing status cannot bypass the deletion privilege.
- **Client concurrency contract:** technical store revision is required; public conditional writes
  are a separate API decision. No content-history API is implied.
- **Enterprise creation setting:** specify its configuration name, permission and exact treatment
  of newly auto-approved terms in certification history without weakening later review.
- **Unmatched telemetry and Git sources:** persistent lookup tracking, source-controlled edit
  rules and accepting a merged PR as approval need separate decisions.

## Issue scope and CSV tracking

The design acceptance criteria of [#14238](https://github.com/apache/gravitino/issues/14238)
span both delivery steps. The OSS design's Delivery Scope and Tracking table maps domain grants
to S2.3, snapshots to S2.2, review/no-self-approval to S2.1, and CSV to S2.6 / Enterprise G16.
Those local task IDs have no GitHub issue numbers yet. Keep #14238 open, with the design PR
referencing it as related work, until the remaining designs are accepted or split into linked
follow-up issues. CSV is planned Enterprise work; source references and public APIs do not count
as CSV import/export delivery.

## References

- [Glossary questions and Mark's replies](https://docs.google.com/document/d/1BTgnEPinsMQWMWV5bRYkqzcjLtTHcm9QdYfjvhCI3Vk/edit), including comments dated November 2, 2026.
- [Glossary design PR #14239](https://github.com/apache/gravitino/pull/14239).
- [OSS technical design](glossary-design.md).
