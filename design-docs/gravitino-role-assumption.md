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

# Design: Role Assumption (SET ROLE) — Narrowing Effective Permissions

| Field | Value |
| ---------- | ------------------------------------------------------------------------- |
| Status     | Draft |
| Authors    | @bharos |
| Created    | 2026-06-24 |
| Discussion | [#10894](https://github.com/apache/gravitino/discussions/10894) |
| Scope      | Native authorization path (Iceberg REST catalog + native Gravitino API) |

---

## 1. Summary

When a user holds multiple roles, Gravitino always enforces the union of all of them. A workload has no
way to run with a narrower subset of the access its identity could reach.

This document proposes **role assumption** — the analog of Snowflake's `USE ROLE` and Hive's `SET ROLE`.
A caller declares which role(s) should be active for a request, Gravitino verifies the caller actually
holds them, and authorization is evaluated against only that active set. The feature can only *reduce* a
caller's effective permissions, never expand them. If no role is declared, behavior is exactly as it is
today.

The mechanism is intentionally simple: a request header, `X-Gravitino-Active-Role`, carries the active
role(s), and the server narrows enforcement to match — across access checks, list results, and credential
vending alike. Section 3 defines the header and its values; Section 4 explains how the server enforces
it. Transport is the easy part — the common instinct is to treat this as "just a Trino change," but the
substance is server-side.

---

## 2. Motivation

### 2.1 The problem

A pipeline or AI agent that only needs 5 tables still runs with access to every table (say 105) its
identity can reach. If that workload misbehaves — a logic bug, a bad query, a leaked credential, or an
LLM agent over-reaching — the blast radius is everything the identity could touch, and out-of-scope
access **succeeds silently** instead of being denied and surfaced.

### 2.2 Why it matters now

A few things make this worth doing now. It gives workloads real runtime least-privilege: a job can drop
to exactly the access it needs without anyone having to mint a separate narrowly-scoped identity for it.
It also produces a much cleaner audit signal — with narrowing on, out-of-scope access turns into a hard
deny you can alert on, instead of a successful-but-unexpected access buried in the logs. AI agents are
the sharpest version of the same need: you want an agent to operate inside a declared, minimal scope and
to fail the moment it steps outside it. There's also a concrete migration angle — Hive's SQL Standard
Authorization supports `SET ROLE`, so teams moving HMS tables to Iceberg behind Gravitino lose that
capability today, and this restores it.

### 2.3 Goals

- Let a caller narrow the active role set for the native authorization path (Iceberg REST + native
  API).
- Guarantee narrowing is **subtractive only** — it can never grant access the caller lacks.
- Apply the narrowing consistently everywhere authorization is consulted: direct access checks **and**
  list-result filtering **and** credential vending.
- Be **fully backward compatible**: no declared role ⇒ today's union behavior, byte-for-byte.

### 2.4 Non-goals (initial)

- Dynamic in-session `SET ROLE` switching mid-connection (deferred to a later phase — see Section 11).
- Narrowing for **pushdown-authorized** catalogs (Hive/JDBC via Ranger/JDBC plugins), where
  enforcement happens in the external system — see Section 6.
- Changing how ownership is modeled (its *interaction* with narrowing is an explicit open decision —
  see Section 5).
- Write/`CREATE` semantics (which role owns newly created objects) — flagged for forward-compat only.

---

## 3. The active-role header

The interface is a single request header:

```
X-Gravitino-Active-Role: <value>
```

The caller sets it to declare which of its roles should be active for that request. The server validates
the value against the roles the caller actually holds, then evaluates authorization against only those
roles.

### 3.1 Accepted values

A value is either a role name, a comma-separated list of role names, or one of the reserved keywords
`ALL` / `NONE`:

| Value | Meaning |
|---|---|
| `<role>` (e.g. `analyst`) | Activate a single named role. |
| `<role>,<role>` (e.g. `analyst,reader`) | Activate a list of roles; effective access is the union of just these. |
| `ALL` | Activate every role the caller holds — identical to today's behavior. |
| `NONE` | Activate no roles; all role-derived access is denied. |
| *(header absent)* | Same as `ALL` — fully backward compatible. |

Here `analyst` and `reader` are example role names, not keywords; only `ALL` and `NONE` are reserved.
This mirrors the vocabulary users already know from Hive (`SET ROLE role | ALL | NONE`) and Snowflake
(`USE SECONDARY ROLES ALL | NONE`).

### 3.2 Examples

A reporting job that should only ever read through its `analyst` role:

```
GET /iceberg/v1/namespaces/sales/tables
X-Gravitino-Active-Role: analyst
```

Declaring a role the caller does not actually hold is rejected — this is what keeps the feature
subtractive:

```
X-Gravitino-Active-Role: admin     →  403 Forbidden   (caller is not a member of admin)
```

### 3.3 Semantics

- **Subtractive only.** The server validates membership first, so the header can never grant access the
  caller lacks — at most it removes roles from the evaluated set.
- **Consistent everywhere.** The narrowed set applies to every authorization decision in the request:
  direct access checks, the filtering of list results, and the privileges used for credential vending.
- **Backward compatible.** No header (or `ALL`) means today's union behavior, unchanged.

---

## 4. How the server enforces narrowing

Gravitino evaluates authorization per operation against the set of roles a user holds — their direct
roles plus those inherited from groups. Today that set is always the full union. Narrowing changes one
thing: when an active-role declaration is present, the server evaluates against only the validated
active roles instead of the union.

This fits the existing model cleanly, because authorization policies are already keyed on the role. The
server can evaluate a chosen subset of roles **without any change to the underlying Casbin model** — it
simply evaluates the active roles rather than expanding the user to all of theirs. Deny rules still win
within the narrowed scope, so an explicit deny on an active role continues to deny.

Concretely, this falls out of how the enforce call is shaped today. Authorization is checked by calling
the enforcer with the **user** as the subject; a grouping rule then expands that user to every role they
hold, and that expansion is where the union comes from. But the policies themselves are written against
*roles*, so the very same enforcer can be called with a **role** as the subject to evaluate exactly one
role's grants. Narrowing is therefore just a change of subject: evaluate each validated active role
directly and combine the results, instead of letting the user fan out to their full set. The deny
enforcer is evaluated over the same active subset, so deny-wins is preserved within the narrowed scope.
No policy, grouping, or model definition has to change.

The active-role value is read at the request boundary — the Iceberg REST entry point, or the
authentication filter for the native API — validated against the caller's roles, and carried through the
request so every check sees the same scope.

Two properties matter for correctness:

- **Applied at evaluation time, never by mutating shared state.** The authorization enforcers are shared
  process-wide across all users, so narrowing must work by choosing which roles to evaluate for this
  request — never by editing a user's role bindings, which would affect other concurrent requests.
- **Reaches every authorization decision, because they share one path.** List filtering and credential
  vending run through the same authorization check as direct access. Narrowing therefore cascades
  automatically: a narrowed caller lists only the tables its active roles allow, and is never vended a
  storage credential broader than those roles grant. Credential vending must be wired in deliberately —
  otherwise the narrowing could be bypassed through the storage token.

---

## 5. Ownership — the key open decision

There is one place where narrowing is not automatic, and it needs a decision from the community. In
Gravitino, object **ownership** grants access directly to the owning user or group, independent of
roles. Because most operations are allowed if the caller either holds a granting role *or* owns the
object, an owner stays authorized no matter which roles are active.

So narrowing roles does not, on its own, narrow the access a caller derives from ownership. This is the
one spot where Gravitino differs from Snowflake, where ownership flows through the active (primary)
role. The community needs to choose the semantics:

- **Option A — ownership always applies.** Narrowing affects role-granted privileges only; you keep
  access to objects you own. Simplest and least surprising for existing deployments, but a narrowed
  workload could still reach objects it owns outside its declared scope.
- **Option B — ownership is narrowed too.** When an active set is declared, ownership-derived access is
  honored only if it is reachable through the active roles. A stronger least-privilege guarantee, but a
  larger behavioral change.
- **Option C — make it explicit.** A reserved value in the header grammar lets the caller opt ownership
  in or out, mirroring Snowflake's primary-vs-secondary distinction.

Proposed default: ship **Option A** for v1 — it delivers the read-narrowing that motivates the feature
without surprising existing users — and design the grammar so Option C can be layered on later without a
breaking change.

---

## 6. Scope & limitations

Narrowing governs Gravitino's **native authorization path** — the Iceberg REST catalog and the native
Gravitino API, which Gravitino enforces itself. It does **not** reach catalogs that delegate
authorization to an external system: Hive, JDBC, and Hadoop SQL push enforcement down to Ranger or a
JDBC authorization plugin, which evaluate against the mapped user and groups and never see the
active-role declaration.

This proposal is scoped to the Iceberg REST path, so that boundary is consistent — but it should be
documented clearly so operators don't expect blanket coverage across every catalog type.

---

## 7. Transport — how each engine sends the active role

The mechanism is a request header. Engines differ only in whether they can forward it:

| Access path | Works today? | What's needed |
|---|---|---|
| Direct Gravitino API | ✅ Yes | Server-side change only |
| **Spark** (via Iceberg REST) | ✅ Yes | No code change — a catalog config setting |
| **Trino** (via Iceberg REST) | ❌ No | A small connector setting, *or* the token path (no Trino change) |
| Native Java/Python client | ✅ Yes* | Header passthrough; a first-class param is cleaner |

**Why Spark works but Trino doesn't:**

- Iceberg's REST catalog forwards any `header.*` catalog property as an HTTP header. So
  `spark.sql.catalog.x.header.X-Gravitino-Active-Role=<role>` just works — no client change.
- Trino's Iceberg REST connector builds a curated, strongly-typed config with no arbitrary `header.*`
  passthrough, so Trino needs a small typed setting (e.g. `iceberg.rest-catalog.active-role`) to emit
  the header.

**Trino upstream outlook:** acceptance of a *Gravitino-named* header in Trino core is uncertain, since
Trino avoids vendor-specific coupling. The durable fix is to standardize the header (or an OAuth2 scope)
in the Iceberg REST spec, after which Trino support becomes generic and vendor-neutral. Short-term, an
internal Trino patch is viable for teams that build their own Trino. The identity-level route (Section 8)
— a pre-scoped credential or an IdP-issued scoped token — likely needs no Trino change at all, because in
OAuth mode Trino already forwards the user's bearer token.

---

## 8. Security & trust boundaries

It's worth being honest about what the header does and doesn't protect against.

The header is a *cooperative* control. Because the server validates membership and only ever *removes*
roles, it can never escalate access — a client that ignores or strips the header just falls back to its
normal, already-granted access. That makes it a solid defense against *accidental* over-reach (a buggy
job, a drifting agent), turning out-of-scope access into a hard, auditable deny, and it means the header
doesn't need to be tamper-proof to be safe.

What it doesn't do is constrain a principal that refuses to cooperate. Enforcing narrowing on an
untrusted or compromised principal means binding the scope to the *identity* it's issued — e.g. a token
whose claims carry only the active role — not a header it controls. Gravitino is the resource server, not
the authorization server, so it shouldn't mint that token itself; the scoping would come from the IdP.
Not all IdPs can do this (Azure Entra, for one, emits all of a user's groups with no per-role
down-scoping), so this is a real complexity rather than a guaranteed option — noted here, to be detailed
if there's interest.

---

## 9. Prior art

For reference, how comparable systems expose the same capability:

| System | Mechanism | Ownership of created objects |
|---|---|---|
| Snowflake | Primary role + secondary roles (`USE SECONDARY ROLES ALL \| NONE`) | Tied to the primary role |
| Hive (SQL Std Auth) | `SET ROLE role \| ALL \| NONE` | Grant-based (n/a) |
| PostgreSQL | `SET ROLE` / `SET SESSION AUTHORIZATION` | New objects owned by the current role |

The relevant lesson is Snowflake's: it couples ownership to the active role. Gravitino couples ownership
to the user or group directly, which is exactly why the ownership decision in Section 5 exists.

---

## 10. Options considered

| Option | Narrows enforcement? | Enforces on uncooperative principal? | Engine support | Verdict |
|---|---|---|---|---|
| **A. Request header** (`X-Gravitino-Active-Role`) — caller declares the active role in an HTTP header | ✅ | ❌ (cooperative) | Spark ✅, API ✅, Trino needs setting | **Phase 1** — ship first |
| **B. Native API parameter** — same as A, exposed as a client call argument instead of a raw header | ✅ | ❌ | Native clients | Fold into A (nicer ergonomics) |
| **C. IdP-issued scoped token** — active set comes from access-token claims, issued already-narrowed by the authorization server | ✅ | ✅ | Engine via bearer token | Depends on IdP support (Entra can't per-role); Gravitino stays the resource server |
| **D. Dynamic SQL `SET ROLE`** — `SET ROLE` mid-session in Spark/Trino SQL, narrowing later queries | ✅ | depends | Needs per-engine session propagation | **Phase 3** — large; only if demand |
| **E. Separate scoped catalogs/identities** — pre-provision a distinct narrow identity/catalog per job (today's workaround) | ✅ | ✅ | All | Operationally heavy; doesn't scale per-job — the thing we're replacing |

---

## 11. Phased rollout

The work splits naturally into a few phases. Phase 1 is the server-side narrowing itself — validating
the declared roles, enforcing per active role, and keeping list filtering and credential vending
consistent — together with the header transport. That alone covers the native API and Spark today, and
it's backward compatible, so it lands the motivating use cases without waiting on anything else. A small
follow-up (call it Phase 1b) adds the typed Trino setting that emits the header, which brings Trino in.
Phase 2 binds the narrow scope to the caller's *identity* rather than a header — an IdP-issued scoped
token — so it holds even against an uncooperative client and reaches Trino through the existing OAuth
path. It depends on IdP capabilities (Gravitino doesn't mint the token), so it's a later, optional step.
Phase 3 — dynamic in-session
`SET ROLE` — is the largest piece, and only worth doing if there's real demand for switching roles
mid-session.

---

## 12. Decisions needed from the community

1. **Approach.** Adopt header-based, subtractive-only narrowing for the native path (Iceberg REST +
   native API), landing Spark and the native API first?
2. **Ownership semantics (Section 5) — the key decision.** When roles are narrowed, does access a
   user gets from *owning* an object still apply?
   - **A.** Ownership always applies — owners keep access regardless of active roles.
   - **B.** Ownership is narrowed too — true least-privilege, but a larger behavioral change.
   - **C.** Caller chooses, via the grammar — opt ownership in or out per request.
   Proposed default: ship A, with the grammar designed so C can be added later without a breaking change.
3. **v1 grammar.** Start with a single active role plus `ALL` / `NONE`, and add comma-separated lists
   shortly after?
4. **Standardization.** Pursue an Iceberg REST spec proposal (a standard header or OAuth2 scope) so
   Trino and other engines get vendor-neutral support, instead of relying on a Gravitino-specific
   header long-term?
5. **Enforcing on an uncooperative principal (Phase 2).** Agree the header is a cooperative control, and
   that enforcing narrowing on an untrusted principal belongs in an IdP-issued scoped token — not minted
   by Gravitino, which stays the resource server — accepting that IdP support for this varies?

**Flagged for later (not a v1 decision):** when narrowed `CREATE` is eventually supported, which role
owns newly created objects? Snowflake ties this to a primary role; noting it now only so the v1 grammar
doesn't preclude that choice.
