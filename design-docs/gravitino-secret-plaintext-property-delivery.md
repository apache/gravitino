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
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: Secret Plaintext Property Delivery for Connectors, Lance, and IRC

## 1. Background

Entity secrets ([design](gravitino-entity-secrets.md), epic [#12297](https://github.com/apache/gravitino/issues/12297))
persist secret material as **URN strings** in catalog / schema / fileset properties. On the
**default** HTTP load path, Gravitino **omits** those keys (same idea as
`PropertiesMetadata.hidden`), so UI and generic clients never see URNs or plaintext.

| Path | What callers get today |
| ---- | ---------------------- |
| `GET .../catalogs/{c}` (`loadCatalog` → `Catalog.properties()`) | Non-hidden, non-URN keys only |
| `GET .../credentials` (`SupportsCredentials`) | Credential-provider material only (S3/OSS/Azure/GCS/JDBC credential types, …) |
| In-process `CatalogManager.createBaseCatalog` | Already calls `SecretManager.toPlaintextProperties` for catalog conf |

**Gap:** remote / HTTP-driven runtimes cannot open a JDBC (or custom Vault) connection when the
password lives only as a secret URN:

- **Spark / Flink / Trino connectors** that load catalogs over the Java/Python client
- **Lance REST** / **Iceberg REST (IRC)** that configure backends from Gravitino HTTP metadata
- Any aux service that is **not** co-located with `CatalogManager`’s plaintext conf injection

`getCredentials` does **not** cover arbitrary secret-manager keys (e.g. custom Vault props, or
JDBC password stored via `secretBindings`). Those keys are omitted from default `loadCatalog` and
never appear on the credentials API.

---

## 2. Goals

1. **HTTP delivery of resolved secrets**: authorized callers can obtain catalog (and later schema /
   fileset) properties where **secret URNs are replaced by plaintext** via
   `SecretManager.toPlaintextProperties` / `readSecret`.
2. **Keep default `loadCatalog` safe**: unchanged omit-URN / omit-hidden behavior for UI and
   generic clients ([entity secrets §5.9.3](gravitino-entity-secrets.md)).
3. **Do not duplicate credential vending**: properties that belong to the **credentials** path
   remain omitted from this delivery API; callers continue to use `getCredentials`.
4. **Wire consumers**: document and update connector / Lance / IRC load paths to use the resolved
   property channel when they need secret-manager material.

---

## 3. Non-Goals

1. **Changing default GET/list omit semantics**: default `loadCatalog` / list responses stay
   omit-URN / omit-hidden.
2. **Returning credential-provider secrets on this API**: S3/OSS/Azure/… keys that are owned by
   credential vending stay on `getCredentials` only.
3. **Client-side URN parsing / direct provider access**: resolution stays server-side only.
4. **New secret providers**: out of scope (still epic [#12297](https://github.com/apache/gravitino/issues/12297)).

---

## 4. Solution Investigations

| Approach | Pros | Cons | Decision |
| -------- | ---- | ---- | -------- |
| **A. Change default `loadCatalog` to return URN→plaintext** | One API; connectors need no change | Breaks §5.9.3; UI / audits see passwords; major security regression | **Rejected** |
| **B. Query flag on `loadCatalog`** e.g. `?resolveSecrets=true` | Reuses DTO; small OpenAPI delta | Easy to misuse; same URL as “safe” load; authz/audit harder to reason about | Rejected as primary |
| **C. Dedicated resolved-properties API** (chosen) | Explicit privilege surface; default load stays safe; clear for connectors/Lance/IRC | Extra endpoint + client method | **Chosen** |
| **D. Only in-process resolve (no HTTP)** | Already partly done in `createBaseCatalog` | Does not help remote connectors / Lance / IRC | Rejected as sole solution |

---

## 5. Proposal

### 5.1 Response rules (resolved property map)

Start from **raw entity properties** (including hidden and URN values). Build the response map:

| Persisted `(key, value)` | Resolved API response |
| ------------------------ | --------------------- |
| Key is a **credential property** (see §5.2) | **Omit** (use `getCredentials`) |
| Value matches secret URN recognition rule | **Include** `key → readSecret(urn)` (plaintext) |
| Key is `PropertiesMetadata.hidden` and value is **not** a secret URN (legacy plaintext secret) | **Omit** (unchanged; encourage migration to `secretBindings`) |
| Other | **Include** unchanged |

Internal keys such as gravitino string-id may follow existing DTO stripping rules.

```
raw entity properties
        │
        ▼
┌───────────────────────┐
│ omit credential keys  │──── getCredentials stays the only path for those
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│ URN → toPlaintext     │──── SecretManager.readSecret / toPlaintextProperties
└───────────┬───────────┘
            ▼
┌───────────────────────┐
│ omit legacy hidden    │──── non-URN hidden values still redacted
│ plaintext secrets     │
└───────────┬───────────┘
            ▼
   resolved properties map
```

### 5.2 What counts as a “credential property”

Keys that are **owned by credential vending** / `SupportsCredentials`, not by the entity-secrets
SPI. Concretely (implementation may centralize a helper):

- Keys declared hidden **and** used only as credential-provider inputs (e.g. static
  `s3-access-key-id` / `s3-secret-access-key` when credential providers are configured), and
- Any key whose sole supported retrieval path in docs is
  [credential vending](../docs/security/credential-vending.md).

**Not** credential properties for this purpose: keys stored as secret URNs for general connection
config (e.g. `jdbc-password` via `secretBindings`, custom Vault keys). Those **must** appear as
plaintext on the resolved API.

Exact key classification should reuse existing catalog `PropertiesMetadata` + credential-provider
detection where possible, documented per catalog in follow-up PRs.

### 5.3 API changes

#### Chosen: dedicated endpoint (catalog first; schema/fileset follow)

```http
GET /api/metalakes/{metalake}/catalogs/{catalog}/properties?view=resolved
```

**Request:** path metalake + catalog; query `view=resolved` (required for v1; avoids an empty
“raw dump” of URNs).

**Response:** `200 OK`

```json
{
  "code": 0,
  "properties": {
    "jdbc-url": "jdbc:mysql://…",
    "jdbc-user": "app",
    "jdbc-password": "S3cret!Passw0rd"
  }
}
```

**Behavior:**

- Authz: same as `loadCatalog` at minimum (`USE_CATALOG`); implementation may tighten (e.g. require
  metalake admin) — call out in the implementing PR.
- Never returns credential-property keys (§5.2).
- Never returns raw `urn:gravitino-secret:…` strings for resolved keys.
- Missing catalog → same errors as `loadCatalog`.

**Old `GET .../catalogs/{catalog}`:** unchanged (omit URN / omit hidden).

**Java / Python clients:** add e.g. `loadCatalogResolvedProperties(catalog)` (name TBD) that calls
the new endpoint. Do **not** change default `loadCatalog().properties()` semantics.

Optional later: schema / fileset siblings under the same pattern.

### 5.4 User process

1. Admin creates a catalog with `secretBindings` / `secretReferences` (URN persisted).
2. UI / normal clients call `loadCatalog` → no password / no URN in `properties`.
3. Spark connector / Lance / IRC (or job runtime) calls **resolved properties** API (or client
   helper) → receives plaintext for secret-manager keys.
4. Same runtime calls **`getCredentials`** when it needs credential-vended cloud tokens — not this
   API.

### 5.5 Implementation process

```
Connector / Lance / IRC
        │  GET .../catalogs/{c}/properties?view=resolved
        ▼
CatalogOperations (REST)
        │  authz + load entity
        ▼
SecretPropertyDelivery (core helper)
        │  filter credential keys
        │  SecretManager.toPlaintextProperties(raw)
        │  omit legacy hidden non-URN
        ▼
JSON properties map
```

In-process path remains: `createBaseCatalog` already injects plaintext conf; align filtering with
the same helper so HTTP and in-process rules do not drift.

### 5.6 Consumer wiring (follow-up tasks)

| Consumer | Change |
| -------- | ------ |
| Spark / Flink / Trino connectors | When initializing catalog options from Gravitino, use resolved properties (or in-process conf if embedded) for secret-manager keys; keep `getCredentials` for vending |
| Lance REST | Namespace / catalog bootstrap: fetch resolved properties instead of default `loadCatalog` props for connection secrets |
| Iceberg REST (IRC) | Catalog config build: merge resolved Gravitino properties where URN-backed secrets are required |

---

## 6. Task Breakdown

### Phase 1: Server + clients

- [x] Core helper: build resolved property map (credential omit + URN→plaintext + legacy hidden omit)
- [x] REST `GET .../catalogs/{catalog}/properties?view=resolved` + OpenAPI
- [x] Java / Python client methods
- [x] Unit + REST tests (URN resolves; credential keys absent; default loadCatalog unchanged)

### Phase 2: Consumers `(parallel after Phase 1)`

- [x] Spark connector: use resolved properties for secret-manager keys
- [x] Flink / Trino connectors: same pattern
- [x] Lance REST: catalog/schema bootstrap uses resolved properties
- [x] Iceberg REST (IRC): catalog conf uses resolved properties where needed
- [ ] Docs: credential vending vs secret plaintext delivery

### Phase 3 (optional)

- [x] Schema / fileset resolved-properties endpoints
- [ ] Authz hardening / audit events for resolved reads

---

## 7. Relation to existing design

This document **extends** [gravitino-entity-secrets.md](gravitino-entity-secrets.md):

- §5.9.3 default GET/list omit rules stay in force.
- Goal “server-side resolution on use” is realized for **HTTP consumers** via an **explicit**
  resolved-properties API, not by weakening default `loadCatalog`.
