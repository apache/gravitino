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

# Design of Entity Secrets Management in Apache Gravitino

## 1. Background

Gravitino entities often require **connection secrets** in properties, for example:

| Category          | Where                                     | Example keys                                       |
| ----------------- | ----------------------------------------- | -------------------------------------------------- |
| JDBC              | Catalog                                   | `jdbc-password`                                    |
| Static cloud keys | Catalog / schema / fileset (esp. Fileset) | `s3-secret-access-key`, `aws-secret-access-key`, … |
| Kerberos          | Schema / fileset (Fileset catalog)        | keytab / principal related properties              |

Today these values are commonly stored as **plaintext strings** in entity properties JSON
(`catalog_meta` / `schema_meta` / `fileset_version_info`). API responses may redact them, but
persistence is not a secrets manager. That creates:

1. **Security risk** — DB / backup / support dumps may expose long-lived secrets.
2. **Governance gap** — enterprises already operate a central secret store and want Gravitino to
   **reference** it, not fork a second password silo.

Peer systems solve the same problem with an abstraction between **metadata** and **secret
material**:

- **Apache Polaris** exposes a `UserSecretsManager` SPI and persists a typed `SecretReference`
  object on the entity (not an ambiguous plaintext string).
- **Databricks** provides a **Secrets** service (scopes); Unity Catalog connections / foreign
  catalogs reference secrets via `secret(scope, key)` instead of embedding passwords as bare
  strings in connection options.

Gravitino should follow the same split: **Apache Gravitino (OSS) defines the SPI, URN model,
REST contract, and an in-memory provider**; **production backends** (HashiCorp Vault, OpenBao,
AWS Secrets Manager, GCP Secret Manager, Azure Key Vault, …) plug into this SPI and are specified
in the Enterprise backends design.

---

## 2. Goals

1. **Secrets SPI**: define a pluggable `GravitinoSecretProvider` for write /
   read / delete of secret material behind durable **references**.

2. **REST + persistence split**: HTTP **`properties`** stays **`map<string, string>`**; optional
   **`secretReferences`** (key → locator object; server **builds** the URN) and/or
   **`secretBindings`** (key → provider name for write-through) mark secrets on **create**; **alter**
   adds `@type`s **`setSecretBinding`** / **`setSecretReference`** (§5.9.4) for **catalog, schema, and
   fileset** (see §5.9). **Persistence** stays an all-string JSON map on each entity's properties
   column. Secret property values are stored as **URN strings**; reserved key
   **`gravitino.secret.keys`** (all secret keys) is comma-separated and server-managed.
   Whether to `deleteSecret` on entity drop is decided from the **URN shape** (write-through embeds
   `entityType`/`entityId`/`propertyKey` — §5.5.2 C), not a second reserved list.

3. **Backend registry**: register named secrets-provider instances in **server configuration
   files**. Catalogs reference instances by **`provider_name` in the secret URN**. Clients
   **discover** registered names via read-only `GET /api/secrets/providers` (§5.9.6) —
   registration is still conf-only (no CRUD REST).

4. **Backward compatible reads**: existing all-string entity properties continue to
   work as plaintext with no migration required.

5. **Omit secrets on GET/list and audit**: GET/list **omit** any key listed in
   persisted **`gravitino.secret.keys`** (same strip behavior as today's `PropertiesMetadata.hidden`).

6. **In-memory provider**: ship a process-local `InMemorySecretsProvider` for UT / IT / local
   quick-start (not for production).

7. **Server-side resolution only**: resolve references on the Gravitino server when loading
   catalogs / schemas / filesets or connecting; call `readSecret` **on each use**.

## 3. Non-Goals

1. **Sensitive-key allowlists as the resolution gate**: keys listed in
   **`gravitino.secret.keys`** resolve via the secrets provider (value is a URN string); other
   keys stay plaintext. REST **`secretReferences` / `secretBindings`** declare which keys are
   secrets on create.

2. **Plaintext backend credentials in configuration**: long-lived credential **values** must not
   appear in `gravitino.properties`. Operators inject them via **environment variables** (or cloud
   IAM) before startup; configuration stores only **env var names** / non-secret settings.

3. **Production secret backends in Apache Gravitino**: HashiCorp Vault, OpenBao, AWS Secrets
   Manager, GCP Secret Manager, and Azure Key Vault clients are **out of scope** for this OSS
   design. They plug into the same SPI and are specified in the Enterprise backends design.

4. **Table / column encryption KMS**: encrypting table data with cloud KMS / Vault Transit is a
   separate feature (different SPI and config namespace).

## 4. Industry Approaches (Polaris and Databricks)

This section compares **Apache Polaris** and **Databricks Secrets**.

### 4.1 Apache Polaris — typed `SecretReference`, not string sniffing

On create, inline plaintext is **write-through** via `UserSecretsManager.writeSecret`; only the
`SecretReference` object is stored. Reads call `readSecret(reference)`.

**Takeaway:** typed persistence + SPI; secret material lives in the secrets manager. URN shell follows
[RFC 8141](https://www.rfc-editor.org/rfc/rfc8141.html) `urn:<NID>:<NSS>` with NID `polaris-secret`
(`urn:polaris-secret:<type>:<type-specific-identifier>`); identifier **semantics** stay per-provider.

### 4.2 Databricks — Secrets service + references from connections

Databricks stores secret material in the **Secrets** platform service. Connections recommend
`secret('scope', 'key')` instead of password literals. Runtime resolves from the Secrets service;
displays redact as `[REDACTED]`.

**Takeaway:** password material in a secrets service; catalog/connection config holds references.

### 4.3 Cross-product summary

| Topic                                   | Apache Polaris                                                               | Databricks                                                                         |
| --------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Where secret material lives             | `UserSecretsManager` backend                                                 | Databricks Secrets service                                                         |
| What is persisted on catalog/connection | Typed `SecretReference` object                                               | `secret(scope,key)`                                                                |
| Secret binding model                    | **Fixed allowlist** (`clientSecret`, `bearerToken`, …); always write-through | **Same property** may be plaintext **or** `secret(scope,key)`                      |
| Official backend kinds                  | SPI — any impl (in-memory, Vault, …)                                         | **Databricks-backed** + **Azure Key Vault** (no first-class HashiCorp Vault scope) |

### 4.4 Why Gravitino follows Polaris (not Databricks) for the reference shape

Both peers share one idea we keep: **secret material lives outside catalog metadata**; catalogs hold
**references**, and does not expose secret material on read. **How** that idea is expressed differs
— and Gravitino’s product shape matches **Polaris** more closely than **Databricks Secrets**.

| Dimension                      | Databricks                                                    | Polaris                                          | Gravitino choice                                                                                                                                                                                           |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Where secrets are stored       | First-party **Secrets** service (scopes)                      | Pluggable **`UserSecretsManager`** (BYO / impl)  | **BYO backends** via SPI — not a new Gravitino “scopes” product                                                                                                                                            |
| How catalogs reference secrets | Platform DSL `secret(scope, key)` in SQL / connection options | Typed **`SecretReference`** object on the entity | REST: create **`secretReferences` / `secretBindings`**; alter **`setSecretBinding` / `setSecretReference`**; persistence: **URN string** + reserved **`gravitino.secret.keys`**; no SQL/`secret()` runtime |
| Secret binding model           | **Same property** may be plaintext **or** `secret(scope,key)` | **Fixed allowlist** only; always write-through   | **`secretReferences` / `secretBindings`** on create; alter via **`setSecretBinding` / `setSecretReference`**; persist listed keys in **`gravitino.secret.keys`**; omit those keys on GET                   |
| Multi-backend / multi-instance | Scoped under the Databricks Secrets service                   | SPI type + URN `type-specific-identifier`        | Named entries in server conf; URN embeds **`provider_name`** only (`className` selects implementation at factory time)                                                                                     |
| Official backend kinds         | Databricks-backed + **Azure Key Vault** only                  | SPI — implementer’s choice                       | **In-memory** in OSS; production backends via SPI (Enterprise)                                                                                                                                           |

---

## 5. Proposal

### 5.1 Value model (REST vs persistence)

| Layer                               | Shape                 | Role                                                                                                                   |
| ----------------------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **REST** `properties`               | `map<string, string>` | Unchanged from today — create HTTP values are strings                                                                  |
| **REST** `secretReferences`         | `map<string, object>` | Optional on **create** — **property key → KMS locator** (external ref; server builds URN — §5.9.2)                     |
| **REST** alter secret `@type`s      | in `updates`          | **`setSecretBinding`** / **`setSecretReference`** (§5.9.4) — same `{ "updates": [...] }` body; `setProperty` unchanged |
| **REST** `secretBindings`           | `map<string, string>` | Optional on **create** — **property key → provider name** (write-through; plaintext in `properties`)                   |
| **Persistence** entity `properties` | JSON **string map**   | `catalog_meta` / `schema_meta` / `fileset_version_info` — secret keys store URN strings; see reserved keys below       |

**Reserved persistence keys** (server-managed; clients must not set them in REST `properties`):

| Key                     | Meaning                                                                                           |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| `gravitino.secret.keys` | Comma-separated keys whose values are secret **URN strings** (external ref **and** write-through) |

| Rule    | Behavior                                                             |
| ------- | -------------------------------------------------------------------- |
| Present | At least one secret key; omit the key entirely when the set is empty |
| Example | `gravitino.secret.keys=jdbc-password,s3-secret-access-key`           |

After create/alter:

```text
gravitino.secret.keys  = all keys from secretReferences ∪ secretBindings (and prior secrets kept)
```

Drop-time KMS delete uses URN shape (§5.5.2 C) — no separate ownership list.

**Server-side resolve path** (entity load / connect — uses **`gravitino.secret.keys`**, not JSON type):

| Condition                                | Runtime behavior                                                  |
| ---------------------------------------- | ----------------------------------------------------------------- |
| Key **in** `gravitino.secret.keys`       | Value is a URN string → parse `provider_name` → `readSecret(urn)` |
| Key **not** in the list (or list absent) | Use value as plaintext; **do not** call secrets provider          |

#### 5.1.1 URN shape

```text
urn:gravitino-secret:<provider_name>:<type-specific-identifier>
```

| Part                         | Unified? | Rule                                                                                                                                                                                                   |
| ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `urn:gravitino-secret`       | Yes      | Fixed scheme + namespace                                                                                                                                                                               |
| `<provider_name>`            | Yes      | Config key `gravitino.secret.provider.<name>.*`; **authoritative** for registry lookup (backend endpoint / auth live here — **not** in the URN)                                              |
| `<type-specific-identifier>` | No       | Address of the secret **inside** the backend selected by `provider_name`. Colon-separated `[a-zA-Z0-9_-]+` segments. Layout is defined by the provider implementation (`className`). |

Secret URNs follow [RFC 8141](https://www.rfc-editor.org/rfc/rfc8141.html) `urn:<NID>:<NSS>` — a
persistent, location-independent name (not a fetch URL). Gravitino uses informal NID
`gravitino-secret`.

| RFC 8141 term                       | Role                                              | Gravitino mapping                            |
| ----------------------------------- | ------------------------------------------------- | -------------------------------------------- |
| **Scheme**                          | Always `urn`                                      | `urn`                                        |
| **NID** (Namespace Identifier)      | Which naming system; unique across all `urn:` IDs | `gravitino-secret`                           |
| **NSS** (Namespace Specific String) | Concrete ID within that namespace                 | `<provider_name>:<type-specific-identifier>` |

We use `urn:gravitino-secret:` so a secret property value is unambiguously a **Gravitino secret
handle**. Resolve by **`provider_name`** only; the SPI parses `<type-specific-identifier>`.

##### Why catalog **and** schema / fileset

Fileset catalogs may attach Kerberos / cloud credentials on **schema** and **fileset** properties
(not only on the catalog). The same secrets SPI and URN envelope therefore apply to all three
entity levels. Write-through identifiers must include an **entity type** plus a **stable entity id**
so that (1) secrets for different levels never collide, and (2) renames of metalake/catalog/schema/
fileset **names** do not invalidate stored URNs.

##### Write-through / `in-memory`: `type-specific-identifier` naming

```text
<type-specific-identifier>  ::=  <entityType>:<entityId>:<propertyKey>
```

| Segment         | Values                            | Notes                                                       |
| --------------- | --------------------------------- | ----------------------------------------------------------- |
| `<entityType>`  | `catalog`, `schema`, or `fileset` | Discriminator for property-bag owner                        |
| `<entityId>`    | Stable numeric id                 | From `SecretWriteContext`; survives rename                  |
| `<propertyKey>` | Entity property key               | e.g. `jdbc-password`, `s3-secret-access-key` (last segment) |

Examples:

```text
urn:gravitino-secret:memory:catalog:10042:jdbc-password
urn:gravitino-secret:memory:schema:20007:authentication-type
urn:gravitino-secret:memory:fileset:30019:s3-secret-access-key
```

The property key from `SecretWriteContext` is the last segment (not an opaque ordinal).
Re-writing the same property key overwrites that URN / map entry (no ordinal allocation).

Provider-specific **external** identifiers (Vault KV mount/path, AWS secret name, …) are defined
by each production backend. Apache Gravitino only requires that write-through URNs embed
`<entityType>:<entityId>:<propertyKey>` so drop can decide whether to call `deleteSecret`
(§5.5.2 C). See the Enterprise secrets-backends design for Vault / OpenBao / AWS / GCP / Azure
URN layouts and locator `attributes`.

Legacy persistence (no `secretReferences` / `secretBindings` on create):

```json
{
  "jdbc-url": "jdbc:postgresql://db.example.com:5432/inventory",
  "jdbc-user": "app_reader",
  "jdbc-password": "S3cret!Passw0rd"
}
```

Reference persistence (after external ref or write-through):

```json
{
  "jdbc-url": "jdbc:postgresql://db.example.com:5432/inventory",
  "jdbc-user": "app_reader",
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password",
  "gravitino.secret.keys": "jdbc-password"
}
```

(Write-through stores the same URN under the secret key; ownership is visible in the URN shape.)
### 5.2 Secrets-provider instance registry

Register named instances in
**server configuration** (`gravitino.conf` / `gravitino.properties` and included files), not in a
database table.

**Authentication model:** production backends may require credentials. Operators place them in
**environment variables** (or rely on cloud IAM) **before** starting Gravitino. Configuration
stores only **env var names** and non-secret settings (`className`, region, …) — never plaintext
credential values. The in-memory provider needs no credentials.

```properties
gravitino.secret.providers=memory

# In-memory (OSS default for tests / local)
gravitino.secret.provider.memory.className=org.apache.gravitino.secrets.memory.InMemorySecretsProvider
```

Additional named instances (Vault, OpenBao, AWS Secrets Manager, …) use the same
`gravitino.secret.provider.<name>.*` pattern; their `className` and settings are defined by the
Enterprise backends design.

Same shape as `gravitino.eventListener.names` + `gravitino.eventListener.{name}.class`: the
list holds **instance names**; `className` selects the implementation; remaining keys are
instance settings.

| Key pattern                                  | Meaning                                                                       |
| -------------------------------------------- | ----------------------------------------------------------------------------- |
| `gravitino.secret.providers`                 | Comma-separated **instance names** (cluster scope)                            |
| `gravitino.secret.provider.<name>.className` | Fully qualified `GravitinoSecretProvider` implementation class (**required**) |
| `gravitino.secret.provider.<name>.*`         | Implementation-specific settings (see each backend's design)                  |

**Startup sequence (v1):**

1. Operator exports or injects env vars (if the backend needs them) with non-expired credentials.
2. Operator starts Gravitino.
3. Provider factory loads each instance's **`className`**, passes the remaining
   `gravitino.secret.provider.<name>.*` keys, resolves env var **names** from the process
   environment, and constructs live `GravitinoSecretProvider` instances.

Example entry summary:

| provider_name | className (short)          | settings (excerpt)        |
| ------------- | -------------------------- | ------------------------- |
| `memory`      | `…InMemorySecretsProvider` | (none beyond `className`) |

Operators register or change these entries by **editing configuration and restarting** the Gravitino
server (v1). See §8 for the full configuration reference.

### 5.3 Architecture

```
 gravitino.conf (cluster)
   gravitino.secret.providers=memory
   gravitino.secret.provider.memory.className=…

 Catalog load / create
        │
        ▼
  read gravitino.secret.keys (comma-separated keys; absent ⇒ none)
        │
        ▼
  for each property key:
    in list  → value is URN → parse provider_name → SPI.readSecret(urn)
    not in list → plaintext as stored
        │
        ▼
  catalog_meta.properties: all-string JSON map (+ optional gravitino.secret.keys)
  GET/list: keys in gravitino.secret.keys are omitted (stripped)
```

### 5.4 OSS vs production backends

| Layer                                                | Apache Gravitino (this design)                          | Production backends (Enterprise design)               |
| ---------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------- |
| SPI + `gravitino.secret.keys` + resolve/omit-on-read | Yes                                                     | Same SPI contract                                       |
| Load providers from server conf                      | Yes                                                     | Same                                                    |
| **In-memory** secrets provider                       | **Yes** (UT/IT / local; process-local, lost on restart) | Optional for tests                                      |
| Vault / OpenBao / AWS / GCP / Azure **clients**      | No                                                      | **Yes** — Enterprise entity-secrets backends design     |

Missing / unloadable `className` ⇒ startup or resolve fails with a clear error.

### 5.5 SPI design

#### 5.5.1 Chosen shape: one SPI instance per configured provider

**Core** loads named entries from server configuration, and for each name builds **one**
`GravitinoSecretProvider` by reflecting the entry’s **`className`** and passing the remaining
instance properties. Catalog resolve does:

```text
key in gravitino.secret.keys  →  URN string value
                                    →  parse provider_name from URN
                                    →  lookup live instance by name (implementation from `className`)
                                    →  instance.readSecret(urn)
```

Illustrative Java (names TBD):

```java
/**
 * Backend client for a single configured secrets provider.
 * Not a cluster-wide facade — core routes by provider_name parsed from the URN.
 * Instance name / className are bound at factory time; each impl must implement type().
 */
public interface GravitinoSecretProvider {

  String type();

  /**
   * Write-through: store plaintext in this backend and return a durable reference URN.
   * Read-only / external-ref-only implementations throw UnsupportedOperationException.
   * Core may wrap a SPI-returned type-specific identifier into the full URN using the factory-
   * bound provider name — or the impl returns the full URN.
   */
  String writeSecret(String plaintext, SecretWriteContext context);

  /** Fetch secret material. Caller must not log or return this to HTTP GET/list. */
  String readSecret(String urn);

  /**
   * Best-effort delete. Used on entity drop (and create rollback) for
   * <b>Gravitino-managed</b> secrets only — see §5.5.2 C.
   */
  void deleteSecret(String urn);
}

/**
 * Context for write-through path generation / GC metadata (Polaris forEntity analogue).
 * External-ref path never calls writeSecret — this type is unused there.
 *
 * <p>Do <b>not</b> put renameable names (metalake / catalog / schema / fileset <b>names</b>) into
 * generated URNs. Prefer stable ids so renames do not orphan or collide paths.
 */
public final class SecretWriteContext {
  private final String entityType;  // catalog | schema | fileset (URN segment; lowercase)
  private final long entityId;      // stable id for that entity
  private final String propertyKey; // e.g. jdbc-password, s3-secret-access-key
}
```

**Mapping to entity property JSON** after write-through:

```text
SPI writeSecret → urn string
  (URN envelope provider_name from factory binding; identifier includes entityType + entityId + propertyKey)
Core persists:
  "jdbc-password": "<returned-urn>"
  "gravitino.secret.keys": "jdbc-password"
```

URN / write-through paths must use **stable entity ids** + **entity type** + **property key**, not
display names.

#### 5.5.2 Concrete examples

Create request shapes: §5.9.2 / §5.9.5. Below is the SPI / URN outcome only.

**A. External reference (no `writeSecret`)**

Ops stores the secret in an external backend. Client create uses `secretReferences` with a
provider-specific locator (`provider` + `attributes`). Core **builds** the URN and persists:

```text
jdbc-password = urn:gravitino-secret:<provider_name>:<type-specific-identifier>
gravitino.secret.keys = jdbc-password
```

Load path: key in `gravitino.secret.keys` → `readSecret(urn)` → backend read.
Locator `attributes` keys are **provider-defined**. Read-only backends still support external
refs; they reject write-through.

**B. Write-through (in-memory example)**

Client create uses `secretBindings` (`jdbc-password` → `memory`) plus plaintext in
`properties`. Core calls `writeSecret(plaintext, SecretWriteContext("catalog", entityId,
"jdbc-password"))`, persists e.g.:

```text
jdbc-password = urn:gravitino-secret:memory:catalog:10042:jdbc-password
gravitino.secret.keys = jdbc-password
```

Trailing `catalog:<id>:jdbc-password` marks write-through for drop (§5.5.2 C). Backends that are
external-ref-only: `writeSecret` throws → API rejects write-through for that provider.

**C. Drop entity (catalog / schema / fileset) — URN shape decides delete**

No new drop query param (no `purgeSecrets`). On drop of a catalog, schema, or fileset, for each
key in **`gravitino.secret.keys`**, parse the stored URN:

| URN type-specific identifier (after `provider_name`) | Behavior on drop |
| ---------------------------------------------------- | ---------------- |
| **Write-through shaped for this entity** — trailing segments are `<entityType>:<entityId>:<propertyKey>` where `entityType` ∈ {`catalog`,`schema`,`fileset`}, `entityId` equals this entity's id, and `propertyKey` equals the property key (in-memory: identifier is exactly those three; other backends may prefix path segments before the same trailing triple) | Best-effort `deleteSecret(urn)`, then drop metadata |
| **Anything else** (operator-managed external address) | Drop metadata only — **do not** delete the remote secret |

### 5.6 In-memory secrets provider (OSS default)

OSS ships a default `GravitinoSecretProvider` for **tests / local / quick-start**.

| Aspect           | Rule                                                                             |
| ---------------- | -------------------------------------------------------------------------------- |
| Config           | `className=…InMemorySecretsProvider`                                             |
| Intended use     | UT / IT / local only — **not** production                                        |
| Durability       | Process-local `ConcurrentHashMap`; **lost on restart**                           |
| Multi-node       | Each JVM has its own map — do not share write-through secrets across servers     |
| Auth / conf keys | None beyond `className` (no `uri`, `tokenEnv`, …); SPI `type()` returns `memory` |

**Config example:**

```properties
gravitino.secret.providers=memory
gravitino.secret.provider.memory.className=org.apache.gravitino.secrets.memory.InMemorySecretsProvider
```

**Storage:** map value is the secret material as a **Base64 string** (encoding only — not
cryptographic protection). Entity metadata still stores only the URN.

```text
map[URN] = Base64(plaintext)
```

| SPI method     | Behavior                                                                                                                                                                                                           |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `writeSecret`  | Base64-encode plaintext. Allocate URN `urn:gravitino-secret:<provider_name>:<entityType>:<entityId>:<propertyKey>` from `SecretWriteContext`. Store Base64 under that URN (overwrite if same key). Return the URN. |
| `readSecret`   | Look up map by URN; Base64-decode; return plaintext. Missing URN → treat as gone (impl-defined: null / error). Provider name in URN must match this instance.                                                      |
| `deleteSecret` | `map.remove(urn)` — best-effort; used by create rollback and drop for **managed** secrets (§5.5.2 C).                                                                                                              |

**Type-specific identifier:** `<entityType>:<entityId>:<propertyKey>` — see §5.1.1 (catalog / schema / fileset).

**External `secretReferences`:** may point at URNs whose `provider_name` is an `in-memory`
instance only if that process previously wrote them (or tests pre-seeded the map). Pointing an
in-memory URN at another JVM is undefined.

### 5.7 Backward compatibility

- Empty registry (no `gravitino.secret.providers`) + all-string properties ⇒ today’s behavior.
- Migrate gradually: configure providers, set `secretReferences` / `secretBindings` on create so
  selected keys store URN strings and appear in `gravitino.secret.keys`.

### 5.8 Credential refresh

Production backends may need periodic credential refresh (tokens, short-lived cloud creds). The
in-memory provider has none. Refresh policy is backend-specific and documented with each
Enterprise backend implementation.

### 5.9 Entity REST API (catalog / schema / fileset)

The same secrets rules apply to **catalog**, **schema**, and **fileset** property bags. Today each
models `properties` as `map<string, string>`. Secrets management **keeps** those string maps, adds
optional **`secretReferences`** / **`secretBindings`** on **create**, and on **alter** adds
**`setSecretBinding`** / **`setSecretReference`** `@type`s (§5.9.4); persists URN strings plus
reserved **`gravitino.secret.keys`**, and **omits** those keys on GET/list (like today’s hidden
properties).

Fileset catalogs may place Kerberos / cloud credentials on schema and fileset properties; JDBC and
other catalogs primarily use catalog properties. One REST contract covers all three so connectors
do not diverge.

#### 5.9.1 Schema change

| Layer                      | Today                         | v1                                                                                               |
| -------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------ |
| REST `properties`          | `map<string, string>`         | **Unchanged** (create)                                                                           |
| REST `secretReferences`    | —                             | **New optional** on **create** (property key → **locator object**; server builds URN)            |
| REST `secretBindings`      | —                             | **New optional** on **create** (property key → provider name)                                    |
| REST alter `updates`       | existing `@type`s             | **Add** `setSecretBinding` / `setSecretReference` (§5.9.4); `setProperty` stays plaintext string |
| REST list providers        | —                             | **New** `GET /api/secrets/providers` — safe metadata only (§5.9.6)                               |
| Persistence                | string map per entity         | Same; secret values = **URN strings**; + `gravitino.secret.keys` (drop delete via URN shape)     |
| Tables                     | —                             | `catalog_meta.properties`, `schema_meta.properties`, `fileset_version_info.properties`           |
| REST response `properties` | hidden keys stripped          | Keys in `gravitino.secret.keys` **omitted**; also omit reserved `gravitino.secret.keys` itself   |
| OpenAPI / DTOs             | per-entity create/update DTOs | Create: add `secretReferences` + `secretBindings`; alter: new update `@type`s; + list-providers  |

Persisted example (write-through owned key):

```json
{
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password",
  "gravitino.secret.keys": "jdbc-password",
}
```

External-ref example (operator-managed address — drop must **not** `deleteSecret`):

```json
{
  "jdbc-password": "urn:gravitino-secret:<provider>:<type-specific-identifier>",
  "gravitino.secret.keys": "jdbc-password"
}
```

**Affected endpoints** (entity paths unchanged; secrets behavior shared — §5.9.2–5.9.5, drop §5.5.2 C;
plus cluster list §5.9.6):

| Method   | Path                                                                           | Entity / scope |
| -------- | ------------------------------------------------------------------------------ | -------------- |
| `GET`    | `/api/secrets/providers`                                                       | Cluster        |
| `POST`   | `/metalakes/{metalake}/catalogs`                                               | Catalog        |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog        |
| `GET`    | `/metalakes/{metalake}/catalogs?details=true`                                  | Catalog        |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog        |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog        |
| `POST`   | `/metalakes/{metalake}/catalogs/{catalog}/schemas`                             | Schema         |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema         |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema         |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema         |
| `POST`   | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets`           | Fileset        |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset        |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset        |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset        |

List-with-details endpoints that return properties for schema/fileset follow the same omit-on-read
rules as catalog list.

#### 5.9.2 Create (request)

**`properties`** and **`secretBindings`** are `map<string, string>`.
**`secretReferences`** is `map<string, object>` (same for catalog, schema, and fileset create bodies).

**Locator object** (each `secretReferences` value — shared by create and by alter
`setSecretReference`):

| Field        | Required | Meaning                                                                                                      |
| ------------ | -------- | ------------------------------------------------------------------------------------------------------------ |
| `provider`   | Yes      | Registered `provider_name`                                                                                   |
| `attributes` | No       | Provider-specific locator keys (`map<string, string>`). Empty / omitted ⇒ empty map. Never a raw URN string. |

**`attributes`** are provider-specific. The REST schema stays the same for every backend; each
`GravitinoSecretProvider` documents required keys (e.g. Vault `mount`/`path`, AWS `secretId`, …).

The Gravitino **property key** (map key on create, or `property` on alter) is **not** duplicated
inside `attributes` for backends that use it as the last URN segment — the server binds that key
when building the URN.

Server builds:

```text
urn:gravitino-secret:<provider>:<type-specific-identifier>
```

| Situation                                                                        | Server behavior                                                                                                                                                                                      |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Key **not** in either secret map                                                 | Persist `properties` value as plain **string** (legacy / non-secret).                                                                                                                                |
| Key in **`secretReferences`**                                                    | Validate locator; **build URN**; persist **URN string**; key **must not** be in `properties`; **do not** `readSecret` on write; add to `gravitino.secret.keys` only.             |
| Key in **`secretBindings`**                                                      | `properties[key]` required; plaintext (not `******`) → `writeSecret` via named provider; persist **returned URN string**; add to `gravitino.secret.keys`. |
| Key in **both** `secretReferences` and `secretBindings`                          | **Reject**.                                                                                                                                                                                          |
| Key in `secretReferences` **and** also present in `properties`                   | **Reject**.                                                                                                                                                                                          |
| Key in `secretBindings`, value in `properties` is `******`                       | **Reject** (no existing value to preserve).                                                                                                                                                          |
| Key in `secretBindings` but missing from `properties`                            | **Reject**.                                                                                                                                                                                          |
| `secretBindings` / locator `provider` not in `gravitino.secret.providers`        | **Reject**.                                                                                                                                                                                          |
| Locator `attributes` value is a raw `urn:gravitino-secret:...` string            | **Reject** — use locator attributes, not a client-built URN.                                                                                                                                         |
| Client sends reserved `gravitino.secret.keys`                                    | **Reject** — server-managed only.                                                                                                                                                                    |
| Client sends a raw `urn:gravitino-secret:...` string as `secretReferences` value | **Reject** in v1 — use the locator object (server builds the URN).                                                                                                                                   |

When create succeeds with any secret keys, persist the reserved keys as in §5.1. If there are no
secret keys, **omit** `gravitino.secret.keys`.

**Write-through example** (`secretBindings` + plaintext in `properties`):

```json
{
  "name": "mysql_staging",
  "type": "relational",
  "provider": "jdbc-mysql",
  "secretBindings": {
    "jdbc-password": "memory"
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app",
    "jdbc-password": "S3cret!Passw0rd"
  }
}
```

Only `jdbc-password` is write-through; `jdbc-url` and `jdbc-user` stay plain strings.

**External reference example** (`secretReferences`; secret key **not** in `properties`):

```json
{
  "name": "mysql_prod",
  "type": "relational",
  "provider": "jdbc-mysql",
  "secretReferences": {
    "jdbc-password": {
      "provider": "asm_prod",
      "attributes": {
        "secretId": "prod/mysql/app",
        "jsonKey": "password"
      }
    }
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://db.example.com/sales",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app"
  }
}
```

Server builds a provider-specific URN. The `attributes` map is opaque to core beyond validation
by the selected provider. Concrete URN layouts live in the Enterprise backends design.

#### 5.9.3 GET and list (response)

Responses **never** include resolved secret material or URNs. Entity DTOs keep
`properties` as `Map<String, String>` (catalog / schema / fileset).

| Persisted value                                                                                  | GET / list response                                                  |
| ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| Key listed in **`gravitino.secret.keys`**                                                        | **Omit** the key (same strip as today’s `PropertiesMetadata.hidden`) |
| Key `gravitino.secret.keys`                                                                      | **Omit** from HTTP response (internal only)                          |
| Key **not** listed, legacy path (no `gravitino.secret.keys`, key is `PropertiesMetadata.hidden`) | **Omit** the key (unchanged today)                                   |
| Other non-secret string                                                                          | Return the string unchanged                                          |

**Reserved keys** live in each entity’s properties JSON (`catalog_meta` / `schema_meta` /
`fileset_version_info`). Used only for server-side resolve / omit / drop; **never**
returned on create/GET/list/alter. Legacy entities without `gravitino.secret.keys` keep
connector `PropertiesMetadata.hidden` strip behavior.

#### 5.9.4 Alter (request)

Entity alter (`PUT` catalog / schema / fileset) keeps today’s **`updates`** array. Secrets on alter
use **two new `@type`s** (request body stays `{ "updates": [ ... ] }` only; **no** create-style
sibling maps). Existing **`setProperty`** stays a **string** `value` (plaintext only).

**Common fields** (every update item has `@type` + `property`; other fields depend on `@type`):

| Field      | Required | Meaning       |
| ---------- | -------- | ------------- |
| `@type`    | Yes      | Discriminator |
| `property` | Yes      | Property key  |

| `@type`              | Fields (flat; not nested under `value`)                                                     | Behavior                                                                                                                                      |
| -------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `setProperty`        | `value` (**string** plaintext)                                                              | Today’s plaintext set. If key already in `gravitino.secret.keys`, in-place `writeSecret` via provider in the **current** URN; persist new URN |
| `setSecretBinding`   | `provider` (instance name) + `value` (plaintext string)                                     | Write-through bind/re-bind (`writeSecret`); persist returned URN; update `gravitino.secret.keys`               |
| `setSecretReference` | `provider` (instance name) + `attributes` (`map<string,string>`; same locator as §5.9.2)    | External ref; server builds URN from locator; update `gravitino.secret.keys`. Required `attributes` keys are provider-defined.         |

| Rule                                                                                     | Behavior                                                                                          |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `setSecretBinding` missing `provider` / string `value`, or `value` is `******`           | **Reject**                                                                                        |
| `setSecretReference` missing `provider`                                                  | **Reject**                                                                                        |
| `setSecretReference` `attributes` value is a raw `urn:gravitino-secret:...` string       | **Reject** — use locator attributes, not a client-built URN                                       |
| `setSecretReference` missing attributes required by the selected provider                 | **Reject**                                                                                        |
| `provider` unknown                                                                       | **Reject**                                                                                        |
| `setProperty` `value` is `******`                                                        | **Reject**                                                                                        |
| `setProperty` `value` is `urn:gravitino-secret:...`                                      | **Reject** in v1 — use `setSecretReference` or `setSecretBinding`                                 |
| `removeProperty` on a secret key                                                         | Remove from properties and `gravitino.secret.keys`; **do not** `deleteSecret` on alter (§5.5.2 C) |
| Any secret `@type` / `setProperty` on `gravitino.secret.keys`                            | **Reject** — server-managed only                                                                  |
| Other `@type`s (`rename` / `updateComment` / …)                                          | Unchanged                                                                                         |

OpenAPI: add `SetSecretBindingRequest` / `SetSecretReferenceRequest` (and schema/fileset
equivalents) to the catalog-update oneOf. Examples: TC-4–TC-5 below.

After a successful alter, refresh `gravitino.secret.keys` (omit when empty).

#### 5.9.5 API test cases (create + alter)

All examples use metalake `prod`. HTTP **200** on success; response bodies omitted (keys in `gravitino.secret.keys` omitted on GET/list — §5.9.3). Each case shows **request** + persisted **DB `properties`**.

- **TC-1–TC-3** — `POST …/catalogs` (create)
- **TC-4–TC-5** — `PUT …/catalogs/{catalog}` (alter; `setSecretReference` / `setSecretBinding` in `updates`)

Reject cases follow §5.9.2 / §5.9.4 and existing catalog semantics — no separate fixtures.

---

**TC-1 — Create: external reference via locator (200)**

Request:

```json
{
  "name": "mysql_prod",
  "type": "relational",
  "provider": "jdbc-mysql",
  "comment": "Production MySQL catalog",
  "secretReferences": {
    "jdbc-password": {
      "provider": "asm_prod",
      "attributes": {
        "secretId": "prod/mysql/app",
        "jsonKey": "password"
      }
    }
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://db.example.com:3306/sales",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app"
  }
}
```

DB `properties` (persisted):

```json
{
  "jdbc-url": "jdbc:mysql://db.example.com:3306/sales",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password",
  "gravitino.secret.keys": "jdbc-password"
}
```

---

**TC-2 — Create: write-through (200)**

Request:

```json
{
  "name": "mysql_staging",
  "type": "relational",
  "provider": "jdbc-mysql",
  "comment": "Staging MySQL catalog",
  "secretBindings": {
    "jdbc-password": "memory"
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app",
    "jdbc-password": "S3cret!Passw0rd"
  }
}
```

DB `properties` (illustrative returned URN):

```json
{
  "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password",
  "gravitino.secret.keys": "jdbc-password",
}
```

---

**TC-3 — Create: legacy plaintext, no `secretReferences` / `secretBindings` (200)**

Request:

```json
{
  "name": "mysql_legacy",
  "type": "relational",
  "provider": "jdbc-mysql",
  "comment": "Legacy plaintext catalog",
  "properties": {
    "jdbc-url": "jdbc:mysql://legacy.example.com:3306/app",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app",
    "jdbc-password": "S3cret!Passw0rd"
  }
}
```

DB `properties` (plaintext; reserved key **absent**):

```json
{
  "jdbc-url": "jdbc:mysql://legacy.example.com:3306/app",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "S3cret!Passw0rd"
}
```

---

**TC-4 — Alter: external reference via `setSecretReference` (200)**

Prior DB (no secret yet, or prior plaintext omitted here). Rebind `jdbc-password`:

```json
{
  "updates": [
    {
      "@type": "setProperty",
      "property": "jdbc-url",
      "value": "jdbc:mysql://db.example.com:3306/sales"
    },
    {
      "@type": "setSecretReference",
      "property": "jdbc-password",
      "provider": "asm_prod",
      "attributes": {
        "secretId": "prod/mysql/app",
        "jsonKey": "password"
      }
    }
  ]
}
```

DB `properties` (persisted):

```json
{
  "jdbc-url": "jdbc:mysql://db.example.com:3306/sales",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "urn:gravitino-secret:asm_prod:name:prod:mysql:app:jsonKey:password",
  "gravitino.secret.keys": "jdbc-password"
}
```

---

**TC-5 — Alter: write-through via `setSecretBinding` (200)**

Request (flat `provider` + plaintext `value`):

```json
{
  "updates": [
    {
      "@type": "setSecretBinding",
      "property": "jdbc-password",
      "provider": "memory",
      "value": "S3cret!Passw0rd"
    }
  ]
}
```

DB `properties` (illustrative write-through URN):

```json
{
  "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password",
  "gravitino.secret.keys": "jdbc-password"
}
```

#### 5.9.6 List secrets providers (request / response)

Create/alter require clients to send a **registered instance name** (`secretBindings` values,
locator / binding-object `provider`). Unknown names are **Reject**. Web UI and API clients
therefore need discovery — without hardcoding names from ops docs.

**Registration** stays file-based (§8): no REST create/update/delete of providers.
**Discovery** is a read-only cluster-scoped endpoint (not under a metalake path):

```http
GET /api/secrets/providers
```

Authenticated like other metadata APIs. Empty registry ⇒ empty `providers` array (not an error).

| Response field | Required | Meaning                                                                                  |
| -------------- | -------- | ---------------------------------------------------------------------------------------- |
| `name`         | Yes      | Instance name from `gravitino.secret.providers` (same string used in bindings / locator) |
| `type`         | Yes      | From the live SPI instance’s `type()` (e.g. `memory`) — not a conf key                   |
| `uri`          | No       | Backend address from conf when present (omit for in-memory)                              |

**Must not return:** `tokenEnv`, auth tokens, or any other secret-bearing conf.
Do **not** list secrets inside any backend.

Example response (`200`):

```json
{
  "code": 0,
  "providers": [
    { "name": "memory", "type": "memory" }
  ]
}
```

UI/clients should call this for provider pickers; create/alter still validate names against the
**live** registry (list is advisory — a provider removed between list and bind still **Reject**s).

---

## 6. Gravitino Core Changes

### 6.1 Current state

String-only `catalog_meta.properties`; no secrets-provider registry.

### 6.2 Decoding / resolve

1. Read optional `gravitino.secret.keys` (comma-separated keys). If absent ⇒ no secret keys.
2. For each property key **in** that list → treat value as URN → parse `provider_name` →
   `readSecret`.
3. For every other key → use the string value as plaintext.

---

## 7. Data Model

### 7.1 Storage overview

| Storage                       | Content                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| **Server configuration**      | Cluster-level named backends (`gravitino.secret.provider.<name>.*`)                  |
| **`catalog_meta.properties`** | All-string JSON map; secret values = URN strings                                     |
| **`gravitino.secret.keys`**   | Reserved key inside that map — comma-separated secret property keys (absent if none) |
| External secret backends      | Secret material                                                                      |

No new database table is introduced for secrets-provider registration.

### 7.2 Reserved key `gravitino.secret.keys`

| Aspect             | Rule                                                                                           |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| Location           | Inside `catalog_meta.properties` (same JSON map as other catalog properties)                   |
| Value              | Comma-separated property keys, e.g. `jdbc-password` or `jdbc-password,api-token`               |
| Absent             | Catalog has no secret properties                                                               |
| Not a secret value | Lists key **names** only; **never** returned on HTTP create/GET/list                           |
| Reserved           | Clients must not set it on create; server writes it from `secretReferences` / `secretBindings` |

### 7.3 URN

```text
urn:gravitino-secret:<provider_name>:<type-specific-identifier>
```

- **Syntax** is shared (scheme + `provider_name` + colon-separated identifier segments).
- **`<type-specific-identifier>`** is the secret's **in-backend address** for that SPI `type`.
  Write-through identifiers use `<entityType>:<entityId>:<propertyKey>` (§5.1.1). External-ref
  layouts are provider-defined (Enterprise backends design).
- On write/resolve: route by `<provider_name>` only; `className` selects the implementation at factory time.
- Renaming a provider in configuration requires operator migration of catalog URNs (or keeping the
  old name in conf).
- A property holds the URN **string** only when its key appears in `gravitino.secret.keys`.

---

## 8. Configuration

Named secrets-provider backends are **registered** in **server configuration files**, not via REST
create/update/delete or database tables. Clients may **list** registered instances via
`GET /api/secrets/providers` (§5.9.6) — that endpoint returns safe metadata only, never credentials
or endpoint URLs.

### 8.1 Provider list and per-provider keys

Aligned with `gravitino.eventListener.names` / `gravitino.eventListener.{name}.class`: list =
**instance names**; each instance has **`className`** plus settings.

| Key                                          | Description                                        |
| -------------------------------------------- | -------------------------------------------------- |
| `gravitino.secret.providers`                 | Comma-separated **instance names** (cluster scope) |
| `gravitino.secret.provider.<name>.className` | FQCN of `GravitinoSecretProvider` (**required**)   |
| `gravitino.secret.provider.<name>.*`         | Implementation-specific settings                   |

**Built-in implementation class (OSS):**

| Role                    | `className`                                                   |
| ----------------------- | ------------------------------------------------------------- |
| In-memory (tests/local) | `org.apache.gravitino.secrets.memory.InMemorySecretsProvider` |

**Example — in-memory for tests / local:**

```properties
gravitino.secret.providers=memory
gravitino.secret.provider.memory.className=org.apache.gravitino.secrets.memory.InMemorySecretsProvider
```

Production backend `className` values and settings (Vault, OpenBao, AWS, GCP, Azure) are defined in
the Enterprise entity-secrets backends design. Config change requires edit + **restart**.

---

## 9. Work Plan and Checklist

### 9.1 Work plan

| Phase | Work item                                                                                                                          |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------- |
| 0     | SPI + URN string model + reserved `gravitino.secret.keys` (drop delete via URN shape)                                              |
| 1     | In-memory `GravitinoSecretProvider` (§5.6; Base64 map value; UT/IT / local)                                                        |
| 2     | Load named providers from server configuration + env-based login creds                                                             |
| 3     | Resolve by list membership + omit-on-read GET/list (strip keys in `gravitino.secret.keys`)                                         |
| 4     | Write-through persisting URN strings + `gravitino.secret.keys`                                                                     |
| 5     | Entity REST (catalog/schema/fileset): secret maps + omit-on-read; drop URN-shape cleanup; list providers; OpenAPI / clients (§5.9) |
| 6     | Docs + unit / integration tests for SPI, in-memory provider, and REST contracts                                                    |

Production backends (Vault, OpenBao, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) and
Enterprise UI / Helm are tracked in the Enterprise backends design.

### 9.2 Checklist

| Area          | Checklist                                                                                                                                            |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Registry      | **File config** to register, **cluster scope**, no `metalake_id`, no DB table; **list** via `GET /api/secrets/providers`                             |
| Multi-backend | Multiple named conf entries; refs use `provider_name` in URN                                                                                         |
| Resolution    | Key in `gravitino.secret.keys` → URN string → `readSecret`; else plaintext                                                                           |
| URN           | `urn:gravitino-secret:<name>:<identifier>`; write-through id = `entityType:entityId:propertyKey`; route by `provider_name`                           |
| Configuration | `gravitino.secret.providers` + `provider.<name>.className` + settings; env var names; edit + restart                                                 |
| GET / list    | Keys in `gravitino.secret.keys` **omitted**; also omit `gravitino.secret.keys` itself from response                                                  |
| REST API      | Create: `secretReferences` / `secretBindings`; alter: `setSecretBinding` / `setSecretReference` (§5.9.4); list providers (§5.9.6); server builds URN |
| Drop          | No `purgeSecrets` param; `deleteSecret` only when URN is write-through-shaped for this entity (§5.5.2 C)                                             |
| Persistence   | All-string map on catalog/schema/fileset; `gravitino.secret.keys` (comma-separated; omit when empty)                                                 |
| Frontend      | Provider picker from list API; create maps; alter `setSecretBinding` / `setSecretReference`; detail/list omits secret keys                           |
| Rotation      | Backend credential refresh is backend-specific (§5.8)                                                                                                |
| Compat        | all-string properties still work with empty / unset provider list                                                                                    |
| Security      | no plaintext tokens in conf; omit secret keys on read; TLS; never persist `******` as secret material                                                |
| Scope         | This design: SPI + **in-memory provider** + conf loader + decode + REST; production backends: Enterprise design                                      |

---

## 10. References

1. [Polaris – `UserSecretsManager`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/secrets/UserSecretsManager.java)
2. [Polaris – `SecretReference`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/secrets/SecretReference.java)
3. [Databricks – Secret management](https://docs.databricks.com/aws/en/security/secrets/)
4. [Databricks – CREATE CONNECTION](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection)
5. Enterprise design: entity secrets backends (Vault, OpenBao, AWS, GCP, Azure)
