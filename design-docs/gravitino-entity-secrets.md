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

Gravitino should define a pluggable **secrets-provider SPI**, persist durable **URN references**
instead of plaintext for marked keys, expose a clear **REST create/alter contract**, and ship an
**in-memory provider** for tests and local use.

---

## 2. Goals

1. **Secrets provider interface**: define a pluggable `GravitinoSecretProvider` for write /
   read / delete of secret material behind durable **references**.

2. **Follow existing `KmsClient` patterns when implementing `GravitinoSecretProvider`**: only a
   minority of products can be connected the same way for both table-encryption KMS and entity
   secrets. What can be reused is the Factory / registry style and, where applicable, the same
   connection setup — not the `KmsClient` interface itself.

3. **REST + persistence split**: HTTP **`properties`** stays **`map<string, string>`**; optional
   **`secretReferences`** (key → locator object; server **builds** the URN) and/or
   **`secretBindings`** (key → `{ provider, plaintext }` for write-through) mark secrets on
   **create**; **alter** adds `@type`s **`setSecretBinding`** / **`setSecretReference`** (§5.9.4)
   for **catalog, schema, and fileset** (see §5.9). **Persistence** stays an all-string JSON map on
   each entity's properties column. Secret property values are stored as **URN strings**. A property
   is treated as a secret when its value matches the **URN recognition rule** (§5.1): starts with
   `urn:gravitino-secret` and ends with that property's key. Whether to `deleteSecret` on entity drop
   or alter `removeProperty` is decided from the **URN shape** (write-through embeds
   `entityType`/`entityId`/`propertyKey` — §5.5.2 C).

4. **Backward compatible reads**: existing all-string entity properties continue to
   work as plaintext with no migration required.

5. **Omit secrets on GET/list and audit**: GET/list **omit** any property whose value matches the
   URN recognition rule (§5.1) (same strip behavior as today's `PropertiesMetadata.hidden`).

6. **In-memory provider**: ship a process-local `InMemorySecretsProvider` for UT / IT / local
   quick-start (not for production).

7. **Server-side resolution only**: resolve references on the Gravitino server when loading
   catalogs / schemas / filesets or connecting; call `readSecret` **on each use**.

## 3. Non-Goals

1. **Fixed sensitive-key allowlists as the resolution gate**: Polaris-style fixed property-name
   allowlists are out of scope. Secrets are identified by **URN-shaped values** (§5.1), not by a
   hardcoded or reserved list of property names. REST **`secretReferences` / `secretBindings`**
   declare which keys become secrets on create.

2. **Plaintext provider credentials in configuration**: if a future provider needs credentials,
   long-lived credential **values** must not appear in `gravitino.properties`. The in-memory
   provider needs none. Configuration stores only non-secret settings (and env var **names** when
   a provider requires them).

3. **Additional provider implementations**: this design ships only **`InMemorySecretsProvider`**.
   Other backends are out of scope here.

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

| Topic                                   | Apache Polaris                                                               | Databricks                                                                  |
| --------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Where secret material lives             | `UserSecretsManager` backend                                                 | Databricks Secrets service                                                  |
| What is persisted on catalog/connection | Typed `SecretReference` object                                               | `secret(scope,key)`                                                         |
| Secret binding model                    | **Fixed allowlist** (`clientSecret`, `bearerToken`, …); always write-through | **Same property** may be plaintext **or** `secret(scope,key)`               |
| Official backend kinds                  | SPI — any implementation                                                     | **Databricks-backed** + Azure Key Vault (peer product; not Gravitino scope) |

### 4.4 Why Gravitino follows Polaris (not Databricks) for the reference shape

Both peers share one idea we keep: **secret material lives outside catalog metadata**; catalogs hold
**references**, and does not expose secret material on read. **How** that idea is expressed differs
— and Gravitino’s product shape matches **Polaris** more closely than **Databricks Secrets**.

| Dimension                      | Databricks                                                    | Polaris                                          | Gravitino choice                                                                                                                                                                                    |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Where secrets are stored       | First-party **Secrets** service (scopes)                      | Pluggable **`UserSecretsManager`** (BYO / impl)  | Pluggable SPI + in-memory provider — not a new Gravitino “scopes” product                                                                                                                           |
| How catalogs reference secrets | Platform DSL `secret(scope, key)` in SQL / connection options | Typed **`SecretReference`** object on the entity | REST: create **`secretReferences` / `secretBindings`**; alter **`setSecretBinding` / `setSecretReference`**; persistence: **URN string**; recognize secrets by URN shape; no SQL/`secret()` runtime |
| Secret binding model           | **Same property** may be plaintext **or** `secret(scope,key)` | **Fixed allowlist** only; always write-through   | **`secretReferences` / `secretBindings`** on create; alter via **`setSecretBinding` / `setSecretReference`**; omit URN-shaped keys on GET                                                           |
| Multi-backend / multi-instance | Scoped under the Databricks Secrets service                   | SPI type + URN `type-specific-identifier`        | Named entries in server conf; URN embeds **`provider_name`** only (`className` selects implementation at factory time)                                                                              |
| Official backend kinds         | Databricks-backed + Azure Key Vault (peer)                    | SPI — implementer’s choice                       | **In-memory** provider shipped; SPI remains pluggable                                                                                                                                               |

---

## 5. Proposal

### 5.1 Value model (REST vs persistence)

| Layer                               | Shape                 | Role                                                                                                                   |
| ----------------------------------- | --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **REST** `properties`               | `map<string, string>` | Unchanged from today — non-secret create HTTP values are strings                                                           |
| **REST** `secretReferences`         | `map<string, object>` | Optional on **create** — **property key → locator** (external ref; server builds URN — §5.9.2)                            |
| **REST** alter secret `@type`s      | in `updates`          | **`setSecretBinding`** / **`setSecretReference`** (§5.9.4) — same `{ "updates": [...] }` body; `setProperty` unchanged    |
| **REST** `secretBindings`           | `map<string, object>` | Optional on **create** — **property key → `{ provider, plaintext }`** (write-through; plaintext **not** in `properties`) |
| **Persistence** entity `properties` | JSON **string map**   | `catalog_meta` / `schema_meta` / `fileset_version_info` — secret keys store URN strings (§5.1 recognition rule)           |

**Secret recognition rule** (server-side; no reserved metadata key):

A property `(key, value)` is treated as a **secret property** when **both** hold:

1. `value` **starts with** `urn:gravitino-secret`
2. `value` **ends with** `key` (the property key)

Server-built URNs always place the property key as the **last segment**, so create/alter paths
satisfy this rule by construction. Plaintext values never match.

**Server-side resolve path** (entity load / connect — URN shape, not a key list):

| Condition                          | Runtime behavior                                                  |
| ---------------------------------- | ----------------------------------------------------------------- |
| Value matches the recognition rule | Value is a URN string → parse `provider_name` → `readSecret(urn)` |
| Value does **not** match           | Use value as plaintext; **do not** call secrets provider          |

Drop / `removeProperty` `deleteSecret` uses URN shape (§5.5.2 C).

#### 5.1.1 URN shape

```text
urn:gravitino-secret:<provider_name>:<type-specific-identifier>
```

| Part                         | Unified? | Rule                                                                                                                                                                                  |
| ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `urn:gravitino-secret`       | Yes      | Fixed scheme + namespace                                                                                                                                                              |
| `<provider_name>`            | Yes      | Config key `gravitino.secret.provider.<name>.*`; **authoritative** for registry lookup (provider settings live here — **not** in the URN)                                             |
| `<type-specific-identifier>` | No       | Address of the secret **inside** the provider selected by `provider_name`. Colon-separated `[a-zA-Z0-9_-]+` segments. Layout is defined by the provider implementation (`className`). |

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
For recognition (§5.1), the full URN must also **end with the property key**.

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

Write-through URNs (including in-memory) embed `<entityType>:<entityId>:<propertyKey>` so drop
can decide whether to call `deleteSecret` (§5.5.2 C). External-ref identifier layouts are defined
by each provider implementation, but the built URN **must still end with the property key** so the
recognition rule applies. This design only specifies the in-memory write-through form.

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
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password"
}
```

(Write-through stores the URN under the secret key; ownership is visible in the URN shape.)

### 5.2 Secrets-provider instance registry

Register named instances in
**server configuration** (`gravitino.conf` / `gravitino.properties` and included files), not in a
database table.

**Authentication model:** the in-memory provider needs no credentials. Configuration stores
`className` and any non-secret settings — never plaintext credential values.

```properties
gravitino.secret.providers=memory

# In-memory (default for tests / local)
gravitino.secret.provider.memory.className=org.apache.gravitino.secrets.memory.InMemorySecretsProvider
```

Same shape as `gravitino.eventListener.names` + `gravitino.eventListener.{name}.class`: the
list holds **instance names**; `className` selects the implementation; remaining keys are
instance settings.

| Key pattern                                  | Meaning                                                                       |
| -------------------------------------------- | ----------------------------------------------------------------------------- |
| `gravitino.secret.providers`                 | Comma-separated **instance names** (cluster scope)                            |
| `gravitino.secret.provider.<name>.className` | Fully qualified `GravitinoSecretProvider` implementation class (**required**) |
| `gravitino.secret.provider.<name>.*`         | Implementation-specific settings (none for in-memory beyond `className`)      |

**Startup sequence (v1):**

1. Operator starts Gravitino.
2. Provider factory loads each instance's **`className`**, passes the remaining
   `gravitino.secret.provider.<name>.*` keys, and constructs live `GravitinoSecretProvider`
   instances.

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
  for each property (key, value):
    value starts with urn:gravitino-secret
      AND value ends with key
        → parse provider_name → SPI.readSecret(urn)
    else → plaintext as stored
        │
        ▼
  catalog_meta.properties: all-string JSON map (secret values = URN strings)
  GET/list: omit keys whose values match the URN recognition rule
```

### 5.4 Scope of this design

| In scope                                       | Out of scope                                      |
| ---------------------------------------------- | ------------------------------------------------- |
| SPI + URN recognition + resolve / omit-on-read | Additional provider implementations beyond memory |
| Load providers from server conf                |                                                   |
| **In-memory** secrets provider (UT/IT / local) |                                                   |

Missing / unloadable `className` ⇒ startup or resolve fails with a clear error.

### 5.5 `GravitinoSecretProvider`

#### 5.5.1 One provider instance per configured name

Core loads each named conf entry into **one** live `GravitinoSecretProvider` (via `className`)
and passes the remaining instance properties. Catalog resolve does:

```text
value matches URN recognition rule (§5.1)
  →  parse provider_name from URN
  →  lookup live instance by name
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
   * Core may wrap a provider-returned type-specific identifier into the full URN using the
   * factory-bound provider name — or the impl returns the full URN.
   */
  String writeSecret(String plaintext, SecretWriteContext context);

  /** Fetch secret material. Caller must not log or return this to HTTP GET/list. */
  String readSecret(String urn);

  /**
   * Best-effort delete. Used on entity drop, alter removeProperty, and create
   * rollback for <b>Gravitino-managed</b> (write-through) secrets only — see §5.5.2 C.
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
writeSecret → urn string
  (URN envelope provider_name from factory binding; identifier includes entityType + entityId + propertyKey)
Core persists:
  "jdbc-password": "<returned-urn>"
```

URN / write-through paths must use **stable entity ids** + **entity type** + **property key**, not
display names.

#### 5.5.2 Concrete examples

Create request shapes: §5.9.2 / §5.9.5. Below is the provider / URN outcome only.

**A. Write-through (in-memory)**

Client create uses typed `secretBindings` (`jdbc-password` → `{ "provider": "memory",
"plaintext": "…" }`). Core calls `writeSecret(plaintext, SecretWriteContext("catalog", entityId,
"jdbc-password"))`, persists e.g.:

```text
jdbc-password = urn:gravitino-secret:memory:catalog:10042:jdbc-password
```

Trailing `catalog:<id>:jdbc-password` marks write-through for drop (§5.5.2 C). The value starts
with `urn:gravitino-secret` and ends with `jdbc-password`, so resolve / omit treat it as a secret.

**B. External reference (`secretReferences`)**

The REST contract accepts a locator (`provider` + `attributes`). Core **builds** the URN and
persists it without calling `writeSecret`. Required `attributes` keys are defined by the selected
provider. The built URN **must end with the property key**. **`InMemorySecretsProvider` rejects
external-ref binds** in this design (it is write-through only); other providers may accept them
via the same interface.

**C. Drop entity / `removeProperty` — URN shape decides delete**

No new drop query param (no `purgeSecrets`). The same ownership check applies when:

- dropping a catalog, schema, or fileset, or
- alter `removeProperty` removes a secret key

For each candidate property whose value matches the URN recognition rule (§5.1), parse the stored
URN:

| URN type-specific identifier (after `provider_name`)                                                                                                                                                                                                           | Behavior                                                                  |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **Write-through shaped for this entity** — for in-memory, the identifier is exactly `<entityType>:<entityId>:<propertyKey>` where `entityType` ∈ {`catalog`,`schema`,`fileset`}, `entityId` equals this entity's id, and `propertyKey` equals the property key | Best-effort `deleteSecret(urn)`, then drop / remove the property metadata |
| **Anything else** (including external-ref URNs)                                                                                                                                                                                                                | Drop / remove property metadata only — **do not** call `deleteSecret`     |

Rationale: write-through secrets are **Gravitino-managed**; once the property (or entity) is gone,
leaving material in the provider would orphan it. External refs are owned outside Gravitino.

### 5.6 In-memory secrets provider (OSS default)

OSS ships a default `GravitinoSecretProvider` for **tests / local / quick-start**.

| Aspect           | Rule                                                                         |
| ---------------- | ---------------------------------------------------------------------------- |
| Config           | `className=…InMemorySecretsProvider`                                         |
| Intended use     | UT / IT / local only — **not** production                                    |
| Durability       | Process-local `ConcurrentHashMap`; **lost on restart**                       |
| Multi-node       | Each JVM has its own map — do not share write-through secrets across servers |
| Auth / conf keys | None beyond `className`; SPI `type()` returns `memory`                       |

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
| `deleteSecret` | `map.remove(urn)` — best-effort; used by create rollback, entity drop, and alter `removeProperty` for **managed** (write-through) secrets (§5.5.2 C).                                                              |

**Type-specific identifier:** `<entityType>:<entityId>:<propertyKey>` — see §5.1.1 (catalog / schema / fileset).

**External `secretReferences`:** **`InMemorySecretsProvider` rejects them** (write-through only).
Use `secretBindings` / `setSecretBinding` with this provider.

### 5.7 Backward compatibility

- Empty registry (no `gravitino.secret.providers`) + all-string properties ⇒ today’s behavior.
- Migrate gradually: configure providers, set `secretReferences` / `secretBindings` on create so
  selected keys store URN strings that match the recognition rule (§5.1).

### 5.8 Credential refresh

The in-memory provider has no login credentials and needs no refresh.

### 5.9 Entity REST API (catalog / schema / fileset)

The same secrets rules apply to **catalog**, **schema**, and **fileset** property bags. Today each
models `properties` as `map<string, string>`. Secrets management **keeps** those string maps, adds
optional **`secretReferences`** / **`secretBindings`** on **create**, and on **alter** adds
**`setSecretBinding`** / **`setSecretReference`** `@type`s (§5.9.4); persists URN strings, and
**omits** keys whose values match the URN recognition rule on GET/list (like today’s hidden
properties).

Fileset catalogs may place Kerberos / cloud credentials on schema and fileset properties; JDBC and
other catalogs primarily use catalog properties. One REST contract covers all three so connectors
do not diverge.

#### 5.9.1 Schema change

| Layer                      | Today                         | v1                                                                                               |
| -------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------ |
| REST `properties`          | `map<string, string>`         | **Unchanged** (create; non-secret keys only when using secret maps)                      |
| REST `secretReferences`    | —                             | **New optional** on **create** (property key → **locator object**; server builds URN)    |
| REST `secretBindings`      | —                             | **New optional** on **create** (property key → **`{ provider, plaintext }`**)            |
| REST alter `updates`       | existing `@type`s             | **Add** `setSecretBinding` / `setSecretReference` (§5.9.4); `setProperty` stays plaintext string |
| REST list providers        | —                             | **New** `GET /configs/secrets/providers` — static config discovery (§5.9.6)                      |
| Persistence                | string map per entity         | Same; secret values = **URN strings** (recognized by URN shape; drop delete via URN shape)       |
| Tables                     | —                             | `catalog_meta.properties`, `schema_meta.properties`, `fileset_version_info.properties`           |
| REST response `properties` | hidden keys stripped          | Keys whose values match the URN recognition rule **omitted**                                     |
| OpenAPI / DTOs             | per-entity create/update DTOs | Create: add `secretReferences` + `secretBindings`; alter: new update `@type`s; + list-providers  |

Persisted example (write-through owned key):

```json
{
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password"
}
```

Write-through ownership is visible in the URN shape (`…:catalog:<id>:<propertyKey>`). Drop and
alter `removeProperty` call `deleteSecret` only for that shape (§5.5.2 C).

**Affected endpoints** (entity paths unchanged; secrets behavior shared — §5.9.2–5.9.5, drop §5.5.2 C;
plus static config list §5.9.6):

| Method   | Path                                                                           | Entity / scope                        |
| -------- | ------------------------------------------------------------------------------ | ------------------------------------- |
| `GET`    | `/configs/secrets/providers`                                                   | Cluster                               |
| `POST`   | `/metalakes/{metalake}/catalogs`                                               | Catalog                               |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog (only omit secret property)   |
| `GET`    | `/metalakes/{metalake}/catalogs?details=true`                                  | Catalog (only omit secret property)   |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog                               |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}`                                     | Catalog                               |
| `POST`   | `/metalakes/{metalake}/catalogs/{catalog}/schemas`                             | Schema                                |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema (only omit secret property)    |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema                                |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}`                    | Schema                                |
| `POST`   | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets`           | Fileset                               |
| `GET`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset (only omit secret property)   |
| `PUT`    | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset                               |
| `DELETE` | `/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets/{fileset}` | Fileset                               |

List-with-details endpoints that return properties for schema/fileset follow the same omit-on-read
rules as catalog list.

#### 5.9.2 Create (request)

**`properties`** is `map<string, string>`.
**`secretBindings`** and **`secretReferences`** are `map<string, object>` (same for catalog,
schema, and fileset create bodies).

**Binding object** (each `secretBindings` value — shared shape with alter `setSecretBinding`):

| Field       | Required | Meaning                                                    |
| ----------- | -------- | ---------------------------------------------------------- |
| `provider`  | Yes      | Registered `provider_name`                                 |
| `plaintext` | Yes      | Plaintext secret to write through (must not be `******`)   |

**Locator object** (each `secretReferences` value — shared by create and by alter
`setSecretReference`):

| Field        | Required | Meaning                                                                                                      |
| ------------ | -------- | ------------------------------------------------------------------------------------------------------------ |
| `provider`   | Yes      | Registered `provider_name`                                                                                   |
| `attributes` | Yes      | Provider-specific locator keys (`map<string, string>`); must be non-null and non-empty. Never a raw URN string. |

**`attributes`** are provider-specific. The REST schema stays the same for every provider; each
`GravitinoSecretProvider` documents required keys. **`InMemorySecretsProvider` does not use
`attributes`** (write-through only).

The Gravitino **property key** (map key on create, or `property` on alter) is bound by the server
when building write-through URNs (and as the last segment of any built URN).

Server builds:

```text
urn:gravitino-secret:<provider>:<type-specific-identifier>
```

| Situation                                                                        | Server behavior                                                                                                                                                  |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Key **not** in either secret map                                                 | Persist `properties` value as plain **string** (legacy / non-secret).                                                                                            |
| Key in **`secretReferences`**                                                    | Validate locator; **build URN** (must end with the property key); persist **URN string**; key **must not** be in `properties`; **do not** `readSecret` on write. |
| Key in **`secretBindings`**                                                      | Validate binding; `plaintext` (not `******`) → `writeSecret` via named provider; persist **returned URN string**; key **must not** be in `properties`.          |
| Key in **both** `secretReferences` and `secretBindings`                          | **Reject**.                                                                                                                                                      |
| Key in `secretReferences` **or** `secretBindings` **and** also present in `properties` | **Reject** (no overlap with `properties`).                                                                                                                 |
| `secretBindings` missing `provider` / `plaintext`, or `plaintext` is `******`    | **Reject**.                                                                                                                                                      |
| `secretBindings` / locator `provider` not in `gravitino.secret.providers`        | **Reject**.                                                                                                                                                      |
| Locator `attributes` value is a raw `urn:gravitino-secret:...` string            | **Reject** — use locator attributes, not a client-built URN.                                                                                                     |
| Client sends a raw `urn:gravitino-secret:...` string as `secretReferences` value | **Reject** in v1 — use the locator object (server builds the URN).                                                                                               |

**Write-through example** (typed `secretBindings`; plaintext **not** in `properties`):

```json
{
  "name": "mysql_staging",
  "type": "relational",
  "provider": "jdbc-mysql",
  "secretBindings": {
    "jdbc-password": {
      "provider": "memory",
      "plaintext": "S3cret!Passw0rd"
    }
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app"
  }
}
```

Only `jdbc-password` is write-through; `jdbc-url` and `jdbc-user` stay plain strings.

**External reference:** `secretReferences` / `setSecretReference` are part of the REST contract
for providers that support locators. **`InMemorySecretsProvider` rejects them**; use
`secretBindings` / `setSecretBinding` instead (examples below and in §5.9.5).

#### 5.9.3 GET and list (response)

Responses **never** include resolved secret material or URNs. Entity DTOs keep
`properties` as `Map<String, String>` (catalog / schema / fileset).

| Persisted value                                            | GET / list response                                                  |
| ---------------------------------------------------------- | -------------------------------------------------------------------- |
| Value matches the URN recognition rule (§5.1)              | **Omit** the key (same strip as today’s `PropertiesMetadata.hidden`) |
| Key is `PropertiesMetadata.hidden` (legacy plaintext path) | **Omit** the key (unchanged today)                                   |
| Other non-secret string                                    | Return the string unchanged                                          |

Legacy entities with plaintext secrets keep connector `PropertiesMetadata.hidden` strip behavior.
URN-shaped values are omitted by the recognition rule even when the key is not in a connector
hidden list.

#### 5.9.4 Alter (request)

Entity alter (`PUT` catalog / schema / fileset) keeps today’s **`updates`** array. Secrets on alter
use **two new `@type`s** (request body stays `{ "updates": [ ... ] }` only; **no** create-style
sibling maps). Existing **`setProperty`** stays a **string** `value` (plaintext only).

**Common fields** (every update item has `@type` + `property`; other fields depend on `@type`):

| Field      | Required | Meaning       |
| ---------- | -------- | ------------- |
| `@type`    | Yes      | Discriminator |
| `property` | Yes      | Property key  |

| `@type`              | Fields (flat; not nested under `value`)                                                  | Behavior                                                                                                                                                  |
| -------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `setProperty`        | `value` (**string** plaintext)                                                           | Today’s plaintext set. If the **current** value matches the URN recognition rule, in-place `writeSecret` via provider in the current URN; persist new URN |
| `setSecretBinding`   | `provider` (instance name) + `plaintext` (plaintext string)                              | Write-through bind/re-bind (`writeSecret`); persist returned URN                                                                                          |
| `setSecretReference` | `provider` (instance name) + `attributes` (`map<string,string>`; same locator as §5.9.2) | External ref; server builds URN from locator (must end with the property key). Required `attributes` keys are provider-defined.                           |

| Rule                                                                               | Behavior                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `setSecretBinding` missing `provider` / string `plaintext`, or `plaintext` is `******` | **Reject**                                                                                                                                                                                                                                         |
| `setSecretReference` missing `provider`                                            | **Reject**                                                                                                                                                                                                                                         |
| `setSecretReference` `attributes` value is a raw `urn:gravitino-secret:...` string | **Reject** — use locator attributes, not a client-built URN                                                                                                                                                                                        |
| `setSecretReference` missing attributes required by the selected provider          | **Reject**                                                                                                                                                                                                                                         |
| `provider` unknown                                                                 | **Reject**                                                                                                                                                                                                                                         |
| `setProperty` `value` is `******`                                                  | **Reject**                                                                                                                                                                                                                                         |
| `setProperty` `value` is `urn:gravitino-secret:...`                                | **Reject** in v1 — use `setSecretReference` or `setSecretBinding`                                                                                                                                                                                  |
| `removeProperty` on a secret key                                                   | Remove from properties; if the **current** value is write-through-shaped for this entity (§5.5.2 C), best-effort `deleteSecret(urn)` (same rule as entity drop). External-ref / other URN shapes: remove property only — **do not** `deleteSecret` |
| Other `@type`s (`rename` / `updateComment` / …)                                    | Unchanged                                                                                                                                                                                                                                          |

OpenAPI: add `SetSecretBindingRequest` / `SetSecretReferenceRequest` (and schema/fileset
equivalents) to the catalog-update oneOf. Example: TC-3 below.

#### 5.9.5 API test cases (create + alter)

All examples use metalake `prod`. HTTP **200** on success; response bodies omitted (URN-shaped
secret keys omitted on GET/list — §5.9.3). Each case shows **request** + persisted **DB `properties`**.

- **TC-1–TC-2** — `POST …/catalogs` (create)
- **TC-3** — `PUT …/catalogs/{catalog}` (alter; `setSecretBinding` in `updates`)

Reject cases follow §5.9.2 / §5.9.4 and existing catalog semantics — no separate fixtures.

---

**TC-1 — Create: write-through via `secretBindings` (200)**

Request:

```json
{
  "name": "mysql_staging",
  "type": "relational",
  "provider": "jdbc-mysql",
  "comment": "Staging MySQL catalog",
  "secretBindings": {
    "jdbc-password": {
      "provider": "memory",
      "plaintext": "S3cret!Passw0rd"
    }
  },
  "properties": {
    "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
    "jdbc-driver": "com.mysql.cj.jdbc.Driver",
    "jdbc-user": "app"
  }
}
```

DB `properties` (illustrative returned URN):

```json
{
  "jdbc-url": "jdbc:mysql://staging.example.com:3306/app",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password"
}
```

---

**TC-2 — Create: legacy plaintext, no `secretReferences` / `secretBindings` (200)**

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

DB `properties` (plaintext):

```json
{
  "jdbc-url": "jdbc:mysql://legacy.example.com:3306/app",
  "jdbc-driver": "com.mysql.cj.jdbc.Driver",
  "jdbc-user": "app",
  "jdbc-password": "S3cret!Passw0rd"
}
```

---

**TC-3 — Alter: write-through via `setSecretBinding` (200)**

Request (flat `provider` + `plaintext`):

```json
{
  "updates": [
    {
      "@type": "setSecretBinding",
      "property": "jdbc-password",
      "provider": "memory",
      "plaintext": "S3cret!Passw0rd"
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
  "jdbc-password": "urn:gravitino-secret:memory:catalog:10042:jdbc-password"
}
```

#### 5.9.6 List secrets providers (request / response)

Create/alter require clients to send a **registered instance name** (`secretBindings` values,
locator / binding-object `provider`). Unknown names are **Reject**. Web UI and API clients
therefore need discovery — without hardcoding names from ops docs.

**Registration** stays file-based (§8): no REST create/update/delete of providers.
**Discovery** is a read-only **static configuration** endpoint under `/configs` (same family as
`GET /configs`; not under a metalake `/api` path). Future subsystems may follow the same pattern
(e.g. `/configs/kms/providers`).

```http
GET /configs/secrets/providers
```

Same auth model as `GET /configs`. Empty registry ⇒ empty `providers` array (not an error).

**Authorization:** the response is safe provider metadata only (`name` / `type` / optional `uri`) —
no secret material — so there is **no additional privilege check** beyond that auth model.
Binding secrets still requires the usual catalog / schema / fileset create or alter privileges.

| Response field | Required | Meaning                                                                                  |
| -------------- | -------- | ---------------------------------------------------------------------------------------- |
| `name`         | Yes      | Instance name from `gravitino.secret.providers` (same string used in bindings / locator) |
| `type`         | Yes      | Provider kind from static registration (conf / loaded provider metadata), e.g. `memory`  |
| `uri`          | No       | Optional non-secret provider endpoint from conf when present (omit for in-memory)        |

**Must not return:** credentials or any secret-bearing conf.
Do **not** list secret material from the provider.

Example response (`200`):

```json
{
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

1. For each property `(key, value)`, apply the URN recognition rule (§5.1): value starts with
   `urn:gravitino-secret` **and** ends with `key`.
2. If it matches → treat value as URN → parse `provider_name` → `readSecret`.
3. Otherwise → use the string value as plaintext.

---

## 7. Data Model

### 7.1 Storage overview

| Storage                       | Content                                                             |
| ----------------------------- | ------------------------------------------------------------------- |
| **Server configuration**      | Cluster-level named backends (`gravitino.secret.provider.<name>.*`) |
| **`catalog_meta.properties`** | All-string JSON map; secret values = URN strings                    |
| In-memory provider map        | Secret material (process-local; not in the DB)                      |

No new database table is introduced for secrets-provider registration.

### 7.2 Secret recognition (URN shape)

| Aspect                  | Rule                                                                                |
| ----------------------- | ----------------------------------------------------------------------------------- |
| Gate                    | Value **starts with** `urn:gravitino-secret` **and** **ends with** the property key |
| Persist                 | Server writes URN strings under the secret property keys (create/alter)             |
| Resolve                 | Matching values → `readSecret`; others stay plaintext                               |
| GET/list                | Matching keys are **omitted**                                                       |
| Drop / `removeProperty` | Matching write-through-shaped URNs may `deleteSecret` (§5.5.2 C)                    |

### 7.3 URN

```text
urn:gravitino-secret:<provider_name>:<type-specific-identifier>
```

- **Syntax** is shared (scheme + `provider_name` + colon-separated identifier segments).
- **`<type-specific-identifier>`** is the secret's address for that SPI `type`. For in-memory
  write-through: `<entityType>:<entityId>:<propertyKey>` (§5.1.1).
- On write/resolve: route by `<provider_name>` only; `className` selects the implementation at factory time.
- Renaming a provider in configuration requires operator migration of catalog URNs (or keeping the
  old name in conf).
- A property is a secret when its value matches the recognition rule (§5.1 / §7.2).

---

## 8. Configuration

Named secrets-provider backends are **registered** in **server configuration files**, not via REST
create/update/delete or database tables. Clients may **list** registered instances via
`GET /configs/secrets/providers` (§5.9.6) — that endpoint returns safe static metadata only, never
credentials or secret-bearing conf (optional non-secret `uri` from conf when present).

### 8.1 Provider list and per-provider keys

Aligned with `gravitino.eventListener.names` / `gravitino.eventListener.{name}.class`: list =
**instance names**; each instance has **`className`** plus settings.

| Key                                          | Description                                        |
| -------------------------------------------- | -------------------------------------------------- |
| `gravitino.secret.providers`                 | Comma-separated **instance names** (cluster scope) |
| `gravitino.secret.provider.<name>.className` | FQCN of `GravitinoSecretProvider` (**required**)   |
| `gravitino.secret.provider.<name>.*`         | Implementation-specific settings                   |

**Built-in implementation class:**

| Role                    | `className`                                                   |
| ----------------------- | ------------------------------------------------------------- |
| In-memory (tests/local) | `org.apache.gravitino.secrets.memory.InMemorySecretsProvider` |

**Example — in-memory for tests / local:**

```properties
gravitino.secret.providers=memory
gravitino.secret.provider.memory.className=org.apache.gravitino.secrets.memory.InMemorySecretsProvider
```

Config change requires edit + **restart**.

---

## 9. Work Plan and Checklist

### 9.1 Work plan

| Phase | Work item                                                                                                                          |
| ----- | ---------------------------------------------------------------------------------------------------------------------------------- |
| 0     | SPI + URN string model + URN recognition rule (drop delete via URN shape)                                                          |
| 1     | In-memory `GravitinoSecretProvider` (§5.6; Base64 map value; UT/IT / local)                                                        |
| 2     | Load named providers from server configuration                                                                                     |
| 3     | Resolve by URN recognition + omit-on-read GET/list                                                                                 |
| 4     | Write-through persisting URN strings                                                                                               |
| 5     | Entity REST (catalog/schema/fileset): secret maps + omit-on-read; drop URN-shape cleanup; list providers; OpenAPI / clients (§5.9) |
| 6     | Docs + unit / integration tests for SPI, in-memory provider, and REST contracts                                                    |

### 9.2 Checklist

| Area          | Checklist                                                                                                                                            |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Registry      | **File config** to register, **cluster scope**, no `metalake_id`, no DB table; **list** via `GET /configs/secrets/providers`                         |
| Providers     | Named conf entries; refs use `provider_name` in URN; v1 ships **in-memory** only                                                                     |
| Resolution    | Value starts with `urn:gravitino-secret` and ends with property key → `readSecret`; else plaintext                                                   |
| URN           | `urn:gravitino-secret:<name>:<identifier>`; write-through id = `entityType:entityId:propertyKey`; route by `provider_name`                           |
| Configuration | `gravitino.secret.providers` + `provider.<name>.className` (+ settings); edit + restart                                                              |
| GET / list    | Keys whose values match the URN recognition rule **omitted**                                                                                         |
| REST API      | Create: `secretReferences` / `secretBindings`; alter: `setSecretBinding` / `setSecretReference` (§5.9.4); list providers (§5.9.6); server builds URN |
| Drop / alter  | No `purgeSecrets` param; `deleteSecret` on drop or `removeProperty` only when URN is write-through-shaped for this entity (§5.5.2 C)                 |
| Persistence   | All-string map on catalog/schema/fileset; secret values are URN strings                                                                              |
| Clients       | List providers API; create maps; alter `setSecretBinding` / `setSecretReference`; detail/list omits secret keys                                      |
| Rotation      | In-memory: none (§5.8)                                                                                                                               |
| Compat        | all-string properties still work with empty / unset provider list                                                                                    |
| Security      | omit secret keys on read; never persist `******` as secret material                                                                                  |
| Scope         | SPI + **in-memory provider** + conf loader + decode + REST                                                                                           |

---

## 10. References

1. [Polaris – `UserSecretsManager`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/secrets/UserSecretsManager.java)
2. [Polaris – `SecretReference`](https://github.com/apache/polaris/blob/main/polaris-core/src/main/java/org/apache/polaris/core/secrets/SecretReference.java)
3. [Databricks – Secret management](https://docs.databricks.com/aws/en/security/secrets/)
4. [Databricks – CREATE CONNECTION](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection)
