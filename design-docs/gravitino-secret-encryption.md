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

# Design of KMS-Backed Secret Encryption in Apache Gravitino

## 1. Background

The entity secrets design ([gravitino-entity-secrets.md](gravitino-entity-secrets.md)) landed the
`SecretProvider` SPI, the `urn:gravitino-secret` reference model, the REST
`secretBindings` / `secretReferences` contract, and server-side resolution. What it deliberately
did **not** ship is a production storage backend. Today's state:

1. **Only an in-memory provider exists.** `InMemorySecretsProvider` is documented as
   test-only: values are Base64-encoded (encoding, not encryption), storage is process-local and
   lost on restart, and its own Javadoc states it must not be used in production. Production
   deployments have no usable secret backend at all.

2. **Entity secrets persisted by a future DB-backed provider would still need a key.** Storing
   secrets in the relational entity store removes the durability problem, but the ciphertext is
   only as safe as the key that encrypted it. A key read from `gravitino.conf`, an environment
   variable, or a startup script sits in the same trust domain as the data it protects: whoever
   can read the config can usually read the key too.

3. **Sensitive server configuration is plaintext.** Values such as
   `gravitino.entity.store.relational.jdbcPassword` are stored in cleartext in `gravitino.conf`,
   which widens exposure through version control, shared storage, and container images.

4. **The existing `KmsClient` cannot help.** It is a read-only key-inspection interface; its
   Javadoc states it "does not perform cryptographic operations". Wrapping or unwrapping key
   material is outside its contract.

The missing piece is a **trust boundary**: a place where the encryption key lives that an attacker
who compromises the host filesystem and database dump cannot reach. External KMS services provide
exactly that, at the cost of latency and availability if used on the hot path. This design uses
each where it fits: KMS once at startup, local cryptography afterwards.

---

## 2. Goals

1. **Durable, encrypted secret backend**: a production `SecretProvider` implementation
   (provisional type `db`) that persists secrets in the relational entity store as AES-GCM
   ciphertext; no table stores plaintext secret material.

2. **KMS-protected master key**: the Gravitino master key exists in plaintext only in process
   memory while the server runs; on disk it appears only as a blob wrapped by a configured seal
   implementation.

3. **Pluggable seal SPI**: a new wrap/unwrap interface independent of `KmsClient`, with a
   `local` implementation for development and cloud KMS implementations (AWS, GCP) selected from
   server configuration.

4. **Encrypted server configuration**: sensitive server config values (starting with
   `gravitino.entity.store.relational.jdbcPassword`) may be stored as ciphertext and are decrypted
   transparently at startup.

5. **Unchanged secret consumption**: URN recognition, `readSecret` resolution, omit-on-read
   masking, and drop-time cleanup behave exactly as specified by the entity secrets design; no
   engine or connector changes.

## 3. Non-Goals

1. **Extending `KmsClient` with crypto methods**: its Javadoc documents it as a read-only
   inspection client that "does not perform cryptographic operations"; a separate seal SPI keeps
   both contracts honest.

2. **Per-secret KMS calls**: catalog connection setup is a high-frequency, latency-sensitive path,
   and a KMS outage would halt metadata operations. The KMS is contacted once per process start.

3. **Master key rotation and multi-version ciphertexts**: v1 keeps a single active master key;
   rotation requires re-encryption workflows and deserves its own follow-up design.

4. **Engine- or client-side encryption**: secrets resolve server-side and REST responses never
   include plaintext (unchanged from the entity secrets design).

5. **Plaintext KMS credentials in configuration**: seal implementations authenticate through the
   environment's standard credential chains (IAM role, workload identity, env-var names);
   `gravitino.conf` holds only non-secret settings.

## 4. Solution Investigations

### 4.1 Option A: master key stored in a local file or environment variable

The simplest protection most self-hosted software uses: generate a random master key and deliver
it to the server through a local file, an environment variable, or a system property.

- **Pros:** trivial to implement and operate; no external service dependency; startup has no
  availability coupling beyond local disk.
- **Cons:** the key and the data it protects share the same host — whoever can read the config
  file or the database dump can typically read the environment, startup scripts, and crash dumps
  as well, so the encryption adds no real trust boundary; distributing and rotating the key across
  a server fleet is manual and error-prone.
- **Decision:** rejected for production; retained as the `local` seal implementation for
  development and tests, where convenience outweighs the missing boundary.

### 4.2 Option B: every secret encrypted directly by the cloud KMS

No local master key; each `writeSecret` / `readSecret` call invokes the KMS encrypt/decrypt API.

- **Pros:** single key, single service, no key-lifecycle code in Gravitino at all.
- **Cons:** a KMS round trip on every catalog connection (latency, quotas, cost); a KMS outage
  takes down all metadata operations that touch secrets; basic operation acquires a hard cloud
  dependency.
- **Decision:** rejected for the runtime path. KMS usage is confined to a single unwrap at
  startup.

### 4.3 Option C: envelope encryption with a KMS-protected master key (chosen)

A random data-encryption key (the Gravitino master key) is wrapped by a KMS-held key-encryption
key. The wrapped blob is the only at-rest form of the master key; the plaintext key lives only in
process memory and encrypts everything else (secret table rows, config values) locally.

- **Pros:** ciphertext everywhere at rest (config file, entity store, backups); the KMS is called
  once per process start; the hot path uses local AES; the trust boundary moves off-host to
  IAM-governed KMS permissions.
- **Cons:** one more SPI to maintain, a startup dependency on KMS availability, and operator
  bootstrap tooling.
- **Decision:** chosen. This is the standard pattern used by AWS, GCP, and Vault users alike.

### 4.4 Extending `KmsClient` vs. a new seal SPI

- **What:** add `wrap` / `unwrap` to the existing `KmsClient`.
- **Pros:** one fewer interface.
- **Cons:** `KmsClient` is documented as a read-only inspection interface; repurposing it mixes
  query and crypto semantics and breaks its stated contract.
- **Decision:** rejected; a new seal SPI lives alongside `KmsClient`.

---

## 5. Proposal

### 5.1 Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│ KEK (key-encryption key)                                              │
│ Lives inside the external KMS (AWS KMS / GCP KMS / Vault transit).    │
│ Gravitino never sees its material; it can only ask the KMS to act.    │
└──────────────────────────────┬────────────────────────────────────────┘
                               │ wrap / unwrap   (once per process start)
┌──────────────────────────────▼────────────────────────────────────────┐
│ Gravitino master key (DEK)                                            │
│ At rest: wrapped blob in gravitino.conf (never plaintext on disk).    │
│ At runtime: SecretKey in process memory, zeroed on shutdown.          │
└──────────────────────────────┬────────────────────────────────────────┘
                               │ AES-GCM (local, high-frequency)
┌──────────────────────────────▼────────────────────────────────────────┐
│ Ciphertext: secret table rows in the entity store; encrypted server   │
│ config values in gravitino.conf.                                      │
└───────────────────────────────────────────────────────────────────────┘
```

### 5.2 Seal SPI (new)

A small client interface (package and final names are a review decision):

```java
/**
 * Wraps and unwraps key material using keys held by an external KMS.
 * Implementations must not log key material.
 */
public interface SealClient extends AutoCloseable {

  /** Encrypts (wraps) plaintext key material under the backing KMS key. */
  byte[] wrap(byte[] plaintext);

  /** Decrypts (unwraps) a blob previously produced by {@link #wrap(byte[])}. */
  byte[] unwrap(byte[] wrapped);
}
```

Registration follows the established `gravitino.kms.*` / `gravitino.secret.*` instance style:

```properties
gravitino.seal.providers=aws
gravitino.seal.provider.aws.className=<SealClientFactory implementation>
```

| Implementation | Backing | Authentication | Intended use |
| -------------- | ------- | -------------- | ------------ |
| `local` | symmetric key file on disk | file permissions | development and tests only |
| `aws` | AWS KMS Encrypt/Decrypt | default credential chain | production |
| `gcp` | Cloud KMS CryptoKey encrypt/decrypt | application default credentials | production |
| `vault-transit` | HashiCorp Vault transit engine | Vault token (env var) | follow-up |

### 5.3 Master key bootstrap

1. The operator generates a random 256-bit master key (tooling ships with the feature) and wraps
   it with the selected seal implementation, producing an opaque blob.
2. The blob is stored as a non-secret-looking configuration value (provisional key
   `gravitino.security.master.key.wrapped`).
3. On startup, Gravitino resolves the configured seal client, calls `unwrap`, and holds the
   result in a process-wide holder backed by a wipeable `SecretKey` / `byte[]` that is zeroed on
   shutdown. The plaintext key is never logged, persisted, or returned by any API.
4. When no wrapped master key is configured and no configuration value is encrypted, the server
   behaves exactly as today.

Startup ordering (unwrap must precede entity store initialization, because the JDBC password
itself may be encrypted):

```
SealClientRegistry → unwrap(master key) → MasterKeyHolder (memory)
    → decrypt encrypted config values → entity store connects
    → SecretProviderRegistry loads the db provider
```

### 5.4 DB-backed secret provider (type `db`)

Implements the existing `SecretProvider` SPI unchanged; registered like any other provider:

```properties
gravitino.secret.providers=db
gravitino.secret.provider.db.className=<JdbcSecretsProvider implementation>
```

Storage is a new table managed by the entity store's versioned schema (exact DDL is an
implementation detail):

| Column | Purpose |
| ------ | ------- |
| `urn` (primary key) | full URN string, identical to the value stored in entity properties |
| `ciphertext` | `iv ‖ ciphertext ‖ tag` |
| `created_at` / `updated_at` | audit |

| SPI method | Behavior |
| ---------- | -------- |
| `writeSecret` | encrypt plaintext with the in-memory master key: AES-256-GCM, fresh random IV per write, the URN as additional authenticated data (binds each ciphertext row to its identity and blocks re-binding attacks); upsert the row; return the URN |
| `readSecret` | select the ciphertext by URN, decrypt, return the plaintext |
| `deleteSecret` | delete the row by URN (best-effort, under the existing ownership rules from the entity secrets design) |

URNs keep the write-through shape already specified by the entity secrets design
(`urn:gravitino-secret:<providerName>:<entityType>:<entityId>:<propertyKey>`), so drop-time
`deleteSecret` cleanup works without new rules. Secret material is handled in wipeable
`byte[]` / `char[]` buffers, per the guidance documented on `InMemorySecretsProvider`.

### 5.5 Encrypted server configuration

Sensitive server configuration values — in v1, `gravitino.entity.store.relational.jdbcPassword` —
may be stored as ciphertext instead of plaintext. An encrypted value carries a recognizable
marker so that plaintext and ciphertext coexist in the same file; the exact marker syntax is an
implementation detail settled during development. Rules:

- A value without the ciphertext marker is used as-is (backward compatible).
- An encrypted value is decrypted once at startup with the in-memory master key; additional
  sensitive keys can be enabled incrementally.
- An encrypted value with no resolvable master key fails startup with an explicit error. There is
  no default-key fallback; development setups use the `local` seal implementation instead.
- The marker applies to server configuration only. Entity properties keep the
  `urn:gravitino-secret` recognition rule; the two namespaces do not overlap.

Note the bootstrap constraint that motivates the separate `wrapped master key` config: the entity
store's own password cannot be stored in the secret table backed by that same entity store, so
it is encrypted directly with the master key.

### 5.6 User process

Operator setup (once):

1. Generate a random master key and produce the wrapped blob with the admin tooling, using the
   same seal implementation the server will use (e.g. the AWS KMS key).
2. Put `gravitino.security.master.key.wrapped`, the `gravitino.seal.*` settings, and the
   `gravitino.secret.provider.db.*` settings into `gravitino.conf`.
3. Optionally store the entity store JDBC password in encrypted form (encrypted with the master
   key).
4. Start the server; unwrap and decryption happen automatically.

Day-to-day usage (no change from the entity secrets design):

1. A user creates a catalog with a `secretBindings` entry naming the `db` provider.
2. The server calls `writeSecret`, stores the ciphertext row, and persists the URN in the
   catalog's properties.
3. On load or connection, the server recognizes the URN, calls `readSecret`, and hands the
   plaintext only to the underlying connector.
4. GET / list responses omit URN-shaped keys exactly as before.
5. Dropping the entity or removing the property triggers `deleteSecret` under the existing URN
   ownership rules.

### 5.7 Implementation process

```
Startup (once per process)
  gravitino.conf ──► SealClientRegistry ──► SealClient (aws | gcp | local)
  wrapped master key ─────────────────────► unwrap() ──► MasterKeyHolder (memory only)
  encrypted config values ──► decrypt with master key ──► entity store connects
  EntityStore ready ──► SecretProviderRegistry loads the db provider

Runtime (per secret)
  create + secretBindings ─► writeSecret: AES-GCM(masterKey, AAD=URN) ─► secret table row
                                                   └► entity properties store the URN
  catalog load ─► URN recognized ─► readSecret ─► SELECT ─► decrypt ─► plaintext to connector
  drop / removeProperty ─► URN-shape ownership check ─► deleteSecret (existing rules)
```

### 5.8 Backward compatibility

- No wrapped master key, no encrypted configuration values, and no `db` provider configured:
  behavior is identical to today; `InMemorySecretsProvider` and plaintext entity properties keep
  working.
- Existing entities with plaintext secret properties continue to work (per the entity secrets
  design); users migrate them at their own pace via `setSecretBinding` / re-create.
- Nothing in the REST surface, the URN format, or the resolution rules changes; existing clients
  and connectors are unaffected.

---


