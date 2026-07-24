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

# Design: Idempotency-Key Support for the Gravitino Iceberg REST Server

| Field   | Value                                                   |
| ------- | ------------------------------------------------------- |
| Status  | Draft                                                   |
| Authors | @laserninja                                             |
| Created | 2026-07-16                                              |
| Module  | `iceberg/iceberg-rest-server`, `iceberg/iceberg-common` |

---

## 1. Background

The Iceberg REST spec defines an optional `Idempotency-Key` header (UUIDv7) on
all mutation endpoints (create/update/delete for tables, namespaces, and views).
When present, the server must:

1. Execute the mutation on first receipt and store the response.
2. Return the stored response on subsequent retries with the same key.
3. Advertise the key reuse window via `idempotency-key-lifetime` in
   `GET /v1/config`.

Without this, network failures and client retries can cause duplicate
table/namespace/view creation or deletion.

### 1.1 Multi-Node Problem

Gravitino's Iceberg REST server can run as multiple replicas behind a load
balancer. A purely in-memory cache (e.g., Caffeine) is node-local:

- **Retry hits different node**: The second node has no record of the first
  execution and re-runs the mutation (duplicate side-effect).
- **Node crash**: All cached responses are lost; retries execute again.

A production-grade solution requires shared, durable storage for idempotency
records.

### 1.2 Prior Art

Apache Polaris introduced database-backed idempotency support in
[apache/polaris#3205](https://github.com/apache/polaris/pull/3205) with a
Postgres-backed `IdempotencyStore` SPI. Their design includes heartbeat-based
liveness, executor ownership, crash reconciliation, and minimal response
reconstruction. Gravitino can adopt a simplified version of this pattern that
fits its existing relational backend and shorter mutation lifecycle.

Note that in Polaris idempotency is **disabled by default** and must be
explicitly enabled. This design follows the same convention (see
[Section 6](#6-configuration)).

### 1.3 Idempotency-Key Format: UUIDv7 (RFC 9562)

The Iceberg REST spec requires the `Idempotency-Key` header to be a **UUIDv7 in
string form**, as defined by [RFC 9562](https://www.rfc-editor.org/rfc/rfc9562).
RFC 9562 obsoletes RFC 4122 and standardizes the newer time-ordered UUID
versions (6, 7, and 8). The requirement is stated directly in the spec's
`Idempotency-Key` parameter definition: *"Key format: UUIDv7 in string form
(RFC 9562)"* (see
[`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)).

UUIDv7 encodes a 48-bit Unix timestamp (milliseconds) in its most-significant
bits, followed by version/variant bits and random data. This gives it two
properties this design benefits from:

- **Time-ordered / monotonic**: Keys generated later sort after keys generated
  earlier. Used as a primary key, inserts are append-mostly, which reduces
  B-tree page splits and index fragmentation on MySQL/PostgreSQL compared to
  fully-random UUIDv4.
- **Embedded creation time**: The timestamp prefix lets the server derive an
  approximate creation instant from the key itself, useful for diagnostics and
  as a sanity check, and aligns naturally with TTL-based expiry.

**Why this design follows RFC 9562:**

1. **Spec compliance**: The Iceberg REST spec mandates `UUIDv7 in string form
   (RFC 9562)`. Validating against RFC 9562 (version nibble `7`, correct
   variant bits, 36-char canonical form) lets the server reject malformed keys
   with `400 Bad Request` before touching storage, and guarantees
   interoperability with any spec-compliant client.
2. **Storage efficiency**: The time-ordered property keeps the
   `idempotency_key` primary-key index compact and insert-friendly, which
   matters most for the `jdbc` store under high write throughput.
3. **No custom format**: Following the standard means the server does not
   impose a non-standard key format that clients would have to special-case.

**Validation rules applied by the server:**

- Length must be exactly 36 characters (canonical hyphenated form), matching
  the spec's `minLength`/`maxLength` of 36.
- Must parse as a UUID whose version field reports `7`.
- Must use the RFC 9562 variant (the two most-significant bits of the variant
  octet are `10`).

Correctness does **not** depend on the embedded timestamp: expiry uses a
server-assigned `expires_at` (see [Section 4](#4-database-schema)). The
timestamp is used only as a defense-in-depth sanity check and for diagnostics.

---

## 2. Goals

1. Prevent duplicate side-effects when clients retry mutations with the same key.
2. Work correctly in multi-node deployments (shared storage).
3. Survive node failures (durable state).
4. Minimal latency overhead on the happy path (single extra DB round-trip).
5. Configurable storage backend (pluggable SPI).
6. Spec-compliant: validate UUIDv7 format, advertise lifetime in config.

### Non-Goals

- Cross-datacenter replication of idempotency records.
- Idempotency for read (GET/HEAD) operations.
- Guaranteed delivery (client-side concern).
- Heartbeat/liveness tracking (mutations are sub-second).

---

## 3. Architecture

### 3.1 Request Flow Overview

```
Client                 Load Balancer            Node A / Node B
  |                         |                         |
  |-- POST + Idempotency-Key: <uuid> --------------->|
  |                         |                         |
  |                         |          +--------------+---------------+
  |                         |          | 1. Validate UUIDv7 format    |
  |                         |          | 2. store.reserve(key)        |
  |                         |          |    +- RESERVED -> execute    |
  |                         |          |    +- DUPLICATE -> replay    |
  |                         |          | 3. Execute mutation          |
  |                         |          | 4. store.finalize(key, resp) |
  |                         |          +--------------+---------------+
  |<-- Response ------------------------------------ |
```

### 3.2 IdempotencyStore SPI

A storage-agnostic interface that any backend can implement:

```java
public interface IdempotencyStore extends Closeable {

  enum ReserveResult { RESERVED, DUPLICATE }

  /**
   * Attempt to reserve a key. Returns DUPLICATE if already reserved or finalized.
   *
   * @param idempotencyKey the client-provided UUIDv7 key
   * @param operationBinding request identity, e.g. "POST /v1/cat1/namespaces/ns1/tables"
   * @param expiresAt instant after which this record may be purged
   * @return RESERVED if newly claimed, DUPLICATE if already exists
   */
  ReserveResult reserve(String idempotencyKey, String operationBinding, Instant expiresAt);

  /**
   * Load a finalized record for replay.
   *
   * @param idempotencyKey the client-provided key
   * @return the record if finalized, empty if not yet finalized or not found
   */
  Optional<IdempotencyRecord> load(String idempotencyKey);

  /**
   * Mark a reserved key as finalized with the response to replay on duplicates.
   *
   * @param idempotencyKey the client-provided key
   * @param httpStatus the HTTP status code of the original response
   * @param responseSummary serialized response body (JSON string)
   */
  void finalize(String idempotencyKey, int httpStatus, String responseSummary);

  /**
   * Release a reservation so the client can retry with the same key.
   * Called when the mutation fails with a retryable error.
   *
   * @param idempotencyKey the client-provided key
   */
  void release(String idempotencyKey);

  /**
   * Purge expired records. Called periodically by a background task.
   *
   * @param before cutoff instant; records expiring before this time are removed
   * @return number of records purged
   */
  int purgeExpired(Instant before);
}
```

### 3.3 IdempotencyRecord

```java
public class IdempotencyRecord {
  private final String idempotencyKey;     // UUIDv7 (RFC 9562), 36-char canonical form
  private final String operationBinding;   // e.g., "POST /v1/cat1/namespaces/ns1/tables"
  private final Integer httpStatus;        // null while reserved / in-progress
  private final String responseSummary;    // serialized response body (JSON)
  private final long createdAtMs;          // unix epoch millis (matches BIGINT column)
  private final long expiresAtMs;          // unix epoch millis (matches BIGINT column)
}
```

Timestamps are stored as `long` unix epoch milliseconds to match Gravitino's
existing metadata tables (e.g., the `deleted_at` / `timestamp` columns in
`metalake_meta`, `catalog_meta`, and `commit_metrics_report`), which all use
`BIGINT` epoch millis rather than SQL `TIMESTAMP`.

### 3.4 Storage Implementations

| Implementation               | Use Case              | Durability | Multi-Node |
| ---------------------------- | --------------------- | ---------- | ---------- |
| `InMemoryIdempotencyStore`   | Dev, testing, single  | No         | No         |
| `JdbcIdempotencyStore`       | Production            | Yes        | Yes        |

The JDBC implementation leverages Gravitino's existing relational backend
(H2 for dev, MySQL or PostgreSQL for production).

### 3.5 In-Memory Store Capacity and Eviction

The `InMemoryIdempotencyStore` is a node-local Caffeine cache bounded by two
limits:

- **Time-based**: `Caffeine.expireAfterWrite(lifetime)` removes records once
  their reuse window has elapsed (same TTL as `expires_at`).
- **Size-based**: `Caffeine.maximumSize(idempotency-max-entries)` caps the
  number of retained records.

When the number of stored keys exceeds `idempotency-max-entries`, Caffeine
evicts entries using its Window-TinyLFU policy (approximate LRU) **before** they
naturally expire. The consequences are:

- An evicted-but-not-yet-expired key loses its stored response. A subsequent
  retry with that key is then treated as a new request and the mutation is
  re-executed. This weakens the idempotency guarantee under memory pressure.
- Because eviction can also drop an in-progress `RESERVED` entry, the in-memory
  store is explicitly documented as **best-effort, single-node, dev/test only**.
  Production deployments MUST use the `jdbc` store, which has no such cap.

Mitigations:

- Size `idempotency-max-entries` for the expected peak throughput multiplied by
  the key lifetime (roughly `requests_per_second * lifetime_seconds`).
- Emit an eviction-rate metric and log a warning when eviction is frequent,
  signaling the cache is under-provisioned or that the `jdbc` store should be
  used instead.

---

## 4. Database Schema

The table name, column names, and column types follow Gravitino's existing
metadata table conventions (see `scripts/mysql/schema-*.sql` and
`scripts/mysql/iceberg-metrics-schema-*.sql`): a `*_meta` table name,
backtick-quoted `snake_case` columns, `BIGINT` unix-epoch-millis timestamps, a
MEDIUMTEXT `audit_info` column, per-column `COMMENT`s, and
`ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`.

MySQL:

```sql
CREATE TABLE IF NOT EXISTS `iceberg_idempotency_key_meta` (
    `idempotency_key`   VARCHAR(36)         NOT NULL COMMENT 'client-provided UUIDv7 idempotency key',
    `operation_binding` VARCHAR(512)        NOT NULL COMMENT 'request identity, e.g. POST /v1/{prefix}/namespaces/{ns}/tables',
    `http_status`       INT UNSIGNED        DEFAULT NULL COMMENT 'HTTP status of finalized response; NULL while reserved',
    `response_summary`  MEDIUMTEXT          DEFAULT NULL COMMENT 'serialized response body (JSON) for replay',
    `audit_info`        MEDIUMTEXT          NOT NULL COMMENT 'idempotency record audit info',
    `created_at`        BIGINT(20) UNSIGNED NOT NULL COMMENT 'record created at (unix epoch millis)',
    `expires_at`        BIGINT(20) UNSIGNED NOT NULL COMMENT 'record expires at (unix epoch millis)',
    PRIMARY KEY (`idempotency_key`),
    KEY `idx_iik_expires` (`expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'Iceberg REST idempotency key records';
```

The H2 and PostgreSQL variants follow the same column names and types (with
database-specific keywords) and are shipped alongside the existing scripts as
`scripts/h2/iceberg-idempotency-schema-<version>-h2.sql` and
`scripts/postgresql/iceberg-idempotency-schema-<version>-postgresql.sql`,
mirroring the layout of the existing `iceberg-metrics-schema-<version>-*.sql`
files.

### 4.1 Row Lifecycle

1. **RESERVED**: Row inserted with `http_status = NULL`. Mutation is executing.
2. **FINALIZED**: `http_status` and `response_summary` written. Ready for replay.
3. **EXPIRED**: `expires_at < now()`. Purged by background cleanup
   (see [Section 5.4](#54-expiry-and-cleanup-of-outdated-data)).

### 4.2 Conflict Handling

`reserve()` uses `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL),
`INSERT IGNORE` (MySQL), or equivalent. If zero rows inserted, the key is
already reserved or finalized - return `DUPLICATE` and load the existing record
for replay.

---

## 5. Detailed Request Flow

### 5.1 Happy Path (First Request)

1. If idempotency is disabled (`idempotency-enabled=false`, the default),
   ignore the `Idempotency-Key` header and execute the mutation directly.
2. Extract `Idempotency-Key` header; validate UUIDv7 format (400 if invalid).
3. If header absent, execute mutation directly (no idempotency overhead).
4. Call `store.reserve(key, binding, now + lifetime)`.
5. Result = `RESERVED`: execute mutation, call `store.finalize(key, status, body)`.
6. Return response to client.

### 5.2 Replay Path (Duplicate Key)

1. Extract and validate header.
2. Call `store.reserve(key, binding, expiresAt)` - returns `DUPLICATE`.
3. Call `store.load(key)`:
   - If finalized: reconstruct and return cached response.
   - If in-progress (not yet finalized): return `409 Conflict` with
     `Retry-After` header (another node is still executing).
4. Return cached response.

### 5.3 Execution Failure

If the mutation throws an exception:

- **Retryable errors** (e.g., transient DB failure): call `store.release(key)`
  so the client can retry with the same key.
- **Terminal errors** (e.g., `AlreadyExistsException`): call
  `store.finalize(key, 409, errorBody)` so retries get the same error response.

This matches the Iceberg REST spec's finalization rules for the
`Idempotency-Key` header:

- **Finalize & replay**: `200`, `201`, `204`, and deterministic terminal `4xx`
  responses (including `409` such as `AlreadyExists`, `NamespaceNotEmpty`).
- **Do not finalize** (not stored/replayed): `5xx` responses, which are treated
  as retryable and cause `release()`.

### 5.4 Expiry and Cleanup of Outdated Data

Every record carries `expires_at = created_at + idempotency-key-lifetime`. The
spec states catalogs SHOULD NOT expire keys before the advertised lifetime, so
cleanup only ever removes records whose window has already elapsed.

**JDBC store.** A background task scheduled every
`idempotency-cleanup-interval` calls `store.purgeExpired(now)`, which runs:

```sql
DELETE FROM iceberg_idempotency_key_meta WHERE expires_at < ? LIMIT ?;
```

- Deletes are performed in bounded batches (using `LIMIT` / repeated execution)
  to avoid long-held locks and large transactions.
- The `idx_iik_expires` index makes the predicate efficient.
- The delete is safe to run concurrently on multiple nodes: it is idempotent
  and only targets already-expired rows, so no leader election or distributed
  lock is required. A small grace period is added to the cutoff to tolerate
  clock skew between nodes.

**In-memory store.** No separate task is needed: Caffeine's
`expireAfterWrite(lifetime)` removes expired entries lazily/automatically, and
size-based eviction bounds memory
(see [Section 3.5](#35-in-memory-store-capacity-and-eviction)).

---

## 6. Configuration

All keys use the `gravitino.iceberg-rest.` prefix used by the Iceberg REST
server. Only `idempotency-key-lifetime` is defined by the Iceberg REST spec and
advertised to clients; every other key is Gravitino server-side configuration
and is **not** exposed via `GET /v1/config`.

| Config Key                                       | Default     | Description                                                                                              |
| ------------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.idempotency-enabled`     | `false`     | Master switch. Disabled by default (matching Polaris). When `false`, the `Idempotency-Key` header is ignored and no lifetime is advertised. |
| `gravitino.iceberg-rest.idempotency-key-lifetime`| `PT30M`     | Client reuse window (ISO-8601 duration). Advertised to clients as the spec-defined `idempotency-key-lifetime` in `GET /v1/config`. |
| `gravitino.iceberg-rest.idempotency-store-type`  | `in-memory` | `in-memory` or `jdbc`.                                                                                   |
| `gravitino.iceberg-rest.idempotency-max-entries` | `10000`     | Max cached entries (in-memory store only). See [Section 3.5](#35-in-memory-store-capacity-and-eviction).|
| `gravitino.iceberg-rest.idempotency-cleanup-interval` | `PT5M` | Interval for the background task that purges expired records (jdbc store).                               |

### 6.1 Iceberg Spec Configuration

The only idempotency setting defined by the Iceberg REST spec is
`idempotency-key-lifetime`, together with the `Idempotency-Key` request header.
Both are defined in the Iceberg REST OpenAPI spec:

- `Idempotency-Key` parameter and `idempotency-key-lifetime` `CatalogConfig`
  field:
  [`open-api/rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

When idempotency is enabled, it is advertised in `GET /v1/config` as:

```json
{
  "defaults": {
    "idempotency-key-lifetime": "PT30M"
  }
}
```

Per the spec, presence of this field signals that the server supports
`Idempotency-Key` semantics; when absent (i.e., `idempotency-enabled=false`),
clients MUST assume idempotency is not supported.

### 6.2 Database Configuration (`jdbc` store)

When `idempotency-store-type=jdbc`, the store connects to a relational database
using the following keys, mirroring the JDBC configuration style already used by
Gravitino's Iceberg REST metrics store:

| Config Key                                              | Default | Description                                                        |
| ------------------------------------------------------- | ------- | ------------------------------------------------------------------ |
| `gravitino.iceberg-rest.idempotency-jdbc-url`           | (none)  | JDBC connection URL, e.g. `jdbc:mysql://host:3306/gravitino`.       |
| `gravitino.iceberg-rest.idempotency-jdbc-driver`        | (none)  | JDBC driver class name.                                            |
| `gravitino.iceberg-rest.idempotency-jdbc-user`          | (none)  | Database user.                                                     |
| `gravitino.iceberg-rest.idempotency-jdbc-password`      | (none)  | Database password. Supplied via secret/env; never logged.          |
| `gravitino.iceberg-rest.idempotency-jdbc-max-pool-size` | `10`    | Maximum JDBC connection pool size.                                |

By default the `jdbc` store reuses the same backend as Gravitino's relational
entity store; the keys above allow pointing it at a dedicated database when
needed.

---

## 7. Comparison with Polaris

| Aspect              | Polaris (PR #3205)              | This Design                                     |
| ------------------- | ------------------------------- | ----------------------------------------------- |
| Storage             | Postgres-specific               | Gravitino relational backend (H2/MySQL/PG)      |
| Heartbeat           | Yes (long-running ops)          | No (mutations are sub-second)                   |
| Reconciliation      | Yes (crash recovery)            | Simplified: TTL-based expiry + release on fail  |
| Executor ownership  | Yes (multi-worker)              | No (single-owner per key via DB constraint)     |
| Response storage    | Minimal summary + pointer       | Full response body (mutation responses are small)|

Gravitino's Iceberg mutations (create/drop/update) are typically sub-second,
which makes heartbeat-based liveness and crash reconciliation unnecessary
complexity. The simpler reserve-execute-finalize model is sufficient. If
long-running operations are added in the future (e.g., async purge), heartbeat
support can be added to the SPI without breaking existing implementations.

---

## 8. Implementation Phases

### Phase 1: SPI and In-Memory Store

- Define `IdempotencyStore` interface and `IdempotencyRecord` in
  `iceberg/iceberg-common`.
- Implement `InMemoryIdempotencyStore` using Caffeine cache.
- Wire into endpoint operations with `@HeaderParam("Idempotency-Key")`.
- Advertise `idempotency-key-lifetime` in `GET /v1/config`.
- Unit tests for all mutation endpoints.

### Phase 2: JDBC Store

- Add `iceberg_idempotency_key_meta` table schema for H2, MySQL, and PostgreSQL.
- Implement `JdbcIdempotencyStore` using Gravitino's existing connection pool.
- Background cleanup task for expired records.
- Integration tests with testcontainers.

### Phase 3: Observability (Out of Scope for Now)

Deferred until the SPI and JDBC store land; not required for the initial
implementation:

- Metrics: cache hit rate, replay count, store latency, in-memory eviction rate.

---

## 9. Testing Strategy

- **Unit tests**: Store SPI implementations (in-memory and JDBC), UUIDv7 /
  RFC 9562 validation (reject UUIDv4, wrong length, bad variant), replay logic,
  terminal vs. retryable error handling, in-memory size-based eviction, and
  `purgeExpired` batching.
- **Integration tests**: Multi-threaded concurrent reserve/finalize on the same
  key, TTL-based expiry, cleanup task removing only expired rows, JDBC store
  with H2.
- **E2E tests**: With `idempotency-enabled=false` the header is ignored; with
  it enabled, retry with same key returns cached response, different key
  executes fresh, invalid key returns 400, and `idempotency-key-lifetime` is
  advertised in `GET /v1/config`.
