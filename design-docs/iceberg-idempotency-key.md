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
  private final String idempotencyKey;
  private final String operationBinding;   // e.g., "POST /v1/cat1/namespaces/ns1/tables"
  private final Integer httpStatus;        // null while in-progress
  private final String responseSummary;    // serialized response body (JSON)
  private final Instant createdAt;
  private final Instant expiresAt;
}
```

### 3.4 Storage Implementations

| Implementation               | Use Case              | Durability | Multi-Node |
| ---------------------------- | --------------------- | ---------- | ---------- |
| `InMemoryIdempotencyStore`   | Dev, testing, single  | No         | No         |
| `JdbcIdempotencyStore`       | Production            | Yes        | Yes        |

The JDBC implementation leverages Gravitino's existing relational backend
(H2 for dev, MySQL or PostgreSQL for production).

---

## 4. Database Schema

```sql
CREATE TABLE IF NOT EXISTS idempotency_records (
  idempotency_key   VARCHAR(64)  NOT NULL PRIMARY KEY,
  operation_binding TEXT         NOT NULL,
  http_status       INTEGER,              -- NULL while in-progress
  response_summary  TEXT,                 -- JSON body for replay
  created_at        TIMESTAMP   NOT NULL,
  expires_at        TIMESTAMP   NOT NULL
);

CREATE INDEX idx_idemp_expires ON idempotency_records (expires_at);
```

### 4.1 Row Lifecycle

1. **RESERVED**: Row inserted with `http_status = NULL`. Mutation is executing.
2. **FINALIZED**: `http_status` and `response_summary` written. Ready for replay.
3. **EXPIRED**: `expires_at < now()`. Purged by background cleanup.

### 4.2 Conflict Handling

`reserve()` uses `INSERT ... ON CONFLICT DO NOTHING` (PostgreSQL),
`INSERT IGNORE` (MySQL), or equivalent. If zero rows inserted, the key is
already reserved or finalized - return `DUPLICATE` and load the existing record
for replay.

---

## 5. Detailed Request Flow

### 5.1 Happy Path (First Request)

1. Extract `Idempotency-Key` header; validate UUIDv7 format (400 if invalid).
2. If header absent, execute mutation directly (no idempotency overhead).
3. Call `store.reserve(key, binding, now + lifetime)`.
4. Result = `RESERVED`: execute mutation, call `store.finalize(key, status, body)`.
5. Return response to client.

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

---

## 6. Configuration

| Config Key                         | Default     | Description                      |
| ---------------------------------- | ----------- | -------------------------------- |
| `idempotency-key-lifetime-minutes` | 30          | TTL for idempotency records      |
| `idempotency-store-type`           | `in-memory` | `in-memory` or `jdbc`            |
| `idempotency-max-entries`          | 10000       | Max entries (in-memory only)     |

Advertised in `GET /v1/config` as:

```json
{
  "defaults": {
    "idempotency-key-lifetime": "PT30M"
  }
}
```

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

- Add `idempotency_records` table schema for H2, MySQL, and PostgreSQL.
- Implement `JdbcIdempotencyStore` using Gravitino's existing connection pool.
- Background cleanup task for expired records.
- Integration tests with testcontainers.

### Phase 3: Observability (Future)

- Metrics: cache hit rate, replay count, store latency.
- Admin API to inspect/purge idempotency records.

---

## 9. Testing Strategy

- **Unit tests**: Store SPI implementations (in-memory and JDBC), UUIDv7
  validation, replay logic, terminal vs. retryable error handling.
- **Integration tests**: Multi-threaded concurrent reserve/finalize on the same
  key, TTL-based expiry, JDBC store with H2.
- **E2E tests**: Retry with same key returns cached response, different key
  executes fresh, invalid key returns 400.
