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

# Design of Governed Data Sharing in Gravitino

## Background

Gravitino governs who inside an organization can read a table. It has no way to give a table to
someone outside the organization (a partner, a customer, another business unit with its own
identity provider) without either copying the data or making that party a full Gravitino user.

Today a team that wants to share `sales.orders.daily` with a partner has three options:

| Option | Problem |
|--------|---------|
| Copy files to a bucket the partner can read | A second copy to pay for, keep fresh and delete. No record of what the partner read |
| Add the partner to the organization's IdP and grant roles | The partner gets the whole Gravitino API, sees catalog and schema listings, and has no expiry. Revoking means editing IdP accounts |
| Give the partner static storage keys | No table boundary, no expiry, no audit, and the keys outlive the agreement |

The building blocks for a better answer already exist in Gravitino:

1. **Iceberg REST service** (`iceberg/iceberg-rest-server`). Every major engine can read from it:
   Spark, Trino, Flink, PyIceberg, DuckDB, Snowflake and others. In auxiliary mode (inside the
   Gravitino server) it has access control and the entity store. It supports loading a table with
   only only the snapshots referenced by branches and tags (`IcebergRESTUtils.SnapshotMode.REFS`), server-side scan planning, and
   credential vending through `X-Iceberg-Access-Delegation`.
2. **Path-scoped credential vending.** `s3-token`, `adls-token`, `gcs-token`, `oss-token` and
   `cos-token` providers issue short-lived credentials limited to a table's location, with
   `READ` or `WRITE` privilege (`CredentialPrivilege`). Their lifetime is set by
   `*-token-expire-in-secs`.
3. **Audit events** carry an `eventSource`, so requests through the Iceberg REST service are
   already attributable.

What is missing is a model for *what* is shared, *with whom*, *until when*, and a way for an
outside party to authenticate without being a Gravitino user.

Searches of upstream issues, pull requests and discussions found no existing proposal for data
sharing, shares or recipients.

---

## Goals

1. **Shares and recipients**: A metalake can define shares (named sets of tables) and recipients
   (external parties). Each can be created, listed, updated and dropped through REST, the Java
   client and the Python client.
2. **Standard clients**: A recipient reads shared tables with an unmodified Iceberg REST client,
   configured with a URI, a warehouse name and a bearer token, and no Gravitino-specific library.
3. **Read-only, table-bounded access**: A recipient can list and read only the tables in shares
   granted to them. Every write endpoint is rejected, and vended credentials are `READ` and
   scoped to the shared table's location.
4. **Expiry and revocation**: Grants and tokens can carry an expiry. Revoking a grant, removing a
   table from a share or revoking a token rejects the recipient's next request on every server
   node. The time until already-vended storage credentials stop working is bounded by a
   configured maximum.
5. **Two-party consent**: Adding a table to a share requires ownership of the table or one of its
   ancestors. Granting a share to a recipient requires owning both the share and the recipient.
6. **Visibility for data owners**: The owner of a table can list every share that contains it.
7. **Audit**: Every recipient request produces an audit event naming the recipient, share, table
   and token.

---

## Non-Goals

1. **Row filters and column masks for recipients**: Restrictions that the reading engine enforces
   cannot be trusted when the engine belongs to another organization. Sharing a subset of rows or
   columns means sharing a view materialized as a table. A server-enforced form (scan planning
   returning pre-filtered files) can follow once the Iceberg REST read restrictions work lands.
2. **The Delta Sharing protocol**: Delta Sharing has wide client support, but its file-list model
   does not represent Iceberg delete files. The share model here is protocol-neutral, so a Delta
   Sharing adapter for tables without deletes can be added later without changing the model.
3. **Non-Iceberg tables**: Phase 1 shares tables served by the Iceberg REST service. Hive, Paimon,
   Lance and JDBC tables are out of scope until a protocol for them is chosen.
4. **Views, filesets, models and topics**: Tables only in this design.
5. **Standalone Iceberg REST server**: The standalone server has no entity store and no access
   control. Sharing requires the auxiliary-service deployment.
6. **Delivering tokens**: Gravitino returns a token once to the recipient's owner. Getting it to
   the partner (secret manager, encrypted email) is the operator's job.
7. **Federated recipient identity**: Recipients authenticate with Gravitino-issued tokens. Trusting
   a partner's own OIDC issuer is a possible follow-up.
8. **Hard revocation of in-flight credentials**: Cloud providers cannot recall an issued STS or SAS
   token. Revocation stops new access immediately and bounds old access by credential lifetime.

---

## Solution Investigations

### Serving protocol

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Make recipients Gravitino users with roles | No new model | Full API surface, catalog listings visible, IdP accounts for outsiders, no expiry, revocation spread over role edits | Rejected |
| B. A Delta Sharing server | Large client ecosystem, a protocol designed for this | Cannot express Iceberg delete files; a second table protocol to maintain; most Gravitino lakehouse tables are Iceberg | Deferred (Non-Goal 2) |
| C. Read-only Iceberg REST endpoint per share (chosen) | Every Iceberg engine works unchanged; reuses snapshot filtering, scan planning and credential vending; one more Gravitino can consume a share as an ordinary REST catalog | Iceberg tables only in phase 1 | **Chosen** |
| D. Copy data into a partner bucket on a schedule | Partner needs no access to the source | Cost, staleness, deletion obligations; not a catalog feature | Rejected |

### Recipient authentication

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Reuse the server's OAuth or Kerberos authenticator | One mechanism | Requires the partner in the organization's IdP, which is what sharing is meant to avoid | Rejected |
| Gravitino-issued bearer tokens, hashed at rest, valid only on share endpoints (chosen) | Works with every Iceberg client's `token` property; no external dependency; independent expiry and rotation | Gravitino must store and validate tokens | **Chosen** |

### Where shares live

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Encode shares as tags or policies on tables | No new entities | A share has its own owner, recipients and expiry, which tags do not model; removing a tag would silently revoke a partner | Rejected |
| New `SHARE` and `RECIPIENT` entities in the relational store (chosen) | Explicit ownership, expiry and audit; consistent on every node | New tables and meta services | **Chosen** |

---

## Proposal

### 1. Concepts

```text
Metalake
 ├── Share "acme_orders"        (owner: sales-eng)
 │    ├── object: TABLE sales.orders.daily    as  orders.daily      history: REFS
 │    └── object: TABLE sales.orders.returns  as  orders.returns    history: ALL
 └── Recipient "acme"           (owner: partner-ops)
      ├── token tk_7f3c… expires 2027-01-31
      └── grant: acme_orders  expires 2026-12-31
```

- A **share** is a named, owned set of tables in one metalake. Each entry has the **alias** the
  recipient sees, as `<namespace>.<table>`. The default alias is the source schema and table name,
  so source catalog names never leak.
- A **recipient** is an external party. It is not a user, has no roles, and can only call share
  endpoints.
- A **grant** gives a recipient read access to every current entry of a share, optionally until an
  expiry time.
- A **token** authenticates a recipient. A recipient can hold up to two active tokens so they can
  be rotated without downtime.

### 2. Management API

All under `/api/metalakes/{metalake}`. Request and response bodies follow the existing
entity conventions (`ShareDTO`, `RecipientDTO`, and `...UpdatesRequest` with `@type`-tagged
updates).

| Method and path | Purpose | Required |
|-----------------|---------|----------|
| `POST /shares` | Create a share | `CREATE_SHARE` on the metalake |
| `GET /shares`, `GET /shares/{share}` | List or load | Share owner or metalake owner |
| `PUT /shares/{share}` | Updates: `setComment`, `addObject`, `removeObject`, `setObjectHistory` | Share owner, plus ownership of the table for `addObject` |
| `DELETE /shares/{share}` | Drop a share and all its grants | Share owner or metalake owner |
| `POST /recipients` | Create a recipient | `CREATE_RECIPIENT` on the metalake |
| `GET /recipients`, `GET /recipients/{recipient}` | List or load | Recipient owner or metalake owner |
| `PUT /recipients/{recipient}` | Updates: `setComment`, `setAllowedAddresses` | Recipient owner |
| `DELETE /recipients/{recipient}` | Drop a recipient, its tokens and grants | Recipient owner or metalake owner |
| `POST /recipients/{recipient}/tokens` | Issue a token | Recipient owner |
| `GET /recipients/{recipient}/tokens` | List token ids, prefixes, expiry and last use | Recipient owner |
| `DELETE /recipients/{recipient}/tokens/{tokenId}` | Revoke a token | Recipient owner or metalake owner |
| `POST /shares/{share}/grants` | Grant a share to a recipient | Owner of both, or metalake owner |
| `DELETE /shares/{share}/grants/{recipient}` | Revoke a grant | Owner of either, or metalake owner |
| `GET /objects/{type}/{fullName}/shares` | Shares containing an object | Owner of the object or an ancestor |

`CREATE_SHARE` and `CREATE_RECIPIENT` are new metalake-level `Privilege.Name` values. Shares and
recipients are added to `MetadataObject.Type` for ownership, so the existing owner API sets and
transfers their owners. They take no other privileges.

#### PUT /shares/{share} update: addObject

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `object` | object | yes | `{ "type": "TABLE", "fullName": "lakehouse.sales.daily" }` |
| `alias` | string | no | `<namespace>.<table>` seen by recipients. Default: source schema and table |
| `history` | string | no | `REFS` (default: snapshots referenced by a branch or tag) or `ALL` |

**Behavior:** fails with `400` if:

- the table's catalog is not served by the Iceberg REST service
- the catalog's credential provider is not path-scoped (`*-token`)
- the provider's configured lifetime exceeds `gravitino.share.maxCredentialLifetimeSecs`
- the alias is already used in the share

It fails with `403` if the caller does not own the table, its schema, its catalog or the
metalake. The adding user is recorded on the entry.

#### POST /recipients/{recipient}/tokens

**Request:** `{ "expiresAt": "2027-01-31T00:00:00Z" }`. Optional, capped by
`gravitino.share.maxTokenLifetimeDays`.

**Response:** `201 Created`. The secret is returned only in this response.

```json
{
  "id": "tk_7f3c21",
  "token": "gvs_tk_7f3c21_3Jb0...redacted...",
  "expiresAt": "2027-01-31T00:00:00Z",
  "profile": {
    "uri": "https://gravitino.example.com/iceberg-share/",
    "token": "gvs_tk_7f3c21_3Jb0...",
    "warehouses": ["acme_orders"]
  }
}
```

**Behavior:** rejects a third active token with `409`. The server stores a SHA-256 hash of the
secret and the `tk_...` prefix only.

#### POST /shares/{share}/grants

**Request:** `{ "recipient": "acme", "expiresAt": "2026-12-31T00:00:00Z" }`. `expiresAt` is
optional.

**Response:** `201 Created` with the grant. Granting again replaces the expiry.

### 3. Recipient endpoint

The Iceberg REST service mounts a second, read-only application at `/iceberg-share/`. A share is
an Iceberg warehouse:

```properties
# Spark
spark.sql.catalog.acme=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.acme.type=rest
spark.sql.catalog.acme.uri=https://gravitino.example.com/iceberg-share/
spark.sql.catalog.acme.warehouse=acme_orders
spark.sql.catalog.acme.token=gvs_tk_7f3c21_...
spark.sql.catalog.acme.header.X-Iceberg-Access-Delegation=vended-credentials
```

| Endpoint | Behavior |
|----------|----------|
| `GET /v1/config?warehouse={share}` | Returns `prefix={share}` and read-only defaults |
| `GET /v1/{share}/namespaces`, `GET`/`HEAD .../namespaces/{ns}` | Namespaces derived from aliases, with no properties, so source locations are not exposed |
| `GET /v1/{share}/namespaces/{ns}/tables` | Aliases in that namespace |
| `GET`/`HEAD .../tables/{table}` | Source table metadata, trimmed to referenced snapshots when `history=REFS`, with `READ` credentials when access delegation is requested |
| `POST .../tables/{table}/plan`, `.../tasks` | Server-side scan planning, when enabled on the service |
| `GET .../tables/{table}/credentials` | Credential refresh, which long-running jobs call through `client.refresh-credentials-endpoint`. Runs the same checks, so a revoked recipient cannot renew |
| `POST .../tables/{table}/metrics` | Accepted and dropped |
| Any other route or method | `403 Forbidden` in Iceberg `ErrorResponse` form |

Every request runs these checks, in order. The first failure returns `401` for token problems and
`404` for everything else, so a recipient cannot probe for shares or tables they were not given:

1. The bearer token has the `gvs_` form, its hash matches an active token, and it has not expired.
2. The client address matches the recipient's allowed addresses, if any are set. This uses the
   trusted-proxy rules for `X-Forwarded-For`.
3. The warehouse names a share with an unexpired grant to this recipient.
4. The namespace and table resolve to a current entry of the share, and the source table exists.

Recipient requests never pass through Gravitino user authorization, because the recipient has no
user. The authority comes from the share entry, which was created by the table's owner.

Token, grant and entry lookups are cached per node for `gravitino.share.authCacheTtlSecs`
(default 30). Revocation therefore takes effect on every node within that time, and is immediate
on the node that served the revocation.

### 4. Credentials and revocation window

Vended credentials are requested with `CredentialPrivilege.READ` for the source table's location.
`addObject` refuses catalogs whose provider lifetime exceeds
`gravitino.share.maxCredentialLifetimeSecs` (default 3600), so the worst-case access after
revocation is that lifetime plus `authCacheTtlSecs`.

| Event | New requests | Credentials already vended |
|-------|--------------|----------------------------|
| Token revoked or expired | Rejected | Valid until their own expiry |
| Grant revoked or expired | Rejected | Valid until their own expiry |
| Table removed from share | Rejected for that table | Valid until their own expiry |
| Table dropped at source | `404` | Files gone or unreachable per storage lifecycle |

`history=REFS` trims the returned metadata to snapshots referenced by a branch or tag, using the existing `SnapshotMode.REFS` filtering. Credentials are scoped to
the table location, so a recipient that already knows an old file path could still read it. The
documentation states this, and owners who need a hard boundary share a table written for the
purpose.

### 5. Data model

Added to `scripts/{h2,mysql,postgresql}/schema-2.0.0-*.sql` and the `1.3.0-to-2.0.0` upgrade
scripts. Shown in MySQL form:

```sql
CREATE TABLE IF NOT EXISTS `share_meta` (
  `share_id`        BIGINT(20) UNSIGNED NOT NULL,
  `share_name`      VARCHAR(128) NOT NULL,
  `metalake_id`     BIGINT(20) UNSIGNED NOT NULL,
  `share_comment`   VARCHAR(256) DEFAULT '',
  `audit_info`      MEDIUMTEXT NOT NULL,
  `current_version` INT UNSIGNED NOT NULL DEFAULT 1,
  `last_version`    INT UNSIGNED NOT NULL DEFAULT 1,
  `deleted_at`      BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`share_id`),
  UNIQUE KEY `uk_mid_sn_del` (`metalake_id`, `share_name`, `deleted_at`)
);

CREATE TABLE IF NOT EXISTS `share_object_rel` (
  `id`                   BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `share_id`             BIGINT(20) UNSIGNED NOT NULL,
  `metadata_object_id`   BIGINT(20) UNSIGNED NOT NULL,
  `metadata_object_type` VARCHAR(64) NOT NULL,
  `alias_namespace`      VARCHAR(128) NOT NULL,
  `alias_name`           VARCHAR(128) NOT NULL,
  `history_mode`         VARCHAR(16) NOT NULL,
  `added_by`             VARCHAR(128) NOT NULL,
  `audit_info`           MEDIUMTEXT NOT NULL,
  `deleted_at`           BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_sid_alias_del` (`share_id`, `alias_namespace`, `alias_name`, `deleted_at`),
  KEY `idx_obj` (`metadata_object_id`, `metadata_object_type`)
);

CREATE TABLE IF NOT EXISTS `recipient_meta` (
  `recipient_id`      BIGINT(20) UNSIGNED NOT NULL,
  `recipient_name`    VARCHAR(128) NOT NULL,
  `metalake_id`       BIGINT(20) UNSIGNED NOT NULL,
  `recipient_comment` VARCHAR(256) DEFAULT '',
  `allowed_addresses` TEXT NULL COMMENT 'JSON list of CIDRs',
  `audit_info`        MEDIUMTEXT NOT NULL,
  `current_version`   INT UNSIGNED NOT NULL DEFAULT 1,
  `last_version`      INT UNSIGNED NOT NULL DEFAULT 1,
  `deleted_at`        BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`recipient_id`),
  UNIQUE KEY `uk_mid_rn_del` (`metalake_id`, `recipient_name`, `deleted_at`)
);

CREATE TABLE IF NOT EXISTS `recipient_token` (
  `token_id`     VARCHAR(32) NOT NULL,
  `recipient_id` BIGINT(20) UNSIGNED NOT NULL,
  `token_hash`   CHAR(64) NOT NULL COMMENT 'hex SHA-256 of the secret',
  `created_at`   BIGINT(20) UNSIGNED NOT NULL,
  `expires_at`   BIGINT(20) UNSIGNED NULL,
  `revoked_at`   BIGINT(20) UNSIGNED NULL,
  `last_used_at` BIGINT(20) UNSIGNED NULL,
  PRIMARY KEY (`token_id`),
  KEY `idx_rid` (`recipient_id`)
);

CREATE TABLE IF NOT EXISTS `share_recipient_rel` (
  `id`           BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `share_id`     BIGINT(20) UNSIGNED NOT NULL,
  `recipient_id` BIGINT(20) UNSIGNED NOT NULL,
  `expires_at`   BIGINT(20) UNSIGNED NULL,
  `granted_by`   VARCHAR(128) NOT NULL,
  `audit_info`   MEDIUMTEXT NOT NULL,
  `deleted_at`   BIGINT(20) UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_sid_rid_del` (`share_id`, `recipient_id`, `deleted_at`),
  KEY `idx_rid` (`recipient_id`)
);
```

Entries reference tables by id, so renaming a source table keeps it shared under the same alias.
Dropping a table soft-deletes its `share_object_rel` rows in the same transaction, through the
existing metadata-object cleanup in the meta services. Dropping a metalake drops its shares and
recipients. `last_used_at` is updated at most once a minute per token.

### 6. Audit

Recipient requests dispatch events through the existing event bus:

- The user is `recipient:<name>` and the event source is `GRAVITINO_ICEBERG_REST_SERVER`.
- `customInfo` carries `share`, `alias`, the source table full name, and `tokenId`.
- The operation types are the existing `LOAD_TABLE`, `LIST_TABLE`, `PLAN_TABLE_SCAN` and their
  siblings. Rejected recipient requests are `AUTHORIZATION_DENIAL` events.
- Management operations get new operation types, from `CREATE_SHARE` through
  `REVOKE_RECIPIENT_TOKEN`, with pre, post and failure events like other entities.

These rows become queryable through the audit store proposed in
[#14922](https://github.com/apache/gravitino/issues/14922), once it lands.

### 7. Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `gravitino.share.enabled` | `false` | Mount `/iceberg-share/` and enable the management API |
| `gravitino.share.maxCredentialLifetimeSecs` | `3600` | Largest provider lifetime allowed for shared tables |
| `gravitino.share.maxTokenLifetimeDays` | `365` | Cap on token `expiresAt`. Tokens without an expiry get this |
| `gravitino.share.authCacheTtlSecs` | `30` | Per-node cache of token, grant and entry lookups |

### User process

1. The data owner, who holds `CREATE_SHARE`, creates the share `acme_orders` and adds
   `lakehouse.sales.daily` with the alias `orders.daily`.
2. A partner manager, who holds `CREATE_RECIPIENT`, creates the recipient `acme`, sets its allowed
   address range and issues a token. They deliver the profile to Acme over a secure channel.
3. The share owner and the recipient owner grant `acme_orders` to `acme` until 2026-12-31. If one
   person owns both, one call does it.
4. Acme configures Spark, Trino or PyIceberg with the profile and runs
   `SELECT * FROM acme.orders.daily`. Their engine sees one namespace and one table, and reads the
   files with short-lived read credentials.
5. The table owner calls `GET /objects/table/lakehouse.sales.daily/shares` during a review and sees
   `acme_orders`.
6. On 2026-12-31 the grant expires. Acme's next request returns `404`, and their last credentials
   expire within the configured lifetime.

### Implementation process

```text
Recipient engine ──Bearer gvs_…──► /iceberg-share/v1/...  (Iceberg REST aux service)
                                        │
                              ShareAuthenticationFilter
                                        │ token hash ─► RecipientTokenCache ─► recipient_token
                                        │ address check (trusted proxies)
                                        ▼
                              ShareResolver
                                        │ share + grant + alias ─► ShareCache ─► share_* tables
                                        │ alias → source NameIdentifier
                                        ▼
                              ReadOnlyShareTableOperations
                                        │ IcebergCatalogWrapperManager.loadTable(source)
                                        │ snapshot filter (history=REFS)
                                        │ CredentialOperationDispatcher (READ, table path)
                                        ▼
                              EventBus ─► audit (recipient:<name>)

Gravitino REST ──► ShareOperations / RecipientOperations
                        │ authorization expressions (owner, CREATE_SHARE, CREATE_RECIPIENT)
                        ▼
                   ShareManager / RecipientManager ─► ShareMetaService / RecipientMetaService
```

`ReadOnlyShareTableOperations` delegates to the same catalog wrapper code paths as
`IcebergTableOperations`. It does not reimplement table loading, snapshot filtering or scan
planning. It only substitutes the alias for the source identifier on the way in, and on the way
out removes the source identifiers and table properties listed in
`gravitino.share.redactedTableProperties`, default `owner,comment`.

Recipient tokens are accepted only by `ShareAuthenticationFilter`, which is installed only on
`/iceberg-share/*`. The main `AuthenticationFilter` rejects any token with the `gvs_` prefix, so a
leaked recipient token cannot reach the Gravitino API or the regular Iceberg REST endpoint.

### Backward compatibility

- Off by default. With `gravitino.share.enabled=false` nothing is mounted and no new code runs on
  requests.
- New privileges `CREATE_SHARE` and `CREATE_RECIPIENT`, and new metadata object types `SHARE` and
  `RECIPIENT`, follow the existing rules for newly added enum values: older clients cannot
  deserialize roles or owners that reference them.
- New tables are added through the existing 2.0.0 schema and upgrade scripts. No existing table
  changes.

---

## Task Breakdown

### Phase 1: Model and management API

- [ ] Add `Share`, `Recipient`, `ShareChange` and `RecipientChange` interfaces and DTOs in `api/`
      and `common/`
- [ ] Add `CREATE_SHARE` and `CREATE_RECIPIENT` privileges and the `SHARE` and `RECIPIENT` metadata
      object types, with ownership support
- [ ] Add the five tables to H2, MySQL and PostgreSQL schema and upgrade scripts
- [ ] Add `ShareMetaService` and `RecipientMetaService` with mappers, plus cascade cleanup on table,
      schema, catalog and metalake drop
- [ ] Add `ShareManager` and `RecipientManager` with event dispatchers and new operation types
- [ ] Add token issuance, hashing, rotation limit and revocation
- [ ] Add REST endpoints for shares, recipients, tokens, grants and `/objects/.../shares` with
      authorization expressions
- [ ] Java client and Python client support (parallel)
- [ ] OpenAPI spec (`docs/open-api/*.yaml`, validated with `./gradlew :docs:build`)

### Phase 2: Recipient endpoint

- [ ] Add `ShareAuthenticationFilter`, the `gvs_` rejection in `AuthenticationFilter`, and the
      token, grant and entry caches
- [ ] Add the read-only `/iceberg-share/` application: config, namespaces, tables, load, scan
      planning, with every other route returning `403`
- [ ] Enforce path-scoped providers and `maxCredentialLifetimeSecs` in `addObject`
- [ ] Recipient audit events and `last_used_at` tracking
- [ ] Docker integration tests: Spark and PyIceberg read a shared table with vended S3 credentials
      from MinIO, then fail after grant revocation, token revocation and table removal
- [ ] User documentation `docs/data-sharing.md`, covering the revocation window and the
      `history=REFS` caveat

### Phase 3: Follow-ups

- [ ] CLI and web UI for shares and recipients (parallel)
- [ ] Delta Sharing adapter for tables without delete files
- [ ] Federated recipient identity through a trusted external OIDC issuer
