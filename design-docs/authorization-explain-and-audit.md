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

# Design of Authorization Explain, Access Review and Queryable Audit in Gravitino

## Background

Gravitino can say "no", but it cannot say why, and it cannot say who else can say "yes". Three
questions come up in every governed deployment, and today each one ends in a server log:

| Question | What a user has today |
|----------|-----------------------|
| "Why was I denied?" | A `403` whose message echoes the endpoint's raw authorization expression |
| "Who can read this table?" | `GET /objects/{type}/{fullName}/roles` lists role names bound to that one object, not its ancestors, and not the users behind them |
| "Who read, changed or was denied on this table last week?" | `grep` over `gravitino_audit.log` on every server node |

### How a request is authorized

`BaseMetadataAuthorizationMethodInterceptor` reads the `@AuthorizationExpression` on the REST
method, converts it to OGNL with `AuthorizationExpressionConverter`, and evaluates it with
`AuthorizationExpressionEvaluator`. The leaves of the expression are calls on `GravitinoAuthorizer`
(`authorize`, `deny`, `isOwner`, `isServiceAdmin`, `isSelf`, and a few others).
`JcasbinAuthorizer` answers `authorize` and `deny` from two JCasbin enforcers, one for `ALLOW` rows
and one for `DENY` rows. Each enforcer is keyed by role id, metadata type, metadata id and
privilege.

Loading a table, for example, is:

```text
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)
```

`ANY_SELECT_TABLE` expands to an `ALLOW` on any of the metalake, catalog, schema or table, and no
`DENY` on any of them. A caller who holds `SELECT_TABLE` but not `USE_SCHEMA` is denied, and the
documentation calls this out because it surprises people.

### What a denial produces

```text
403 Forbidden
User 'alice' is not authorized to perform operation 'loadTable' on metadata
'lake.sales.orders.daily' with expression 'ANY(OWNER, METALAKE, CATALOG) || ...'
```

The same denial is dispatched as an `AuthorizationDenialFailureEvent`, carrying the user, object,
method name and expression, and written by `FileAuditWriter`. Nothing records which leaf failed,
whether a `DENY` fired, or which role it came from. `JcasbinAuthorizer.diagnoseDenial` logs a
per-role policy count, but only at `DEBUG` level and only on the node that served the request.

### What the audit log can and cannot do

`AuditLogManager` registers one `ASYNC_ISOLATED` event listener that formats each post event and
hands it to one `AuditLogWriter`. The default writer, `FileAuditWriter`, logs through Log4j2. A
deployment with three server nodes has three audit files and no way to filter them by metalake,
object, user or outcome without shipping them to an external log system first. The events
themselves are rich: `operationType` (172 values, including `AUTHORIZATION_DENIAL`),
`operationStatus`, `eventSource` (Gravitino, Iceberg REST, Lance REST), `remoteAddress`, and a
redacted `customInfo` map.

Related: [#14245](https://github.com/apache/gravitino/issues/14245) (authorization hardening epic,
bug fixes only), [#14765](https://github.com/apache/gravitino/issues/14765) (Iceberg REST denials
losing audit context, fixed), [#12758](https://github.com/apache/gravitino/issues/12758)
(tag-based access control, which adds a new source of grants that explain must later report),
`design-docs/gravitino-role-assumption.md` (active roles).

---

## Goals

1. **Explain a decision**: `POST /api/metalakes/{metalake}/authorization/explain` returns, for one
   principal, one operation or privilege and one object, the allow or deny result together with a
   trace of every check in the expression and the roles that granted or denied each one.
2. **Exact agreement**: For every registered authorization expression, the `allowed` field of an
   explain response equals the result the interceptor would compute for the same principal, active
   roles and stored grants. A test enforces this over every expression constant.
3. **Actionable output**: A failed explain lists, for each top-level alternative of the expression,
   the checks that would have to pass for that alternative to succeed and the `DENY` grants that
   block it.
4. **Access review**: `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/access?privilege=...`
   returns every role that grants or denies the privilege on the object or any ancestor, the users
   and groups holding those roles, and the owners along the chain.
5. **Queryable audit**: With `gravitino.audit.store.enabled=true`, every audit event is persisted
   in the relational store and can be filtered by user, object, operation type, outcome, source and
   time through `GET /api/metalakes/{metalake}/audit/events`, with the same answer from every server
   node.
6. **Self-service**: Any metalake user can explain their own requests and query their own audit
   events without an administrator.
7. **No new leakage**: Neither explain nor audit query reveals the existence of an object, or the
   grants of another principal, beyond what the caller can already learn through existing APIs.

---

## Non-Goals

1. **Changing authorization semantics**: Explain reads decisions and never changes them. Any
   disagreement between explain and enforcement is an explain bug.
2. **Explaining external authorization plugins**: Ranger or other pushed-down enforcement happens
   in the engine or plugin. Explain covers the Gravitino server's own checks only.
3. **Operation-level "who can" across all principals**: Access review answers at privilege level.
   Whether a principal can run a whole operation also depends on group membership that arrives in
   the caller's token at request time and is not stored, so it cannot be enumerated. Explain
   answers the operation-level question for one named principal.
4. **Server-wide audit queries**: Events with no metalake, such as metalake creation by a service
   administrator, are stored but not queryable in this design. A follow-up can add an admin-only
   `/api/audit/events`.
5. **Replacing log shipping**: The file writer stays the default. The audit store is a bounded,
   queryable window with retention, not a long-term archive or a SIEM.
6. **Historical explain**: Explain evaluates current grants. Replaying an old audit event explains
   it against today's grants, and the response says so.

---

## Solution Investigations

### Explain

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Richer `403` message listing missing privileges | No new endpoint | Grows every denial, leaks grant structure to callers of an endpoint they may not be entitled to diagnose, cannot explain another principal | Rejected |
| B. Second, hand-written "explainer" that reimplements the rules | Easy to shape output | Two implementations of authorization drift, which is exactly the failure mode an explain API must not have | Rejected |
| C. Re-evaluate the same parsed OGNL tree without short-circuiting, through a recording authorizer (chosen) | Uses the interceptor's expression, converter and authorizer, so there is one source of truth; testable for equality | Needs an AST walk and a small authorizer extension for role attribution | **Chosen** |

### Role attribution

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| JCasbin `enforceEx` explanation | Built in | Reports one matched rule per effect, not every contributing role; depends on effect-model internals | Rejected |
| Filtered policy lookup per request role, under the existing read lock (chosen) | Same data and lock as `enforce`, already used by `hasDenyPolicy` and `diagnoseDenial`; returns every matching role | One extra in-memory scan per leaf, only on explain requests | **Chosen** |

### Access review

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Read JCasbin enforcer state | Fast | Holds only roles of users who recently made requests | Rejected |
| Read role grants and role holders from the relational store (chosen) | Complete; existing `listRolesByMetadataObject`, `listUsersByRoleIdent`, `listGroupsByRoleIdent` | A few queries per ancestor | **Chosen** |

### Queryable audit

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| A. Document shipping file logs to Elasticsearch or Loki | No code | No Gravitino API, no permission-aware access, no self-service, every deployment builds its own | Rejected as the only answer; still supported |
| B. A new `AuditLogWriter` implementation | Reuses the writer SPI | `AuditLogManager` loads exactly one writer, so choosing the store would turn off the file log; writers receive formatted lines, not typed fields | Rejected |
| C. A separate audit store listener with its own switch, writing typed rows to the relational store (chosen) | Runs beside the file writer; typed columns allow indexed filters; shared by all nodes | Adds write volume to the metadata database, bounded by filters and retention | **Chosen** |

---

## Proposal

### 1. Operation registry

At startup each server collects every REST method carrying `@AuthorizationExpression` into an
`AuthorizationOperationRegistry`, keyed by a stable operation id `<source>:<Resource>.<method>`:

```text
gravitino:TableOperations.loadTable
iceberg-rest:IcebergTableOperations.loadTable
lance-rest:LanceTableOperations.describeTable
```

Startup fails if two methods map to the same id. On a denial the interceptor adds one response
header, leaving the error body unchanged for existing clients:

```text
X-Gravitino-Authz-Operation: gravitino:TableOperations.loadTable
```

The id is also stored with the denial event (section 5), so an audit row can be explained directly.

### 2. Explain API

#### POST /api/metalakes/{metalake}/authorization/explain

**Request:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `operation` | string | one of `operation`, `privilege`, `auditEventId` | Operation id from the registry |
| `privilege` | string | one of the three | A single privilege, such as `SELECT_TABLE`, evaluated with the matching `ANY_*` rule |
| `auditEventId` | string | one of the three | Re-explain a stored denial: takes operation, object and principal from the event |
| `object` | object | yes, unless `auditEventId` | `{ "type": "TABLE", "fullName": "sales.orders.daily" }` |
| `principal` | object | no | `{ "name": "bob", "groups": ["analysts"] }`. Defaults to the caller, with the caller's token groups |

The `X-Gravitino-Active-Roles` header applies exactly as it does for the operation being
explained. When `principal` names someone else, active roles default to all of that principal's
roles.

**Response:** `200 OK`

```json
{
  "allowed": false,
  "operation": "gravitino:TableOperations.loadTable",
  "principal": { "name": "alice", "groups": ["analysts"], "explicitGroups": false },
  "evaluatedAt": "2026-11-11T18:40:02Z",
  "trace": {
    "kind": "OR", "result": false,
    "children": [
      { "kind": "CHECK", "check": "OWNER", "object": "METALAKE:lake", "result": false },
      { "kind": "CHECK", "check": "OWNER", "object": "CATALOG:sales", "result": false },
      { "kind": "AND", "result": false, "children": [
          { "kind": "CHECK", "check": "PRIVILEGE", "privilege": "USE_CATALOG", "result": true,
            "allowedBy": [ { "role": "analyst", "grantedOn": "METALAKE:lake" } ], "deniedBy": [] },
          { "kind": "CHECK", "check": "PRIVILEGE", "privilege": "USE_SCHEMA", "result": false,
            "allowedBy": [], "deniedBy": [] },
          { "kind": "CHECK", "check": "PRIVILEGE", "privilege": "SELECT_TABLE", "result": false,
            "allowedBy": [ { "role": "analyst", "grantedOn": "CATALOG:sales" } ],
            "deniedBy": [ { "role": "contractor", "grantedOn": "SCHEMA:sales.orders" } ] }
      ] }
    ]
  },
  "alternatives": [
    { "description": "Own the metalake or catalog", "satisfied": false,
      "missing": ["OWNER on METALAKE:lake or CATALOG:sales"], "blockedBy": [] },
    { "description": "USE_CATALOG, USE_SCHEMA and SELECT_TABLE, MODIFY_TABLE or table ownership",
      "satisfied": false,
      "missing": ["USE_SCHEMA on SCHEMA:sales.orders or an ancestor"],
      "blockedBy": ["DENY SELECT_TABLE from role contractor on SCHEMA:sales.orders"] }
  ],
  "preChecks": [
    { "check": "USER_IN_METALAKE", "result": true },
    { "check": "ACTIVE_ROLES_HELD", "result": true }
  ],
  "notes": []
}
```

**Behavior:**

- `trace` mirrors the parsed expression: `OR`, `AND` and `NOT` nodes, and `CHECK` leaves for
  `PRIVILEGE`, `OWNER`, `SERVICE_ADMIN`, `SELF`, `SET_OWNER` and `GRANT_PRIVILEGE` calls. Every
  child is evaluated, including those the interceptor would short-circuit, so all alternatives are
  visible. `result` on composite nodes uses normal boolean semantics, so the root equals `allowed`.
- A `PRIVILEGE` leaf reports the scope it was evaluated at and both effects: `allowedBy` lists
  `ALLOW` grants among the request's active roles, `deniedBy` lists `DENY` grants among all held
  roles, because inactive roles still deny (see the role assumption design).
- `alternatives` is derived from the top-level `OR`. For each branch it lists the failing positive
  leaves as `missing` and the fired `DENY` leaves as `blockedBy`. Labels for the shared macros
  (`ANY_USE_SCHEMA` and similar) come from a description table in
  `AuthorizationExpressionConstants`, so they read in privilege terms rather than OGNL.
- `preChecks` covers what the interceptor runs before the expression: the user belongs to the
  metalake, and named active roles are held. A failed pre-check sets `allowed` to `false`, and the
  trace still shows what the expression would have returned.
- Operations whose authorization completes inside a request-specific handler (the Iceberg REST
  `LoadTableAuthzHandler`, `RenameTableAuthzHandler` and siblings, and Lance) report a
  `HANDLER` node. Handlers move to the same recording authorizer in phase 1, and until a handler is
  converted its node carries `explained: false` and the handler's boolean result only.
- `principal.explicitGroups` is `true` when the request supplied `groups` for another principal.
  The server cannot verify IdP group claims for someone who is not making the request, so the
  result is conditional on that list, and `notes` says so.
- `auditEventId` explains the event's operation, object and principal against current grants and
  adds a note giving the event time.

**Errors:** `400` if none or more than one of `operation`, `privilege` and `auditEventId` is set,
or if the operation id is unknown. `403` if the caller may not explain the named principal.
`404` if `auditEventId` does not exist or is not visible to the caller.

**Who may call it:**

| Explaining | Required |
|------------|----------|
| Yourself | Metalake user. No further privilege |
| Another user or group | `METALAKE::OWNER` or `METALAKE::MANAGE_GRANTS` |

**Leakage rules.** Explaining yourself reveals only your own role names, which `GET /users/{self}`
already returns. A missing object and an object you hold no grants on produce the same trace,
because `authorizeObject` already returns "not authorized" for an unresolved id, so explain gives
no existence oracle. `grantedOn` for your own grants names objects you were granted on, which you
can already see via your roles.

### 3. Access review API

#### GET /api/metalakes/{metalake}/objects/{type}/{fullName}/access

**Query parameters:** `privilege` (required, one `Privilege.Name`), `limit` (principals, 1-1000,
default 200).

**Response:** `200 OK`

```json
{
  "object": "TABLE:sales.orders.daily",
  "privilege": "SELECT_TABLE",
  "grants": [
    { "role": "analyst", "grantedOn": "CATALOG:sales", "condition": "ALLOW" },
    { "role": "contractor", "grantedOn": "SCHEMA:sales.orders", "condition": "DENY" }
  ],
  "principals": [
    { "type": "USER", "name": "alice", "via": ["analyst", "contractor"], "effect": "DENIED",
      "traversal": { "useCatalog": true, "useSchema": false } },
    { "type": "GROUP", "name": "analysts", "via": ["analyst"], "effect": "ALLOWED",
      "traversal": { "useCatalog": true, "useSchema": true } }
  ],
  "owners": [
    { "object": "METALAKE:lake", "owner": { "type": "USER", "name": "admin" } },
    { "object": "TABLE:sales.orders.daily", "owner": { "type": "GROUP", "name": "sales-eng" } }
  ],
  "truncated": false
}
```

**Behavior:**

- The ancestor chain is metalake, catalog, every level of a multi-level schema, then the object,
  the same chain `JcasbinAuthorizer.buildSchemaInheritanceChain` walks.
- `grants` contains every role securable object on that chain carrying the privilege, with its
  condition.
- `principals` contains each user and group bound to at least one listed role. `effect` is
  `DENIED` if any of that subject's roles denies the privilege anywhere on the chain, otherwise
  `ALLOWED`. Evaluation uses that subject's own roles only. A user may gain or lose access through
  token groups, which is stated in the documentation and why explain exists.
- `traversal` reports `USE_CATALOG` and `USE_SCHEMA` for the same subject, because most data
  operations need them (see Background).
- Owners of the metalake and catalog satisfy most operations outright, so they are always
  reported.
- `truncated` is `true` when more than `limit` principals exist.

**Who may call it:** the caller must pass `hasMetadataPrivilegePermission(metalake, type,
fullName)`, the check that already gates granting and revoking on the object. Anyone who can change
who has access can see who has access.

### 4. Authorizer extension

One default method is added to `GravitinoAuthorizer`:

```java
/**
 * Explains a single privilege decision. The default derives the effect from authorize and deny
 * and reports no role attribution, so authorizers that cannot attribute still work.
 */
default PrivilegeDecision explain(
    Principal principal,
    String metalake,
    MetadataObject metadataObject,
    Privilege.Name privilege,
    AuthorizationRequestContext requestContext) {
  boolean allowed = authorize(principal, metalake, metadataObject, privilege, requestContext);
  boolean denied = deny(principal, metalake, metadataObject, privilege, requestContext);
  return PrivilegeDecision.unattributed(allowed, denied);
}
```

`JcasbinAuthorizer` overrides it. Under `rolePolicyLock.readLock()` it filters each enforcer's `p`
rows by request role subject, metadata type, metadata id and action, and maps role ids back to
names through the loaded-roles cache. This is the same lookup `diagnoseDenial` and `hasDenyPolicy`
already perform, so it adds no new consistency rules. `PassThroughAuthorizer` keeps the default.

### 5. Audit store

#### Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `gravitino.audit.store.enabled` | `false` | Persist audit events in the relational store |
| `gravitino.audit.store.categories` | `WRITE,FAILURE,DENIAL` | Any of `READ`, `WRITE`, `FAILURE`, `DENIAL` |
| `gravitino.audit.store.retentionDays` | `30` | Rows older than this are purged |
| `gravitino.audit.store.queueCapacity` | `10000` | Pending events before new ones are dropped and counted |
| `gravitino.audit.store.batchSize` | `200` | Rows per insert batch |

The store is independent of `gravitino.audit.enabled`, so a deployment can keep the file log,
the store, or both.

Every `OperationType` is classified as `READ` or `WRITE` in an explicit `EnumMap`. A unit test
fails when a new `OperationType` has no classification, so the default (no successful reads, which
dominate volume) stays correct as operations are added. Any `FailureEvent` is `FAILURE`, and
`AUTHORIZATION_DENIAL` is `DENIAL`.

#### Data model

Added to `scripts/{h2,mysql,postgresql}/schema-2.0.0-*.sql` and the `1.3.0-to-2.0.0` upgrade
scripts:

```sql
CREATE TABLE IF NOT EXISTS `audit_event` (
  `id`               BIGINT UNSIGNED NOT NULL COMMENT 'IdGenerator value, tie-breaker only',
  `metalake_name`    VARCHAR(128)    NULL,
  `event_time`       BIGINT          NOT NULL COMMENT 'epoch millis',
  `user_name`        VARCHAR(128)    NOT NULL,
  `operation_type`   VARCHAR(64)     NOT NULL,
  `category`         VARCHAR(16)     NOT NULL,
  `operation_status` VARCHAR(16)     NOT NULL,
  `event_source`     VARCHAR(32)     NOT NULL,
  `object_type`      VARCHAR(32)     NULL,
  `object_name`      VARCHAR(1024)   NULL COMMENT 'full name below the metalake',
  `authz_operation`  VARCHAR(256)    NULL COMMENT 'operation id, denials only',
  `remote_address`   VARCHAR(64)     NULL,
  `server_node`      VARCHAR(128)    NOT NULL COMMENT 'host name of the writing server',
  `custom_info`      MEDIUMTEXT      NULL COMMENT 'redacted JSON',
  PRIMARY KEY (`id`),
  KEY `idx_ae_time` (`metalake_name`, `event_time`, `id`),
  KEY `idx_ae_user` (`metalake_name`, `user_name`, `event_time`),
  KEY `idx_ae_object` (`metalake_name`, `object_name`(255), `event_time`),
  KEY `idx_ae_status` (`metalake_name`, `operation_status`, `event_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'audit events';
```

Object names are stored rather than ids on purpose. An audit row must still describe a table after
it is renamed or dropped, which is exactly when people go looking. `custom_info` passes through
`AuditLogRedactor.redactCustomInfo` before it is written, the same as the JSON file formatter.

#### Write path

```text
EventBus ── post event ──► AuditStoreListener (ASYNC_ISOLATED)
                              │  classify, filter by category
                              ▼
                         bounded queue ──► batch writer thread ──► audit_event (INSERT batch)
                                                    │
                                     failures: retry once, then drop and count
```

Audit writes never block or fail a user request. Dropped events are exported as the
`audit-store-dropped-events` metric and logged at `WARN` at most once a minute. Retention runs on
the existing garbage collector schedule as a batched `DELETE ... WHERE event_time < ? LIMIT ?`.

#### GET /api/metalakes/{metalake}/audit/events

**Query parameters:**

| Name | Description |
|------|-------------|
| `user` | Exact user name |
| `objectType`, `object` | Exact object, for example `TABLE` and `sales.orders.daily` |
| `objectPrefix` | Everything under a catalog or schema, for example `sales.orders.` |
| `operationType` | One or more `OperationType` values |
| `status` | `SUCCESS`, `FAILURE` or `UNKNOWN` |
| `category` | `READ`, `WRITE`, `FAILURE` or `DENIAL` |
| `source` | An `EventSource`: `GRAVITINO_SERVER`, `GRAVITINO_ICEBERG_REST_SERVER` or `GRAVITINO_LANCE_REST_SERVER` |
| `from`, `to` | ISO-8601 instants. `to` defaults to now, `from` to 24 hours earlier |
| `limit`, `pageToken` | 1-500, default 100. Keyset pagination on `(event_time, id)` descending |

**Response:** `200 OK`

```json
{
  "events": [
    {
      "id": "1733912402000123",
      "time": "2026-11-11T18:40:02.000Z",
      "user": "alice",
      "operationType": "AUTHORIZATION_DENIAL",
      "category": "DENIAL",
      "status": "FAILURE",
      "source": "GRAVITINO_SERVER",
      "object": { "type": "TABLE", "fullName": "sales.orders.daily" },
      "authzOperation": "gravitino:TableOperations.loadTable",
      "remoteAddress": "10.0.4.7",
      "serverNode": "gravitino-1",
      "customInfo": {}
    }
  ],
  "nextPageToken": "eyJ0IjoxNzMzOTEyNDAyMDAwLCJpZCI6MTczMzkxMjQwMjAwMDEyM30"
}
```

**Behavior:** returns `404` when the audit store is disabled, so clients can tell "off" from
"empty". The time window is capped at `gravitino.audit.store.retentionDays`. Every query uses one of
the four indexes, because `metalake_name` and a time range are always present.

**Who may call it:**

| Query | Required |
|-------|----------|
| `user` equal to the caller | Metalake user |
| Anything else | `METALAKE::OWNER` or the new metalake-level privilege `VIEW_AUDIT` |

`VIEW_AUDIT` is added to `Privilege.Name` with the next free bit and is bindable on the metalake
only. Audit rows name objects the reader may not be able to load, which is the point of an audit.
That is why reading other users' events is a separate, explicit privilege rather than derived from
object access.

### 6. Clients and tooling

| Surface | Addition |
|---------|----------|
| Java client | `GravitinoMetalake#explainAuthorization(ExplainRequest)`, `#reviewAccess(MetadataObject, Privilege.Name)`, `#listAuditEvents(AuditEventQuery)` |
| Python client | The same three on `GravitinoMetalake` |
| CLI | `gcli authz explain`, `gcli authz access`, `gcli audit list` |
| Web UI | "Why?" link on access-denied pages, an "Access" tab on object pages, an audit page for `VIEW_AUDIT` holders |
| MCP server | Read-only `explain_access` and `list_audit_events` tools |

### User process

1. Alice runs `SELECT * FROM sales.orders.daily` in Spark and gets a `403` carrying
   `X-Gravitino-Authz-Operation: gravitino:TableOperations.loadTable`.
2. Alice calls explain with that operation and the table, with no special privilege. The response
   says she holds `USE_CATALOG` and `SELECT_TABLE` through `analyst`, is missing `USE_SCHEMA`, and
   that a `DENY SELECT_TABLE` from `contractor` would block her anyway.
3. Alice sends the response to the catalog owner, who calls access review for `SELECT_TABLE` on the
   table and sees that `contractor` denies it on the whole `sales.orders` schema.
4. The owner removes `contractor` from Alice and grants `USE_SCHEMA` to `analyst`. Alice's explain
   now returns `allowed: true`.
5. An auditor with `VIEW_AUDIT` queries `/audit/events?object=sales.orders.daily&from=...` to see
   every read, change and denial on the table across all server nodes, and explains any denial row
   with `auditEventId`.

### Implementation process

```text
Client ──► ExplainOperations (REST)
              │ resolve operation id ─► AuthorizationOperationRegistry ─► expression string
              │ permission check (self, or OWNER/MANAGE_GRANTS)
              │ PrincipalUtils.doAs(target principal)
              ▼
         AuthorizationExplainer
              │ parse: AuthorizationExpressionConverter + the shared OGNL parse cache
              │ pre-checks: checkCurrentUser, findUnheldRoles
              │ walk AST (no short circuit), one OgnlContext per leaf evaluation
              ▼
         RecordingAuthorizer  ──delegates──►  GravitinoAuthorizer (JcasbinAuthorizer)
              │ records each call + result      │ authorize / deny / isOwner ...
              │ for authorize/deny: explain()   │ explain(): filtered p rows under read lock
              ▼
         ExplainResponse (trace, alternatives, preChecks, notes)
```

The walker evaluates each leaf AST node on its own through `Ognl.getValue(node, context, root)`,
with `authorizer` bound to the recording wrapper. It combines child results itself for `ASTOr`,
`ASTAnd` and `ASTNot`, and treats any other node type as a leaf. A fresh
`AuthorizationRequestContext` is used, so explain never shares per-request caches with real
traffic.

The equality goal (Goal 2) is enforced by `TestAuthorizationExplainerEquivalence`. It iterates over
every `@AuthorizationExpression` value in the registry, over a fixture of grant combinations
(owners, allow and deny at each level, active-role narrowing), and asserts that
`explain.allowed == AuthorizationExpressionEvaluator.evaluate`.

### Backward compatibility

- No existing endpoint, response body, event or configuration default changes. The only visible
  difference on existing calls is the new `X-Gravitino-Authz-Operation` header on `403` responses.
- `GravitinoAuthorizer#explain` is a default method, so third-party authorizers compile and run
  unchanged.
- `VIEW_AUDIT` is a new enum value. Older clients deserializing a role that contains it fail the
  same way they would for any privilege added in a newer server, the existing behavior for new
  privileges.
- The audit store is off by default. Turning it on requires the 2.0.0 schema, which is applied by
  the existing upgrade scripts.

---

## Task Breakdown

### Phase 1: Explain

- [ ] Add `AuthorizationOperationRegistry` with startup uniqueness checks, and the
      `X-Gravitino-Authz-Operation` header on denials in `BaseMetadataAuthorizationMethodInterceptor`
- [ ] Add `PrivilegeDecision` and the default `GravitinoAuthorizer#explain`, and implement it in
      `JcasbinAuthorizer` with role attribution
- [ ] Add `RecordingAuthorizer` and `AuthorizationExplainer` (non-short-circuit AST walk,
      pre-checks, alternatives) in `server-common`
- [ ] Add `TestAuthorizationExplainerEquivalence` over every registered expression
- [ ] Add macro descriptions to `AuthorizationExpressionConstants` for readable alternatives
- [ ] Add `POST /api/metalakes/{metalake}/authorization/explain` in `server/`, with self and
      `MANAGE_GRANTS` permission rules
- [ ] Route the Iceberg REST and Lance authorization handlers through the recording authorizer
      (parallel)
- [ ] Java client, Python client and CLI support for explain (parallel)
- [ ] OpenAPI spec (`docs/open-api/*.yaml`, validated with `./gradlew :docs:build`) and
      `docs/security/access-control.md` section "Diagnosing a Denial"

### Phase 2: Access review

- [ ] Add `AccessReviewService` in `core/` over `listRolesByMetadataObject`,
      `listUsersByRoleIdent`, `listGroupsByRoleIdent` and owner lookups along the ancestor chain
- [ ] Add `GET /api/metalakes/{metalake}/objects/{type}/{fullName}/access` in `server/`
- [ ] Java client, Python client and CLI support (parallel)
- [ ] OpenAPI spec and documentation

### Phase 3: Queryable audit

- [ ] Add `audit_event` to H2, MySQL and PostgreSQL schema and upgrade scripts
- [ ] Add the `OperationType` to category classification with a completeness test
- [ ] Add `AuditStoreListener`, the batch writer and metrics in `core/`, wired from
      `AuditLogManager` behind `gravitino.audit.store.enabled`
- [ ] Record the operation id on `AuthorizationDenialFailureEvent`
- [ ] Add retention purge to the garbage collector
- [ ] Add `VIEW_AUDIT` to `Privilege.Name`, privilege mapping and documentation
- [ ] Add `GET /api/metalakes/{metalake}/audit/events` with keyset pagination
- [ ] Support `auditEventId` in explain
- [ ] Java client, Python client and CLI support (parallel)
- [ ] OpenAPI spec, `docs/gravitino-server-config.md` and an audit guide
- [ ] Docker integration test: denial in Spark through Iceberg REST, then audit query and explain
      on a two-node server against MySQL
