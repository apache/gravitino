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

# Design: Spark Iceberg REST Catalog Automatic Registration for Apache Gravitino

---

## Background

Connecting Spark to Gravitino takes little configuration:

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://127.0.0.1:8090
spark.sql.gravitino.metalake=test
```

But accessing Iceberg tables through the Gravitino Iceberg REST server still requires hand-written
configuration per catalog, duplicating what the REST server already manages and needing an edit
whenever catalogs are added or removed:

```text
spark.sql.catalog.iceberg_prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_prod.type=rest
spark.sql.catalog.iceberg_prod.uri=http://127.0.0.1:9001/iceberg/
spark.sql.catalog.iceberg_prod.warehouse=iceberg_prod
```

---

## Goals

1. **Automatic Iceberg registration**: A Spark session configured with only the new plugin and the
   Iceberg REST server URI registers one Spark Iceberg REST catalog per catalog served by that
   server, with no per-catalog configuration.
2. **Server-authoritative catalog list**: The REST server tells Spark which catalogs it serves, so
   Spark never guesses catalog names. 
3. **User configuration always wins**: A catalog the user configured by hand is never touched, and
   this is enforced by mechanism rather than by convention.
4. **Zero impact when disabled**: Users who do not add the new plugin see no behavior change.

---

## Non-Goals

1. **Lance registration in V1**: The design covers Lance (see [Lance support](#lance-support)), but
   V1 implements only Iceberg; Lance registration ships later, once its authorization gap is closed.
2. **Engines beyond Spark**: Flink and Trino may reuse the listing endpoint later.
3. **Iceberg REST specification changes**: The listing endpoint is a Gravitino-private extension.

---

## Proposal

Two new pieces — a **catalog-listing endpoint** on the Iceberg REST server and a Spark plugin that
consumes it — plus one **ordering rule** that makes the interaction with `GravitinoSparkPlugin`
deterministic.

### Catalog-listing endpoint

#### GET `{iceberg-rest-base}/gravitino/v1/catalogs`

Placed outside the Iceberg REST specification's `/v1/` namespace (default deployment:
`http://<host>:9001/iceberg/gravitino/v1/catalogs`) to mark it as a Gravitino-private extension.

**Request:** No parameters.

**Response:** `200 OK`

```json
{
  "catalogs": [
    { "name": "iceberg_prod", "properties": {} },
    { "name": "iceberg_audit", "properties": {} }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `catalogs[].name` | string | Catalog name as accepted by this server's `warehouse` parameter |
| `catalogs[].properties` | map | Reserved for non-sensitive, client-relevant metadata; V1 defines no keys. Per-catalog client configuration already arrives via `GET /v1/config?warehouse=<name>`, so this endpoint only enumerates names |

### GravitinoIcebergRestSparkPlugin

Once configured, this plugin fetches the catalog list from the Gravitino Iceberg REST server at
Spark session startup and writes the corresponding `spark.sql.catalog.*` entries, so users no longer
hand-write them. Which catalogs are registered, and under what Spark name, is decided by
`CatalogRegistrationPolicy`.

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoIcebergRestSparkPlugin
```

It issues the listing request with Iceberg's own `RESTClient` (`org.apache.iceberg.rest.HTTPClient`,
the one `RESTCatalog` uses), not the Gravitino client: the target is the Iceberg REST server, the
Iceberg runtime is already on this plugin's classpath, and it parses `ErrorResponse` the same way
table calls do (authentication is covered below).

Configuration uses the `spark.sql.gravitino.icebergRest.*` prefix, disjoint from the existing
plugin's keys:

| Configuration | Required | Default | Description |
|---------------|----------|---------|-------------|
| `spark.sql.gravitino.icebergRest.uri` | Yes | None | Base URI of the Iceberg REST server, e.g. `http://127.0.0.1:9001/iceberg/` |
| `spark.sql.gravitino.icebergRest.catalogProperties.<key>` | No | None | Client properties copied into every generated catalog as `spark.sql.catalog.<name>.<key>`, applied as defaults below the generated keys (see precedence below), and passed to the listing client (static auth only, see Authentication) |
| `spark.sql.gravitino.icebergRest.registrationPolicy` | No | Default implementation | FQCN of a `CatalogRegistrationPolicy` |

#### Registration policy interface

```java
/** Decides whether an advertised Iceberg REST catalog is registered, and under what Spark name. */
@DeveloperApi
public interface CatalogRegistrationPolicy {

  /**
   * Whether to register this catalog automatically as a Spark Iceberg REST catalog.
   *
   * @param catalogName a catalog name advertised by the Iceberg REST server; names already claimed
   *     by user configuration are filtered out by the plugin and never reach this method
   * @return true to register, false to skip
   */
  boolean shouldRegister(String catalogName);

  /**
   * The Spark catalog name to register an accepted catalog under. Defaults to the advertised name.
   *
   * @param catalogName the accepted catalog name
   * @return the Spark catalog name
   */
  default String sparkCatalogName(String catalogName) {
    return catalogName;
  }
}
```

The default implementation registers every advertised catalog under its advertised name, keeping a
1:1 identity between the Spark catalog name, the REST server catalog name, and `warehouse`.
Deployments that need to register a subset, or to rename catalogs on the Spark side, implement the
interface and point `registrationPolicy` at their class.

#### Generated configuration

For each accepted catalog, with `<sparkName>` from `sparkCatalogName`, the plugin generates:

```text
spark.sql.catalog.<sparkName>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<sparkName>.type=rest
spark.sql.catalog.<sparkName>.uri=<spark.sql.gravitino.icebergRest.uri>
spark.sql.catalog.<sparkName>.warehouse=<advertisedName>
```

`warehouse` is always the advertised name, so a rename changes the Spark-facing name only and
routing stays server-authoritative; the plugin logs any non-identity mapping. It also injects
`IcebergSparkSessionExtensions` into `spark.sql.extensions`, reusing the existing deduplication so a
manually configured extension is not added twice.

**How user configuration interacts.** User configuration has the highest priority; the plugin fills
in only what the user left unset. Precedence for each registered catalog, high to low, enforced by
the plugin so no policy can weaken it:

1. **User implementation key** `spark.sql.catalog.<name>`: the user owns that name entirely — the
   catalog is dropped before the policy runs and nothing is generated for it.
2. **User per-catalog sub-key** (`spark.sql.catalog.<name>.warehouse`, `.s3.access-key-id`, …): wins
   over the generated value, including the core routing keys `type`, `uri`, and `warehouse`.
3. **Plugin-generated keys**: the implementation class, `type`, `uri`, and `warehouse`.
4. **Global `catalogProperties.<key>`**: copied in as a per-catalog default, so it never overrides a
   generated key — a stray `catalogProperties.warehouse` cannot hijack routing.

Startup still fails fast only for policy output that cannot be resolved: a returned Spark name that
duplicates another catalog's name, collides with a name the user already configured, or is not a
valid Spark identifier.

**Authentication.** Secured deployments configure authentication once through
`icebergRest.catalogProperties.<key>`, which the plugin copies into every generated catalog, so no
per-catalog authentication configuration is needed. The generated catalogs support whatever the
Iceberg `RESTCatalog` accepts, including the full OAuth2 client-credentials flow.

The plugin's own listing call is the exception. A raw `RESTClient` does not run the
`RESTCatalog` auth lifecycle — `AuthManager` loading and `AuthSession` token exchange happen a layer
above it — so in V1 the listing call authenticates with static forms only (`token`, `header.*`).
OAuth2-only deployments must additionally supply a static token for listing (the generated catalogs
still use the full OAuth2 flow). Reproducing the `AuthManager`/`AuthSession` lifecycle, with proper
resource cleanup, so listing can share the catalogs' OAuth2 credentials is future work.

**Storage credentials.** The plugin generates no storage configuration; how data-plane credentials
reach Spark is intentionally left to the two existing Iceberg REST paths:

- **Credential vending** (`data-access=vended-credentials` on the Gravitino catalog): the load-table
  response vends per-table credentials, and the client needs nothing configured. This is the
  zero-configuration path this design targets.
- **No vending**: `GET /v1/config` returns no static access keys, so users supply credentials on the
  client side — through environment variables or the storage SDK's default credential chain, or
  per-catalog sub-keys such as `spark.sql.catalog.<name>.s3.access-key-id`.

### Plugin ordering and precedence

When both plugins are configured, `GravitinoIcebergRestSparkPlugin` **must be listed first**:

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoIcebergRestSparkPlugin,\
              org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
```

The REST plugin validates this at initialization and fails fast otherwise. Initializing first is
what lets it treat every `spark.sql.catalog.*` entry it sees as user-written, with no markers and no
state shared between the plugins.

`GravitinoSparkPlugin` needs no new rule: `registerCatalog` already asserts
`!sparkConf.contains("spark.sql.catalog." + name)` before writing, and the caller catches the
failure per catalog, so an already-registered name is skipped while the rest still register. Only
the reporting changes: today the skip is logged as `Register catalog X failed` with a stack trace,
which would now fire once per REST-registered catalog and read as an error.

### User process

1. Deploy the Gravitino Iceberg REST server (either config provider works — the listing endpoint
   reflects both).
2. Add to Spark configuration:

   ```text
   spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoIcebergRestSparkPlugin
   spark.sql.gravitino.icebergRest.uri=http://127.0.0.1:9001/iceberg/
   ```

   To keep `GravitinoSparkPlugin` for non-Iceberg catalogs, list it **after** the REST plugin.
3. Start the session — the plugin lists catalogs and registers one Spark catalog per selected name.
4. Query: `SELECT * FROM iceberg_prod.db.table`. Catalogs added later appear after a session
   restart, with no Spark configuration change.

### Implementation process

```text
Spark Driver startup (spark.plugins: GravitinoIcebergRestSparkPlugin, GravitinoSparkPlugin)
  ├─ GravitinoIcebergRestSparkPlugin.init()
  │    ├─ validate plugin ordering (fail fast if listed after GravitinoSparkPlugin)
  │    ├─ snapshot SparkConf  (only user-written entries exist at this point)
  │    ├─ GET {uri}/gravitino/v1/catalogs ──► Gravitino Iceberg REST Server
  │    │     (auth via icebergRest.catalogProperties.*)  ├─ dynamic provider → Gravitino server
  │    │                                                 └─ static provider  → local config
  │    ├─ drop names claimed by user conf → policy.shouldRegister / policy.sparkCatalogName
  │    └─ apply the user-configuration rules above, then write
  │       spark.sql.catalog.<sparkName>.* and inject extensions
  │
  ├─ GravitinoSparkPlugin.init()   (if enabled)
  │    └─ existing behavior: already skips any <name> whose spark.sql.catalog.<name> is set
  │
  └─ Table access: Spark → Iceberg REST protocol → Gravitino Iceberg REST Server → backend
```

Governance is unchanged: table access still flows through the Iceberg REST server, which remains the
enforcement point for authorization and audit.

---

## Lance support

A design sketch, not part of V1. Lance reuses everything above — the registration policy, the
ordering rule, and the user-configuration precedence — and differs only in discovery and the
generated entries.

**Discovery needs no new API.** The Lance Namespace protocol's root list,
`GET {lance-rest-base}/v1/namespace/list`, already returns the `lakehouse-generic` catalogs from
Gravitino, and the Lance REST server rejects anything else, so the advertised names are by
construction valid `parent` values. The list is paginated, so the plugin follows `page_token`.
Filtering to "only Lance catalogs" is neither possible nor needed: `format` is a table property, not
a catalog-level marker, and the server serves exactly this set.

The plugin could instead ask the Gravitino server for the generic catalog list directly. The
deciding difference is the user's mental model. With the REST server, discovery source and data
source are the same endpoint — the one `lanceRest.uri` Spark already queries for tables — and the
model is identical to Iceberg: point the plugin at a REST server, get its catalogs. Going through
Gravitino splits that into two systems (Gravitino lists them, the REST server serves them) plus the
unstated assumption that the names line up, and makes Lance discovery behave differently from
Iceberg. That mismatch stays invisible until a name does not line up, which fails at query time
rather than at configuration time. Discovery therefore stays server-authoritative; Gravitino's
per-principal authorization, the one thing this path would have reused, is instead added on the
Lance server (see below).

**A parallel plugin.** `GravitinoLanceRestSparkPlugin` mirrors the Iceberg keys under
`spark.sql.gravitino.lanceRest.{uri, catalogProperties.<key>, registrationPolicy}` and lists
catalogs with `org.lance.namespace.client.apache.api.NamespaceApi`, the plain-Java client in the
lance-spark bundle, for the same reason the Iceberg plugin uses Iceberg's `RESTClient`. The ordering
rule generalizes: every REST registration plugin comes before `GravitinoSparkPlugin`.

For each accepted catalog it generates:

```text
spark.sql.catalog.<sparkName>=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.<sparkName>.impl=rest
spark.sql.catalog.<sparkName>.uri=<spark.sql.gravitino.lanceRest.uri>
spark.sql.catalog.<sparkName>.parent=<advertisedName>
```

`parent` is Lance's `warehouse`, and the four core keys are the implementation key, `impl`, `uri`,
and `parent`. Two differences from the Iceberg plugin:

- **No session extension is injected.** Gravitino's documented Lance Spark usage never sets
  `spark.sql.extensions`, and the extension class is absent from older lance-spark bundles, so
  injecting it would risk breaking startup.
- **Storage configuration is static pass-through.** Lance delivers it per table in
  `DescribeTableResponse.storageOptions`, resolved from the catalog's and table's `lance.storage.*`
  properties. The plugin generates nothing either way; unlike Iceberg vending, secrets in catalog
  properties reach the client, with no vending equivalent today.

One gap must close before Lance ships: the root list is authenticated but not
authorization-filtered, so every authenticated caller sees all catalog names. Filtering it with
`MetadataAuthzHelper`, as the Iceberg server already does for namespace listing, is a prerequisite —
not a follow-up.

---

## Limitations and Future Work

1. **Flink and Trino**: The server side is reusable as-is; only registration differs. Flink allows
   one catalog store per session, so it would resolve REST catalogs lazily inside
   `GravitinoCatalogStore` instead of adding a plugin. Trino would extend `CatalogRegister` to issue
   `CREATE CATALOG ... USING iceberg` with `iceberg.catalog.type=rest`, and additionally needs
   storage configuration in its native form plus a per-catalog credential-vending flag, which the
   reserved `properties` field can carry.
2. **Multiple REST servers**: V1 supports one `icebergRest.uri` per session; a named-server list is
   out of scope.