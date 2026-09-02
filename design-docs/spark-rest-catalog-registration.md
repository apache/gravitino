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

# Design: Spark Lakehouse REST Catalog Automatic Registration for Apache Gravitino

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

1. **Engines beyond Spark**: Flink and Trino may reuse the listing endpoint later.
2. **Iceberg REST specification changes**: The listing endpoint is a Gravitino-private extension.

---

## Proposal

Two new pieces — a **catalog-listing endpoint** on the Iceberg REST server and a single Spark
plugin, `GravitinoLakehouseRESTDiscoveryPlugin`, that consumes it — plus one **ordering rule** that
makes the interaction with `GravitinoSparkPlugin` deterministic. Each lakehouse format plugs into
the plugin as a provider; V1 ships Iceberg.

### Catalog-listing endpoint

#### GET `{iceberg-rest-base}/gravitino/v1/management/catalogs`

A Gravitino-private extension outside the Iceberg REST specification's `/v1/` namespace (default
deployment: `http://<host>:9001/iceberg/gravitino/v1/management/catalogs`). `management` is the
scope for further Gravitino management APIs on this server.

**Request:** No parameters.

**Response:** `200 OK`

```json
{
  "catalogs": [
    { "name": "iceberg_prod" },
    { "name": "iceberg_audit" }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `catalogs[].name` | string | Catalog name as accepted by this server's `warehouse` parameter |

The response only enumerates names: per-catalog client configuration already arrives via
`GET /v1/config?warehouse=<name>`. Because JSON objects extend compatibly, per-catalog fields can be
added later if an engine ever needs registration-time metadata (e.g. Trino's vending flag), so V1
does not speculatively define any.

### GravitinoLakehouseRESTDiscoveryPlugin

Once configured, this single plugin fetches the catalog list from each configured REST server at
Spark session startup and writes the corresponding `spark.sql.catalog.*` entries, so users no longer
hand-write them. Which catalogs are registered, and under what Spark name, is decided by
`CatalogRegistrationPolicy`.

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoLakehouseRESTDiscoveryPlugin
```

The plugin is format-agnostic. Each lakehouse format is a **provider** — Iceberg in V1, Lance later
(see [Lance support](#lance-support)) — carrying its own engine runtime and config prefix
(`icebergREST.*`, `lanceREST.*`). A provider is active only when its `uri` is set, so the URI
doubles as the per-format switch and no `enable*` flag is needed; if a `uri` is set but its provider
is not on the classpath, the plugin fails fast. Dependency isolation is preserved — a user who needs
only Iceberg puts only the Iceberg provider on the classpath — while `spark.plugins` lists one
plugin and there is a single ordering rule.

The registration policy and user-configuration precedence below are shared by every provider; the
listing client, generated entries, and credential handling are provider-specific.

#### Registration policy interface

A single plugin-level policy, selected with `spark.sql.gravitino.REST.registrationPolicy`, applies
to every provider (the default implementation applies when unset). Each method receives the `format`
that advertised the catalog — the same token as the config prefix, `"iceberg"` or `"lance"` — so one
policy can still apply format-specific rules without a policy per format.

```java
/** Decides whether an advertised REST catalog is registered, and under what Spark name. */
@DeveloperApi
public interface CatalogRegistrationPolicy {

  /**
   * Whether to register this catalog automatically as a Spark REST catalog.
   *
   * @param format the lakehouse format that advertised the catalog, e.g. "iceberg" or "lance"
   * @param catalogName a catalog name advertised by that format's REST server; names already
   *     claimed by user configuration are filtered out by the plugin and never reach this method
   * @return true to register, false to skip
   */
  boolean shouldRegister(String format, String catalogName);

  /**
   * The Spark catalog name to register an accepted catalog under. Defaults to the advertised name.
   *
   * @param format the lakehouse format that advertised the catalog
   * @param catalogName the accepted catalog name
   * @return the Spark catalog name
   */
  default String registeredCatalogName(String format, String catalogName) {
    return catalogName;
  }
}
```

The default implementation registers every advertised catalog under its advertised name, keeping a
1:1 identity between the Spark catalog name and the REST server catalog name. Deployments that need
to register a subset, rename catalogs on the Spark side, or treat formats differently implement the
interface and point `registrationPolicy` at their class.

#### User configuration precedence

User configuration has the highest priority; the plugin fills in only what the user left unset.
Precedence for each registered catalog, high to low, enforced by the plugin so no policy can weaken
it:

1. **User implementation key** `spark.sql.catalog.<name>`: the user owns that name entirely — the
   catalog is dropped before the policy runs and nothing is generated for it.
2. **User per-catalog sub-key** (`spark.sql.catalog.<name>.<key>`): wins over the generated value,
   including the provider's core routing keys.
3. **Plugin-generated keys**: the implementation class and the provider's core routing keys.
4. **Global `catalogProperties.<key>`**: copied in as a per-catalog default, so it never overrides a
   generated key — a stray `catalogProperties` core key cannot hijack routing.

Startup still fails fast only for policy output that cannot be resolved: a returned Spark name that
duplicates another catalog's name, collides with a name the user already configured, or is not a
valid Spark identifier.

#### Iceberg provider

The provider issues the listing request with Iceberg's own `RESTClient`
(`org.apache.iceberg.rest.HTTPClient`, the one `RESTCatalog` uses), not the Gravitino client: the
target is the Iceberg REST server, the Iceberg runtime is already on its classpath, and it parses
`ErrorResponse` the same way table calls do (authentication below). It uses the
`spark.sql.gravitino.icebergREST.*` prefix, disjoint from the existing plugin's keys:

| Configuration | Required | Default | Description |
|---------------|----------|---------|-------------|
| `spark.sql.gravitino.icebergREST.uri` | Yes | None | Base URI of the Iceberg REST server, e.g. `http://127.0.0.1:9001/iceberg/`; setting it activates the Iceberg provider |
| `spark.sql.gravitino.icebergREST.catalogProperties.<key>` | No | None | Client properties copied into every generated catalog as `spark.sql.catalog.<name>.<key>`, applied as defaults below the generated keys (see precedence above), and passed to the listing client (static auth only, see Authentication) |

For each accepted catalog, with `<sparkName>` from `registeredCatalogName`, it generates:

```text
spark.sql.catalog.<sparkName>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<sparkName>.type=rest
spark.sql.catalog.<sparkName>.uri=<spark.sql.gravitino.icebergREST.uri>
spark.sql.catalog.<sparkName>.warehouse=<advertisedName>
```

The four core keys are the implementation key, `type`, `uri`, and `warehouse`. `warehouse` is always
the advertised name, so a rename changes the Spark-facing name only and routing stays
server-authoritative; the plugin logs any non-identity mapping. It also injects
`IcebergSparkSessionExtensions` into `spark.sql.extensions`, reusing the existing deduplication so a
manually configured extension is not added twice.

**Authentication.** Secured deployments configure authentication once through
`icebergREST.catalogProperties.<key>`, which the plugin copies into every generated catalog, so no
per-catalog authentication configuration is needed. The generated catalogs support whatever the
Iceberg `RESTCatalog` accepts, including the full OAuth2 client-credentials flow.

The listing call is the exception. A raw `RESTClient` does not run the `RESTCatalog` auth
lifecycle — `AuthManager` loading and `AuthSession` token exchange happen a layer above it — so in
V1 the listing call authenticates with static forms only (`token`, `header.*`). OAuth2-only
deployments must additionally supply a static token for listing (the generated catalogs still use
the full OAuth2 flow). Reproducing the `AuthManager`/`AuthSession` lifecycle, with proper resource
cleanup, so listing can share the catalogs' OAuth2 credentials is future work.

**Storage credentials.** The provider generates no storage configuration; how data-plane credentials
reach Spark is intentionally left to the two existing Iceberg REST paths:

- **Credential vending** (`data-access=vended-credentials` on the Gravitino catalog): the load-table
  response vends per-table credentials, and the client needs nothing configured. This is the
  zero-configuration path this design targets.
- **No vending**: `GET /v1/config` returns no static access keys, so users supply credentials on the
  client side — through environment variables or the storage SDK's default credential chain, or
  per-catalog sub-keys such as `spark.sql.catalog.<name>.s3.access-key-id`.

### Plugin ordering and precedence

When both plugins are configured, `GravitinoLakehouseRESTDiscoveryPlugin` **must be listed first**:

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoLakehouseRESTDiscoveryPlugin,\
              org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
```

The discovery plugin validates this at initialization and fails fast otherwise. Initializing first
is what lets it treat every `spark.sql.catalog.*` entry it sees as user-written, with no markers and
no state shared between the plugins.

`GravitinoSparkPlugin` needs no new rule: `registerCatalog` already asserts
`!sparkConf.contains("spark.sql.catalog." + name)` before writing, and the caller catches the
failure per catalog, so an already-registered name is skipped while the rest still register. Only
the reporting changes: today the skip is logged as `Register catalog X failed` with a stack trace,
which would now fire once per REST-registered catalog and read as an error.

### User process

1. Deploy the Gravitino Iceberg REST server (with either `dynamic-config-provider` or
   `static-config-provider` — the listing endpoint reflects both).
2. Add to Spark configuration:

   ```text
   spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoLakehouseRESTDiscoveryPlugin
   spark.sql.gravitino.icebergREST.uri=http://127.0.0.1:9001/iceberg/
   ```

   To keep `GravitinoSparkPlugin` for other catalogs, list it **after** the discovery plugin.
3. Start the session — the plugin lists catalogs and registers one Spark catalog per selected name.
4. Query: `SELECT * FROM iceberg_prod.db.table`. Catalogs added later appear after a session
   restart, with no Spark configuration change.

### Implementation process

```text
Spark Driver startup (spark.plugins: GravitinoLakehouseRESTDiscoveryPlugin, GravitinoSparkPlugin)
  ├─ GravitinoLakehouseRESTDiscoveryPlugin.init()
  │    ├─ validate plugin ordering (fail fast if listed after GravitinoSparkPlugin)
  │    ├─ snapshot SparkConf  (only user-written entries exist at this point)
  │    └─ for each provider whose <format>REST.uri is set (Iceberg, Lance, …):
  │         ├─ fail fast if the provider is not on the classpath
  │         ├─ list catalogs ──► its Gravitino REST server → backend
  │         ├─ drop names claimed by user conf → policy.shouldRegister/registeredCatalogName(format,name)
  │         └─ apply the precedence rules above, then write
  │            spark.sql.catalog.<sparkName>.* and inject the provider's extensions
  │
  ├─ GravitinoSparkPlugin.init()   (if enabled)
  │    └─ existing behavior: already skips any <name> whose spark.sql.catalog.<name> is set
  │
  └─ Table access: Spark → REST protocol → Gravitino REST server → backend
```

Governance is unchanged: table access still flows through the REST server, which remains the
enforcement point for authorization and audit.

---

## Lance support

A design sketch, not part of V1. Lance plugs into the same plugin as a second provider, reusing the
registration policy, the ordering rule, and the user-configuration precedence, and differing only in
discovery and the generated entries.

**Discovery needs no new API.** The Lance Namespace protocol's root list,
`GET {lance-rest-base}/v1/namespace/list`, already returns the `lakehouse-generic` catalogs from
Gravitino, and the Lance REST server rejects anything else, so the advertised names are by
construction valid `parent` values. The list is paginated, so the plugin follows `page_token`.
Filtering to "only Lance catalogs" is neither possible nor needed: `format` is a table property, not
a catalog-level marker, and the server serves exactly this set.

The plugin could instead ask the Gravitino server for the generic catalog list directly. The
deciding difference is the user's mental model. With the REST server, discovery source and data
source are the same endpoint — the one `lanceREST.uri` Spark already queries for tables — and the
model is identical to Iceberg: point the plugin at a REST server, get its catalogs. Going through
Gravitino splits that into two systems (Gravitino lists them, the REST server serves them) plus the
unstated assumption that the names line up, and makes Lance discovery behave differently from
Iceberg. That mismatch stays invisible until a name does not line up, which fails at query time
rather than at configuration time. Discovery therefore stays server-authoritative; Gravitino's
per-principal authorization, the one thing this path would have reused, is instead added on the
Lance server (see below).

**The Lance provider.** It uses the `spark.sql.gravitino.lanceREST.*` prefix, mirroring the Iceberg
keys (`uri`, `catalogProperties.<key>`), and lists catalogs with
`org.lance.namespace.client.apache.api.NamespaceApi`, the plain-Java client in the lance-spark
bundle, for the same reason the Iceberg provider uses Iceberg's `RESTClient`. Setting
`lanceREST.uri` activates it; it and `icebergREST.uri` can be set together to register both formats
from the one plugin.

For each accepted catalog it generates:

```text
spark.sql.catalog.<sparkName>=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.<sparkName>.impl=rest
spark.sql.catalog.<sparkName>.uri=<spark.sql.gravitino.lanceREST.uri>
spark.sql.catalog.<sparkName>.parent=<advertisedName>
```

`parent` is set to the catalog name (`<advertisedName>`) and selects the target catalog — the same
role `warehouse` plays in the Iceberg provider. The four core keys are the implementation key,
`impl`, `uri`, and `parent`. Like the Iceberg provider, it injects the format's session extension
(`org.lance.spark.extensions.LanceSparkSessionExtensions`) into `spark.sql.extensions` with the same
deduplication; the extension class ships in the lance-spark bundle the active provider already
requires, so a lance-spark version that provides it is a prerequisite.

One difference from the Iceberg provider: storage configuration is static pass-through. Lance
delivers it per table in `DescribeTableResponse.storageOptions`, resolved from the catalog's and
table's `lance.storage.*` properties. The plugin generates nothing either way; unlike Iceberg
vending, secrets in catalog properties reach the client, with no vending equivalent today.

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
   storage configuration in its native form plus a per-catalog credential-vending flag; a
   per-catalog field can be added to the listing response (compatibly) to carry that.
2. **Multiple REST servers per format**: one `uri` per format per session (one `icebergREST.uri`,
   one `lanceREST.uri`); registering several servers of the same format is out of scope.
