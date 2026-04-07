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

# Design: AWS Glue Data Catalog Support for Apache Gravitino

## 1. Problem Statement and Goals

### 1.1 Problem

**Gravitino currently cannot federate AWS Glue Data Catalog.** This is a significant gap because:

1. **Large user base on AWS**: The majority of cloud-native data lakes run on AWS with Glue Data Catalog as the central metadata service (default for Athena, Redshift Spectrum, EMR, Lake Formation). These organizations cannot bring their Glue metadata into Gravitino's unified management layer.
2. **No native integration path**: The only workaround is pointing Gravitino's Hive catalog at Glue's HMS-compatible Thrift endpoint (`metastore.uris = thrift://...`), which is undocumented, region-limited, and cannot leverage Glue-native features (catalog ID, cross-account access, VPC endpoints).
3. **Competitive landscape**: Dremio, Trino, Spark, and Athena all support Glue with a single unified connection that presents all table types. Users expect the same from Gravitino.

### 1.2 Goals

After this feature is implemented:

1. **Register AWS Glue Data Catalog in Gravitino with a single catalog**:
   ```bash
   gcli catalog create --name my_glue --provider glue \
     --properties aws-region=us-east-1,aws-glue-catalog-id=123456789012
   ```

2. **All table types in the Glue catalog are visible through a single Gravitino catalog**:
   ```bash
   gcli schema list --catalog my_glue
   # Returns all Glue databases

   gcli table list --catalog my_glue --schema my_database
   # Returns Hive tables, Iceberg tables, Delta tables, Parquet tables — everything
   ```

3. **AWS-native authentication**: static credentials, or default credential chain (environment variables, instance profile, container credentials).

4. **Metadata preservation**: Glue table parameters (`table_type`, `metadata_location`, `spark.sql.sources.provider`, etc.) pass through Gravitino's API layer intact, so downstream tools can correctly identify table formats.

---

## 2. Background

### 2.1 AWS Glue Data Catalog

AWS Glue Data Catalog is a managed metadata repository storing:

- **Databases** — logical groupings, equivalent to Gravitino schemas.
- **Tables** — metadata records containing column definitions, storage descriptors, partition keys, and user-defined parameters.
- **Views** — virtual tables defined by SQL. Glue stores them like tables with a special `TableType=VIRTUAL_VIEW` field.

Tables in a single Glue catalog are heterogeneous — they coexist in the same database regardless of format:

| Format | How Glue Stores It |
|---|---|
| **Hive** | Full metadata in `StorageDescriptor` (columns, SerDe, InputFormat, OutputFormat, location). The majority of tables in most Glue catalogs. |
| **Iceberg** | `Parameters["table_type"] = "ICEBERG"` and `Parameters["metadata_location"]` pointing to Iceberg metadata JSON. `StorageDescriptor.Columns` is typically empty. |
| **Delta Lake** | `Parameters["spark.sql.sources.provider"] = "delta"` and `Parameters["location"]`. |
| **Parquet/CSV/ORC** | `StorageDescriptor` with appropriate `InputFormat` / `OutputFormat`. |

A complete Glue integration must list all of these table types without filtering.

### 2.2 How Query Engines Use Glue

Both Trino and Spark have native Glue support — they call the AWS Glue SDK directly, not via HMS Thrift:

| Engine | Hive Tables on Glue | Iceberg Tables on Glue |
|---|---|---|
| **Trino** | Hive connector with `hive.metastore=glue` | Iceberg connector with `iceberg.catalog.type=glue` |
| **Spark** | Hive catalog with `AWSGlueDataCatalogHiveClientFactory` | Iceberg catalog with `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog` |

### 2.3 Gravitino's Catalog Plugin Architecture

Gravitino loads catalogs as plugins at runtime. Each catalog is a self-contained Gradle module that provides:

- A `BaseCatalog<T>` subclass registered via `META-INF/services/org.apache.gravitino.CatalogProvider`.
- A `CatalogOperations` implementation that handles the actual metadata calls.
- Property metadata classes describing the catalog's configuration schema.

The `BaseCatalog.newOps(config)` factory method creates the `CatalogOperations` instance. Once initialized, upstream code interacts only with the `SupportsSchemas` and `TableCatalog` interfaces — the underlying implementation is completely opaque to the caller.

---

## 3. Design Alternatives

### Alternative A: New `catalog-glue` Module (This Document)

Create a standalone `catalogs/catalog-glue/` that calls the AWS Glue SDK directly. A single Gravitino catalog backed by `provider=glue` exposes all table types from a Glue Data Catalog.

**Pros**:
- Single catalog per Glue Data Catalog — matches how Dremio, Athena, and other tools work.
- Full control over Glue-specific behavior (pagination, catalog ID, VPC endpoints).
- No filtering: all table types are visible; `table_type` and `metadata_location` pass through intact.
- Clean foundation for Phase 2 query engine integration.

**Cons**:
- More new code than Alternative B.
- Phase 2 query engine mixed-table support requires additional work (see Section 6).

### Alternative B: Glue as a Metastore Type (Rejected)

Extend existing Hive and Iceberg catalogs with `metastore-type=glue` / `catalog-backend=glue`. Users create two separate Gravitino catalogs to cover Hive and Iceberg tables from the same Glue Data Catalog.

**Why rejected**: Industry standard (Dremio, Athena, AWS console) is one connection = all table types. Requiring two catalogs confuses users and diverges from the expected experience.

---

## 4. Configuration Properties

Glue is a separate AWS service from S3. The Glue region and credentials may differ from S3 storage credentials, so Glue properties use their own `aws-*` namespace:

| Property | Required | Default | Description |
|---|---|---|---|
| `aws-region` | Yes | — | AWS region for the Glue Data Catalog |
| `aws-access-key-id` | No | Default credential chain | AWS access key for Glue API authentication. **Sensitive**: not visible to catalog readers via Gravitino API. |
| `aws-secret-access-key` | No | Default credential chain | AWS secret key for Glue API authentication. **Sensitive**: not visible to catalog readers via Gravitino API. |
| `aws-glue-catalog-id` | Yes | — | Glue catalog ID. Required because an AWS account can have multiple Glue catalogs (e.g., default catalog and federated S3 Tables catalog). |
| `aws-glue-endpoint` | No | AWS default regional endpoint | Custom Glue endpoint URL (for VPC endpoints or LocalStack testing). |
| `default-table-format` | No | `iceberg` | Default format for tables created via Gravitino's `createTable()` API. Accepted values: `iceberg`, `hive`. |
| `table-type-filter` | No | `all` | Comma-separated list of table types exposed by `listTables()` and `loadTable()`. Accepted values: `all`, `hive`, `iceberg`, `delta`, `parquet`. Use to restrict visible table types for backwards compatibility with existing systems that cannot handle mixed-format catalogs. |

**Authentication priority**: Static credentials (`aws-access-key-id` + `aws-secret-access-key`) → Default credential chain (environment variables, instance profile, container credentials). STS AssumeRole (`aws-role-arn`) is a future enhancement — static credentials are sufficient for the initial release, including cross-account access.

---

## 5. Server-Side Design: `catalog-glue` Module

### 5.1 Module Structure

```
catalogs/catalog-glue/
├── build.gradle.kts
└── src/
    └── main/
        ├── java/org/apache/gravitino/catalog/glue/
        │   ├── GlueCatalog.java                    # extends BaseCatalog<GlueCatalog>
        │   ├── GlueCatalogOperations.java           # CatalogOperations, SupportsSchemas, TableCatalog
        │   ├── GlueCatalogPropertiesMetadata.java
        │   ├── GlueSchemaPropertiesMetadata.java
        │   ├── GlueTablePropertiesMetadata.java
        │   ├── GlueCatalogCapability.java
        │   ├── GlueSchema.java                     # Gravitino Schema implementation
        │   ├── GlueTable.java                      # Gravitino Table implementation
        │   └── GlueClientProvider.java             # AWS SDK v2 GlueClient factory
        └── resources/
            └── META-INF/services/
                └── org.apache.gravitino.CatalogProvider  # = o.a.g.catalog.glue.GlueCatalog
```

### 5.2 AWS SDK Dependency

Use **AWS SDK v2** (`software.amazon.awssdk:glue`) — consistent with existing S3, STS, IAM, and KMS dependencies in `gradle/libs.versions.toml`, all pinned to `awssdk = "2.29.52"`.

Add to `gradle/libs.versions.toml`:
```toml
aws-glue = { group = "software.amazon.awssdk", name = "glue", version.ref = "awssdk" }
```

`build.gradle.kts` dependencies:
```kotlin
dependencies {
    implementation(libs.aws.glue)
    implementation(libs.aws.sts)   // For credential chain (already in version catalog)
    compileOnly(project(":api"))
    compileOnly(project(":core"))
    compileOnly(project(":common"))
}
```

### 5.3 GlueClientProvider

`GlueClientProvider` builds an authenticated `GlueClient` (AWS SDK v2) from catalog configuration:

```java
public class GlueClientProvider {

  public static GlueClient buildClient(Map<String, String> config) {
    GlueClientBuilder builder = GlueClient.builder()
        .region(Region.of(config.get("aws-region")));

    // Custom endpoint (VPC endpoint or LocalStack)
    String endpoint = config.get("aws-glue-endpoint");
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }

    // Static credentials (if provided), otherwise default credential chain
    String accessKey = config.get("aws-access-key-id");
    String secretKey = config.get("aws-secret-access-key");
    if (accessKey != null && secretKey != null) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKey, secretKey)));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }

    return builder.build();
  }
}
```

### 5.4 Schema Operations

`GlueCatalogOperations` implements `SupportsSchemas` by mapping to Glue Database API:

| Gravitino Operation | Glue API |
|---|---|
| `listSchemas(namespace)` | `GlueClient.getDatabases()` (paginated) |
| `createSchema(ident, comment, properties)` | `GlueClient.createDatabase(DatabaseInput)` |
| `loadSchema(ident)` | `GlueClient.getDatabase(name)` → `GlueSchema` |
| `alterSchema(ident, changes)` | `GlueClient.updateDatabase(name, DatabaseInput)` |
| `dropSchema(ident, cascade)` | If cascade: delete all tables first; then `GlueClient.deleteDatabase(name)` |
| `schemaExists(ident)` | `getDatabase()` + catch `EntityNotFoundException` |

**Glue Database → Gravitino Schema mapping**:

| Glue Field | Gravitino Field |
|---|---|
| `Database.name` | schema name |
| `Database.description` | schema comment |
| `Database.parameters` | schema properties |
| `Database.locationUri` | `"location"` property |

### 5.5 Table Operations

**Key design principle: present all table types by default, with opt-in filtering.**

Unlike `HiveCatalogOperations` (which filters out Iceberg/Paimon/Hudi tables), `GlueCatalogOperations` returns every table in a Glue database by default. The `table-type-filter` catalog property allows restricting the visible table types for backwards compatibility with existing systems or older query engine versions that cannot handle mixed-format catalogs.

#### Table Listing

```java
@Override
public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
  String databaseName = namespace.level(namespace.length() - 1);
  // Paginate through all tables — no type filter
  List<Table> glueTables = new ArrayList<>();
  String nextToken = null;
  do {
    GetTablesResponse response = glueClient.getTables(
        GetTablesRequest.builder()
            .databaseName(databaseName)
            .catalogId(catalogId)
            .nextToken(nextToken)
            .build());
    glueTables.addAll(response.tableList());
    nextToken = response.nextToken();
  } while (nextToken != null);

  return glueTables.stream()
      .map(t -> NameIdentifier.of(namespace, t.name()))
      .toArray(NameIdentifier[]::new);
}
```

#### Table Loading and Type Detection

When loading a table, detect the format from Glue table parameters and map accordingly:

```java
@Override
public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
  software.amazon.awssdk.services.glue.model.Table glueTable =
      getGlueTable(ident);  // calls GlueClient.getTable()

  String tableType = glueTable.parameters().getOrDefault("table_type", "").toUpperCase();

  switch (tableType) {
    case "ICEBERG":
      return buildIcebergProxyTable(glueTable);  // preserves metadata_location
    default:
      return buildHiveFormatTable(glueTable);    // maps StorageDescriptor → columns
  }
}
```

`buildHiveFormatTable()` maps `StorageDescriptor.columns()` to Gravitino `Column[]` and storage properties. `buildIcebergProxyTable()` returns a `GlueTable` with the full `parameters()` map in `properties()` — `table_type` and `metadata_location` survive intact.

#### Glue Table → Gravitino Table Mapping

| Glue Field | Gravitino Field | Notes |
|---|---|---|
| `Table.name` | table name | |
| `Table.description` | table comment | |
| `StorageDescriptor.columns` | `Column[]` | For Hive-format tables |
| `Table.partitionKeys` | partition columns | |
| `StorageDescriptor.location` | `"location"` property | |
| `StorageDescriptor.serdeInfo.serializationLibrary` | `"serde-lib"` property | |
| `StorageDescriptor.inputFormat` | `"input-format"` property | |
| `StorageDescriptor.outputFormat` | `"output-format"` property | |
| `Table.parameters` | `properties()` (merged) | Includes `table_type`, `metadata_location`, etc. — all pass through |
| `Table.tableType` | `"external-table"` / `"managed-table"` property | `EXTERNAL_TABLE` vs `MANAGED_TABLE` |

**Metadata passthrough guarantee**: Every key-value pair in `Table.parameters` is included in the Gravitino `Table.properties()` map unchanged. This ensures `table_type=ICEBERG`, `metadata_location=s3://...`, `spark.sql.sources.provider=delta`, and any other format indicators survive Gravitino's metadata proxy layer.

#### Table CRUD

| Gravitino Operation | Glue API | Notes |
|---|---|---|
| `createTable(ident, columns, comment, properties, ...)` | `GlueClient.createTable(TableInput)` | Format determined by `default-table-format` (default: `iceberg`) |
| `alterTable(ident, changes)` | `GlueClient.updateTable(TableInput)` | |
| `dropTable(ident)` | `GlueClient.deleteTable(name)` | |
| `purgeTable(ident)` | `GlueClient.deleteTable(name)` | Phase 1: same as drop (no data deletion from S3 — query engine responsibility) |
| `tableExists(ident)` | `getTable()` + catch `EntityNotFoundException` | |

**Default table format**: When `createTable()` is called without an explicit `table_type` property, `GlueCatalogOperations` uses the `default-table-format` catalog property to determine the format. The default is `iceberg` — `createTable()` builds an Iceberg-compatible `TableInput` (setting `Parameters["table_type"]="ICEBERG"` and `Parameters["metadata_location"]` to the initial metadata path). If `default-table-format=hive`, a standard Hive `StorageDescriptor`-based `TableInput` is produced instead. Users can also override per-table by setting `table_type` explicitly in the table properties passed to `createTable()`.

### 5.6 Views

Glue stores views as tables with `TableType=VIRTUAL_VIEW` and `ViewOriginalText` / `ViewExpandedText` fields. Gravitino's `ViewCatalog` interface is not yet fully implemented; how `catalog-glue` should expose Glue views will be determined once the Gravitino View API is in place.

In Phase 1, views are hidden — `listTables()` and `loadTable()` filter out entries where `TableType=VIRTUAL_VIEW`, making them invisible through the Gravitino API.

### 5.7 Architecture Overview

```
                      Gravitino Server
                            |
                    provider=glue
                            |
                  GlueCatalogOperations
                            |
                  GlueClientProvider
                            |
                    AWS SDK v2 GlueClient
                            |
               AWS Glue Data Catalog (us-east-1)
                            |
            +-------+-------+-------+-------+
            |       |       |       |       |
          Hive   Iceberg  Delta  Parquet  Views
         tables  tables  tables  tables
   (StorageDescriptor)  (table_type=ICEBERG)  (all parameters pass through)
```

**End-to-end data flow**:

```
# Registration
gcli catalog create --name my_glue --provider glue \
  --properties aws-region=us-east-1,aws-glue-catalog-id=123456789012
  → GravitinoCatalogManager.createCatalog()
    → CatalogPluginLoader loads catalog-glue plugin
      → GlueCatalog.newOps(config)
        → GlueCatalogOperations.initialize(config)
          → GlueClientProvider.buildClient(config) → GlueClient

# Metadata query
gcli table list --catalog my_glue --schema analytics
  → GlueCatalogOperations.listTables(Namespace.of("my_glue", "analytics"))
    → GlueClient.getTables(databaseName="analytics", catalogId="123456789012")
    → Returns ALL non-view tables: [orders (Hive), events (Iceberg), sessions (Parquet), ...]
    // VIRTUAL_VIEW entries are filtered out at this layer (see Section 5.6)

gcli table details --catalog my_glue --schema analytics --table events
  → GlueCatalogOperations.loadTable(NameIdentifier...)
    → GlueClient.getTable("analytics", "events")
    → Parameters["table_type"] = "ICEBERG"
    → Returns GlueTable with properties():
        {"table_type": "ICEBERG", "metadata_location": "s3://bucket/events/metadata/...", ...}
```

---

## 6. Query Engine Integration (Phase 2)

Phase 1 delivers full metadata API support — all table types are visible and queryable through Gravitino. Phase 2 extends query engine connectors so that engines can actually execute queries against mixed-format Glue databases through a single Gravitino catalog.

### 6.1 Trino

A Glue database typically contains Hive, Iceberg, Delta, and Parquet tables coexisting. Gravitino's Trino connector must route queries to the correct internal Trino connector implementation per table. Three approaches were evaluated.

#### Approach 1: Per-Table Dispatch Inside GravitinoConnector

Extend `GravitinoConnector` to hold two internal connector instances simultaneously (Hive and Iceberg). A new `GlueTableHandle` carries the table's format, and every connector method (`getMetadata`, `getSplitManager`, `getPageSourceProvider`, etc.) routes to the correct internal connector.

**Pros**: Works on all Trino versions (435+); single Gravitino catalog maps to a single Trino catalog.

**Cons**: High implementation cost (~9 new classes); `GlueMetadata` alone must override 20+ methods with identical routing boilerplate. Transaction lifecycle is complex — `beginTransaction` must open transactions on both connectors simultaneously. Session properties and table properties from Hive and Iceberg connectors may conflict. Every Gravitino Trino version subproject (435–478+) must carry its own copy. Essentially reimplements what Trino already provides natively in its Lakehouse connector.

#### Approach 2: Trino Lakehouse Connector (Recommended)

The Trino [Lakehouse connector](https://trino.io/docs/current/connector/lakehouse.html) (`connector.name=lakehouse`) natively handles all table formats (Hive, Iceberg, Delta Lake, Hudi) through a single connector instance, with AWS Glue as a supported metastore backend.

A new `GlueConnectorAdapter` maps the Gravitino `glue` catalog to a Trino Lakehouse catalog:

```
Gravitino catalog (provider=glue)
  → GlueConnectorAdapter.buildInternalConnectorConfig()
    → { "connector.name": "lakehouse",
        "hive.metastore": "glue",
        "hive.metastore.glue.region": "us-east-1",
        "hive.metastore.glue.catalogid": "123456789012",
        "hive.metastore.glue.aws-access-key": "<aws-access-key-id>",
        "hive.metastore.glue.aws-secret-key": "<aws-secret-access-key>" }
        // aws-access-key-id / aws-secret-access-key omitted when using default credential chain
        // aws-glue-endpoint maps to hive.metastore.glue.endpoint-url (VPC / LocalStack)
  → Trino Lakehouse connector handles all table formats natively
```

**Pros**: Minimal Gravitino code (one new `GlueConnectorAdapter`, ~30 lines); no routing logic, no transaction coordination, no property conflicts. Single Gravitino catalog = single Trino catalog. Future table format additions are automatically supported as Trino extends the Lakehouse connector.

**Cons**: Requires **Trino ≥ 477**. This feature is only supported in Trino version 477 and above.

#### Approach 3: Hive Connector Table Redirection

Trino's Hive connector supports [table redirection](https://trino.io/docs/current/connector/hive.html#table-redirection): when it encounters a table with `table_type=ICEBERG`, it transparently redirects the query to a pre-configured Iceberg catalog. For a single Gravitino catalog, Gravitino's connector would need to automatically register a hidden Iceberg companion catalog in Trino's internal catalog manager (pointing to the same Glue) and set `hive.iceberg-catalog-name` to reference it.

**Pros**: Works on Trino versions below 477; single Gravitino catalog from the user's perspective.

**Cons**: Gravitino must manage the hidden companion catalog's lifecycle (create/update/delete must stay in sync). View redirection is not supported. Only covers Hive→Iceberg; Delta and Hudi require additional companion catalogs.

#### Decision

**Use Approach 2 (Lakehouse connector)** for the following reasons:

- Trino's Lakehouse connector already solves exactly this problem natively. Approach 1 would duplicate that work inside Gravitino with significantly higher implementation cost and ongoing maintenance burden.
- Approach 3 requires Gravitino to manage the lifecycle of hidden internal catalogs, adding complexity that outweighs the benefit of supporting older Trino versions.
- A single `GlueConnectorAdapter` (~30 lines) is all that is needed in Gravitino — all mixed-format routing complexity is delegated to Trino.

**For users on Trino < 477**: Trino's Hive connector can only execute queries against Hive-format tables. Set `table-type-filter=hive` on the Gravitino `catalog-glue` catalog so that only Hive-format tables are exposed — Trino then operates correctly without encountering Iceberg or Delta tables it cannot handle. Mixed-format query support requires upgrading to Trino ≥ 477 and removing the filter.

---

### 6.2 Spark

In Gravitino's Spark connector, Hive-format tables and Iceberg tables require fundamentally different processing paths: Hive tables are handled through Kyuubi's `HiveTableCatalog`, while Iceberg tables must go through the Iceberg native `SparkCatalog`. When a single Glue database contains both formats, the connector needs a routing mechanism to direct each table to the correct handler. Two approaches were evaluated.

#### Approach 1: SparkSessionCatalog (Iceberg's implementation of Spark's CatalogExtension)

Spark provides a `CatalogExtension` interface that allows a catalog to wrap the global `spark_catalog`. Iceberg implements this as `org.apache.iceberg.spark.SparkSessionCatalog`: when a table carries `table_type=ICEBERG`, it intercepts the load and delegates to the Iceberg native reader; Hive-format tables fall back to the wrapped `spark_catalog` for standard Hive table handling.

**Pros**: Standard Iceberg solution; no changes to Gravitino internals; mixed-format reads work correctly in pure Spark setups.

**Cons**: Two fundamental blockers. First, the Hive-format fallback path is wrong: `SparkSessionCatalog` falls back to the wrapped `spark_catalog` (Spark's built-in single-HMS session catalog), not to Kyuubi's `HiveTableCatalog`. Gravitino relies on Kyuubi precisely because it supports multiple independent HMS instances per named catalog — routing Hive tables through `spark_catalog` breaks this multi-catalog capability entirely. Second, `SparkSessionCatalog` is designed to replace the global `spark_catalog` and is incompatible with Gravitino's named catalog architecture — substituting it causes `SparkHiveTable` cast failures and breaks all Hive DDL.

#### Approach 2: Per-Table Dispatch Inside GravitinoHiveCatalog (Recommended)

`GravitinoHiveCatalog.createSparkTable()` is an abstract template method and the sole table-object construction exit — all table loads pass through it. When this method detects `table_type=ICEBERG` in `gravitinoTable.properties()`, it bypasses Kyuubi and delegates to an internally maintained Iceberg native `SparkCatalog` (initialized with the Iceberg `GlueCatalog` backend using the same AWS credentials and catalog ID). Hive-format tables continue through the existing Kyuubi path unchanged:

```
GravitinoHiveCatalog.createSparkTable(gravitinoTable, sparkTable)
  │
  ├── table_type = "ICEBERG"
  │     └── icebergBackingCatalog.loadTable(identifier)   ← Iceberg native SparkCatalog
  │
  └── everything else
        └── existing Kyuubi SparkHiveTable path (unchanged)
```

When initializing the backing `SparkCatalog`, `GravitinoHiveCatalog` maps the Gravitino catalog properties as follows:

| Gravitino Property | Iceberg SparkCatalog Property | Notes |
|---|---|---|
| _(fixed)_ | `catalog-impl` = `org.apache.iceberg.aws.glue.GlueCatalog` | Selects Iceberg's Glue backend |
| `aws-region` | `client.region` | AWS region for the Glue API client |
| `aws-glue-catalog-id` | `glue.id` | 12-digit AWS account / catalog ID |
| `aws-glue-endpoint` | `glue.endpoint` | Omitted if not set; used for VPC endpoints or LocalStack |
| `aws-access-key-id` + `aws-secret-access-key` | `client.credentials-provider` → `StaticCredentialsProvider` | Omitted when using default credential chain |

**Pros**: No changes to Kyuubi or any external component; the backing Iceberg catalog is an internal field of `GravitinoHiveCatalog`, never registered with Spark SQL's catalog manager — no naming conflicts. No new dependencies; `iceberg-spark-runtime` is already present as `compileOnly`/`testImplementation`. The backing catalog is initialized lazily (double-checked locking) only on first Iceberg table encounter.

**Cons**: `BaseCatalog.loadTable()` calls Kyuubi's `loadSparkTable()` before `createSparkTable()`, producing one redundant HMS round-trip for Iceberg tables. No functional impact, minor overhead only.

#### Decision

**Use Approach 2 (per-table dispatch inside `GravitinoHiveCatalog`).** The intercept point is a well-defined, single-exit template method, keeping the change minimal and isolated entirely within the Gravitino connector. Approach 1 (`SparkSessionCatalog`) is the natural Iceberg solution but is blocked by Kyuubi's architecture — it cannot be used without abandoning the Kyuubi Hive path that Gravitino depends on.

---

### 6.3 Flink

In Gravitino's Flink connector, Hive-format and Iceberg tables require different processing paths. The chosen approach mirrors the Spark design: `GravitinoGlueCatalog` (a new subclass of `GravitinoHiveCatalog`) holds an internal lazily-initialized Iceberg `FlinkCatalog` instance (backed by Iceberg's `GlueCatalog`, using the same AWS credentials and catalog ID). When `getTable()` detects `table_type=ICEBERG` in the Gravitino table properties, it delegates to this internal catalog to produce a properly typed `CatalogTable` with full schema and Iceberg execution semantics. Hive-format tables continue through the existing Kyuubi-backed path unchanged:

```
GravitinoGlueCatalog.getTable(objectPath)
  │
  ├── table_type = "ICEBERG"
  │     └── icebergFlinkCatalog.getTable(objectPath)   ← Iceberg native FlinkCatalog
  │
  └── everything else
        └── super.getTable() → existing HiveDynamicTableFactory path (unchanged)
```

When initializing the backing `FlinkCatalog`, `GravitinoGlueCatalog` maps the Gravitino catalog properties as follows:

| Gravitino Property | Iceberg FlinkCatalog Property | Notes |
|---|---|---|
| _(fixed)_ | `catalog-impl` = `org.apache.iceberg.aws.glue.GlueCatalog` | Selects Iceberg's Glue backend |
| `aws-region` | `client.region` | AWS region for the Glue API client |
| `aws-glue-catalog-id` | `glue.id` | 12-digit AWS account / catalog ID |
| `aws-glue-endpoint` | `glue.endpoint` | Omitted if not set; used for VPC endpoints or LocalStack |
| `aws-access-key-id` + `aws-secret-access-key` | `client.credentials-provider` → `StaticCredentialsProvider` | Omitted when using default credential chain |

**Pros**: Clean encapsulation — credentials are held within the internal catalog instance, never appearing in table options or `SHOW CREATE TABLE` output; schema is resolved natively by the Iceberg catalog from the metadata file; consistent with the Spark approach. The backing catalog is initialized lazily (double-checked locking) only on first Iceberg table encounter.

**Cons**: Requires `iceberg-flink-runtime` as a `compileOnly` dependency (same pattern as Spark's `iceberg-spark-runtime`).

**For `catalog-hive` with mixed-format HMS** (Phase 3): the same `GravitinoHiveCatalog.getTable()` override applies with an HMS-backed Iceberg `FlinkCatalog` — no additional design required once the Glue implementation is in place.

---

## 7. Security

### 7.1 Two-Layer Security Model

`catalog-glue` operates under two independent security layers that address different concerns:

| Layer | Managed By | Controls |
|---|---|---|
| Gravitino RBAC | Gravitino server (Jcasbin) | Who can perform metadata operations through the Gravitino API |
| AWS IAM | AWS | Which Glue API calls and S3 data paths the service credential can access |

The two layers are orthogonal: Gravitino RBAC governs metadata-level access (can this user create a schema?), while IAM governs the data-level and API-level access of the server process itself. Both must be configured correctly for end-to-end security.

### 7.2 Gravitino RBAC

`catalog-glue` participates in Gravitino's standard RBAC model without requiring any special capability declaration. The full set of standard privileges applies:

| Gravitino Privilege | Allowed Operations |
|---|---|
| `USE_CATALOG` | List schemas in the Glue catalog |
| `CREATE_SCHEMA` | Create a Glue database |
| `USE_SCHEMA` | List and load tables in a schema |
| `CREATE_TABLE` | Create a Glue table |
| `MODIFY_TABLE` | Alter or drop a Glue table |
| `SELECT_TABLE` | Load table metadata (columns, properties) |

Roles, users, and groups are managed through Gravitino's standard access control API (`/api/metalakes/{metalake}/roles`, etc.) and enforced by the Jcasbin authorizer on every API request before it reaches `GlueCatalogOperations`.

**Authorization plugin**: Phase 1 does not integrate an external authorization backend (Ranger or Lake Formation). AWS Lake Formation integration, which provides column-level and row-level Glue table permissions, is a future Phase 2 consideration.

### 7.3 AWS IAM Minimum Permissions

**Key point**: the `aws-access-key-id` configured in `catalog-glue` serves a dual role — Gravitino uses it to call the Glue API for metadata operations, and the same credential is passed through to Trino and Spark (via the property mappings in Sections 6.1 and 6.2) for both Glue metadata access and S3 data read/write. A single IAM policy must therefore cover both planes.

**Note**: Static credentials have security limitations (transfer on wire, shared privileges between Gravitino and query engines). Future enhancement: credential vending (https://github.com/apache/gravitino/issues/10415) will provide dynamic, scoped credentials for better security isolation.

This policy should be attached to the IAM user or role that owns the `aws-access-key-id`. When using the default credential chain (instance profile, container credentials, etc.), attach it to the corresponding IAM role instead.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueMetadataAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetCatalog",
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateDatabase", "glue:UpdateDatabase", "glue:DeleteDatabase",
        "glue:GetTable", "glue:GetTables",
        "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account-id>:catalog",
        "arn:aws:glue:<region>:<account-id>:database/*",
        "arn:aws:glue:<region>:<account-id>:table/*/*"
      ]
    },
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<warehouse-bucket>",
        "arn:aws:s3:::<warehouse-bucket>/*"
      ]
    }
  ]
}
```

> Phase 2: add `glue:GetPartitions`, `glue:BatchCreatePartition`, `glue:DeletePartitions` when partition support is implemented.

### 7.4 Cross-Account Access

When the Glue catalog belongs to a different AWS account, the `aws-glue-catalog-id` property must be set to that account's 12-digit ID. The credential must be granted cross-account Glue access via a resource-based policy on the target account, or via STS AssumeRole (Phase 2: `aws-role-arn` property).

---

## 8. Testing Strategy

### 8.1 Unit Tests

| Test Class | Location | Coverage |
|---|---|---|
| `TestGlueCatalogPropertiesMetadata` | `catalog-glue/src/test/` | Required/optional properties, validation |
| `TestGlueClientProvider` | same | Credential chain selection, endpoint override |
| `TestGlueCatalogOperations` | same | Mock `GlueClient`; verify Schema and Table CRUD calls |
| `TestGlueTableTypeDetection` | same | Hive, Iceberg, Delta, Parquet, view table type mapping |
| `TestGlueTableMapping` | same | `StorageDescriptor` → `Column[]` conversion, property passthrough |

### 8.2 Integration Tests (LocalStack)

LocalStack provides a local AWS Glue API endpoint, enabling integration tests without a real AWS account.

```java
@Tag("gravitino-docker-test")
public class CatalogGlueIT extends AbstractCatalogIT {

  @Override
  protected void startNecessaryContainer() {
    containerSuite.startLocalStackContainer();
  }

  @Override
  protected Map<String, String> createCatalogProperties() {
    return ImmutableMap.of(
        "aws-region",            "us-east-1",
        "aws-glue-catalog-id",   "000000000000",   // LocalStack default account ID
        "aws-glue-endpoint",     localStackEndpoint,
        "aws-access-key-id",     "test",
        "aws-secret-access-key", "test");
  }
}
```

Test coverage:
- Schema CRUD: `createDatabase`, `getDatabase`, `listDatabases`, `updateDatabase`, `deleteDatabase`
- Table CRUD: `createTable` (default Iceberg format), `getTable`, `listTables`, `updateTable`, `deleteTable`
- Mixed type listing: Create Hive and Iceberg tables in same database; verify both appear in `listTables()`
- Metadata passthrough: Verify `table_type` and `metadata_location` survive `loadTable()`
- Property validation: Missing `aws-region` or `aws-glue-catalog-id` → informative error

### 7.3 Build Verification

```bash
# Compile
./gradlew :catalogs:catalog-glue:build -x test

# Unit tests
./gradlew :catalogs:catalog-glue:test -PskipITs

# Integration tests (Docker + LocalStack)
./gradlew :catalogs:catalog-glue:test -PskipTests -PskipDockerTests=false \
  --tests "*.CatalogGlueIT"
```

---

## 9. Implementation Plan

### Phase 1: Core `catalog-glue` Module

| # | Work Item | Files | Description |
|---|---|---|---|
| 1.1 | Version catalog + settings | `gradle/libs.versions.toml`, `settings.gradle.kts` | Add `aws-glue` dependency; add `include("catalogs:catalog-glue")` |
| 1.2 | `GlueClientProvider` | `GlueClientProvider.java` | Build `GlueClient` from config: region, credentials, endpoint |
| 1.3 | Property metadata | `GlueCatalogPropertiesMetadata.java`, `GlueSchemaPropertiesMetadata.java`, `GlueTablePropertiesMetadata.java` | Required: `aws-region`, `aws-glue-catalog-id`. Optional: credentials, endpoint |
| 1.4 | `GlueSchema` | `GlueSchema.java` | Gravitino Schema backed by Glue Database |
| 1.5 | `GlueTable` | `GlueTable.java` | Gravitino Table backed by Glue Table; full `parameters()` passthrough |
| 1.6 | Schema CRUD | `GlueCatalogOperations.java` (schema methods) | `listSchemas`, `createSchema`, `loadSchema`, `alterSchema`, `dropSchema` |
| 1.7 | Table CRUD | `GlueCatalogOperations.java` (table methods) | `listTables`, `loadTable`, `createTable`, `alterTable`, `dropTable` — all types, no filtering |
| 1.8 | Partition support | `GlueCatalogOperations.java` | `SupportsPartitions` for Hive-format partitioned tables |
| 1.9 | `GlueCatalog` + wiring | `GlueCatalog.java`, `GlueCatalogCapability.java`, `build.gradle.kts`, `META-INF/services/` | Plugin registration, `shortName()="glue"` |
| 1.10 | Unit tests | `TestGlue*.java` | Mock-based tests for all components |
| 1.11 | Integration tests | `CatalogGlueIT.java` | LocalStack-backed end-to-end tests |

### Phase 2: Query Engine Integration

| # | Work Item | Files | Description |
|---|---|---|---|
| 2.1 | Trino Lakehouse adapter | `GlueConnectorAdapter.java` in `trino-connector/` | Maps `provider=glue` → `connector.name=lakehouse` + `hive.metastore=glue`; requires Trino ≥ 477 |
| 2.2 | Spark mixed-type routing | `GravitinoHiveCatalog.createSparkTable()` | Intercept on `table_type=ICEBERG`; delegate to lazily initialized Iceberg `SparkCatalog` |
| 2.3 | Flink mixed-type routing | `GravitinoGlueCatalog.java` in `flink-connector/` | New subclass of `GravitinoHiveCatalog`; `getTable()` delegates to lazily initialized Iceberg `FlinkCatalog` (GlueCatalog backend) for Iceberg tables |
| 2.6 | View CRUD | `GlueCatalogOperations.java` | Full `ViewCatalog` interface implementation (pending Gravitino View API) |
| 2.7 | STS AssumeRole | `GlueClientProvider.java` | `aws-role-arn` property for cross-account access via STS |

### Phase 3: Extending Mixed-Format Support to Hive Catalog

The mixed-format routing built for `catalog-glue` in Phase 2 is not Glue-specific. The same problem exists for `catalog-hive` when the underlying Hive Metastore contains both Hive-format and Iceberg tables in the same database — a common pattern when Iceberg tables are created directly by Spark or Trino on top of an existing HMS.

Once the routing mechanism is proven in `catalog-glue`, the same approach can be applied to `catalog-hive`:

| Component | Change |
|---|---|
| Trino connector | `HiveConnectorAdapter` maps `provider=hive` → Trino Lakehouse connector when `list-all-tables=true`, enabling mixed-format query execution without a separate Iceberg catalog |
| Spark connector | `GravitinoHiveCatalog.createSparkTable()` intercept (already planned for Phase 2) applies equally to `catalog-hive` — no additional changes required once the Glue implementation is in place |
| Server-side | `HiveCatalogOperations` can adopt `table-type-filter` semantics to expose Iceberg/Delta tables alongside Hive tables when `list-all-tables=true` |

This phase depends on Phase 2 completion and can reuse its implementation directly, making the incremental cost low.
