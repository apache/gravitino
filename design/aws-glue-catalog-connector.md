# Design: AWS Glue Data Catalog Support for Apache Gravitino

## 1. Problem Statement and Goals

### 1.1 Problem

**Gravitino currently cannot federate AWS Glue Data Catalog.** This is a significant gap because:

1. **Large user base on AWS**: The majority of cloud-native data lakes run on AWS with Glue Data Catalog as the central metadata service (default for Athena, Redshift Spectrum, EMR, Lake Formation). These organizations cannot bring their Glue metadata into Gravitino's unified management layer.
2. **No native integration path**: The only workaround is pointing Gravitino's Hive catalog at Glue's HMS-compatible Thrift endpoint (`metastore.uris = thrift://...`), which is undocumented, region-limited, and cannot leverage Glue-native features (catalog ID, cross-account access, VPC endpoints).
3. **Competitive landscape**: Trino, Spark, and other engines all have first-class Glue support with dedicated configuration. Users expect the same from Gravitino.

### 1.2 Goals

After this feature is implemented:

1. **Register AWS Glue Data Catalog in Gravitino**:
   ```bash
   # Hive-format tables
   gcli catalog create --name hive_on_glue --provider hive \
     --properties metastore-type=glue,s3-region=us-east-1

   # Iceberg-format tables
   gcli catalog create --name iceberg_on_glue --provider lakehouse-iceberg \
     --properties catalog-backend=glue,warehouse=s3://bucket/iceberg,s3-region=us-east-1
   ```

2. **Standard Gravitino API works against Glue catalogs**:
   ```bash
   gcli schema list --catalog hive_on_glue
   gcli table list --catalog hive_on_glue --schema my_database
   gcli table details --catalog iceberg_on_glue --schema analytics --table events
   ```

3. **Trino and Spark connect transparently** — Trino uses `hive.metastore=glue` / `iceberg.catalog.type=glue`; Spark uses `AWSGlueDataCatalogHiveClientFactory` / `GlueCatalog`. Users query Glue tables through Gravitino without knowing the underlying mechanism.

4. **AWS-native authentication** (reuses existing S3 properties): static credentials, STS AssumeRole, or default credential chain (environment variables, instance profile).

## 2. Background

### 2.1 AWS Glue Data Catalog

AWS Glue Data Catalog is a managed metadata repository storing:
- **Databases** — logical groupings, equivalent to Gravitino schemas.
- **Tables** — metadata records containing column definitions, storage descriptors, partition keys, and user-defined parameters.

Tables come in two formats:

| Format | How Glue Stores It |
|---|---|
| **Hive** | Full metadata in `StorageDescriptor` (columns, SerDe, InputFormat, OutputFormat, location). The majority of tables in most Glue catalogs (legacy ETL, Athena CTAS, Redshift Spectrum). |
| **Iceberg** | `Parameters["table_type"] = "ICEBERG"` and `Parameters["metadata_location"]` pointing to Iceberg metadata JSON on S3. `StorageDescriptor.Columns` is typically empty. Growing rapidly. |

A complete Glue integration must handle both table formats.

### 2.2 How Query Engines Use Glue

Trino and Spark both have native Glue support — they call the AWS Glue SDK directly, not via HMS Thrift:

| Engine | Hive Tables on Glue | Iceberg Tables on Glue |
|---|---|---|
| **Trino** | Hive connector with `hive.metastore=glue` | Iceberg connector with `iceberg.catalog.type=glue` |
| **Spark** | Hive catalog with `AWSGlueDataCatalogHiveClientFactory` | Iceberg catalog with `catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog` |

Both engines use a **one-catalog-to-one-connector** model — a single catalog handles either Hive-format or Iceberg-format tables, not both. This is consistent with Gravitino's existing catalog model.

### 2.3 Gravitino's Current Architecture

Gravitino's catalog plugin system provides:
- **Hive catalog** (`provider=hive`): Connects to HMS via Thrift. Client chain: `HiveCatalogOperations` → `CachedClientPool` → `HiveClientImpl` → `HiveShimV2/V3` → `IMetaStoreClient`.
- **Iceberg catalog** (`provider=lakehouse-iceberg`): Supports pluggable backends (`catalog-backend=hive|jdbc|rest|memory|custom`). Each backend maps to a different Iceberg `Catalog` implementation.
- **Trino/Spark connectors**: Property converters translate Gravitino catalog properties into engine-specific properties.

## 3. Design Alternatives

### Alternative A: New `catalog-glue` Module

Create a standalone `catalogs/catalog-glue/` with its own `GlueCatalogOperations`, type converters, and entity classes. Directly call the AWS Glue SDK for both Hive and Iceberg tables.

**Pros**: Full control over Glue-specific behavior. Single catalog for mixed table formats.
**Cons**:
- Duplicates logic already in Hive catalog (type conversion, partition handling, SerDe parsing) and Iceberg catalog (schema conversion, metadata loading).
- Trino/Spark integration requires a "Composite Connector" that routes queries based on table type — a significant architectural change.
- Larger implementation surface area and maintenance burden.

### Alternative B: Glue as a Metastore Type (Chosen)

Extend the existing Hive and Iceberg catalogs with Glue as a backend option.

**Pros**:
- Reuses all existing catalog logic, type conversion, property handling, and entity models.
- Trino/Spark integration works almost for free — both engines already have native Glue support.
- Much smaller change set (~15 files modified, 1 new file vs. ~15 new files).
- Consistent with how Trino and Spark model Glue (as a metastore variant, not a separate catalog type).

**Cons**:
- Users must create two Gravitino catalogs to cover both Hive and Iceberg tables from the same Glue Data Catalog.
- Cannot add Glue-only features (e.g., Glue crawlers) without extending the generic interfaces.

**Decision**: Alternative B — the reuse benefits and Trino/Spark alignment outweigh the minor UX cost of two catalogs.

## 4. Detailed Design

### 4.1 Configuration Properties

Gravitino already defines standardized AWS/S3 properties in `S3Properties.java`:

| Existing Property | Used By |
|---|---|
| `s3-access-key-id` / `s3-secret-access-key` | Iceberg, Hive (S3 storage + Glue auth) |
| `s3-region` | Iceberg, Hive (S3 storage + Glue region) |
| `s3-role-arn` / `s3-external-id` | Iceberg, Hive (STS AssumeRole) |
| `s3-endpoint` | Iceberg, Hive (custom S3 endpoint) |

We **reuse `s3-region` as the AWS region** (Glue and S3 are always co-located) and **reuse `s3-access-key-id` / `s3-secret-access-key` for authentication**. Only two new Glue-specific properties:

| New Property | Required | Description |
|---|---|---|
| `aws-glue-catalog-id` | No | Glue catalog ID (defaults to caller's AWS account). For cross-account access. |
| `aws-glue-endpoint` | No | Custom Glue endpoint (for VPC endpoints or testing). |

**Authentication priority**: Static credentials → STS AssumeRole (`s3-role-arn`) → Default credential chain (environment variables, instance profile).

### 4.2 Iceberg Catalog + Glue Backend

Add `GLUE` as a new `IcebergCatalogBackend` enum value. Use Iceberg's built-in `org.apache.iceberg.aws.glue.GlueCatalog`.

#### Data Flow

```
User: catalog-backend=glue, warehouse=s3://..., s3-region=us-east-1
  → IcebergCatalogOperations.initialize()
    → IcebergCatalogUtil.loadCatalogBackend(GLUE, config)
      → loadGlueCatalog(config)
        → new GlueCatalog().initialize("glue", {
            "warehouse": "s3://...",
            "client.region": "us-east-1",
            "glue.catalog-id": "..." })
  → All existing IcebergCatalogOperations methods work unchanged
```

`GlueCatalog` is an official Iceberg implementation with full Schema CRUD + Table CRUD support — this is the lowest-risk part of the design.

#### Engine Integration

**Trino** — `IcebergCatalogPropertyConverter.java`: Add `case "glue":` → `iceberg.catalog.type=glue` + AWS region/catalog-id.

**Spark** — No code change needed. The existing generic `all.put(ICEBERG_CATALOG_TYPE, catalogBackend)` already handles `"glue"`.

### 4.3 Hive Catalog + Glue Backend

Add `metastore-type=glue` property. Use AWS's `aws-glue-datacatalog-hive3-client` library which provides an `IMetaStoreClient` implementation backed by the Glue SDK.

#### Data Flow

```
User: metastore-type=glue, s3-region=us-east-1
  → HiveCatalogOperations.initialize()
    → mergeProperties(conf) — maps Glue properties
    → CachedClientPool(properties)
      → HiveClientPool.newClient()
        → HiveClientFactory.createHiveClient()      ← MODIFIED: skip hive2/3 detection
          → HiveClientClassLoader.createLoader(HIVE3, ...)  ← always Hive3 for Glue
          → HiveClientImpl(HIVE3, properties)
            → detects metastore.type=glue
            → new GlueShim(properties)               ← NEW (replaces HiveShimV3)
              → createMetaStoreClient()
                → AWSGlueDataCatalogHiveClientFactory.create(hiveConf)
                → returns AWSCatalogMetastoreClient (implements IMetaStoreClient)
  → All existing HiveCatalogOperations methods work unchanged
```

#### GlueShim and Hive2/Hive3 Compatibility

**Problem**: `HiveClientFactory.createHiveClient()` probes the remote HMS to detect Hive2 vs Hive3 (tries `getCatalogs()`, falls back on error). This detection is irrelevant for Glue — there is no remote HMS to probe.

**Solution**: When `metastore.type=glue`, skip version detection and always use Hive3 classloader:

```java
// In HiveClientFactory.createHiveClient():
public HiveClient createHiveClient() {
  String metastoreType = properties.getProperty("metastore.type", "hive");
  if ("glue".equalsIgnoreCase(metastoreType)) {
    return createGlueClient();  // Always Hive3, no probe
  }
  // ... existing hive2/hive3 detection logic unchanged ...
}

private HiveClient createGlueClient() {
  if (backendClassLoader == null) {
    synchronized (classLoaderLock) {
      if (backendClassLoader == null) {
        backendClassLoader = HiveClientClassLoader.createLoader(
            HIVE3, Thread.currentThread().getContextClassLoader());
      }
    }
  }
  return createHiveClientInternal(backendClassLoader);
}
```

**Why Hive3 classloader?**

1. **JAR loading path**: `HiveClientClassLoader.getJarDirectory()` maps `HIVE3` → `hive-metastore3-libs/`. The Glue client JAR is placed in this directory (see Section 4.4).
2. **API compatibility**: AWS provides `aws-glue-datacatalog-hive2-client` and `aws-glue-datacatalog-hive3-client`. The `IMetaStoreClient` interfaces differ between versions (Hive3 adds catalog-aware methods). The JAR must match the Hive version in the same directory. We choose Hive3 as the actively maintained variant.

**Future extension**: For Hive2 environments, add `aws-glue-datacatalog-hive2-client` to `hive-metastore2-libs` and select classloader by configuration.

#### GlueShim Design

`GlueShim` extends `HiveShimV2` and overrides only `createMetaStoreClient()`:

| Shim | `createMetaStoreClient()` Implementation |
|---|---|
| `HiveShimV2` | `RetryingMetaStoreClient.getProxy(hiveConf)` → Thrift to HMS |
| `HiveShimV3` | Same as V2 (V3 only adds catalog-aware method overrides) |
| `GlueShim` | `AWSGlueDataCatalogHiveClientFactory.create(hiveConf)` → Glue SDK |

All three return `IMetaStoreClient`. `HiveClientImpl` selects the shim:

```java
// In HiveClientImpl constructor:
String metastoreType = properties.getProperty("metastore.type", "hive");
if ("glue".equalsIgnoreCase(metastoreType)) {
  shim = new GlueShim(properties);
} else {
  switch (hiveVersion) {
    case HIVE2: shim = new HiveShimV2(properties); break;
    case HIVE3: shim = new HiveShimV3(properties); break;
  }
}
```

All upstream code (`HiveClientPool`, `CachedClientPool`, `HiveCatalogOperations`) is unchanged — it programs against the `HiveClient` interface.

#### IMetaStoreClient Relationship

```
org.apache.hadoop.hive.metastore.IMetaStoreClient    ← Hive standard interface
    ├── HiveMetaStoreClient (Thrift impl, connects to HMS)
    └── AWSCatalogMetastoreClient (Glue impl, via AWS Glue SDK)
         └── Created by AWSGlueDataCatalogHiveClientFactory.create(hiveConf)
```

`AWSCatalogMetastoreClient` is a drop-in replacement for `HiveMetaStoreClient`. All upstream code is completely unaware of the difference.

#### Engine Integration

**Trino** — `HiveConnectorAdapter.java`:
- When `metastore-type=glue`: set `hive.metastore=glue` + `hive.metastore.glue.region` + `hive.metastore.glue.catalogid`.
- When `hive` (default): existing `hive.metastore.uri` path unchanged.

**Spark** — `HivePropertiesConverter.java`:
- When `metastore-type=glue`: set `spark.hadoop.hive.metastore.client.factory.class = com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory`.

### 4.4 Dependency Management

#### Iceberg + Glue

| Dependency | Target Module | Scope |
|---|---|---|
| `org.apache.iceberg:iceberg-aws` — Contains `GlueCatalog` implementation. Transitively depends on `software.amazon.awssdk:glue`. Already in version catalog as `libs.iceberg.aws`. | `iceberg/iceberg-common/build.gradle.kts` | `compileOnly` (provided at runtime by `bundles/bundle-aws`) |

No changes to `gradle/libs.versions.toml` required.

#### Hive + Glue

| Dependency | Target Module | Scope |
|---|---|---|
| `com.amazonaws:aws-glue-datacatalog-hive3-client` — Implements `IMetaStoreClient` via Glue SDK. Provides `AWSGlueDataCatalogHiveClientFactory`. | `catalogs/hive-metastore3-libs/build.gradle.kts` | `implementation` (packaged into `hive-metastore3-libs/`) |

**Why `hive-metastore3-libs`?** The Hive catalog uses `HiveClientClassLoader` for class isolation — it loads JARs from `hive-metastore2-libs/` or `hive-metastore3-libs/`. GlueShim uses the Hive3 classloader (see Section 4.3), so the Glue client JAR must be in `hive-metastore3-libs`.

### 4.5 End-to-End Architecture

```
                        Gravitino Server
                              |
            +------ provider=hive ------+------- provider=lakehouse-iceberg ------+
            |     metastore-type=glue   |         catalog-backend=glue            |
            |                           |                                         |
     HiveCatalogOperations       IcebergCatalogOperations
            |                           |
     HiveClientImpl                   IcebergCatalogUtil
     -> GlueShim                      -> loadGlueCatalog()
     -> AWSCatalogMetastoreClient     -> org.apache.iceberg.aws.glue.GlueCatalog
     (impl IMetaStoreClient)          (impl org.apache.iceberg.catalog.Catalog)
            |                           |
            +-------- AWS Glue SDK -----+
                          |
                  AWS Glue Data Catalog
                          |
               +----------+----------+
               |                     |
          Hive Tables          Iceberg Tables
     (StorageDescriptor)    (metadata_location)


                        Query Engines
                              |
        +---- Trino ----+               +---- Spark ----+
        |               |               |               |
   Hive Connector  Iceberg Connector  HiveCatalog  SparkCatalog
   metastore=glue  catalog.type=glue  factory=AWS  catalog-impl=GlueCatalog
```

## 5. Testing Strategy

### 5.1 Unit Tests

**Property conversion** — extend existing test classes:

| Test Class | New Test Cases |
|---|---|
| `TestIcebergCatalogPropertyConverter` | `testGlueBackendProperty()`, `testGlueBackendMissingWarehouse()` |
| `TestHiveConnectorAdapter` | `testBuildGlueConfig()`, `testBuildGlueConfigWithCatalogId()` |

**Hive client routing** — new `TestHiveClientImpl` (in `hive-metastore-common/src/test/`):
- `testGlueShimSelection()`: verify `GlueShim` created when `metastore.type=glue`
- `testDefaultHiveShimSelection()`: verify `HiveShimV2/V3` when `metastore.type=hive` or unset
- `testHiveClientFactorySkipsProbeForGlue()`: verify Hive3 classloader used directly

**GlueShim** — new `TestGlueShim` (in `hive-metastore-common/src/test/`):
- `testCreateMetaStoreClient()`: mock factory, verify correct invocation
- `testGlueShimExtendsHiveShimV2()`: verify inheritance
- `testGluePropertiesPassedToHiveConf()`: verify property propagation

### 5.2 Integration Tests — Reuse Existing Test Framework

The project has well-established integration test inheritance hierarchies. Glue tests inherit from existing parent classes — only override environment initialization.

**Catalog operations**:

| New Test Class | Extends | Override |
|---|---|---|
| `CatalogHiveGlueIT` | `CatalogHive2IT` (23 tests) | `startNecessaryContainer()` → LocalStack; `createCatalogProperties()` → `metastore-type=glue` |
| `CatalogIcebergGlueIT` | `CatalogIcebergBaseIT` (15 tests) | `initIcebergCatalogProperties()` → `catalog-backend=glue` |

Example:
```java
@Tag("gravitino-docker-test")
public class CatalogHiveGlueIT extends CatalogHive2IT {
  @Override
  protected void startNecessaryContainer() {
    containerSuite.startLocalStackContainer();
  }

  @Override
  protected Map<String, String> createCatalogProperties() {
    return ImmutableMap.of(
        "metastore-type", "glue",
        "s3-region", "us-east-1",
        "aws-glue-endpoint", localStackEndpoint);
  }
}
```

All parent tests (`testCreateHiveTable`, `testAlterTable`, `testListPartitions`, etc.) automatically run against the Glue backend — no rewriting needed.

**Hive + Glue supported operations** (all covered by inherited `CatalogHive2IT` tests):

- Schema: `createDatabase`, `getDatabase`, `getAllDatabases`, `alterDatabase`, `dropDatabase(cascade)`
- Table: `createTable`, `getTable`, `getAllTables`, `alterTable`, `dropTable(deleteData)`, `purgeTable`, `getTableObjectsByName`
- Partition: `listPartitionNames`, `listPartitions`, `listPartitions(filter)`, `getPartition`, `addPartition`, `dropPartition(deleteData)`

These are all directly supported by `AWSCatalogMetastoreClient` (`IMetaStoreClient`). GlueShim only creates the client instance — upstream `HiveShim` methods work automatically.

**Trino E2E**: Add Glue catalog configuration in `TrinoQueryITBase`, reuse existing SQL test scripts.

**Spark E2E**:

| New Test Class | Extends | Override |
|---|---|---|
| `SparkHiveGlueCatalogIT` | `SparkHiveCatalogIT` | `getCatalogConfigs()` → Glue config |
| `SparkIcebergGlueCatalogIT` | `SparkIcebergCatalogIT` | Catalog properties → `catalog-backend=glue` |

All `SparkCommonIT` tests (31 DDL/DML/query tests) are automatically inherited.

### 5.3 Build Verification

```bash
./gradlew build -x test                # Compile
./build.sh sp                          # Spotless formatting
./gradlew test -PskipITs               # Unit tests
./gradlew test -PskipTests -PskipDockerTests=false  # Integration tests (Docker + LocalStack)
```

## 6. Implementation Plan

### Phase 1: Full Glue Backend Support (Implement All Existing Interfaces)

Phase 1 makes the Glue backend pass all existing integration tests. The Hive catalog implements `SupportsSchemas` + `TableCatalog` (including partitions), and the Iceberg catalog implements `SupportsSchemas` + `TableCatalog`. All methods must be fully implemented — not read-only.

#### WI-1: Iceberg + Glue Backend

No new interface implementation is required. Apache Iceberg's `GlueCatalog` (from `iceberg-aws`) already fully implements the `Catalog` interface — including Schema CRUD (`listNamespaces`, `createNamespace`, `loadNamespaceMetadata`, `setProperties`, `dropNamespace`) and Table CRUD (`listTables`, `createTable`, `loadTable`, `renameTable`, `dropTable`). This work item wires Gravitino's configuration layer to instantiate `GlueCatalog` as a new backend.

| # | Work Item | Files Involved | Description |
|---|---|---|---|
| 1.1 | Add `GLUE` enum value | `IcebergCatalogBackend.java` | Add `GLUE` to the enum |
| 1.2 | Add Glue property constants | `IcebergConstants.java` | `GLUE_CATALOG_ID`, `GLUE_ENDPOINT` |
| 1.3 | Add property mapping | `IcebergPropertiesUtils.java` | `s3-region` → `client.region`, `aws-glue-catalog-id` → `glue.catalog-id`, etc. |
| 1.4 | Declare property metadata | `IcebergCatalogPropertiesMetadata.java` | Register `aws-glue-catalog-id`, `aws-glue-endpoint` |
| 1.5 | Add ConfigEntry | `IcebergConfig.java` | Glue-related configuration entries |
| 1.6 | Implement `loadGlueCatalog()` | `IcebergCatalogUtil.java` | `case GLUE:` branch, instantiate and initialize `GlueCatalog` |
| 1.7 | Add dependency | `iceberg/iceberg-common/build.gradle.kts` | `compileOnly(libs.iceberg.aws)` |
| 1.8 | Unit tests | `TestIcebergCatalogPropertyConverter.java` | `testGlueBackendProperty()`, `testGlueBackendMissingWarehouse()` |

#### WI-2: Hive + Glue Backend

No new catalog interface implementation is required. AWS's `AWSCatalogMetastoreClient` (from `aws-glue-datacatalog-hive3-client`) already fully implements the `IMetaStoreClient` interface — including all Schema, Table, and Partition operations:

- Schema: `createDatabase`, `getDatabase`, `getAllDatabases`, `alterDatabase`, `dropDatabase(cascade)`
- Table: `createTable`, `getTable`, `getAllTables`, `alterTable`, `dropTable(deleteData)`, `purgeTable`, `getTableObjectsByName`
- Partition: `listPartitionNames`, `listPartitions`, `listPartitions(filter)`, `getPartition`, `addPartition`, `dropPartition(deleteData)`

This work item creates a thin routing layer (`GlueShim` — one method override) that plugs `AWSCatalogMetastoreClient` into the existing Hive client chain. All upstream code (`HiveShim`, `HiveClientImpl`, `HiveCatalogOperations`) works unchanged through the `IMetaStoreClient` / `HiveClient` interfaces.

| # | Work Item | Files Involved | Description |
|---|---|---|---|
| 2.1 | Add Hive constants | `HiveConstants.java` | `METASTORE_TYPE`, `METASTORE_TYPE_GLUE`, `AWS_GLUE_CATALOG_ID`, `AWS_GLUE_ENDPOINT` |
| 2.2 | Declare property metadata | `HiveCatalogPropertiesMetadata.java` | Register `metastore-type` + Glue properties; make `metastore.uris` conditionally required |
| 2.3 | Property mapping | `HiveCatalogOperations.java` | Add Glue properties to `GRAVITINO_CONFIG_TO_HIVE`; add Glue branch in `initialize()` |
| 2.4 | Modify HiveClientFactory | `HiveClientFactory.java` | Skip hive2/3 detection when `metastore.type=glue`, use Hive3 classloader |
| 2.5 | Modify HiveClientImpl | `HiveClientImpl.java` | Select `GlueShim` when `metastore.type=glue` |
| 2.6 | Create GlueShim | `GlueShim.java` (new) | Extends `HiveShimV2`, overrides `createMetaStoreClient()` to use `AWSGlueDataCatalogHiveClientFactory` |
| 2.7 | Add dependency | `hive-metastore3-libs/build.gradle.kts` | `implementation("com.amazonaws:aws-glue-datacatalog-hive3-client:...")` |
| 2.8 | Unit tests | `TestHiveClientImpl.java` (new), `TestGlueShim.java` (new) | Routing, GlueShim |

#### WI-3: Integration Tests (Catalog Operations)

| # | Work Item | Files Involved | Description |
|---|---|---|---|
| 3.1 | LocalStack container support | `ContainerSuite.java` | Add `startLocalStackContainer()` |
| 3.2 | Hive + Glue integration test | `CatalogHiveGlueIT.java` (new) | Extends `CatalogHive2IT`, LocalStack |
| 3.3 | Iceberg + Glue integration test | `CatalogIcebergGlueIT.java` (new) | Extends `CatalogIcebergBaseIT` |

#### WI-4: Trino and Spark Connector Support

| # | Work Item | Files Involved | Description |
|---|---|---|---|
| 4.1 | Iceberg — Trino property conversion | `IcebergCatalogPropertyConverter.java` | `case "glue":` → `iceberg.catalog.type=glue` |
| 4.2 | Hive — Trino property conversion | `HiveConnectorAdapter.java` | `metastore-type=glue` → `hive.metastore=glue` |
| 4.3 | Hive — Spark property conversion | `HivePropertiesConverter.java` | Set `hive.metastore.client.factory.class` |
| 4.4 | Unit tests | `TestIcebergCatalogPropertyConverter.java`, `TestHiveConnectorAdapter.java` | Trino Glue config tests |
| 4.5 | Trino + Glue E2E | `TrinoQueryITBase.java` + SQL files | Glue catalog config, reuse existing SQL tests |
| 4.6 | Spark + Hive + Glue E2E | `SparkHiveGlueCatalogIT.java` (new) | Extends `SparkHiveCatalogIT` |
| 4.7 | Spark + Iceberg + Glue E2E | `SparkIcebergGlueCatalogIT.java` (new) | Extends `SparkIcebergCatalogIT` |

Iceberg — Spark requires no code change (generic passthrough).

#### WI-5: Documentation and CI

| # | Work Item | Description |
|---|---|---|
| 5.1 | Update Gravitino user documentation | Add Glue configuration examples to Hive catalog and Iceberg catalog docs |
| 5.2 | CI configuration | Ensure Glue integration tests run in CI (requires LocalStack container) |

---

### Phase 2: Advanced Features (Future Iterations)

| # | Work Item | Description | Priority |
|---|---|---|---|
| P2-1 | **Cross-account Glue access** | Support `aws-glue-catalog-id` + STS AssumeRole (`s3-role-arn`) for cross-account access | High |
| P2-2 | **Hive2 Glue client support** | Add `aws-glue-datacatalog-hive2-client` to `hive-metastore2-libs` for Hive2 environments | Medium |
| P2-3 | **Glue Table Filter** | Filter Iceberg tables from Hive catalog (`Parameters["table_type"]="ICEBERG"` hidden) to avoid confusion | Medium |
| P2-4 | **VPC Endpoint support** | Validate `aws-glue-endpoint` for VPC private link scenarios, add integration tests | Medium |
| P2-5 | **Paimon / Hudi on Glue** | Extend Glue support to Paimon, Hudi, and other lakehouse formats | Low |
| P2-6 | **Unified Catalog experience** | Investigate single Gravitino catalog exposing both Hive and Iceberg tables (requires routing layer in Trino/Spark connectors) | Low |
| P2-7 | **AWS Lake Formation integration** | Integrate Lake Formation fine-grained access control into Gravitino's authorization module | Low |
