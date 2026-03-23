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

# Gravitino Flink Connector Multi-Version Support Spec

| Field | Value |
| --- | --- |
| Status | Proposal |
| Owner | FANNG |
| Created | 2026-03-16 |
| Target Release | 1.3.0 |
| Related Issue | [#9710](https://github.com/apache/gravitino/issues/9710) |
| Related PR | N/A |

## Goal

Add maintained support for Flink `1.18`, `1.19`, and `1.20` with versioned runtime artifacts, similar to the Spark connector release model.

The output of this work should be:

- one shared `flink-common` module
- one connector module per supported Flink minor
- one runtime shadow jar per supported Flink minor
- CI and docs that explicitly cover the supported Flink matrix

## Non-Goals

- Supporting Flink `1.17` or Flink `2.x`
- Changing connector behavior beyond what is required for multi-version compatibility
- Adding new connector features such as materialized table support
- Reworking provider semantics for Hive, Iceberg, JDBC, or Paimon
- Adding OAuth2 or Kerberos real-environment validation to the required test matrix for this delivery
- Producing one universal jar that works across all Flink minors

Note: Flink `2.0` support is explicitly out of scope for this delivery. It should be treated as a larger follow-up compatibility lane rather than a small extension of the `1.18` to `1.20` work, but the module split and extension hooks introduced here should keep a clean path for that future effort. Follow-up implementation findings for that lane are recorded in [flink_2_0_adaptation_notes.md](flink_2_0_adaptation_notes.md).

## Current Connector Model

The current Flink connector is not a generic table runtime connector. It is primarily a Flink `CatalogStore` plus a set of Flink `Catalog` implementations that proxy metadata operations to Gravitino and delegate data/table behavior to native Flink ecosystem catalogs.

### Main Flink Interaction Points

1. Catalog store bootstrapping

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/store/GravitinoCatalogStoreFactory.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/store/GravitinoCatalogStore.java`

These classes integrate with:

- `CatalogStoreFactory`
- `CatalogStore`
- `CatalogDescriptor`
- `FactoryUtil.createCatalogStoreFactoryHelper(...)`
- ServiceLoader discovery of Flink `Factory`

2. Catalog SPI implementations

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalog.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalogFactory.java`

These classes integrate with:

- `AbstractCatalog`
- `CatalogBaseTable`
- `CatalogTable`
- `ResolvedCatalogBaseTable`
- `ResolvedCatalogTable`
- `CatalogDatabase`
- `ObjectPath`
- Flink `TableChange`
- Flink `Schema`

3. Provider-specific wrappers around native Flink ecosystem catalogs

- Hive: `GravitinoHiveCatalog`, `GravitinoHiveCatalogFactory`
- Iceberg: `GravitinoIcebergCatalog`, `GravitinoIcebergCatalogFactory`
- JDBC: `GravitinoJdbcCatalog`, `GravitinoJdbcCatalogFactory`
- Paimon: `GravitinoPaimonCatalog`, `GravitinoPaimonCatalogFactory`

These delegate to:

- `org.apache.flink.table.catalog.hive.HiveCatalog`
- `org.apache.iceberg.flink.FlinkCatalog`
- `org.apache.flink.connector.jdbc.catalog.JdbcCatalog`
- `org.apache.paimon.flink.FlinkCatalogFactory`

4. Flink-to-Gravitino conversion utilities

- `TypeUtils`
- `TableUtils`
- `CatalogPropertiesConverter`
- `SchemaAndTablePropertiesConverter`
- `PartitionConverter`
- `FlinkGenericTableUtil`

These are where Flink schema/type/property semantics are translated into Gravitino APIs.

## Findings From Code Review

### What the connector actually depends on

The connector depends much more on the Flink Table/Catalog SPI than on the DataStream runtime. The version-sensitive surface is therefore:

- Flink table/catalog API jars
- native provider jars tied to the Flink minor
- runtime shadow jar assembly
- tests and workflows that hardcode a single module path today

### Flink SPI stability from 1.18 to 1.20

Reviewing local Flink sources shows that the APIs used by the current connector are mostly stable across `release-1.18`, `release-1.19`, and `origin/release-1.20`.

Observed changes are mostly additive:

- `CatalogDescriptor` gained optional comment support in `1.20`
- `CatalogTable` gained a builder and distribution-related API in `1.20`
- `CatalogPropertiesUtil` gained materialized-table serialization in `1.20`
- `Catalog` gained more documentation and some additional default-method-oriented expectations in `1.20`

Important point: the connector currently uses APIs that remain present and source-compatible across `1.18`, `1.19`, and `1.20`, including:

- `CatalogStoreFactory`
- `CatalogDescriptor.of(name, configuration)`
- `CatalogPropertiesUtil.serializeCatalogTable(...)`
- `CatalogPropertiesUtil.deserializeCatalogTable(...)`
- `CatalogTable.of(...)`
- `ResolvedCatalogBaseTable`
- `TableChange` classes used in `BaseCatalog`

Conclusion: the Java implementation is likely to stay almost entirely shared. The hard part is the build and dependency matrix, not large API shims.

### External dependency constraints are stricter than Flink SPI changes

As of `March 16, 2026`, Maven Central metadata shows:

- `org.apache.iceberg:iceberg-flink-runtime-1.20` starts at Iceberg `1.7.0`
- `org.apache.iceberg:iceberg-flink-runtime-1.19` includes Iceberg `1.6.1`
- `org.apache.iceberg:iceberg-flink-runtime-1.18` includes Iceberg `1.6.1`
- `org.apache.paimon:paimon-flink-1.18`, `1.19`, and `1.20` all include Paimon `1.2.0`
- `org.apache.flink:flink-connector-jdbc` publishes `3.2.0-1.18`, `3.2.0-1.19`, `3.3.0-1.19`, and `3.3.0-1.20`

This means the current single global connector dependency model is not sufficient:

- a single global `flink = 1.18.0` is not sufficient
- a single global `flinkjdbc = 3.2.0-1.18` is not sufficient
- a single global `iceberg4connector = 1.6.1` is not sufficient for Flink `1.20`

This is the main reason the connector must be split by Flink minor.

## Design Decision

Follow the Spark connector model more directly:

- introduce a compiled `:flink-connector:flink-common` module
- keep `:flink-connector:flink-1.18`, `:flink-connector:flink-1.19`, and `:flink-connector:flink-1.20` as thin versioned modules
- keep one runtime shadow module per Flink minor

### Why this is acceptable

This is workable if `flink-common` is treated as a binary-compatible common layer compiled against the lowest supported baseline and if version-sensitive code is isolated.

Recommended rule set:

- compile `flink-common` against Flink `1.18`
- use `compileOnly` for Flink/provider dependencies in `flink-common`
- allow versioned `flink-1.18`, `flink-1.19`, `flink-1.20` modules to add small overlays for minor-specific APIs
- keep runtime packaging and test execution version-specific

This intentionally differs from the Spark connector approach that compiles the common layer against the latest Spark minor. For Flink `1.18` to `1.20`, the catalog/table API drift is mostly additive, so compiling `flink-common` against the lowest supported baseline is the safer default because it reduces accidental references to newer API only present in `1.19` or `1.20`.

Important clarification: `compileOnly` avoids bundling dependencies, but it does not solve binary compatibility by itself. The bytecode in `flink-common` is still compiled against a concrete API surface. Therefore:

- `flink-common` must only use API and provider methods that remain binary-compatible across `1.18`, `1.19`, and `1.20`
- anything version-sensitive must move into the versioned modules

## Proposed Project Layout

```text
flink-connector/
  flink-common/
    build.gradle.kts
    src/main/java/...
    src/main/resources/...
    src/test/java/...
    src/test/resources/...
  v1.18/
    build.gradle.kts
    flink/
      build.gradle.kts          # depends on :flink-connector:flink-common
      src/main/java/...         # only if a 1.18-only shim is needed
      src/test/java/...         # only if a 1.18-only test is needed
    flink-runtime/
      build.gradle.kts
      src/test/java/...
  v1.19/
    build.gradle.kts
    flink/
      build.gradle.kts
      src/main/java/...
      src/test/java/...
    flink-runtime/
      build.gradle.kts
      src/test/java/...
  v1.20/
    build.gradle.kts
    flink/
      build.gradle.kts
      src/main/java/...
      src/test/java/...
    flink-runtime/
      build.gradle.kts
      src/test/java/...
```

### Gradle project names

Add these projects in `settings.gradle.kts` under the existing Scala `2.12` guard:

- `:flink-connector:flink-common`
- `:flink-connector:flink-1.18`
- `:flink-connector:flink-runtime-1.18`
- `:flink-connector:flink-1.19`
- `:flink-connector:flink-runtime-1.19`
- `:flink-connector:flink-1.20`
- `:flink-connector:flink-runtime-1.20`

Current-scope Scala note:

- Flink `1.18`, `1.19`, and `1.20` in this delivery all use Scala `2.12` artifacts
- the module layout and publication names should therefore consistently keep the `_2.12` suffix
- supporting a different Scala suffix is out of scope for this change and should be treated as a separate compatibility question

### Project migration rule

After Phase 1:

- `:flink-connector:flink` is replaced by `:flink-connector:flink-common` plus `:flink-connector:flink-1.18`
- `:flink-connector:flink-runtime` is replaced by `:flink-connector:flink-runtime-1.18`
- the old `:flink-connector:flink` and `:flink-connector:flink-runtime` entries should be removed from `settings.gradle.kts`
- do not keep the old project names as Gradle aliases because that makes publication, workflow targeting, and review reasoning ambiguous
- `settings.gradle.kts` should map the new project names to their explicit project directories

### Module responsibility split

`flink-common` should contain the implementation that is expected to remain common across all supported Flink minors, for example:

- `catalog/BaseCatalog.java`
- `catalog/BaseCatalogFactory.java`
- `catalog/GravitinoCatalogManager.java`
- property/type/partition conversion utilities
- `GravitinoCatalogStore` and `GravitinoCatalogStoreFactory`
- provider classes only if their used APIs are confirmed binary-compatible across `1.18` to `1.20`

Versioned `flink-*` modules should contain:

- the published version-specific connector artifact
- minor-specific factory/catalog overlay classes if needed
- minor-specific SPI resource files if needed
- minor-specific tests if needed

### Provider placement matrix

The initial provider split should be reviewed explicitly instead of relying only on a general rule.

| Provider | Initial placement | Why | Required validation | Move trigger |
| --- | --- | --- | --- | --- |
| Hive | start in `flink-common` only if shared code uses constructors and helpers that remain ABI-compatible | current wrapper logic is mostly shared, but Hive connector drift is historically the most likely provider risk inside `1.18` to `1.20` | compile on all versioned classpaths, instantiate factory/wrapper, and run real-environment smoke | `NoSuchMethodError`, `ClassNotFoundException`, compile break, or provider-specific test failure |
| Iceberg | start in `flink-common` with versioned runtime dependencies | the wrapper logic is thin, but `iceberg-flink-runtime-*` versions must be matched by Flink minor | compile on all versioned classpaths, instantiate factory/wrapper, and run Iceberg smoke | constructor/method drift or runtime linkage failure on versioned artifact |
| JDBC | start in `flink-common` with versioned connector dependencies | the wrapper is thin, but JDBC connector artifacts already differ by Flink minor | compile on all versioned classpaths, instantiate factory/wrapper, and run JDBC smoke | constructor drift, artifact layout drift, or runtime linkage failure |
| Paimon | start in `flink-common` with versioned runtime dependencies | Paimon `1.2.0` is available for `1.18` to `1.20`, so a shared wrapper is a reasonable first cut | compile on all versioned classpaths, instantiate factory/wrapper, and run Paimon smoke | factory API drift or runtime linkage failure |

This gives:

- Spark-aligned repository structure
- one shared compiled jar
- one dependency graph per Flink minor at the published connector/runtime layer
- a clean place for minor-specific shims if binary compatibility breaks appear later

## Dependency Matrix

Introduce version-specific aliases in `gradle/libs.versions.toml`.

Minimum required split:

- `flink18`, `flink19`, `flink20`
- `flinkJdbc18`, `flinkJdbc19`, `flinkJdbc20`
- `iceberg4flink18`, `iceberg4flink19`, `iceberg4flink20`

Paimon can stay shared if the chosen version works for all three minors, but using explicit aliases is still cleaner:

- `paimon4flink18`, `paimon4flink19`, `paimon4flink20`

Also introduce version-specific library aliases where necessary so the versioned modules do not depend on one global Flink connector version.

### Recommended baseline

Keep the dependency decision explicit per Flink minor instead of trying to force one global value.

Example direction:

| Variant | Flink | JDBC connector | Iceberg runtime | Paimon runtime |
| --- | --- | --- | --- | --- |
| `1.18` | `1.18.x` | `3.2.0-1.18` | `1.6.1` or newer | `1.2.0` |
| `1.19` | `1.19.x` | `3.2.0-1.19` or `3.3.0-1.19` | `1.6.1` or newer | `1.2.0` |
| `1.20` | `1.20.x` | `3.3.0-1.20` | `>= 1.7.0` | `1.2.0` |

Important rule: do not keep `iceberg4connector = 1.6.1` globally if Flink `1.20` is introduced.

## `flink-common` Dependency Policy

`flink-common` should compile against the lowest supported baseline, recommended as Flink `1.18`.

Recommended dependency posture for `flink-common`:

- `compileOnly` on Flink `1.18` table/catalog APIs
- `compileOnly` on provider baseline artifacts used directly by shared code
- `testImplementation` on the same baseline to keep common unit tests executable

Versioned modules should then use their own version-specific dependencies:

- `:flink-connector:flink-1.18` depends on `:flink-connector:flink-common` and Flink `1.18` deps
- `:flink-connector:flink-1.19` depends on `:flink-connector:flink-common` and Flink `1.19` deps
- `:flink-connector:flink-1.20` depends on `:flink-connector:flink-common` and Flink `1.20` deps

### What `compileOnly` does and does not solve

What it helps with:

- keeps `flink-common` thin
- avoids bundling Flink/provider jars in the common artifact
- matches the Spark connector structure more closely

What it does not help with:

- method signature drift
- constructor signature drift
- removed classes
- changed inheritance hierarchies

Therefore reviewers should treat `compileOnly` as a packaging choice, not a compatibility mechanism.

## Extension Model For Newer Catalog APIs

Version-specific Flink catalog classes are exception-based, not default-based.

That means:

- do not create `GravitinoHiveCatalogFlink118`, `GravitinoHiveCatalogFlink119`, `GravitinoHiveCatalogFlink120` by default
- keep provider catalog classes in `flink-common` as long as their used API remains compatible
- add version-specific classes only when compilation, tests, or runtime ABI checks prove they are necessary

### How Flink `1.20`-only catalog APIs should be supported

If Flink `1.20` adds catalog-related API that should be supported, the implementation should prefer the following escalation order.

1. Helper-level shim

Use this when the difference is object construction or metadata enrichment.

Examples:

- `CatalogDescriptor` adds comment support
- `CatalogTable` adds builder/distribution support
- `CatalogPropertiesUtil` adds new serializable catalog metadata

Recommended approach:

- keep the shared algorithm in `flink-common`
- move the version-sensitive object creation into an overridable helper
- override that helper in `:flink-connector:flink-1.20`

2. Base-layer shim

Use this when the difference affects the common Gravitino catalog bridge rather than one provider.

Examples:

- new default methods on Flink `Catalog`
- new table metadata conversion logic needed for all providers
- new catalog-store descriptor assembly logic

Recommended approach:

- add `BaseCatalog120`, `GravitinoCatalogStore120`, or similar thin adaptors in `v1.20`
- keep provider classes reusing the base-layer shim instead of duplicating logic

3. Provider-level shim

Use this only when the drift is provider-specific.

Examples:

- Hive-specific constructor or factory behavior changes
- Iceberg/Paimon/JDBC wrapper behavior changes that do not affect the other providers

Recommended approach:

- introduce the smallest possible `v1.20` provider-specific subclass or factory class
- do not fork all provider classes just because one provider changed

### Hook points that should be pre-created in this implementation

To make later `1.20` catalog API support cheap, this implementation should add explicit extension hooks now, even if the initial `1.18`, `1.19`, and `1.20` behavior is identical.

Recommended hooks:

1. Catalog descriptor construction hook

Location:

- `GravitinoCatalogStore`

Purpose:

- allow `v1.20` to build `CatalogDescriptor` with newer metadata such as comment support without rewriting store logic

Example shape:

```java
protected CatalogDescriptor newCatalogDescriptor(
    String catalogName,
    Configuration configuration) {
  return CatalogDescriptor.of(catalogName, configuration);
}
```

Then `v1.20` can override if newer `CatalogDescriptor` fields should be populated.

2. Flink table construction hook

Location:

- `BaseCatalog`

Purpose:

- isolate the version-sensitive creation of `CatalogTable` / `CatalogBaseTable`
- make it possible for `v1.20` to switch from `CatalogTable.of(...)` to builder-based construction or set newer metadata such as distribution

Example shape:

```java
protected CatalogTable newCatalogTable(
    org.apache.flink.table.api.Schema schema,
    String comment,
    List<String> partitionKeys,
    Map<String, String> options) {
  return CatalogTable.of(schema, comment, partitionKeys, options);
}
```

3. Generic table deserialize/serialize hook

Location:

- `FlinkGenericTableUtil`
- or a thin helper owned by `BaseCatalog`

Purpose:

- isolate future changes in `CatalogPropertiesUtil` behavior
- make it possible to support newer Flink catalog-table metadata without touching the shared alter/create flow

4. Factory creation hook

Location:

- provider factories in `flink-common`

Purpose:

- allow a versioned module to swap in a version-specific factory class only if needed
- keep ServiceLoader wiring minimal

### What must be done now versus later

Do now in this multi-version support change:

- add the extension hooks above in `flink-common`
- keep the default implementation behavior identical to the current connector
- leave `v1.20` free to override those hooks later

Do later only when required by real Flink `1.20` API usage:

- add `v1.20`-specific subclasses
- enrich descriptors/tables with `1.20`-only metadata
- implement new `Catalog` methods that need non-default behavior

## Future Flink 2.0 Compatibility Lane

Flink `2.0` is still out of scope for this delivery, but later implementation and debugging work has already confirmed the shape of that compatibility lane. This section records those findings so the future follow-up does not need to rediscover the same breakpoints.

Detailed implementation notes for that follow-up are documented in [flink_2_0_adaptation_notes.md](flink_2_0_adaptation_notes.md).

### Confirmed architectural conclusion

Yes, this architecture can support Flink `2.0`, with one important caveat:

- the overall `flink-common + versioned modules + versioned runtime modules` architecture is still valid
- but Flink `2.0` must be treated as a new major compatibility lane, not as just another `1.x` minor

In practice, this means:

- add `:flink-connector:flink-2.0`
- add `:flink-connector:flink-runtime-2.0`
- add a real `v2.0` shim layer at the version-sensitive Flink API boundaries
- do not assume `flink-common` bytecode compiled against `1.18` will run unchanged on `2.0`

### Confirmed breakpoints from real implementation work

Later Flink `2.0` implementation and validation work confirmed that the following are real breakpoints, not just theoretical risks:

- `CatalogTable.of(...)` is no longer available; `CatalogTable.newBuilder()` must be supported
- `CatalogPropertiesUtil.serializeCatalogTable(...)` requires a newer compatibility path in the `2.0` lane
- Iceberg's `FlinkCatalogFactory` changed from `createCatalog(String, Map<String, String>)` to `createCatalog(CatalogFactory.Context)`
- Flink `2.0` JDBC artifacts and class packages changed, so direct `1.x` imports are not stable enough
- real Flink `2.0.x` distributions expose stricter runtime behavior than repo-local tests, especially for Iceberg REST URI handling and JDBC driver/classloader visibility

This confirms that Flink `2.0` needs both:

- versioned modules and dependencies
- narrow compatibility helpers in shared code

### Minimum code changes required for a future Flink `2.0` follow-up

At minimum, future Flink `2.0` support should include all of the following:

1. Add versioned modules

- `:flink-connector:flink-2.0`
- `:flink-connector:flink-runtime-2.0`

2. Add shared compatibility helpers at the actual breakpoints

The later implementation work showed that these hooks are required:

- shared `CatalogTable` construction in `BaseCatalog`
- generic table serialization/deserialization in `FlinkGenericTableUtil`
- Iceberg factory creation in `GravitinoIcebergCatalog` and `GravitinoIcebergCatalogFactory`
- JDBC factory creation in `GravitinoJdbcCatalog`
- `ServiceLoader` discovery hardening in `GravitinoCatalogStore`
- tests that directly used `CatalogTable.of(...)`

3. Add `v2.0` API adapters where new catalog surface should be supported

Examples:

- model-related methods on `Catalog`
- newer catalog descriptor metadata
- materialized-table-related table metadata if Gravitino chooses to expose it later

4. Re-evaluate provider wrappers independently

Flink `2.0` provider dependencies should not be assumed compatible with the `1.18` to `1.20` wrappers even when the core catalog SPI is bridged.

### Provider status from current Flink `2.0` experiments

The later Flink `2.0` work produced a more concrete provider picture than this spec originally had:

| Area | Repo-level status | Real external `2.0.1` status | Notes |
| --- | --- | --- | --- |
| Core catalog lane | validated | indirectly validated through Paimon and Iceberg REST flows | the shared architecture and compat-helper approach work |
| Paimon | validated | passed | a good candidate for the initial supported provider set |
| Iceberg REST | validated | passed | URI shape and REST auxiliary service handling matter |
| JDBC | validated in repo-level tests | not yet passed | real SQL client still hits driver/classloader issues such as `No suitable driver found` |
| Hive | not enabled | not validated | keep out of the initial `2.0` support claim unless separate proof is added |

Implication:

- Iceberg REST and Paimon look feasible for a first `2.0` support cut
- JDBC requires a follow-up focused on real-distribution SQL client driver loading
- Hive remains the highest-risk provider and should not be promised without direct evidence

### Recommended staged strategy for a future Flink `2.0` effort

Do Flink `2.0` in two stages instead of trying to carry every provider at once.

Stage 1: core compatibility lane plus the validated provider subset

- create `flink-2.0` and `flink-runtime-2.0`
- keep common catalog-store bootstrapping working
- adapt common table/catalog object construction through shared helpers
- validate the core lane, Paimon, and Iceberg REST
- add unit tests, runtime jar checks, and at least one real-distribution smoke path

Stage 2: provider completion and broader surface

- finish JDBC real-distribution validation and driver/classloader handling
- validate whether Hive is possible, partial, or must stay unsupported
- add any model/materialized-table handling only if the product scope actually requires it
- extend CI once the supported provider matrix is explicit

### What this current implementation should do now for future Flink `2.0`

Even though Flink `2.0` is out of scope for this delivery, this implementation should avoid choices that would block it later.

Required now:

- keep all version-sensitive Flink object construction behind hooks
- avoid hard-coding `CatalogTable.of(...)` deep inside shared logic
- keep provider construction behind thin helpers or movable wrappers
- keep provider wrappers movable into version modules if `2.0` requires it
- make `ServiceLoader`-based factory discovery tolerant of optional provider linkage failures

Not required now:

- creating `v2.0` source directories
- adding `2.0` Gradle modules
- changing provider support statements for the current release

### Future Flink 2.0 acceptance for a separate follow-up

Flink `2.0` should only be considered supported in a future task when all of the following are true:

1. `:flink-connector:flink-2.0` and `:flink-connector:flink-runtime-2.0` build successfully.
2. Shared common logic runs correctly through a `v2.0` shim layer.
3. The supported provider matrix for `2.0` is explicitly documented and does not overclaim Hive or JDBC.
4. Repo-level tests pass for the declared `2.0` scope.
5. At least one real-distribution `2.0` smoke lane passes for each declared supported provider.
6. Docs do not imply that all `1.x` providers automatically carry over to `2.0`.
7. CI has at least one dedicated Flink `2.0` validation path.

## Artifact Naming

Keep the existing naming pattern and extend it by Flink minor:

- `gravitino-flink-1.18_2.12`
- `gravitino-flink-1.19_2.12`
- `gravitino-flink-1.20_2.12`
- `gravitino-flink-connector-runtime-1.18_2.12`
- `gravitino-flink-connector-runtime-1.19_2.12`
- `gravitino-flink-connector-runtime-1.20_2.12`

This is already aligned with how the current `1.18` jar is named.

Naming note:

- the connector jar intentionally keeps the existing `gravitino-flink-*` pattern instead of adding an extra `connector` token
- the runtime jar intentionally keeps the existing `gravitino-flink-connector-runtime-*` pattern
- this avoids renaming existing published coordinates while still making the Flink minor explicit

## Detailed Implementation Plan

### Phase 1: Mechanical split without behavior change

1. Create `flink-common` and move the current shared implementation into it.
2. Add `v1.18`, `v1.19`, and `v1.20` version folders.
3. Create versioned Gradle subprojects and wire them in `settings.gradle.kts`.
4. Extract the extension hooks needed for future `1.20` catalog API support into `flink-common`.
5. Make `:flink-connector:flink-1.18` depend on `:flink-connector:flink-common`.
6. Keep `1.18` behavior unchanged.
7. Remove the old `:flink-connector:flink` and `:flink-connector:flink-runtime` project entries once `flink-common`, `flink-1.18`, and `flink-runtime-1.18` are wired.

Acceptance for Phase 1:

- `:flink-connector:flink-common:test` passes
- `:flink-connector:flink-1.18:test -PskipITs` passes
- `:flink-connector:flink-runtime-1.18:test` passes
- the old `:flink-connector:flink` and `:flink-connector:flink-runtime` projects are no longer present in `settings.gradle.kts`

### Phase 2: Add 1.19 with zero or near-zero Java divergence

1. Create `:flink-connector:flink-1.19` and `:flink-connector:flink-runtime-1.19`.
2. Make `:flink-connector:flink-1.19` depend on `:flink-connector:flink-common`.
3. Use `1.19` dependency aliases.
4. Compile and run unit tests.
5. Only add `v1.19/flink/src/main/java` overlay files if compilation proves they are needed.
6. Run provider runtime linkage validation on the `1.19` dependency graph, not only common baseline tests.

Expected outcome:

- no provider behavior changes
- probably no Java shim classes needed
- any provider wrapper that fails runtime linkage moves into the `1.19` module instead of staying in `flink-common`

### Phase 3: Add 1.20 and handle dependency deltas first

1. Create `:flink-connector:flink-1.20` and `:flink-connector:flink-runtime-1.20`.
2. Make `:flink-connector:flink-1.20` depend on `:flink-connector:flink-common`.
3. Use `1.20` dependency aliases.
4. Set Iceberg runtime to a version that actually publishes `iceberg-flink-runtime-1.20`.
5. Set JDBC connector to `3.3.0-1.20` or another validated `1.20` variant.
6. Compile against the common jar plus `1.20` deps.
7. Only if compilation or runtime linking breaks appear, add minimal `v1.20` overlay classes.
8. Run provider runtime linkage validation on the `1.20` dependency graph before declaring the shared provider wrappers safe.

### Phase 4: Keep SPI resources aligned

Ensure the service registration remains available for each versioned module:

- `META-INF/services/org.apache.flink.table.factories.Factory`

If all factory classes remain in `flink-common`, the service file can remain there and the versioned modules should not contribute a duplicate `Factory` service file.

If a version-specific factory class is introduced, the versioned module's service file should replace the common effective service list for that variant.

Required rule in that case:

- the versioned module must re-list every factory class that should be visible for that variant, including common factories that still apply
- do not rely on resource merge order between `flink-common` and the versioned module
- do not ship duplicated factory entries for the same identifier in the final runtime artifact

### Phase 5: Update tests and workflows

Update:

- `.github/workflows/flink-integration-test-action.yml`
- `.github/workflows/backend-integration-test-action.yml`
- any other workflow or script that hardcodes `:flink-connector:flink`

Recommended workflow strategy:

- run unit tests for all three Flink variants
- run runtime jar dependency tests for all three runtime variants
- run Flink integration tests for all three variants, or at minimum add a matrix input that can target each variant explicitly

### Phase 6: Update docs and build instructions

Update all Flink docs that currently hardcode `1.18`, including at least:

- `docs/flink-connector/flink-connector.md`
- `docs/flink-connector/flink-catalog-hive.md`
- `docs/flink-connector/flink-catalog-iceberg.md`
- `docs/flink-connector/flink-catalog-jdbc.md`
- `docs/flink-connector/flink-catalog-paimon.md`
- `docs/how-to-build.md`
- `docs/index.md`

Docs should:

- state the supported Flink matrix explicitly
- show the correct runtime jar name pattern per Flink minor
- mention provider-side dependency differences where users need extra jars

## Testing Plan

### Unit tests

Run for each variant:

```bash
./gradlew :flink-connector:flink-common:test -PskipITs -PskipDockerTests=false
./gradlew :flink-connector:flink-1.18:test -PskipITs -PskipDockerTests=false
./gradlew :flink-connector:flink-1.19:test -PskipITs -PskipDockerTests=false
./gradlew :flink-connector:flink-1.20:test -PskipITs -PskipDockerTests=false
```

### `flink-common` baseline test scope

`flink-common:test` runs only against the Flink `1.18` baseline dependency graph.

This means:

- it validates shared logic against the lowest supported API surface
- it does not by itself prove binary compatibility on `1.19` or `1.20`
- any regression test that depends on `1.19` or `1.20` linkage should live in the corresponding versioned module
- versioned module test runs are required to validate the shared bytecode under `1.19` and `1.20` classpaths

### Versioned provider runtime linkage validation

Compilation alone is not enough for `1.19` and `1.20`.

For each versioned module, validation should include at least one provider-level runtime linkage check that proves shared wrappers can be loaded and used on that minor's classpath.

Minimum expectation:

- load or instantiate the provider factory and wrapper path for Hive, Iceberg, JDBC, and Paimon on the versioned module classpath
- fail the version lane if the run hits `NoSuchMethodError`, `NoClassDefFoundError`, `ClassNotFoundException`, or equivalent constructor/method linkage errors
- treat real-environment smoke coverage as the stronger confirmation, but still keep a smaller versioned-module linkage check in the repository test lane where practical

### Runtime jar tests

Run for each runtime variant:

```bash
./gradlew :flink-connector:flink-runtime-1.18:test
./gradlew :flink-connector:flink-runtime-1.19:test
./gradlew :flink-connector:flink-runtime-1.20:test
```

### Integration tests

Run the existing integration suite against each variant in both existing modes:

- `-PtestMode=embedded`
- `-PtestMode=deploy`

Minimum required coverage:

- Hive catalog
- JDBC catalog
- Paimon catalog
- Iceberg catalog
- catalog store bootstrap path
- generic Hive table flow

### Real Flink environment validation

Repository-local integration tests are necessary but not sufficient.

For this delivery, the real-environment lane is a simple smoke-validation lane. In this document, `smoke` means a minimal real-distribution end-to-end check that proves the connector can load, bootstrap, and complete a basic documented catalog flow. It should prove that the versioned runtime jar can work in a real Flink distribution with the basic documented flow, but it does not need to cover the full authentication matrix.

For every supported Flink minor, the connector must also be validated in a real Flink runtime environment to prove that:

- the versioned runtime jar can be loaded by that Flink distribution
- ServiceLoader-based factory discovery works with the real classloader layout
- SQL examples from the public documentation actually work against that Flink version
- Gravitino-managed catalogs can be used end-to-end from the real Flink client/session environment

Current-scope rule:

- require simple authentication mode only
- do not require OAuth2 validation
- do not require Kerberos validation

This validation is required for:

- Flink `1.18.x`
- Flink `1.19.x`
- Flink `1.20.x`

### Real environment validation modes

At minimum, each supported Flink minor should be validated with one simple real-distribution smoke path. The preferred options are:

1. SQL/TableEnvironment mode

- start a real Flink distribution of the target version
- place the version-matched Gravitino runtime jar and required provider jars in the Flink classpath
- create a real `TableEnvironment` or SQL client session
- verify catalog-store bootstrap and catalog operations

2. SQL client mode

- launch the Flink SQL client from the real distribution
- configure `table.catalog-store.kind=gravitino`
- execute representative SQL flows from the connector documentation

At least one of the two modes above must be automated per supported Flink minor in this delivery.

`Deployed cluster` validation is useful but optional for this delivery and can be added later as a stronger compatibility lane.

### Test content sources

The real-environment validation suite should be derived from two sources:

1. Existing integration tests under:

- `flink-connector/flink-common/src/test/java/org/apache/gravitino/flink/connector/integration/test`

2. Public Flink connector documentation under:

- `docs/flink-connector/flink-connector.md`
- `docs/flink-connector/flink-catalog-hive.md`
- `docs/flink-connector/flink-catalog-iceberg.md`
- `docs/flink-connector/flink-catalog-jdbc.md`
- `docs/flink-connector/flink-catalog-paimon.md`

The real-environment validation plan should not invent a separate product surface. It should prove that the documented usage and the current IT coverage both work on each supported Flink version.

### Required real-environment smoke scenarios

For every supported Flink minor, validate the following minimum scenario groups in a real Flink environment.

1. Catalog store bootstrap

- `table.catalog-store.kind=gravitino`
- `table.catalog-store.gravitino.gravitino.uri`
- `table.catalog-store.gravitino.gravitino.metalake`
- representative client config pass-through under `table.catalog-store.gravitino.gravitino.client.*`

2. Authentication

- simple mode

3. Hive catalog

- create/use Hive catalog managed by Gravitino
- create raw Hive table with `'connector'='hive'`
- create generic table in Hive catalog
- verify generic table round-trip behavior
- verify documented native interoperability scenarios where applicable

4. Iceberg catalog

- create/use Iceberg catalog managed by Gravitino
- create Iceberg table
- create partitioned Iceberg table
- validate Iceberg REST or Hive-backed scenarios covered by the current IT suite and docs

5. JDBC catalog

- create/use JDBC catalog managed by Gravitino
- create database/table
- insert/select through the documented SQL flow
- verify version-specific JDBC provider jars are correctly loaded

6. Paimon catalog

- create/use Paimon catalog managed by Gravitino
- validate at least one supported backend per release test lane
- cover the documented SQL flow and current IT-backed table operations

7. Common DDL and schema/table lifecycle

- create schema/database
- alter schema properties
- create table
- alter table comment
- add/drop/rename columns
- rename table
- set/reset table properties

### Mapping expectation between docs and tests

The implementation should maintain an explicit mapping between:

- documented examples
- existing Java integration tests
- real-environment validation scripts/jobs

At review time, it should be possible to answer:

- which doc example is validated by which automated job
- which current integration test behavior is also exercised on real Flink `1.18`, `1.19`, and `1.20`

### CI recommendation for real-environment validation

The near-term target is a lightweight Flink-version smoke matrix in CI.

Recommended minimum:

- run repository integration tests per Flink versioned module
- add at least one simple real-distribution smoke validation job per supported Flink minor
- keep OAuth2, Kerberos, and broader deployed-cluster validation out of the required PR lane for this delivery

Recommended matrix dimensions:

- Flink version: `1.18`, `1.19`, `1.20`
- mode: `embedded`, `deploy`, `real-distribution-smoke`

### Real-environment validation acceptance

Support for a Flink minor should not be declared complete until the corresponding simple real Flink environment smoke validation passes for that minor.

### What must be verified across all minors

- `table.catalog-store.kind=gravitino` still boots correctly
- `CREATE CATALOG` still discovers the right factory
- schema operations still translate to Gravitino correctly
- table create/load/alter/drop still translate correctly
- `TableChange` conversion still behaves the same
- runtime shadow jars still exclude SLF4J classes
- the connector works in a real Flink runtime of that minor, not only in repository-local tests

## Acceptance Criteria

The work is done when all of the following are true:

1. Flink `1.18`, `1.19`, and `1.20` each have a dedicated connector module and runtime module.
2. Shared code lives in `:flink-connector:flink-common`.
3. Each runtime module produces a correctly versioned runtime jar.
4. Existing behavior for the current `1.18` connector is preserved.
5. Unit tests pass for the common module and all supported Flink minors.
6. Integration tests can be run against all supported Flink minors.
7. Simple real Flink environment smoke validation passes for all supported Flink minors.
8. User docs no longer claim that only Flink `1.18` is supported.

## Risks and Review Focus

### Risk 1: Iceberg version drift

`1.20` cannot reuse the current global Iceberg connector version if that version is `1.6.1`.

Review focus:

- verify the chosen Iceberg version exists for all intended Flink minors
- verify the chosen version does not accidentally force Spark connector changes unless that is intentional

### Risk 2: CI time growth

Tripling the Flink matrix will increase test time.

Review focus:

- whether all three variants run on every PR
- whether full integration tests should be split into a matrix or run selectively

### Risk 3: Hidden provider-specific API drift

Even if Flink SPI is stable, provider artifacts may drift across minors.

Review focus:

- Iceberg factory construction
- Paimon factory construction
- JDBC catalog factory/runtime
- Hive catalog helper behavior

### Risk 4: Mechanical refactor regressions

Moving shared sources into `flink-common` can break:

- service resource lookup
- test resource lookup
- Gradle source set wiring

Review focus:

- resource paths
- service registration
- test reports and workflow artifact upload paths

### Risk 5: False confidence from `compileOnly`

`compileOnly` may make the module graph look clean while still leaving runtime ABI problems.

Review focus:

- whether `flink-common` directly references constructors or methods whose signatures differ by minor
- whether provider wrappers should stay in `flink-common` or move into versioned modules

### Risk 6: Provider-level runtime ABI drift across `1.18` to `1.20`

Even if the Flink SPI remains largely stable, the native provider connectors can still drift in ways that break shared wrappers at runtime.

Concrete examples include:

- `HiveCatalog` constructor or helper signature drift across Hive connector variants bundled with different Flink minors
- `org.apache.iceberg.flink.FlinkCatalog` constructor or factory drift across `iceberg-flink-runtime-1.18`, `1.19`, and `1.20`
- `JdbcCatalog` constructor drift across JDBC connector minors such as `3.2.0-1.18` and `3.3.0-1.20`

Review focus:

- whether shared provider wrappers only use methods and constructors proven stable across the supported minors
- whether provider factories can be loaded and instantiated on each versioned classpath
- whether a provider should be moved out of `flink-common` as soon as runtime linkage checks fail

## AI Guardrails

If an AI agent implements this spec, it should follow these rules:

1. Treat the current `1.18` connector as the behavior baseline.
2. Build `flink-common` first against the baseline Flink `1.18` API.
3. Add extension hooks in `flink-common` before adding any `1.20`-specific implementation.
4. Introduce version-specific Java shims only if compilation, tests, or runtime linking require them.
5. Do not use reflection unless a compile-time version shim is impossible.
6. Do not add version-specific Flink catalog classes unless compilation, tests, or runtime ABI checks require them.
7. Do not rename factory identifiers or connector property keys.
8. Keep the SPI service file content identical unless a version-specific factory class is introduced, and if one is introduced the versioned module must own the full final service list for that variant.
9. Update docs and workflows in the same change set as the code split.

## Expected Touched Areas

At minimum, expect changes in:

- `settings.gradle.kts`
- `gradle/libs.versions.toml`
- `flink-connector/`
- `.github/workflows/flink-integration-test-action.yml`
- `.github/workflows/backend-integration-test-action.yml`
- `docs/flink-connector/*.md`
- `docs/how-to-build.md`
- `docs/index.md`

## Recommended Execution Order

1. Create `flink-common`.
2. Move current shared implementation into `flink-common`.
3. Extract extension hooks for future `1.20` catalog API support.
4. Create versioned Gradle projects.
5. Make `1.18` green again with `flink-common` in place.
6. Add `1.19` and keep it common-backed if possible.
7. Add `1.20` and solve dependency-version issues first.
8. Add minor-specific shims only if compilation, tests, or runtime ABI checks prove they are necessary.
9. Update CI.
10. Update docs.
