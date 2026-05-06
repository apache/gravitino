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

# Gravitino Flink Connector Versioned Architecture Spec

| Field | Value |
| --- | --- |
| Status | Proposal |
| Owner | FANNG |
| Created | 2026-03-25 |
| Target Release | 1.3.0 |
| Related Issue | [#9710](https://github.com/apache/gravitino/issues/9710) |
| Related PR | N/A |

## Goal

Define the target architecture for versioned Gravitino Flink connector support and the staged
delivery plan for Flink `1.18`, `1.19`, and `1.20`.

The final output of this work should be:

- one shared `flink-common` module for stable base logic
- one connector module per supported Flink minor
- one runtime shadow jar per supported Flink minor
- one set of version-specific catalog and factory entry classes per supported Flink minor
- one set of version-specific integration-test entry classes per supported Flink minor
- typed version compatibility hooks without reflection-based provider dispatch

## Non-Goals

- Supporting Flink `1.17`
- Supporting Flink `2.x` in this delivery
- Producing one universal jar for all Flink minors
- Adding new connector features unrelated to version compatibility
- Reworking catalog semantics beyond what is needed for versioned support
- Depending on a smoke helper shell script as part of the long-term validation model

Note: Flink `2.0` support remains out of scope for the current staged delivery. The architecture in
this spec is intentionally `2.x`-ready, but the actual `2.0` lane should be delivered as a
separate follow-up.

## Current Connector Model

The Gravitino Flink connector is not a generic table runtime connector. It is primarily:

- a Flink `CatalogStore`
- a set of Flink `Catalog` wrappers around Gravitino metadata
- a bridge from Gravitino metadata operations to native Flink ecosystem catalogs

The main interaction points are:

1. Catalog store bootstrapping

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/store/GravitinoCatalogStoreFactory.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/store/GravitinoCatalogStore.java`

2. Shared base catalog SPI implementation

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalog.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalogFactory.java`

3. Provider-specific wrappers

- Hive
- Iceberg
- JDBC
- Paimon

4. Flink-to-Gravitino conversion helpers

- `CatalogPropertiesConverter`
- `SchemaAndTablePropertiesConverter`
- `PartitionConverter`
- `TypeUtils`
- `FlinkGenericTableUtil`

## Design Decision

Follow the Spark connector model more directly, but adapted to Flink SPI usage:

- keep shared base behavior in `flink-common`
- move version selection into version-specific catalog and factory entry classes
- give each supported Flink minor its own SPI service descriptor
- give each supported Flink minor its own integration-test entry classes
- keep compatibility logic typed and version-scoped instead of using shared reflection helpers

This means Flink version support is modeled as:

- shared base implementation
- version-specific entry layers
- version-specific compatibility implementations
- version-specific integration-test entry classes

## Final Architecture

### Shared module

`flink-common` contains only stable shared logic:

- shared base catalog implementations
- shared base factory implementations
- shared property, type, and partition conversion logic
- shared catalog-store implementation
- shared integration-test base classes
- shared compatibility interfaces and baseline helpers

Representative classes in `flink-common`:

- `BaseCatalog`
- `BaseCatalogFactory`
- `GravitinoCatalogStore`
- `CatalogPropertiesConverter`
- `SchemaAndTablePropertiesConverter`
- `PartitionConverter`
- `TypeUtils`
- `CatalogCompat`
- `DefaultCatalogCompat`

### Versioned connector modules

Each Flink minor owns its own connector entry module:

- `:flink-connector:flink-1.18`
- `:flink-connector:flink-1.19`
- `:flink-connector:flink-1.20`

Each version module contains:

- version-specific catalog entry classes
- version-specific factory entry classes
- version-specific compatibility implementation
- version-specific service descriptor
- version-specific integration-test entry classes

Examples:

- `GravitinoHiveCatalogFlink118`
- `GravitinoIcebergCatalogFlink118`
- `GravitinoJdbcCatalogFlink118`
- `GravitinoPaimonCatalogFlink118`
- `CatalogCompatFlink118`

The same pattern applies to `1.19` and `1.20`.

### Versioned runtime modules

Each supported Flink minor owns its own runtime jar:

- `:flink-connector:flink-runtime-1.18`
- `:flink-connector:flink-runtime-1.19`
- `:flink-connector:flink-runtime-1.20`

Each runtime module:

- depends on the matching versioned connector module
- builds a shadow jar for that Flink minor
- merges service descriptors from common and versioned modules
- validates runtime-jar contents in dedicated runtime tests

## Compatibility Model

### Compatibility must be typed and versioned

Compatibility is expressed through version-specific typed classes, not shared reflection-heavy
utilities.

Rules:

- version-specific provider behavior belongs in version-specific catalog and factory classes
- version-specific Flink API adaptation belongs in version-specific `CatalogCompat`
  implementations
- shared compatibility interfaces may live in `flink-common`
- reflection should not be the default compatibility mechanism

### Recommended boundary

Shared:

- `CatalogCompat` interface
- `DefaultCatalogCompat` for the lowest supported baseline if useful

Versioned:

- `CatalogCompatFlink118`
- `CatalogCompatFlink119`
- `CatalogCompatFlink120`

`BaseCatalog` should call a version hook such as `catalogCompat()` instead of deciding version
behavior itself.

### Provider compatibility rule

Provider differences should be expressed in version-specific provider classes whenever possible.

Examples:

- JDBC factory selection for Flink `1.19` and `1.20`
- Iceberg factory path differences
- Hive version-specific validation behavior
- version-specific test expectation overrides

Do not centralize provider version selection in one shared compatibility utility.

## Project Layout

```text
flink-connector/
  flink-common/
    build.gradle.kts
    src/main/java/...
    src/main/resources/...
    src/test/java/...
    src/test/resources/...

  v1.18/
    flink/
      build.gradle.kts
      src/main/java/...
      src/main/resources/...
      src/test/java/...
    flink-runtime/
      build.gradle.kts
      src/test/java/...

  v1.19/
    flink/
      build.gradle.kts
      src/main/java/...
      src/main/resources/...
      src/test/java/...
    flink-runtime/
      build.gradle.kts
      src/test/java/...

  v1.20/
    flink/
      build.gradle.kts
      src/main/java/...
      src/main/resources/...
      src/test/java/...
    flink-runtime/
      build.gradle.kts
      src/test/java/...
```

## Gradle Project Naming

Under the existing Scala `2.12` guard, the target project set is:

- `:flink-connector:flink-common`
- `:flink-connector:flink-1.18`
- `:flink-connector:flink-runtime-1.18`
- `:flink-connector:flink-1.19`
- `:flink-connector:flink-runtime-1.19`
- `:flink-connector:flink-1.20`
- `:flink-connector:flink-runtime-1.20`

The old single-version projects:

- `:flink-connector:flink`
- `:flink-connector:flink-runtime`

should be removed once the staged migration is complete.

## Staged Delivery Plan

### Phase 1: framework split plus Flink 1.18 baseline

This phase establishes the architecture and one supported baseline lane.

Scope:

- introduce `flink-common`
- introduce `flink-1.18`
- introduce `flink-runtime-1.18`
- move shared code from the old `flink` module into `flink-common`
- add version-specific `1.18` catalog, factory, compat, and integration-test entry classes
- move `1.18` CI to run from `:flink-connector:flink-1.18:test`
- ensure the `1.18` runtime jar merges service descriptors correctly
- remove dependence on a smoke helper script

Outcome:

- the framework is reviewable with one concrete Flink baseline
- later Flink minors can be added as follow-up PRs instead of in the same refactor

### Phase 2: add Flink 1.19 support

Scope:

- add `flink-1.19`
- add `flink-runtime-1.19`
- add `CatalogCompatFlink119`
- add `1.19` version-specific catalog, factory, and integration-test classes
- absorb `1.19` provider differences into `1.19` version classes
- extend CI and runtime validation to `1.19`

### Phase 3: add Flink 1.20 support

Scope:

- add `flink-1.20`
- add `flink-runtime-1.20`
- add `CatalogCompatFlink120`
- add `1.20` version-specific catalog, factory, and integration-test classes
- absorb `1.20` provider and API differences into `1.20` version classes
- extend CI and runtime validation to `1.20`

## Testing Strategy

### Shared tests

`flink-common` should contain:

- shared unit tests for common logic
- shared integration-test base classes
- shared test resources

It should not be treated as the final execution target for version validation.

### Versioned tests

Each versioned connector module should contain:

- version-specific integration-test entry classes
- version-specific expectation overrides where needed
- real execution against that Flink minor's classpath

Examples:

- `GravitinoCatalogManagerIT118`
- `FlinkHiveCatalogIT118`
- `FlinkHiveKerberosClientIT118`
- `FlinkIcebergRestCatalogIT118`
- `FlinkJdbcMysqlCatalogIT118`
- `FlinkPaimonLocalFileSystemBackendIT118`

The same pattern should be used for later versions.

### Runtime tests

Each runtime module should validate:

- runtime-jar dependency boundaries
- service descriptor merge correctness
- presence of common and version-specific factories in the final shadow jar

### CI rule

CI should run concrete version module tasks directly.

Examples:

- `:flink-connector:flink-1.18:test`
- later `:flink-connector:flink-1.19:test`
- later `:flink-connector:flink-1.20:test`

Do not rely on a dedicated smoke helper shell script for version validation.

## Runtime Packaging Rules

Runtime shadow jars must merge factory service descriptors from:

- `flink-common`
- the matching versioned connector module

The final runtime jar for a supported Flink minor must expose:

- `GravitinoCatalogStoreFactory`
- the version-specific catalog factories for that minor

This must be validated in runtime-jar tests.

## Provider Strategy

### Hive

- shared Hive base logic stays in `flink-common`
- version-specific entry classes own minor-specific behavior
- version-specific test classes may override assertions where validation behavior differs

### Iceberg

- shared conversion and base behavior stays in `flink-common`
- version-specific catalog and factory classes own version-specific create paths
- REST and deploy-path validation should run through versioned integration-test classes

### JDBC

- JDBC minor-version differences are expected
- JDBC provider selection and factory path differences should live in versioned classes
- do not rely on one shared JDBC compatibility utility for version behavior

### Paimon

- expected to stay relatively thin
- still gets versioned catalog, factory, and integration-test entry classes for consistency

## Documentation Strategy

Implementation-facing documentation should describe:

- the final versioned architecture
- the staged delivery plan
- the rule that each supported Flink minor owns its own entry classes and integration tests

Release-facing user docs should describe only completed support lanes.

This means:

- Phase 1 release and docs should describe Flink `1.18` support on the new framework
- Flink `1.19` and `1.20` should only be added to user-facing support documentation after their
  phases are completed

## Future Flink 2.0 Lane

Flink `2.0` remains out of scope for the current staged delivery.

However, this architecture is intentionally `2.x`-ready:

- add `v2.0/flink`
- add `v2.0/flink-runtime`
- add `CatalogCompatFlink20`
- add `Flink20` versioned catalog, factory, and integration-test entry classes

That follow-up should be treated as a new major compatibility lane, not as a small extension of
the `1.x` minor series.
