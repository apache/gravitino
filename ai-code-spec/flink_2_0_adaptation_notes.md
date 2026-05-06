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

# Gravitino Flink Connector Flink 2.0 Adaptation Notes

| Field | Value |
| --- | --- |
| Status | Draft |
| Owner | FANNG |
| Created | 2026-03-17 |
| Target Release | 1.3.0 |
| Related Issue | [#9710](https://github.com/apache/gravitino/issues/9710) |
| Related PR | N/A |

## Purpose

This document records what changed between the Flink `1.x` lane and Flink `2.0`, why those changes matter to the Gravitino Flink connector, how the connector was adapted, and which parts were actually validated.

This is not the main multi-version implementation spec. That broader document remains `ai-code-spec/flink_multi_version_support_spec.md`. This document focuses only on the Flink `2.0` compatibility lane and the implementation/debugging findings that came out of that work.

## Scope

The Gravitino Flink connector is mainly:

- a `CatalogStore`
- a set of Flink `Catalog` wrappers
- conversion utilities between Flink table metadata and Gravitino metadata

It is not primarily a DataStream runtime integration. For that reason, the Flink `2.0` adaptation is mainly about:

- catalog/table API compatibility
- provider factory compatibility
- dependency and runtime artifact layout changes
- real Flink `2.0.x` validation

The main source areas involved are:

- `flink-connector/flink-common`
- `flink-connector/v2.0/flink`
- `flink-connector/v2.0/flink-runtime`

## High-Level Conclusion

Flink `2.0` support is not a simple dependency bump from `1.20`.

The main reasons are:

1. Flink `2.0` removes legacy catalog/table construction APIs used directly by the shared connector code.
2. Iceberg and JDBC both change their Flink-facing integration surface.
3. Real Flink `2.0` distributions expose runtime behaviors that are not fully visible in repo-local tests.

At the same time, the connector did not need a full rewrite. The chosen structure still works:

- keep shared logic in `flink-common`
- add `flink-2.0` and `flink-runtime-2.0`
- bridge the actual API and ABI breakpoints through narrow compatibility helpers

That result is important because it validates the general `flink-common + versioned modules + versioned runtime modules` architecture for future major-version work.

## What Changed In Flink 2.0

### 1. `CatalogTable.of(...)` is no longer available

In Flink `1.20`, `CatalogTable` still exposes:

- `CatalogTable.of(Schema, String, List<String>, Map<String, String>)`
- `CatalogTable.of(Schema, String, List<String>, Map<String, String>, Long)`

In Flink `2.0.1`, those static factory methods are no longer the supported path. The creation path is builder-based:

- `CatalogTable.newBuilder()`

Why this matters:

- shared connector code used `CatalogTable.of(...)` directly
- shared unit and integration tests also used `CatalogTable.of(...)`

Affected connector paths included:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalog.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/hive/FlinkGenericTableUtil.java`
- shared tests under `flink-connector/flink-common/src/test/java`

### 2. `CatalogPropertiesUtil` needs a newer compatibility path

The connector already used `CatalogPropertiesUtil`, which was better than relying on deprecated `CatalogTable.toProperties()`.

However, Flink `2.0` introduced a newer `CatalogPropertiesUtil.serializeCatalogTable(...)` path that requires an additional `SqlFactory` parameter.

Why this matters:

- the connector must serialize `ResolvedCatalogTable` for generic-table handling
- a single hard-coded method call is not sufficient across `1.x` and `2.0`

### 3. The `Catalog` surface grows beyond the classic `1.x` lane

Compared with `1.20`, the Flink `2.0` `Catalog` interface includes a broader surface such as model-related APIs.

Why this matters:

- it confirms that Flink `2.0` is a major compatibility lane, not just another minor
- it means shared wrapper code should be prepared for additive or default-method-oriented API growth

This was not the first blocker for the Gravitino connector, but it is part of the reason `2.0` should not be treated as `1.21`.

### 4. The JDBC connector layout changes in Flink 2.0

In the `1.x` lane, the connector typically works with artifacts like:

- `flink-connector-jdbc-3.3.0-1.20`

In Flink `2.0`, JDBC is split into artifacts such as:

- `flink-connector-jdbc-core-4.0.0-2.0`
- `flink-connector-jdbc-mysql-4.0.0-2.0`
- `flink-connector-jdbc-postgres-4.0.0-2.0`

Why this matters:

- class packages changed
- the connector cannot assume one old package layout
- real SQL client behavior depends on how those jars are loaded

### 5. Iceberg changes its Flink factory entry point

For the Iceberg runtime used with the `1.x` lane, `org.apache.iceberg.flink.FlinkCatalogFactory` supports:

- `createCatalog(String, Map<String, String>)`

For the runtime used with Flink `2.0`, the supported entry point is:

- `createCatalog(CatalogFactory.Context)`

Why this matters:

- the Gravitino Iceberg wrapper cannot use the same direct factory call in both lanes
- this is a provider ABI break, not just a Flink core API change

### 6. Real Flink distributions expose stricter runtime behavior

This came from real validation, not just source comparison.

Observed examples:

- Iceberg REST needs the correct base URI shape, such as `http://127.0.0.1:9001/iceberg/`
- JDBC depends on driver visibility and driver registration in the real SQL client runtime
- local Flink distributions may still need Hadoop-related jars depending on the tested path

Why this matters:

- compile success plus embedded IT success is not enough to claim real Flink `2.0` support

## How Gravitino Was Adapted

### 1. Added a dedicated Flink 2.0 module and runtime module

Relevant files:

- `settings.gradle.kts`
- `flink-connector/v2.0/flink/build.gradle.kts`
- `flink-connector/v2.0/flink-runtime/build.gradle.kts`
- `gradle/libs.versions.toml`

What changed:

- added `:flink-connector:flink-2.0`
- added `:flink-connector:flink-runtime-2.0`
- bound the `2.0` lane to explicit provider versions

Why:

- Flink `2.0` needs its own dependency and runtime artifact lane
- the shared code can stay shared only if the version binding is kept out of `flink-common`

### 2. Introduced `FlinkCatalogCompatUtils`

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/utils/FlinkCatalogCompatUtils.java`

What changed:

- if `CatalogTable.of(...)` exists, use it
- otherwise create the table through `CatalogTable.newBuilder()`
- if the legacy `CatalogPropertiesUtil.serializeCatalogTable(...)` path exists, use it
- otherwise use the newer `SqlFactory`-based path

Why:

- this is the narrowest way to keep `BaseCatalog` and `FlinkGenericTableUtil` shared
- it removes direct `1.x` assumptions from the common layer

### 3. Moved `BaseCatalog` table construction behind the compat helper

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/catalog/BaseCatalog.java`

What changed:

- `newCatalogTable(...)` became the central construction hook
- shared table conversion now calls `FlinkCatalogCompatUtils.createCatalogTable(...)`

Why:

- `BaseCatalog` is the main path that converts Gravitino table metadata back into Flink table metadata
- if this path is version-sensitive, the whole connector is version-sensitive

### 4. Updated Hive generic-table serialization and reconstruction

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/hive/FlinkGenericTableUtil.java`

What changed:

- generic-table serialization now goes through `FlinkCatalogCompatUtils.serializeCatalogTable(...)`
- generic-table reconstruction now goes through `FlinkCatalogCompatUtils.createCatalogTable(...)`

Why:

- generic tables are one of the most direct places where Flink catalog metadata shape matters
- this kept the shared Hive-side utility compatible with both the `1.x` and `2.0` lanes

### 5. Introduced `IcebergCatalogCompatUtils`

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/utils/IcebergCatalogCompatUtils.java`

What changed:

- if the old Iceberg factory API is present, call `createCatalog(String, Map<String, String>)`
- otherwise wrap the existing Flink `CatalogFactory.Context` and call `createCatalog(Context)`

Related integration points:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/iceberg/GravitinoIcebergCatalog.java`
- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/iceberg/GravitinoIcebergCatalogFactory.java`

Why:

- Iceberg changed its Flink factory ABI
- the connector needs a single shared wrapper that can bridge both styles

### 6. Introduced `JdbcCatalogCompatUtils`

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/utils/JdbcCatalogCompatUtils.java`

What changed:

- try compatible JDBC catalog factory class names across lanes
- try compatible JDBC dynamic table factory class names across lanes

Related integration point:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/jdbc/GravitinoJdbcCatalog.java`

Why:

- Flink `2.0` moves JDBC classes into the newer `core` package layout
- a single direct import path is not stable enough across lanes

Important limitation:

- this solves the connector-side ABI problem
- it does not by itself solve every real SQL client driver-loading issue in external Flink `2.0.x` environments

### 7. Hardened `CatalogStore` service discovery

Relevant file:

- `flink-connector/flink-common/src/main/java/org/apache/gravitino/flink/connector/store/GravitinoCatalogStore.java`

What changed:

- `ServiceLoader` scanning now tolerates:
  - `ServiceConfigurationError`
  - `NoClassDefFoundError`
  - `LinkageError`

Why:

- in a multi-provider, multi-version environment, some factories may be visible but not fully initializable on a given classpath
- the connector should skip optional provider failures instead of failing catalog-store discovery completely

### 8. Added Flink 2.0-specific test and runtime wiring

Relevant files:

- `flink-connector/v2.0/flink/build.gradle.kts`
- `flink-connector/v2.0/flink-runtime/build.gradle.kts`
- `flink-connector/v2.0/flink-runtime/src/test/java/org/apache/gravitino/flink/runtime/TestRuntimeJarDependencies.java`

What changed:

- reused shared tests from `flink-common`
- bound the `2.0` lane to Flink `2.0.1`
- bound provider jars to `2.0`-compatible versions
- excluded unsupported Hive-oriented tests from the current `2.0` lane
- added runtime jar assertions

Why:

- Flink `2.0` must be validated as its own lane, not as an alias of `1.20`

### 9. Fixed Iceberg REST deploy validation issues

Relevant files:

- `integration-test-common/src/test/java/org/apache/gravitino/integration/test/util/BaseIT.java`
- `integration-test-common/src/test/java/org/apache/gravitino/integration/test/util/TestBaseIT.java`
- `flink-connector/flink-common/src/test/java/org/apache/gravitino/flink/connector/integration/test/iceberg/FlinkIcebergRestCatalogIT.java`

What changed:

- explicitly enabled the auxiliary Iceberg REST service in deploy mode
- normalized wildcard hosts such as `0.0.0.0` to loopback for local test access
- waited for auxiliary service readiness
- aligned the REST SQL creation path with the actual REST backend expectation

Why:

- otherwise the deploy-mode validation failure can be mistaken for a Flink `2.0` compatibility failure
- the same fixes benefit both `1.20` and `2.0`

## Validation Summary

### Repo-level validation

The following areas were validated in the current `2.0` implementation work:

- shared compatibility-helper tests in `flink-common`
- `flink-2.0` compile and test lane
- `flink-runtime-2.0` build and runtime jar checks
- representative embedded integration tests for:
  - catalog store
  - JDBC
  - Paimon
  - Iceberg
- deploy-mode Iceberg REST integration tests

### Real external Flink 2.0.1 validation

External validation was also run against a real Flink `2.0.1` distribution.

Current result:

- `Paimon filesystem`: passed
- `Iceberg REST`: passed
- `JDBC`: not yet passed in the real external SQL client environment

The JDBC external failure currently shows up as:

- `java.sql.SQLException: No suitable driver found for jdbc:mysql://127.0.0.1:3306/...`

This was reproduced in both:

- the catalog-store path
- the direct `CREATE CATALOG` path

This means the remaining JDBC issue is not specific to `CatalogStore`. It is a real external runtime and classloader issue that still needs dedicated follow-up.

## Current Support Boundary For The Flink 2.0 Lane

Based on current implementation and validation evidence, the Flink `2.0` lane is best described as:

- validated and good candidates for initial support:
  - core catalog lane
  - Paimon
  - Iceberg REST lane
- adapted in repo-level code but still incomplete in real external validation:
  - JDBC
- not part of the current `2.0` support scope:
  - Hive lane
  - model support
  - broader Flink `2.x` feature surface outside the current connector scope

## Why This Is A Moderate Compatibility Project, Not A Rewrite

The Flink `2.0` work is not a full connector rewrite because:

- the connector architecture remains valid
- most shared Gravitino conversion logic stays reusable
- the breakpoints are concentrated in a few catalog and provider boundaries

At the same time, it is not a trivial version bump because:

- `CatalogTable.of(...)` disappeared
- `CatalogPropertiesUtil` needs a new compatibility path
- Iceberg factory entry points changed
- JDBC packaging changed
- real `2.0` runtime validation exposed environment-sensitive behavior

In practice, the work is a focused compatibility project:

- one new version lane
- a few targeted compatibility helpers
- provider-specific factory bridging
- test and runtime validation updates

## Summary

The Flink `2.0` adaptation required concrete work in three places:

1. Flink catalog/table API compatibility
2. provider factory and packaging compatibility
3. deploy and external runtime validation

The key result is positive: the chosen `flink-common + versioned module + thin compatibility helper` architecture is sufficient for Flink `2.0`.

The remaining JDBC external runtime issue is important, but it is now isolated. It should be treated as a focused follow-up in the `2.0` lane, not as evidence that the overall adaptation approach is wrong.
