---
title: "Design: Engine-native Catalog Access Mode for Gravitino Connectors"
slug: /unified-engine-access
keywords:
  - unified engine access
  - spark connector
  - lance
  - iceberg
  - engine-access-mode
  - native catalog
license: "This software is licensed under the Apache License version 2."
---

## Background

Gravitino can manage multiple lakehouse catalogs and lets compute engines access the underlying table
data in various ways. For example, Spark can access some catalogs through the Gravitino Spark
connector, or access Iceberg/Lance tables directly through the Iceberg REST catalog or Lance REST
Namespace.

In mixed Iceberg-and-Lance query scenarios, the Spark side still requires users to maintain several
sets of configuration manually:

```text
spark.sql.gravitino.uri=http://127.0.0.1:8090
spark.sql.gravitino.metalake=test

spark.sql.catalog.iceberg_rest=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_rest.type=rest
spark.sql.catalog.iceberg_rest.uri=http://127.0.0.1:9001/iceberg/

spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance.impl=rest
spark.sql.catalog.lance.uri=http://127.0.0.1:9101/lance
spark.sql.catalog.lance.parent=lance_catalog
```

This creates several problems:

1. Users must understand Gravitino catalogs, the Iceberg REST catalog, the Lance REST Namespace,
   and the catalog configuration of each engine simultaneously.
2. Every time a Gravitino catalog is added or modified, the configuration on the Spark, Flink,
   Trino, and other engine sides must be updated in sync.
3. Each engine independently duplicates the translation work from catalog properties to engine
   catalog configuration.
4. The value of Gravitino as a unified metadata entry point is diminished.

This design takes a lightweight approach: no new discovery REST API is introduced; the engine side
declares the access strategy per catalog provider, and native connector configuration is
automatically derived from the catalog's existing properties by each engine connector.

## Goals

1. Users only need to configure the Gravitino server address and metalake.
2. Spark can automatically discover and register Iceberg catalogs and Lance native catalogs.
3. The same semantics can be extended to Flink, Trino, Doris, Daft, and other engines.
4. Support controlling the access mode per catalog provider: use the Gravitino connector/API or
   the engine's native connector.
5. The access mode is configured on the engine side per catalog provider; native connector
   configuration reuses the catalog's existing properties.
6. In the first phase, no new discovery REST API is introduced; the existing
   `listCatalogsInfo()` / `loadCatalog()` calls are reused.

## Non-Goals

1. In the first phase, full Lance support across all engines is not required simultaneously.
2. Long-lived static credentials from catalog properties should not be transparently forwarded to
   engines; that path is only a compatibility or testing fallback.

## Core Design

A new engine-side, provider-level access mode configuration is introduced:

```text
spark.sql.gravitino.<provider>.engine-access-mode = auto | gravitino | native
```

The semantics are:

| Value       | Meaning |
|-------------|---------|
| `auto`      | Default. The Gravitino connector automatically selects the access method based on whether the current engine has a Gravitino connector for the given provider. If a Gravitino connector exists for the provider, it falls back to `gravitino`; otherwise it falls back to `native`. |
| `gravitino` | Force the use of the Gravitino connector/API. |
| `native`    | Force the use of the engine's native connector/catalog, for example Spark Iceberg `SparkCatalog`, Spark Lance `LanceNamespaceSparkCatalog`, Trino/Doris native Iceberg catalog, or Lance REST Namespace. |

### Access Mode Selection

The engine connector reads the corresponding configuration based on the catalog provider, for example:

```text
spark.sql.gravitino.lakehouse-iceberg.engine-access-mode=native
spark.sql.gravitino.lakehouse-lance.engine-access-mode=native
```

If no provider-level configuration is set, `auto` is used.

| Catalog       | `auto` rule |
|---------------|-------------|
| Iceberg       | Defaults to `gravitino`, preserving the existing Gravitino Spark connector behavior. Switches to an Iceberg native catalog only when `spark.sql.gravitino.lakehouse-iceberg.engine-access-mode=native` is set explicitly. |
| Lance         | Defaults to `native`, because there is currently no Lance Gravitino connector. If the conversion to a Lance native catalog fails, an `UnsupportedException` is thrown immediately. |
| Other catalogs | Preserves the existing Gravitino connector behavior. |

No new native-specific catalog properties are added. The engine connector derives the native
configuration from the existing `provider` and catalog properties, for example: Iceberg uses
`catalog-backend`, `uri`, `warehouse`, and `data-access`; Lance uses `namespace-backend`, `uri`,
and `location`.

### DDL Operations and Metadata Consistency

When `engine-access-mode=native`, all DDL and DML operations are passed directly to the
engine-native connector. This means `CREATE TABLE`, `DROP TABLE`, and similar operations bypass
Gravitino and do not update Gravitino's metadata view, which can cause divergence between
Gravitino's catalog state and the actual data state.

**Users should be aware of this risk.** Whether to allow a given provider to use native access is
controlled by the user on the engine side. Gravitino does not enforce in the first phase that only
REST backends are eligible for native access. Enabling native mode is intentional and explicit;
operators should ensure it does not conflict with their governance, auditing, or access-control
requirements.

:::note
Gravitino does not enforce that only REST backends support native access. Whether a specific
backend makes sense for native access is determined by the user's configuration.
:::

## Catalog Examples

### Iceberg

```text
name = iceberg
type = RELATIONAL
provider = lakehouse-iceberg

catalog-backend = rest
uri = http://127.0.0.1:9001/iceberg/
warehouse = s3://contacts/raw/iceberg
data-access = vended-credentials
```

Notes:

1. `catalog-backend=rest` indicates the Iceberg catalog backend is an Iceberg REST catalog.
2. `uri` is the Iceberg REST endpoint; it is also used by the Spark connector to generate the
   Iceberg Spark catalog `uri`.
3. `data-access=vended-credentials` carries the existing Iceberg REST semantics and is used by the
   engine connector to automatically inject the Iceberg REST credential delegation header.

### Lance

Whether to introduce a new dedicated `lakehouse-lance` provider or continue expressing Lance
catalogs with the existing `lakehouse-generic + format=lance` is an open question. This document
favors a dedicated `lakehouse-lance` provider because the semantics are clearer and it provides a
better foundation for Lance-specific capability declarations, property validation, and
engine-native configuration translation.

Example:

```text
name = lance_catalog
type = RELATIONAL
provider = lakehouse-lance

namespace-backend = rest
uri = http://127.0.0.1:9101/lance
location = s3://contacts/raw/lance
```

Notes:

1. `namespace-backend=rest` indicates the Lance catalog uses the Lance REST Namespace protocol.
2. `uri` is the Lance REST endpoint; it is also used by the Spark connector to generate the Lance
   Spark catalog `uri`.
3. The Lance Spark connector `parent` parameter defaults to the Gravitino catalog name.

:::note
The use of `type = RELATIONAL` for Lance catalogs is an open question. Lance tables support
columnar/vector storage semantics, which may not cover all relational SQL operations. Community
input is welcome on whether a new catalog type (e.g. `LAKEHOUSE`) or a more relaxed interpretation
of `RELATIONAL` is appropriate here.
:::

If compatibility with existing implementations is required in the first phase,
`lakehouse-generic` can be used as a compatibility path with the following convention:

```text
provider = lakehouse-generic
format = lance
namespace-backend = rest
```

In the first phase, `lakehouse-lance` is the recommended provider. `lakehouse-generic + format=lance`
is only a transitional path for compatibility with existing catalogs and is not the long-term
recommended modeling approach.

## Spark Design

Users only configure:

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://127.0.0.1:8090
spark.sql.gravitino.metalake=test
```

Optional overrides:

```text
spark.sql.gravitino.lakehouse-iceberg.engine-access-mode=native
spark.sql.gravitino.lakehouse-lance.engine-access-mode=native
spark.sql.gravitino.enableIcebergSupport=true
spark.sql.gravitino.enableLanceSupport=true
```

The Spark connector then automatically registers Iceberg and Lance catalogs based on the switches.
Under `auto`, Iceberg uses the Gravitino catalog by default; Lance uses the native catalog.

`spark.sql.gravitino.enableLanceSupport` defaults to `false` to avoid loading Lance catalogs or
extensions when the user has not explicitly included the Lance Spark connector dependency.

### Driver Plugin Behavior

`GravitinoDriverPlugin` at startup:

1. Reads `spark.sql.gravitino.uri` and `spark.sql.gravitino.metalake`.
2. Calls the existing Gravitino client `listCatalogsInfo()`.
   - If the Gravitino server is unreachable at startup, catalog registration is skipped and
     no exception is thrown; access failures will surface when the catalog is first used.
3. For each `RELATIONAL` catalog, reads `provider` and `properties`.
4. Decides whether to process Iceberg/Lance catalogs based on
   `spark.sql.gravitino.enableIcebergSupport` and `spark.sql.gravitino.enableLanceSupport`.
5. Reads the engine-side access-mode configuration for the catalog provider; defaults to `auto` if
   not configured.
6. Decides whether to register a Gravitino catalog or a native catalog based on the final access
   mode.
7. Injects the necessary Spark SQL extensions based on the enabled support flags and the final
   registered catalog type.

### Registration Rules

#### Access Mode and Enable Flag Interaction

The following table describes the combined behavior of `enable*` flags and `engine-access-mode`:

| `enableIcebergSupport` | `engine-access-mode` | Result |
|------------------------|----------------------|--------|
| `false` (default)      | any                  | Iceberg catalog is not registered; no Iceberg extensions injected. |
| `true`                 | `auto` / `gravitino` | Existing Gravitino Spark connector behavior; Iceberg extensions injected only if already needed. |
| `true`                 | `native`             | Native Iceberg Spark catalog registered; Iceberg extensions injected. |

The same logic applies to `enableLanceSupport` / `lakehouse-lance`.

#### Iceberg native

Iceberg native is registered only when explicitly set to native:

```text
spark.sql.gravitino.enableIcebergSupport = true
provider = lakehouse-iceberg
spark.sql.gravitino.lakehouse-iceberg.engine-access-mode = native
catalog-backend = rest
```

The first phase defines only the `catalog-backend=rest` to Spark Iceberg REST catalog translation.
If the Iceberg native connector gains support for additional backends in the future, those can be
added as further translation rules under the same provider-level access mode.

Generated configuration:

```text
spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<catalog>.type=rest
spark.sql.catalog.<catalog>.uri=<uri>
spark.sql.catalog.<catalog>.header.X-Iceberg-Access-Delegation=vended-credentials
```

`header.X-Iceberg-Access-Delegation` is injected automatically only when the catalog has
`data-access=vended-credentials` set. Other credential scenarios are covered in the
[Credential Design](#credential-design) section.

#### Lance native

Lance native is registered under `auto` or when explicitly set to native:

```text
spark.sql.gravitino.enableLanceSupport = true
provider = lakehouse-lance
spark.sql.gravitino.lakehouse-lance.engine-access-mode = native or auto
namespace-backend = rest
```

Generated configuration:

```text
spark.sql.catalog.<catalog>=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.<catalog>.impl=rest
spark.sql.catalog.<catalog>.uri=<uri>
spark.sql.catalog.<catalog>.parent=<catalog>
```

The Lance Spark connector `parent` parameter defaults to the Gravitino catalog name.

### Spark Extensions

The Spark connector injects extensions based on the enable flags and the final registered catalog
type:

```text
org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
org.lance.spark.extensions.LanceSparkSessionExtensions
```

If `spark.sql.gravitino.enableIcebergSupport=false`, no Iceberg catalog is loaded and no Iceberg
extensions are injected. If `spark.sql.gravitino.enableLanceSupport=false`, no Lance catalog is
loaded and no Lance extensions are injected.

Existing deduplication logic can be reused to avoid duplicate extension registration when users
also set extensions manually.

## Multi-Engine Extensibility

This design does not expand on the concrete implementations for other engines in the first phase.
Architecturally, the provider-level `engine-access-mode` describes a universal selection semantic
for choosing between catalog-provider-to-engine-native access configurations. Each engine only
needs to perform three steps in its own Gravitino connector, catalog adapter, or helper layer:

1. Read the Gravitino catalog's `provider`, catalog properties, and engine-side provider-level
   access mode configuration.
2. Decide based on engine capability whether to use Gravitino access or engine-native access.
3. Translate catalog properties into that engine's own catalog/connector configuration.

This design can therefore be extended to Flink, Trino, Doris, Daft, and other engines. Flink and
Trino can reuse these semantics in their respective catalog adapter/connector configuration
translation layers; Doris can translate Gravitino catalog properties into Doris external catalog
configuration; Daft can translate Gravitino catalog properties into PyIceberg/Lance reader
configuration in a Python helper or session attach layer.

### Known Limitation: No Dynamic Catalog Refresh

The current design registers catalogs once at Spark session startup by calling
`listCatalogsInfo()`. Catalogs added, removed, or modified in Gravitino after the session starts
are not visible until the session is restarted. This is a known first-phase limitation and is
expected to be addressed in a future iteration.

## Credential Design

This design reuses Gravitino's existing credential capabilities. The recommended paths are:

| Scenario | Behavior |
|----------|----------|
| Iceberg REST catalog with `data-access=vended-credentials` | The Spark native Iceberg catalog automatically sets `header.X-Iceberg-Access-Delegation=vended-credentials`; the Iceberg REST server performs credential vending during table access requests. |
| `data-access` not set, or non-Iceberg-REST native access | The Gravitino connector calls the Gravitino catalog credential API to obtain a credential and translates it into the current engine connector's required configuration. |
| Credential unavailable | No credential is issued; the engine relies on permissions already present in the runtime environment. |

Reading static storage credentials from catalog properties is only a historical-compatibility or
testing mechanism, not a recommended design path. It should be phased out over time to avoid
long-lived secrets persisting in catalog properties.

Lance REST credential vending needs to be addressed in a future iteration. In Lance native access,
credentials should also be fetched preferentially via the Gravitino catalog credential API.

## Open Questions

1. Should Lance catalogs use a dedicated `lakehouse-lance` provider, or continue to be expressed
   as `lakehouse-generic + format=lance`? This document favors a dedicated provider for clearer
   semantics.
2. Should native access be restricted to REST catalog backends only? This document favors leaving
   that choice to the user, but the documentation must make clear that native access can bypass
   Gravitino's authorization, auditing, and governance systems.
3. Should DDL operations (such as `CREATE TABLE`, `DROP TABLE`) in native mode be routed through
   the native catalog or blocked to prevent Gravitino metadata divergence? This directly affects
   metadata consistency and is left open for community input.
4. Should the startup-time `listCatalogsInfo()` call do a full load of all catalogs or use lazy
   per-catalog loading? A metalake with many catalogs could affect Spark session startup time.

## Summary

This proposal introduces only engine-side, provider-level access mode configuration — for example
`spark.sql.gravitino.lakehouse-iceberg.engine-access-mode=auto|gravitino|native` — and derives
native connector configuration from the catalog's existing properties. On the Spark side,
`spark.sql.gravitino.enableIcebergSupport` and `spark.sql.gravitino.enableLanceSupport` control
whether Iceberg/Lance catalogs and the corresponding Spark extensions are loaded. Under `auto`,
Iceberg preserves the existing Gravitino connector behavior; Lance defaults to native because there
is currently no Lance Gravitino connector.

This approach is simple to implement, compatible with the existing Gravitino API, and well-suited
for validating the Spark scenario first. The same provider-level access mode semantics can later be
extended to Flink, Trino, Doris, Daft, and other engines without changing the core model.

Users should be aware that native access delegates actual operations to the engine's native
connector; DDL operations may not be reflected in Gravitino's metadata, and Gravitino's
authorization, auditing, and governance capabilities may be bypassed. Native mode should be enabled
deliberately and with full understanding of these trade-offs.
