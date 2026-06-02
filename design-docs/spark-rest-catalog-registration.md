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

# Design: Spark REST Catalog Automatic Registration

## Problem

Spark users can already connect to Gravitino with a small amount of configuration:

```text
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://127.0.0.1:8090
spark.sql.gravitino.metalake=test
```

However, when users want Spark to access Iceberg or Lance through their REST protocols, they still
need to hand-write Spark catalog configuration for each catalog:

```text
spark.sql.catalog.iceberg_prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_prod.type=rest
spark.sql.catalog.iceberg_prod.uri=http://127.0.0.1:9001/iceberg/
spark.sql.catalog.iceberg_prod.warehouse=iceberg_prod

spark.sql.catalog.lance_vectors=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance_vectors.impl=rest
spark.sql.catalog.lance_vectors.uri=http://127.0.0.1:9101/lance
spark.sql.catalog.lance_vectors.parent=lance_vectors
```

This duplicates information already managed by Gravitino and forces users to update Spark
configuration whenever Gravitino catalogs change.

## Goal

The Gravitino Spark connector should register Spark REST catalogs automatically for supported
Gravitino catalogs. The user-facing goal is simple: Gravitino remains the catalog inventory, and
Spark derives the Iceberg/Lance REST catalog configuration from that inventory.

V1 scope is Spark only.

Non-goals in V1:

1. Do not introduce a generic access-mode framework.
2. Do not introduce a dedicated Lance catalog provider.
3. Do not change the Gravitino REST API contract.

## Supported Catalogs

This design supports the following Gravitino catalog providers in V1:

| Gravitino catalog provider | Extra catalog constraints    | Default Spark registration                | REST catalog registration                                         |
|----------------------------|------------------------------|-------------------------------------------|-------------------------------------------------------------------|
| `lakehouse-iceberg`        | None                         | Existing Gravitino Spark catalog behavior | Enabled only when `spark.sql.gravitino.iceberg.enableRestAccess=true` |
| `lakehouse-generic`        | Catalog-level `format=lance` | Existing behavior                         | Enabled only when `spark.sql.gravitino.lance.enableRestAccess=true` |
| Other providers            | None                         | Existing behavior                         | Not supported in V1                                               |

For `lakehouse-generic + format=lance`, `format=lance` is interpreted as a catalog-level marker only
for Lance REST catalog registration. This does not change the existing behavior of other generic
catalogs. When the marker is present, the catalog is treated as a Lance catalog, and tables under
this catalog must be Lance tables. Generic catalogs without catalog-level `format=lance` are ignored
by Lance REST catalog registration. Because there is no Lance Gravitino Spark catalog in V1, Lance
only supports REST catalog registration.

## Spark Configuration

Iceberg REST catalog registration is disabled by default to preserve existing Gravitino Spark
connector behavior. Users enable it with:

```text
spark.sql.gravitino.iceberg.enableRestAccess=true
```

Lance REST catalog registration is disabled by default because registration needs to load Lance
Spark catalog and extension classes, and users may not have the Lance Spark runtime on the
classpath. Users explicitly enable Lance REST access with:

```text
spark.sql.gravitino.lance.enableRestAccess=true
```

When Lance REST access is enabled, the Spark connector registers REST catalogs for
`lakehouse-generic + format=lance` catalogs.

Optional REST service URI overrides:

| Configuration | Default |
|---------------|---------|
| `spark.sql.gravitino.iceberg.restUri` | Inferred from `spark.sql.gravitino.uri` |
| `spark.sql.gravitino.lance.restUri` | Inferred from `spark.sql.gravitino.uri` |

The Iceberg switch applies to all Gravitino Iceberg catalogs visible to the Spark connector in V1.
Per-catalog REST registration override is future work.

## Registration Precedence

For each Spark catalog name, the connector should register exactly one Spark catalog implementation.
When `spark.sql.gravitino.iceberg.enableRestAccess=true`, REST catalog registration replaces the
existing Gravitino Spark catalog registration for matching `lakehouse-iceberg` catalogs and keeps the
same Spark catalog name.

This is an explicit opt-in behavior change. Users who do not set
`spark.sql.gravitino.iceberg.enableRestAccess=true` keep the existing Gravitino Spark catalog
behavior.

## REST URI Resolution

Spark needs the REST service URI for REST catalog registration. The URI is resolved in this
order:

1. Use the explicit Spark-side override if configured:
   - `spark.sql.gravitino.iceberg.restUri`
   - `spark.sql.gravitino.lance.restUri`
2. Otherwise infer the endpoint from `spark.sql.gravitino.uri` using the default Gravitino auxiliary
   service layout.

Default inference reuses only the scheme and host from `spark.sql.gravitino.uri`. It replaces the
port and path with the REST service defaults:

| REST service | Default port | Default path |
|--------------|--------------|--------------|
| Gravitino Iceberg REST server | `9001` | `/iceberg/` |
| Lance REST service | `9101` | `/lance` |

For example, `spark.sql.gravitino.uri=http://gravitino.example.com:8090` infers:

```text
spark.sql.gravitino.iceberg.restUri=http://gravitino.example.com:9001/iceberg/
spark.sql.gravitino.lance.restUri=http://gravitino.example.com:9101/lance
```

When the connector infers a REST URI, it should log a warning with the inferred URI and the source
`spark.sql.gravitino.uri`. Deployments with a different host, port, path, protocol, or gateway must
configure the explicit override. Production deployments should prefer explicit `restUri`
configuration over inference.

## Iceberg Mapping

This mapping requires the Gravitino Iceberg REST server to expose catalog names that match Gravitino
catalog names. With `dynamic-config-provider`, this is the intended behavior because the REST server
loads catalog configuration from Gravitino and registers catalogs by Gravitino catalog name. With
`static-config-provider`, users must manually configure REST-server catalog names to match the
corresponding Gravitino catalog names.

For a Gravitino Iceberg catalog:

```text
name = iceberg_prod
type = RELATIONAL
provider = lakehouse-iceberg

catalog-backend = jdbc
uri = jdbc:postgresql://127.0.0.1:5432
warehouse = s3://warehouse/iceberg_prod
data-access = vended-credentials
```

The Gravitino catalog properties describe the backend used by Gravitino Iceberg REST server. For
example, `uri` may be a JDBC URL, Hive Metastore URI, or upstream Iceberg REST URI. Spark should not
use this catalog property as the Iceberg REST client URI.

Generated Spark configuration:

```text
spark.sql.catalog.iceberg_prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_prod.type=rest
spark.sql.catalog.iceberg_prod.uri=<resolved-iceberg-rest-uri>
spark.sql.catalog.iceberg_prod.warehouse=iceberg_prod
```

If the Gravitino Iceberg catalog has `data-access=vended-credentials`, Spark also generates:

```text
spark.sql.catalog.iceberg_prod.header.X-Iceberg-Access-Delegation=vended-credentials
```

Multiple Gravitino Iceberg catalogs can share the same Gravitino Iceberg REST server URI. Spark
registers one Spark catalog per Gravitino catalog and uses `warehouse=<gravitino-catalog-name>` to
select the target catalog inside the Iceberg REST server:

```text
spark.sql.catalog.iceberg_prod.uri=http://127.0.0.1:9001/iceberg/
spark.sql.catalog.iceberg_prod.warehouse=iceberg_prod

spark.sql.catalog.iceberg_audit.uri=http://127.0.0.1:9001/iceberg/
spark.sql.catalog.iceberg_audit.warehouse=iceberg_audit
```

The recommended Iceberg REST server configuration is `dynamic-config-provider`, where the REST
server loads Iceberg catalog configurations from Gravitino and registers them by Gravitino catalog
name. `static-config-provider` can also work, but users must configure each REST-server catalog
using the same name as the corresponding Gravitino catalog; otherwise
`warehouse=<gravitino-catalog-name>` will not resolve to the intended backend catalog. Spark cannot
detect this mismatch reliably, so incorrect REST-server catalog naming may route requests to the
wrong catalog.

## Lance Mapping

For a Gravitino generic catalog used as a Lance catalog:

```text
name = lance_vectors
type = RELATIONAL
provider = lakehouse-generic

format = lance
location = s3://warehouse/lance_vectors
```

Generated Spark configuration:

```text
spark.sql.catalog.lance_vectors=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance_vectors.impl=rest
spark.sql.catalog.lance_vectors.uri=<resolved-lance-rest-uri>
spark.sql.catalog.lance_vectors.parent=lance_vectors
```

Multiple Gravitino Lance catalogs can share the same Lance REST service URI. Spark registers one
Spark catalog per Gravitino catalog and uses `parent=<gravitino-catalog-name>` to select the target
catalog in the Lance REST Namespace:

```text
spark.sql.catalog.lance_vectors.uri=http://127.0.0.1:9101/lance
spark.sql.catalog.lance_vectors.parent=lance_vectors

spark.sql.catalog.lance_archive.uri=http://127.0.0.1:9101/lance
spark.sql.catalog.lance_archive.parent=lance_archive
```

## Spark Extensions

The Spark connector injects extensions based on the generated Spark catalog type:

```text
org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
org.lance.spark.extensions.LanceSparkSessionExtensions
```

Existing deduplication logic can be reused when users also set extensions manually.

## Credentials

Iceberg REST catalog access has two credential paths.

If the Gravitino Iceberg catalog has `data-access=vended-credentials`, Spark does not fetch storage
credentials such as AK/SK itself. Instead, Spark injects:

```text
spark.sql.catalog.<catalog>.header.X-Iceberg-Access-Delegation=vended-credentials
```

The Gravitino Iceberg REST server then performs credential vending during Iceberg REST table access.

If `data-access=vended-credentials` is not set, Spark does not inject the delegation header. In that
case, Spark still needs storage access configuration, such as AK/SK and storage-related parameters.
The Spark connector should obtain those credentials and storage parameters from the existing
Gravitino credential/catalog configuration path and translate them into the Spark REST catalog
configuration required by Iceberg. The V1 translation should cover the storage properties required
by current Gravitino Iceberg catalogs, including object store credentials, endpoint, region, path
style access, and other storage-specific options that the Iceberg Spark REST catalog must receive
to read and write table data without credential vending.

The V1 S3-compatible storage mapping is:

| Gravitino credential or storage information | Spark Iceberg REST catalog configuration |
|---------------------------------------------|------------------------------------------|
| Access key ID | `spark.sql.catalog.<catalog>.s3.access-key-id` |
| Secret access key | `spark.sql.catalog.<catalog>.s3.secret-access-key` |
| Session token, if present | `spark.sql.catalog.<catalog>.s3.session-token` |
| Endpoint | `spark.sql.catalog.<catalog>.s3.endpoint` |
| Region | `spark.sql.catalog.<catalog>.s3.region` |
| Path-style access flag | `spark.sql.catalog.<catalog>.s3.path-style-access` |

Other object store or file system options should follow the same rule: translate the existing
Gravitino credential/catalog information into the corresponding Iceberg Spark catalog property for
the generated Spark catalog name.

Lance REST catalog registration has no additional credential parameter in V1.

## Governance

Spark REST catalog access talks to the Gravitino-managed Iceberg or Lance REST service after
registration. In this design, REST catalog access does not bypass Gravitino governance as long as
those REST services enforce Gravitino authorization and audit policy. The implementation should keep
the REST service as the governance enforcement point instead of routing Spark directly to unmanaged
backend catalogs.

## Limitations and Future Work

1. V1 is Spark only.
2. Iceberg REST catalog registration is controlled by
   `spark.sql.gravitino.iceberg.enableRestAccess` and applies to all Gravitino Iceberg catalogs
   visible to the Spark connector. Per-catalog override is future work.
3. Lance REST catalog registration requires `spark.sql.gravitino.lance.enableRestAccess=true` and
   `lakehouse-generic + format=lance`.
4. V1 supports `lakehouse-iceberg` and `lakehouse-generic + format=lance` only.
5. A dedicated `lakehouse-lance` provider can be discussed after the
   `lakehouse-generic + format=lance` path is validated.
