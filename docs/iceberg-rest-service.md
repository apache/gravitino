---
title: "Iceberg REST Catalog Service"
slug: "/iceberg-rest-service"
keywords:
  - Iceberg REST catalog
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Iceberg REST server implements the [Apache Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
Access the Iceberg REST endpoint at `http://$ip:$port/iceberg/`.

The Iceberg REST server and the main Gravitino server expose different interfaces and manage different table types:

| Item               | Gravitino Iceberg REST server                                                                            | Gravitino server                                                                                     |
|--------------------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| Interfaces         | [Iceberg REST API spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) | [Gravitino unified interfaces](https://gravitino.apache.org/docs/latest/api/rest/gravitino-rest-api) |
| Managed table type | Iceberg table only                                                                                       | JDBC, Hive, Iceberg, Hudi, Paimon, etc                                                               |

### Capabilities

The Iceberg REST server provides:

- Apache Iceberg 1.11 REST API support for most namespace, table, and view operations.
- Hierarchical namespaces.
- Hive, JDBC, and REST catalog backends.
- Credential vending for S3, GCS, OSS, and ADLS.
- S3, HDFS, OSS, GCS, ADLS, and custom storage support.
- Event listeners and audit logging.
- OAuth2 and HTTPS.
- Access control in auxiliary mode.
- Pluggable metrics storage.
- Table metadata and scan plan caches.

The following Iceberg REST API features are not implemented:

- Multi-table transactions.
- View registration.

## Deployment

### Deployment Modes

Choose one of these deployment modes:

| Mode              | Package                               | Classpath                  | Access control |
|-------------------|---------------------------------------|----------------------------|----------------|
| Standalone server | Gravitino Iceberg REST server package | `libs`                     | No             |
| Standalone server | Gravitino server package              | `iceberg-rest-server/libs` | No             |
| Auxiliary service | Gravitino server package              | `iceberg-rest-server/libs` | Yes            |

### Package Build

For instructions on building and installing the Gravitino server package, see [Build Gravitino](./how-to-build.md) and [Install Gravitino](./how-to-install.md).

Build the standalone Iceberg REST server package:

```shell
./gradlew compileIcebergRESTServer -x test
```

Create the compressed package in the distribution directory:

```shell
./gradlew assembleIcebergRESTServer -x test
```

### Package Layout

The standalone Iceberg REST server package has the following layout:

```text
|── ...
└── distribution/gravitino-iceberg-rest-server
    |── bin/
    |   └── gravitino-iceberg-rest-server.sh    # Gravitino Iceberg REST server Launching scripts.
    |── conf/                                   # All configurations for Gravitino Iceberg REST server.
    |   ├── gravitino-iceberg-rest-server.conf  # Gravitino Iceberg REST server configuration.
    |   ├── gravitino-env.sh                    # Environment variables, etc., JAVA_HOME, GRAVITINO_HOME, and more.
    |   └── log4j2.properties                   # log4j configuration for the Gravitino Iceberg REST server.
    |   └── hdfs-site.xml & core-site.xml       # HDFS configuration files.
    |── libs/                                   # Gravitino Iceberg REST server dependencies libraries.
    |── logs/                                   # Gravitino Iceberg REST server logs. Automatically created after the server starts.
```

## Configuration

Standalone and auxiliary deployments use different configuration files:

- Standalone server: `gravitino-iceberg-rest-server.conf`.
- Auxiliary service: `gravitino.conf`.

Both files use the same Iceberg REST configuration items.
The `gravitino.auxService.iceberg-rest.` prefix has been deprecated since `0.6.0-incubating`.
If both `gravitino.auxService.iceberg-rest.key` and `gravitino.iceberg-rest.key` are present, `gravitino.iceberg-rest.key` takes precedence.
The following sections use the `gravitino.iceberg-rest.` prefix.

The server-level `gravitino.fetchFile.blockUnsafeRemoteUri` configuration controls whether remote files such as Kerberos keytabs may resolve to unsafe addresses. It defaults to `true`. Configure it in `gravitino-iceberg-rest-server.conf` for standalone mode or `gravitino.conf` for auxiliary mode.

### Service Configuration

#### Auxiliary Service

| Configuration item                 | Description                                                                                                                                                                                                                            | Default value | Required | Since Version |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.names`       | The auxiliary service name of the Gravitino Iceberg REST catalog service. Use **`iceberg-rest`**.                                                                                                                                      | (none)        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.classpath` | The classpath of the Gravitino Iceberg REST catalog service; includes the directory containing jars and configuration. It supports both absolute and relative paths, for example, `iceberg-rest-server/libs, iceberg-rest-server/conf` | (none)        | Yes      | 0.2.0         |

These settings apply only to `gravitino.conf`.
Do not add them to the standalone server configuration.

#### HTTP Server

| Configuration item                               | Description                                                                                                                                                                                                                                          | Default value                                                                | Required | Since Version |
|--------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|---------------|
| `gravitino.iceberg-rest.host`                    | The host of the Gravitino Iceberg REST catalog service.                                                                                                                                                                                              | `0.0.0.0`                                                                    | No       | 0.2.0         |
| `gravitino.iceberg-rest.httpPort`                | The port of the Gravitino Iceberg REST catalog service.                                                                                                                                                                                              | `9001`                                                                       | No       | 0.2.0         |
| `gravitino.iceberg-rest.minThreads`              | The minimum number of threads in the thread pool used by the Jetty web server. `minThreads` is 8 if the value is less than 8.                                                                                                                        | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0         |
| `gravitino.iceberg-rest.maxThreads`              | The maximum number of threads in the thread pool used by the Jetty web server. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be greater than or equal to `minThreads`.                                                        | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.2.0         |
| `gravitino.iceberg-rest.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by Gravitino Iceberg REST catalog service.                                                                                                                                                             | `100`                                                                        | No       | 0.2.0         |
| `gravitino.iceberg-rest.stopTimeout`             | The amount of time in ms for the Gravitino Iceberg REST catalog service to stop gracefully. For more information, see `org.eclipse.jetty.server.Server#setStopTimeout`.                                                                              | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.iceberg-rest.idleTimeout`             | The timeout in ms of idle connections.                                                                                                                                                                                                               | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.iceberg-rest.requestHeaderSize`       | The maximum size of an HTTP request.                                                                                                                                                                                                                 | `131072`                                                                     | No       | 0.2.0         |
| `gravitino.iceberg-rest.responseHeaderSize`      | The maximum size of an HTTP response.                                                                                                                                                                                                                | `131072`                                                                     | No       | 0.2.0         |
| `gravitino.iceberg-rest.customFilters`           | Comma-separated list of filter class names to apply to the APIs.                                                                                                                                                                                     | (none)                                                                       | No       | 0.4.0         |

The filter in `customFilters` should be a standard javax servlet filter.
Specify filter parameters by setting configuration entries in the style `gravitino.iceberg-rest.<class name of filter>.param.<param name>=<value>`.

#### Asynchronous Table Purge

By default, dropping a table with `purgeRequested=true` is synchronous: the catalog entry and the table files are removed before the `DELETE` returns.

When the Iceberg REST service runs inside Gravitino (as an auxiliary service), a client can instead request asynchronous purge by adding the header `X-Gravitino-Async-Purge: true` to `DELETE ...?purgeRequested=true`. The drop then returns `204 No Content` once the table is removed from the catalog, and the files are deleted in the background. The table is gone from `LIST` immediately, but recreating it (`createTable` / `registerTable` with the same name) returns `409 Conflict` until the file cleanup finishes.

The header name is case-insensitive (per the HTTP standard), but its value must be exactly `true`. Any other value, or no header, uses the synchronous default, so standard Iceberg clients are unaffected. Asynchronous purge is only available in auxiliary mode; in standalone mode the header is ignored.

The settings below tune the background workers and are optional.

| Configuration item                                            | Description                                                                                          | Default value | Required | Since Version |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.async-cleanup.worker-threads`         | Worker pool size per server. Each worker claims and runs cleanup jobs from the shared backend table. | `2`           | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.delete-threads`         | Server-wide file-delete pool size shared by cleanup jobs.                                            | `4`           | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.delete-batch-size`      | Number of files per bulk-delete batch.                                                               | `1000`        | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.poll-interval-secs`     | Worker polling interval in seconds. This also controls retry pacing for pending jobs.                | `5`           | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.heartbeat-timeout-secs` | Age in seconds after which a running job with no fresh heartbeat can be reclaimed by another worker. | `300`         | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.max-attempts`           | Number of failed attempts before a cleanup job is marked `FAILED`.                                   | `5`           | No       | 1.3.0         |
| `gravitino.iceberg-rest.async-cleanup.retention-hours`        | Retention time for terminal `SUCCEEDED` or `FAILED` cleanup rows before pruning.                     | `720`         | No       | 1.3.0         |

### Catalog Backend Configuration

:::info
The Gravitino Iceberg REST catalog service uses the memory catalog backend by default. Specify a Hive, JDBC or REST catalog backend for production environment.
:::

#### Hive Backend Configuration

| Configuration item                                                        | Description                                                                                                                                  | Default value                                                                  | Required | Since Version |
|---------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`                                  | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`hive`** for the Hive catalog backend.                    | `memory`                                                                       | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                                              | The Hive metadata address, such as `thrift://127.0.0.1:9083`.                                                                                | (none)                                                                         | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse`                                        | The warehouse directory of the Hive catalog, such as `/user/hive/warehouse-hive/`.                                                           | (none)                                                                         | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name`                             | The catalog backend name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables. | `hive` for Hive backend, `jdbc` for JDBC backend, `memory` for memory backend  | No       | 0.5.2         |

#### JDBC Backend Configuration

| Configuration item                            | Description                                                                                                                                                                                                                                            | Default value           | Required | Since Version |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`jdbc`** for the JDBC catalog backend.                                                                                                                              | `memory`                | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                  | The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432` for Postgres, or `jdbc:mysql://127.0.0.1:3306/` for mysql.                                                                                                                     | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse`            | The warehouse directory of JDBC catalog. Set the HDFS prefix if using HDFS, such as `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc`                                                                                                                   | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name` | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables.                                                                                                                   | `jdbc` for JDBC backend | No       | 0.5.2         |
| `gravitino.iceberg-rest.jdbc-user`            | The username of the JDBC connection.                                                                                                                                                                                                                   | (none)                  | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-password`        | The password of the JDBC connection.                                                                                                                                                                                                                   | (none)                  | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-initialize`      | Whether to initialize the meta tables when creating the JDBC catalog.                                                                                                                                                                                  | `true`                  | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-driver`          | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL.                                                                                                                                               | (none)                  | Yes      | 0.3.0         |
| `gravitino.iceberg-rest.jdbc-schema-version`  | The schema version of the JDBC catalog. Defaults to `V1` to enable view support. Set to `V0` only if you need to opt out of view support. Once the underlying database is migrated to V1, this property is no longer required on subsequent restarts.  | `V1`                    | No       | 1.2.0         |
| `gravitino.iceberg-rest.jdbc.strict-mode`     | Whether the JDBC catalog runs in strict mode. Defaults to `true` so that creating a table or view in a namespace that does not exist fails with `NoSuchNamespace` (HTTP 404), matching the Iceberg REST specification. Set to `false` to restore the legacy behavior of implicitly creating the namespace.  | `true`                  | No       | 1.3.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
Download the corresponding JDBC driver to the `iceberg-rest-server/libs` directory.
If you are using multiple JDBC catalog backends, setting `jdbc-initialize` to true may not take effect for RDBMS like `Mysql`, you should create Iceberg meta tables explicitly.
:::

#### REST Backend Configuration

Use the REST backend to proxy another Iceberg REST catalog server (IRC2). The Gravitino Iceberg REST service acts as IRC1 and forwards catalog operations to IRC2.

By default, when the backend catalog is a REST catalog, IRC1 skips authorization and behaves as a proxy. IRC2 handles authorization. If you want IRC1 to keep authorization checks, set `gravitino.iceberg-rest.disable-rest-authz=false`.

| Configuration item                                                  | Description                                                                                                                                                    | Default value | Required | Since Version |
|---------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`                            | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`rest`** for the REST catalog backend.                                      | `memory`      | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                                        | The Iceberg REST catalog URI (IRC2), such as `http://127.0.0.1:9001/iceberg`.                                                                                  | (none)        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse`                                  | The catalog name in the Iceberg REST spec. Set to a specific catalog name, or leave empty to use the default catalog on IRC2.                                  | (none)        | No       | 0.2.0         |
| `gravitino.iceberg-rest.rest-client-connection-timeout-ms`          | The HTTP connection timeout in milliseconds for IRC1 requests to the REST catalog backend.                                                                     | `10000`       | No       | 1.3.0         |
| `gravitino.iceberg-rest.rest-client-socket-timeout-ms`              | The HTTP socket timeout in milliseconds for IRC1 requests to the REST catalog backend.                                                                         | `60000`       | No       | 1.3.0         |
| `gravitino.iceberg-rest.data-access`                                | Data access mode exposed to Iceberg REST clients via `/v1/config`. Supported values: `vended-credentials`, `remote-signing`.                                   | (none)        | No       | 1.3.0         |
| `gravitino.iceberg-rest.disable-rest-authz`                         | Whether IRC1 disables authorization when the target backend catalog is a REST catalog. Set to `false` if you want IRC1 to enforce authorization before proxying. | `true`        | No       | 1.3.0         |

IRC1 configuration example if IRC2 using HDFS storage:

```text
gravitino.iceberg-rest.catalog-backend = rest
gravitino.iceberg-rest.uri = http://127.0.0.1:9001/iceberg
```

IRC1 configuration example if IRC2 using S3 storage:

```text
gravitino.iceberg-rest.catalog-backend = rest
gravitino.iceberg-rest.uri = http://127.0.0.1:9001/iceberg
gravitino.iceberg-rest.s3-access-key-id = xx
gravitino.iceberg-rest.s3-secret-access-key = xx
gravitino.iceberg-rest.s3-region = xx
gravitino.iceberg-rest.credential-providers = s3-secret-key
gravitino.iceberg-rest.header.X-Iceberg-Access-Delegation = vended-credentials
```

IRC1 must also configure S3 configurations if the client side requests credential vending.

:::caution
If IRC2 does not enforce authorization, keeping `gravitino.iceberg-rest.disable-rest-authz=true` can leave operations unprotected. Set it to `false` to enforce authorization in IRC1.
:::

`data-access` is returned in `/v1/config` defaults for REST clients:

- `vended-credentials`: clients should request credential vending (`X-Iceberg-Access-Delegation: vended-credentials`).
- `remote-signing`: Gravitino doesn't support this mode yet.

#### Custom Backend Configuration

| Configuration item                            | Description                                                                                                                   | Default value | Required | Since Version    |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`custom`** for the custom catalog backend. | `memory`      | Yes      | 0.2.0            |
| `gravitino.iceberg-rest.catalog-backend-impl` | The fully-qualified class name of a custom catalog implementation, only worked if `catalog-backend` is `custom`.              | (none)        | No       | 0.7.0-incubating |

If you want to use a custom Iceberg Catalog as `catalog-backend`, you can add a corresponding jar file to the classpath and load a custom Iceberg Catalog implementation by specifying the `catalog-backend-impl` property.

### Multi-Catalog Configuration

The Gravitino Iceberg REST server supports multiple catalog backend, and you could use `catalog-config-provider` to control the behavior about how to manage catalog backend configurations.

| Configuration item                               | Description                                                                                                                                                                                                                                                                                                                                      | Default value            | Required | Since Version    |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------|------------------|
| `gravitino.iceberg-rest.catalog-config-provider` | The className of catalog configuration provider, Gravitino provides build-in `static-config-provider` and `dynamic-config-provider`, you could also develop a custom class that implements `apache.gravitino.iceberg.service.provider.IcebergConfigProvider` and add the corresponding jar file to the Iceberg REST service classpath directory. | `static-config-provider` | No       | 0.7.0-incubating |

Use `static-config-provider` to manage catalog configuration in the file, the catalog configuration is loaded when the server start up and couldn't be changed.
While `dynamic-config-provider` is used to manage catalog config through Gravitino server, you could add&delete&change the catalog configurations dynamically.

#### Static Catalog Configuration Provider

The static catalog configuration provider retrieves the catalog configuration from the configuration file of the Gravitino Iceberg REST server. You could configure the default catalog with `gravitino.iceberg-rest.<param name>=<value>`. For others, use `gravitino.iceberg-rest.catalog.<catalog name>.<param name>=<value>` to config the catalog with `catalog name`.

For instance, you could configure three different catalogs, the default catalog and `hive_backend` and `jdbc_backend` catalogs separately.

```text
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:postgresql://127.0.0.1:5432
gravitino.iceberg-rest.warehouse = hdfs://127.0.0.1:9000/user/hive/warehouse-postgresql
...
gravitino.iceberg-rest.catalog.hive_backend.catalog-backend = hive
gravitino.iceberg-rest.catalog.hive_backend.uri = thrift://127.0.0.1:9084
gravitino.iceberg-rest.catalog.hive_backend.warehouse = /user/hive/warehouse-hive/
...
gravitino.iceberg-rest.catalog.jdbc_backend.catalog-backend = jdbc
gravitino.iceberg-rest.catalog.jdbc_backend.uri = jdbc:mysql://127.0.0.1:3306/
gravitino.iceberg-rest.catalog.jdbc_backend.warehouse = hdfs://127.0.0.1:9000/user/hive/warehouse-mysql
...
```

#### Dynamic Catalog Configuration Provider

The dynamic catalog configuration provider retrieves the catalog configuration from the Gravitino server, and the catalog configuration could be updated dynamically.

| Configuration item                                          | Description                                                                                                                                                                                         | Default value | Required                                                             | Since Version    |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------|------------------|
| `gravitino.iceberg-rest.gravitino-uri`                      | The uri of Gravitino server address, only worked if `catalog-config-provider` is `dynamic-config-provider`. Not required when running as an auxiliary service embedded in Gravitino server.         | (none)        | Yes, when using `dynamic-config-provider` in standalone mode         | 0.7.0-incubating |
| `gravitino.iceberg-rest.gravitino-metalake`                 | The metalake name that `dynamic-config-provider` used to request to Gravitino, only worked if `catalog-config-provider` is `dynamic-config-provider`.                                               | (none)        | Yes, when using `dynamic-config-provider`                            | 0.7.0-incubating |
| `gravitino.iceberg-rest.default-catalog-name`               | The default catalog name used by Iceberg REST server if the Iceberg REST client doesn't specify the catalog name explicitly. Only worked if `catalog-config-provider` is `dynamic-config-provider`. | (none)        | No                                                                   | 1.0.0            |
| `gravitino.iceberg-rest.catalog-cache-eviction-interval-ms` | Catalog cache eviction interval.                                                                                                                                                                    | 3600000       | No                                                                   | 0.7.0-incubating |

:::tip
When using `dynamic-config-provider`, the behavior differs based on deployment mode:

- **Auxiliary mode** (embedded in Gravitino server): The service uses internal interfaces to access Gravitino directly. The `gravitino-uri` configuration is **not required** and will be ignored if provided.
- **Standalone mode**: The service uses HTTP/REST APIs to communicate with Gravitino server. The `gravitino-uri` configuration is **required** and must point to your Gravitino server.

Authorization features are only available when running in auxiliary mode.
:::
 
```text
gravitino.iceberg-rest.catalog-cache-eviction-interval-ms = 300000
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
# gravitino-uri is only required when running as a standalone server
# When running as an auxiliary service (embedded in Gravitino server), this is not needed
gravitino.iceberg-rest.gravitino-uri = http://127.0.0.1:8090
gravitino.iceberg-rest.gravitino-metalake = test
```

Suppose there are two Iceberg catalogs `hive_catalog` and `jdbc_catalog` in Gravitino server, `dynamic-config-provider` will poll the catalog properties internally and register `hive_catalog` and `jdbc_catalog` in Iceberg REST server side. Dynamic config provider will get all catalog properties, for the properties that start with `gravitino.bypass.` prefix, it will remove the prefix and use the rest part as the catalog property key.

#### Client Catalog Selection

Access different catalogs by setting the `warehouse` to the specific catalog name in the Iceberg REST client configuration. The default catalog will be used if you do not specify a `warehouse`. For instance, suppose there are three catalog backends: default catalog, `hive_catalog` and `jdbc_catalog`, consider the case of SparkSQL:

```shell
./bin/spark-sql -v \
...
--conf spark.sql.catalog.default_rest_catalog.type=rest  \
--conf spark.sql.catalog.default_rest_catalog.uri=http://127.0.0.1:9001/iceberg/ \
...
--conf spark.sql.catalog.hive_backend_rest_catalog.type=rest  \
--conf spark.sql.catalog.hive_backend_rest_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.hive_backend_rest_catalog.warehouse=hive_backend \
...
--conf spark.sql.catalog.jdbc_backend_rest_catalog.type=rest  \
--conf spark.sql.catalog.jdbc_backend_rest_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.jdbc_backend_rest_catalog.warehouse=jdbc_backend \
...
```

In the Spark SQL side, you could use `default_rest_catalog` to access the default catalog backend, and use `hive_backend_rest_catalog` and `jdbc_backend_rest_catalog` to access the `hive_backend` and `jdbc_backend` catalog backend respectively.

### Security and Access Control

#### OAuth2

Refer to [OAuth2 Configuration](./security/how-to-authenticate#server-configuration) for how to enable OAuth2.

When enabling OAuth2 and leveraging a dynamic configuration provider to retrieve catalog information from the Gravitino server, use the following configuration parameters to establish OAuth2 authentication for secure communication with the Gravitino server:

| Configuration item                                   | Description                                                                                      | Default value         | Required          | Since Version |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------|-----------------------|-------------------|---------------|
| `gravitino.iceberg-rest.gravitino-auth-type`         | The auth type for communicating with the Gravitino server. Supported values: `simple`, `oauth2`. | `simple`              | No                | 1.0.0         |
| `gravitino.iceberg-rest.gravitino-simple.user-name`  | The username when using `simple` auth type.                                                      | `iceberg-rest-server` | No                | 1.0.0         |
| `gravitino.iceberg-rest.gravitino-oauth2.server-uri` | The OAuth2 server uri address.                                                      | (none)                | Yes, for `oauth2` | 1.0.0         | 
| `gravitino.iceberg-rest.gravitino-oauth2.credential` | The credential to request the OAuth2 token.                                         | (none)                | Yes, for `oauth2` | 1.0.0         | 
| `gravitino.iceberg-rest.gravitino-oauth2.token-path` | The path for token of the default OAuth server.                                     | (none)                | Yes, for `oauth2` | 1.0.0         | 
| `gravitino.iceberg-rest.gravitino-oauth2.scope`      | The scope to request the OAuth2 token.                                              | (none)                | Yes, for `oauth2` | 1.0.0         | 

Here is an example of how to enable OAuth2 for Gravitino Iceberg REST server:

```text
gravitino.authenticators = oauth
gravitino.authenticator.oauth.serviceAudience = test
gravitino.authenticator.oauth.defaultSignKey = xx
gravitino.authenticator.oauth.tokenPath = oauth2/token
gravitino.authenticator.oauth.serverUri = http://localhost:8177
```

Add extra configurations if using `dynamic-config-provider` in standalone mode:

```text
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake = test
# gravitino-uri is required when running in standalone mode
gravitino.iceberg-rest.gravitino-uri = http://127.0.0.1:8090
gravitino.iceberg-rest.gravitino-auth-type = oauth2
gravitino.iceberg-rest.gravitino-oauth2.server-uri = http://localhost:8177
gravitino.iceberg-rest.gravitino-oauth2.credential = test:test
gravitino.iceberg-rest.gravitino-oauth2.token-path = oauth2/token
gravitino.iceberg-rest.gravitino-oauth2.scope = test
```

Please refer the following configuration If you are using Spark to access Iceberg REST catalog with OAuth2 enabled:

```shell
./bin/spark-sql -v \
--conf spark.jars=/Users/fanng/deploy/demo/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.rest.rest.auth.type=oauth2 \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.rest.prefix=${catalog_name} \
--conf spark.sql.catalog.rest.credential=test:test \
--conf spark.sql.catalog.rest.scope=test \
--conf spark.sql.catalog.rest.oauth2-server-uri=http://localhost:8177/oauth2/token
```

##### OAuth 2.0 Token Refresh for Iceberg REST Clients

OAuth 2.0 token refresh challenges may arise in certain query engines when accessing the Gravitino Iceberg REST Catalog (IRC).
These are often linked to identity providers without full token exchange support, or to authentication models in which child sessions inherit the expiration policies of their parent sessions.

The following Apache Iceberg change is relevant to this behavior:

| Version         | Change                                                                                                                                                                                 |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Iceberg 1.11.0+ | Supports disabling token exchange, renewing tokens with client credentials, and ensuring that child `AuthSession` instances use their own expiration instead of inheriting the parent session expiration. |

**Apache Iceberg OAuth 2.0 configuration**

**Spark**

Set the following catalog property to disable token exchange:

```text
spark.sql.catalog.${catalog_name}.token-exchange-enabled=false
```

**Flink**

Set the following catalog property to disable token exchange:

```sql
  'token-exchange-enable' = 'false'
```

**Trino**

Use Trino 479 or later, and set the following properties in the catalog configuration:

```properties
iceberg.rest-catalog.session=NONE
iceberg.rest-catalog.oauth2.token-exchange-enabled=false
```

Omit `iceberg.rest-catalog.session=NONE` because `NONE` is the default value.

#### HTTPS

Refer to [HTTPS Configuration](./security/how-to-use-https.md#apache-iceberg-rest-service-configuration) for how to enable HTTPS for Gravitino Iceberg REST server.

#### Backend Authentication

For JDBC backend, you can use the `gravitino.iceberg-rest.jdbc-user` and `gravitino.iceberg-rest.jdbc-password` to authenticate the JDBC connection. For Hive backend, you can use the `gravitino.iceberg-rest.authentication.type` to specify the authentication type, and use the `gravitino.iceberg-rest.authentication.kerberos.principal` and `gravitino.iceberg-rest.authentication.kerberos.keytab-uri` to authenticate the Kerberos connection.
The detailed configuration items are as follows:

| Configuration item                                                        | Description                                                                                                                                                                                                                                           | Default value | Required                                                                                                                                                             | Since Version    |
|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `gravitino.iceberg-rest.authentication.type`                              | The type of authentication for Iceberg rest catalog backend. This configuration only applicable for Hive backend, and only supports `Kerberos`, `simple` currently. As for JDBC backend, only username/password authentication was supported now. | `simple`      | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.impersonation-enable`              | Whether to enable impersonation for the Iceberg catalog                                                                                                                                                                                               | `false`       | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.hive.metastore.sasl.enabled`                      | Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore.                                                                                                                                                               | `false`       | No, This value should be true in most case(Some will use SSL protocol, but it rather rare) if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos. | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                                                          | (none)        | required if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                                                                | (none)        | required if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Iceberg catalog.                                                                                                                                                                                        | 60            | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                                                            | 60            | No                                                                                                                                                                   | 0.7.0-incubating |

#### Credential Vending

Refer to [Credential vending](./security/credential-vending.md) for more details.

#### Access Control

##### Prerequisites

To use access control with the Iceberg REST service:

1. The Iceberg REST service must be running as an auxiliary service within the Gravitino server (standalone mode is not supported for access control)
2. Enable authorization in the Gravitino server by setting `gravitino.authorization.enable = true`
3. Use the [dynamic configuration provider](#dynamic-catalog-configuration-provider) to retrieve catalog configurations from Gravitino

:::note
Access control for the Iceberg REST Catalog (IRC) is only supported when running as an auxiliary service embedded in the Gravitino server. Standalone Iceberg REST server deployments do not support access control features.

When running as an auxiliary service, the `gravitino.iceberg-rest.gravitino-uri` configuration is **not required**. The service will use internal interfaces to access Gravitino directly, providing better performance and avoiding the need for HTTP-based communication.
:::

Refer to [Access Control](./security/access-control.md) for details on how to configure authorization, create roles, and grant privileges in Gravitino.

##### Access Control Flow

When access control is enabled:

1. Clients authenticate with the Iceberg REST service (Now we support Basic auth and OAuth2)
2. The Iceberg REST service sends the authenticated user identity, target metadata object, and requested operation to the Gravitino server for authorization verification
3. Gravitino verifies the user has the necessary privileges to perform the operation on the specified metadata object
4. Upon successful authorization, the Iceberg REST service executes the operation; otherwise, it returns an authorization error

Refer to [Access Control](./security/access-control.md) for the complete list of privileges and how to grant them.


### Storage

If `gravitino.iceberg-rest.io-impl` is not configured, the Iceberg REST service uses
`org.apache.iceberg.io.ResolvingFileIO`, which selects a `FileIO` implementation based
on the URI scheme:

- S3: `s3`, `s3a`, or `s3n`
- OSS: `oss`
- GCS: `gs` or `gcs`
- ADLS: `abfs`, `abfss`, `wasb`, or `wasbs`
- To override the default, explicitly configure `gravitino.iceberg-rest.io-impl`.
- Ensure that the corresponding storage bundle is available in the Iceberg REST server classpath.

#### S3

| Configuration item                            | Description                                                                                                                                                                                                         | Default value                           | Required | Since Version    |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl`              | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.aws.s3.S3FileIO` to explicitly use S3FileIO.                                                                                           | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)                                  | No       | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)                                  | No       | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-path-style-access` | Whether to use path style access for S3.                                                                                                                                                                            | false                                   | No       | 0.9.0-incubating |

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.iceberg-rest.s3.sse.type`.

Refer to [S3 credentials](./security/credential-vending.md#s3-credentials) for credential related configurations.

:::info
 - For the JDBC catalog backend, set the `gravitino.iceberg-rest.warehouse` parameter to `s3://{bucket_name}/${prefix_name}`. 
 - For the Hive catalog backend, set `gravitino.iceberg-rest.warehouse` to `s3a://{bucket_name}/${prefix_name}`. 
 - Additionally, download the [Gravitino Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aws-bundle) and place it in the classpath of Iceberg REST server.
:::

#### OSS

| Configuration item                    | Description                                                                                                                     | Default value                           | Required | Since Version    |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl`      | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.aliyun.oss.OSSFileIO` to explicitly use OSSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |
| `gravitino.iceberg-rest.oss-endpoint` | The endpoint of Aliyun OSS service.                                                                                             | (none)                                  | No       | 0.7.0-incubating |
| `gravitino.iceberg-rest.oss-region`   | The region of the OSS service, like `oss-cn-hangzhou`, only used when `credential-providers` is `oss-token`.                    | (none)                                  | No       | 0.8.0-incubating |

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`, you could config it directly by `gravitino.iceberg-rest.client.security-token`.

Refer to [OSS credentials](./security/credential-vending.md#oss-credentials) for credential related configurations.

Download the [Gravitino Iceberg Aliyun bundle jar](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aliyun-bundle) and place it in the classpath of the Iceberg REST server. For classpath details, see [Package Layout](#package-layout).

:::info
Please set the `gravitino.iceberg-rest.warehouse` parameter to `oss://{bucket_name}/${prefix_name}`.
:::

#### GCS

Supports using static GCS credential file or generating GCS token to access GCS data.

| Configuration item               | Description                                                                                                                  | Default value                           | Required | Since Version    |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl` | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.gcp.gcs.GCSFileIO` to explicitly use GCSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`, you could config it directly by `gravitino.iceberg-rest.gcs.project-id`.

Refer to [GCS credentials](./security/credential-vending.md#gcs-credentials) for credential related configurations.

:::note
Ensure that the credential file is accessible by the Gravitino server. For example, the server may be running on a GCE machine, or you may set the environment variable `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json` even when `gcs-service-account-file` is already configured.
:::

:::info
Please set `gravitino.iceberg-rest.warehouse` to `gs://{bucket_name}/${prefix_name}`, and download [Gravitino Iceberg gcp bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-gcp-bundle) and place it to the classpath of Gravitino Iceberg REST server, `iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

#### ADLS

| Configuration item               | Description                                                                                                                         | Default value                           | Required | Since Version    |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl` | The IO implementation for `FileIO` in Iceberg. Set it to `org.apache.iceberg.azure.adlsv2.ADLSFileIO` to explicitly use ADLSFileIO. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.8.0-incubating |

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`, you could config it directly by `gravitino.iceberg-rest.adls.read.block-size-bytes`.

Refer to [ADLS credentials](./security/credential-vending.md#adls-credentials) for credential related configurations.

:::info
Please set `gravitino.iceberg-rest.warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`, and download the [Gravitino Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-azure-bundle) and place it in the classpath of Iceberg REST server.
:::

#### HDFS

Place HDFS configuration file to the classpath of the Iceberg REST server, `iceberg-rest-server/conf` for Gravitino server package, `conf` for standalone Gravitino Iceberg REST server package. When writing to HDFS, the Gravitino Iceberg REST catalog service can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [Access Apache Hadoop](gravitino-server-config.md#access-apache-hadoop) for more details.

:::info
Builds with Hadoop 2.10.x. There may be compatibility issues when accessing Hadoop 3.x clusters.
:::

#### Other Storage

For storages not managed by Gravitino directly, configure them through custom catalog properties.

| Configuration item               | Description                                                                                                               | Default value                           | Required | Since Version    |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl` | The IO implementation for `FileIO` in Iceberg. Use the fully qualified class name to override the default implementation. | `org.apache.iceberg.io.ResolvingFileIO` | No       | 0.6.0-incubating |

To pass custom properties such as `security-token` to your custom `FileIO`, configure them via `gravitino.iceberg-rest.security-token`. The `security-token` is included in the properties when the `FileIO` initialize method is invoked.

:::info
Please set the `gravitino.iceberg-rest.warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`. Additionally, download corresponding jars in the classpath of Iceberg REST server, `iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

### Feature Configuration

#### Views

View operations are supported when using the JDBC catalog backend with schema version `V1`. The default schema version is now `V1`, so view support is enabled out of the box. Iceberg will automatically migrate the database schema on the first restart and detect the migration on all subsequent restarts.

| Configuration item                           | Description                                                                                                          | Default value | Required | Since Version |
|----------------------------------------------|----------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.jdbc-schema-version` | The schema version of the JDBC catalog backend. Defaults to `V1` to enable view operations. Set to `V0` to opt out.  | `V1`          | No       | 1.2.0         |

#### Additional Iceberg Catalog Properties

Add other properties defined in [Iceberg catalog properties](https://iceberg.apache.org/docs/1.10.0/configuration/#catalog-properties).
The `clients` property for example:

| Configuration item               | Description                          | Default value | Required |
|----------------------------------|--------------------------------------|---------------|----------|
| `gravitino.iceberg-rest.clients` | The client pool size of the catalog. | `2`           | No       |

:::info
`catalog-impl` has no effect.
:::

#### Event Listeners

Gravitino generates pre-event and post-event for table operations and provide a pluggable event listener to allow you to inject custom logic. For more details, refer to [Event listener configuration](gravitino-server-config.md#event-listener-configuration).

#### Audit Logging

Gravitino provides a pluggable audit log mechanism, refer to [Audit log configuration](gravitino-server-config.md#audit-log-configuration).

#### Metrics Store

Gravitino provides a pluggable metrics store interface to store and delete Iceberg metrics. Develop a class that implements `org.apache.gravitino.iceberg.service.metrics.IcebergMetricsStore` and add the corresponding jar file to the Iceberg REST service classpath directory.

| Configuration item                              | Description                                                                                                                         | Default value | Required | Since Version |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.metricsStore`           | The Iceberg metrics storage class name.                                                                                             | (none)        | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsStoreRetainDays` | The days to retain Iceberg metrics in store, the value not greater than 0 means retain forever.                                     | -1            | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsQueueCapacity`   | The size of queue to store metrics temporally before storing to the persistent storage. Metrics will be dropped when queue is full. | 1000          | No       | 0.4.0         |

If you want to use jdbc as metrics store, you can set the `gravitino.iceberg-rest.metricsStore` to `jdbc`, and set the following configurations to connect to the database.
Initialize the database using the sql scripts in the directory `scripts`.
Download the corresponding JDBC driver to the `iceberg-rest-server/libs` directory.

| Configuration item                                  | Description                                                                                                                                         | Default value | Required | Since Version |
|-----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.jdbc-metrics.url`           | The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432/database` for Postgres, or `jdbc:mysql://127.0.0.1:3306/database` for mysql. | (none)        | Yes      | 1.1.0         |
| `gravitino.iceberg-rest.jdbc-metrics.jdbc-user`     | The username of the JDBC connection.                                                                                                                | (none)        | No       | 1.1.0         |
| `gravitino.iceberg-rest.jdbc-metrics.jdbc-password` | The password of the JDBC connection.                                                                                                                | (none)        | No       | 1.1.0         |
| `gravitino.iceberg-rest.jdbc-metrics.jdbc-driver`   | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL.                                            | (none)        | Yes      | 1.1.0         |

#### Table Metadata Cache

Gravitino features a pluggable cache system for updating or retrieving table metadata in the cache. It validates the location of table metadata against the catalog backend to ensure the correctness of cached data.

| Configuration item                                           | Description                                                                                                                                                                           | Default value                                                       | Required | Since Version |
|--------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|----------|---------------|
| `gravitino.iceberg-rest.table-metadata-cache-impl`           | The implementation of the table metadata cache. Set to empty string("") if `catalog-backend` is `rest` catalog, or `custom` catalog without the `SupportsMetadataLocation` interface. | `org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache` | No       | 1.1.0         |
| `gravitino.iceberg-rest.table-metadata-cache-capacity`       | The capacity of the table metadata cache.                                                                                                                                             | 1000                                                                | No       | 1.1.0         |
| `gravitino.iceberg-rest.table-metadata-cache-expire-minutes` | The expiration time (in minutes) of the table metadata cache.                                                                                                                         | 60                                                                  | No       | 1.1.0         |

Gravitino provides the build-in `org.apache.gravitino.iceberg.common.cache.LocalTableMetadataCache` to store the cached data in the memory. You could also implement your custom table metadata cache by implementing the `org.apache.gravitino.iceberg.common.cache.TableMetadataCache` interface.

#### Scan Plan Cache

Gravitino caches scan plan results to speed up repeated queries with identical parameters. The cache uses snapshot ID as part of the cache key, so queries against different snapshots will not use stale cached data.

Plan scan responses follow the Iceberg 1.11 REST API: completed plans return structured `file-scan-tasks` only. Legacy `plan-tasks` JSON strings (used by some Iceberg 1.9.x–1.10.x clients) are not emitted.

| Configuration item                                         | Description                                              | Default value | Required | Since Version |
|------------------------------------------------------------|----------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.scan-plan-cache-impl`              | The implementation of the scan plan cache.               | (none)        | No       | 1.2.0         |
| `gravitino.iceberg-rest.scan-plan-cache-capacity`          | The capacity of the scan plan cache.                     | 200           | No       | 1.2.0         |
| `gravitino.iceberg-rest.scan-plan-cache-expire-minutes`    | The expiration time (in minutes) of the scan plan cache. | 60            | No       | 1.2.0         |

The scan plan cache uses snapshot ID as part of the cache key, ensuring automatic invalidation when table data changes. This can provide significant speedup for repeated queries like dashboard refreshes or BI tool queries.

Gravitino provides the built-in `org.apache.gravitino.iceberg.service.cache.LocalScanPlanCache` to store the cached data in memory. Also implement your custom scan plan cache by implementing the `org.apache.gravitino.iceberg.service.cache.ScanPlanCache` interface.

#### Extension Packages

| Configuration item                          | Description                                                  | Default value | Required | Since Version    |
|---------------------------------------------|--------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.extension-packages` | Comma-separated list of Iceberg REST API packages to expand. | (none)        | No       | 0.7.0-incubating |

### Operations

#### Health Check Endpoints

The Iceberg REST server exposes three health check endpoints following the same [MicroProfile Health](https://microprofile.io/project/eclipse/microprofile-health) semantics as the main Gravitino server. All endpoints are exempt from authentication. The readiness probe checks whether the `IcebergCatalogWrapperManager` has been initialized. It performs no I/O and has no configurable timeout.

| Endpoint                     | Description                                                                                                                | HTTP status |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------|-------------|
| `GET /iceberg/health/live`   | Liveness probe. Returns 200 as long as the HTTP server thread can respond.                                                 | 200         |
| `GET /iceberg/health/ready`  | Readiness probe. Returns 200 when the catalog wrapper manager is initialized; 503 when initialization is not yet complete. | 200 / 503   |
| `GET /iceberg/health`        | Aggregate check. Returns 200 when both liveness and readiness pass; 503 when any check fails.                              | 200 / 503   |

Root-level aliases are also available for global traffic managers that require probes at well-known root paths:

| Alias               | Forwards to                 |
|---------------------|-----------------------------|
| `GET /health`       | `GET /iceberg/health`       |
| `GET /health/live`  | `GET /iceberg/health/live`  |
| `GET /health/ready` | `GET /iceberg/health/ready` |
| `GET /health.html`  | `GET /iceberg/health`       |

**Response format:**

All endpoints return a JSON body with the same shape as the main Gravitino server. The `code` field is always `0`. `status` is `UP` or `DOWN`. Liveness reports `httpServer` and readiness reports `catalogWrapperManager`.

Healthy response (HTTP 200):

```json
{
  "code": 0,
  "status": "UP",
  "checks": [
    { "name": "httpServer", "status": "UP", "details": {} },
    { "name": "catalogWrapperManager", "status": "UP", "details": {} }
  ]
}
```

Unhealthy response (HTTP 503):

```json
{
  "code": 0,
  "status": "DOWN",
  "checks": [
    { "name": "httpServer", "status": "UP", "details": {} },
    { "name": "catalogWrapperManager", "status": "DOWN", "details": { "reason": "catalog wrapper manager not initialized" } }
  ]
}
```

#### Memory Settings

The Iceberg REST server uses `GRAVITINO_MEM` for JVM heap/metaspace flags. Default: `-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m`. Launch scripts append `GRAVITINO_MEM` to `JAVA_OPTS`; set it to adjust heap/metaspace sizes.
Example tuning:
- Development: `GRAVITINO_MEM="-Xms1g -Xmx1g -XX:MaxMetaspaceSize=512m"`
- Medium workloads: `GRAVITINO_MEM="-Xms4g -Xmx4g -XX:MaxMetaspaceSize=1g"`
- Higher concurrency or catalog counts: increase heap and metaspace accordingly.

## Service Startup and Verification

### Auxiliary Service

Start the Iceberg REST service as an auxiliary service in the Gravitino server:

```shell
./bin/gravitino.sh start
```

### Standalone Server

Start the standalone Iceberg REST server:

```shell
./bin/gravitino-iceberg-rest-server.sh start
```

### Service Verification

Verify that the service is running:

```shell
curl http://127.0.0.1:9001/iceberg/v1/config
```

Example response: `{"defaults":{},"overrides":{}, "endpoints":["GET /v1/{prefix}/namespaces", ...]}%`.

## Docker

### Container Startup

Run the Iceberg REST server in a Docker container:

```shell
docker run -d -p 9001:9001 apache/gravitino-iceberg-rest:latest
```

### Environment Variables

The Docker image supports local storage by default.
For cloud or remote storage, configure the corresponding [storage](#storage) settings through these environment variables:

| Environment Variable                                           | Configuration items                                        | Since version |
|----------------------------------------------------------------|------------------------------------------------------------|---------------|
| `GRAVITINO_ICEBERG_REST_HOST`                                  | `gravitino.iceberg-rest.host`                              | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_HTTP_PORT`                             | `gravitino.iceberg-rest.httpPort`                          | 1.1.0         |
| `GRAVITINO_ICEBERG_REST_URI`                                   | `gravitino.iceberg-rest.uri`                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_IO_IMPL`                               | `gravitino.iceberg-rest.io-impl`                           | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_CATALOG_BACKEND`                       | `gravitino.iceberg-rest.catalog-backend`                   | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_DRIVER`                           | `gravitino.iceberg-rest.jdbc-driver`                       | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_USER`                             | `gravitino.iceberg-rest.jdbc-user`                         | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_PASSWORD`                         | `gravitino.iceberg-rest.jdbc-password`                     | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_WAREHOUSE`                             | `gravitino.iceberg-rest.warehouse`                         | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_REST_CLIENT_CONNECTION_TIMEOUT_MS`     | `gravitino.iceberg-rest.rest-client-connection-timeout-ms` | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_REST_CLIENT_SOCKET_TIMEOUT_MS`         | `gravitino.iceberg-rest.rest-client-socket-timeout-ms`     | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS`                  | `gravitino.iceberg-rest.credential-providers`              | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_GCS_SERVICE_ACCOUNT_FILE`              | `gravitino.iceberg-rest.gcs-service-account-file`          | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ACCESS_KEY`                         | `gravitino.iceberg-rest.s3-access-key-id`                  | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_SECRET_KEY`                         | `gravitino.iceberg-rest.s3-secret-access-key`              | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ENDPOINT`                           | `gravitino.iceberg-rest.s3-endpoint`                       | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_REGION`                             | `gravitino.iceberg-rest.s3-region`                         | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_PATH_STYLE_ACCESS`                  | `gravitino.iceberg-rest.s3-path-style-access`              | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ROLE_ARN`                           | `gravitino.iceberg-rest.s3-role-arn`                       | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_EXTERNAL_ID`                        | `gravitino.iceberg-rest.s3-external-id`                    | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_TOKEN_SERVICE_ENDPOINT`             | `gravitino.iceberg-rest.s3-token-service-endpoint`         | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_NAME`            | `gravitino.iceberg-rest.azure-storage-account-name`        | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_KEY`             | `gravitino.iceberg-rest.azure-storage-account-key`         | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_TENANT_ID`                       | `gravitino.iceberg-rest.azure-tenant-id`                   | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_ID`                       | `gravitino.iceberg-rest.azure-client-id`                   | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_SECRET`                   | `gravitino.iceberg-rest.azure-client-secret`               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ACCESS_KEY`                        | `gravitino.iceberg-rest.oss-access-key-id`                 | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_SECRET_KEY`                        | `gravitino.iceberg-rest.oss-secret-access-key`             | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ENDPOINT`                          | `gravitino.iceberg-rest.oss-endpoint`                      | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_REGION`                            | `gravitino.iceberg-rest.oss-region`                        | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ROLE_ARN`                          | `gravitino.iceberg-rest.oss-role-arn`                      | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_EXTERNAL_ID`                       | `gravitino.iceberg-rest.oss-external-id`                   | 1.3.0         |

### Deprecated Environment Variables

Use the replacement environment variables instead of these deprecated names.

| Deprecated Environment variables                         | New environment variables                            | Since version    | Deprecated version |
|----------------------------------------------------------|------------------------------------------------------|------------------|--------------------|
| `GRAVITINO_CREDENTIAL_PROVIDER_TYPE`                     | `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS`        | 0.7.0-incubating | 0.8.0-incubating   |
| `GRAVITINO_GCS_CREDENTIAL_FILE_PATH`                     | `GRAVITINO_ICEBERG_REST_GCS_SERVICE_ACCOUNT_FILE`    | 0.7.0-incubating | 0.8.0-incubating   |

### Custom Image Build

Build a custom image to add configuration or logic:

```shell
sh ./dev/docker/build-docker.sh --platform linux/arm64 --type iceberg-rest-server --image apache/gravitino-iceberg-rest --tag $tag
```

### Playground

Try Spark with the Gravitino Iceberg REST catalog service in the [playground](./how-to-use-the-playground.md#apache-iceberg-rest-service).

## Access Control Tutorial

Enable access control for the Iceberg REST server with Gravitino's dynamic configuration provider.

:::note
Access control requires the Iceberg REST server to run as an auxiliary service in the Gravitino server.
Standalone deployments do not support Iceberg REST catalog authorization.
:::

Complete these steps:

### Step 1: Enable Authorization and the Dynamic Configuration Provider

Add the following to your Gravitino server configuration file (`gravitino.conf`).
Note that access control is only supported when running the Iceberg REST server as an auxiliary service within the Gravitino server:

```properties
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = adminUser

gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake = test
# Note: gravitino-uri is not required when running as an auxiliary service
# The service will use internal interfaces to access Gravitino
```

Restart the Iceberg REST server after updating the configuration.

### Step 2: Create a Metalake

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test"
}' http://localhost:8090/api/metalakes
```

### Step 3: Create a Catalog

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog1",
  "type": "ICEBERG",
  "comment": "Iceberg catalog",
  "properties": {}
}' http://localhost:8090/api/metalakes/test/catalogs
```

### Step 4: Create a Role and Grant Privileges

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
   "name": "role1",
   "properties": {},
   "securableObjects": [
      {
         "fullName": "catalog1",
         "type": "CATALOG",
         "privileges": [
            {
               "name": "USE_CATALOG",
               "condition": "ALLOW"
            },
            {
               "name": "USE_SCHEMA",
               "condition": "ALLOW"
            },
            {
               "name": "SELECT_TABLE",
               "condition": "ALLOW"
            }
         ]
      }
   ]
}' http://localhost:8090/api/metalakes/test/roles
```

### Step 5: Verify Denied Access

Before granting any privileges, verify that the user cannot access the catalog. Try to list tables as `user1` (replace with your actual authentication method):

```shell
curl -u user1:password -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:9001/iceberg/v1/catalog1/namespaces/default/tables
```

This should return an error indicating insufficient privileges (such as HTTP 403 Forbidden).

### Step 6: Grant the Role to a User

Now grant the role with privileges to the user:

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
    "roleNames": ["role1"]
}' http://localhost:8090/api/metalakes/test/permissions/users/user1/grant
```

### Step 7: Verify Granted Access

After granting the role with privileges, repeat the request as `user1`:

```shell
curl -u user1:password -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:9001/iceberg/v1/catalog1/namespaces/default/tables
```

This time, the request should succeed and return the list of tables.

### Summary

- Enable authorization and set the configuration provider to `dynamic-config-provider`.
- Create a metalake.
- Create a catalog.
- Create a role and grant privileges.
- Assign the role to a user.

For more details, see the [Access Control documentation](./security/access-control.md).
