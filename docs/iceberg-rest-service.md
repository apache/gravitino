---
title: "Iceberg REST catalog service"
slug: /iceberg-rest-service
keywords:
  - Iceberg REST catalog
license: "This software is licensed under the Apache License version 2."
---

## Background

The Apache Gravitino Iceberg REST Server follows the [Apache Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and acts as an Iceberg REST catalog server, you could access the Iceberg REST endpoint with the uri `http://$ip:$port/iceberg/`.

There are some key difference between Gravitino Iceberg REST server and Gravitino server.

| Items              | Gravitino Iceberg REST server                                                                            | Gravitino server                                                                                     | 
|--------------------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| Interfaces         | [Iceberg REST API spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) | [Gravitino unified interfaces](https://gravitino.apache.org/docs/latest/api/rest/gravitino-rest-api) |
| Managed table type | Iceberg table only                                                                                       | JDBC, Hive, Iceberg, Hudi, Paimon, etc                                                               |

### Capabilities

- Supports the Apache Iceberg REST API defined in Iceberg 1.9, and supports all namespace and table interfaces. The following interfaces are not implemented yet:
  - multi table transaction
  - pagination
  - scan planning
  - load table credentials
- Works as a catalog proxy, supporting `Hive` and `JDBC` as catalog backend.
- Supports credential vending for `S3`、`GCS`、`OSS` and `ADLS`.
- Supports different storages like `S3`, `HDFS`, `OSS`, `GCS`, `ADLS` and provides the capability to support other storages.
- Supports event listener.
- Supports Audit log.
- Supports OAuth2 and HTTPS.
- Provides a pluggable metrics store interface to store and delete Iceberg metrics.

## Server management

There are three deployment scenarios for Gravitino Iceberg REST server:
- A standalone server in a standalone Gravitino Iceberg REST server package, the classpath is `libs`.
- A standalone server in the Gravitino server package, the classpath is `iceberg-rest-server/libs`.
- An auxiliary service embedded in the Gravitino server, the classpath is `iceberg-rest-server/libs`.

For detailed instructions on how to build and install the Gravitino server package, please refer to [How to build](./how-to-build.md) and [How to install](./how-to-install.md). To build the Gravitino Iceberg REST server package, use the command `./gradlew compileIcebergRESTServer -x test`. Alternatively, to create the corresponding compressed package in the distribution directory, use `./gradlew assembleIcebergRESTServer -x test`. The Gravitino Iceberg REST server package includes the following files:

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

## Apache Gravitino Iceberg REST catalog server configuration

There are distinct configuration files for standalone and auxiliary server: `gravitino-iceberg-rest-server.conf` is used for the standalone server, while `gravitino.conf` is for the auxiliary server. Although the configuration files differ, the configuration items remain the same.

Starting with version `0.6.0-incubating`, the prefix `gravitino.auxService.iceberg-rest.` for auxiliary server configurations has been deprecated. If both `gravitino.auxService.iceberg-rest.key` and `gravitino.iceberg-rest.key` are present, the latter will take precedence. The configurations listed below use the `gravitino.iceberg-rest.` prefix.

### Configuration to enable Iceberg REST service in Gravitino server.

| Configuration item                 | Description                                                                                                                                                                                                                            | Default value | Required | Since Version |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.names`       | The auxiliary service name of the Gravitino Iceberg REST catalog service. Use **`iceberg-rest`**.                                                                                                                                      | (none)        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.classpath` | The classpath of the Gravitino Iceberg REST catalog service; includes the directory containing jars and configuration. It supports both absolute and relative paths, for example, `iceberg-rest-server/libs, iceberg-rest-server/conf` | (none)        | Yes      | 0.2.0         |

Please note that, it only takes affect in `gravitino.conf`, you don't need to specify the above configurations if start as a standalone server.

### HTTP server configuration

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
You can also specify filter parameters by setting configuration entries in the style `gravitino.iceberg-rest.<class name of filter>.param.<param name>=<value>`.

### Catalog backend configuration

:::info
The Gravitino Iceberg REST catalog service uses the memory catalog backend by default. You can specify a Hive or JDBC catalog backend for production environment.
:::

#### Hive backend configuration

| Configuration item                                                        | Description                                                                                                                                  | Default value                                                                  | Required | Since Version |
|---------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`                                  | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`hive`** for the Hive catalog backend.                    | `memory`                                                                       | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                                              | The Hive metadata address, such as `thrift://127.0.0.1:9083`.                                                                                | (none)                                                                         | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse`                                        | The warehouse directory of the Hive catalog, such as `/user/hive/warehouse-hive/`.                                                           | (none)                                                                         | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name`                             | The catalog backend name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables. | `hive` for Hive backend, `jdbc` for JDBC backend, `memory` for memory backend  | No       | 0.5.2         |

#### JDBC backend configuration

| Configuration item                            | Description                                                                                                                          | Default value            | Required | Since Version |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`jdbc`** for the JDBC catalog backend.            | `memory`                 | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                  | The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432` for Postgres, or `jdbc:mysql://127.0.0.1:3306/` for mysql.   | (none)                   | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse `           | The warehouse directory of JDBC catalog. Set the HDFS prefix if using HDFS, such as `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc` | (none)                   | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name` | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables. | `jdbc` for JDBC backend  | No       | 0.5.2         |
| `gravitino.iceberg-rest.jdbc-user`            | The username of the JDBC connection.                                                                                                 | (none)                   | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-password`        | The password of the JDBC connection.                                                                                                 | (none)                   | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-initialize`      | Whether to initialize the meta tables when creating the JDBC catalog.                                                                | `true`                   | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-driver`          | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL.                             | (none)                   | Yes      | 0.3.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
You must download the corresponding JDBC driver to the `iceberg-rest-server/libs` directory.
If you are using multiple JDBC catalog backends, setting `jdbc-initialize` to true may not take effect for RDBMS like `Mysql`, you should create Iceberg meta tables explicitly.
:::

#### Custom backend configuration

| Configuration item                            | Description                                                                                                                   | Default value | Required | Since Version    |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`custom`** for the custom catalog backend. | `memory`      | Yes      | 0.2.0            |
| `gravitino.iceberg-rest.catalog-backend-impl` | The fully-qualified class name of a custom catalog implementation, only worked if `catalog-backend` is `custom`.              | (none)        | No       | 0.7.0-incubating |

If you want to use a custom Iceberg Catalog as `catalog-backend`, you can add a corresponding jar file to the classpath and load a custom Iceberg Catalog implementation by specifying the `catalog-backend-impl` property.

### Multiple catalog backend support

The Gravitino Iceberg REST server supports multiple catalog backend, and you could use `catalog-config-provider` to control the behavior about how to manage catalog backend configurations.

| Configuration item                               | Description                                                                                                                                                                                                                                                                                                                                      | Default value            | Required | Since Version    |
|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------|------------------|
| `gravitino.iceberg-rest.catalog-config-provider` | The className of catalog configuration provider, Gravitino provides build-in `static-config-provider` and `dynamic-config-provider`, you could also develop a custom class that implements `apache.gravitino.iceberg.service.provider.IcebergConfigProvider` and add the corresponding jar file to the Iceberg REST service classpath directory. | `static-config-provider` | No       | 0.7.0-incubating |

Use `static-config-provider` to manage catalog configuration in the file, the catalog configuration is loaded when the server start up and couldn't be changed.
While `dynamic-config-provider` is used to manage catalog config though Gravitino server, you could add&delete&change the catalog configurations dynamically.

#### Static catalog configuration provider

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

#### Dynamic catalog configuration provider

The dynamic catalog configuration provider retrieves the catalog configuration from the Gravitino server, and the catalog configuration could be updated dynamically.

| Configuration item                                          | Description                                                                                                                                                                                         | Default value | Required | Since Version    |
|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.gravitino-uri`                      | The uri of Gravitino server address, only worked if `catalog-config-provider` is `dynamic-config-provider`.                                                                                         | (none)        | No       | 0.7.0-incubating |
| `gravitino.iceberg-rest.gravitino-metalake`                 | The metalake name that `dynamic-config-provider` used to request to Gravitino, only worked if `catalog-config-provider` is `dynamic-config-provider`.                                               | (none)        | No       | 0.7.0-incubating |
| `gravitino.iceberg-rest.default-catalog-name`               | The default catalog name used by Iceberg REST server if the Iceberg REST client doesn't specify the catalog name explicitly. Only worked if `catalog-config-provider` is `dynamic-config-provider`. | (none)        | No       | 1.0.0            |
| `gravitino.iceberg-rest.catalog-cache-eviction-interval-ms` | Catalog cache eviction interval.                                                                                                                                                                    | 3600000       | No       | 0.7.0-incubating |

```text
gravitino.iceberg-rest.catalog-cache-eviction-interval-ms = 300000
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-uri = http://127.0.0.1:8090
gravitino.iceberg-rest.gravitino-metalake = test
```

Suppose there are two Iceberg catalogs `hive_catalog` and `jdbc_catalog` in Gravitino server, `dynamic-config-provider` will poll the catalog properties internally and register `hive_catalog` and `jdbc_catalog` in Iceberg REST server side.

#### How to access the specific catalog

You can access different catalogs by setting the `warehouse` to the specific catalog name in the Iceberg REST client configuration. The default catalog will be used if you do not specify a `warehouse`. For instance, suppose there are three catalog backends: default catalog, `hive_catalog` and `jdbc_catalog`, consider the case of SparkSQL:

```shell
./bin/spark-sql -v \
...
--conf spark.sql.catalog.default_rest_catalog.type=rest  \
--conf spark.sql.catalog.default_rest_catalog.uri=http://127.0.0.1:9001/iceberg/ \
...
--conf spark.sql.catalog.hive_backend_catalog.type=rest  \
--conf spark.sql.catalog.hive_backend_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.hive_backend_catalog.warehouse=hive_backend \
...
--conf spark.sql.catalog.jdbc_backend_catalog.type=rest  \
--conf spark.sql.catalog.jdbc_backend_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.jdbc_backend_catalog.warehouse=jdbc_backend \
...
```

In the Spark SQL side, you could use `default_rest_catalog` to access the default catalog backend, and use `hive_backend_catalog` and `jdbc_backend_catalog` to access the `hive_backend` and `jdbc_backend` catalog backend respectively.

### Security

#### OAuth2

Please refer to [OAuth2 Configuration](./security/how-to-authenticate#server-configuration) for how to enable OAuth2.

When enabling OAuth2 and leveraging a dynamic configuration provider to retrieve catalog information from the Gravitino server, please use the following configuration parameters to establish OAuth2 authentication for secure communication with the Gravitino server:

| Configuration item                                   | Description                                                                         | Default value         | Required          | Since Version |
|------------------------------------------------------|-------------------------------------------------------------------------------------|-----------------------|-------------------|---------------|
| `gravitino.iceberg-rest.gravitino-auth-type`         | The auth type to communicate with Gravitino server, supports `simple` and `oauth2`. | `simple`              | No                | 1.0.0         |
| `gravitino.iceberg-rest.gravitino-simple.user-name`  | The username when using `simple` auth type.                                         | `iceberg-rest-server` | No                | 1.0.0         |
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

You should add extra configurations if using `dynamic-config-provider`:

```text
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake = test
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
--conf spark.jars=/Users/fanng/deploy/demo/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
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

#### HTTPS

Please refer to [HTTPS Configuration](./security/how-to-use-https/#apache-iceberg-rest-services-configuration) for how to enable HTTPS for Gravitino Iceberg REST server.

#### Backend authentication

For JDBC backend, you can use the `gravitino.iceberg-rest.jdbc-user` and `gravitino.iceberg-rest.jdbc-password` to authenticate the JDBC connection. For Hive backend, you can use the `gravitino.iceberg-rest.authentication.type` to specify the authentication type, and use the `gravitino.iceberg-rest.authentication.kerberos.principal` and `gravitino.iceberg-rest.authentication.kerberos.keytab-uri` to authenticate the Kerberos connection.
The detailed configuration items are as follows:

| Configuration item                                                        | Description                                                                                                                                                                                                                                            | Default value | Required                                                                                                                                                             | Since Version    |
|---------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `gravitino.iceberg-rest.authentication.type`                              | The type of authentication for Iceberg rest catalog backend. This configuration only applicable for for Hive backend, and only supports `Kerberos`, `simple` currently. As for JDBC backend, only username/password authentication was supported now.  | `simple`      | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.impersonation-enable`              | Whether to enable impersonation for the Iceberg catalog                                                                                                                                                                                                | `false`       | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.hive.metastore.sasl.enabled`                      | Whether to enable SASL authentication protocol when connect to Kerberos Hive metastore.                                                                                                                                                                | `false`       | No, This value should be true in most case(Some will use SSL protocol, but it rather rare) if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos. | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                                                           | (none)        | required if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                                                                 | (none)        | required if the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Iceberg catalog.                                                                                                                                                                                         | 60            | No                                                                                                                                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                                                             | 60            | No                                                                                                                                                                   | 0.7.0-incubating |

### Credential vending

Please refer to [Credential vending](./security/credential-vending.md) for more details.

### Storage

#### S3 configuration

| Configuration item                            | Description                                                                                                                                                                                                         | Default value | Required                                       | Since Version    |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------------------------|------------------|
| `gravitino.iceberg-rest.io-impl`              | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aws.s3.S3FileIO` for S3.                                                                                                                     | (none)        | No                                             | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)        | No                                             | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)        | No                                             | 0.6.0-incubating |
| `gravitino.iceberg-rest.s3-path-style-access` | Whether to use path style access for S3.                                                                                                                                                                            | false         | No                                             | 0.9.0-incubating |

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.iceberg-rest.s3.sse.type`.

Please refer to [S3 credentials](./security/credential-vending.md#s3-credentials) for credential related configurations.

:::info
To configure the JDBC catalog backend, set the `gravitino.iceberg-rest.warehouse` parameter to `s3://{bucket_name}/${prefix_name}`. For the Hive catalog backend, set `gravitino.iceberg-rest.warehouse` to `s3a://{bucket_name}/${prefix_name}`. Additionally, download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle) and place it in the classpath of Iceberg REST server.
:::

#### OSS configuration

| Configuration item                                | Description                                                                                                                                                                                           | Default value   | Required                                             | Since Version    |
|---------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|------------------------------------------------------|------------------|
| `gravitino.iceberg-rest.io-impl`                  | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aliyun.oss.OSSFileIO` for OSS.                                                                                                 | (none)          | No                                                   | 0.6.0-incubating |
| `gravitino.iceberg-rest.oss-endpoint`             | The endpoint of Aliyun OSS service.                                                                                                                                                                   | (none)          | No                                                   | 0.7.0-incubating |
| `gravitino.iceberg-rest.oss-region`               | The region of the OSS service, like `oss-cn-hangzhou`, only used when `credential-providers` is `oss-token`.                                                                                          | (none)          | No                                                   | 0.8.0-incubating |

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`, you could config it directly by `gravitino.iceberg-rest.client.security-token`.

Please refer to [OSS credentials](./security/credential-vending.md#oss-credentials) for credential related configurations.

Additionally, Iceberg doesn't provide Iceberg Aliyun bundle jar which contains OSS packages, there are two alternatives to use OSS packages:
1. Use [Gravitino Aliyun bundle jar with hadoop packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle).
2. Use [Aliyun JAVA SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip) and extract `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar` jars.

Please place the above jars in the classpath of Iceberg REST server, please refer to [server management](#server-management) for classpath details.

:::info
Please set the `gravitino.iceberg-rest.warehouse` parameter to `oss://{bucket_name}/${prefix_name}`.
:::

#### GCS

Supports using static GCS credential file or generating GCS token to access GCS data.

| Configuration item                                | Description                                                                                        | Default value | Required | Since Version    |
|---------------------------------------------------|----------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl`                  | The io implementation for `FileIO` in Iceberg, use `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS. | (none)        | No       | 0.6.0-incubating |

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`, you could config it directly by `gravitino.iceberg-rest.gcs.project-id`.

Please refer to [GCS credentials](./security/credential-vending.md#gcs-credentials) for credential related configurations.

:::note
Please ensure that the credential file can be accessed by the Gravitino server. For example, if the server is running on a GCE machine, or you can set the environment variable as `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`, even when the `gcs-service-account-file` has already been configured.
:::

:::info
Please set `gravitino.iceberg-rest.warehouse` to `gs://{bucket_name}/${prefix_name}`, and download [Iceberg gcp bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle) and place it to the classpath of Gravitino Iceberg REST server, `iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

#### ADLS

| Configuration item                                  | Description                                                                                                                                                                                        | Default value | Required | Since Version    |
|-----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl`                    | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for ADLS.                                                                                          | (none)        | Yes      | 0.8.0-incubating |

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`, you could config it directly by `gravitino.iceberg-rest.adls.read.block-size-bytes`.

Please refer to [ADLS credentials](./security/credential-vending.md#adls-credentials) for credential related configurations.

:::info
Please set `gravitino.iceberg-rest.warehouse` to `abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`, and download the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle) and place it in the classpath of Iceberg REST server.
:::

#### HDFS configuration

You should place HDFS configuration file to the classpath of the Iceberg REST server, `iceberg-rest-server/conf` for Gravitino server package, `conf` for standalone Gravitino Iceberg REST server package. When writing to HDFS, the Gravitino Iceberg REST catalog service can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md#how-to-access-apache-hadoop) for more details.

:::info
Builds with Hadoop 2.10.x. There may be compatibility issues when accessing Hadoop 3.x clusters.
:::

#### Other storages

For other storages that are not managed by Gravitino directly, you can manage them through custom catalog properties.

| Configuration item               | Description                                                                             | Default value | Required | Since Version    |
|----------------------------------|-----------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.io-impl` | The IO implementation for `FileIO` in Iceberg, please use the full qualified classname. | (none)        | No       | 0.6.0-incubating |

To pass custom properties such as `security-token` to your custom `FileIO`, you can directly configure it by `gravitino.iceberg-rest.security-token`. `security-token` will be included in the properties when the initialize method of `FileIO` is invoked.

:::info
Please set the `gravitino.iceberg-rest.warehouse` parameter to `{storage_prefix}://{bucket_name}/${prefix_name}`. Additionally, download corresponding jars in the classpath of Iceberg REST server, `iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

### View support

You could access the view interface if using JDBC backend and enable `jdbc.schema-version` property.

| Configuration item                           | Description                                                                                | Default value | Required | Since Version    |
|----------------------------------------------|--------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.jdbc.schema-version` | The schema version of JDBC catalog backend, setting to `V1` if supporting view operations. | (none)        | NO       | 0.7.0-incubating |

### Other Apache Iceberg catalog properties

You can add other properties defined in [Iceberg catalog properties](https://iceberg.apache.org/docs/1.9.2/configuration/#catalog-properties).
The `clients` property for example:

| Configuration item               | Description                          | Default value | Required |
|----------------------------------|--------------------------------------|---------------|----------|
| `gravitino.iceberg-rest.clients` | The client pool size of the catalog. | `2`           | No       |

:::info
`catalog-impl` has no effect.
:::

### Event listener

Gravitino generates pre-event and post-event for table operations and provide a pluggable event listener to allow you to inject custom logic. For more details, please refer to [Event listener configuration](gravitino-server-config.md#event-listener-configuration).

### Audit log

Gravitino provides a pluggable audit log mechanism, please refer to [Audit log configuration](gravitino-server-config.md#audit-log-configuration).

### Apache Iceberg metrics store configuration

Gravitino provides a pluggable metrics store interface to store and delete Iceberg metrics. You can develop a class that implements `org.apache.gravitino.iceberg.service.metrics.IcebergMetricsStore` and add the corresponding jar file to the Iceberg REST service classpath directory.

| Configuration item                              | Description                                                                                                                         | Default value | Required | Since Version |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.metricsStore`           | The Iceberg metrics storage class name.                                                                                             | (none)        | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsStoreRetainDays` | The days to retain Iceberg metrics in store, the value not greater than 0 means retain forever.                                     | -1            | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsQueueCapacity`   | The size of queue to store metrics temporally before storing to the persistent storage. Metrics will be dropped when queue is full. | 1000          | No       | 0.4.0         |

### Misc configurations

| Configuration item                          | Description                                                  | Default value | Required | Since Version    |
|---------------------------------------------|--------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.iceberg-rest.extension-packages` | Comma-separated list of Iceberg REST API packages to expand. | (none)        | No       | 0.7.0-incubating |

## Starting the Iceberg REST server

To start as an auxiliary service with Gravitino server:

```shell
./bin/gravitino.sh start 
```

To start a standalone Gravitino Iceberg REST catalog server:

```shell
./bin/gravitino-iceberg-rest-server.sh start
```

To verify whether the service has started:

```shell
curl  http://127.0.0.1:9001/iceberg/v1/config
```

Normally you will see the output like `{"defaults":{},"overrides":{}, "endpoints":["GET /v1/{prefix}/namespaces", ...]}%`.

## Exploring the Apache Gravitino Iceberg REST catalog service with Apache Spark

### Deploying Apache Spark with Apache Iceberg support

Follow the [Spark Iceberg start guide](https://iceberg.apache.org/docs/1.9.2/spark-getting-started/) to set up Apache Spark's and Apache Iceberg's environment.

### Starting the Apache Spark client with the Apache Iceberg REST catalog

| Configuration item                       | Description                                                               |
|------------------------------------------|---------------------------------------------------------------------------|
| `spark.sql.catalog.${catalog-name}.type` | The Spark catalog type; should set to `rest`.                             |
| `spark.sql.catalog.${catalog-name}.uri`  | Spark Iceberg REST catalog URI, such as `http://127.0.0.1:9001/iceberg/`. |

For example, we can configure Spark catalog options to use Gravitino Iceberg REST catalog with the catalog name `rest`.

```shell
./bin/spark-sql -v \
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog  \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/
```

You may need to adjust the Iceberg Spark runtime jar file name according to the real version number in your environment. If you want to access the data stored in cloud, you need to download corresponding jars (please refer to the cloud storage part) and place it in the classpath of Spark. If you want to enable credential vending, please set `credential-providers` to a proper value in the server side, set `spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation` = `vended-credentials` in the client side.

For other storages not managed by Gravitino, the properties wouldn't transfer from the server to client automatically, if you want to pass custom properties to initialize `FileIO`, you could add it by `spark.sql.catalog.${iceberg_catalog_name}.${configuration_key}` = `{property_value}`.

### Exploring Apache Iceberg with Apache Spark SQL

```sql
// First change to use the `rest` catalog
USE rest;
CREATE DATABASE IF NOT EXISTS dml;
CREATE TABLE dml.test (id bigint COMMENT 'unique id') using iceberg;
DESCRIBE TABLE EXTENDED dml.test;
INSERT INTO dml.test VALUES (1), (2);
SELECT * FROM dml.test;
```

## Exploring the Apache Gravitino Iceberg REST catalog service with Trino

### Deploying Trino with Apache Iceberg support

To configure the Iceberg connector, create a catalog properties file like `etc/catalog/rest.properties` that references the Iceberg connector.

```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:9001/iceberg/
fs.hadoop.enabled=true
```

Please refer to [Trino Iceberg document](https://trino.io/docs/current/connector/iceberg.html) for more details.

### Exploring Apache Iceberg with Trino SQL

```sql
USE rest.dml;
DELETE FROM rest.dml.test WHERE id = 2;
SELECT * FROM test;
```

## Exploring the Apache Gravitino Iceberg REST catalog service with Apache Doris

### Creating Iceberg catalog in Apache Doris

```
CREATE CATALOG iceberg PROPERTIES (
    "uri" = "http://localhost:9001/iceberg/",
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "s3.endpoint" = "http://s3.ap-southeast-2.amazonaws.com",
    "s3.region" = "ap-southeast-2",
    "s3.access_key" = "xxx",
    "s3.secret_key" = "xxx"
);
```

### Exploring Apache Iceberg with Apache Doris SQL

```sql
SWITCH iceberg;
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values(1);
SELECT * FROM t;
```

## Exploring the Apache Gravitino Iceberg REST catalog service with StarRocks

### Creating Iceberg catalog in StarRocks

```
CREATE EXTERNAL CATALOG 'iceberg'
COMMENT "Gravitino Iceberg REST catalog on MinIO"
PROPERTIES
(
  "type"="iceberg",
  "iceberg.catalog.type"="rest",
  "iceberg.catalog.uri"="http://iceberg-rest:9001/iceberg",
  "aws.s3.access_key"="admin",
  "aws.s3.secret_key"="password",
  "aws.s3.endpoint"="http://minio:9000",
  "aws.s3.enable_path_style_access"="true",
  "client.factory"="com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);
```

Please note that, you should set `client.factory` explicitly.

### Exploring Apache Iceberg with StarRocks SQL

```sql
SET CATALOG iceberg;
CREATE DATABASE db;
USE db;
CREATE TABLE t(a int);
INSERT INTO t values(1);
SELECT * FROM t;
```

## Docker instructions

You could run Gravitino Iceberg REST server though docker container:

```shell
docker run -d -p 9001:9001 apache/gravitino-iceberg-rest:latest
```

Gravitino Iceberg REST server in docker image could access local storage by default, you could set the following environment variables if the storage is cloud/remote storage like S3, please refer to [storage section](#storage) for more details.

| Environment variables                  | Configuration items                                 | Since version    |
|----------------------------------------|-----------------------------------------------------|------------------|
| `GRAVITINO_IO_IMPL`                    | `gravitino.iceberg-rest.io-impl`                    | 0.7.0-incubating |
| `GRAVITINO_URI`                        | `gravitino.iceberg-rest.uri`                        | 0.7.0-incubating |
| `GRAVITINO_CATALOG_BACKEND`            | `gravitino.iceberg-rest.catalog-backend`            | 1.0.0            |
| `GRAVITINO_JDBC_DRIVER`                | `gravitino.iceberg-rest.jdbc-driver`                | 0.9.0-incubating |
| `GRAVITINO_JDBC_USER`                  | `gravitino.iceberg-rest.jdbc-user`                  | 0.9.0-incubating |
| `GRAVITINO_JDBC_PASSWORD`              | `gravitino.iceberg-rest.jdbc-password`              | 0.9.0-incubating |
| `GRAVITINO_WAREHOUSE`                  | `gravitino.iceberg-rest.warehouse`                  | 0.7.0-incubating |
| `GRAVITINO_CREDENTIAL_PROVIDERS`       | `gravitino.iceberg-rest.credential-providers`       | 0.8.0-incubating |
| `GRAVITINO_GCS_SERVICE_ACCOUNT_FILE`   | `gravitino.iceberg-rest.gcs-service-account-file`   | 0.8.0-incubating |
| `GRAVITINO_S3_ACCESS_KEY`              | `gravitino.iceberg-rest.s3-access-key-id`           | 0.7.0-incubating |
| `GRAVITINO_S3_SECRET_KEY`              | `gravitino.iceberg-rest.s3-secret-access-key`       | 0.7.0-incubating |
| `GRAVITINO_S3_ENDPOINT`                | `gravitino.iceberg-rest.s3-endpoint`                | 0.9.0-incubating |
| `GRAVITINO_S3_REGION`                  | `gravitino.iceberg-rest.s3-region`                  | 0.7.0-incubating |
| `GRAVITINO_S3_ROLE_ARN`                | `gravitino.iceberg-rest.s3-role-arn`                | 0.7.0-incubating |
| `GRAVITINO_S3_EXTERNAL_ID`             | `gravitino.iceberg-rest.s3-external-id`             | 0.7.0-incubating |
| `GRAVITINO_S3_TOKEN_SERVICE_ENDPOINT`  | `gravitino.iceberg-rest.s3-token-service-endpoint`  | 0.8.0-incubating |
| `GRAVITINO_S3_PATH_STYLE_ACCESS`       | `gravitino.iceberg-rest.s3-path-style-access`       | 1.0.0            |
| `GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME` | `gravitino.iceberg-rest.azure-storage-account-name` | 0.8.0-incubating |
| `GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY`  | `gravitino.iceberg-rest.azure-storage-account-key`  | 0.8.0-incubating |
| `GRAVITINO_AZURE_TENANT_ID`            | `gravitino.iceberg-rest.azure-tenant-id`            | 0.8.0-incubating |
| `GRAVITINO_AZURE_CLIENT_ID`            | `gravitino.iceberg-rest.azure-client-id`            | 0.8.0-incubating |
| `GRAVITINO_AZURE_CLIENT_SECRET`        | `gravitino.iceberg-rest.azure-client-secret`        | 0.8.0-incubating |
| `GRAVITINO_OSS_ACCESS_KEY`             | `gravitino.iceberg-rest.oss-access-key-id`          | 0.8.0-incubating |
| `GRAVITINO_OSS_SECRET_KEY`             | `gravitino.iceberg-rest.oss-secret-access-key`      | 0.8.0-incubating |
| `GRAVITINO_OSS_ENDPOINT`               | `gravitino.iceberg-rest.oss-endpoint`               | 0.8.0-incubating |
| `GRAVITINO_OSS_REGION`                 | `gravitino.iceberg-rest.oss-region`                 | 0.8.0-incubating |
| `GRAVITINO_OSS_ROLE_ARN`               | `gravitino.iceberg-rest.oss-role-arn`               | 0.8.0-incubating |
| `GRAVITINO_OSS_EXTERNAL_ID`            | `gravitino.iceberg-rest.oss-external-id`            | 0.8.0-incubating |

The below environment is deprecated, please use the corresponding configuration items instead.

| Deprecated Environment variables     | New environment variables            | Since version    | Deprecated version |
|--------------------------------------|--------------------------------------|------------------|--------------------|
| `GRAVITINO_CREDENTIAL_PROVIDER_TYPE` | `GRAVITINO_CREDENTIAL_PROVIDERS`     | 0.7.0-incubating | 0.8.0-incubating   |
| `GRAVITINO_GCS_CREDENTIAL_FILE_PATH` | `GRAVITINO_GCS_SERVICE_ACCOUNT_FILE` | 0.7.0-incubating | 0.8.0-incubating   |

Or build it manually to add custom configuration or logics:

```shell
sh ./dev/docker/build-docker.sh --platform linux/arm64 --type iceberg-rest-server --image apache/gravitino-iceberg-rest --tag $tag
```

You could try Spark with Gravitino REST catalog service in our [playground](./how-to-use-the-playground.md#using-apache-iceberg-rest-service).

