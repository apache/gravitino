---
title: "Iceberg REST catalog service"
slug: /iceberg-rest-service
keywords:
  - Iceberg REST catalog
license: "This software is licensed under the Apache License version 2."
---

## Background

The Apache Gravitino Iceberg REST Server follows the [Apache Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and acts as an Iceberg REST catalog server.

### Capabilities

- Supports the Apache Iceberg REST API defined in Iceberg 1.5, and supports all namespace and table interfaces. The following interfaces are not implemented yet:
  - token
  - view
  - multi table transaction
  - pagination
- Works as a catalog proxy, supporting `Hive` and `JDBC` as catalog backend.
- Supports HDFS and S3 storage.
- Supports OAuth2 and HTTPS.
- Provides a pluggable metrics store interface to store and delete Iceberg metrics.

## Server management

There are three deployment scenarios for Gravitino Iceberg REST server:
- A standalone server with a standalone Gravitino Iceberg REST server package.
- A standalone server in the Gravitino server package.
- An auxiliary service embedded in the Gravitino server.
 
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

Starting with version `0.6.0`, the prefix `gravitino.auxService.iceberg-rest.` for auxiliary server configurations has been deprecated. If both `gravitino.auxService.iceberg-rest.key` and `gravitino.iceberg-rest.key` are present, the latter will take precedence. The configurations listed below use the `gravitino.iceberg-rest.` prefix.

### Configuration to enable Iceberg REST service in Gravitino server.

| Configuration item                 | Description                                                                                                                                                                                                                            | Default value | Required | Since Version |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.names`       | The auxiliary service name of the Gravitino Iceberg REST catalog service. Use **`iceberg-rest`**.                                                                                                                                      | (none)        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.classpath` | The classpath of the Gravitino Iceberg REST catalog service; includes the directory containing jars and configuration. It supports both absolute and relative paths, for example, `iceberg-rest-server/libs, iceberg-rest-server/conf` | (none)        | Yes      | 0.2.0         |

Please note that, it only takes affect in `gravitino.conf`, you don't need to specify the above configurations if start as a standalone server.

### REST catalog server configuration

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

### Security

Gravitino Iceberg REST server supports OAuth2 and HTTPS, please refer to [Security](./security.md) for more details.

### Storage

Gravitino Iceberg REST server supports S3 and HDFS for storage.

#### S3 configuration

Gravitino Iceberg REST service supports using static access-key-id and secret-access-key to access S3 data.

| Configuration item                            | Description                                                                                                                                                                                                         | Default value | Required | Since Version |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.io-impl`              | The IO implementation for `FileIO` in Iceberg, use `org.apache.iceberg.aws.s3.S3FileIO` for S3.                                                                                                                     | (none)        | No       | 0.6.0         |
| `gravitino.iceberg-rest.s3-access-key-id`     | The static access key ID used to access S3 data.                                                                                                                                                                    | (none)        | No       | 0.6.0         |
| `gravitino.iceberg-rest.s3-secret-access-key` | The static secret access key used to access S3 data.                                                                                                                                                                | (none)        | No       | 0.6.0         |
| `gravitino.iceberg-rest.s3-endpoint`          | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | (none)        | No       | 0.6.0         |
| `gravitino.iceberg-rest.s3-region`            | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | (none)        | No       | 0.6.0         |

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`, you could config it directly by `gravitino.iceberg-rest.s3.sse.type`.

:::info
Please set `gravitino.iceberg-rest.warehouse` to `s3://{bucket_name}/${prefix_name}` for Jdbc catalog backend, `s3a://{bucket_name}/${prefix_name}` for Hive catalog backend.
:::

#### HDFS configuration

You should place HDFS configuration file to the classpath of the Iceberg REST server, `iceberg-rest-server/conf` for Gravitino server package, `conf` for standalone Gravitino Iceberg REST server package. When writing to HDFS, the Gravitino Iceberg REST catalog service can only operate as the specified HDFS user and doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md#how-to-access-apache-hadoop) for more details.

:::info
Builds with Hadoop 2.10.x. There may be compatibility issues when accessing Hadoop 3.x clusters.
:::

### Apache Gravitino Iceberg catalog backend configuration

:::info
The Gravitino Iceberg REST catalog service uses the memory catalog backend by default. You can specify a Hive or JDBC catalog backend for production environment.
:::

#### Apache Hive backend configuration

| Configuration item                            | Description                                                                                                                                  | Default value                                                                 | Required | Since Version |
|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`hive`** for a Hive catalog.                              | `memory`                                                                      | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                  | The Hive metadata address, such as `thrift://127.0.0.1:9083`.                                                                                | (none)                                                                        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse`            | The warehouse directory of the Hive catalog, such as `/user/hive/warehouse-hive/`.                                                           | (none)                                                                        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name` | The catalog backend name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables. | `hive` for Hive backend, `jdbc` for JDBC backend, `memory` for memory backend | No       | 0.5.2         |

#### JDBC backend configuration

| Configuration item                            | Description                                                                                                                          | Default value           | Required | Since Version |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-------------------------|----------|---------------|
| `gravitino.iceberg-rest.catalog-backend`      | The Catalog backend of the Gravitino Iceberg REST catalog service. Use the value **`jdbc`** for a JDBC catalog.                      | `memory`                | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.uri`                  | The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432` for Postgres, or `jdbc:mysql://127.0.0.1:3306/` for mysql.   | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.warehouse `           | The warehouse directory of JDBC catalog. Set the HDFS prefix if using HDFS, such as `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc` | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.catalog-backend-name` | The catalog name passed to underlying Iceberg catalog backend. Catalog name in JDBC backend is used to isolate namespace and tables. | `jdbc` for JDBC backend | No       | 0.5.2         |
| `gravitino.iceberg-rest.jdbc.user`            | The username of the JDBC connection.                                                                                                 | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.jdbc.password`        | The password of the JDBC connection.                                                                                                 | (none)                  | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-initialize`      | Whether to initialize the meta tables when creating the JDBC catalog.                                                                | `true`                  | No       | 0.2.0         |
| `gravitino.iceberg-rest.jdbc-driver`          | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL.                             | (none)                  | Yes      | 0.3.0         |

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name` to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
You must download the corresponding JDBC driver to the `iceberg-rest-server/libs` directory.
:::

### Other Apache Iceberg catalog properties

You can add other properties defined in [Iceberg catalog properties](https://iceberg.apache.org/docs/1.5.2/configuration/#catalog-properties).
The `clients` property for example:

| Configuration item               | Description                          | Default value | Required |
|----------------------------------|--------------------------------------|---------------|----------|
| `gravitino.iceberg-rest.clients` | The client pool size of the catalog. | `2`           | No       |

:::info
`catalog-impl` has no effect.
:::

### Apache Iceberg metrics store configuration

Gravitino provides a pluggable metrics store interface to store and delete Iceberg metrics. You can develop a class that implements `org.apache.gravitino.catalog.lakehouse.iceberg.web.metrics` and add the corresponding jar file to the Iceberg REST service classpath directory.

| Configuration item                              | Description                                                                                                                         | Default value | Required | Since Version |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.iceberg-rest.metricsStore`           | The Iceberg metrics storage class name.                                                                                             | (none)        | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsStoreRetainDays` | The days to retain Iceberg metrics in store, the value not greater than 0 means retain forever.                                     | -1            | No       | 0.4.0         |
| `gravitino.iceberg-rest.metricsQueueCapacity`   | The size of queue to store metrics temporally before storing to the persistent storage. Metrics will be dropped when queue is full. | 1000          | No       | 0.4.0         |

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

Normally you will see the output like `{"defaults":{},"overrides":{}}%`.

## Exploring the Apache Gravitino Iceberg REST catalog service with Apache Spark

### Deploying Apache Spark with Apache Iceberg support

Follow the [Spark Iceberg start guide](https://iceberg.apache.org/docs/1.5.2/getting-started/) to set up Apache Spark's and Apache Iceberg's environment. 

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

You may need to adjust the Iceberg Spark runtime jar file name according to the real version number in your environment. If you want to access the data stored in S3, you need to download [iceberg-aws-bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle) jar and place it in the classpath of Spark, no extra config is needed because S3 related properties is transferred from Iceberg REST server to Iceberg REST client automaticly. 

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

## Docker instructions

You could run Gravitino server though docker container:

```shell
docker run -d -p 9001:9001 datastrato/iceberg-rest-server:0.6
```

Or build it manually to add custom logics:

```shell
sh ./dev/docker/build-docker.sh --platform linux/arm64 --type iceberg-rest-server --image datastrato/iceberg-rest-server --tag 0.6
```

You could try Spark with Gravitino REST catalog service in our [playground](./how-to-use-the-playground.md#using-iceberg-rest-service).
