---
title: "Iceberg REST catalog service"
slug: /iceberg-rest-service
keywords:
  - Iceberg REST catalog
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Background

The Gravitino Iceberg REST Server follows the [Apache Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and acts as an Iceberg REST catalog server.

### Capabilities

- Supports the Apache Iceberg REST API defined in Iceberg 1.3.1, and supports all namespace and table interfaces. `Token`, and `Config` interfaces aren't supported yet.
- Works as a catalog proxy, supporting `Hive` and `JDBC` as catalog backend.
- Provides a pluggable metrics store interface to store and delete Iceberg metrics.
- When writing to HDFS, the Gravitino Iceberg REST catalog service can only operate as the specified HDFS user and
  doesn't support proxying to other HDFS users. See [How to access Apache Hadoop](gravitino-server-config.md#how-to-access-apache-hadoop) for more details.

:::info
Builds with Apache Iceberg `1.3.1`. The Apache Iceberg table format version is `1` by default.
Builds with Hadoop 2.10.x, there may be compatibility issues when accessing Hadoop 3.x clusters.
:::

## Gravitino Iceberg REST catalog service configuration

Assuming the Gravitino server is deployed in the `GRAVITINO_HOME` directory, you can locate the configuration options in [`$GRAVITINO_HOME/conf/gravitino.conf`](gravitino-server-config.md). There are four configuration properties for the Iceberg REST catalog service:

1. [**REST Catalog Server Configuration**](#rest-catalog-server-configuration): you can specify the HTTP server properties like host and port.

2. [**Gravitino Iceberg metrics store Configuration**](#iceberg-metrics-store-configuration): you could implement a custom Iceberg metrics store and set corresponding configuration.

3. [**Gravitino Iceberg Catalog backend Configuration**](#gravitino-iceberg-catalog-backend-configuration): you have the option to set the specified catalog-backend to either `jdbc` or `hive`.

4. [**Other Iceberg Catalog Properties Defined by Apache Iceberg**](#other-apache-iceberg-catalog-properties): allows you to configure additional properties defined by Apache Iceberg.

Please refer to the following sections for details.

### REST catalog server configuration

| Configuration item                                          | Description                                                                                                                                                                                                                                                | Default value                                                                | Required | Since Version |
|-------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|---------------|
| `gravitino.auxService.names`                                | The auxiliary service name of the Gravitino Iceberg REST catalog service, use **`iceberg-rest`** for the Gravitino Iceberg REST catalog service.                                                                                                           | (none)                                                                       | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.classpath`               | The classpath of the Gravitino Iceberg REST catalog service, includes the directory containing jars and configuration. It supports both absolute paths and relative paths, for example, `catalogs/lakehouse-iceberg/libs, catalogs/lakehouse-iceberg/conf` | (none)                                                                       | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.host`                    | The host of the Gravitino Iceberg REST catalog service.                                                                                                                                                                                                    | `0.0.0.0`                                                                    | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.httpPort`                | The port of the Gravitino Iceberg REST catalog service.                                                                                                                                                                                                    | `9001`                                                                       | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.minThreads`              | The minimum number of threads in the thread pool used by the Jetty web server. `minThreads` is 8 if the value is less than 8.                                                                                                                              | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.maxThreads`              | The maximum number of threads in the thread pool used by the Jetty web server. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be greater than or equal to `minThreads`.                                                              | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by Gravitino Iceberg REST catalog service.                                                                                                                                                                   | `100`                                                                        | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.stopTimeout`             | The amount of time in ms for the Gravitino Iceberg REST catalog service to stop gracefully. For more information see `org.eclipse.jetty.server.Server#setStopTimeout`.                                                                                     | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.idleTimeout`             | The timeout in ms of idle connections.                                                                                                                                                                                                                     | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.requestHeaderSize`       | The maximum size of an HTTP request.                                                                                                                                                                                                                       | `131072`                                                                     | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.responseHeaderSize`      | The maximum size of an HTTP response.                                                                                                                                                                                                                      | `131072`                                                                     | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.customFilters`           | Comma separated list of filter class names to apply to the APIs.                                                                                                                                                                                           | (none)                                                                       | No       | 0.4.0         |


The filter in the customFilters should be a standard javax servlet Filter.
Filter parameters can also be specified in the configuration, by setting config entries of the form `gravitino.auxService.iceberg-rest.<class name of filter>.param.<param name>=<value>`

### Iceberg metrics store configuration

Gravitino provides a pluggable metrics store interface to store and delete Iceberg metrics. You can develop a class that implements `com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics` and add the corresponding jar file to the Iceberg REST service classpath directory.


| Configuration item                                         | Description                                                                                                                         | Default value | Required | Since Version |
|------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.iceberg-rest.metricsStore`           | The Iceberg metrics storage class name.                                                                                             | (none)        | No       | 0.4.0         |
| `gravitino.auxService.iceberg-rest.metricsStoreRetainDays` | The days to retain Iceberg metrics in store, the value not greater than 0 means retain forever.                                     | -1            | No       | 0.4.0         |
| `gravitino.auxService.iceberg-rest.metricsQueueCapacity`   | The size of queue to store metrics temporally before storing to the persistent storage. Metrics will be dropped when queue is full. | 1000          | No       | 0.4.0         |


### Gravitino Iceberg catalog backend configuration

:::info
The Gravitino Iceberg REST catalog service uses the memory catalog backend by default. You can specify using a Hive or JDBC catalog backend in a production environment.
:::

#### Hive backend configuration

| Configuration item                                  | Description                                                                                                 | Default value | Required   | Since Version |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------|---------------|------------|---------------|
| `gravitino.auxService.iceberg-rest.catalog-backend` | The Catalog backend of Gravitino Iceberg REST catalog service, use the value **`hive`** for a Hive catalog. | `memory`      | Yes        | 0.2.0         |
| `gravitino.auxService.iceberg-rest.uri`             | The Hive metadata address, such as `thrift://127.0.0.1:9083`.                                               | (none)        | Yes        | 0.2.0         |
| `gravitino.auxService.iceberg-rest.warehouse `      | The warehouse directory of the Hive catalog, such as `/user/hive/warehouse-hive/`.                          | (none)        | Yes        | 0.2.0         |

#### JDBC backend configuration

| Configuration item                                  | Description                                                                                                                        | Default value | Required | Since Version |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.iceberg-rest.catalog-backend` | The Catalog backend of Gravitino Iceberg REST catalog service, use the value **`jdbc`** for a JDBC catalog.                        | `memory`      | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.uri`             | The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432` for Postgres, or `jdbc:mysql://127.0.0.1:3306/` for mysql. | (none)        | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.warehouse `      | The warehouse directory of JDBC catalog, set HDFS prefix if using HDFS, such as `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc`   | (none)        | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.jdbc.user`       | The username of the JDBC connection.                                                                                               | (none)        | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.jdbc.password`   | The password of the JDBC connection.                                                                                               | (none)        | Yes      | 0.2.0         |
| `gravitino.auxService.iceberg-rest.jdbc-initialize` | Whether to initialize the meta tables when creating the JDBC catalog.                                                              | `true`        | No       | 0.2.0         |
| `gravitino.auxService.iceberg-rest.jdbc-driver`     | `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL, `org.postgresql.Driver` for PostgreSQL.                           | (none)        | Yes      | 0.3.0         |

:::caution
You must download the corresponding JDBC driver to the `catalogs/lakehouse-iceberg/libs` directory.
:::

### Other Apache Iceberg catalog properties

You can add other properties defined in [Iceberg catalog properties](https://iceberg.apache.org/docs/1.3.1/configuration/#catalog-properties).
The `clients` property for example:

| Configuration item                          | Description                          | Default value | Required |
|---------------------------------------------|--------------------------------------|---------------|----------|
| `gravitino.auxService.iceberg-rest.clients` | The client pool size of the catalog. | `2`           | No       |

:::info
`catalog-impl` has no effect.
:::

### HDFS configuration

The Gravitino Iceberg REST catalog service adds the HDFS configuration files, `core-site.xml` and `hdfs-site.xml` from the directory defined by `gravitino.auxService.iceberg-rest.classpath`, for example, `catalogs/lakehouse-iceberg/conf`, to the classpath.

## Starting the Gravitino Iceberg REST catalog service

Starting the Gravitino Iceberg REST catalog service:

```shell
./bin/gravitino.sh start
```

How to find out whether the Gravitino Iceberg REST catalog service has started:

```shell
curl  http://127.0.0.1:9001/iceberg/application.wadl
```

## Exploring the Gravitino and Apache Iceberg REST catalog service with Apache Spark

### Deploying Apache Spark with Apache Iceberg support

Follow the [Spark Iceberg start guide](https://iceberg.apache.org/docs/latest/getting-started/) to set up Apache Spark's and Apache Iceberg's environment. Please keep the Apache Spark version consistent with the `spark-iceberg-runtime` version.

### Starting the Apache Spark client with the Apache Iceberg REST catalog

| Configuration item                       | Description                                                               |
|------------------------------------------|---------------------------------------------------------------------------|
| `spark.sql.catalog.${catalog-name}.type` | The Spark catalog type, should set to `rest`.                             |
| `spark.sql.catalog.${catalog-name}.uri`  | Spark Iceberg REST catalog URI, such as `http://127.0.0.1:9001/iceberg/`. |

For example:

```shell
./bin/spark-shell -v \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog  \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/
```

### Exploring Apache Iceberg with Apache Spark SQL

```sql
// First change to use the `rest` catalog
USE rest;
CREATE DATABASE IF NOT EXISTS dml;
CREATE TABLE dml.test (id bigint COMMENT 'unique id') using iceberg
DESCRIBE TABLE EXTENDED dml.test;
INSERT INTO dml.test VALUES (1), (2);
SELECT * FROM dml.test
```

You could try Spark with Gravitino REST catalog service in our [playground](./how-to-use-the-playground.md#using-iceberg-rest-service).
