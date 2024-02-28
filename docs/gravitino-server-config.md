---
title: Gravitino configuration
slug: /gravitino-server-config
keywords:
  - configuration
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

## Introduction

Gravitino supports several configurations:

1. **Gravitino server configuration**: Used to start up the Gravitino server.
2. **Gravitino catalog properties configuration**: Used to make default values for different catalogs.
3. **Some other configurations**: Includes HDFS and other configurations.

## Gravitino server configurations

You can customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases.
We strongly recommend that you read the following sections to understand the configuration file so you can change the default values to suit your specific situation and usage scenario.

The `gravitino.conf` file lists the configuration items in the following table. It groups those items into the following categories:

### Gravitino HTTP Server configuration

| Configuration item                                    | Description                                                                                                                                                                       | Default value                                                                | Required | Since version |
|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|---------------|
| `gravitino.server.webserver.host`                     | The host of the Gravitino server.                                                                                                                                                     | `0.0.0.0`                                                                    | No       | 0.1.0         |
| `gravitino.server.webserver.httpPort`                 | The port on which the Gravitino server listens for incoming connections.                                                                                                          | `8090`                                                                       | No       | 0.1.0         |
| `gravitino.server.webserver.minThreads`               | The minimum number of threads in the thread pool used by the Jetty webserver. `minThreads` is 8 if the value is less than 8.                                                          | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0         |
| `gravitino.server.webserver.maxThreads`               | The maximum number of threads in the thread pool used by the Jetty webserver. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`. | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.1.0         |
| `gravitino.server.webserver.threadPoolWorkQueueSize`  | The size of the queue in the thread pool used by the Jetty webserver.                                                                                                                 | `100`                                                                        | No       | 0.1.0         |
| `gravitino.server.webserver.stopTimeout`              | Time in milliseconds to gracefully shut down the Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`.                                           | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.server.webserver.idleTimeout`              | The timeout in milliseconds of idle connections.                                                                                                                                  | `30000`                                                                      | No       | 0.2.0         |
| `gravitino.server.webserver.requestHeaderSize`        | Maximum size of HTTP requests.                                                                                                                                                    | `131072`                                                                     | No       | 0.1.0         |
| `gravitino.server.webserver.responseHeaderSize`       | Maximum size of HTTP responses.                                                                                                                                                   | `131072`                                                                     | No       | 0.1.0         |
| `gravitino.server.shutdown.timeout`                   | Time in milliseconds to gracefully shut down of the Gravitino webserver.                                                                                                           | `3000`                                                                       | No       | 0.2.0         |
| `gravitino.server.webserver.customFilters`            | Comma-separated list of filter class names to apply to the API.                                                                                                                  | (none)                                                                       | No       | 0.4.0         |

The filter in the customFilters should be a standard javax servlet filter.
You can also specify filter parameters by setting configuration entries of the form `gravitino.server.webserver.<class name of filter>.param.<param name>=<value>`.

### Storage configuration

| Configuration item                                | Description                                                                                                                                                                                                                                                | Default value                    | Required                         | Since version |
|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------|----------------------------------|---------------|
| `gravitino.entity.store`                          | Which storage implementation to use. Key-value pair storage and relational storage are currently supported, the default value is `kv`, and the optional value is `relational`.                                                                             | `kv`                             | No                               | 0.1.0         |
| `gravitino.entity.store.kv`                       | Detailed implementation of KV storage. `RocksDB` storage is currently supported, and the implementation is `RocksDBKvBackend`.                                                                                                                             | `RocksDBKvBackend`               | No                               | 0.1.0         |
| `gravitino.entity.store.kv.rocksdbPath`           | The storage path for RocksDB storage implementation. It supports both absolute and relative path, if the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is `${GRAVITINO_HOME}/data/rocksdb`           | `${GRAVITINO_HOME}/data/rocksdb` | No                               | 0.1.0         |
| `graivitino.entity.serde`                         | The serialization/deserialization class used to support entity storage. `proto' is currently supported.                                                                                                                                                    | `proto`                          | No                               | 0.1.0         |
| `gravitino.entity.store.maxTransactionSkewTimeMs` | The maximum skew time of transactions in milliseconds.                                                                                                                                                                                                     | `2000`                           | No                               | 0.3.0         |
| `gravitino.entity.store.kv.deleteAfterTimeMs`     | The maximum time in milliseconds that deleted and old-version data is kept. Set to at least 10 minutes and no longer than 30 days.                                                                                                                         | `604800000`(7 days)              | No                               | 0.3.0         |
| `gravitino.entity.store.relational`               | Detailed implementation of Relational storage. `MySQL` is currently supported, and the implementation is `JDBCBackend`.                                                                                                                                    | `JDBCBackend`                    | No                               | 0.5.0         |
| `gravitino.entity.store.relational.jdbcUrl`       | The database url that the `JDBCBackend` needs to connect to. If you use `MySQL`, you should firstly initialize the database tables yourself by executing the file named `mysql_init.sql` in the `src/main/resources/mysql` directory of the `core` module. | (none)                           | Yes if you use `JdbcBackend`     | 0.5.0         |
| `gravitino.entity.store.relational.jdbcDriver`    | The jdbc driver name that the `JDBCBackend` needs to use. You should place the driver Jar package in the `${GRAVITINO_HOME}/libs/` directory.                                                                                                              | (none)                           | Yes if you use `JdbcBackend`     | 0.5.0         |
| `gravitino.entity.store.relational.jdbcUser`      | The username that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                                 | (none)                           | Yes if you use `JdbcBackend`     | 0.5.0         |
| `gravitino.entity.store.relational.jdbcPassword`  | The password that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                                 | (none)                           | Yes if you use `JdbcBackend`     | 0.5.0         |

:::caution
We strongly recommend that you change the default value of `gravitino.entity.store.kv.rocksdbPath`, as it's under the deployment directory and future version upgrades may remove it.
:::

### Catalog configuration

| Configuration item                           | Description                                                                                                                                                                                       | Default value | Required | Since version |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs` | The interval in milliseconds to evict the catalog cache; default 3600000ms(1h).                                                                                                                    | `3600000`     | No       | 0.1.0         |
| `gravitino.catalog.classloader.isolated`     | Whether to use an isolated classloader for catalog. If `true`, an isolated classloader loads all catalog-related libraries and configurations, not the AppClassLoader. The default value is `true`. | `true`        | No       | 0.1.0         |

### Auxiliary service configuration

| Configuration item            | Description                                                                                                                    | Default value | Since Version |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.names ` | The auxiliary service name of the Gravitino Iceberg REST server. Use **`iceberg-rest`** for the Gravitino Iceberg REST server. | (none)        | 0.2.0         |

Refer to [Iceberg REST catalog service](iceberg-rest-service.md) for configuration details.

### Security configuration

Refer to [security](security.md) for HTTPS and authentication configurations.

## Gravitino catalog properties configuration

There are three types of catalog properties:

1. **Gravitino-defined properties**: These properties simplify the catalog creation process.
2. **Properties with the `gravitino.bypass.` prefix**: These properties aren't managed by Gravitino; instead, they bypass the underlying system for advanced usage.
3. **Other properties**: Stored in Gravitino storage, these properties don't bypass the underlying system.

Catalog properties are either defined in catalog configuration files as default values or specified explicitly when creating a catalog.

:::info
Explicit specifications take precedence over formal configurations.
:::

:::caution
These rules only apply to the catalog properties and don't affect the schema or table properties.
:::

| catalog provider    | catalog properties                                                                      | catalog properties configuration file path               |
|---------------------|-----------------------------------------------------------------------------------------|----------------------------------------------------------|
| `hive`              | [Hive catalog properties](apache-hive-catalog.md#catalog-properties)                    | `catalogs/hive/conf/hive.conf`                           |
| `lakehouse-iceberg` | [Lakehouse Iceberg catalog properties](lakehouse-iceberg-catalog.md#catalog-properties) | `catalogs/lakehouse-iceberg/conf/lakehouse-iceberg.conf` |
| `jdbc-mysql`        | [MySQL catalog properties](jdbc-mysql-catalog.md#catalog-properties)                    | `catalogs/jdbc-mysql/conf/jdbc-mysql.conf`               |
| `jdbc-postgresql`   | [PostgreSQL catalog properties](jdbc-postgresql-catalog.md#catalog-properties)          | `catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`     |

:::info
The Gravitino server automatically adds the catalog properties configuration directory to classpath.
:::

## Some other configurations

You could put HDFS configuration file to the catalog properties configuration dir, like `catalogs/lakehouse-iceberg/conf/`.

## How to set up runtime environment variables

The Gravitino server lets you set up runtime environment variables by editing the `gravitino-env.sh` file, located in the `conf` directory.

### How to access Apache Hadoop

Currently, due to the absence of a comprehensive user permission system, Gravitino can only use a single username for
Apache Hadoop access. Ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN, etc.) access
permissions; otherwise, you may encounter a `Permission denied` error. There are two ways to resolve this error:

* Grant Gravitino startup user permissions in Hadoop
* Specify the authorized Hadoop username in the environment variables `HADOOP_USER_NAME` before starting the Gravitino server.
