---
title: Apache Gravitino configuration
slug: /gravitino-server-config
keywords:
  - configuration
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino supports several configurations:

1. **Gravitino server configuration**: Used to start up the Gravitino server.
2. **Gravitino catalog properties configuration**: Used to make default values for different catalogs.
3. **Some other configurations**: Includes HDFS and other configurations.

## Apache Gravitino server configurations

You can customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases.
We strongly recommend that you read the following sections to understand the configuration file, so you can change the default values to suit your specific situation and usage scenario.

The `gravitino.conf` file lists the configuration items in the following table. It groups those items into the following categories:

### Apache Gravitino HTTP Server configuration

| Configuration item                                   | Description                                                                                                                                                                           | Default value                                                                | Required | Since version    |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|------------------|
| `gravitino.server.webserver.host`                    | The host of the Gravitino server.                                                                                                                                                     | `0.0.0.0`                                                                    | No       | 0.1.0            |
| `gravitino.server.webserver.httpPort`                | The port on which the Gravitino server listens for incoming connections.                                                                                                              | `8090`                                                                       | No       | 0.1.0            |
| `gravitino.server.webserver.minThreads`              | The minimum number of threads in the thread pool used by the Jetty webserver. `minThreads` is 8 if the value is less than 8.                                                          | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0            |
| `gravitino.server.webserver.maxThreads`              | The maximum number of threads in the thread pool used by the Jetty webserver. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`. | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.1.0            |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by the Jetty webserver.                                                                                                                 | `100`                                                                        | No       | 0.1.0            |
| `gravitino.server.webserver.stopTimeout`             | Time in milliseconds to gracefully shut down the Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`.                                              | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.idleTimeout`             | The timeout in milliseconds of idle connections.                                                                                                                                      | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.requestHeaderSize`       | Maximum size of HTTP requests.                                                                                                                                                        | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.webserver.responseHeaderSize`      | Maximum size of HTTP responses.                                                                                                                                                       | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.shutdown.timeout`                  | Time in milliseconds to gracefully shut down of the Gravitino webserver.                                                                                                              | `3000`                                                                       | No       | 0.2.0            |
| `gravitino.server.webserver.customFilters`           | Comma-separated list of filter class names to apply to the API.                                                                                                                       | (none)                                                                       | No       | 0.4.0            |
| `gravitino.server.rest.extensionPackages`            | Comma-separated list of REST API packages to expand                                                                                                                                   | (none)                                                                       | No       | 0.6.0-incubating |

The filter in the customFilters should be a standard javax servlet filter.
You can also specify filter parameters by setting configuration entries of the form `gravitino.server.webserver.<class name of filter>.param.<param name>=<value>`.

### Storage configuration

#### Storage backend configuration

Currently, Gravitino only supports JDBC database backend, and the default implementation is H2 database as it's an embedded database, has no external dependencies and is very suitable for local development or tests.
If you are going to use H2 in the production environment, Gravitino will not guarantee the data consistency and durability. It's highly recommended using MySQL as the backend database.  

The following table lists the storage configuration items:

| Configuration item                                | Description                                                                                                                                                                                                                                             | Default value                 | Required                                        | Since version    |
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|-------------------------------------------------|------------------|
| `gravitino.entity.store`                          | Which entity storage implementation to use. Only`relational` storage is currently supported.                                                                                                                                                            | `relational`                  | No                                              | 0.1.0            |
| `gravitino.entity.serde`                          | The serialization/deserialization class used to support entity storage. `proto' is currently supported.                                                                                                                                                 | `proto`                       | No                                              | 0.1.0            |
| `gravitino.entity.store.maxTransactionSkewTimeMs` | The maximum skew time of transactions in milliseconds.                                                                                                                                                                                                  | `2000`                        | No                                              | 0.3.0            |
| `gravitino.entity.store.kv.deleteAfterTimeMs`     | It is deprecated since Gravitino 0.5.0. Please use `gravitino.entity.store.deleteAfterTimeMs` instead.                                                                                                                                                  | `604800000`(7 days)           | No                                              | 0.3.0            |
| `gravitino.entity.store.deleteAfterTimeMs`        | The maximum time in milliseconds that deleted and old-version data is kept. Set to at least 10 minutes and no longer than 30 days.                                                                                                                      | `604800000`(7 days)           | No                                              | 0.5.0            |
| `gravitino.entity.store.versionRetentionCount`    | The Count of versions allowed to be retained, including the current version, used to delete old versions data. Set to at least 1 and no greater than 10.                                                                                                | `1`                           | No                                              | 0.5.0            |
| `gravitino.entity.store.relational`               | Detailed implementation of Relational storage. `H2`, `MySQL` and `PostgreSQL` is currently supported, and the implementation is `JDBCBackend`.                                                                                                          | `JDBCBackend`                 | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUrl`       | The database url that the `JDBCBackend` needs to connect to. If you use `MySQL` or `PostgreSQL`, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/{DATABASE_TYPE}/` directory. | `jdbc:h2`                     | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcDriver`    | The jdbc driver name that the `JDBCBackend` needs to use. You should place the driver Jar package in the `${GRAVITINO_HOME}/libs/` directory.                                                                                                           | `org.h2.Driver`               | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUser`      | The username that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                   | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcPassword`  | The password that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                   | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.storagePath`   | The storage path for embedded JDBC storage implementation. It supports both absolute and relative path, if the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is `${GRAVITINO_HOME}/data/jdbc`     | `${GRAVITINO_HOME}/data/jdbc` | No                                              | 0.6.0-incubating |


:::caution
We strongly recommend that you change the default value of `gravitino.entity.store.relational.storagePath`, as it's under the deployment directory and future version upgrades may remove it.
:::

#### Create JDBC backend schema and table 

For H2 database, All tables needed by Gravitino are created automatically when the Gravitino server starts up. For MySQL, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/mysql/` directory.

### Tree lock configuration

Gravitino server uses tree lock to ensure the consistency of the data. The tree lock is a memory lock (Currently, Gravitino only supports in memory lock) that can be used to ensure the consistency of the data in Gravitino server. The configuration items are as follows:

| Configuration item                   | Description                                                   | Default value | Required | Since Version |
|--------------------------------------|---------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.lock.maxNodes`            | The maximum number of tree lock nodes to keep in memory       | 100000        | No       | 0.5.0         |
| `gravitino.lock.minNodes`            | The minimum number of tree lock nodes to keep in memory       | 1000          | No       | 0.5.0         |
| `gravitino.lock.cleanIntervalInSecs` | The interval in seconds to clean up the stale tree lock nodes | 60            | No       | 0.5.0         |

### Catalog configuration

| Configuration item                           | Description                                                                                                                                                                                         | Default value | Required | Since version |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs` | The interval in milliseconds to evict the catalog cache; default 3600000ms(1h).                                                                                                                     | `3600000`     | No       | 0.1.0         |
| `gravitino.catalog.classloader.isolated`     | Whether to use an isolated classloader for catalog. If `true`, an isolated classloader loads all catalog-related libraries and configurations, not the AppClassLoader. The default value is `true`. | `true`        | No       | 0.1.0         |

### Auxiliary service configuration

| Configuration item            | Description                                                                                                                    | Default value | Since Version |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.names ` | The auxiliary service name of the Gravitino Iceberg REST server. Use **`iceberg-rest`** for the Gravitino Iceberg REST server. | (none)        | 0.2.0         |

Refer to [Iceberg REST catalog service](iceberg-rest-service.md) for configuration details.

### Event listener configuration

Gravitino provides event listener mechanism to allow users to capture the events which are provided by Gravitino server to integrate some custom operations.

To leverage the event listener, you must implement the `EventListenerPlugin` interface and place the JAR file in the classpath of the Gravitino server. Then, add configurations to gravitino.conf to enable the event listener.

| Property name                          | Description                                                                                            | Default value | Required | Since Version |
|----------------------------------------|--------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.eventListener.names`        | The name of the event listener, For multiple listeners, separate names with a comma, like "audit,sync" | (none)        | Yes      | 0.5.0         |
| `gravitino.eventListener.{name}.class` | The class name of the event listener, replace `{name}` with the actual listener name.                  | (none)        | Yes      | 0.5.0         | 
| `gravitino.eventListener.{name}.{key}` | Custom properties that will be passed to the event listener plugin.                                    | (none)        | Yes      | 0.5.0         | 

#### Event

Gravitino triggers a specific event upon the completion of the operation, with varying events being generated for different operations.

| operation type     | event                                                                                                                                                                                                                                                                           | 
|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table operation    | `CreateTableEvent`, `AlterTableEvent`, `DropTableEvent`, `LoadTableEvent`, `ListTableEvent`, `PurgeTableFailureEvent`, `CreateTableFailureEvent`, `AlterTableFailureEvent`, `DropTableFailureEvent`, `LoadTableFailureEvent`, `ListTableFailureEvent`, `PurgeTableFailureEvent` |
| fileset operation  | `CreateFileSetEvent`, `AlterFileSetEvent`, `DropFileSetEvent`, `LoadFileSetEvent`, `ListFileSetEvent`, `CreateFileSetFailureEvent`, `AlterFileSetFailureEvent`, `DropFileSetFailureEvent`, `LoadFileSetFailureEvent`, `ListFileSetFailureEvent`                                 |
| topic operation    | `CreateTopicEvent`, `AlterTopicEvent`, `DropTopicEvent`, `LoadTopicEvent`, `ListTopicEvent`, `CreateTopicFailureEvent`, `AlterTopicFailureEvent`, `DropTopicFailureEvent`, `LoadTopicFailureEvent`, `ListTopicFailureEvent`                                                     |
| schema operation   | `CreateSchemaEvent`, `AlterSchemaEvent`, `DropSchemaEvent`, `LoadSchemaEvent`, `ListSchemaEvent`, `CreateSchemaFailureEvent`, `AlterSchemaFailureEvent`, `DropSchemaFailureEvent`, `LoadSchemaFailureEvent`, `ListSchemaFailureEvent`                                           |
| catalog operation  | `CreateCatalogEvent`, `AlterCatalogEvent`, `DropCatalogEvent`, `LoadCatalogEvent`, `ListCatalogEvent`, `CreateCatalogFailureEvent`, `AlterCatalogFailureEvent`, `DropCatalogFailureEvent`, `LoadCatalogFailureEvent`, `ListCatalogFailureEvent`                                 |
| metalake operation | `CreateMetalakeEvent`, `AlterMetalakeEvent`, `DropMetalakeEvent`, `LoadMetalakeEvent`, `ListMetalakeEvent`, `CreateMetalakeFailureEvent`, `AlterMetalakeFailureEvent`, `DropMetalakeFailureEvent`, `LoadMetalakeFailureEvent`, `ListMetalakeFailureEvent`                       |

#### Event listener plugin

The `EventListenerPlugin` defines an interface for event listeners that manage the lifecycle and state of a plugin. This includes handling its initialization, startup, and shutdown processes, as well as handing events triggered by various operations.

The plugin provides several operational modes for how to process event, supporting both synchronous and asynchronous processing approaches.

- **SYNC**: Events are processed synchronously, immediately following the associated operation. This mode ensures events are processed before the operation's result is returned to the client, but it may delay the main process if event processing takes too long.

- **ASYNC_SHARED**: This mode employs a shared queue and dispatcher for asynchronous event processing. It prevents the main process from being blocked, though there's a risk events might be dropped if not promptly consumed. Sharing a dispatcher can lead to poor isolation in case of slow listeners.
 
- **ASYNC_ISOLATED**: Events are processed asynchronously, with each listener having its own dedicated queue and dispatcher thread. This approach offers better isolation but at the expense of multiple queues and dispatchers.

For more details, please refer to the definition of the plugin.

### Audit log configuration

The audit log framework defines how audit logs are formatted and written to various storages. The formatter defines an interface that transforms different `Event` types into a unified `AuditLog`. The writer defines an interface to writing AuditLog to different storages.

Gravitino provides a default implement to log basic audit information to a file, you could extend the audit system by implementation corresponding interfaces.

| Property name                         | Description                            | Default value                               | Required | Since Version              |
|---------------------------------------|----------------------------------------|---------------------------------------------|----------|----------------------------|
| `gravitino.audit.enabled`             | The audit log enable flag.             | false                                       | NO       | 0.7.0-incubating           |
| `gravitino.audit.writer.className`    | The class name of audit log writer.    | org.apache.gravitino.audit.FileAuditWriter  | NO       | 0.7.0-incubating           | 
| `gravitino.audit.formatter.className` | The class name of audit log formatter. | org.apache.gravitino.audit.SimpleFormatter  | NO       | 0.7.0-incubating           | 

#### Audit log formatter

The Formatter defines an interface that formats metadata audit logs into a unified format. `SimpleFormatter` is a default implement to format audit information, you don't need to do extra configurations.

#### Audit log writer

The `AuditLogWriter` defines an interface that enables the writing of metadata audit logs to different storage mediums such as files, databases, etc.

Writer configuration begins with `gravitino.audit.writer.${name}`, where ${name} is replaced with the actual writer name defined in method `name()`. `FileAuditWriter` is a default implement to log audit information, whose name is `file`.

| Property name                                       | Description                                                                             | Default value       | Required | Since Version              |
|-----------------------------------------------------|-----------------------------------------------------------------------------------------|---------------------|----------|----------------------------|
| `gravitino.audit.writer.file.fileName`              | The audit log file name, the path is ${sys:gravitino.log.path}/${fileName}.             | gravitino_audit.log | NO       | 0.7.0-incubating           |
| `gravitino.audit.writer.file.append`                | Whether the log will be written to the end or the beginning of the file.                | true                | NO       | 0.7.0-incubating           |

### Security configuration

Refer to [security](security/security.md) for HTTPS and authentication configurations.

### Metrics configuration

| Property name                             | Description                                          | Default value | Required | Since Version |
|-------------------------------------------|------------------------------------------------------|---------------|----------|---------------|
| `gravitino.metrics.timeSlidingWindowSecs` | The seconds of Gravitino metrics time sliding window | 60            | No       | 0.5.1         |

## Apache Gravitino catalog properties configuration

There are three types of catalog properties:

1. **Gravitino defined properties**: Gravitino defines these properties as the necessary
   configurations for the catalog to work properly.
2. **Properties with the `gravitino.bypass.` prefix**: These properties are not managed by
   Gravitino and pass directly to the underlying system for advanced usage.
3. **Other properties**: Gravitino doesn't leverage these properties, just store them. Users
   can use them for their own purposes.

Catalog properties are either defined in catalog configuration files as default values or
specified as `properties` explicitly when creating a catalog.

:::info
The catalog properties explicitly specified in the `properties` field take precedence over the
default values in the catalog configuration file.

These rules only apply to the catalog properties and don't affect the schema or table properties.
:::

Below is a list of catalog properties that will be used by all Gravitino catalogs:

| Configuration item  | Description                                                                                                                                                                                                                                                | Default value | Required | Since version    |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `package`           | The path of the catalog package, Gravitino leverages this path to load the related catalog libs and configurations. The package should consist two folders, `conf` (for catalog related configurations) and `libs` (for catalog related dependencies/jars) | (none)        | No       | 0.5.0            |
| `cloud.name`        | The property to specify the cloud that the catalog is running on. The valid values are `aws`, `azure`, `gcp`, `on_premise` and `other`.                                                                                                                    | (none)        | No       | 0.6.0-incubating |
| `cloud.region-code` | The property to specify the region code of the cloud that the catalog is running on.                                                                                                                                                                       | (none)        | No       | 0.6.0-incubating |


The following table lists the catalog specific properties and their default paths:

| catalog provider    | catalog properties                                                                      | catalog properties configuration file path               |
|---------------------|-----------------------------------------------------------------------------------------|----------------------------------------------------------|
| `hive`              | [Hive catalog properties](apache-hive-catalog.md#catalog-properties)                    | `catalogs/hive/conf/hive.conf`                           |
| `lakehouse-iceberg` | [Lakehouse Iceberg catalog properties](lakehouse-iceberg-catalog.md#catalog-properties) | `catalogs/lakehouse-iceberg/conf/lakehouse-iceberg.conf` |
| `jdbc-mysql`        | [MySQL catalog properties](jdbc-mysql-catalog.md#catalog-properties)                    | `catalogs/jdbc-mysql/conf/jdbc-mysql.conf`               |
| `jdbc-postgresql`   | [PostgreSQL catalog properties](jdbc-postgresql-catalog.md#catalog-properties)          | `catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`     |
| `jdbc-doris`        | [Doris catalog properties](jdbc-doris-catalog.md#catalog-properties)                    | `catalogs/jdbc-doris/conf/jdbc-doris.conf`               |
| `lakehouse-paimon`  | [Lakehouse Paimon catalog properties](lakehouse-paimon-catalog.md#catalog-properties)   | `catalogs/lakehouse-paimon/conf/lakehouse-paimon.conf`   |
| `kafka`             | [Kafka catalog properties](kafka-catalog.md#catalog-properties)                         | `catalogs/kafka/conf/kafka.conf`                         |

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
