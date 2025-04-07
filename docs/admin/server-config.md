---
title: Apache Gravitino config 
slug: /gravitino-server-config
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino supports several configurations:

1. **Gravitino server configuration**: Used to start up the Gravitino server.
1. **Gravitino catalog properties configuration**: Used to make default values for different catalogs.
1. **Some other configurations**: Includes HDFS and other configurations.

## Apache Gravitino server configurations

You can customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory.
The default values are sufficient for most use cases.
We strongly recommend that you read the following sections to understand the configuration file,
so you can change the default values to suit your specific situation and usage scenario.

The `gravitino.conf` file lists the configuration items in the following table.
It groups those items into the following categories:

### Apache Gravitino HTTP Server configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.server.webserver.host</tt></td>
  <td>The host of the Gravitino server.</td>
  <td>`0.0.0.0`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.httpPort</tt></td>
  <td>The port on which the Gravitino server listens for incoming connections.</td>
  <td>`8090`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.minThreads</tt></td>
  <td>The minimum number of threads in the thread pool used by the Jetty webserver.
      `minThreads` is 8 if the value is less than 8.</td>
  <td>`Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)`</td>
  <td>No</td>
  <td>0.2.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.maxThreads</tt></td>
  <td>The maximum number of threads in the thread pool used by the Jetty webserver.
      `maxThreads` is 8 if the value is less than 8,
      and `maxThreads` must be great or equal to `minThreads`.</td>
  <td>`Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.threadPoolWorkQueueSize</tt></td>
  <td>The size of the queue in the thread pool used by the Jetty webserver.</td>
  <td>`100`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.stopTimeout</tt></td>
  <td>Time in milliseconds to gracefully shut down the Jetty webserver.
      Check `org.eclipse.jetty.server.Server#setStopTimeout` for more details.</td>
  <td>`30000`</td>
  <td>No</td>
  <td>0.2.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.idleTimeout</tt></td>
  <td>The timeout in milliseconds of idle connections.</td>
  <td>`30000`</td>
  <td>No</td>
  <td>0.2.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.requestHeaderSize</tt></td>
  <td>Maximum size of HTTP requests.</td>
  <td>`131072`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.responseHeaderSize</tt></td>
  <td>Maximum size of HTTP responses.</td>
  <td>`131072`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.shutdown.timeout</tt></td>
  <td>Time in milliseconds to gracefully shut down of the Gravitino webserver.</td>
  <td>`3000`</td>
  <td>No</td>
  <td>0.2.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.customFilters</tt></td>
  <td>Comma-separated list of filter class names to apply to the API.</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.4.0</td>
</tr>
<tr>
  <td><tt>gravitino.server.rest.extensionPackages</tt></td>
  <td>Comma-separated list of REST API packages to expand.</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.6.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.server.visibleConfigs</tt></td>
  <td>List of configs that are visible in the config servlet.</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.9.0-incubating</td>
</tr>
</tbody>
</table>

The filter in the customFilters should be a standard javax servlet filter.
You can also specify filter parameters by setting configuration entries
in the format `gravitino.server.webserver.<filter class name>.param.<name>=<value>`.

### Storage configuration

#### Storage backend configuration

Currently, Gravitino only supports JDBC database backend.
The default implementation is H2 database as it's an embedded database,
has no external dependencies and is very suitable for local development or tests.
If you are going to use H2 in the production environment,
Gravitino will not guarantee the data consistency and durability.
It's highly recommended using MySQL as the backend database.  

The following table lists the storage configuration items:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.entity.store</tt></td>
  <td>Which entity storage implementation to use. Only`relational` storage is currently supported.</td>
  <td>`relational`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.maxTransactionSkewTimeMs</tt></td>
  <td>The maximum skew time of transactions in milliseconds.</td>
  <td>`2000`</td>
  <td>No</td>
  <td>0.3.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.deleteAfterTimeMs</tt></td>
  <td>The maximum time in milliseconds that deleted and old-version data is kept.
      Set to at least 10 minutes and no longer than 30 days.</td>
  <td>`604800000`(7 days)</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.versionRetentionCount</tt></td>
  <td>The Count of versions allowed to be retained, including the current version,
      used to delete old versions data. Set to at least 1 and no greater than 10.</td>
  <td>`1`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational</tt></td>
  <td>Detailed implementation of Relational storage.
      `H2`, `MySQL` and `PostgreSQL` are currently supported,
      and the implementation is `JDBCBackend`.</td>
  <td>`JDBCBackend`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.jdbcUrl</tt></td>
  <td>The database url that the `JDBCBackend` needs to connect to.
      If you use `MySQL` or `PostgreSQL`, you should firstly initialize the database tables yourself
      by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/{DATABASE_TYPE}/` directory.</td>
  <td>`jdbc:h2`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.jdbcDriver</tt></td>
  <td>The JDBC driver name that the `JDBCBackend` needs to use.
      You should place the driver Jar package in the `${GRAVITINO_HOME}/libs/` directory.
      This options is needed if the jdbc connection url is not `jdbc:h2`</td>
  <td>`org.h2.Driver`</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.jdbcUser</tt></td>
  <td>The username that the `JDBCBackend` needs to use when connecting the database.
      It is required for `MySQL`.
      It is required if the JDBC connection URL is not `jdbc:h2`</td>
  <td>`gravitino`</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.jdbcPassword</tt></td>
  <td>The password that the `JDBCBackend` needs to use when connecting the database.
      It is required for `MySQL`.
      It is required if the JDBC connection URL is not `jdbc:h2`.</td>
  <td>`gravitino`</td>
  <td>Yes|No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.storagePath</tt></td>
  <td>The storage path for embedded JDBC storage implementation.
      It supports both absolute and relative path.
      If the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`.
      Default value is `${GRAVITINO_HOME}/data/jdbc`.</td>
  <td>`${GRAVITINO_HOME}/data/jdbc`</td>
  <td>No</td>
  <td>0.6.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.maxConnections</tt></td>
  <td>The maximum number of connections for the JDBC Backend connection pool.</td>
  <td>`100`</td>
  <td>No</td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.entity.store.relational.maxWaitMillis</tt></td>
  <td>The maximum wait time in milliseconds for a connection from the JDBC Backend connection pool.</td>
  <td>`1000`</td>
  <td>No</td>
  <td>0.9.0-incubating</td>
</tr>
</tbody>
</table>

:::caution
We strongly recommend that you change the default value for `gravitino.entity.store.relational.storagePath`,
as it's under the deployment directory and future version upgrades may remove it.
:::

#### Create JDBC backend schema and table 

For H2 database, All tables needed by Gravitino are created automatically when the Gravitino server starts up.
For MySQL, you should firstly initialize the database tables yourself
by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/mysql/` directory.

### Tree lock configuration

Gravitino server uses tree lock to ensure the consistency of the data.
The tree lock is a memory lock (Currently, Gravitino only supports in memory lock)
that can be used to ensure the consistency of the data in Gravitino server.
The configuration items are as follows:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.lock.maxNodes</tt></td>
  <td>The maximum number of tree lock nodes to keep in memory.</td>
  <td>`100000`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.lock.minNodes</tt></td>
  <td>The minimum number of tree lock nodes to keep in memory.</td>
  <td>`1000`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.lock.cleanIntervalInSecs</tt></td>
  <td>The interval in seconds to clean up the stale tree lock nodes.</td>
  <td>`60`</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
</tbody>
</table>

### Catalog configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.catalog.cache.evictionIntervalMs</tt></td>
  <td>The interval in milliseconds to evict the catalog cache.</td>
  <td>`3600000` (1 hour)</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
<tr>
  <td><tt>gravitino.catalog.classloader.isolated</tt></td>
  <td>Whether to use an isolated classloader for catalog.
      If `true`, an isolated classloader loads all catalog-related libraries and configurations,
      not the AppClassLoader.</td>
  <td>`true`</td>
  <td>No</td>
  <td>0.1.0</td>
</tr>
</tbody>
</table>

### Auxiliary service configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.auxService.names</tt></td>
  <td>The auxiliary service name of the Gravitino Iceberg REST server.
    Use **`iceberg-rest`** for the Gravitino Iceberg REST server.</td>
  <td>(none)</td>
  <td>0.2.0</td>
</tr>
</tbody>
</table>

Refer to [Iceberg REST catalog service](./iceberg-server.md) for configuration details.

### Event listener configuration

Gravitino provides event listener mechanism to allow users to capture the events
which are emitted from the Gravitino server to integrate some custom operations.
For details about the events that are generated from the Gravitino server,
check the [event reference](../reference/events.md).

To leverage the event listening mechanism, you must implement the `EventListenerPlugin` interface
and place the JAR file in the CLASSPATH of the Gravitino server.
Then, add configurations to `conf/gravitino.conf` to enable the event listener.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.eventListener.names</tt></td>
  <td>The name of the event listener.
      For multiple listeners, separate names with a comma, e.g. "audit,sync".</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.eventListener.&lt;name&gt;.class</tt></td>
  <td>The class name of the event listener, replace `<name>` with the actual listener name.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>gravitino.eventListener.&lt;name&gt;.&lt;key&gt;</tt></td>
  <td>Custom properties that will be passed to the event listener plugin.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>0.5.0</td>
</tr>
</tbody>
</table>

#### Event listener plugin

The `EventListenerPlugin` defines an interface for event listeners
that manage the lifecycle and state of a plugin.
This includes handling its initialization, startup, and shutdown processes,
as well as handing events triggered by various operations.

The plugin provides several operational modes for how to process event,
supporting both synchronous and asynchronous processing approaches.

- **SYNC**: Events are processed synchronously, immediately following the associated operation.
  This mode ensures events are processed before the operation's result is returned to the client,
  but it may delay the main process if event processing takes too long.

- **ASYNC_SHARED**: This mode employs a shared queue and dispatcher for asynchronous event processing.
  It prevents the main process from being blocked, though there's a risk events might be dropped if not promptly consumed.
  Sharing a dispatcher can lead to poor isolation in case of slow listeners.
 
- **ASYNC_ISOLATED**: Events are processed asynchronously,
  with each listener having its own dedicated queue and dispatcher thread.
  This approach offers better isolation but at the expense of multiple queues and dispatchers.

When processing pre-event, you could throw a `ForbiddenException` to skip the following executions.
For more details, please refer to the definition of the plugin.

### Audit log configuration

The audit log framework defines how audit logs are formatted and written to various storages.
The formatter defines an interface that transforms different `Event` types into a unified `AuditLog`.
The writer defines an interface to writing AuditLog to different storages.

Gravitino provides a default implement to log basic audit information to a file.
You could extend the audit system by implementation corresponding interfaces.

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.audit.enabled</tt></td>
  <td>The audit log enable flag.</td>
  <td>false</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.audit.writer.className</tt></td>
  <td>The class name of audit log writer.</td>
  <td>`org.apache.gravitino.audit.FileAuditWriter`</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.audit.formatter.className</tt></td>
  <td>The class name of audit log formatter.</td>
  <td>`org.apache.gravitino.audit.SimpleFormatter`</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
</tbody>
</table>

#### Audit log formatter

The Formatter defines an interface that formats metadata audit logs into a unified format.
`SimpleFormatter` is a default implement to format audit information,
you don't need to do extra configuration operations.

#### Audit log writer

The `AuditLogWriter` defines an interface that enables the writing of metadata audit logs
to different storage mediums such as files, databases, etc.

Writer configuration begins with `gravitino.audit.writer.${name}`,
where `${name}` is replaced with the actual writer name defined in method `name()`.
`FileAuditWriter` is a default implement to log audit information, whose name is `file`.

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.audit.writer.file.fileName</tt></td>
  <td>The audit log file name, the path is `${sys:gravitino.log.path}/${fileName}`.</td>
  <td>`gravitino_audit.log`</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.audit.writer.file.flushIntervalSecs</tt></td>
  <td>The flush interval time of the audit file in seconds.</td>
  <td>`10`</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td><tt>gravitino.audit.writer.file.append</tt></td>
  <td>Whether the log will be written to the end or the beginning of the file.</td>
  <td>`true`</td>
  <td>No</td>
  <td>0.7.0-incubating</td>
</tr>
</tbody>
</table>

### Security configuration

Refer to [security](../security/index.md) for HTTPS and authentication configurations.

### Metrics configuration

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.metrics.timeSlidingWindowSecs</tt></td>
  <td>The seconds of Gravitino metrics time sliding window.</td>
  <td>`60`</td>
  <td>No</td>
  <td>0.5.1</td>
</tr>
</tbody>
</table>

## Apache Gravitino catalog properties configuration

There are three types of catalog properties:

1. **Gravitino defined properties**: Gravitino defines these properties as the necessary configurations
   for the catalog to work properly.
2. **Properties with the `gravitino.bypass.` prefix**: These properties are not managed by Gravitino
   and pass directly to the underlying system for advanced usage.
3. **Other properties**: Gravitino doesn't leverage these properties, just store them.
   Users can use them for their own purposes.

Catalog properties are either defined in catalog configuration files as default values
or specified as `properties` explicitly when creating a catalog.

:::info
The catalog properties explicitly specified in the `properties` field take precedence
over the default values in the catalog configuration file.

These rules only apply to the catalog properties and don't affect the schema or table properties.
:::

Below is a list of catalog properties that will be used by all Gravitino catalogs:

<table>
<thead>
<tr>
  <th>Configuration Option</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>package</tt></td>
  <td>The path of the catalog package.
    Gravitino leverages this path to load the related catalog libs and configurations.
    The package should consist two folders, `conf` (for catalog related configurations)
    and `libs` (for catalog related dependencies/JARs).</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.5.0</td>
</tr>
<tr>
  <td><tt>cloud.name</tt></td>
  <td>The property to specify the cloud that the catalog is running on.
    The valid values are `aws`, `azure`, `gcp`, `on_premise` and `other`.</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.6.0-incubating</td>
</tr>
<tr>
  <td><tt>cloud.region-code</tt></td>
  <td>The property to specify the region code of the cloud that the catalog is running on.</td>
  <td>(none)</td>
  <td>No</td>
  <td>0.6.0-incubating</td>
</tr>
</tbody>
</table>

The following table lists the catalog specific properties and their default paths:

<table>
<thead>
<tr>
  <th>Catalog provider</th>
  <th>Catalog prperties</th>
  <th>Catalog properties config file</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hadoop</tt></td>
  <td>[Apache Hadoop](../catalogs/fileset/hadoop/hadoop-catalog.md#catalog-properties)</td>
  <td>`catalogs/hadoop/conf/hadoop.conf`</td>
</tr>
<tr>
  <td><tt>hive</tt></td>
  <td>[Apache Hive](../catalogs/relational/hive/index.md#catalog-properties)</td>
  <td>`catalogs/hive/conf/hive.conf`</td>
</tr>
<tr>
  <td><tt>jdbc-doris</tt></td>
  <td>[Doris](../catalogs/relational/jdbc/doris.md#catalog-properties)</td>
  <td>`catalogs/jdbc-doris/conf/jdbc-doris.conf`</td>
</tr>
<tr>
  <td><tt>jdbc-mysql</tt></td>
  <td>[MySQL](../catalogs/relational/jdbc/mysql.md#catalog-properties)</td>
  <td>`catalogs/jdbc-mysql/conf/jdbc-mysql.conf`</td>
</tr>
<tr>
  <td><tt>jdbc-oceanbase</tt></td>
  <td>[OceanBase](../catalogs/relational/jdbc/oceanbase.md#catalog-properties)</td>
  <td>`catalogs/jdbc-oceanbase/conf/jdbc-oceanbase.conf`</td>
</tr>
<tr>
  <td><tt>jdbc-postgresql</tt></td>
  <td>[PostgreSQL](../catalogs/relational/jdbc/postgresql.md#catalog-properties)</td>
  <td>`catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`</td>
</tr>
<tr>
  <td><tt>kafka</tt></td>
  <td>[Kafka](../catalogs/messaging/kafka/index.md#catalog-properties)</td>
  <td>`catalogs/kafka/conf/kafka.conf`</td>
</tr>
<tr>
  <td><tt>lakehouse-hudi</tt></td>
  <td>[Lakehouse Hudi](../catalogs/relational/lakehouse/hudi.md#catalog-properties)</td>
  <td>`catalogs/lakehouse-hudi/conf/lakehouse-hudi.conf`</td>
</tr>
<tr>
  <td><tt>lakehouse-iceberg</tt></td>
  <td>[Lakehouse Iceberg](../catalogs/relational/lakehouse/iceberg.md#catalog-properties)</td>
  <td>`catalogs/lakehouse-iceberg/conf/lakehouse-iceberg.conf`</td>
</tr>
<tr>
  <td><tt>lakehouse-paimon</tt></td>
  <td>[Lakehouse Paimon](../catalogs/relational/lakehouse/paimon.md#catalog-properties)</td>
  <td>`catalogs/lakehouse-paimon/conf/lakehouse-paimon.conf`</td>
</tr>
</tbody>
</table>

:::info
The Gravitino server automatically adds the catalog properties configuration directory to classpath.
:::

## Other configurations

You could put HDFS configuration file to the catalog properties configuration dir,
like `catalogs/lakehouse-iceberg/conf/`.

### How to set up runtime environment variables

The Gravitino server lets you set up runtime environment variables
by editing the `gravitino-env.sh` file, located in the `conf` directory.

### How to access Apache Hadoop

Currently, due to the absence of a comprehensive user permission system,
Gravitino can only use a single username for Apache Hadoop access.
Ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN, etc.) access permissions;
otherwise, you may encounter a `Permission denied` error.
There are two ways to resolve this error:

* Grant Gravitino startup user permissions in Hadoop
* Specify the authorized Hadoop username in the environment variables `HADOOP_USER_NAME`
  before starting the Gravitino server.


