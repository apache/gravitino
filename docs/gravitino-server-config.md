---
title: "Gravitino Server Configuration"
slug: "/gravitino-server-config"
keywords:
  - configuration
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino supports several configurations:

1. **Gravitino server configuration**: Used to start up the Gravitino server.
2. **Gravitino catalog properties configuration**: Used to make default values for different catalogs.
3. **Some other configurations**: Includes HDFS and other configurations.

## Example Configurations

The examples below show typical contents of `${GRAVITINO_HOME}/conf/gravitino.conf` for two common scenarios. Each property is documented in detail in the sections that follow.

### Development

Defaults are appropriate for local development with the embedded H2 backend. Opt in to the Iceberg REST server for local query-engine testing.

```properties
# Sample gravitino.conf (development)

# Iceberg REST server: auxiliary mode with shared Gravitino catalogs
gravitino.auxService.names=iceberg-rest
gravitino.iceberg-rest.catalog-config-provider=dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake=test

# Authentication defaults to `simple` mode (anonymous user).
# See the Authentication section below to enable OAuth or Kerberos.
```

### Production

Configuration for production load: externally managed MySQL backend, larger cache and lock limits, audit logging, and OAuth authentication. Properties matching their default values are omitted.

```properties
# Sample gravitino.conf (production)

# Storage backend: externally managed MySQL (overrides default embedded H2)
gravitino.entity.store.relational.jdbcUrl=jdbc:mysql://gravitino-db.example.com:3306/gravitino
gravitino.entity.store.relational.jdbcDriver=com.mysql.cj.jdbc.Driver
gravitino.entity.store.relational.jdbcUser=<your-mysql-user>
gravitino.entity.store.relational.jdbcPassword=<set-via-secret-management>
gravitino.entity.store.relational.maxConnections=200

# Tree lock: sized for a larger metadata graph
gravitino.lock.maxNodes=500000
gravitino.lock.minNodes=5000

# Cache: stats enabled for observability, larger capacity
gravitino.cache.enableStats=true
gravitino.cache.maxEntries=100000

# Audit logging
gravitino.audit.enabled=true

# Iceberg REST server: auxiliary mode with shared Gravitino catalogs
gravitino.auxService.names=iceberg-rest
gravitino.iceberg-rest.catalog-config-provider=dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake=production

# Authentication: OAuth 2.0 / OIDC with JWKS-based token validation.
# Example values shown for Azure AD; substitute your identity provider's URLs.
# For Kerberos, simple auth, or static-key OAuth, see the Authentication section below.
gravitino.authenticators=oauth
gravitino.authenticator.oauth.provider=oidc
gravitino.authenticator.oauth.tokenValidatorClass=org.apache.gravitino.server.authentication.JwksTokenValidator
gravitino.authenticator.oauth.authority=https://login.microsoftonline.com/<tenant-id>/v2.0
gravitino.authenticator.oauth.jwksUri=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
gravitino.authenticator.oauth.clientId=<your-app-client-id>
gravitino.authenticator.oauth.serviceAudience=<your-app-client-id-or-api-identifier>
gravitino.authenticator.oauth.scope=openid profile email
gravitino.authenticator.oauth.principalFields=preferred_username,email,sub
```

Initialize the MySQL backend before starting the server. See [Relational Backend Storage](how-to-use-relational-backend-storage.md) for setup steps and SQL scripts.

## Server Properties

Customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases. Changes to `gravitino.conf` take effect after restarting the Gravitino server.

### HTTP Server

| gravitino.conf property                                   | Description                                                                                                                                                                           | Default value                                                                | Required | Since version    |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|------------------|
| `gravitino.server.webserver.host`                    | The host of the Gravitino server.                                                                                                                                                     | `0.0.0.0`                                                                    | No       | 0.1.0            |
| `gravitino.server.webserver.httpPort`                | The port on which the Gravitino server listens for incoming connections.                                                                                                              | `8090`                                                                       | No       | 0.1.0            |
| `gravitino.server.webserver.minThreads`              | The minimum number of threads in the thread pool used by the Jetty webserver. `minThreads` is 8 if the value is less than 8.                                                          | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0            |
| `gravitino.server.webserver.maxThreads`              | The maximum number of threads in the thread pool used by the Jetty webserver. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`. | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.1.0            |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by the Jetty webserver.                                                                                                                 | `100`                                                                        | No       | 0.1.0            |
| `gravitino.server.webserver.stopTimeout`             | Time in milliseconds to gracefully shut down the Jetty webserver, for more, see `org.eclipse.jetty.server.Server#setStopTimeout`.                                              | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.idleTimeout`             | The timeout in milliseconds of idle connections.                                                                                                                                      | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.requestHeaderSize`       | Maximum size of HTTP requests.                                                                                                                                                        | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.webserver.responseHeaderSize`      | Maximum size of HTTP responses.                                                                                                                                                       | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.shutdown.timeout`                  | Time in milliseconds to gracefully shut down of the Gravitino webserver.                                                                                                              | `3000`                                                                       | No       | 0.2.0            |
| `gravitino.server.webserver.customFilters`           | Comma-separated list of filter class names to apply to the API.                                                                                                                       | (none)                                                                       | No       | 0.4.0            |
| `gravitino.server.rest.extensionPackages`            | Comma-separated list of REST API packages to expand                                                                                                                                   | (none)                                                                       | No       | 0.6.0-incubating |
| `gravitino.server.visibleConfigs`                    | List of configs that are visible in the config servlet                                                                                                                                | (none)                                                                       | No       | 0.9.0-incubating |

The filter in the customFilters should be a standard javax servlet filter.
Specify filter parameters by setting configuration entries of the form `gravitino.server.webserver.<class name of filter>.param.<param name>=<value>`.

### Storage

#### Storage Backend

Gravitino only supports JDBC database backend, and the default implementation is H2 database as it's an embedded database, has no external dependencies and is very suitable for local development or tests.
If you are going to use H2 in the production environment, Gravitino will not guarantee the data consistency and durability. It's highly recommended using MySQL as the backend database.

For step-by-step setup of MySQL or PostgreSQL backends, including connector JARs and schema initialization, see [Relational Backend Storage](how-to-use-relational-backend-storage.md).

The following table lists the storage configuration items:

| gravitino.conf property                                | Description                                                                                                                                                                                                                                             | Default value                     | Required                                        | Since version    |
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|-------------------------------------------------|------------------|
| `gravitino.entity.store`                          | Which entity storage implementation to use. Only`relational` storage is supported.                                                                                                                                                            | `relational`                      | No                                              | 0.1.0            |
| `gravitino.entity.store.maxTransactionSkewTimeMs` | The maximum skew time of transactions in milliseconds.                                                                                                                                                                                                  | `2000`                            | No                                              | 0.3.0            |
| `gravitino.entity.store.deleteAfterTimeMs`        | The maximum time in milliseconds that deleted and old-version data is kept. Set to at least 10 minutes and no longer than 30 days.                                                                                                                      | `604800000`(7 days)               | No                                              | 0.5.0            |
| `gravitino.entity.store.versionRetentionCount`    | The Count of versions allowed to be retained, including the current version, used to delete old versions data. Set to at least 1 and no greater than 10.                                                                                                | `1`                               | No                                              | 0.5.0            |
| `gravitino.entity.store.relational`               | Detailed implementation of Relational storage. `H2`, `MySQL` and `PostgreSQL` is supported, and the implementation is `JDBCBackend`.                                                                                                          | `JDBCBackend`                     | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUrl`       | The database url that the `JDBCBackend` needs to connect to. If you use `MySQL` or `PostgreSQL`, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/{DATABASE_TYPE}/` directory. | `jdbc:h2`                         | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcDriver`    | The jdbc driver name that the `JDBCBackend` needs to use. You should place the driver Jar package in the `${GRAVITINO_HOME}/libs/` directory.                                                                                                           | `org.h2.Driver`                   | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUser`      | The username that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                       | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcPassword`  | The password that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                       | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.storagePath`   | The storage path for embedded JDBC storage implementation. It supports both absolute and relative path, if the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is `${GRAVITINO_HOME}/data/jdbc`     | `${GRAVITINO_HOME}/data/jdbc`     | No                                              | 0.6.0-incubating |
| `gravitino.entity.store.relational.maxConnections`| The maximum number of connections for the JDBC Backend connection pool                                                                                                                                                                                  | `100`                             | No                                              | 0.9.0-incubating |
| `gravitino.entity.store.relational.maxWaitMillis` | The maximum wait time in milliseconds for a connection from the JDBC Backend connection pool                                                                                                                                                            | `1000`                            | No                                              | 0.9.0-incubating |


:::caution
We strongly recommend that you change the default value of `gravitino.entity.store.relational.storagePath`, as it's under the deployment directory and future version upgrades may remove it.
:::

#### Create JDBC Backend Schema and Table

For H2 database, All tables needed by Gravitino are created automatically when the Gravitino server starts up. For MySQL, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/mysql/` directory.

### Storage Cache

| gravitino.conf property               | Description                                | Default Value          | Required | Since Version |
|----------------------------------|--------------------------------------------|------------------------|----------|---------------|
| `gravitino.cache.enabled`        | Whether to enable caching                  | `true`                 | No       | 1.0.0         |
| `gravitino.cache.implementation` | Specifies the cache implementation         | `caffeine`             | No       | 1.0.0         |
| `gravitino.cache.maxEntries`     | Maximum number of entries allowed in cache | `10000`                | No       | 1.0.0         |
| `gravitino.cache.expireTimeInMs` | Cache expiration time (in milliseconds)    | `3600000` (about 1 hr) | No       | 1.0.0         |
| `gravitino.cache.enableStats`    | Whether to enable cache statistics logging. When `true`, Gravitino logs hit count, miss count, and load failures every 5 minutes at INFO level. | `false`                | No       | 1.0.0         |
| `gravitino.cache.enableWeigher`  | Whether to enable weight-based eviction. When `true`, `maxEntries` is ignored. | `true`                 | No       | 1.0.0         |
| `gravitino.cache.lockSegments`   | Number of lock segments.                   | `16`                   | No       | 1.0.0         |

#### Eviction Strategies

Gravitino supports multiple eviction strategies including capacity-based, weight-based, and time-based (TTL) eviction. The following describes how they work with Caffeine:

##### Capacity-based eviction

When `gravitino.cache.enableWeigher` is **disabled**, Gravitino limits the number of cached entries using `gravitino.cache.maxEntries` and employs Caffeine’s W-TinyLFU eviction policy to remove the least-used entries when the cache is full.

##### Weight-based eviction

When `gravitino.cache.enableWeigher` is **enabled**, Gravitino uses a combination of `maximumWeight` and a custom weigher to control the total weight of the cache:

- Each entity type has a default weight (e.g., Metalake > Catalog > Schema);
- Entries are evicted based on the combined weight limit (`maximumWeight`);
- If a single cache item exceeds the total weight limit, it will not be cached;
- When this strategy is active, `maxEntries` will be ignored.

##### Time-based eviction

All cache entries are subject to a TTL (Time-To-Live) expiration policy. By default, the TTL is `3600000ms` (1 hour) and can be adjusted via the `gravitino.cache.expireTimeInMs` setting:

- TTL starts at the time of entry creation; once it exceeds the configured duration, the entry expires automatically;
- TTL can work in conjunction with both capacity and weight-based eviction;
- Expired entries will also trigger asynchronous cleanup mechanisms for resource release and logging.

### Tree Lock

The Gravitino server uses a tree lock to ensure data consistency. The tree lock is an in-memory lock; Gravitino currently supports only in-memory locks. The configuration items are as follows:

| gravitino.conf property                   | Description                                                   | Default value | Required | Since Version |
|--------------------------------------|---------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.lock.maxNodes`            | The maximum number of tree lock nodes to keep in memory       | 100000        | No       | 0.5.0         |
| `gravitino.lock.minNodes`            | The minimum number of tree lock nodes to keep in memory       | 1000          | No       | 0.5.0         |
| `gravitino.lock.cleanIntervalInSecs` | The interval in seconds to clean up the stale tree lock nodes | 60            | No       | 0.5.0         |

### Catalog

| gravitino.conf property                           | Description                                                                                                                                                                                         | Default value | Required | Since version |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs` | The interval in milliseconds to evict the catalog cache; default 3600000ms(1h).                                                                                                                     | `3600000`     | No       | 0.1.0         |
| `gravitino.catalog.classloader.isolated`     | Whether to use an isolated classloader for catalog. If `true`, an isolated classloader loads all catalog-related libraries and configurations, not the AppClassLoader. The default value is `true`. | `true`        | No       | 0.1.0         |

### Iceberg REST Server

Gravitino can host an Iceberg REST server inside the Gravitino JVM as an auxiliary service. The properties below enable that hosting; the Iceberg REST server's own properties (HTTP config, catalog backends, security, etc.) are documented separately in [Iceberg REST Catalog Service](iceberg-rest-service.md).

| gravitino.conf property            | Description                                                                                                                                                          | Default value | Required | Since Version |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.auxService.names`       | The auxiliary service name of the Gravitino Iceberg REST catalog service. Use `iceberg-rest`.                                                                        | (none)        | Yes      | 0.2.0         |
| `gravitino.iceberg-rest.classpath` | The classpath of the Gravitino Iceberg REST catalog service. Supports both absolute and relative paths, for example, `iceberg-rest-server/libs, iceberg-rest-server/conf`. | (none)        | Yes      | 0.2.0         |

### Job System

The job system runs maintenance and optimizer jobs on behalf of Gravitino services such as the Table Maintenance Service. Properties below control where jobs run, how their staging files are managed, and how Gravitino polls executors for status.

| gravitino.conf property                       | Description                                                                                                                                | Default value                  | Required | Since Version |
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|----------|---------------|
| `gravitino.job.stagingDir`               | Directory for managing staging files when running jobs.                                                                                    | `/tmp/gravitino/jobs/staging`  | No       | 1.0.0         |
| `gravitino.job.executor`                 | The executor to run jobs. The built-in option is `local`; custom executors can be implemented and set here.                                | `local`                        | No       | 1.0.0         |
| `gravitino.job.stagingDirKeepTimeInMs`   | Time in milliseconds to keep staging files of finished jobs. Minimum recommended value is 10 minutes outside test environments.            | `604800000` (7 days)           | No       | 1.0.0         |
| `gravitino.job.statusPullIntervalInMs`   | Interval in milliseconds to pull job status from the executor. Minimum recommended value is 1 minute outside test environments.            | `300000` (5 minutes)           | No       | 1.0.0         |

#### Local Executor

When `gravitino.job.executor=local` (the default), the following properties further configure the local executor:

| gravitino.conf property                                  | Description                                                                                                                  | Default value                                  | Required    | Since Version |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------|-------------|---------------|
| `gravitino.jobExecutor.local.waitingQueueSize`      | Maximum number of jobs queued for execution.                                                                                 | `100`                                          | No          | 1.0.0         |
| `gravitino.jobExecutor.local.maxRunningJobs`        | Maximum number of jobs running concurrently.                                                                                 | `max(1, min(availableProcessors / 2, 10))`     | No          | 1.0.0         |
| `gravitino.jobExecutor.local.jobStatusKeepTimeInMs` | Time in milliseconds to keep job status in memory after completion.                                                          | `3600000` (1 hour)                             | No          | 1.0.0         |
| `gravitino.jobExecutor.local.sparkHome`             | Path to a Spark installation. Required for Spark-based job templates if the `SPARK_HOME` environment variable is not set.    | (none)                                         | Conditional | 1.0.0         |

See [Table Maintenance Service](table-maintenance-service/optimizer.md) for usage examples.

### Event Listener

Gravitino provides event listener mechanism to allow users to capture the events which are provided by Gravitino server to integrate some custom operations.

To leverage the event listener, you must implement the `EventListenerPlugin` interface and place the JAR file in the classpath of the Gravitino server. Then, add configurations to gravitino.conf to enable the event listener.

| gravitino.conf property                     | Description                                                                                            | Default value | Required | Since Version |
|----------------------------------------|--------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.eventListener.names`        | The name of the event listener, For multiple listeners, separate names with a comma, like "audit,sync" | (none)        | Yes      | 0.5.0         |
| `gravitino.eventListener.{name}.class` | The class name of the event listener, replace `{name}` with the actual listener name.                  | (none)        | Yes      | 0.5.0         | 
| `gravitino.eventListener.{name}.{key}` | Custom properties that will be passed to the event listener plugin.                                    | (none)        | Yes      | 0.5.0         | 

#### Event

Gravitino triggers a pre-event before the operation, a post-event after the completion of the operation and a failure event after the operation failed.

##### Post-event

| Operation type                          | Post-event                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Since Version    |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| table operation                         | `CreateTableEvent`, `AlterTableEvent`, `DropTableEvent`, `LoadTableEvent`, `ListTableEvent`, `PurgeTableEvent`, `CreateTableFailureEvent`, `AlterTableFailureEvent`, `DropTableFailureEvent`, `LoadTableFailureEvent`, `ListTableFailureEvent`, `PurgeTableFailureEvent`                                                                                                                                                                                                                                                                                                                                                                                         | 0.5.0            |
| fileset operation                       | `CreateFilesetEvent`, `AlterFilesetEvent`, `DropFilesetEvent`, `LoadFilesetEvent`, `ListFilesetEvent`, `GetFileLocationEvent`, `ListFilesEvent`, `CreateFilesetFailureEvent`, `AlterFilesetFailureEvent`, `DropFilesetFailureEvent`, `LoadFilesetFailureEvent`, `ListFilesetFailureEvent`, `GetFileLocationFailureEvent`, `ListFilesFailureEvent`                                                                                                                                                                                                                                                                                                                 | 0.5.0            |
| topic operation                         | `CreateTopicEvent`, `AlterTopicEvent`, `DropTopicEvent`, `LoadTopicEvent`, `ListTopicEvent`, `CreateTopicFailureEvent`, `AlterTopicFailureEvent`, `DropTopicFailureEvent`, `LoadTopicFailureEvent`, `ListTopicFailureEvent`                                                                                                                                                                                                                                                                                                                                                                                                                                      | 0.5.0            |
| schema operation                        | `CreateSchemaEvent`, `AlterSchemaEvent`, `DropSchemaEvent`, `LoadSchemaEvent`, `ListSchemaEvent`, `CreateSchemaFailureEvent`, `AlterSchemaFailureEvent`, `DropSchemaFailureEvent`, `LoadSchemaFailureEvent`, `ListSchemaFailureEvent`                                                                                                                                                                                                                                                                                                                                                                                                                            | 0.5.0            |
| catalog operation                       | `CreateCatalogEvent`, `AlterCatalogEvent`, `DropCatalogEvent`, `LoadCatalogEvent`, `ListCatalogEvent`, `EnableCatalogEvent`, `DisableCatalogEvent`, `CreateCatalogFailureEvent`, `AlterCatalogFailureEvent`, `DropCatalogFailureEvent`, `LoadCatalogFailureEvent`, `ListCatalogFailureEvent`, `EnableCatalogFailureEvent`, `DisableCatalogFailureEvent`                                                                                                                                                                                                                                                                                                           | 0.5.0            |
| metalake operation                      | `CreateMetalakeEvent`, `AlterMetalakeEvent`, `DropMetalakeEvent`, `LoadMetalakeEvent`, `ListMetalakeEvent`, `EnableMetalakeEvent`, `DisableMetalakeEvent`, `CreateMetalakeFailureEvent`, `AlterMetalakeFailureEvent`, `DropMetalakeFailureEvent`, `LoadMetalakeFailureEvent`, `ListMetalakeFailureEvent`, `EnableMetalakeFailureEvent`, `DisableMetalakeFailureEvent`                                                                                                                                                                                                                                                                                             | 0.5.0            |
| partition operation                     | `AddPartitionEvent`, `GetPartitionEvent`, `DropPartitionEvent`, `PurgePartitionEvent`, `ListPartitionEvent`, `ListPartitionNamesEvent`, `PartitionExistsEvent`, `AddPartitionFailureEvent`, `GetPartitionFailureEvent`, `DropPartitionFailureEvent`, `PurgePartitionFailureEvent`, `ListPartitionFailureEvent`, `ListPartitionNamesFailureEvent`, `PartitionExistsFailureEvent`                                                                                                                                                                                                                                                                                   | 0.6.0-incubating |
| Iceberg REST server table operation     | `IcebergCreateTableEvent`, `IcebergUpdateTableEvent`, `IcebergDropTableEvent`, `IcebergLoadTableEvent`, `IcebergListTableEvent`, `IcebergTableExistsEvent`, `IcebergRenameTableEvent`, `IcebergRegisterTableEvent`, `IcebergLoadTableCredentialEvent`, `IcebergPlanTableScanEvent`, `IcebergCreateTableFailureEvent`, `IcebergUpdateTableFailureEvent`, `IcebergDropTableFailureEvent`, `IcebergLoadTableFailureEvent`, `IcebergListTableFailureEvent`, `IcebergTableExistsFailureEvent`, `IcebergRenameTableFailureEvent`, `IcebergRegisterTableFailureEvent`, `IcebergLoadTableCredentialFailureEvent`, `IcebergPlanTableScanFailureEvent` | 0.7.0-incubating |
| Iceberg REST server namespace operation | `IcebergCreateNamespaceEvent`, `IcebergUpdateNamespaceEvent`, `IcebergDropNamespaceEvent`, `IcebergLoadNamespaceEvent`, `IcebergListNamespacesEvent`, `IcebergNamespaceExistsEvent`, `IcebergCreateNamespaceFailureEvent`, `IcebergUpdateNamespaceFailureEvent`, `IcebergDropNamespaceFailureEvent`, `IcebergLoadNamespaceFailureEvent`, `IcebergListNamespacesFailureEvent`, `IcebergNamespaceExistsFailureEvent`                                                                                                                                                                                                                                              | 0.8.0-incubating |
| Iceberg REST server view operation      | `IcebergCreateViewEvent`, `IcebergReplaceViewEvent`, `IcebergDropViewEvent`, `IcebergLoadViewEvent`, `IcebergListViewEvent`, `IcebergViewExistsEvent`, `IcebergRenameViewEvent`, `IcebergCreateViewFailureEvent`, `IcebergReplaceViewFailureEvent`, `IcebergDropViewFailureEvent`, `IcebergLoadViewFailureEvent`, `IcebergListViewFailureEvent`, `IcebergViewExistsFailureEvent`, `IcebergRenameViewFailureEvent`                                                                                                                                                                                                                                                      | 0.8.0-incubating |
| tag operation                           | `ListTagsEvent`, `ListTagsInfoEvent`, `CreateTagEvent`, `GetTagEvent`, `AlterTagEvent`, `DeleteTagEvent`, `ListMetadataObjectsForTagEvent`, `ListTagsForMetadataObjectEvent`, `ListTagsInfoForMetadataObjectEvent`, `AssociateTagsForMetadataObjectEvent`, `GetTagForMetadataObjectEvent`, `ListTagsFailureEvent`, `ListTagInfoFailureEvent`, `CreateTagFailureEvent`, `GetTagFailureEvent`, `AlterTagFailureEvent`, `DeleteTagFailureEvent`, `ListMetadataObjectsForTagFailureEvent`, `ListTagsForMetadataObjectFailureEvent`, `ListTagsInfoForMetadataObjectFailureEvent`, `AssociateTagsForMetadataObjectFailureEvent`, `GetTagForMetadataObjectFailureEvent` | 0.9.0-incubating |
| model operation                         | `DeleteModelEvent`, `DeleteModelVersionEvent`, `GetModelEvent`, `GetModelVersionEvent`, `GetModelVersionUriEvent`, `LinkModelVersionEvent`, `ListModelEvent`, `ListModelVersionsEvent`, `ListModelVersionInfosEvent`, `RegisterAndLinkModelEvent`, `RegisterModelEvent`, `AlterModelEvent`, `AlterModelVersionEvent`, `DeleteModelFailureEvent`, `DeleteModelVersionFailureEvent`, `GetModelFailureEvent`, `GetModelVersionFailureEvent`, `GetModelVersionUriFailureEvent`, `LinkModelVersionFailureEvent`, `ListModelFailureEvent`, `ListModelVersionFailureEvent`, `ListModelVersionInfosFailureEvent`, `RegisterAndLinkModelFailureEvent`, `RegisterModelFailureEvent`, `AlterModelFailureEvent`, `AlterModelVersionFailureEvent` | 0.9.0-incubating |
| user operation                          | `AddUserEvent`, `GetUserEvent`, `ListUserNamesEvent`, `ListUsersEvent`, `RemoveUserEvent`, `GrantUserRolesEvent`, `RevokeUserRolesEvent`, `AddUserFailureEvent`, `GetUserFailureEvent`, `GrantUserRolesFailureEvent`, `ListUserNamesFailureEvent`, `ListUsersFailureEvent`, `RemoveUserFailureEvent`, `RevokeUserRolesFailureEvent`                                                                                                                                                                                                                                                                                                                              | 0.9.0-incubating |
| group operation                         | `AddGroupEvent`, `GetGroupEvent`, `ListGroupNamesEvent`, `ListGroupsEvent`, `RemoveGroupEvent`, `GrantGroupRolesEvent`, `RevokeGroupRolesEvent`, `AddGroupFailureEvent`, `GetGroupFailureEvent`, `GrantGroupRolesFailureEvent`, `ListGroupNamesFailureEvent`, `ListGroupsFailureEvent`, `RemoveGroupFailureEvent`, `RevokeGroupRolesFailureEvent`                                                                                                                                                                                                                                                                                                                | 0.9.0-incubating |
| role operation                          | `CreateRoleEvent`, `DeleteRoleEvent`, `GetRoleEvent`, `GrantPrivilegesEvent`, `ListRoleNamesEvent`, `RevokePrivilegesEvent`, `OverridePrivilegesEvent`, `CreateRoleFailureEvent`, `DeleteRoleFailureEvent`, `GetRoleFailureEvent`, `GrantPrivilegesFailureEvent`, `ListRoleNamesFailureEvent`, `RevokePrivilegesFailureEvent`, `OverridePrivilegesFailureEvent`                                                                                                                                                                                                                                                                                                   | 0.9.0-incubating |
| owner operation                         | `SetOwnerEvent`, `GetOwnerEvent`, `SetOwnerFailureEvent`, `GetOwnerFailureEvent`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | 1.0.0            |
| Gravitino server job template operation | `RegisterJobTemplateEvent`, `GetJobTemplateEvent`, `ListJobTemplatesEvent`, `AlterJobTemplateEvent`, `DeleteJobTemplateEvent`, `RegisterJobTemplateFailureEvent`, `GetJobTemplateFailureEvent`, `ListJobTemplatesFailureEvent`, `AlterJobTemplateFailureEvent`, `DeleteJobTemplateFailureEvent`                                                                                                                                                                                                                                                                                                                                                                  | 1.0.1            |
| Gravitino server job operation          | `RunJobEvent`, `GetJobEvent`, `ListJobsEvent`, `CancelJobEvent`, `RunJobFailureEvent`, `GetJobFailureEvent`, `ListJobsFailureEvent`, `CancelJobFailureEvent`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | 1.0.1            |
| Gravitino server statistics operation   | `ListStatisticsEvent`, `UpdateStatisticsEvent`, `DropStatisticsEvent`, `ListPartitionStatisticsEvent`, `UpdatePartitionStatisticsEvent`, `DropPartitionStatisticsEvent`, `ListStatisticsFailureEvent`, `UpdateStatisticsFailureEvent`, `DropStatisticsFailureEvent`, `ListPartitionStatisticsFailureEvent`, `UpdatePartitionStatisticsFailureEvent`, `DropPartitionStatisticsFailureEvent`                                                                                                                                                                                                                                                                       | 1.1.0            |
| policy operation                        | `CreatePolicyEvent`, `AlterPolicyEvent`, `DeletePolicyEvent`, `GetPolicyEvent`, `ListPoliciesEvent`, `ListPolicyInfosEvent`, `EnablePolicyEvent`, `DisablePolicyEvent`, `GetPolicyForMetadataObjectEvent`, `AssociatePoliciesForMetadataObjectEvent`, `ListPolicyInfosForMetadataObjectEvent`, `ListMetadataObjectsForPolicyEvent`, `CreatePolicyFailureEvent`, `AlterPolicyFailureEvent`, `DeletePolicyFailureEvent`, `GetPolicyFailureEvent`, `ListPoliciesFailureEvent`, `ListPolicyInfosFailureEvent`, `EnablePolicyFailureEvent`, `DisablePolicyFailureEvent`, `GetPolicyForMetadataObjectFailureEvent`, `AssociatePoliciesForMetadataObjectFailureEvent`, `ListPolicyInfosForMetadataObjectFailureEvent`, `ListMetadataObjectsForPolicyFailureEvent` | 1.1.0            |
| function operation                      | `RegisterFunctionEvent`, `GetFunctionEvent`, `AlterFunctionEvent`, `DropFunctionEvent`, `ListFunctionEvent`, `ListFunctionInfosEvent`, `RegisterFunctionFailureEvent`, `GetFunctionFailureEvent`, `AlterFunctionFailureEvent`, `DropFunctionFailureEvent`, `ListFunctionFailureEvent`                                                                                                                                                       | 1.3.0            |

##### Pre-event

| Operation type                          | Pre-event                                                                                                                                                                                                                                                                                                                  | Since Version    |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| Iceberg REST server table operation     | `IcebergCreateTablePreEvent`, `IcebergUpdateTablePreEvent`, `IcebergDropTablePreEvent`, `IcebergLoadTablePreEvent`, `IcebergListTablePreEvent`, `IcebergTableExistsPreEvent`, `IcebergRenameTablePreEvent`, `IcebergRegisterTablePreEvent`, `IcebergLoadTableCredentialPreEvent`, `IcebergPlanTableScanPreEvent`           | 0.7.0-incubating |
| Iceberg REST server namespace operation | `IcebergCreateNamespacePreEvent`, `IcebergUpdateNamespacePreEvent`, `IcebergDropNamespacePreEvent`, `IcebergLoadNamespacePreEvent`, `IcebergListNamespacesPreEvent`, `IcebergNamespaceExistsPreEvent`                                                                                                                      | 0.8.0-incubating |
| Iceberg REST server view operation      | `IcebergCreateViewPreEvent`, `IcebergReplaceViewPreEvent`, `IcebergDropViewPreEvent`, `IcebergLoadViewPreEvent`, `IcebergListViewPreEvent`, `IcebergViewExistsPreEvent`, `IcebergRenameViewPreEvent`                                                                                                                       | 0.8.0-incubating |
| Gravitino server table operation        | `CreateTablePreEvent`, `AlterTablePreEvent`, `DropTablePreEvent`, `PurgeTablePreEvent`, `LoadTablePreEvent`, `ListTablePreEvent`                                                                                                                                                                                           | 0.8.0-incubating |
| Gravitino server schema operation       | `CreateSchemaPreEvent`, `AlterSchemaPreEvent`, `DropSchemaPreEvent`, `LoadSchemaPreEvent`, `ListSchemaPreEvent`                                                                                                                                                                                                            | 0.8.0-incubating |
| Gravitino server catalog operation      | `CreateCatalogPreEvent`, `AlterCatalogPreEvent`, `DropCatalogPreEvent`, `LoadCatalogPreEvent`, `ListCatalogPreEvent`, `EnableCatalogPreEvent`, `DisableCatalogPreEvent`                                                                                                                                                     | 0.8.0-incubating |
| Gravitino server metalake operation     | `CreateMetalakePreEvent`, `AlterMetalakePreEvent`, `DropMetalakePreEvent`, `LoadMetalakePreEvent`, `ListMetalakePreEvent`, `EnableMetalakePreEvent`, `DisableMetalakePreEvent`                                                                                                                                              | 0.8.0-incubating |
| Gravitino server partition operation    | `AddPartitionPreEvent`, `DropPartitionPreEvent`, `GetPartitionPreEvent`, `PurgePartitionPreEvent`,`ListPartitionPreEvent`,`ListPartitionNamesPreEvent`                                                                                                                                                                     | 0.8.0-incubating |
| Gravitino server fileset operation      | `CreateFilesetPreEvent`, `AlterFilesetPreEvent`, `DropFilesetPreEvent`, `LoadFilesetPreEvent`,`ListFilesetPreEvent`,`GetFileLocationPreEvent`, `ListFilesPreEvent`                                                                                                                                                         | 0.8.0-incubating |
| Gravitino server model operation        | `DeleteModelPreEvent`, `DeleteModelVersionPreEvent`, `RegisterAndLinkModelPreEvent`, `GetModelPreEvent`, `GetModelVersionPreEvent`, `GetModelVersionUriPreEvent`, `LinkModelVersionPreEvent`, `ListModelPreEvent`, `ListModelVersionPreEvent`, `ListModelVersionInfosPreEvent`, `RegisterModelPreEvent`, `AlterModelPreEvent`, `AlterModelVersionPreEvent` | 0.9.0-incubating |
| Gravitino server tag operation          | `ListTagsPreEvent`, `ListTagsInfoPreEvent`, `CreateTagPreEvent`, `GetTagPreEvent`, `AlterTagPreEvent`, `DeleteTagPreEvent`, `ListMetadataObjectsForTagPreEvent`, `ListTagsForMetadataObjectPreEvent`, `ListTagsInfoForMetadataObjectPreEvent`, `AssociateTagsForMetadataObjectPreEvent`, `GetTagForMetadataObjectPreEvent` | 0.9.0-incubating |
| Gravitino server user operation         | `AddUserPreEvent`, `GetUserPreEvent`, `ListUserNamesPreEvent`, `ListUsersPreEvent`, `RemoveUserPreEvent`, `GrantUserRolesPreEvent`, `RevokeUserRolesPreEvent`                                                                                                                                                              | 0.9.0-incubating |
| Gravitino server group operation        | `AddGroupPreEvent`, `GetGroupPreEvent`, `ListGroupNamesPreEvent`, `ListGroupsPreEvent`, `RemoveGroupPreEvent`, `GrantGroupRolesPreEvent`, `RevokeGroupRolesPreEvent`                                                                                                                                                       | 0.9.0-incubating |
| Gravitino server role operation         | `CreateRolePreEvent`, `DeleteRolePreEvent`, `GetRolePreEvent`, `GrantPrivilegesPreEvent`, `ListRoleNamesPreEvent`, `RevokePrivilegesPreEvent`, `OverridePrivilegesPreEvent`                                                                                                                                                 | 0.9.0-incubating |
| Gravitino server owner operation        | `SetOwnerPreEvent`, `GetOwnerPreEvent`                                                                                                                                                                                                                                                                                     | 1.0.0            |
| Gravitino server job template operation | `RegisterJobTemplatePreEvent`, `GetJobTemplatePreEvent`, `ListJobTemplatesPreEvent`, `AlterJobTemplatePreEvent`, `DeleteJobTemplatePreEvent`                                                                                                                                                                               | 1.0.1            |
| Gravitino server job operation          | `RunJobPreEvent`, `GetJobPreEvent`, `ListJobsPreEvent`, `CancelJobPreEvent`                                                                                                                                                                                                                                                | 1.0.1            |
| Gravitino server statistics operation   | `ListStatisticsPreEvent`, `UpdateStatisticsPreEvent`, `DropStatisticsPreEvent`, `ListPartitionStatisticsPreEvent`, `UpdatePartitionStatisticsPreEvent`, `DropPartitionStatisticsPreEvent`                                                                                                                                  | 1.1.0            |
| policy operation                        | `CreatePolicyPreEvent`, `AlterPolicyPreEvent`, `DeletePolicyPreEvent`, `GetPolicyPreEvent`, `ListPoliciesPreEvent`, `ListPolicyInfosPreEvent`, `EnablePolicyPreEvent`, `DisablePolicyPreEvent`, `GetPolicyForMetadataObjectPreEvent`, `AssociatePoliciesForMetadataObjectPreEvent`, `ListPolicyInfosForMetadataObjectPreEvent`, `ListMetadataObjectsForPolicyPreEvent` | 1.1.0 |
| function operation                      | `RegisterFunctionPreEvent`, `GetFunctionPreEvent`, `AlterFunctionPreEvent`, `DropFunctionPreEvent`, `ListFunctionPreEvent`                                                                                                                                                                                                        | 1.3.0 |

#### Event Listener Plugin

The `EventListenerPlugin` defines an interface for event listeners that manage the lifecycle and state of a plugin. This includes handling its initialization, startup, and shutdown processes, as well as handing events triggered by various operations.

The plugin provides several operational modes for how to process event, supporting both synchronous and asynchronous processing approaches.

- **SYNC**: Events are processed synchronously, immediately following the associated operation. This mode ensures events are processed before the operation's result is returned to the client, but it may delay the main process if event processing takes too long.

- **ASYNC_SHARED**: This mode employs a shared queue and dispatcher for asynchronous event processing. It prevents the main process from being blocked, though there's a risk events might be dropped if not promptly consumed. Sharing a dispatcher can lead to poor isolation in case of slow listeners.
 
- **ASYNC_ISOLATED**: Events are processed asynchronously, with each listener having its own dedicated queue and dispatcher thread. This approach offers better isolation but at the expense of multiple queues and dispatchers.

When processing pre-event, you could throw a `ForbiddenException` to skip the following executions. For more details, refer to the definition of the plugin.

### Audit Log

The audit log framework defines how audit logs are formatted and written to various storages. The formatter defines an interface that transforms different `Event` types into a unified `AuditLog`. The writer defines an interface to writing AuditLog to different storages.

Gravitino provides a default implement to log basic audit information to a file, you could extend the audit system by implementation corresponding interfaces.

| gravitino.conf property                    | Description                            | Default value                               | Required | Since Version              |
|---------------------------------------|----------------------------------------|---------------------------------------------|----------|----------------------------|
| `gravitino.audit.enabled`             | The audit log enable flag.             | false                                       | No       | 0.7.0-incubating           |
| `gravitino.audit.writer.className`    | The class name of audit log writer.    | org.apache.gravitino.audit.FileAuditWriter  | No       | 0.7.0-incubating           | 
| `gravitino.audit.formatter.className` | The class name of audit log formatter. | org.apache.gravitino.audit.SimpleFormatter  | No       | 0.7.0-incubating           | 

#### Audit Log Formatter

The Formatter defines an interface that formats metadata audit logs into a unified format. `SimpleFormatter` is a default implement to format audit information, you don't need to do extra configurations.

#### Audit Log Writer

The `AuditLogWriter` defines an interface that enables the writing of metadata audit logs to different storage mediums such as files, databases, etc.

Writer configuration begins with `gravitino.audit.writer.${name}`, where `${name}` is replaced with the actual writer name defined in method `name()`. `FileAuditWriter` is a default implement to log audit information, whose name is `file`.

| gravitino.conf property                              | Description                                                                   | Default value       | Required | Since Version    |
|-------------------------------------------------|-------------------------------------------------------------------------------|---------------------|----------|------------------|
| `gravitino.audit.writer.file.fileName`          | The audit log file name, the path is `${sys:gravitino.log.path}/${fileName}`. | gravitino_audit.log | No       | 0.7.0-incubating |
| `gravitino.audit.writer.file.flushIntervalSecs` | The flush interval time of the audit file in seconds.                         | 10                  | No       | 0.7.0-incubating |
| `gravitino.audit.writer.file.append`            | Whether the log will be written to the end or the beginning of the file.      | true                | No       | 0.7.0-incubating |

### Security

Refer to [security](security/security.md) for HTTPS configuration.

### Authentication

Gravitino supports three authentication modes: `simple` (the default), `oauth`, and `kerberos`. Set the mode with `gravitino.authenticators`. For setup walkthroughs, client code examples, and worked examples for specific providers (Azure AD, Keycloak), see [Authentication](security/how-to-authenticate.md).

#### General Properties

| gravitino.conf property          | Description                                                                                                                                                                                                | Default value | Required | Since Version    |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `gravitino.authenticator`   | Deprecated since 0.6.0. Use `gravitino.authenticators` instead.                                                                                                                                            | `simple`      | No       | 0.3.0            |
| `gravitino.authenticators`  | Authenticators to enable, set to `simple`, `oauth`, or `kerberos`. Multiple authenticators are comma-separated. When a request is supported by multiple authenticators simultaneously, the first one wins. | `simple`      | No       | 0.6.0-incubating |

#### OAuth Properties

These properties apply when `gravitino.authenticators = oauth`. Configure either static-key validation (set `defaultSignKey`, `serverUri`, `tokenPath`) or JWKS-based validation (set `jwksUri` and `tokenValidatorClass`).

| gravitino.conf property                                            | Description                                                                                                                                                                                                                                              | Default value                                                       | Required                                                                                        | Since Version |
|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|---------------|
| `gravitino.authenticator.oauth.serviceAudience`               | The audience name when Gravitino uses OAuth as the authenticator.                                                                                                                                                                                        | `GravitinoServer`                                                   | No                                                                                              | 0.3.0         |
| `gravitino.authenticator.oauth.allowSkewSecs`                 | JWT allowed skew in seconds when Gravitino uses OAuth as the authenticator.                                                                                                                                                                              | `0`                                                                 | No                                                                                              | 0.3.0         |
| `gravitino.authenticator.oauth.defaultSignKey`                | Signing key of JWT when Gravitino uses OAuth as the authenticator.                                                                                                                                                                                       | (none)                                                              | Yes when using `oauth` as the authenticator                                                     | 0.3.0         |
| `gravitino.authenticator.oauth.signAlgorithmType`             | Signature algorithm when Gravitino uses OAuth as the authenticator. See the Signature Algorithms table below.                                                                                                                                            | `RS256`                                                             | No                                                                                              | 0.3.0         |
| `gravitino.authenticator.oauth.serverUri`                     | URI of the OAuth server. Required for `StaticSignKeyValidator`; not required for JWKS-based validators.                                                                                                                                                  | (none)                                                              | Yes when using `StaticSignKeyValidator`                                                         | 0.3.0         |
| `gravitino.authenticator.oauth.tokenPath`                     | Token path on the OAuth server. Required for `StaticSignKeyValidator`; not required for JWKS-based validators.                                                                                                                                           | (none)                                                              | Yes when using `StaticSignKeyValidator`                                                         | 0.3.0         |
| `gravitino.authenticator.oauth.provider`                      | OAuth provider type (`default` or `oidc`). Determines the Web UI authentication flow. Use `oidc` for Web UI OIDC login, `default` for legacy login or API-only authentication.                                                                           | `default`                                                           | No                                                                                              | 1.0.0         |
| `gravitino.authenticator.oauth.clientId`                      | OAuth client ID for Web UI authentication.                                                                                                                                                                                                               | (none)                                                              | Yes when provider is `oidc`                                                                     | 1.0.0         |
| `gravitino.authenticator.oauth.authority`                     | OAuth authority/issuer URL for OIDC providers used in Web UI authentication (for example, Azure AD tenant URL).                                                                                                                                          | (none)                                                              | Yes when provider is `oidc`                                                                     | 1.0.0         |
| `gravitino.authenticator.oauth.scope`                         | OAuth scopes for Web UI authentication (space-separated).                                                                                                                                                                                                | (none)                                                              | Yes when provider is `oidc`                                                                     | 1.0.0         |
| `gravitino.authenticator.oauth.jwksUri`                       | JWKS URI for server-side OAuth token validation. Required when using JWKS-based validation.                                                                                                                                                              | (none)                                                              | Yes when `tokenValidatorClass` is `org.apache.gravitino.server.authentication.JwksTokenValidator` | 1.0.0         |
| `gravitino.authenticator.oauth.principalFields`               | JWT claim field(s) to use as principal identity. Comma-separated list provides fallback in order (for example, `preferred_username,email,sub`).                                                                                                          | `sub`                                                               | No                                                                                              | 1.0.0         |
| `gravitino.authenticator.oauth.groupsFields`                  | JWT claim field(s) to use as group membership. Comma-separated list provides fallback in order (for example, `groups,roles`).                                                                                                                            | `groups`                                                            | No                                                                                              | 1.3.0         |
| `gravitino.authenticator.oauth.tokenValidatorClass`           | Fully qualified class name of the OAuth token validator. Use `org.apache.gravitino.server.authentication.JwksTokenValidator` for JWKS-based validation or `org.apache.gravitino.server.authentication.StaticSignKeyValidator` for static-key validation. | `org.apache.gravitino.server.authentication.StaticSignKeyValidator` | No                                                                                              | 1.0.0         |
| `gravitino.authenticator.oauth.principalMapper`               | Principal mapper type for OAuth. Use `regex` for regex-based mapping, or a fully qualified class name implementing `org.apache.gravitino.auth.PrincipalMapper`.                                                                                          | `regex`                                                             | No                                                                                              | 1.2.0         |
| `gravitino.authenticator.oauth.principalMapper.regex.pattern` | Regex pattern for OAuth principal mapping. The first capture group becomes the mapped principal. Used only when `principalMapper = regex`.                                                                                                              | `^(.*)$`                                                            | No                                                                                              | 1.2.0         |
| `gravitino.authenticator.oauth.groupMapper`                   | Group mapper type for OAuth. Use `regex` for regex-based mapping, or a fully qualified class name implementing `org.apache.gravitino.auth.GroupMapper`.                                                                                                  | `regex`                                                             | No                                                                                              | 1.3.0         |
| `gravitino.authenticator.oauth.groupMapper.regex.pattern`     | Regex pattern for OAuth group mapping. The first capture group becomes the mapped group. Used only when `groupMapper = regex`.                                                                                                                          | `^(.*)$`                                                            | No                                                                                              | 1.3.0         |

#### Kerberos Properties

These properties apply when `gravitino.authenticators = kerberos`. The Iceberg REST service does not support Kerberos.

| gravitino.conf property                                               | Description                                                                                                                                                        | Default value | Required                                       | Since Version |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|------------------------------------------------|---------------|
| `gravitino.authenticator.kerberos.principal`                     | Kerberos principal used for the HTTP endpoint. The principal must start with `HTTP/`.                                                                              | (none)        | Yes when using `kerberos` as the authenticator | 0.4.0         |
| `gravitino.authenticator.kerberos.keytab`                        | Location of the keytab file with credentials for the principal.                                                                                                    | (none)        | Yes when using `kerberos` as the authenticator | 0.4.0         |
| `gravitino.authenticator.kerberos.principalMapper`               | Principal mapper type for Kerberos. Use `regex` for regex-based mapping, or a fully qualified class name implementing `org.apache.gravitino.auth.PrincipalMapper`. | `regex`       | No                                             | 1.2.0         |
| `gravitino.authenticator.kerberos.principalMapper.regex.pattern` | Regex pattern for Kerberos principal mapping. The first capture group becomes the mapped principal. Used only when `principalMapper = regex`.                      | `([^@]+).*`   | No                                             | 1.2.0         |

#### Signature Algorithms

Gravitino supports the following signature algorithms (used with `gravitino.authenticator.oauth.signAlgorithmType`):

| Algorithm | Description                                    |
|-----------|------------------------------------------------|
| HS256     | HMAC using SHA-256                             |
| HS384     | HMAC using SHA-384                             |
| HS512     | HMAC using SHA-512                             |
| RS256     | RSASSA-PKCS-v1_5 using SHA-256                 |
| RS384     | RSASSA-PKCS-v1_5 using SHA-384                 |
| RS512     | RSASSA-PKCS-v1_5 using SHA-512                 |
| ES256     | ECDSA using P-256 and SHA-256                  |
| ES384     | ECDSA using P-384 and SHA-384                  |
| ES512     | ECDSA using P-521 and SHA-512                  |
| PS256     | RSASSA-PSS using SHA-256 and MGF1 with SHA-256 |
| PS384     | RSASSA-PSS using SHA-384 and MGF1 with SHA-384 |
| PS512     | RSASSA-PSS using SHA-512 and MGF1 with SHA-512 |

### Metrics

Gravitino exposes runtime metrics via JMX and an HTTP endpoint. The property below tunes metrics collection. See [Metrics](metrics.md) for available metrics and how to retrieve them.

| gravitino.conf property                        | Description                                          | Default value | Required | Since Version |
|-------------------------------------------|------------------------------------------------------|---------------|----------|---------------|
| `gravitino.metrics.timeSlidingWindowSecs` | The seconds of Gravitino metrics time sliding window | 60            | No       | 0.5.1         |

### Memory Settings

`GRAVITINO_MEM` sets JVM heap/metaspace flags for the Gravitino server and is also read by the Iceberg REST server and Lance REST server launchers.

Default: `-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m` (see `bin/common.sh`). Launch scripts append this to `JAVA_OPTS`; override `GRAVITINO_MEM` when you need different heap/metaspace sizes.

Typical values:
- Development: `-Xms1g -Xmx1g -XX:MaxMetaspaceSize=512m`
- Moderate production: `-Xms4g -Xmx4g -XX:MaxMetaspaceSize=1g`
- Larger deployments: `-Xms8g -Xmx8g -XX:MaxMetaspaceSize=1g` or higher depending on catalog count, plugins, and query concurrency

## Catalog Properties

Catalog properties configure a catalog. They come from three sources, with higher-priority sources overriding lower ones:

1. **Per-catalog properties** supplied as the `properties` map when calling the catalog creation API via REST, CLI, or one of the language SDKs. Highest priority.
2. **Provider-wide defaults** in the catalog's configuration file at `<GRAVITINO_HOME>/catalogs/<provider>/conf/<provider>.conf`. Operators edit this file to set defaults that apply to every catalog of a given provider type.
3. **Implementation defaults** coded into the catalog itself. Used when neither of the above supplies a value.

When the same property is set in both the API call and the configuration file, the API value wins.

The `properties` map is a set of key-value pairs supplied at catalog creation. For example, a REST request to create a PostgreSQL catalog might include:

```json
{
  "jdbc-url": "jdbc:postgresql://localhost:5432/mydb",
  "jdbc-user": "admin",
  "jdbc-password": "secret",
  "jdbc-driver": "org.postgresql.Driver"
}
```

The CLI equivalent passes `--properties jdbc-url=...,jdbc-user=...`. The Java and Python SDKs accept a `Map<String, String>` or `dict`.

### Property categories

Gravitino interprets property names in three ways:

- **Implementation-defined properties** (for example, `jdbc-url`, `jdbc-driver`): defined by the catalog implementation. Each catalog's reference page documents required and optional properties along with their defaults.
- **Bypass properties** (any name prefixed with `gravitino.bypass.`): passed unchanged to the underlying data source. For example, `gravitino.bypass.maxWaitMillis` is forwarded to the JDBC connection pool as `maxWaitMillis`. Use these to set source-specific options Gravitino does not interpret.
- **Custom properties** (any other name): stored alongside the catalog but unused by Gravitino. Available for the user's own tooling.

### Configuration file format

The provider configuration file uses standard `java.util.Properties` format: one `key=value` per line, `#` for comments. Most catalogs ship with the file present but with all entries commented out, so out of the box the file contributes no defaults. The Hive catalog is an exception that ships with a single non-commented default.

If the file is missing or unreadable, catalog creation still succeeds. Gravitino logs a warning and treats the file as empty.

### Configuration file paths by provider

The catalogs below read from these paths in a default Gravitino installation:

| catalog provider    | catalog reference                                                                       | configuration file path                                                  |
|---------------------|-----------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `hive`              | [Hive catalog](apache-hive-catalog.md)                                                  | `<GRAVITINO_HOME>/catalogs/hive/conf/hive.conf`                           |
| `lakehouse-iceberg` | [Lakehouse Iceberg catalog](lakehouse-iceberg-catalog.md)                               | `<GRAVITINO_HOME>/catalogs/lakehouse-iceberg/conf/lakehouse-iceberg.conf` |
| `lakehouse-paimon`  | [Lakehouse Paimon catalog](lakehouse-paimon-catalog.md)                                 | `<GRAVITINO_HOME>/catalogs/lakehouse-paimon/conf/lakehouse-paimon.conf`   |
| `lakehouse-hudi`    | [Lakehouse Hudi catalog](lakehouse-hudi-catalog.md)                                     | `<GRAVITINO_HOME>/catalogs/lakehouse-hudi/conf/lakehouse-hudi.conf`       |
| `jdbc-mysql`        | [MySQL catalog](jdbc-mysql-catalog.md)                                                  | `<GRAVITINO_HOME>/catalogs/jdbc-mysql/conf/jdbc-mysql.conf`               |
| `jdbc-postgresql`   | [PostgreSQL catalog](jdbc-postgresql-catalog.md)                                        | `<GRAVITINO_HOME>/catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`     |
| `jdbc-doris`        | [Doris catalog](jdbc-doris-catalog.md)                                                  | `<GRAVITINO_HOME>/catalogs/jdbc-doris/conf/jdbc-doris.conf`               |
| `jdbc-oceanbase`    | [OceanBase catalog](jdbc-oceanbase-catalog.md)                                          | `<GRAVITINO_HOME>/catalogs/jdbc-oceanbase/conf/jdbc-oceanbase.conf`       |
| `kafka`             | [Kafka catalog](kafka-catalog.md)                                                       | `<GRAVITINO_HOME>/catalogs/kafka/conf/kafka.conf`                         |
| `fileset`           | [Fileset catalog](fileset-catalog.md)                                                   | `<GRAVITINO_HOME>/catalogs/fileset/conf/fileset.conf`                     |
| `model`             | [Model catalog](model-catalog.md)                                                       | `<GRAVITINO_HOME>/catalogs/model/conf/model.conf`                         |

For catalogs whose implementation lives outside Gravitino's built-in directory, set the `package` property on the catalog. The configuration file location becomes `<package>/conf/<provider>.conf`.

### Common properties

The properties below apply to all Gravitino catalogs regardless of provider. Like provider-specific properties, they can be supplied via the API call or the catalog's configuration file (with API values winning on conflict).

| Configuration item  | Description                                                                                                                                                                                                                                                  | Default value | Required | Since version    |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `package`           | Path to the catalog package directory for catalogs not shipped with Gravitino. The package directory must contain a `conf` subdirectory (for the catalog's configuration file) and a `libs` subdirectory (for the catalog's JAR dependencies).                | (none)        | No       | 0.5.0            |
| `cloud.name`        | The cloud the catalog runs on. Valid values: `aws`, `azure`, `gcp`, `on_premise`, `other`.                                                                                                                                                                    | (none)        | No       | 0.6.0-incubating |
| `cloud.region-code` | The region code of the cloud the catalog runs on.                                                                                                                                                                                                             | (none)        | No       | 0.6.0-incubating |

## Other Properties

You could put HDFS configuration file to the catalog properties configuration dir, like `catalogs/lakehouse-iceberg/conf/`.

## Docker Deployment

Run the Gravitino server in a Docker container:

```shell
docker run -d -p 8090:8090 apache/gravitino:latest
```

The Gravitino Docker image supports injecting configuration values via environment variables by translating them to corresponding entries in `gravitino.conf` at container startup.

This is done using a startup script that parses environment variables prefixed with `GRAVITINO_` and rewrites the configuration file accordingly.

These variables override the corresponding entries in `gravitino.conf` at startup.

| Environment Variable                                     | Configuration Key                                    | Default Value                                        | Since Version |
|----------------------------------------------------------|------------------------------------------------------|------------------------------------------------------|---------------|
| `GRAVITINO_SERVER_SHUTDOWN_TIMEOUT`                      | `gravitino.server.shutdown.timeout`                  | `3000`                                               | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_HOST`                        | `gravitino.server.webserver.host`                    | `0.0.0.0`                                            | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_HTTP_PORT`                   | `gravitino.server.webserver.httpPort`                | `8090`                                               | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_MIN_THREADS`                 | `gravitino.server.webserver.minThreads`              | `24`                                                 | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_MAX_THREADS`                 | `gravitino.server.webserver.maxThreads`              | `200`                                                | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_STOP_TIMEOUT`                | `gravitino.server.webserver.stopTimeout`             | `30000`                                              | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_IDLE_TIMEOUT`                | `gravitino.server.webserver.idleTimeout`             | `30000`                                              | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE` | `gravitino.server.webserver.threadPoolWorkQueueSize` | `100`                                                | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_REQUEST_HEADER_SIZE`         | `gravitino.server.webserver.requestHeaderSize`       | `131072`                                             | 1.0.0         |
| `GRAVITINO_SERVER_WEBSERVER_RESPONSE_HEADER_SIZE`        | `gravitino.server.webserver.responseHeaderSize`      | `131072`                                             | 1.0.0         |
| `GRAVITINO_ENTITY_STORE`                                 | `gravitino.entity.store`                             | `relational`                                         | 1.0.0         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL`                      | `gravitino.entity.store.relational`                  | `JDBCBackend`                                        | 1.0.0         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL`             | `gravitino.entity.store.relational.jdbcUrl`          | `jdbc:h2`                                            | 1.0.0         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER`          | `gravitino.entity.store.relational.jdbcDriver`       | `org.h2.Driver`                                      | 1.0.0         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_USER`            | `gravitino.entity.store.relational.jdbcUser`         | `gravitino`                                          | 1.0.0         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_PASSWORD`        | `gravitino.entity.store.relational.jdbcPassword`     | `gravitino`                                          | 1.0.0         |
| `GRAVITINO_CATALOG_CACHE_EVICTION_INTERVAL_MS`           | `gravitino.catalog.cache.evictionIntervalMs`         | `3600000`                                            | 1.0.0         |
| `GRAVITINO_AUTHORIZATION_ENABLE`                         | `gravitino.authorization.enable`                     | `false`                                              | 1.0.0         |
| `GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE`               | `gravitino.authorization.threadPoolSize`             | `100`                                                | 1.0.0         |
| `GRAVITINO_AUTHORIZATION_SERVICE_ADMINS`                 | `gravitino.authorization.serviceAdmins`              | `anonymous`                                          | 1.0.0         |
| `GRAVITINO_AUX_SERVICE_NAMES`                            | `gravitino.auxService.names`                         | `iceberg-rest`                                       | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_CLASSPATH`                       | `gravitino.iceberg-rest.classpath`                   | `iceberg-rest-server/libs, iceberg-rest-server/conf` | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_HOST`                            | `gravitino.iceberg-rest.host`                        | `0.0.0.0`                                            | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_HTTP_PORT`                       | `gravitino.iceberg-rest.httpPort`                    | `9001`                                               | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_CATALOG_BACKEND`                 | `gravitino.iceberg-rest.catalog-backend`             | `memory`                                             | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_WAREHOUSE`                       | `gravitino.iceberg-rest.warehouse`                   | `/tmp/`                                              | 1.0.0         |

:::note
This feature is supported in the Gravitino Docker image starting from version `1.0.0`.
:::

Usage Example:

To start a container and override the default HTTP port:

```shell
docker run --rm -d \
  -e GRAVITINO_SERVER_WEBSERVER_HTTP_PORT=8080 \
  -p 8080:8080 \
  apache/gravitino:<tag>
```

To configure JDBC backend with PostgreSQL:

```shell
docker run --rm -d \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL="jdbc:postgresql://localhost:5432/database1" \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER="org.postgresql.Driver" \
  -p 8090:8090 \
  apache/gravitino:<tag>
```

Verify that the configuration was applied correctly by inspecting the container's `gravitino.conf`:

```shell
docker exec -it <container_id> cat /root/gravitino/conf/gravitino.conf
```

:::note
If both `gravitino.conf` and environment variable exist, the container’s startup script will overwrite the config file value with the environment variable.
:::

:::note Hadoop access
Due to the absence of a comprehensive user permission system, Gravitino can only use a single username for Apache Hadoop access. Ensure that the user running the container has Hadoop (HDFS, YARN) access permissions, or set the `HADOOP_USER_NAME` environment variable to a username authorized for Hadoop access.
:::

## Kubernetes Deployment

To deploy Gravitino on Kubernetes, use the official Helm chart. See [Install Gravitino with Helm](chart.md) for installation steps. The `gravitino.conf` properties documented on this page can be set through the Helm chart’s `values.yaml` file or via `--set` flags at install time.

## Binary Deployment Notes

For binary deployments, set JVM and shell environment variables in `${GRAVITINO_HOME}/conf/gravitino-env.sh`. Common uses include setting `HADOOP_USER_NAME` for Hadoop access (when the deploying user lacks Hadoop permissions) and adjusting `GRAVITINO_MEM` for heap sizing. See [Memory Settings](#memory-settings) for heap size guidance.

:::note Hadoop access
Due to the absence of a comprehensive user permission system, Gravitino can only use a single username for Apache Hadoop access. Ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN) access permissions, or set `HADOOP_USER_NAME` in `gravitino-env.sh` to a username authorized for Hadoop access.
:::
