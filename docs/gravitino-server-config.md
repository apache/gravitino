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

## Gravitino Server Configurations

Customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases.
We strongly recommend that you read the following sections to understand the configuration file, so you can change the default values to suit your specific situation and usage scenario.

The `gravitino.conf` file lists the configuration items in the following table. It groups those items into the following categories:

### HTTP Server Configuration

| Configuration item                                   | Description                                                                                                                                                                           | Default value                                                                | Required | Since version    |
|------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|----------|------------------|
| `gravitino.server.webserver.host`                    | The host of the Gravitino server.                                                                                                                                                     | `0.0.0.0`                                                                    | No       | 0.1.0            |
| `gravitino.server.webserver.httpPort`                | The port on which the Gravitino server listens for incoming connections.                                                                                                              | `8090`                                                                       | No       | 0.1.0            |
| `gravitino.server.webserver.minThreads`              | The minimum number of threads in the thread pool used by the Jetty webserver. `minThreads` is 8 if the value is less than 8.                                                          | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)` | No       | 0.2.0            |
| `gravitino.server.webserver.maxThreads`              | The maximum number of threads in the thread pool used by the Jetty webserver. `maxThreads` is 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`. | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`              | No       | 0.1.0            |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by the Jetty webserver.                                                                                                                 | `100`                                                                        | No       | 0.1.0            |
| `gravitino.server.webserver.stopTimeout`             | Time in milliseconds to gracefully shut down the Jetty webserver, for more, see `org.eclipse.jetty.server.Server#setStopTimeout`.                                                     | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.idleTimeout`             | The timeout in milliseconds of idle connections.                                                                                                                                      | `30000`                                                                      | No       | 0.2.0            |
| `gravitino.server.webserver.requestHeaderSize`       | Maximum size of HTTP requests.                                                                                                                                                        | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.webserver.responseHeaderSize`      | Maximum size of HTTP responses.                                                                                                                                                       | `131072`                                                                     | No       | 0.1.0            |
| `gravitino.server.shutdown.timeout`                  | Time in milliseconds to gracefully shut down of the Gravitino webserver.                                                                                                              | `3000`                                                                       | No       | 0.2.0            |
| `gravitino.server.webserver.customFilters`           | Comma-separated list of filter class names to apply to the API.                                                                                                                       | (none)                                                                       | No       | 0.4.0            |
| `gravitino.server.rest.extensionPackages`            | Comma-separated list of REST API packages to expand                                                                                                                                   | (none)                                                                       | No       | 0.6.0-incubating |
| `gravitino.server.visibleConfigs`                    | List of configs that are visible in the config servlet                                                                                                                                | (none)                                                                       | No       | 0.9.0-incubating |

The filter in the customFilters should be a standard javax servlet filter.
Specify filter parameters by setting configuration entries of the form `gravitino.server.webserver.<class name of filter>.param.<param name>=<value>`.

### Storage Configuration

#### Storage Backend Configuration

Gravitino only supports JDBC database backend, and the default implementation is H2 database as it's an embedded database, has no external dependencies and is very suitable for local development or tests.
If you are going to use H2 in the production environment, Gravitino will not guarantee the data consistency and durability. It's highly recommended using MySQL as the backend database.  

The following table lists the storage configuration items:

| Configuration item                                 | Description                                                                                                                                                                                                                                             | Default value                     | Required                                        | Since version    |
|----------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------|-------------------------------------------------|------------------|
| `gravitino.entity.store`                           | Which entity storage implementation to use. Only`relational` storage is supported.                                                                                                                                                                      | `relational`                      | No                                              | 0.1.0            |
| `gravitino.entity.store.maxTransactionSkewTimeMs`  | The maximum skew time of transactions in milliseconds.                                                                                                                                                                                                  | `2000`                            | No                                              | 0.3.0            |
| `gravitino.entity.store.deleteAfterTimeMs`         | The maximum time in milliseconds that deleted and old-version data is kept. Set to at least 10 minutes and no longer than 30 days.                                                                                                                      | `604800000`(7 days)               | No                                              | 0.5.0            |
| `gravitino.entity.store.versionRetentionCount`     | The Count of versions allowed to be retained, including the current version, used to delete old versions data. Set to at least 1 and no greater than 10.                                                                                                | `1`                               | No                                              | 0.5.0            |
| `gravitino.entityChangeLog.pollIntervalSecs`       | The interval in seconds for polling the entity change log. The poller invalidates stale local caches (e.g. the catalog cache) across HA nodes by consuming change log records. Must be positive.                                                        | `3`                               | No                                              | 1.3.0            |
| `gravitino.entityChangeLog.retentionSecs`          | The retention time in seconds for entity change log rows. Expired rows are pruned periodically. Set to `0` to disable automatic cleanup. Must be non-negative.                                                                                          | `86400`(1 day)                    | No                                              | 1.3.0            |
| `gravitino.entityChangeLog.cleanupIntervalSecs`    | The interval in seconds for pruning expired entity change log rows. Must be positive.                                                                                                                                                                   | `3600`(1 hour)                    | No                                              | 1.3.0            |
| `gravitino.entity.store.relational`                | Detailed implementation of Relational storage. `H2`, `MySQL` and `PostgreSQL` is supported, and the implementation is `JDBCBackend`.                                                                                                                    | `JDBCBackend`                     | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUrl`        | The database url that the `JDBCBackend` needs to connect to. If you use `MySQL` or `PostgreSQL`, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/{DATABASE_TYPE}/` directory. | `jdbc:h2`                         | No                                              | 0.5.0            |
| `gravitino.entity.store.relational.jdbcDriver`     | The jdbc driver name that the `JDBCBackend` needs to use. You should place the driver Jar package in the `${GRAVITINO_HOME}/libs/` directory.                                                                                                           | `org.h2.Driver`                   | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcUser`       | The username that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                       | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.jdbcPassword`   | The password that the `JDBCBackend` needs to use when connecting the database. It is required for `MySQL`.                                                                                                                                              | `gravitino`                       | Yes if the jdbc connection url is not `jdbc:h2` | 0.5.0            |
| `gravitino.entity.store.relational.storagePath`    | The storage path for embedded JDBC storage implementation. It supports both absolute and relative path, if the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is `${GRAVITINO_HOME}/data/jdbc`     | `${GRAVITINO_HOME}/data/jdbc`     | No                                              | 0.6.0-incubating |
| `gravitino.entity.store.relational.maxConnections` | The maximum number of connections for the JDBC Backend connection pool                                                                                                                                                                                  | `100`                             | No                                              | 0.9.0-incubating |
| `gravitino.entity.store.relational.maxWaitMillis`  | The maximum wait time in milliseconds for a connection from the JDBC Backend connection pool                                                                                                                                                            | `1000`                            | No                                              | 0.9.0-incubating |


:::caution
We strongly recommend that you change the default value of `gravitino.entity.store.relational.storagePath`, as it's under the deployment directory and future version upgrades may remove it.
:::

#### Create JDBC Backend Schema and Table

For H2 database, All tables needed by Gravitino are created automatically when the Gravitino server starts up. For MySQL, you should firstly initialize the database tables yourself by executing the ddl scripts in the `${GRAVITINO_HOME}/scripts/mysql/` directory.

### Storage Cache Configuration

To enable storage caching, modify the following settings in the `${GRAVITINO_HOME}/conf/gravitino.conf` file:

```
# Whether to enable the cache
gravitino.cache.enabled=true

# Specify the cache implementation (no need to use the fully qualified class name)
gravitino.cache.implementation=caffeine

# Number of lock segments for cache concurrency optimization
gravitino.cache.lockSegments=16
```

| Configuration Key                | Description                                | Default Value          | Required | Since Version |
|----------------------------------|--------------------------------------------|------------------------|----------|---------------|
| `gravitino.cache.enabled`        | Whether to enable caching                  | `true`                 | Yes      | 1.0.0         |
| `gravitino.cache.implementation` | Specifies the cache implementation         | `caffeine`             | Yes      | 1.0.0         |
| `gravitino.cache.maxEntries`     | Maximum number of entries allowed in cache | `10000`                | No       | 1.0.0         |
| `gravitino.cache.expireTimeInMs` | Cache expiration time (in milliseconds)    | `3600000` (about 1 hr) | No       | 1.0.0         |
| `gravitino.cache.enableStats`    | Whether to enable cache statistics logging | `false`                | No       | 1.0.0         |
| `gravitino.cache.enableWeigher`  | Whether to enable weight-based eviction    | `true`                 | No       | 1.0.0         |
| `gravitino.cache.lockSegments`   | Number of lock segments.                   | `16`                   | No       | 1.0.0         |

- `gravitino.cache.enableWeigher`: When enabled, eviction is based on weight and `maxEntries` will be ignored.
- `gravitino.cache.expireTimeInMs`: Controls the cache TTL in milliseconds.
- If `gravitino.cache.enableStats` is enabled, Gravitino will log cache statistics (hit count, miss count, load failures, etc.) every 5 minutes at the Info level.

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

### Job Configuration

The following table lists the job configuration items:

| Configuration Item                     | Description                                                                                                                                                               | Default Value                 | Required | Since Version |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|----------|---------------|
| `gravitino.job.stagingDir`             | Directory for managing staging files when running jobs.                                                                                                                   | `/tmp/gravitino/jobs/staging` | No       | 1.0.0         |
| `gravitino.job.executor`               | The executor to run jobs. By default it is `local`; users can implement their own executor and set it here.                                                               | `local`                       | No       | 1.0.0         |
| `gravitino.job.stagingDirKeepTimeInMs` | The time in milliseconds to keep the staging files of the finished job in the job staging directory. The minimum recommended value is 10 minutes if you are not testing.  | `604800000` (7 days)          | No       | 1.0.0         |
| `gravitino.job.statusPullIntervalInMs` | The interval in milliseconds to pull the job status from the job executor. The minimum recommended value is 1 minute if you are not testing.                              | `300000` (5 minutes)          | No       | 1.0.0         |

### Tree Lock Configuration

The Gravitino server uses a tree lock to ensure data consistency. The tree lock is an in-memory lock; Gravitino currently supports only in-memory locks. The configuration items are as follows:

| Configuration item                   | Description                                                   | Default value | Required | Since Version |
|--------------------------------------|---------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.lock.maxNodes`            | The maximum number of tree lock nodes to keep in memory       | 100000        | No       | 0.5.0         |
| `gravitino.lock.minNodes`            | The minimum number of tree lock nodes to keep in memory       | 1000          | No       | 0.5.0         |
| `gravitino.lock.cleanIntervalInSecs` | The interval in seconds to clean up the stale tree lock nodes | 60            | No       | 0.5.0         |

### Catalog Configuration

| Configuration item                           | Description                                                                                                                                                                                         | Default value | Required | Since version |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs`           | The interval in milliseconds to evict the catalog cache; default 3600000ms(1h).                                                                                                                                                                                                                                                                                                                          | `3600000` | No | 0.1.0 |
| `gravitino.catalog.classloader.isolated`               | Whether to use an isolated classloader for catalog. If `true`, an isolated classloader loads all catalog-related libraries and configurations, not the AppClassLoader. The default value is `true`.                                                                                                                                                                                                       | `true`    | No | 0.1.0 |
| `gravitino.catalog.credential.backfillToProperties`    | For backward compatibility only: if `true`, the server exposes hidden catalog credentials (such as jdbc-user and jdbc-password) in the catalog properties response so that connectors that do not support credential vending can still read them. **Enabling this is a security risk** — credentials are visible to anyone who can read catalog properties. Disable once all connectors are upgraded.       | `false`   | No | 1.3.0 |

### Schema configuration

| Configuration item          | Description                                                                                                                                                                                                                                                                                                                                                                          | Default value | Required | Since version |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.schema.separator` | The separator used to represent a hierarchical (multi-level) schema in schema names at the API boundary, e.g. `:` for `A:B:C`. It only takes effect for catalogs that support hierarchical schemas (currently the Iceberg catalog). The value must not be blank and must not contain `.` or the internal physical separator (ASCII-1, `\u0001`). See [Hierarchical schema](./lakehouse-iceberg-catalog.md#hierarchical-schema). | `:`           | No       | 1.3.0         |

### Auxiliary Service Configuration

| Configuration item            | Description                                                                                                                    | Default value | Since Version |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.names ` | The auxiliary service name of the Gravitino Iceberg REST server. Use **`iceberg-rest`** for the Gravitino Iceberg REST server. | (none)        | 0.2.0         |

Refer to [Iceberg REST catalog service](iceberg-rest-service.md) for configuration details.

### Event Listener Configuration

Gravitino provides event listener mechanism to allow users to capture the events which are provided by Gravitino server to integrate some custom operations.

To leverage the event listener, you must implement the `EventListenerPlugin` interface and place the JAR file in the classpath of the Gravitino server. Then, add configurations to gravitino.conf to enable the event listener.

| Property name                          | Description                                                                                            | Default value | Required | Since Version |
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

### Audit Log Configuration

The audit log framework defines how audit logs are formatted and written to various storages. The formatter defines an interface that transforms different `Event` types into a unified `AuditLog`. The writer defines an interface to writing AuditLog to different storages.

Gravitino provides a default implementation to log basic audit information to a file. You can extend the audit system by implementing the corresponding interfaces.

| Property name                         | Description                            | Default value                                   | Required | Since Version     |
|---------------------------------------|----------------------------------------|-------------------------------------------------|----------|-------------------|
| `gravitino.audit.enabled`             | The audit log enable flag.             | false                                           | NO       | 0.7.0-incubating  |
| `gravitino.audit.writer.className`    | The class name of audit log writer.    | org.apache.gravitino.audit.FileAuditWriter      | NO       | 0.7.0-incubating  |
| `gravitino.audit.formatter.className` | The class name of audit log formatter. | org.apache.gravitino.audit.v2.SimpleFormatterV2 | NO       | 0.7.0-incubating  |

#### Audit Log Formatter

The `Formatter` interface transforms an `Event` into an `AuditLog`. `SimpleFormatterV2` is the default implementation. `JsonAuditFormatter` is also available when structured JSON output is required. It emits one JSON object per line, serializes `customInfo`, and formats `timestamp` as ISO 8601 with millisecond precision and zone offset. Both simple and JSON formatters mask sensitive values such as `Authorization`, `Cookie`, `X-Amz-Security-Token`, `s3.access-key-id`, and `jdbc-password`.

#### Audit Log Writer

The `AuditLogWriter` interface enables writing audit logs to different storage mediums (files, databases, etc.).

`FileAuditWriter` is the default implementation. It delegates all file management — rotation, compression, and retention — to Log4j2 via a dedicated logger named `gravitino.audit`. The appender is configured in `conf/log4j2.properties` (see the `audit_file` appender group). The default configuration rotates daily and on 256 MB, compresses rotated files with gzip, and deletes files older than 30 days.

##### Deprecated FileAuditWriter properties

The following `gravitino.audit.writer.file.*` properties were accepted in earlier versions but are now **deprecated** and have no effect. `FileAuditWriter` emits a `WARN` log at startup if any of them are present. Configure the equivalent behavior directly in `conf/log4j2.properties` instead.

| Deprecated property                             | Migration: configure in `conf/log4j2.properties`                  |
|-------------------------------------------------|-------------------------------------------------------------------|
| `gravitino.audit.writer.file.fileName`          | `appender.audit_file.fileName`                                    |
| `gravitino.audit.writer.file.append`            | `appender.audit_file.append`                                      |
| `gravitino.audit.writer.file.flushIntervalSecs` | Use `immediateFlush` on the appender or an async appender wrapper |

Example — change the audit log path:

```properties
# conf/log4j2.properties
appender.audit_file.fileName = /var/log/gravitino/my_audit.log
appender.audit_file.filePattern = /var/log/gravitino/my_audit_%d{yyyyMMdd}.%i.log.gz
```

Example — adjust retention to 90 days:

```properties
appender.audit_file.strategy.delete.ifLastModified.age = 90d
```

### Security Configuration

Refer to [security](security/security.md) for HTTPS and authentication configurations.

| Configuration Item                         | Description                                                                                                                                                                                                                                          | Default Value | Required | Since Version |
|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.fetchFile.blockUnsafeRemoteUri` | Whether to block remote file URIs from resolving to unsafe addresses from the Gravitino server side. This applies to job files and catalog files such as Kerberos keytabs. Disable this only for trusted URIs that require access to such addresses. | `true`        | No       | 1.3.0         |

### Metrics Configuration

| Property name                             | Description                                          | Default value | Required | Since Version |
|-------------------------------------------|------------------------------------------------------|---------------|----------|---------------|
| `gravitino.metrics.timeSlidingWindowSecs` | The seconds of Gravitino metrics time sliding window | 60            | No       | 0.5.1         |

### Health Check Endpoints

Gravitino exposes three health check endpoints following [MicroProfile Health](https://microprofile.io/project/eclipse/microprofile-health) semantics. All endpoints are exempt from authentication so that Kubernetes probes, load balancers, and global traffic managers can reach them without credentials.

| Endpoint                | Description                                                                                                                                                                           | HTTP status |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `GET /api/health/live`  | Liveness probe. Returns 200 as long as the HTTP server thread can respond. Use this to determine whether to restart a pod.                                                            | 200         |
| `GET /api/health/ready` | Readiness probe. Returns 200 when the entity store is reachable within the configured timeout; 503 when the entity store is unavailable or slow. Use this to control traffic routing. | 200 / 503   |
| `GET /api/health`       | Aggregate check. Returns 200 when both liveness and readiness pass; 503 when any check fails.                                                                                         | 200 / 503   |

Root-level aliases are also available for global traffic managers that require probes at well-known root paths:

| Alias               | Forwards to             |
|---------------------|-------------------------|
| `GET /health`       | `GET /api/health`       |
| `GET /health/live`  | `GET /api/health/live`  |
| `GET /health/ready` | `GET /api/health/ready` |
| `GET /health.html`  | `GET /api/health`       |

**Configuration:**

| Property name                                        | Description                                                                               | Default value | Required | Since version |
|------------------------------------------------------|-------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `gravitino.server.health.entityStore.probeTimeoutMs` | Timeout in milliseconds for the entity-store readiness probe used by `/api/health/ready`. | `2000`        | No       | 1.3.0         |

**Response format:**

All endpoints return a JSON body with the same shape. The `code` field is always `0`. `status` is `UP` or `DOWN`. `checks` lists per-component results; liveness reports `httpServer` and readiness reports `entityStore`.

Healthy response (HTTP 200):

```json
{
  "code": 0,
  "status": "UP",
  "checks": [
    { "name": "httpServer", "status": "UP", "details": {} },
    { "name": "entityStore", "status": "UP", "details": {} }
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
    { "name": "entityStore", "status": "DOWN", "details": { "reason": "timeout" } }
  ]
}
```

Possible `reason` values in the `entityStore` DOWN check: `timeout`, `interrupted`, `probe-rejected`, or the class name of an unexpected exception.

### Memory Settings

`GRAVITINO_MEM` sets JVM heap/metaspace flags for the Gravitino server and is also read by the Iceberg REST server and Lance REST server launchers.

Default: `-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m` (see `bin/common.sh`). Launch scripts append this to `JAVA_OPTS`; override `GRAVITINO_MEM` when you need different heap/metaspace sizes.

Typical values:
- Development: `-Xms1g -Xmx1g -XX:MaxMetaspaceSize=512m`
- Moderate production: `-Xms4g -Xmx4g -XX:MaxMetaspaceSize=1g`
- Larger deployments: `-Xms8g -Xmx8g -XX:MaxMetaspaceSize=1g` or higher depending on catalog count, plugins, and query concurrency

## Catalog Properties Configuration

There are three types of catalog properties:

1. **Gravitino defined properties**: Gravitino defines these properties as the necessary
   configurations for the catalog to work properly.
2. **Properties with the `gravitino.bypass.` prefix**: These properties are not managed by
   Gravitino and pass directly to the underlying system for advanced usage.

:::warning
Using `gravitino.bypass.` properties to pass credentials, tokens, or access keys can expose
sensitive values in plaintext, because these properties are not fully managed by Gravitino and may
be returned in plaintext via REST API responses. If an underlying system requires credentials to be
passed this way, restrict access to the related REST APIs.
:::

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
| `lakehouse-paimon`  | [Lakehouse Paimon catalog properties](lakehouse-paimon-catalog.md#catalog-properties)   | `catalogs/lakehouse-paimon/conf/lakehouse-paimon.conf`   |
| `lakehouse-hudi`    | [Lakehouse Hudi catalog properties](lakehouse-hudi-catalog.md#catalog-properties)       | `catalogs/lakehouse-hudi/conf/lakehouse-hudi.conf`       |
| `jdbc-mysql`        | [MySQL catalog properties](jdbc-mysql-catalog.md#catalog-properties)                    | `catalogs/jdbc-mysql/conf/jdbc-mysql.conf`               |
| `jdbc-postgresql`   | [PostgreSQL catalog properties](jdbc-postgresql-catalog.md#catalog-properties)          | `catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`     |
| `jdbc-doris`        | [Doris catalog properties](jdbc-doris-catalog.md#catalog-properties)                    | `catalogs/jdbc-doris/conf/jdbc-doris.conf`               |
| `jdbc-oceanbase`    | [OceanBase catalog properties](jdbc-oceanbase-catalog.md#catalog-properties)            | `catalogs/jdbc-oceanbase/conf/jdbc-oceanbase.conf`       |
| `kafka`             | [Kafka catalog properties](kafka-catalog.md#catalog-properties)                         | `catalogs/kafka/conf/kafka.conf`                         |
| `fileset`           | [Fileset catalog properties](fileset-catalog#catalog-properties)                        | `catalogs/fileset/conf/fileset.conf`                     |
| `model`             | [Fileset catalog properties](model-catalog#catalog-properties)                          | `catalogs/model/conf/model.conf`                         |

:::info
The Gravitino server automatically adds the catalog properties configuration directory to classpath.
:::

## Some Other Configurations

You could put HDFS configuration file to the catalog properties configuration dir, like `catalogs/lakehouse-iceberg/conf/`.

## Docker Instructions

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
| `GRAVITINO_ICEBERG_REST_URI`                             | `gravitino.iceberg-rest.uri`                         | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_IO_IMPL`                         | `gravitino.iceberg-rest.io-impl`                     | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_CATALOG_BACKEND`                 | `gravitino.iceberg-rest.catalog-backend`             | `memory`                                             | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_DRIVER`                     | `gravitino.iceberg-rest.jdbc-driver`                 | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_USER`                       | `gravitino.iceberg-rest.jdbc-user`                   | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_JDBC_PASSWORD`                   | `gravitino.iceberg-rest.jdbc-password`               | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_WAREHOUSE`                       | `gravitino.iceberg-rest.warehouse`                   | `/tmp/`                                              | 1.0.0         |
| `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS`            | `gravitino.iceberg-rest.credential-providers`        | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_GCS_SERVICE_ACCOUNT_FILE`        | `gravitino.iceberg-rest.gcs-service-account-file`    | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ACCESS_KEY`                   | `gravitino.iceberg-rest.s3-access-key-id`            | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_SECRET_KEY`                   | `gravitino.iceberg-rest.s3-secret-access-key`        | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ENDPOINT`                     | `gravitino.iceberg-rest.s3-endpoint`                 | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_REGION`                       | `gravitino.iceberg-rest.s3-region`                   | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_PATH_STYLE_ACCESS`            | `gravitino.iceberg-rest.s3-path-style-access`        | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_ROLE_ARN`                     | `gravitino.iceberg-rest.s3-role-arn`                 | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_EXTERNAL_ID`                  | `gravitino.iceberg-rest.s3-external-id`              | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_S3_TOKEN_SERVICE_ENDPOINT`       | `gravitino.iceberg-rest.s3-token-service-endpoint`   | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_NAME`      | `gravitino.iceberg-rest.azure-storage-account-name`  | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_KEY`       | `gravitino.iceberg-rest.azure-storage-account-key`   | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_TENANT_ID`                 | `gravitino.iceberg-rest.azure-tenant-id`             | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_ID`                 | `gravitino.iceberg-rest.azure-client-id`             | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_SECRET`             | `gravitino.iceberg-rest.azure-client-secret`         | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ACCESS_KEY`                  | `gravitino.iceberg-rest.oss-access-key-id`           | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_SECRET_KEY`                  | `gravitino.iceberg-rest.oss-secret-access-key`       | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ENDPOINT`                    | `gravitino.iceberg-rest.oss-endpoint`                | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_REGION`                      | `gravitino.iceberg-rest.oss-region`                  | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_ROLE_ARN`                    | `gravitino.iceberg-rest.oss-role-arn`                | (none)                                               | 1.3.0         |
| `GRAVITINO_ICEBERG_REST_OSS_EXTERNAL_ID`                 | `gravitino.iceberg-rest.oss-external-id`             | (none)                                               | 1.3.0         |

:::note
The Gravitino server Docker image bundles MySQL and PostgreSQL JDBC drivers under
`jdbc-drivers/`. The container startup script links them into `libs/` and
`iceberg-rest-server/libs/` for the auxiliary Iceberg REST service.

For cloud storage backends such as S3, OSS, Azure, or GCS, place the corresponding Iceberg
bundle jars under `iceberg-bundles/`. The startup script links them into
`iceberg-rest-server/libs/` at container startup.
:::

### Usage examples

To start a container and override the default HTTP port:

```shell
docker run --rm -d \
  -e GRAVITINO_SERVER_WEBSERVER_HTTP_PORT=8080 \
  -p 8080:8080 \
  apache/gravitino:<tag>
```

To configure the relational entity store with PostgreSQL:

```shell
docker run --rm -d \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL="jdbc:postgresql://localhost:5432/database1" \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER="org.postgresql.Driver" \
  -p 8090:8090 \
  apache/gravitino:<tag>
```

To configure the auxiliary Iceberg REST service with local storage:

```shell
docker run --rm -d \
  -p 8090:8090 \
  -p 9001:9001 \
  -e GRAVITINO_ICEBERG_REST_WAREHOUSE=/tmp/warehouse/ \
  -e GRAVITINO_ICEBERG_REST_HTTP_PORT=9001 \
  apache/gravitino:<tag>
```

Verify that the configuration was applied correctly by inspecting the container's `gravitino.conf`:

```shell
docker exec -it <container_id> cat /opt/gravitino/conf/gravitino.conf
```

To verify that the auxiliary Iceberg REST service is running:

```shell
curl http://127.0.0.1:9001/iceberg/v1/config
```

:::note
If both `gravitino.conf` and environment variable exist, the container’s startup script will overwrite the config file value with the environment variable.
:::


## Set Up Runtime Environment Variables

The Gravitino server supports configuring runtime environment variables in two ways:

1. **Local deployment:** Modify `gravitino-env.sh` located in the `conf` directory.
2. **Docker container deployment:** Use environment variable injection during container startup. *(Since 1.0.0)*

### Access Apache Hadoop

Due to the absence of a comprehensive user permission system, Gravitino can only use a single username for
Apache Hadoop access. Ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN, etc.) access
permissions; otherwise, you may encounter a `Permission denied` error. There are two ways to resolve this error:

* Grant Gravitino startup user permissions in Hadoop
* Specify the authorized Hadoop username in the environment variables `HADOOP_USER_NAME` before starting the Gravitino server.
