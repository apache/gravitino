---
title: "Gravitino Server Configuration"
slug: "/gravitino-server-config"
keywords:
  - configuration
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino server reads `conf/gravitino.conf` at startup. Almost every property has a
default, so the server starts with an empty file, and most deployments change only a handful of
them. The exception is `gravitino.authorization.serviceAdmins`, which you must set once you turn
authorization on.

This page covers the server itself. Catalog properties, which configure an individual catalog
rather than the server, are covered further down. Properties for the auxiliary services live with
those services: see [Iceberg REST Catalog Service](iceberg-rest-service.md) and
[Security](security/how-to-authenticate.md).

## Quick Start

### Development

The defaults are already right for local work. The server listens on `0.0.0.0:8090` and keeps its
metadata in an embedded H2 database, so no configuration is needed:

```shell
${GRAVITINO_HOME}/bin/gravitino.sh start
```

One property is still worth setting. By default, H2 writes its database files to
`${GRAVITINO_HOME}/data/jdbc`, which is inside the unpacked distribution. Upgrading Gravitino
means unpacking a new distribution, so the metadata sits in the very directory you are about to
replace or abandon. Move it somewhere the upgrade does not touch:

```text
# conf/gravitino.conf
gravitino.entity.store.relational.storagePath = /var/lib/gravitino/data/jdbc
```

Nothing here is authenticated. The default `simple` authenticator takes whatever username the
client sends, the server speaks plain HTTP, and authorization is off, so every caller can do
everything. Together with H2, for which Gravitino makes no consistency or durability guarantee,
that makes this configuration unfit for anything but local work.

### Production

A production server keeps metadata in MySQL or PostgreSQL, authenticates its callers, enforces
authorization, and writes an audit log:

```text
# conf/gravitino.conf
# Entity store
gravitino.entity.store.relational.jdbcUrl      = jdbc:mysql://{db_host}:3306/{database}
gravitino.entity.store.relational.jdbcDriver   = com.mysql.cj.jdbc.Driver
gravitino.entity.store.relational.jdbcUser     = {username}
gravitino.entity.store.relational.jdbcPassword = {password}

# Transport
gravitino.server.webserver.enableHttps      = true
gravitino.server.webserver.keyStorePath     = /etc/gravitino/tls/server.jks
gravitino.server.webserver.keyStorePassword = {keystore_password}
gravitino.server.webserver.managerPassword  = {manager_password}

# Authentication
gravitino.authenticators                            = oauth
gravitino.authenticator.oauth.jwksUri               = {jwks_uri}
gravitino.authenticator.oauth.tokenValidatorClass   = org.apache.gravitino.server.authentication.JwksTokenValidator
gravitino.authenticator.oauth.serviceAudience       = {audience}
gravitino.authenticator.oauth.principalFields       = preferred_username,email,sub

# Authorization
gravitino.authorization.enable        = true
gravitino.authorization.serviceAdmins = {admin_user}

# Audit log
gravitino.audit.enabled = true

# Entity cache
gravitino.cache.enabled        = true
gravitino.cache.implementation = caffeine
gravitino.cache.lockSegments   = 16
gravitino.cache.enableStats    = true
```

Only `gravitino.cache.enableStats` changes behavior here; it logs hit count, miss count, and load
failures every five minutes, which is what makes a cache problem visible in production. The three
lines above it restate defaults, and are spelled out so the cache configuration is reviewable in
one place rather than inferred from its absence. Raise `lockSegments` above the default if the
server runs hot enough for cache lock contention to show up in profiles.

Four things this block depends on:

**The database schema is not created for you.** Initialize it and put the JDBC driver jar in
`${GRAVITINO_HOME}/libs/` before the first start. See
[Relational Backend Storage](how-to-use-relational-backend-storage.md).

**HTTPS replaces HTTP rather than joining it.** A server with `enableHttps` set no longer serves
plain HTTP, so clients and the Web UI must move to `httpsPort`, which defaults to `8433`. See
[HTTPS](security/how-to-use-https.md).

**The authenticator is one line, its provider is not.** The block above validates JWTs against a
JWKS endpoint, which is the common case for an external identity provider. Static sign keys,
Kerberos, and the OIDC login flow for the Web UI each take a different set of properties. See
[How to Authenticate](security/how-to-authenticate.md), or
[Local users and groups](security/local-users-and-groups.md) to keep users and groups in
Gravitino's own metadata store instead of an external provider.

**`gravitino.authorization.serviceAdmins` has no default.** It is the one property here that
Gravitino will not fill in for you, and enabling authorization without it fails at startup. What
those admins and everyone else may then do is the subject of
[Access Control](security/access-control.md).

Give the JVM more than the 1 GB it takes by default:

```shell
export GRAVITINO_MEM="-Xms4g -Xmx4g -XX:MaxMetaspaceSize=1g"
```

### Docker

The Gravitino image does not simply run the server against the configuration file you give it. At
startup the entrypoint rewrites `conf/gravitino.conf`, applying its own defaults over roughly two
dozen properties and then applying any supported environment variables. Configure the container
through environment variables:

```shell
docker run --rm -d \
  -p 8090:8090 \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL="jdbc:postgresql://{db_host}:5432/{database}" \
  -e GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER="org.postgresql.Driver" \
  apache/gravitino:{tag}
```

To supply a configuration file instead, for example from a Kubernetes ConfigMap, disable the
rewrite with `SKIP_CONFIG_REWRITE=true`. See
[Container Configuration](#container-configuration) for what the rewrite does and which variables
it recognizes.

### Running More Than One Server

Servers behind a load balancer share the entity store but keep local caches. Each server polls the
entity change log and invalidates entries that another server has modified. The defaults are safe:
a three second poll, with poll failures logged and retried on the next cycle. Point the load
balancer's health check at `GET /health/ready` so a server that has lost its database stops
receiving traffic.

## Server Configuration

Every property in this section belongs in `${GRAVITINO_HOME}/conf/gravitino.conf`, one
`property = value` pair per line. The server reads the file once, at startup, so a change
takes effect on the next restart. A Default Value of `(empty)` means the property exists with an
empty string or list; `(none)` means it has no default at all.

### Serving Requests

#### HTTP Server

| Configuration Item                                   | Description                                                                                                                                                                                  | Default Value                           |
|------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `gravitino.server.webserver.host`                    | The address the server binds to.                                                                                                                                                             | `0.0.0.0`                               |
| `gravitino.server.webserver.httpPort`                | The port the server listens on.                                                                                                                                                              | `8090`                                  |
| `gravitino.server.webserver.minThreads`              | Minimum threads in the Jetty thread pool. Values below 8 are raised to 8.                                                                                                                    | Twice the processor count, 8 to 100     |
| `gravitino.server.webserver.maxThreads`              | Maximum threads in the Jetty thread pool. Values below 8 are raised to 8, and the value must be at least `minThreads`.                                                                       | Four times the processor count, min 400 |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | Size of the Jetty thread pool work queue.                                                                                                                                                    | `100`                                   |
| `gravitino.server.webserver.idleTimeout`             | Timeout in milliseconds for idle connections.                                                                                                                                                | `30000`                                 |
| `gravitino.server.webserver.stopTimeout`             | Time in milliseconds Jetty waits for a graceful shutdown. See `org.eclipse.jetty.server.Server#setStopTimeout`.                                                                              | `30000`                                 |
| `gravitino.server.shutdown.timeout`                  | Time in milliseconds for the Gravitino server itself to shut down gracefully.                                                                                                                | `3000`                                  |
| `gravitino.server.webserver.requestHeaderSize`       | Maximum size in bytes of an HTTP request header.                                                                                                                                             | `131072`                                |
| `gravitino.server.webserver.responseHeaderSize`      | Maximum size in bytes of an HTTP response header.                                                                                                                                            | `131072`                                |
| `gravitino.server.webserver.customFilters`           | Comma-separated list of servlet filter class names to apply to the API.                                                                                                                      | (empty)                                 |
| `gravitino.server.rest.extensionPackages`            | Comma-separated list of packages to scan for additional REST resources.                                                                                                                      | (empty)                                 |
| `gravitino.server.visibleConfigs`                    | Comma-separated list of extra properties to expose on the unauthenticated `GET /configs` endpoint, on top of the fixed set it always returns. Additive, so each entry widens what is public. | (empty)                                 |

Filters named in `customFilters` must be standard `javax.servlet` filters. Pass parameters to a
filter with properties of the form
`gravitino.server.webserver.{filter_class_name}.param.{param_name} = {value}`.

`GET /configs` backs the Web UI, so it answers without authentication and always returns
`gravitino.authenticators`, `gravitino.authorization.enable`, and `gravitino.schema.separator`.
It adds `gravitino.authorization.serviceAdmins` when authorization is on, and the OAuth client
settings when `oauth` is among the authenticators. Treat anything you add through
`visibleConfigs` as public.

Two further groups of `gravitino.server.webserver.*` properties are documented elsewhere, because
they belong to features rather than to the web server itself. TLS, key stores, trust stores, and
client certificate authentication are in [HTTPS](security/how-to-use-https.md). The CORS filter
and its allowed origins, methods, and headers, needed when a browser client runs on a different
origin than the server, are in [CORS](security/how-to-use-cors.md).

#### Schema Names

Catalogs that support hierarchical schemas expose the hierarchy as a single delimited name at
the API boundary. Internally the levels are stored with ASCII-1 as the physical separator, so
the configured separator is an external representation only, and it may be neither blank, nor
`.`, nor ASCII-1.

| Configuration Item           | Description                                                                                                                                                       | Default Value |
|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.schema.separator` | Separator representing a multi-level schema name at the API boundary, as in `A:B:C`. See [Hierarchical schema](lakehouse-iceberg-catalog.md#hierarchical-schema). | `:`           |

#### Health Check Endpoints

Gravitino exposes three health endpoints following
[MicroProfile Health](https://microprofile.io/project/eclipse/microprofile-health) semantics. All
of them are exempt from authentication, so Kubernetes probes, load balancers, and traffic managers
reach them without credentials.

| Endpoint                | Root Alias          | Description                                                                                                                                 | HTTP Status |
|-------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `GET /api/health/live`  | `GET /health/live`  | Liveness. Returns 200 as long as an HTTP server thread can respond. Use it to decide whether to restart a pod.                              | 200         |
| `GET /api/health/ready` | `GET /health/ready` | Readiness. Returns 200 when the entity store answers within the probe timeout, 503 when it is unavailable or slow. Use it to route traffic. | 200 or 503  |
| `GET /api/health`       | `GET /health`       | Aggregate. Returns 200 when both of the above pass. Also aliased as `GET /health.html`.                                                     | 200 or 503  |

| Configuration Item                                   | Description                                                         | Default Value |
|------------------------------------------------------|---------------------------------------------------------------------|---------------|
| `gravitino.server.health.entityStore.probeTimeoutMs` | Timeout in milliseconds for the entity store probe behind `/ready`. | `2000`        |

Every endpoint returns the same JSON shape, but not the same checks. `code` is always `0`,
`status` is `UP` or `DOWN`, and `checks` carries one entry per component probed. `/live` reports
`httpServer` alone, `/ready` reports `entityStore` alone, and the aggregate endpoint reports both:

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

A failing `entityStore` check reports `timeout`, `interrupted`, `probe-rejected`,
`entity store not initialized`, or the simple class name of an unexpected exception.

#### JVM Memory

`GRAVITINO_MEM` sets the heap and metaspace flags. The launch scripts append it to `JAVA_OPTS`, and
the Iceberg REST server and Lance REST server launchers read the same variable. Set it in
`conf/gravitino-env.sh` or in the environment before starting the server.

The default, from `bin/common.sh`, is `-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m`. Raise it in
line with catalog count, plugin count, and query concurrency: `-Xms4g -Xmx4g
-XX:MaxMetaspaceSize=1g` suits a moderate production server, and larger deployments go beyond that.

#### Metrics

| Configuration Item                        | Description                                          | Default Value |
|-------------------------------------------|------------------------------------------------------|---------------|
| `gravitino.metrics.timeSlidingWindowSecs` | Width in seconds of the metrics time sliding window. | `60`          |

### Storing Metadata

#### Storage Backend

Gravitino stores metadata over JDBC. H2 is the default because it is embedded and needs nothing
external, which makes it right for local development and wrong for anything else: Gravitino makes
no consistency or durability guarantee for metadata held in H2. Production deployments use MySQL or
PostgreSQL, and the setup procedure for both is in
[Relational Backend Storage](how-to-use-relational-backend-storage.md).

The driver, user, and password properties are required whenever the URL is not `jdbc:h2`.

| Configuration Item                                 | Description                                                                                                                                                                                                          | Default Value                 |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------|
| `gravitino.entity.store`                           | Entity storage implementation. `relational` is the only supported value.                                                                                                                                             | `relational`                  |
| `gravitino.entity.store.relational`                | Relational storage implementation. `JDBCBackend` is the only supported value, and it covers H2, MySQL, and PostgreSQL.                                                                                               | `JDBCBackend`                 |
| `gravitino.entity.store.relational.jdbcUrl`        | Database URL the backend connects to.                                                                                                                                                                                | `jdbc:h2`                     |
| `gravitino.entity.store.relational.jdbcDriver`     | Driver class name. Place the driver jar in `${GRAVITINO_HOME}/libs/`.                                                                                                                                                | `org.h2.Driver`               |
| `gravitino.entity.store.relational.jdbcUser`       | Database username.                                                                                                                                                                                                   | `gravitino`                   |
| `gravitino.entity.store.relational.jdbcPassword`   | Database password.                                                                                                                                                                                                   | `gravitino`                   |
| `gravitino.entity.store.relational.storagePath`    | Where embedded H2 keeps its files. A relative value resolves against `${GRAVITINO_HOME}`. The default sits inside the deployment directory, so an upgrade that replaces that directory discards the data. Change it. | `${GRAVITINO_HOME}/data/jdbc` |
| `gravitino.entity.store.relational.maxConnections` | Maximum size of the JDBC connection pool.                                                                                                                                                                            | `100`                         |
| `gravitino.entity.store.relational.maxWaitMillis`  | Maximum wait in milliseconds for a connection from the pool.                                                                                                                                                         | `1000`                        |
| `gravitino.entity.store.maxTransactionSkewTimeMs`  | Maximum transaction skew in milliseconds.                                                                                                                                                                            | `2000`                        |
| `gravitino.entity.store.deleteAfterTimeMs`         | How long in milliseconds deleted and superseded rows are kept. Accepts 10 minutes to 30 days.                                                                                                                        | `604800000` (7 days)          |
| `gravitino.entity.store.versionRetentionCount`     | Number of entity versions kept, including the current one. Accepts 1 to 10.                                                                                                                                          | `1`                           |

#### Caching

The server caches entities in memory to avoid reading the backend on every request. Caching is on
by default, and the properties below tune what it holds and how it evicts.

| Configuration Item               | Description                                                                         | Default Value      |
|----------------------------------|-------------------------------------------------------------------------------------|--------------------|
| `gravitino.cache.enabled`        | Whether to cache entities at all.                                                   | `true`             |
| `gravitino.cache.implementation` | Cache implementation. Use the short name, not a fully qualified class name.         | `caffeine`         |
| `gravitino.cache.maxEntries`     | Maximum number of cached entries. Ignored when `enableWeigher` is `true`.           | `10000`            |
| `gravitino.cache.expireTimeInMs` | Time to live in milliseconds, measured from entry creation.                         | `3600000` (1 hour) |
| `gravitino.cache.enableWeigher`  | Whether to evict by weight rather than by entry count.                              | `true`             |
| `gravitino.cache.enableStats`    | Whether to log hit count, miss count, and load failures every five minutes at INFO. | `false`            |
| `gravitino.cache.lockSegments`   | Number of lock segments used to reduce contention.                                  | `16`               |

Two eviction limits apply at once. Time to live always applies: an entry older than
`expireTimeInMs` expires and is cleaned up asynchronously. Alongside it, the cache bounds its size
either by count or by weight. With `enableWeigher` disabled, Caffeine's W-TinyLFU policy evicts the
least-used entries once `maxEntries` is reached. With `enableWeigher` enabled, each entity type
carries a weight, larger for entities higher in the hierarchy, and eviction targets a total weight
budget instead; `maxEntries` is ignored, and a single entry heavier than the whole budget is never
cached.

#### Change Log Propagation

Caches are local to each server, so a metalake modified on one server would otherwise stay stale on
its neighbors. Every server writes its changes to an entity change log table and polls that table
to invalidate what other servers have touched. A separate cleaner trims old rows.

| Configuration Item                              | Description                                                              | Default Value   |
|--------------------------------------------------|---------------------------------------------------------------------------|-----------------|
| `gravitino.entityChangeLog.pollIntervalSecs`    | Interval in seconds between polls. Must be positive.                    | `3`             |
| `gravitino.entityChangeLog.retentionSecs`       | How long in seconds change log rows are kept before being pruned. `0` disables cleanup. Must be non-negative. | `86400` (1 day) |
| `gravitino.entityChangeLog.cleanupIntervalSecs` | Interval in seconds between cleanup runs that prune expired change log rows. Must be positive. | `3600` (1 hour) |

#### Tree Lock

Gravitino serializes conflicting metadata operations with an in-memory tree lock. It is the only
lock implementation available, and it is per-server.

| Configuration Item                   | Description                                          | Default Value |
|--------------------------------------|------------------------------------------------------|---------------|
| `gravitino.lock.maxNodes`            | Maximum tree lock nodes held in memory.              | `100000`      |
| `gravitino.lock.minNodes`            | Minimum tree lock nodes held in memory.              | `1000`        |
| `gravitino.lock.cleanIntervalInSecs` | Interval in seconds for reclaiming stale lock nodes. | `60`          |

### Loading Catalogs

These properties govern how the server loads and isolates catalogs. The properties that configure
an individual catalog are covered under [Catalog Properties](#catalog-properties).

`credential.backfillToProperties` below is the escape hatch for connectors that cannot consume
vended credentials; the mechanism it opts out of is described in
[Credential Vending](security/credential-vending.md).

| Configuration Item                                  | Description                                                                                                                                                                                                                                                                                            | Default Value |
|-----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs`        | Interval in milliseconds before an idle catalog is evicted from the catalog cache.                                                                                                                                                                                                                     | `3600000`     |
| `gravitino.catalog.classloader.isolated`            | Whether to load each catalog's libraries and configuration in an isolated classloader rather than the application classloader.                                                                                                                                                                         | `true`        |
| `gravitino.catalog.credential.backfillToProperties` | Whether to return hidden catalog credentials such as `jdbc-user` and `jdbc-password` in the catalog properties response, for connectors that cannot consume vended credentials. Anyone who can read catalog properties can then read those credentials. Turn it off once your connectors are upgraded. | `false`       |

### Securing the Server

#### Authentication

Authentication decides who a caller is. It is off by default: an unconfigured server trusts
whatever username the client sends.

| Configuration Item         | Description                                                                                                    | Default Value |
|----------------------------|----------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.authenticators` | Comma-separated authenticators to enable. Valid values are `simple`, `oauth`, `kerberos`, `basic`, and `none`. | `simple`      |

Naming an authenticator is one line; configuring it is not. Each value reads its own family of
`gravitino.authenticator.*` properties, covered in
[How to Authenticate](security/how-to-authenticate.md). To hold users, password hashes, and group
membership in Gravitino's own relational store rather than an external provider, see
[Local users and groups](security/local-users-and-groups.md).

`gravitino.authenticator`, in the singular, is a deprecated spelling that still works.

#### Authorization

Authorization decides what an authenticated caller may do. It is also off by default, and turning
it on requires naming the service admins, since that property has no default of its own.

| Configuration Item                       | Description                                                                                | Default Value                                                         |
|------------------------------------------|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| `gravitino.authorization.enable`         | Whether to enforce privileges on metadata operations.                                      | `false`                                                               |
| `gravitino.authorization.serviceAdmins`  | Comma-separated users who administer the service. Metalake creation is restricted to them. | (none)                                                                |
| `gravitino.authorization.impl`           | Authorizer implementation.                                                                 | `org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer` |
| `gravitino.authorization.threadPoolSize` | Threads serving authorization checks.                                                      | `100`                                                                 |

The privilege model these properties switch on, roles, grants, ownership, and the
`gravitino.authorization.jcasbin.*` tuning of the default authorizer, is described in
[Access Control](security/access-control.md). To push enforcement down into the underlying systems
through Apache Ranger or a native permission model, see
[Authorization Pushdown](security/authorization-pushdown.md).

#### Remote File Fetching

The server fetches files by URI in two situations: staging a job's files, and loading catalog files
such as a Kerberos keytab. Both accept remote URIs, so both are a path by which a caller who can
create a catalog or submit a job could make the server issue requests of its own.

| Configuration Item                         | Description                                                                                                   | Default Value |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.fetchFile.blockUnsafeRemoteUri` | Whether to refuse remote URIs that resolve to unsafe addresses. Disable only for trusted URIs that need them. | `true`        |

#### Audit Logging

The audit log framework has two halves. A formatter turns an `Event` into an `AuditLog`, and a
writer puts that `AuditLog` somewhere. Both are interfaces, so a deployment with its own log
pipeline can replace either.

| Configuration Item                    | Description                    | Default Value                                     |
|---------------------------------------|--------------------------------|---------------------------------------------------|
| `gravitino.audit.enabled`             | Whether to write an audit log. | `false`                                           |
| `gravitino.audit.formatter.className` | Formatter class name.          | `org.apache.gravitino.audit.v2.SimpleFormatterV2` |
| `gravitino.audit.writer.className`    | Writer class name.             | `org.apache.gravitino.audit.FileAuditWriter`      |

`SimpleFormatterV2` is the default formatter. `JsonAuditFormatter` is available where structured
output is wanted: it emits one JSON object per line, serializes `customInfo`, and writes timestamps
as ISO 8601 with millisecond precision and a zone offset. Both formatters replace the value of a
sensitive `customInfo` key with `***`. The masked keys are `authorization`, `cookie`,
`x-amz-security-token`, `s3.access-key-id`, and `jdbc-password`.

`FileAuditWriter` is the default writer, and it manages no files itself. Rotation, compression, and
retention are delegated to Log4j2 through a logger named `gravitino.audit`, configured by the
`audit_file` appender group in `conf/log4j2.properties`. Out of the box it writes
`gravitino_audit.log` under the log directory, rotates daily and at 256 MB, gzips what it rotates,
and deletes anything older than 30 days. Change the path or the retention there:

```properties
# conf/log4j2.properties
appender.audit_file.fileName    = /var/log/gravitino/my_audit.log
appender.audit_file.filePattern = /var/log/gravitino/my_audit_%d{yyyyMMdd}.%i.log.gz

appender.audit_file.strategy.delete.ifAll.ifLastModified.age = 90d
```

Earlier releases configured the writer directly through `gravitino.audit.writer.file.*`. Those
properties now do nothing, and `FileAuditWriter` logs a warning at startup if it finds any of them.

| Removed Property                                | Configure Instead In `conf/log4j2.properties`                     |
|-------------------------------------------------|-------------------------------------------------------------------|
| `gravitino.audit.writer.file.fileName`          | `appender.audit_file.fileName`                                    |
| `gravitino.audit.writer.file.append`            | `appender.audit_file.append`                                      |
| `gravitino.audit.writer.file.flushIntervalSecs` | `immediateFlush` on the appender, or wrap it in an async appender |

### Extending the Server

#### Event Listeners

An event listener receives the events Gravitino emits around metadata operations, which is how
external systems observe the catalog without polling it. To use one, implement
`EventListenerPlugin`, put the jar on the server classpath, and name it in `gravitino.conf`.

| Configuration Item                     | Description                                                                            | Default Value |
|----------------------------------------|----------------------------------------------------------------------------------------|---------------|
| `gravitino.eventListener.names`        | Comma-separated listener names, as in `audit,sync`.                                    | (empty)       |
| `gravitino.eventListener.{name}.class` | Class name of the listener registered under `{name}`.                                  | (none)        |
| `gravitino.eventListener.{name}.{key}` | Any other property under a listener's name is passed through to that plugin unchanged. | (none)        |

Every name in `names` needs a matching `{name}.class`, or the server fails to build that listener.

Each operation emits up to three events: a pre-event before it runs, a post-event after it
succeeds, and a failure event after it throws. The names follow the operation, so `createTable`
produces `CreateTablePreEvent`, `CreateTableEvent`, and `CreateTableFailureEvent`. Operations
served by the Gravitino IRC endpoint carry an `Iceberg` prefix, as in `IcebergCreateTableEvent`.
Not every operation defines all three. The full set of classes lives in the
[`org.apache.gravitino.listener.api.event`](https://github.com/apache/gravitino/tree/main/core/src/main/java/org/apache/gravitino/listener/api/event)
package.

Throwing a `ForbiddenException` from a pre-event handler stops the operation before it runs, which
makes pre-events a veto point rather than a notification.

A plugin declares how its events are dispatched:

| Mode             | Behavior                                                                                                                         |
|------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `SYNC`           | Processed inline, before the operation's result reaches the client. A slow listener slows the request.                           |
| `ASYNC_SHARED`   | Processed on a queue and dispatcher shared with other listeners. One slow listener degrades the rest, and events can be dropped. |
| `ASYNC_ISOLATED` | Processed on a queue and dispatcher of its own. Better isolation, at the cost of a queue and thread per listener.                |

#### Auxiliary Services

An auxiliary service runs inside the Gravitino server process on its own port. The property has
no default, but the `gravitino.conf` shipped in the distribution sets it to
`iceberg-rest,lance-rest`, so both start unless you change the line.

| Configuration Item           | Description                                                                                                                  | Default Value |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.names` | Comma-separated auxiliary services to start. `iceberg-rest` is the Gravitino IRC server, `lance-rest` the Lance REST server. | (empty)       |

The rest of the IRC configuration, and the `gravitino.lance-rest.*` properties of the Lance REST
server, are documented with those services. See
[Iceberg REST Catalog Service](iceberg-rest-service.md).

#### Jobs

| Configuration Item                     | Description                                                                                                | Default Value                 |
|----------------------------------------|------------------------------------------------------------------------------------------------------------|-------------------------------|
| `gravitino.job.executor`               | Executor that runs jobs. Implement your own and name it here to replace the built-in one.                  | `local`                       |
| `gravitino.job.stagingDir`             | Directory holding staging files for running jobs.                                                          | `/tmp/gravitino/jobs/staging` |
| `gravitino.job.stagingDirKeepTimeInMs` | How long in milliseconds a finished job's staging files are kept. Use at least 10 minutes outside testing. | `604800000` (7 days)          |
| `gravitino.job.statusPullIntervalInMs` | Interval in milliseconds between job status polls. Use at least 1 minute outside testing.                  | `300000` (5 minutes)          |

## Catalog Properties

Catalog properties configure one catalog rather than the server. They come from two places: a
catalog configuration file supplies defaults for every catalog of that provider, and the
`properties` field on a create-catalog request supplies values for that catalog alone. The request
wins. Neither affects schema or table properties.

A catalog property is one of three kinds. Gravitino defines some itself, as the settings a catalog
needs to work. Anything prefixed `gravitino.bypass.` passes straight through to the underlying
system untouched. Anything else Gravitino simply stores for you to use as you like.

Passing credentials, tokens, or access keys through `gravitino.bypass.` exposes them: bypassed
properties are not managed by Gravitino and can come back in plaintext from the REST API. Where an
underlying system leaves no alternative, restrict access to the catalog APIs accordingly.

These properties apply to every catalog:

| Configuration Item  | Description                                                                                                                                            | Default Value |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `package`           | Path to the catalog package, from which Gravitino loads the catalog's libraries and configuration. It holds a `conf` directory and a `libs` directory. | (none)        |
| `cloud.name`        | Cloud the catalog runs on. One of `aws`, `azure`, `gcp`, `on_premise`, or `other`.                                                                     | (none)        |
| `cloud.region-code` | Region code within that cloud.                                                                                                                         | (none)        |

Everything else is per provider. The server adds each configuration directory below to the
classpath automatically, which is also where provider-specific files such as `hdfs-site.xml` go.

| Catalog Provider    | Catalog Properties                                                                      | Configuration File Path                                  |
|---------------------|-----------------------------------------------------------------------------------------|----------------------------------------------------------|
| `hive`              | [Hive catalog properties](apache-hive-catalog.md#catalog-properties)                    | `catalogs/hive/conf/hive.conf`                           |
| `glue`              | [AWS Glue catalog properties](aws-glue-catalog.md#catalog-properties)                   | `catalogs/glue/conf/glue.conf`                           |
| `lakehouse-iceberg` | [Lakehouse Iceberg catalog properties](lakehouse-iceberg-catalog.md#catalog-properties) | `catalogs/lakehouse-iceberg/conf/lakehouse-iceberg.conf` |
| `lakehouse-paimon`  | [Lakehouse Paimon catalog properties](lakehouse-paimon-catalog.md#catalog-properties)   | `catalogs/lakehouse-paimon/conf/lakehouse-paimon.conf`   |
| `lakehouse-hudi`    | [Lakehouse Hudi catalog properties](lakehouse-hudi-catalog.md#catalog-properties)       | `catalogs/lakehouse-hudi/conf/lakehouse-hudi.conf`       |
| `lakehouse-generic` | [Lakehouse Generic catalog properties](lakehouse-generic-catalog.md#catalog-properties) | `catalogs/lakehouse-generic/conf/lakehouse-generic.conf` |
| `jdbc-mysql`        | [MySQL catalog properties](jdbc-mysql-catalog.md#catalog-properties)                    | `catalogs/jdbc-mysql/conf/jdbc-mysql.conf`               |
| `jdbc-postgresql`   | [PostgreSQL catalog properties](jdbc-postgresql-catalog.md#catalog-properties)          | `catalogs/jdbc-postgresql/conf/jdbc-postgresql.conf`     |
| `jdbc-doris`        | [Doris catalog properties](jdbc-doris-catalog.md#catalog-properties)                    | `catalogs/jdbc-doris/conf/jdbc-doris.conf`               |
| `jdbc-starrocks`    | [StarRocks catalog properties](jdbc-starrocks-catalog.md#catalog-properties)            | `catalogs/jdbc-starrocks/conf/jdbc-starrocks.conf`       |
| `jdbc-clickhouse` ‡ | [ClickHouse catalog properties](jdbc-clickhouse-catalog.md#catalog-properties)          | `catalogs/jdbc-clickhouse/conf/jdbc-clickhouse.conf`     |
| `jdbc-hologres` ‡   | [Hologres catalog properties](jdbc-hologres-catalog.md#catalog-properties)              | `catalogs/jdbc-hologres/conf/jdbc-hologres.conf`         |
| `jdbc-oceanbase` ‡  | [OceanBase catalog properties](jdbc-oceanbase-catalog.md#catalog-properties)            | `catalogs/jdbc-oceanbase/conf/jdbc-oceanbase.conf`       |
| `kafka`             | [Kafka catalog properties](kafka-catalog.md#catalog-properties)                         | `catalogs/kafka/conf/kafka.conf`                         |
| `fileset`           | [Fileset catalog properties](fileset-catalog.md#catalog-properties)                     | `catalogs/fileset/conf/fileset.conf`                     |
| `model`             | [Model catalog properties](model-catalog.md#catalog-properties)                         | `catalogs/model/conf/model.conf`                         |

‡ Contributed catalogs, shipped only in the `-all` distribution package. The standard package
does not contain their directories.

## Container Configuration

```shell
docker run --rm -d -p 8090:8090 apache/gravitino:{tag}
```

### How the Container Builds Its Configuration

The container entrypoint rewrites `conf/gravitino.conf` before the JVM starts. It works in two
passes. First it writes its own defaults, unconditionally, over every property it knows a default
for, discarding whatever the file said. Then it applies each supported environment variable that is
set. The result is written back over the original file.

Two consequences worth internalizing. A property you baked into `conf/gravitino.conf` survives only
if the container has no default for it, so a value like a custom `httpPort` in a mounted file is
silently replaced. And the container's defaults are not the server's defaults: the container pins
`minThreads` to 24 and `maxThreads` to 200, where a server started outside a container computes both
from the processor count.

Set `SKIP_CONFIG_REWRITE=true` to disable both passes and run the configuration file exactly as
written. Use this when the file comes from a Kubernetes ConfigMap.

### Supported Environment Variables

The entrypoint recognizes the variables below and ignores every other `GRAVITINO_` variable. The
Container Default column gives the value the first pass writes when the variable is unset; `(none)`
means the property is left alone.

| Environment Variable                                     | Configuration Key                                    | Container Default                                    |
|----------------------------------------------------------|------------------------------------------------------|------------------------------------------------------|
| `GRAVITINO_SERVER_SHUTDOWN_TIMEOUT`                      | `gravitino.server.shutdown.timeout`                  | `3000`                                               |
| `GRAVITINO_SERVER_WEBSERVER_HOST`                        | `gravitino.server.webserver.host`                    | `0.0.0.0`                                            |
| `GRAVITINO_SERVER_WEBSERVER_HTTP_PORT`                   | `gravitino.server.webserver.httpPort`                | `8090`                                               |
| `GRAVITINO_SERVER_WEBSERVER_MIN_THREADS`                 | `gravitino.server.webserver.minThreads`              | `24`                                                 |
| `GRAVITINO_SERVER_WEBSERVER_MAX_THREADS`                 | `gravitino.server.webserver.maxThreads`              | `200`                                                |
| `GRAVITINO_SERVER_WEBSERVER_STOP_TIMEOUT`                | `gravitino.server.webserver.stopTimeout`             | `30000`                                              |
| `GRAVITINO_SERVER_WEBSERVER_IDLE_TIMEOUT`                | `gravitino.server.webserver.idleTimeout`             | `30000`                                              |
| `GRAVITINO_SERVER_WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE` | `gravitino.server.webserver.threadPoolWorkQueueSize` | `100`                                                |
| `GRAVITINO_SERVER_WEBSERVER_REQUEST_HEADER_SIZE`         | `gravitino.server.webserver.requestHeaderSize`       | `131072`                                             |
| `GRAVITINO_SERVER_WEBSERVER_RESPONSE_HEADER_SIZE`        | `gravitino.server.webserver.responseHeaderSize`      | `131072`                                             |
| `GRAVITINO_ENTITY_STORE`                                 | `gravitino.entity.store`                             | `relational`                                         |
| `GRAVITINO_ENTITY_STORE_RELATIONAL`                      | `gravitino.entity.store.relational`                  | `JDBCBackend`                                        |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL`             | `gravitino.entity.store.relational.jdbcUrl`          | `jdbc:h2`                                            |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER`          | `gravitino.entity.store.relational.jdbcDriver`       | `org.h2.Driver`                                      |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_USER`            | `gravitino.entity.store.relational.jdbcUser`         | `gravitino`                                          |
| `GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_PASSWORD`        | `gravitino.entity.store.relational.jdbcPassword`     | `gravitino`                                          |
| `GRAVITINO_CATALOG_CACHE_EVICTION_INTERVAL_MS`           | `gravitino.catalog.cache.evictionIntervalMs`         | `3600000`                                            |
| `GRAVITINO_AUTHORIZATION_ENABLE`                         | `gravitino.authorization.enable`                     | `false`                                              |
| `GRAVITINO_AUTHORIZATION_SERVICE_ADMINS`                 | `gravitino.authorization.serviceAdmins`              | `anonymous`                                          |
| `GRAVITINO_AUX_SERVICE_NAMES`                            | `gravitino.auxService.names`                         | `iceberg-rest`                                       |
| `GRAVITINO_ICEBERG_REST_HOST`                            | `gravitino.iceberg-rest.host`                        | `0.0.0.0`                                            |
| `GRAVITINO_ICEBERG_REST_HTTP_PORT`                       | `gravitino.iceberg-rest.httpPort`                    | `9001`                                               |
| `GRAVITINO_ICEBERG_REST_URI`                             | `gravitino.iceberg-rest.uri`                         | (none)                                               |
| `GRAVITINO_ICEBERG_REST_CLASSPATH`                       | `gravitino.iceberg-rest.classpath`                   | `iceberg-rest-server/libs, iceberg-rest-server/conf` |
| `GRAVITINO_ICEBERG_REST_IO_IMPL`                         | `gravitino.iceberg-rest.io-impl`                     | (none)                                               |
| `GRAVITINO_ICEBERG_REST_CATALOG_BACKEND`                 | `gravitino.iceberg-rest.catalog-backend`             | `memory`                                             |
| `GRAVITINO_ICEBERG_REST_JDBC_DRIVER`                     | `gravitino.iceberg-rest.jdbc-driver`                 | (none)                                               |
| `GRAVITINO_ICEBERG_REST_JDBC_USER`                       | `gravitino.iceberg-rest.jdbc-user`                   | (none)                                               |
| `GRAVITINO_ICEBERG_REST_JDBC_PASSWORD`                   | `gravitino.iceberg-rest.jdbc-password`               | (none)                                               |
| `GRAVITINO_ICEBERG_REST_WAREHOUSE`                       | `gravitino.iceberg-rest.warehouse`                   | `/tmp/`                                              |
| `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS`            | `gravitino.iceberg-rest.credential-providers`        | (none)                                               |
| `GRAVITINO_ICEBERG_REST_GCS_SERVICE_ACCOUNT_FILE`        | `gravitino.iceberg-rest.gcs-service-account-file`    | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_ACCESS_KEY`                   | `gravitino.iceberg-rest.s3-access-key-id`            | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_SECRET_KEY`                   | `gravitino.iceberg-rest.s3-secret-access-key`        | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_ENDPOINT`                     | `gravitino.iceberg-rest.s3-endpoint`                 | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_REGION`                       | `gravitino.iceberg-rest.s3-region`                   | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_PATH_STYLE_ACCESS`            | `gravitino.iceberg-rest.s3-path-style-access`        | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_ROLE_ARN`                     | `gravitino.iceberg-rest.s3-role-arn`                 | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_EXTERNAL_ID`                  | `gravitino.iceberg-rest.s3-external-id`              | (none)                                               |
| `GRAVITINO_ICEBERG_REST_S3_TOKEN_SERVICE_ENDPOINT`       | `gravitino.iceberg-rest.s3-token-service-endpoint`   | (none)                                               |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_NAME`      | `gravitino.iceberg-rest.azure-storage-account-name`  | (none)                                               |
| `GRAVITINO_ICEBERG_REST_AZURE_STORAGE_ACCOUNT_KEY`       | `gravitino.iceberg-rest.azure-storage-account-key`   | (none)                                               |
| `GRAVITINO_ICEBERG_REST_AZURE_TENANT_ID`                 | `gravitino.iceberg-rest.azure-tenant-id`             | (none)                                               |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_ID`                 | `gravitino.iceberg-rest.azure-client-id`             | (none)                                               |
| `GRAVITINO_ICEBERG_REST_AZURE_CLIENT_SECRET`             | `gravitino.iceberg-rest.azure-client-secret`         | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_ACCESS_KEY`                  | `gravitino.iceberg-rest.oss-access-key-id`           | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_SECRET_KEY`                  | `gravitino.iceberg-rest.oss-secret-access-key`       | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_ENDPOINT`                    | `gravitino.iceberg-rest.oss-endpoint`                | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_REGION`                      | `gravitino.iceberg-rest.oss-region`                  | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_ROLE_ARN`                    | `gravitino.iceberg-rest.oss-role-arn`                | (none)                                               |
| `GRAVITINO_ICEBERG_REST_OSS_EXTERNAL_ID`                 | `gravitino.iceberg-rest.oss-external-id`             | (none)                                               |

The image bundles MySQL and PostgreSQL JDBC drivers in `jdbc-drivers/` and links them into `libs/`
and `iceberg-rest-server/libs/` at startup. For cloud storage backends, put the matching Iceberg
bundle jars in `iceberg-bundles/` and they are linked into
`catalogs/lakehouse-iceberg/libs/` and `iceberg-rest-server/libs/` the same way.

### Checking What the Container Did

Read back the rewritten file:

```shell
docker exec -it {container_id} cat /opt/gravitino/conf/gravitino.conf
```

Then confirm the server, and the auxiliary IRC service if you started one:

```shell
curl http://127.0.0.1:8090/health/ready
curl http://127.0.0.1:9001/iceberg/v1/config
```

## Accessing Apache Hadoop

Gravitino reaches Hadoop as a single operating system user, so that user needs the HDFS and YARN
permissions for everything the server will touch. Without them, operations fail with
`Permission denied`. Either grant the user that starts the server the permissions it needs, or set
`HADOOP_USER_NAME` to a user that already has them before starting. For a local deployment, set it
in `conf/gravitino-env.sh`.

## Related

- [Relational Backend Storage](how-to-use-relational-backend-storage.md), for pointing the entity
  store at MySQL or PostgreSQL, including schema initialization and driver installation
- [Iceberg REST Catalog Service](iceberg-rest-service.md), for the `gravitino.iceberg-rest.*`
  properties of the auxiliary service named by `gravitino.auxService.names`
- [How to Authenticate](security/how-to-authenticate.md), for the `gravitino.authenticator.*`
  properties behind each value of `gravitino.authenticators`
- [Local users and groups](security/local-users-and-groups.md), for holding users, password
  hashes, and group membership in Gravitino's own relational store
- [Access Control](security/access-control.md), for the privilege model the authorizer enforces once
  `gravitino.authorization.enable` is set: roles, grants, ownership, and metalake administration
- [Authorization Pushdown](security/authorization-pushdown.md), for propagating those privileges
  into the underlying systems through Apache Ranger or a native permission model
- [Credential Vending](security/credential-vending.md), for issuing temporary storage credentials to
  engines instead of distributing long-lived keys
- [HTTPS](security/how-to-use-https.md), for the `gravitino.server.webserver.*` key store, trust
  store, and client certificate properties
- [CORS](security/how-to-use-cors.md), for letting browser clients served from another origin call
  the API
- [Security](security/how-to-authenticate.md)
