---
title: "How to customize Gravitino server configurations"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## How to configure Gravitino server configurations

Gravitino server can be customized by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases and don't need modification.
But we strongly recommend reading the following sections to understand the configuration file and change the default values to suit your specific situation and usage scenario.

## Configuration file

The file located in `conf/gravitino.conf` is the main configuration file for the Gravitino server.

## Configuration items

The following table lists the configuration items in the `gravitino.conf` file. Those items are grouped into the following categories:

### Server configuration

| Configuration item                                   | Description                                                                                                                                           | Default value | Since version |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.server.webserver.host`                    | The host of Gravitino server.                                                                                                                         | `0.0.0.0`   | 0.1.0         |
| `gravitino.server.webserver.httpPort`                | The port on which the Gravitino server listens for incoming connections.                                                                              | `8090`        | 0.1.0         |
| `gravitino.server.webserver.minThreads`              | The number of min threads in the thread pool which is used by Jetty webserver. `minThreads` will be adjusted to 8 if the value is less than 8.         | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)`          | 0.2.0         |
| `gravitino.server.webserver.maxThreads`              | The number of max threads in the thread pool which is used by Jetty webserver. `maxThreads` will be adjusted to 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`  | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`         | 0.1.0         |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of queue in the thread pool which is used by Jetty webserver.                                                                                    | `100`         | 0.1.0         |
| `gravitino.server.webserver.stopTimeout`             | Time of graceful shutdown of the Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`, unit: ms.                           | `30000`       | 0.2.0         |
| `gravitino.server.webserver.idleTimeout`             | The timeout of idle connections, unit: ms.                                                                                                            | `30000`       | 0.2.0         |
| `gravitino.server.webserver.requestHeaderSize`       | Max size of HTTP request.                                                                                                                             | `131072`      | 0.1.0         |
| `gravitino.server.webserver.responseHeaderSize`      | Max size of HTTP response.                                                                                                                            | `131072`      | 0.1.0         |
| `gravitino.server.shutdown.timeout`                  | Time of graceful shutdown of the Gravitino webserver. Unit: ms.                                                                                              | `3000`        | 0.2.0         |

### Storage configuration

| Configuration item                      | Description                                                                                                                                                                    | Default value                    | Since version |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------|---------------|
| `gravitino.entity.store`                | Which storage implementation to use, currently we only support key-value pair storage, default value is `kv`.                                                                  | `kv`                             | 0.1.0         |
| `gravitino.entity.store.kv`             | Detailed implementation of kv storage, currently we only support `RocksDB` storage implementation `RocksDBKvBackend`.                                                          | `RocksDBKvBackend`               | 0.1.0         |
| `gravitino.entity.store.kv.rocksdbPath` | Directory path of `RocksDBKvBackend`, **we highly recommend you to change this default value** as it's under the deploy directory and version update operations may remove it. | `${GRAVITINO_HOME}/data/rocksdb` | 0.1.0         |

### Catalog configuration

| Configuration item                            | Description                                                                                                                                                                                             | Default value | Since version |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs`  | The interval in milliseconds to evict the catalog cache, default 3600000ms(1h)                                                                                                                          | `3600000`     | 0.1.0         |
| `gravitino.catalog.classloader.isolated`      | Whether to use an isolated classloader for catalog, if it's true, all catalog-related libraries and configurations will be loaded by an isolated classloader NOT by AppClassLoader. Default value is `true` | `true`        | 0.1.0         |

## Authentication

| Configuration item                                  | Description                                                                | Default value     | Since version |
|-----------------------------------------------------|----------------------------------------------------------------------------|-------------------|---------------|
| `gravitino.authenticator`                           | The authenticator which Gravitino uses, setting as `simple` or `oauth`     | `simple`          | 0.3.0         |
| `gravitino.authenicator.oauth.service.audience`     | The audience name when Gravitino uses oauth as the authenticator           | `GravitinoServer` | 0.3.0         |
| `gravitino.authenticator.oauth.allow.skew.seconds`  | The jwt allows skew seconds when Gravitino uses oauth as the authenticator | `0`               | 0.3.0         |
| `gravitino.authenticator.oauth.default.sign.key`    | The sign key of jwt when Gravitino uses oauth as the authenticator         | `null`            | 0.3.0         |
| `gravitino.authenticator.oauth.sign.algorithm.type` | The signature algorithm when Gravitino uses oauth as the authenticator     | `RS256`           | 0.3.0         |
## How to set up runtime environment variables

Gravitino server also supports setting up runtime environment variables by editing the `gravitino-env.sh` file, which is located in the `conf` directory.

### How to access Hadoop

Currently, due to the absence of a comprehensive user permission system, Gravitino can only use a single username for
Hadoop access. Please ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN, etc.) access
permissions; otherwise, you may encounter a `Permission denied` error. There are also several ways to resolve this error:
* Granting the Gravitino startup user permissions in Hadoop
* Specify the authorized Hadoop username in the environment variables `HADOOP_USER_NAME` before starting the Gravitino server.
