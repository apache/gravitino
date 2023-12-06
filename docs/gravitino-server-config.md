---
title: "How to customize Gravitino server configurations"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## How to configure Gravitino server configurations

You can customize the Gravitino server by editing the configuration file `gravitino.conf` in the `conf` directory. The default values are sufficient for most use cases and don't need modification.
It's strongly recommended to read the following sections to understand the configuration file and change the default values to suit your specific situation and usage scenario.

## Configuration file

The file located in `conf/gravitino.conf` is the main configuration file for the Gravitino server.

## Configuration items

The `gravitino.conf` file lists the configuration items in the following table. It groups those items into the following categories:

### Server configuration

| Configuration item                                   | Description                                                                                                                                           | Default value | Since version |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.server.webserver.host`                    | The host of Gravitino server.                                                                                                                         | `0.0.0.0`   | 0.1.0         |
| `gravitino.server.webserver.httpPort`                | The port on which the Gravitino server listens for incoming connections.                                                                              | `8090`        | 0.1.0         |
| `gravitino.server.webserver.minThreads`              | The minimum number of threads in the thread pool used by Jetty webserver. `minThreads` will be adjusted to 8 if the value is less than 8.         | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)`          | 0.2.0         |
| `gravitino.server.webserver.maxThreads`              | The maximum number of threads in the thread pool used by Jetty webserver. `maxThreads` will be adjusted to 8 if the value is less than 8, and `maxThreads` must be great or equal to `minThreads`  | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`         | 0.1.0         |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of the queue in the thread pool used by Jetty webserver.                                                                                    | `100`         | 0.1.0         |
| `gravitino.server.webserver.stopTimeout`             | Time in milliseconds to gracefully shutdown the Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`.                      | `30000`       | 0.2.0         |
| `gravitino.server.webserver.idleTimeout`             | The timeout in milliseconds of idle connections. ms.                                                                                                            | `30000`       | 0.2.0         |
| `gravitino.server.webserver.requestHeaderSize`       | Maximum size of HTTP requests.                                                                                                                             | `131072`      | 0.1.0         |
| `gravitino.server.webserver.responseHeaderSize`      | Maximum size of HTTP responses.                                                                                                                            | `131072`      | 0.1.0         |
| `gravitino.server.shutdown.timeout`                  | Time in milliseconds to gracefully shutdown of the Gravitino webserver.                                                                                              | `3000`        | 0.2.0         |

### Storage configuration

| Configuration item                                | Description                                                                                                                                                                    | Default value                    | Since version |
|---------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------|---------------|
| `gravitino.entity.store`                          | Which storage implementation to use, currently supported is key-value pair storage, the default value is `kv`.                                                                  | `kv`                             | 0.1.0         |
| `gravitino.entity.store.kv`                       | Detailed implementation of Kv storage, currently supported is `RocksDB` storage implementation `RocksDBKvBackend`.                                                          | `RocksDBKvBackend`               | 0.1.0         |
| `gravitino.entity.store.kv.rocksdbPath`           | Directory path of `RocksDBKvBackend`, **It's highly recommend that you change this default value** as it's under the deploy directory and future version upgrades may remove it. | `${GRAVITINO_HOME}/data/rocksdb` | 0.1.0         |
| `gravitino.entity.store.maxTransactionSkewTimeMs` | The maximum skew time of transactions in milliseconds.                                                                                                                         | `2000`                           | 0.3.0         |
| `gravitino.entity.store.kv.deleteAfterTimeMs`     | The maximum time in milliseconds that the deleted data and old version data is kept. Set to at least 10 minutes and no longer than 30 days.                                 | `604800000`(7 days)              | 0.3.0         |

### Catalog configuration

| Configuration item                            | Description                                                                                                                                                                                             | Default value | Since version |
|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.catalog.cache.evictionIntervalMs`  | The interval in milliseconds to evict the catalog cache, default 3600000ms(1h)                                                                                                                          | `3600000`     | 0.1.0         |
| `gravitino.catalog.classloader.isolated`      | Whether to use an isolated classloader for catalog, if true, an isolated classloader loads all catalog-related libraries and configurations, not the AppClassLoader. The default value is `true`. | `true`        | 0.1.0         |

## How to set up runtime environment variables

Gravitino server also supports setting up runtime environment variables by editing the `gravitino-env.sh` file, located in the `conf` directory.

### How to access Apache Hadoop

Currently, due to the absence of a comprehensive user permission system, Gravitino can only use a single username for
Apache Hadoop access. Please ensure that the user starting the Gravitino server has Hadoop (HDFS, YARN, etc.) access
permissions; otherwise, you may encounter a `Permission denied` error. There are also several ways to resolve this error:

* Granting the Gravitino startup user permissions in Hadoop
* Specify the authorized Hadoop username in the environment variables `HADOOP_USER_NAME` before starting the Gravitino server.
