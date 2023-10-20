---
title: "How to customize Gravitino server configurations"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## How to configure Gravitino server configurations

Gravitino server can be customized by editing the configuration file `gravitino.conf` in the `conf` directory, though default values of some configurations are usually sufficient for most use cases and users need not modify them.
But we strongly recommend that you read the following sections to understand the configuration file and change the default values based on your specific situation and usage scenarios.

## Configuration file

The file located in `conf/gravitino.conf` is the main configuration file for the Gravitino server.

## Configuration items

The following table lists the configuration items in the `gravitino.conf` file. Those items are grouped into the following categories:

### Server configuration

| Configuration item                                   | Description                                                                                                                                           | Default value | Since version |
|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.server.webserver.host`                    | The host of Gravitino server.                                                                                                                         | `127.0.0.1`   | 0.1.0         |
| `gravitino.server.webserver.port`                    | The port on which the Gravitino server listens for incoming connections.                                                                              | `8090`        | 0.1.0         |
| `gravitino.server.webserver.coreThreads`             | The number of core threads in the thread pool which is used by Jetty webserver.                                                                            | `24`          | 0.1.0         |
| `gravitino.server.webserver.maxThreads`              | The number of max threads in the thread pool which is used by Jetty webserver.                                                                             | `200`         | 0.1.0         |
| `gravitino.server.webserver.threadPoolWorkQueueSize` | The size of queue in the thread pool which is used by Jetty webserver.                                                                                    | `100`         | 0.1.0         |
| `gravitino.server.webserver.stopIdleTimeout`         | Time of graceful shutdown of the Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`, unit: ms.                           | `30000`       | 0.1.0         |
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
