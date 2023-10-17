---
title: "How to customize Gravitino server configurations"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---


# How to customize Gravitino server configurations

Gravitino server can be customized by editing the configuration file `gravitino.conf` in the `conf` directory, though default values are usually sufficient for most use cases and users need not modify them.
But we strongly recommend that you should read the following sections to understand the configuration file in case that you may need to change it in the future.


## Configuration file

The file located in `conf/gravitino.conf` is the main configuration file for Gravitino server.


## Configuration items

The following table lists the configuration items in the `gravitino.conf` file. Those items are grouped into the following categories:

### Server configuration

| Configuration item                | Description                                                                                                                 | Default value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.server.webserver.host` | The host of Gravitino server.                                                                                               | `127.0.0.1`   |
| `gravitino.server.webserver.port` | The port on which the Gravitino server listens for incoming connections.                                                    | `8080`        |
| `gravitino.server.webserver.coreThreads`     | The number of core thread in thread pool which is used by Jetty webserver.                                                  | `24`          |
| `gravitino.server.webserver.maxThreads`           | The number of max thread in thread pool which is used by Jetty webserver.                                                   | `200`         |
| `gravitino.server.webserver.threadPoolWorkQueueSize`           | The size of queue in thread pool which is used by Jetty webserver.                                                          | `100`        |
| `gravitino.server.webserver.stopIdleTimeout`           | Time of graceful stop for Jetty webserver, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`, unit: ms. | `30000`       |
| `gravitino.server.webserver.requestHeaderSize`           | Max size of Http request.                                                                                                   | `131072`        |
| `gravitino.server.webserver.responseHeaderSize`           | Max size of Http response.                                                                                                  | `131072`        |
| `gravitino.server.shutdown.timeout`           | Time of graceful stop for Gravitino webserver. Unit: ms.                                                                    | `3000`        |



### Storage configuration

| Configuration item | Description                                                                                     | Default value |
| ------------------ |-------------------------------------------------------------------------------------------------| ------------- |
| `gravitino.entity.store` | which storage implementation to use, currently we only support `kv`.                            | `kv` |
| `gravitino.entity.store.kv` | Detailed implementation of kv storage, currently we only support `RocksDBKvBackend`.            | `RocksDBKvBackend` |
| `gravitino.entity.store.kv.rocksdbPath` | Directory path of `RocksDBKvBackend`, **we highly recommend you to change this default value**. | `/tmp/gravitino` |



### Catalog configuration

| Configuration item                           | Description                                                                    | Default value |
|----------------------------------------------|--------------------------------------------------------------------------------|--------------|
| `gravitino.catalog.cache.evictionIntervalMs` | The interval in milliseconds to evict the catalog cache, default 3600000ms(1h) | `3600000`      |
|  `gravitino.catalog.classloader.isolated`    | Whether to use isolated classloader for catalog, default `true`                | `true`        |

### Other configuration
Note: If you do not understand the following configuration items, you can ignore them and just use the default value. But if you want to modify them, you should refer to the following table and modify them according to your needs.


| Configuration item                            | Description                                                                    | Default value                                                       |
|-----------------------------------------------|--------------------------------------------------------------------------------|---------------------------------------------------------------------|
| `gravitino.auxService.names`                  | Gravitino auxiliary service names, separated by ',' like "iceberg-rest, server2" | iceberg-rest                                                        |
| `gravitino.auxService.iceberg-rest.classpath` | Class path of service `iceberg-rest`, separated by ','                         | `catalogs/lakehouse-iceberg/libs, catalogs/lakehouse-iceberg/conf`  |
| `gravitino.auxService.iceberg-rest.host`      | The host of service `iceberg-rest`                                                           | `127.0.0.1`                                                         |
| `gravitino.auxService.iceberg-rest.httpPort`  | The http port of service `iceberg-rest`     | `9001`                                                              |
