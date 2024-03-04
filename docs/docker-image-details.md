---
title: "Docker image details"
slug: /docker-image-details
keyword: docker
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

# User Docker images

There are two kinds of Docker images you can use: the Gravitino Docker image and playground Docker images.

## Gravitino Docker image

You can deploy the service with the Gravitino Docker image.

Container startup commands

```shell
docker run --rm -d -p 8090:8090 -p 9001:9001 datastrato/gravitino
```

Changelog

- gravitino:0.4.0
  - Based on Gravitino 0.4.0, you can know more information from 0.4.0 release notes.


- gravitino:0.3.1
  - Fix some issues


- gravitino:0.3.0
  - Docker image `datastrato/gravitino:0.3.0`
  - Gravitino Server
  - Expose ports:
    - `8090` Gravitino Web UI
    - `9001` Iceberg REST service

## Playground Docker image

You can use the [playground](https://github.com/datastrato/gravitino-playground) to experience the whole Gravitino system with other components.

The playground consists of multiple Docker images.

The Docker images of the playground have suitable configurations for users to experience.

### Hive image

Changelog

- hive:2.7.3-no-yarn
  - Docker image `datastrato/hive:2.7.3-no-yarn`
  - `hadoop-2.7.3`
  - `hive-2.3.9`
  - Don't start YARN when container startup

### Trino image

Changelog

- trino:426-gravitino-0.4.0
  - Based on Gravitino 0.4.0, you can know more information from 0.4.0 release notes.


- trino:426-gravitino-0.3.1
  - Fix some issues


- trino:426-gravitino-0.3.0
  - Docker image `datastrato/trino:426-gravitino-0.3.0`
  - Base on `trino:462`
  - Added Gravitino trino-connector-0.3.0 libraries into the `/usr/lib/trino/plugin/gravitino`

# Developer Docker images

You can use these kinds of Docker images to facilitate integration testing of all catalog and connector modules within Gravitino.

## Gravitino CI Apache Hive image

You can use this kind of image to test the catalog of Apache Hive.

Changelog

- gravitino-ci-hive:0.1.8
  - Change the value of `hive.server2.enable.doAs` to `true`

- gravitino-ci-hive:0.1.7
  - Download MySQL JDBC driver before building the Docker image
  - Set `hdfs` as HDFS superuser group

- gravitino-ci-hive:0.1.6
  - No starting YARN when container startup
  - Removed expose ports:
    - `22` SSH
    - `8088` YARN Service

- gravitino-ci-hive:0.1.5
  - Rollback `Map container hostname to 127.0.0.1 before starting Hadoop` of `datastrato/gravitino-ci-hive:0.1.4`

- gravitino-ci-hive:0.1.4
  - Configure HDFS DataNode data transfer address to be `0.0.0.0:50010`
  - Map the container hostname to `127.0.0.1` before starting Hadoop
  - Expose `50010` port for the HDFS DataNode

- gravitino-ci-hive:0.1.3
  - Change MySQL bind-address from `127.0.0.1` to `0.0.0.0`
  - Add `iceberg` to MySQL users with password `iceberg`
  - Export `3306` port for MySQL

- gravitino-ci-hive:0.1.2
  - Based on `datastrato/gravitino-ci-hive:0.1.1`
  - Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
  - Expose `9000` port in the `Dockerfile` file.

- gravitino-ci-hive:0.1.1
  - Based on `datastrato/gravitino-ci-hive:0.1.0`
  - Modify HDFS/YARN/HIVE `MaxPermSize` from `8GB` to `128MB`
  - Modify `HADOOP_HEAPSIZE` from `8192` to `128`

- gravitino-ci-hive:0.1.0
  - Docker image `datastrato/gravitino-ci-hive:0.1.0`
  - `hadoop-2.7.3`
  - `hive-2.3.9`
  - Expose ports:
    - `22` SSH
    - `9000` HDFS defaultFS
    - `50070` HDFS NameNode
    - `50075` HDFS DataNode HTTP server
    - `50010` HDFS DataNode data transfer
    - `8088` YARN Service
    - `9083` Hive metastore
    - `10000` HiveServer2
    - `10002` HiveServer2 HTTP

## Gravitino CI Trino image

You can use this image to test Trino.

Changelog

- gravitino-ci-trino:0.1.5
  - Add check for the version of gravitino-trino-connector

- gravitino-ci-trino:0.1.4
  - Change `-Xmx1G` to `-Xmx2G` in the config file `/etc/trino/jvm.config`

- gravitino-ci-trino:0.1.3
  - Remove copy content in folder `gravitino-trino-connector` to plugin folder `/usr/lib/trino/plugin/gravitino`

- gravitino-ci-trino:0.1.2
  - Copy JDBC driver 'mysql-connector-java' and 'postgres' to `/usr/lib/trino/iceberg/` folder

- gravitino-ci-trino:0.1.0
  - Docker image `datastrato/gravitino-ci-trino:0.1.0`
  - Based on `trinodb/trino:426` and removed some unused plugins from it.
  - Expose ports:
    - `8080` Trino JDBC port

## Gravitino CI Doris image

You can use this image to test Apache Doris.

Changelog

- gravitino-ci-doris:0.1.0
    - Docker image `datastrato/gravitino-ci-doris:0.1.0`
    - Start Doris BE & FE in one container
    - Please set table properties `"replication_num" = "1"` when creating a table in Doris, because the default replication number is 3, but the Doris container only has one BE.
    - Username: `root`, Password: N/A (password is empty)
    - Expose ports:
        - `8030` Doris FE HTTP port
        - `9030` Doris FE MySQL server port
