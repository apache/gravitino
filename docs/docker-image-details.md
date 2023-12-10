---
title: "Docker image details"
slug: /docker-image-details
keyword: docker
license: Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

# User Docker images
There are 2 kinds of docker images for user Docker images: the Gravitino Docker image and playground Docker images.
## Gravitino Docker image
You can deploy the service with Gravitino Docker image.

Container startup commands
```shell
docker run --rm -d -p 8090:8090 datastrato/gravitino
```
Changelog

gravitino:0.3.0
- Docker image `datastrato/gravitino:0.3.0`
- Gravitino Server
- Expose ports:
  - `8090` Gravitino Web UI

## Playground Docker image
You can use the [playground](https://github.com/datastrato/gravitino-playground) to experience the whole Gravitino system with other components. 
The playground consists of multiple Docker images.
The Docker images of playground have suitable configurations for users to experience.

### Hive image
Changelog
- hive:2.7.3-no-yarn
  - Docker image `datastrato/hive:2.7.3-no-yarn`
  - `hadoop-2.7.3`
  - `hive-2.3.9`
  - Not start YARN when container startup

### Trino image
Changelog
- trino:426-gravitino-0.3.0-SNAPSHOT
  - Docker image `datastrato/trino:426-gravitino-0.3.0-SNAPSHOT`
  - Base on `trino:462`
  - Added Gravitino trino-connector-0.3.0-SNAPSHOT libraries into the `/usr/lib/trino/plugin/gravitino`

# Developer Docker images
You can use this kind of the Docker images to facilitate Gravitino integration testing.
You can use it to test all catalog and connector modules within Gravitino.
## Gravitino CI Apache Hive image
You can use this kind of images to test the catalog of Apache Hive.

Container startup commands
```shell
docker run --rm -d -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50010:50010 -p 50070:50070 -p 50075:50075 datastrato/gravitino-ci-hive
```
Changelog
- gravitino-ci-hive:0.1.6
  - No start YARN when container startup
  - Removed expose ports:
    - `22` SSH
    - `8088` YARN Service
- gravitino-ci-hive:0.1.5
  - Rollback `Map container hostname to 127.0.0.1 before starting Hadoop` of `datastrato/gravitino-ci-hive:0.1.4`
- gravitino-ci-hive:0.1.4
  - Config HDFS DataNode data transfer address to `0.0.0.0:50010` explicitly
  - Map container hostname to `127.0.0.1` before starting Hadoop
  - Expose `50010` port for the HDFS DataNode
- gravitino-ci-hive:0.1.3
  - change Mysql bind-address from `127.0.0.1` to `0.0.0.0`
  - add `iceberg` to Mysql users with password `iceberg`
  - export `3306` port for Mysql
- gravitino-ci-hive:0.1.2
  - base on `datastrato/gravitino-ci-hive:0.1.1`
  - Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
  - Expose `9000` port int the `Dockerfile` file.
- gravitino-ci-hive:0.1.1
  - base on `datastrato/gravitino-ci-hive:0.1.0`
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
    - `9083` Hive Metastore
    - `10000` HiveServer2
    - `10002` HiveServer2 HTTP
## Gravitino CI Trino image
You can use this kind of images to test Trino.

Container startup commands
```shell
docker run --rm -it -p 8080:8080 datastrato/gravitino-ci-trino
```
Changelog
- gravitino-ci-trino:0.1.2
  - Copy JDBC driver 'mysql-connector-java' and 'postgres' to `/usr/lib/trino/iceberg/` folder
- gravitino-ci-trino:0.1.0
  - Docker image `datastrato/gravitino-ci-trino:0.1.0`
  - Base on `trinodb/trino:426` and removed some unused plugins from it.
  - Expose ports:
    - `8080` Trino JDBC port
