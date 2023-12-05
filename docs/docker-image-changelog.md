<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# Docker image change log

## Gravitino

### gravitino:0.3.0-SNAPSHOT
- Docker image `datastrato/gravitino:0.3.0-SNAPSHOT`
- Gravitino Server
- Expose ports:
  - `8090` Gravitino Web UI

## Gravitino development image with Apache Hive

### gravitino-ci-hive:0.1.0
- Docker image `datastrato/gravitino-ci-hive:0.1.0`
- `hadoop-2.7.3`
- `hive-2.3.9`
- Expose ports:
  - `22` SSH
  - `9000` HDFS defaultFS
  - `50070` HDFS NameNode
  - `50075` HDFS DataNode http server
  - `50010` HDFS DataNode data transfer
  - `8088` YARN Service
  - `9083` Hive Metastore
  - `10000` HiveServer2
  - `10002` HiveServer2 HTTP

### gravitino-ci-hive:0.1.1
- base on `datastrato/gravitino-ci-hive:0.1.0`
- Modify HDFS/YARN/HIVE `MaxPermSize` from `8GB` to `128MB`
- Modify `HADOOP_HEAPSIZE` from `8192` to `128

### gravitino-ci-hive:0.1.2
- base on `datastrato/gravitino-ci-hive:0.1.1` 
- Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
- Expose `9000` port int the `Dockerfile` file.

### gravitino-ci-hive:0.1.3
- change mysql bind-address from `127.0.0.1` to `0.0.0.0` 
- add `iceberg` to mysql users with password `iceberg`
- export `3306` port for mysql

### gravitino-ci-hive:0.1.4
- Config HDFS DataNode data transfer address to `0.0.0.0:50010` explicitly
- Map container hostname to `127.0.0.1` before starting Hadoop
- Expose `50010` port for the HDFS DataNode

### gravitino-ci-hive:0.1.5
- Rollback `Map container hostname to 127.0.0.1 before starting Hadoop` of `datastrato/gravitino-ci-hive:0.1.4`

### gravitino-ci-hive:0.1.6
- No start YARN when container startup
- Removed expose ports:
  - `22` SSH
  - `8088` YARN Service

## Gravitino development image with Trino

### gravitino-ci-trino:0.1.0
- Docker image `datastrato/gravitino-ci-trino:0.1.0`
- Base on `trinodb/trino:426` and removed some unused plugins from it.
- Expose ports:
  - `8080` Trino JDBC port

## Hive (For playground image only)
### hive:2.7.3-no-yarn
- Docker image `datastrato/hive:2.7.3-no-yarn`
- `hadoop-2.7.3`
- `hive-2.3.9`
- Not start YARN when container startup

## Trino (For playground image only)
### trino:426-gravitino-0.3.0-SNAPSHOT
- Docker image `datastrato/trino:426-gravitino-0.3.0-SNAPSHOT`
- Base on `trino:462`
- Added Gravitino trino-connector-0.3.0-SNAPSHOT libs into the `/usr/lib/trino/plugin/gravitino`
