<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# Gravitino Docker images
This Docker image is designed to facilitate Gravitino integration testing.
It can be utilized to test all catalog and connector modules within Gravitino.

# Datastrato Docker hub repository
- [Datastrato Docker hub repository address](https://hub.docker.com/r/datastrato)

## How to build Docker image
```
./build-docker.sh --platform [all|linux/amd64|linux/arm64] --type [gravitino|hive|trino] --image {image_name} --tag {tag_name} --latest
```

# Version change history

## Gravitino

### Container startup commands
```
docker run --rm -d -p 8090:8090 datastrato/gravitino
```

### gravitino:0.3.0-SNAPSHOT
- Docker image `datastrato/gravitino:0.3.0-SNAPSHOT`
- Gravitino Server
- Expose ports:
  - `8090` Gravitino Web UI

## Gravitino CI Apache Hive

### Container startup commands
```
docker run --rm -d -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50010:50010 -p 50070:50070 -p 50075:50075 datastrato/gravitino-ci-hive
```

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

## Gravitino CI Trino

### Container startup commands
```
docker run --rm -it -p 8080:8080 datastrato/gravitino-ci-trino
```

### gravitino-ci-trino:0.1.0
- Docker image `datastrato/gravitino-ci-trino:0.1.0`
- Base on `trinodb/trino:426` and removed some unused plugins from it.
- Expose ports:
  - `8080` Trino JDBC port

### gravitino-ci-trino:0.1.2
- Copy JDBC driver 'mysql-connector-java' and 'postgres' to `/usr/lib/trino/iceberg/` folder

## Hive (For experience only)
### hive:2.7.3-no-yarn
- Docker image `datastrato/hive:2.7.3-no-yarn`
- `hadoop-2.7.3`
- `hive-2.3.9`
- Not start YARN when container startup

## Trino (For experience only)
### trino:426-gravitino-0.3.0-SNAPSHOT
- Docker image `datastrato/trino:426-gravitino-0.3.0-SNAPSHOT`
- Base on `trino:462`
- Added Gravitino trino-connector-0.3.0-SNAPSHOT libs into the `/usr/lib/trino/plugin/gravitino`
