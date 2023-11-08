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
./build-docker.sh --platform [all|linux/amd64|linux/arm64] --type [hive|trino] --image {image_name} --tag {tag_name} --latest
```

# Version change history
## Gravitino CI Hive

### Container startup commands
```
docker run --rm -d -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 -p 50010:50010 datastrato/gravitino-ci-hive
```

### 0.1.0
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

### 0.1.1
- base on `datastrato/gravitino-ci-hive:0.1.0`
- Modify HDFS/YARN/HIVE `MaxPermSize` from `8GB` to `128MB`
- Modify `HADOOP_HEAPSIZE` from `8192` to `128

### 0.1.2
- base on `datastrato/gravitino-ci-hive:0.1.1` 
- Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
- Expose `9000` port int the `Dockerfile` file.

### 0.1.3
- change mysql bind-address from `127.0.0.1` to `0.0.0.0` 
- add `iceberg` to mysql users with password `iceberg`
- export `3306` port for mysql

### 0.1.4
- Config HDFS DataNode data transfer address to `0.0.0.0:50010` explicitly
- Map container hostname to `127.0.0.1` before starting Hadoop
- Expose `50010` port for the HDFS DataNode

### 0.1.5
- Rollback `Map container hostname to 127.0.0.1 before starting Hadoop` of `datastrato/gravitino-ci-hive:0.1.4`

## Gravitino CI Trino

### Container startup commands
```
docker run --rm -it -p 8080:8080 datastrato/gravitino-ci-trino
```

### 0.1.0
- Docker image `datastrato/gravitino-ci-trino:0.1.0`
- Base on `trinodb/trino:426` and removed some unused plugins from it.
- Expose ports:
  - `8080` Trino JDBC port