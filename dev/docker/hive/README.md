<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->
# Hadoop and Hive Docker image
This Docker image is used to support Graviton integration testing.
It includes Hadoop-2.x and Hive-2.x, you can use this Docker image to test the Graviton catalog-hive module.

## Build Docker image
```
./build-docker.sh --platform [all|linux/amd64|linux/arm64] --image {image_name} --tag {tag_name}
```

## Run container
```
docker run --rm -d -p 8022:22 -p 8088:8088 -p 9000:9000 -p 9083:9083 -p 10000:10000 -p 10002:10002 -p 50070:50070 -p 50075:50075 datastrato/graviton-ci-hive
```

## Login Docker container
```
ssh -p 8022 datastrato@localhost (password: ds123, this is a sudo user)
```

# Docker hub repository
- [datastrato/graviton-ci-hive](https://hub.docker.com/r/datastrato/graviton-ci-hive)

## Version change history
### 0.1.0
- Docker image `datastrato/graviton-ci-hive:0.1.0`
- `hadoop-2.7.3`
- `hive-2.3.9`
- Expose ports:
  - `22` SSH
  - `9000` HDFS defaultFS
  - `50070` HDFS NameNode
  - `50075` HDFS DataNode
  - `8088` YARN Service
  - `9083` Hive Metastore
  - `10000` HiveServer2
  - `10002` HiveServer2 HTTP

### 0.1.1
- base on `datastrato/graviton-ci-hive:0.1.0`
- Modify HDFS/YARN/HIVE `MaxPermSize` from `8GB` to `128MB`
- Modify `HADOOP_HEAPSIZE` from `8192` to `128

### 0.1.2
- base on `datastrato/graviton-ci-hive:0.1.1` 
- Modify `fs.defaultFS` from `local` to `0.0.0.0` in the `core-site.xml` file.
- Expose `9000` port int the `Dockerfile` file.

### 0.1.3
- change mysql bind-address from `127.0.0.1` to `0.0.0.0` 
- add `iceberg` to mysql users with password `iceberg`
- export `3306` port for mysql
