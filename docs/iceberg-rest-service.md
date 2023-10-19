---
title: "How to setup Gravitino Iceberg REST server"
date: 2023-10-18T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---

## Background
Gravitino Iceberg REST Server follows the [Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and could act as an Iceberg REST catalog server. 

### Capabilities:
* Support Iceberg REST API defined in Iceberg 1.3.1, support all namespace&table interface. `Token`, `ReportMetrics` and `Config` interface are not supported yet.
* Worked as a catalog proxy, supports HiveCatalog and JdbcCatalog for now.
* Build with Iceberg `1.3.1`, which means the Iceberg table format version is `1` by default.

## How to start the Gravitino Iceberg REST server

Suppose the Gravitino server is deployed on the `GRAVITINO_HOME` directory.
All configurations are in `$GRAVITINO_HOME/conf/gravitino.conf`

### Basic Gravitino Iceberg server configuration

| Configuration item                | Description                                                                                                                 | Configuration value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.names ` | The aux service name of Gravitino Iceberg REST server, the name couldn't be changed | ` iceberg-rest `|
| `gravitino.auxService.iceberg-rest.classpath ` | The classpath of Gravitino Iceberg REST server, includes dirs with jar and configuration which support both absolute path and relative path | `catalogs/lakehouse-iceberg/libs, catalogs/lakehouse-iceberg/conf`|
| `gravitino.auxService.iceberg-rest.host` | The host of Gravitino Iceberg REST server | `127.0.0.1`|
| `gravitino.auxService.iceberg-rest.httpPort` | The port Gravitino Iceberg REST server | `9001`|

### Iceberg catalog configuration
Gravitino Iceberg REST server uses MemoryCatalog by default, it's not recommended on production, we could specify HiveCatalog or JdbcCatalog.

#### Hive catalog configuration

| Configuration item                | Description                                                                                                                 | Configuration value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.iceberg-rest.catalog-backend` | Catalog backend of Gravitino Iceberg REST server | `hive`|
| `gravitino.auxService.iceberg-rest.uri` | Hive metadata address | `thrift://127.0.0.1:9083`|
| `gravitino.auxService.iceberg-rest.warehouse ` | Warehouse directory of HiveCatalog | `/user/hive/warehouse-hive/`|

#### Jdbc catalog configuration

| Configuration item                | Description                                                                                                                 | Configuration value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.iceberg-rest.catalog-backend` | Catalog backend of Gravitino Iceberg REST server | `jdbc`|
| `gravitino.auxService.iceberg-rest.uri` | The jdbc connection address | `jdbc:postgresql://127.0.0.1:5432/`|
| `gravitino.auxService.iceberg-rest.warehouse ` | Warehouse directory of JdbcCatalog, you should set HDFS prefix explictly if using HDFS | `/user/hive/warehouse-jdbc/`|
| `gravitino.auxService.iceberg-rest.jdbc.user` | The username of the Jdbc connection| `jdbc username`|
| `gravitino.auxService.iceberg-rest.jdbc.password` | The password of the Jdbc connection  | `jdbc password`|
| `gravitino.auxService.iceberg-rest.jdbc-initialize` | Whether to initialize meta tables when create Jdbc catalog | `true`|

If using Jdbc catalog, you must download responding jdbc driver jars to Gravitino Iceberg REST server classpath, such as `catalogs/lakehouse-iceberg/libs`.

### Other Iceberg catalog properties
We could add other properties defined in [CatalogProperties of Iceberg](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java), please note that `catalog-impl` doesn't take effect. 
Take `clients` properities for example:

| Configuration item                | Description                                                                                                                 | Configuration value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.iceberg-rest.clients` | client pool size of catalog | `10`|


### HDFS configuration
HDFS config files(`core-site.xml` and `hdfs-site.xml`) could be put to dirs defined in `gravitino.auxService.iceberg-rest.classpath `, such as `catalogs/lakehouse-iceberg/conf`, it will be add to the classpath. 

## Start up Gravitino Iceberg REST server
Start up Gravitino Iceberg REST server
```
./bin/gravitino.sh start
```
Check whether Gravitino Iceberg REST server started
```
curl  http://127.0.0.1:9001/iceberg/application.wadl
```

## Exploring Gravitino Iceberg REST server with Spark

### Deploy Spark with Iceberg support

follow [Spark Iceberg start guide](https://iceberg.apache.org/docs/latest/getting-started/) to setup Spark&Iceberg environment. please keep the Spark version consistent with the spark-iceberg-runtime version.


### Start Spark client with Iceberg REST catalog
| Configuration item                | Description                                                                                                                 | Configuration value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `spark.sql.catalog.${catalog-name}.type` | Spark catalog type, should set to `rest` | `rest`|
| `spark.sql.catalog.${catalog-name}.uri` | Spark Iceberg REST catalog uri | `http://127.0.0.1:9001/iceberg/`|

for example:
```
./bin/spark-shell -v \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog  \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/
```

### Explore Iceberg with Spark SQL
```sql
//First change to `rest` catalog, `rest` is the catalog name
USE rest;
CREATE DATABASE IF NOT EXISTS dml;
CREATE TABLE dml.test (id bigint COMMENT 'unique id') using iceberg
DESCRIBE TABLE EXTENDED dml.test;
INSERT INTO dml.test VALUES (1), (2);
SELECT * FROM dml.test
```

If you have any problem or requirement, please create a issue :)
