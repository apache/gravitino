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
All configurations are in [`$GRAVITINO_HOME/conf/gravitino.conf`](gravitino-server-config.md)

### Basic Gravitino Iceberg server configuration

| Configuration item                | Description                                                                                                                 | Default value | Since Version |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|---------------|
| `gravitino.auxService.names ` | The auxiliary service name of Gravitino Iceberg REST server, we should use **`iceberg-rest`** for Gravitino Iceberg REST server | null | 0.2.0         |
| `gravitino.auxService.iceberg-rest.classpath ` | The classpath of Gravitino Iceberg REST server, includes dirs with jar and configuration which support both absolute path and relative path, like `catalogs/lakehouse-iceberg/libs, catalogs/lakehouse-iceberg/conf` | null | 0.2.0         |
| `gravitino.auxService.iceberg-rest.host` | The host of Gravitino Iceberg REST server | `0.0.0.0`| 0.2.0         |
| `gravitino.auxService.iceberg-rest.httpPort` | The port Gravitino Iceberg REST server, **the default port is same with port of Gravitino server, we should set a diffrent port explicitly, like `9001`** | `8090` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.minThreads` | The number of min threads in the thread pool which is used by Jetty webserver. `minThreads` will be adjusted to 4 if the value is less than 4. | `Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 4)` | 0.2.0 |
| `gravitino.auxService.iceberg-rest.maxThreads` | The number of max threads in the thread pool which is used by Jetty webserver. `maxThreads` will be adjusted to 4 if the value is less than 4, and `maxThreads` must be great or equal to `minThreads` | `Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.threadPoolWorkQueueSize` | The size of queue in thread pool which is used by Gravitino Iceberg REST server. | `100` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.stopTimeout` | Time of graceful stop for Gravitino Iceberg REST server, for more, please see `org.eclipse.jetty.server.Server#setStopTimeout`, unit: ms. | `30000` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.idleTimeout` | The timeout of idle connections, unit: ms. | `30000` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.requestHeaderSize` | Max size of Http request. | `131072` | 0.2.0         |
| `gravitino.auxService.iceberg-rest.responseHeaderSize` | Max size of Http response. | `131072` | 0.2.0         |

### Iceberg catalog configuration
Gravitino Iceberg REST server uses memory catalog by default, it's not recommended in production, we could specify Hive catalog or Jdbc catalog.

#### Hive catalog configuration

| Configuration item                | Description                                                                                                                 | Default value |  Since Version |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|-----|
| `gravitino.auxService.iceberg-rest.catalog-backend` | Catalog backend of Gravitino Iceberg REST server, use **`hive`** for Hive catalog | `memory` | 0.2.0 |
| `gravitino.auxService.iceberg-rest.uri` | Hive metadata address, like `thrift://127.0.0.1:9083` | null | 0.2.0 |
| `gravitino.auxService.iceberg-rest.warehouse ` | Warehouse directory of Hive catalog, like `/user/hive/warehouse-hive/` | null | 0.2.0 |

#### Jdbc catalog configuration

| Configuration item                | Description                                                                                                                 | Default value |  Since Version |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|-----|
| `gravitino.auxService.iceberg-rest.catalog-backend` | Catalog backend of Gravitino Iceberg REST server, use **`jdbc`** for Jdbc catalog | `memory`| 0.2.0 |
| `gravitino.auxService.iceberg-rest.uri` | The Jdbc connection address, like `jdbc:postgresql://127.0.0.1:5432` for postgres, `jdbc:mysql://127.0.0.1:3306/` for mysql  | null | 0.2.0 |
| `gravitino.auxService.iceberg-rest.warehouse ` | Warehouse directory of Jdbc catalog, you should set HDFS prefix explicitly if using HDFS, like `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc` | null | 0.2.0 |
| `gravitino.auxService.iceberg-rest.jdbc.user` | The username of the Jdbc connection| null | 0.2.0 |
| `gravitino.auxService.iceberg-rest.jdbc.password` | The password of the Jdbc connection  | null | 0.2.0 |
| `gravitino.auxService.iceberg-rest.jdbc-initialize` | Whether to initialize meta tables when create Jdbc catalog | `true` | 0.2.0 |

If using Jdbc catalog, you must **download responding Jdbc driver jars to Gravitino Iceberg REST server classpath**, such as `catalogs/lakehouse-iceberg/libs`.

### Other Iceberg catalog properties
We could add other properties defined in [CatalogProperties of Iceberg](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java), please note that `catalog-impl` doesn't take effect. 
Take `clients` property for example:

| Configuration item                | Description                                                                                                                 | Default value |
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|
| `gravitino.auxService.iceberg-rest.clients` | Client pool size of catalog | `2` |


### HDFS configuration
HDFS config files(`core-site.xml` and `hdfs-site.xml`) could be put to dirs defined in `gravitino.auxService.iceberg-rest.classpath `, such as `catalogs/lakehouse-iceberg/conf`, it will be added to the classpath. 

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

Follow [Spark Iceberg start guide](https://iceberg.apache.org/docs/latest/getting-started/) to setup Spark&Iceberg environment. please keep the Spark version consistent with the spark-iceberg-runtime version.


### Start Spark client with Iceberg REST catalog
| Configuration item                | Description                                                                                                                 | 
|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `spark.sql.catalog.${catalog-name}.type` | Spark catalog type, should set to `rest` | 
| `spark.sql.catalog.${catalog-name}.uri` | Spark Iceberg REST catalog uri, like `http://127.0.0.1:9001/iceberg/` |

For example:
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

If you have any problem or requirement, please create an issue :)
