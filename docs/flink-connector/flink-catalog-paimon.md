---
title: "Flink connector paimon catalog"
slug: /flink-connector/flink-catalog-paimon
keyword: flink connector paimon catalog
license: "This software is licensed under the Apache License version 2."
---

Accessing data in Paimon and managing Paimon's metadata will become simpler through the Apache Gravitino Flink connector.

## Capabilities

:::caution
Currently, only AppendOnly tables are supported.
:::

Supports most DDL and DML operations in Flink SQL, except such operations:

- Function operations
- Partition operations
- View operations
- Querying UDF
- `LOAD` clause
- `UNLOAD` clause
- `CREATE TABLE LIKE` clause
- `TRUCATE TABLE` clause
- `UPDATE` clause
- `DELETE` clause
- `CALL` clause

## Requirement

* Paimon 0.8

## Getting Started

### Prerequisites

Place the following JAR files in the lib directory of your Flink installation:

* paimon-flink-1.18-0.8.2.jar

* gravitino-flink-connector-runtime-*.jar

### SQL Example

```sql

-- Suppose paimon_catalog is the Paimon catalog name managed by Gravitino
use catalog paimon_catalog;
-- Execute statement succeed.

show databases;
-- +---------------------+
-- |       database name |
-- +---------------------+
-- |             default |
-- | gravitino_paimon_db |
-- +---------------------+

SET 'execution.runtime-mode' = 'batch';
-- [INFO] Execute statement succeed.

SET 'sql-client.execution.result-mode' = 'tableau';
-- [INFO] Execute statement succeed.

CREATE TABLE paimon_tabla_a (
    aa BIGINT,
    bb BIGINT
);

show tables;
-- +----------------+
-- |     table name |
-- +----------------+
-- | paimon_table_a |
-- +----------------+


select * from paimon_table_a;
-- Empty set

insert into paimon_table_a(aa,bb) values(1,2);
-- [INFO] Submitting SQL update statement to the cluster...
-- [INFO] SQL update statement has been successfully submitted to the cluster:
-- Job ID: 74c0c678124f7b452daf08c399d0fee2

select * from paimon_table_a;
-- +----+----+
-- | aa | bb |
-- +----+----+
-- |  1 |  2 |
-- +----+----+
-- 1 row in set
```

## Catalog properties

Gravitino Flink connector will transform below property names which are defined in catalog properties to Flink Paimon connector configuration.

| Gravitino catalog property name | Flink Paimon connector configuration   | Description                                                                                                                                                                                                 | Since Version    |
|---------------------------------|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `metastore`                            | Catalog backend of Gravitino Paimon catalog. Supports `filesystem`, `jdbc` and `hive`.                                                                                                                      | 0.8.0-incubating |
| `uri`                           | `uri`                                  | The URI configuration of the Paimon catalog. `thrift://127.0.0.1:9083` or `jdbc:postgresql://127.0.0.1:5432/db_name` or `jdbc:mysql://127.0.0.1:3306/metastore_db`. It is optional for `FilesystemCatalog`. | 0.8.0-incubating |
| `warehouse`                     | `warehouse`                            | Warehouse directory of catalog. `file:///user/hive/warehouse-paimon/` for local fs, `hdfs://namespace/hdfs/path` for HDFS , `s3://{bucket-name}/path/` for S3 or `oss://{bucket-name}/path` for Aliyun OSS  | 0.8.0-incubating |
| `oss-endpoint`                  | `fs.oss.endpoint`                      | The endpoint of the Aliyun OSS.                                                                                                                                                                             | 0.8.0-incubating |
| `oss-access-key-id`             | `fs.oss.accessKeyId`                   | The access key of the Aliyun OSS.                                                                                                                                                                           | 0.8.0-incubating |
| `oss-accesss-key-secret`        | `fs.oss.accessKeySecret`               | The secret key the Aliyun OSS.                                                                                                                                                                              | 0.8.0-incubating |
| `s3-endpoint`                   | `s3.endpoint`                          | The endpoint of the AWS S3.                                                                                                                                                                                 | 0.8.0-incubating |
| `s3-access-key-id`              | `s3.access-key`                        | The access key of the AWS S3.                                                                                                                                                                               | 0.8.0-incubating |
| `s3-secret-access-key`          | `s3.secret-key`                        | The secret key of the AWS S3.                                                                                                                                                                               | 0.8.0-incubating |
| `jdbc-user`                     | `jdbc.user`                            | The user of JDBC                                                                                                                                                                                            | 0.8.0-incubating |
| `jdbc-password`                 | `jdbc.password`                        | The password of JDBC                                                                                                                                                                                        | 0.8.0-incubating |

Gravitino catalog property names with the prefix `flink.bypass.` are passed to Flink Paimon connector. For example, using `flink.bypass.clients` to pass the `clients` to the Flink Paimon connector.
