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

-- Suppose paimon is the Paimon catalog name managed by Gravitino
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
    bb BIGINT,
    PRIMARY KEY (aa) NOT ENFORCED
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

You can refer to the configuration of [PaimonCatalog](../lakehouse-paimon-catalog.md) for guidance.



