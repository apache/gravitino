---
title: "Flink connector hive catalog"
slug: /flink-connector/flink-catalog-hive
keyword: flink connector hive catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Flink connector enables seamless federation queries across different Hive catalogs.
Accessing data or managing metadata in Hive catalogs becomes straightforward.

## Capabilities

Supports most DDL and DML operations in Flink SQL, except for the following operations:

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

* Hive metastore 2.x
* HDFS 2.x or 3.x

## SQL example


```sql
// Suppose hive_a is the Hive catalog name managed by Gravitino
USE CATALOG hive_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

// Create table
CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    date INT
)
PARTITIONED BY (date);

DESC TABLE EXTENDED employees;

INSERT INTO TABLE employees VALUES (1, 'John Doe', 20240101), (2, 'Jane Smith', 20240101);
SELECT * FROM employees WHERE date = '20240101';
```

## Catalog properties

The configuration of Flink Hive Connector is the same with the original Flink Hive connector.
Gravitino catalog property names with the prefix `flink.bypass.` are passed through to the Flink Hive connector.
For example, `flink.bypass.hive-conf-dir` is translated into`hive-conf-dir` to the Flink Hive connector.
The valid catalog properties are listed below.
Any other properties with the prefix `flink.bypass.` in Gravitino Catalog will be ignored by this Connector.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Flink Hive connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>flink.bypass.default-database</tt></td>
  <td><tt>default-database</tt></td>
  <td>Hive default database.</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>flink.bypass.hive-conf-dir</tt></td>
  <td><tt>hive-conf-dir</tt></td>
  <td>Hive configuration directory.</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>flink.bypass.hive-version</tt></td>
  <td><tt>hive-version</tt></td>
  <td>Hive version</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>flink.bypass.hadoop-conf-dir</tt></td>
  <td><tt>hadoop-conf-dir</tt></td>
  <td>Hadoop configuration directory</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>metastore.uris</tt></td>
  <td><tt>hive.metastore.uris</tt></td>
  <td>Hive metastore URI</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

:::caution
You can set other hadoop properties (prefixed with "hadoop.", "dfs.", "fs.", "hive.")
in Gravitino Catalog properties.
These properties will override the configurations from the `hive-conf-dir` and `hadoop-conf-dir`.
:::

