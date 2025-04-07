---
title: "Flink connector Iceberg catalog"
slug: /flink-connector/flink-catalog-iceberg
keyword: flink connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Flink connector can be used to read and write Iceberg tables,
with the metadata managed by the Gravitino server.
To enable the Flink connector, you must download the Iceberg Flink runtime JAR
and place it into the Flink class path.

## Capabilities

#### Supported DML and DDL operations:

- `CREATE CATALOG`
- `CREATE DATABASE`
- `CREATE TABLE`
- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO & OVERWRITE`
- `SELECT`

#### Operations not supported:

- Partition operations
- View operations
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}&snapshots`
- Query UDF
- `UPDATE` clause
- `DELETE` clause
- `CREATE TABLE LIKE` clause

## SQL example

```sql

-- Suppose iceberg_a is the Iceberg catalog name managed by Gravitino

USE CATALOG iceberg_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE sample (
    id BIGINT COMMENT 'unique id',
    data STRING NOT NULL
) PARTITIONED BY (data) 
WITH ('format-version'='2');

INSERT INTO sample
VALUES (1, 'A'), (2, 'B');

SELECT * FROM sample WHERE data = 'B';

```

## Catalog properties

The Gravitino Flink connector transforms the following properties
in a catalog to Flink connector configuration.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Flink Iceberg connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td><tt>catalog-type</tt></td>
  <td>
    Catalog backend type. Currently, only `Hive` catalog is supported.
    Support to `JDBC` and `Rest` are coming soon.
  </td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td><tt>uri</tt></td>
  <td>URI for the catalog backend</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td><tt>warehouse</tt></td>
  <td>Warehouse in the catalog backend</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>io-impl</tt></td>
  <td><tt>io-impl</tt></td>
  <td>The I/O implementation for `FileIO` in Iceberg.</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-endpoint</tt></td>
  <td><tt>oss.endpoint</tt></td>
  <td>The endpoint of Aliyun OSS service.</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td><tt>client.access-key-id</tt></td>
  <td>The static access key ID used to access OSS data.</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td><tt>client.access-key-secret</tt></td>
  <td>The static secret access key used to access OSS data.</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names with the prefix `flink.bypass.` are passed through
to the Flink iceberg connector.
For example, `flink.bypass.clients` is translated to `clients` via the Flink iceberg connector.

## Storage

### OSS

You need download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip),
and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar`
into the Flink class path.

