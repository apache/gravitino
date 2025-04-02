---
title: "Apache Gravitino Trino connector - Iceberg catalog"
slug: /trino-connector/catalog-iceberg
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

Apache Iceberg is an open table format for huge analytic datasets. 
The Iceberg catalog allows Trino to query data stored in files written in Iceberg format, 
as defined in the Iceberg Table Spec.
The catalog supports Apache Iceberg table spec versions 1 and 2.

## Requirements

To use Iceberg, you need:

- Network access from the Trino coordinator and workers to the distributed object storage.
- Access to a Hive metastore service (HMS), an AWS Glue catalog, a JDBC catalog,
  a REST catalog, or a Nessie server.
- Data files stored in a supported file format.
  These can be configured using file format configuration properties per catalog:

  - ORC
  - Parquet (default)

## Schema operations

### Create a schema

Users can create a schema through Apache Gravitino Trino connector.
For example:

```sql
CREATE SCHEMA catalog.schema_name
```

## Table operations

### Create table

The Apache Gravitino Trino connector currently supports basic Iceberg table creation statements,
such as defining fields, allowing null values, and adding comments.
The Apache Gravitino Trino connector does not support `CREATE TABLE AS SELECT`.

The following example shows how to create a table in the Iceberg catalog:

```shell
CREATE TABLE catalog.schema_name.table_name
(
  name varchar,
  salary int
)
```

### Alter table

The connector supports the following alter table operations:

- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

## Select

The Apache Gravitino Trino connector supports most SELECT statements,
allowing the execution of queries successfully.
Currently, it doesn't support certain query optimizations,
such as push-down or pruning functionalities.

## Table and Schema properties

### Create a schema with properties

Iceberg schema does not support properties.

### Create a table with properties

The following example creates a table with properties:

```sql
CREATE TABLE catalog.dbname.tablename
(
  name varchar,
  salary int
) WITH (
  KEY = 'VALUE',
  ...      
);
```

The following properties are supported by the Iceberg table:

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Reserved</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>partitioning</tt></td>
  <td>Partition columns for the table</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>sorted_by</tt></td>
  <td>Columns used for sorting the table</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>

:::note
A reserved property is one that is read-only for users. 
:::

## Basic usage examples

Before using Iceberg catalogs in Trino with the Gravitino Trino connector,
you need to set up the environment, as shown below:

- Create a metalake and a catalog in Apache Gravitino.
  Assuming that the metalake is named `test` and the catalog is named `iceberg_test`,
  you can use the following command to create them in Apache Gravitino:

  ```bash
  curl -X POST \
    -H "Content-Type: application/json" \
    -d '{"name": "test", "comment": "comment", "properties": {}}' \
    http://gravitino-host:8090/api/metalakes
  
  curl -X POST \
    -H "Content-Type: application/json" \
    -d '{"name": "iceberg_test", "type": "RELATIONAL", "comment": "comment", \
      "provider": "lakehouse-iceberg", "properties": { \
      "uri": "thrift://hive-host:9083", "catalog-backend": "hive", \
      "warehouse": "hdfs://hdfs-host:9000/user/iceberg/warehouse"}}' \
    http://gravitino-host:8090/api/metalakes/test/catalogs
  ```

  For More information about the Iceberg catalog, please refer to
  [Iceberg catalog](../../catalogs/relational/lakehouse/iceberg.md).

- Set the configuration `gravitino.metalake` to the metalake you have created,
  and start the Trino container.

- Use the Trino CLI to connect to the Trino container and run a query.
  Listing all Apache Gravitino managed catalogs:

  ```sql 
  SHOW CATALOGS;
  ```
  
  The results are similar to:
  
  ```text
      Catalog
  ----------------
   gravitino
   jmx
   system
   iceberg_test
  (4 rows)
  
  Query 20231017_082503_00018_6nt3n, FINISHED, 1 node
  ```

The `gravitino` catalog is a catalog defined By Trino catalog configuration. 
The `iceberg_test` catalog is the catalog you created in Apache Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Creating tables and schemas

To create a new schema named `database_01` in the catalog `test.iceberg_test`:

```sql
CREATE SCHEMA iceberg_test.database_01;
```

To create a new table named `table_01` in the schema `iceberg_test.database_01`:

```sql
CREATE TABLE iceberg_test.database_01.table_01
(
  name varchar,
  salary int
) with (
  partitioning = ARRAY['salary'],
  sorted_by = ARRAY['name']
);
```

### Writing data

To insert data into the table `table_01`:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary)
  VALUES ('ice', 12);
```

To insert data into the table `table_01` with a select result:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary)
  SELECT * FROM iceberg_test.database_01.table_01;
```

### Querying data

To query the `table_01` table:

```sql
SELECT * FROM iceberg_test.database_01.table_01;
```

### Modify a table

To add a new column `age` to the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 ADD COLUMN age int;
```

To drop a column `age` from the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 DROP COLUMN age;
```

To rename the `table_01` table to `table_02`:

```sql
ALTER TABLE iceberg_test.database_01.table_01
  RENAME TO iceberg_test.database_01.table_02;
```

### Drop

To drop a schema:

```sql
DROP SCHEMA iceberg_test.database_01;
```

To drop a table:

```sql
DROP TABLE iceberg_test.database_01.table_01;
```

## HDFS username and permissions

Before running any INSERT statements on Iceberg tables, 
you must check that the account Trino uses for accessing HDFS has access to the warehouse directory.
You can override the user name by setting the `HADOOP_USER_NAME` system property
in the Trino JVM config:

```text
-DHADOOP_USER_NAME=<hdfs-user>
```

## S3

When using AWS S3 with the Iceberg catalog, users need to configure the AWS S3-related properties
for the Trino Iceberg connector in the catalog's properties. For more details,
please refer to [Hive connector with Amazon S3](https://trino.io/docs/435/connector/hive-s3.html).
These configurations must be prefixed with `trino.bypass.` to be effective.

To create an Iceberg catalog with AWS S3 configuration in the Trino CLI,
use the following command:

```sql
call gravitino.system.create_catalog(
  'gt_iceberg',
  'lakehouse-iceberg',
  map(
    array[
      'catalog-backend',
      'uri',
      'warehouse',
      'trino.bypass.hive.s3.aws-access-key',
      'trino.bypass.hive.s3.aws-secret-key',
      'trino.bypass.hive.s3.region',
      's3-access-key-id',
      's3-secret-access-key',
      's3-region',
      'io-impl'
    ],
    array[
      'hive',
      'thrift://hive:9083',
      's3a://trino-test-ice/dw2',
      '<aws-access-key>',
      '<aws-secret-key>',
      '<region>',
      '<aws-access-key>',
      '<aws-secret-key>',
      '<region>',
      'org.apache.iceberg.aws.s3.S3FileIO'
    ]
  )
);
```

- The configurations of `trino.bypass.hive.s3.aws-access-key`,
  `trino.bypass.hive.s3.aws-secret-key`, `trino.bypass.hive.s3.region`
  are the required the configurations for the Apache Gravitino Trino connector.

- The configurations of `s3-access-key-id`, `s3-secret-access-key`, `io-impl`,
  and `s3-region` are the required the configurations for the
  [Apache Gravitino Iceberg catalog](../../catalogs/relational/lakehouse/iceberg.md#S3).

- The `location` specifies the storage path on AWS S3.
  Ensure that the specified directory exists on AWS S3 before proceeding.

Once the Iceberg catalog is successfully created, users can create schemas and tables.
For example:

```sql
CREATE SCHEMA gt_iceberg.gt_db03;

CREATE TABLE gt_iceberg.gt_db03.tb01 (
    name varchar,
    salary int
);
```

After running the command, the table is ready for reading and writing operations.

:::note
On the Apache Gravitino server, the AWS S3 support must be enabled for Iceberg catalog.
Please refer to [Apache Gravitino Iceberg catalog](../../catalogs/relational/lakehouse/iceberg.md#S3)
for more details.
:::

