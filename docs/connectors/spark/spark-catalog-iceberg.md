---
title: "Spark connector Iceberg catalog"
slug: /spark-connector/spark-catalog-iceberg
keyword: spark connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector enable users to read and write Iceberg tables,
with the their metadata managed by the Gravitino server.
To support Iceberg catalog using the Spark connector, you must set the configuration
`spark.sql.gravitino.enableIcebergSupport` to `true`,
and download Iceberg Spark runtime JARs into Spark's class path.

## Capabilities

#### Support DML and DDL operations:

- `CREATE TABLE`

   Doesn't support distribution and sort orders.

- `DESCRIBE TABLE`
- `ALTER TABLE`
- `DROP TABLE`
- `INSERT INTO/OVERWRITE`
- `UPDATE`
- `SELECT`
- `MERGE INTO`
- `DELETE FROM`
- `CALL`
- `TIME TRAVEL QUERY`

#### Unsupported operations

- View operations.
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}.snapshots`
- Other Iceberg extension SQLs, like:
  - `ALTER TABLE prod.db.sample ADD PARTITION FIELD xx`
  - `ALTER TABLE ... WRITE ORDERED BY`
  - `ALTER TABLE prod.db.sample CREATE BRANCH branchName`
  - `ALTER TABLE prod.db.sample CREATE TAG tagName`
- AtomicCreateTableAsSelect&AtomicReplaceTableAsSelect

## SQL example

```sql
-- Suppose iceberg_a is the Iceberg catalog name managed by Gravitino
USE iceberg_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employee (
  id bigint,
  name string,
  department string,
  hire_date timestamp
) USING iceberg
PARTITIONED BY (days(hire_date));
DESC TABLE EXTENDED employee;

INSERT INTO employee
VALUES
(1, 'Alice', 'Engineering', TIMESTAMP '2021-01-01 09:00:00'),
(2, 'Bob', 'Marketing', TIMESTAMP '2021-02-01 10:30:00'),
(3, 'Charlie', 'Sales', TIMESTAMP '2021-03-01 08:45:00');

SELECT * FROM employee WHERE date(hire_date) = '2021-01-01';

UPDATE employee SET department = 'Jenny' WHERE id = 1;

DELETE FROM employee WHERE id < 2;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department,
       TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department,
       TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT *;

-- Suppose that the first snapshotId of employee is 1L and the second snapshotId is 2L
-- Rollback the snapshot for iceberg_a.mydatabase.employee to 1L
CALL iceberg_a.system.rollback_to_snapshot('iceberg_a.mydatabase.employee', 1);
-- Set the snapshot for iceberg_a.mydatabase.employee to 2L
CALL iceberg_a.system.set_current_snapshot('iceberg_a.mydatabase.employee', 2);

-- Suppose that the commit timestamp of the first snapshot is older than '2024-05-27 01:01:00'
-- Time travel to '2024-05-27 01:01:00'
SELECT * FROM employee TIMESTAMP AS OF '2024-05-27 01:01:00';
SELECT * FROM employee FOR SYSTEM_TIME AS OF '2024-05-27 01:01:00';

-- Show the details of employee, such as schema and reserved properties
-- like location, current-snapshot-id, provider, format, format-version, etc
DESC EXTENDED employee;
```

For more details about `CALL`, please refer to the [Spark Procedures description](https://iceberg.apache.org/docs/1.5.2/spark-procedures/#spark-procedures)
in the Iceberg official document.

## Catalog properties

Gravitino spark connector will translate some properties defined in catalog properties
to Spark Iceberg connector configurations.

<table>
<thead>
<tr>
  <th>Gravitino catalog property name</th>
  <th>Spark Iceberg connector configuration</th>
  <th>Description</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td><tt>type</tt></td>
  <td>Catalog backend type.Supports `hive` or `jdbc` or `rest` or `custom`.</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>catalog-backend-impl</tt></td>
  <td><tt>catalog-impl</tt></td>
  <td>
    The fully-qualified class name of a custom catalog implementation.
    Only applicable when `catalog-backend` is `custom`.
  </td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td><tt>uri</tt></td>
  <td>Catalog backend URI</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td><tt>warehouse</tt></td>
  <td>Catalog backend warehouse</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td><tt>jdbc.user</tt></td>
  <td>JDBC user name</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td><tt>jdbc.password</tt></td>
  <td>JDBC password</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>io-impl</tt></td>
  <td><tt>io-impl</tt></td>
  <td>The I/O implementation for `FileIO` in Iceberg.</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-endpoint</tt></td>
  <td><tt>s3.endpoint</tt></td>
  <td>
    An alternative endpoint of the S3 service.
    This could be used for S3FileIO with any s3-compatible object storage service
    that has a different endpoint.
    It can be used for accessing a private S3 endpoint in a virtual private cloud.
  </td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-region</tt></td>
  <td><tt>client.region</tt></td>
  <td>The region of the S3 service, like `us-west-2`.</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-access-key-id</tt></td>
  <td><tt>s3.access-key-id</tt></td>
  <td>The static access key ID used to access S3 data</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-secret-access-key</tt></td>
  <td><tt>s3.secret-access-key</tt></td>
  <td>The static secret access key used to access S3 data.</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-path-style-access</tt></td>
  <td><tt>s3.path-style-access</tt></td>
  <td>Whether to use path style access for S3.</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-endpoint</tt></td>
  <td><tt>oss.endpoint</tt></td>
  <td>The endpoint of Aliyun OSS service.</td>
  <td>`0.7.0-incubating`</td>
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
<tr>
  <td><tt>azure-storage-account-name</tt></td>
  <td><tt>adls.auth.shared-key.account.name</tt></td>
  <td>The static storage account name used to access ADLS data.</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-key</tt></td>
  <td><tt>adls.auth.shared-key.account.key</tt></td>
  <td>The static storage account key used to access ADLS data..</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

Gravitino catalog property names with the prefix `spark.bypass.` are passed
to the Spark Iceberg connector.
For example, `spark.bypass.clients` is translated into `clients`
for the Spark Iceberg connector.

:::info
Iceberg catalog property `cache-enabled` is setting to `false` internally and cannot change.
:::

## Storage

Spark connector can convert storage properties in the Gravitino catalog
to Spark Iceberg connector automatically.
No extra configuration is needed for `S3`, `ADLS`, `OSS`, `GCS`.

### S3

Please download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle)
and place it into the class path for Spark.

### OSS

Please download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip)
and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar`
into the class path for Spark.

### GCS

Please make sure the credential file is accessible by Spark.
For example,`export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`.
You need to download the [Iceberg GCP bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle)
and place it into the classs path for Spark.

### ADLS

Please download the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle)
and place it into the class path for Spark.

### Other storage

You may need to add custom configurations with the format
`spark.sql.catalog.<iceberg-catalog-name>.<configuration-key>`.
The corresponding JARs implementing `FileIO` are supposed to be placed
into the class path for Spark.

