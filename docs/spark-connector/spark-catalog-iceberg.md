---
title: "Spark connector Iceberg catalog"
slug: /spark-connector/spark-catalog-iceberg
keyword: spark connector iceberg catalog
license: "This software is licensed under the Apache License version 2."
---

The Apache Gravitino Spark connector offers the capability to read and write Iceberg tables, with the metadata managed by the Gravitino server. To enable the use of the Iceberg catalog within the Spark connector, you must set the configuration `spark.sql.gravitino.enableIcebergSupport` to `true` and download Iceberg Spark runtime jar to Spark classpath.

## Capabilities

#### Support DML and DDL operations:

- `CREATE TABLE`

Doesn't support distribution and sort orders.

- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO&OVERWRITE`
- `SELECT`
- `MERGE INTO`
- `DELETE FROM`
- `UPDATE`
- `CALL`
- `TIME TRAVEL QUERY`
- `DESCRIBE TABLE`

#### Not supported operations:

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
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
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

-- Show the details of employee, such as schema and reserved properties(like location, current-snapshot-id, provider, format, format-version, etc)
DESC EXTENDED employee;
```

For more details about `CALL`, please refer to the [Spark Procedures description](https://iceberg.apache.org/docs/1.5.2/spark-procedures/#spark-procedures) in Iceberg official document.

## Catalog properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Iceberg connector configuration.

| Gravitino catalog property name | Spark Iceberg connector configuration | Description                                                                                                                                                                                                         | Since Version    |
|---------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `catalog-backend`               | `type`                                | Catalog backend type                                                                                                                                                                                                | 0.5.0            |
| `uri`                           | `uri`                                 | Catalog backend uri                                                                                                                                                                                                 | 0.5.0            |
| `warehouse`                     | `warehouse`                           | Catalog backend warehouse                                                                                                                                                                                           | 0.5.0            |
| `jdbc-user`                     | `jdbc.user`                           | JDBC user name                                                                                                                                                                                                      | 0.5.0            |
| `jdbc-password`                 | `jdbc.password`                       | JDBC password                                                                                                                                                                                                       | 0.5.0            |
| `io-impl`                       | `io-impl`                             | The io implementation for `FileIO` in Iceberg.                                                                                                                                                                      | 0.6.0-incubating |
| `s3-endpoint`                   | `s3.endpoint`                         | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. | 0.6.0-incubating | 
| `s3-region`                     | `client.region`                       | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     | 0.6.0-incubating |
| `oss-endpoint`                  | `oss.endpoint`                        | The endpoint of Aliyun OSS service.                                                                                                                                                                                 | 0.7.0-incubating |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Iceberg connector. For example, using `spark.bypass.clients` to pass the `clients` to the Spark Iceberg connector.

:::info
Iceberg catalog property `cache-enabled` is setting to `false` internally and not allowed to change.
:::

## Storage

### S3

You need to add s3 secret to the Spark configuration using `spark.sql.catalog.${iceberg_catalog_name}.s3.access-key-id` and `spark.sql.catalog.${iceberg_catalog_name}.s3.secret-access-key`. Additionally, download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle) and place it in the classpath of Spark.

### OSS

You need to add OSS secret key to the Spark configuration using `spark.sql.catalog.${iceberg_catalog_name}.client.access-key-id` and `spark.sql.catalog.${iceberg_catalog_name}.client.access-key-secret`. Additionally, download the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip) and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar` in the classpath of Spark.

### GCS

No extra configuration is needed. Please make sure the credential file is accessible by Spark, like using `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`, and download [Iceberg gcp bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle) and place it to the classpath of Spark.

### Other storage

You may need to add custom configurations with the format `spark.sql.catalog.${iceberg_catalog_name}.{configuration_key}`. Additionally, place corresponding jars which implement `FileIO` in the classpath of Spark.
