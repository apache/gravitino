---
title: "Trino Connector: Iceberg Catalog"
slug: "/trino-connector/catalog-iceberg"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Iceberg is an open table format for huge analytic datasets. 
The Iceberg catalog allows Trino querying data stored in files written in Iceberg format, 
as defined in the Iceberg Table Spec. The catalog supports Apache Iceberg table spec versions 1 and 2.

## Requirements

To use Iceberg, you need:
- Network access from the Trino coordinator and workers to the distributed object storage.
- Access to a Hive metastore service (HMS), an AWS Glue catalog, a JDBC catalog, a REST catalog, or a Nessie server.
- Data files stored in a supported file format. These can be configured using file format configuration properties per catalog:
  - ORC
  - Parquet (default)

## How Trino Reaches the Catalog

The Gravitino Trino connector loads every `lakehouse-iceberg` catalog through the Gravitino Iceberg
REST server (IRC), regardless of the catalog's `catalog-backend`. `catalog-backend` describes how
Gravitino stores the catalog's metadata; it does not decide how the query engine reaches the data.

This is what makes [credential vending](../security/credential-vending.md) work. Trino only consumes
vended credentials in its `rest` Iceberg catalog type — the `jdbc` and `hive_metastore` types have
nowhere to put the session token of an STS temporary credential — so a catalog with
`credential-providers=s3-token` produces no usable credential on those paths. Routing through the
IRC means every table access gets a freshly issued temporary credential over the Iceberg REST
protocol.

The connector already connects to the Gravitino server (it is how catalogs are discovered in the
first place), so it also asks that server whether it has an Iceberg REST server running as an
[auxiliary service](../iceberg-rest-service.md) for the connector's metalake. By default, a
non-REST `lakehouse-iceberg` catalog is not registered until an endpoint is discovered or configured
explicitly. It is retried during every metadata refresh rather than silently falling back and
disabling credential vending. To retain the behavior from older connector versions, set
`gravitino.iceberg.rest-routing-enabled=false`; this skips discovery and translates the catalog's
`catalog-backend` into the corresponding native Trino Iceberg configuration.

Only the coordinator polls the Gravitino server, so the coordinator resolves the endpoint when a
catalog is registered or refreshed and hands it to every node (coordinator and workers alike)
as part of that catalog's own definition — the same way Trino replicates any other catalog property
cluster-wide. A catalog that could not be registered before the IRC started is registered
automatically after a later discovery poll succeeds; no Trino restart is required.

Set `gravitino.iceberg.rest-uri` to override the discovered endpoint, and it is required — not just
an override — for a standalone IRC (its own process, not the Gravitino server's auxiliary service):
the Gravitino server has no way to know a standalone IRC exists, so discovery never finds one. See
[Limitations](#limitations).

```properties
connector.name=gravitino
gravitino.metalake=test
gravitino.uri=http://gravitino-host:8090

gravitino.iceberg.rest-uri=http://gravitino-host:9001/iceberg
```

The connector derives everything else from the catalog itself. The Gravitino catalog name is passed
as both `iceberg.rest-catalog.warehouse` and `iceberg.rest-catalog.prefix` — the Iceberg client
selects the catalog twice over, first as the query parameter of the `GET /v1/config` call that
discovers it, then as the path segment of every request after that.

The Trino native file system is derived from the catalog's `warehouse` scheme, because vended
credentials are only consumed by Trino's native file systems:

| Warehouse scheme                               | Derived properties                                                                                                                                                 |
|:------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `s3://`, `s3a://`, `s3n://`                    | `fs.native-s3.enabled`, plus `s3.region`, `s3.endpoint` and `s3.path-style-access` where the catalog defines `s3-region`, `s3-endpoint` and `s3-path-style-access` |
| `gs://`                                        | `fs.native-gcs.enabled`                                                                                                                                            |
| `abfs://`, `abfss://`, `wasb://`, `wasbs://`   | `fs.native-azure.enabled`                                                                                                                                          |
| anything else (`hdfs://`, `file://`, `oss://`) | none — only `fs.hadoop.enabled`                                                                                                                                    |

A scheme in the last row has no Trino native file system, so a catalog that vends credentials for it
cannot have them applied; the connector logs a warning when it detects that combination.

On the Gravitino side, the IRC must run with the dynamic config provider so it can serve catalogs
defined in Gravitino:

```properties
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake = test
```

### Authenticating to the Iceberg REST Server

If the IRC has authentication enabled, the internal Iceberg REST catalog that the connector builds
must authenticate to it on its own. This is a separate credential from the one the connector uses
against the main Gravitino server, and it is not reused automatically. Any property prefixed with
`gravitino.iceberg.rest-catalog.` is passed through to the internal catalog with the prefix
rewritten to `iceberg.rest-catalog.`:

```properties
gravitino.iceberg.rest-catalog.security=OAUTH2
gravitino.iceberg.rest-catalog.oauth2.credential=client_id:client_secret
gravitino.iceberg.rest-catalog.oauth2.server-uri=http://your-idp/token
gravitino.iceberg.rest-catalog.oauth2.scope=email
```

Omitting this block against an authenticated IRC surfaces as an authentication error rather than a
missing-configuration error, which is easy to misdiagnose.

Four keys are reserved: `iceberg.rest-catalog.uri`, `.warehouse`, `.prefix` and
`iceberg.catalog.type`. The connector always derives these itself, so setting them through either
`gravitino.iceberg.rest-catalog.` or a catalog's `trino.bypass.` has no effect — the connector logs
when it ignores one.

When `gravitino.client.session.forwardUser=true`, the connector also sets
`iceberg.rest-catalog.session=USER` so that each query carries the end user's identity to the IRC,
keeping per-user credential vending and per-user authorization intact. Set
`gravitino.iceberg.rest-catalog.session` explicitly to override it. See
[Authentication](./authentication.md) for the full setup.

### Limitations

- One IRC serves exactly one metalake, fixed at startup by
  `gravitino.iceberg-rest.gravitino-metalake`. The Gravitino server only reports the IRC's endpoint
  for that metalake. In multi-metalake mode (`gravitino.use-single-metalake=false`), a non-REST
  Iceberg catalog in another metalake therefore requires a metalake-scoped manual URI or remains
  unregistered while REST routing is enabled.
- A catalog created with `catalog-backend=rest` keeps pointing at its own configured `uri` and is
  not re-routed, since it already reaches an Iceberg REST catalog directly.
- A deployment that does not run the IRC must set
  `gravitino.iceberg.rest-routing-enabled=false` to translate non-REST `lakehouse-iceberg` catalogs
  into Trino's `jdbc` or `hive_metastore` catalog types as before.
- Discovery only works for an IRC running as a Gravitino auxiliary service
  (`gravitino.auxService.names=iceberg-rest`), embedded in the same process as the Gravitino server.
  A standalone IRC — its own process, started with `GravitinoIcebergRESTServer` and its own
  `gravitino-iceberg-rest-server.conf` — never registers with the Gravitino server, so the server
  has no way to know it exists; the Gravitino server reports no endpoint even while a standalone IRC
  is running. Set `gravitino.iceberg.rest-uri` manually in this case.

## Schema Operations

### Create a Schema

Users can create a schema through Apache Gravitino Trino connector as follows:

```SQL
CREATE SCHEMA catalog.schema_name
```

## Table Operations

### Create Table

The Apache Gravitino Trino connector supports basic Iceberg table creation statements, such as defining fields,
allowing null values, and adding comments. The Apache Gravitino Trino connector supports `CREATE TABLE AS SELECT`.

:::note
`CREATE OR REPLACE TABLE AS SELECT` is not supported. The Iceberg connector caches the table's UUID
at query-plan time; dropping and recreating the table inside the same transaction causes the
subsequent insert phase to detect a UUID mismatch and fail. Use `DROP TABLE` followed by
`CREATE TABLE AS SELECT` as an alternative.
:::

The following example shows how to create a table in the Iceberg catalog:

```shell
CREATE TABLE catalog.schema_name.table_name
(
  name varchar,
  salary int
)
```

### Alter Table

Support for the following alter table operations:
- Rename table
- Add a column
- Drop a column
- Rename a column
- Change a column type
- Set a table property

### Select

The Apache Gravitino Trino connector supports most SELECT statements, allowing the execution of queries successfully.
It doesn't support certain query optimizations, such as pushdown and pruning functionalities.

### Update

`UPDATE` is only supported for table using v2 or higher of the Iceberg specification. 

### Delete

Support the deletion of entire partitions and deletion of individual rows for table using v2 or higher of the Iceberg specification.
See also [Delete limitation](https://trino.io/docs/current/connector/iceberg.html#data-management).

### Merge

`MERGE` is only supported for table using v2 or higher of the Iceberg specification.

### Table Procedures

The Apache Gravitino Trino connector delegates Iceberg table maintenance procedures
to the underlying Iceberg connector, so they can be invoked via
`ALTER TABLE ... EXECUTE` on Iceberg tables managed by Gravitino. The following
procedures are supported:

| Procedure            | Description                                                                                          |
|----------------------|------------------------------------------------------------------------------------------------------|
| `expire_snapshots`   | Remove old snapshots and their associated metadata/data files to reclaim storage.                    |
| `remove_orphan_files`| Remove files in the table's data directory that are not referenced by any snapshot.                  |
| `optimize`           | Rewrite small data files into fewer, larger files to improve read performance (a.k.a. `rewrite_data_files`). |
| `rewrite_manifests`  | Rewrite the table's manifest files to optimize metadata scans.                                       |

Example usage:

```sql
-- Expire snapshots older than the default retention threshold
ALTER TABLE iceberg_test.database_01.table_01 EXECUTE expire_snapshots;

-- Expire snapshots older than 7 days (requires the minimum retention override
-- to be less than or equal to the requested threshold)
ALTER TABLE iceberg_test.database_01.table_01
  EXECUTE expire_snapshots(retention_threshold => '7d');

-- Remove orphan files
ALTER TABLE iceberg_test.database_01.table_01 EXECUTE remove_orphan_files;

-- Compact small data files
ALTER TABLE iceberg_test.database_01.table_01 EXECUTE optimize;

-- Compact small data files whose size is under a threshold
ALTER TABLE iceberg_test.database_01.table_01
  EXECUTE optimize(file_size_threshold => '128MB');

-- Rewrite manifests for faster metadata scans
ALTER TABLE iceberg_test.database_01.table_01 EXECUTE rewrite_manifests;
```

For the full list of parameters accepted by each procedure, see the
[Trino Iceberg connector documentation](https://trino.io/docs/current/connector/iceberg.html#alter-table-execute).

## Table and Schema Properties

### Create a Schema with Properties

Iceberg schema does not support properties.

### Create a Table with Properties

Users can use the following example to create a table with properties:

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

The following tables are the properties supported by the Iceberg table:

| Property     | Description                     | Default Value | Required | Reserved |
|--------------|---------------------------------|---------------|----------|----------|
| partitioning | Partition columns for the table | (none)        | No       | No       |
| sorted_by    | Sorted columns for the table    | (none)        | No       | No       |

Reserved properties: A reserved property is one can't be set by users but can be read by users. 

## Examples

Complete the following steps before you can use the Iceberg catalog in Trino through Apache Gravitino:

- Create a metalake and catalog in Apache Gravitino. Assuming that the metalake name is `test` and the catalog name is `iceberg_test`,
then you can use the following code to create them in Apache Gravitino:

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "test",
  "comment": "comment",
  "properties": {}
}' http://gravitino-host:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" \
-d '{
  "name": "iceberg_test",
  "type": "RELATIONAL",
  "comment": "comment",
  "provider": "lakehouse-iceberg",
  "properties": {
    "uri": "thrift://hive-host:9083",
    "catalog-backend": "hive",
    "warehouse": "hdfs://hdfs-host:9000/user/iceberg/warehouse"
  }
}' http://gravitino-host:8090/api/metalakes/test/catalogs
```

For More information about the Iceberg catalog, refer to [Iceberg catalog](../lakehouse-iceberg-catalog.md).

- Set the value of configuration `gravitino.metalake` to the metalake you have created, named 'test', and start the Trino container.

Use the Trino CLI to connect to the Trino container and run a query.

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
The `iceberg_test` catalog is the catalog created by you in Apache Gravitino.
Other catalogs are regular user-configured Trino catalogs.

### Create Tables and Schemas

Create a new schema named `database_01` in `test.iceberg_test` catalog.

```sql
CREATE SCHEMA iceberg_test.database_01;
```

Create a new table named `table_01` in schema `iceberg_test.database_01`.

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

### Write Data

Insert data into the table `table_01`:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary) VALUES ('ice', 12);
```

Insert data into the table `table_01` from select:

```sql
INSERT INTO iceberg_test.database_01.table_01 (name, salary) SELECT * FROM iceberg_test.database_01.table_01;
```

Update data into the table `table_01`:

```sql
UPDATE iceberg_test.database_01.table_01 SET name='ice_update' WHERE salary=12;
```

Delete data into the table `table_01`:

```sql
DELETE FROM iceberg_test.database_01.table_01 WHERE name='ice';
```

Merge data into the table `table_01`:

```sql
MERGE INTO iceberg_test.database_01.table_01 t USING iceberg_test.database_01.table_02 s
    ON (t.name = s.name)
    WHEN MATCHED AND s.name = 'bob'
        THEN DELETE
    WHEN MATCHED
        THEN UPDATE
            SET salary = s.salary + t.salary
    WHEN NOT MATCHED
        THEN INSERT (name, salary)
              VALUES (s.name, s.salary);
```

### Query Data

Query the `table_01` table:

```sql
SELECT * FROM iceberg_test.database_01.table_01;
```

### Modify a Table

Add a new column `age` to the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 ADD COLUMN age int;
```

Drop a column `age` from the `table_01` table:

```sql
ALTER TABLE iceberg_test.database_01.table_01 DROP COLUMN age;
```

Rename the `table_01` table to `table_02`:

```sql
ALTER TABLE iceberg_test.database_01.table_01 RENAME TO iceberg_test.database_01.table_02;
```

### Drop

Drop a schema:

```sql
DROP SCHEMA iceberg_test.database_01;
```

Drop a table:

```sql
DROP TABLE iceberg_test.database_01.table_01;
```

## HDFS Username and Permissions

Before running any `Insert` statements for Iceberg tables in Trino, 
you must check that the user Trino is using to access HDFS has access to the warehouse directory.
Override this username by setting the HADOOP_USER_NAME system property in the Trino JVM config, 
replacing hdfs_user with the appropriate username:

```text
-DHADOOP_USER_NAME=hdfs_user
```

## S3

When using AWS S3 within the Iceberg catalog, users need to configure the Trino Iceberg connector's
AWS S3-related properties in the catalog's properties. Refer to the documentation
of [Hive connector with Amazon S3](https://trino.io/docs/current/connector/hive-s3.html).
These configurations must use the `trino.bypass.` prefix in the Iceberg catalog's attributes to be effective.

To create an Iceberg catalog with AWS S3 configuration in the Trino CLI, use the following command:

```sql
call gravitino.system.create_catalog(
    'gt_iceberg',
    'lakehouse-iceberg',
    map(
        array['uri', 'catalog-backend', 'warehouse',
          'trino.bypass.hive.s3.aws-access-key', 'trino.bypass.hive.s3.aws-secret-key', 'trino.bypass.hive.s3.region',
          's3-access-key-id', 's3-secret-access-key', 's3-region', 'io-impl'
        ],
        array['thrift://hive:9083', 'hive', 's3a://trino-test-ice/dw2',
        '<aws-access-key>', '<aws-secret-key>', '<region>',
        '<aws-access-key>', '<aws-secret-key>', '<region>', 'org.apache.iceberg.aws.s3.S3FileIO']
    )
);
```

- The configurations of `trino.bypass.hive.s3.aws-access-key`, `trino.bypass.hive.s3.aws-secret-key`, `trino.bypass.hive.s3.region`
are the required the configurations for the Apache Gravitino Trino connector.
- The configurations of `s3-access-key-id`, `s3-secret-access-key`, `io-impl` and `s3-region`.
are the required the configurations for the [Apache Gravitino Iceberg catalog](../lakehouse-iceberg-catalog.md#s3).
- The `location` specifies the storage path on AWS S3. Ensure that the specified directory exists on AWS S3 before proceeding.

Once the Iceberg catalog is successfully created, users can create schemas and tables as follows:

```sql
CREATE SCHEMA gt_iceberg.gt_db03;

CREATE TABLE gt_iceberg.gt_db03.tb01 (
    name varchar,
    salary int
);
```

After running the command, the tables are ready for data reading and writing operations on AWS S3.

:::note
TThe Iceberg catalog module in the Apache Gravitino server should add AWS S3 support.
Refer to [Apache Gravitino Iceberg catalog](../lakehouse-iceberg-catalog.md#s3).
:::
