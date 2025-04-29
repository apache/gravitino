---
title: Lakehouse Paimon catalog
slug: /lakehouse-paimon-catalog
keywords:
  - lakehouse
  - Paimon
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Apache Gravitino can be used to manage Apache Paimon metadata.

### Requirements

This catalog was built with Apache Paimon *0.8.0*.

## Catalog

### Catalog capabilities

- This catalog works as a proxy, supporting `FilesystemCatalog`, `JdbcCatalog` and `HiveCatalog`.
- This catalog supports DDL operations for Paimon schemas and tables.
  - <tt>alterSchema</tt> is not supported though.

### Catalog properties

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>catalog-backend</tt></td>
  <td>
    The catalog backend for the Gravitino Paimon catalog.
    Supported values include `filesystem`, `jdbc` and `hive`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>uri</tt></td>
  <td>
    The URI of the Paimon catalog. Examples:

    - `thrift://127.0.0.1:9083`
    - `jdbc:postgresql://127.0.0.1:5432/db_name`
    - `jdbc:mysql://127.0.0.1:3306/metastore_db`

    It is optional for `FilesystemCatalog`.

    This is required if the value of `catalog-backend` is not `filesystem`.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>warehouse</tt></td>
  <td>
   The warehouse directory for the catalog.

   - For local filesystem, the format is `file:///user/hive/warehouse-paimon/`;
   - For HDFS, the fomrat is `hdfs://namespace/hdfs/path`;
   - For S3, the format is `s3://{bucket-name}/path/`;
   - For Aliyun OSS, the format is `oss://{bucket-name}/path`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>catalog-backend-name</tt></td>
  <td>
    The catalog name passed to underlying Paimon catalog backend.

    The default value is the property value of `catalog-backend`.
    For example, `jdbc` for JDBC catalog backend.
  </td>
  <td>(see description)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.type</tt></td>
  <td>
    The type of authentication for Paimon catalog backend.
    Currently Gravitino only supports `Kerberos` and `simple`.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>hive.metastore.sasl.enabled</tt></td>
  <td>
    Whether to enable SASL authentication protocol when connecting to a Kerberos Hive metastore.
    This is a raw Hive configuration.

    This value should be `true` in most cases
    when the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.
    In some rare cases, the backend will use SSL protocol.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.principal</tt></td>
  <td>
    The principal of the Kerberos authentication.

    This configuration is required if the value of `authentication.type` is Kerberos.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-uri</tt></td>
  <td>
    The URI of the keytab for Kerberos authentication.

    This is required if the value of `authentication.type` is Kerberos.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.check-interval-sec</tt></td>
  <td>
    The checking interval seconds of Kerberos credential for Paimon catalog.
  </td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-fetch-timeout-sec</tt></td>
  <td>
    The fetch timeout seconds when retrieving Kerberos keytab from
    `authentication.kerberos.keytab-uri`.
  </td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-endpoint</tt></td>
  <td>
    The endpoint of the Aliyun OSS.

    This field is required if the value of `warehouse` is an OSS path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td>
    The access key for the Aliyun OSS.

    This field is required if the value of `warehouse` is an OSS path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-secret</tt></td>
  <td>
    The secret key for the Aliyun OSS.

    This field is required if the value of `warehouse` is an OSS path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-endpoint</tt></td>
  <td>
    The endpoint of the AWS S3.

    This field is required if the value of `warehouse` is an AWS S3 path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-access-key-id</tt></td>
  <td>
    The access key for the AWS S3.

    This field is required if the value of `warehouse` is an AWS S3 path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-secret-access-key</tt></td>
  <td>
    The secret key for the AWS S3.

    This field is required if the value of `warehouse` is an AWS S3 path.
  </td>
  <td>(none)</td>
  <td>(see description)</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::note
If you want to use the `oss` or `s3` warehouse, you need to place related JARs
in the `catalogs/lakehouse-paimon/lib` directory.
More information can be found in the [Paimon S3 page](https://paimon.apache.org/docs/master/filesystems/s3/).
:::

:::note
The hive backend does not support the kerberos authentication now.
:::

Any properties not defined by Gravitino, but prefixed with `gravitino.bypass.`
will be passed to the Paimon catalog and HDFS as configuration.
For example, `gravitino.bypass.table.type` will be translated
into the `table.type` property for a Paimon catalog.

#### JDBC backend

If you are using JDBC backend, you must specify the properties
like `jdbc-user`, `jdbc-password` and `jdbc-driver`.

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>jdbc-user</tt></td>
  <td>
    The JDBC user name to use against Gravitino Paimon catalog for `jdbc` backend.

    This property is required if the value of `catalog-backend` is `jdbc`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc-password</tt></td>
  <td>
    The JDBC password to use against a Gravitino Paimon catalog for `jdbc` backend.

    This property is required if the value of `catalog-backend` is `jdbc`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>jdbc-driver</tt></td>
  <td>
    `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL;
    `org.postgresql.Driver` for PostgreSQL.

    This property is required if the value of `catalog-backend` is `jdbc`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::caution
You must download the corresponding JDBC driver and place it
to the `catalogs/lakehouse-paimon/libs` directory.
:::

### Catalog operations

See [managing relational metadata](../../../metadata/relational.md#catalog-operations).

## Schema

### Schema capabilities

- <tt>createSchema</tt>, <tt>dropSchema</tt>, <tt>loadSchema</tt>,
  and <tt>listSchema</tt> are supported.
- Dascaded dropping of schema is supported.
- <tt>alterSchema</tt> is not supported.

### Schema properties

- No support to specify `location` or to store any schema properties
  when <tt>createSchema</tt> for `FilesystemCatalog`.
- <tt>loadSchema</tt> for `FilesystemCatalog` returns no schema properties.
- No support to store schema comment for `FilesystemCatalog`.

### Schema operations

Refer to [managing relational metadata](../../../metadata/relational.md#schema-operations).

## Table

### Table capabilities

- <tt>createTable</tt>, <tt>purgeTable</tt>, <tt>alterTable</tt>,
  <tt>loadTable</tt>, and <tt>listTable</tt> are supported.
- Column default value through table properties is supported.
  For example, `fields.{columnName}.default-value`.
  Column expression is not supported.

- No support to <tt>dropTable</tt>.
- No support to distributed tables or sorted tables..

:::info
Gravitino Paimon Catalog does not support <tt>dropTable</tt>
because <tt>dropTable</tt> in Paimon will remove both the table metadata
and the table location from the file system and skip the trash,
you should use <tt>purgeTable</tt> instead.
:::

:::info
Paimon does not support auto-increment columns.
:::

### Table changes

- AddColumn
- DeleteColumn
- RemoveProperty
- RenameColumn
- RenameTable
- SetProperty
- UpdateColumnComment
- UpdateColumnNullability
- UpdateColumnPosition
- UpdateColumnType
- UpdateComment

### Table partitions

- Only supports `identity` partitions, such as `day`, `hour`, etc.
- Refer to [Paimon DDL create table](https://paimon.apache.org/docs/0.8/spark/sql-ddl/#create-table)
  documentation for more details.

### Table sort orders

- No support to sorted tables.

### Table distributions

- No support to distributed tables.

### Table indexes

- Only supports primary key Index.

:::info
We cannot specify more than one primary key index.
A primary key index can contain multiple fields as a joint primary key.
:::

:::info
The Paimon table primary key constraint should not be same with partition fields.
The result will be that there is only one record in a partition.
:::

### Table column types

| Gravitino Type              | Apache Paimon Type           |
|-----------------------------|------------------------------|
| `Struct`                    | `Row`                        |
| `Map`                       | `Map`                        |
| `List`                      | `Array`                      |
| `Boolean`                   | `Boolean`                    |
| `Byte`                      | `TinyInt`                    |
| `Short`                     | `SmallInt`                   |
| `Integer`                   | `Int`                        |
| `Long`                      | `BigInt`                     |
| `Float`                     | `Float`                      |
| `Double`                    | `Double`                     |
| `Decimal`                   | `Decimal`                    |
| `String`                    | `VarChar(Integer.MAX_VALUE)` |
| `VarChar`                   | `VarChar`                    |
| `FixedChar`                 | `Char`                       |
| `Date`                      | `Date`                       |
| `Time`                      | `Time`                       |
| `TimestampType withZone`    | `LocalZonedTimestamp`        |
| `TimestampType withoutZone` | `Timestamp`                  |
| `Fixed`                     | `Binary`                     |
| `Binary`                    | `VarBinary`                  |

:::info
Gravitino doesn't support Paimon `MultisetType` type.
:::

### Table properties

You can pass [Paimon table properties](https://paimon.apache.org/docs/0.8/maintenance/configurations/)
to Gravitino when creating a Paimon table.

:::note
- *Reserved*: Fields that cannot be passed to the Gravitino server.
- *Immutable*: Fields that cannot be modified once set.
:::

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Reserved</th>
  <th>Immutable</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>merge-engine</tt></td>
  <td>The table merge-engine.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>sequence.field</tt></td>
  <td>The table sequence.field.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>rowkind.field</tt></td>
  <td>The table rowkind.field.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>comment</tt></td>
  <td>The table comment.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>owner</tt></td>
  <td>The table owner.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>bucket-key</tt></td>
  <td>The table bucket-key.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>primary-key</tt></td>
  <td>The table primary-key.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>partition</tt></td>
  <td>The table partition.</td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

### Table operations

Refer to [managing relational metadata](../../../metadata/relational.md#table-operations).

## HDFS configuration

You can place `core-site.xml` and `hdfs-site.xml` in the `catalogs/lakehouse-paimon/conf` directory
to automatically load as the default HDFS configuration.

:::caution
When reading and writing to HDFS, the Gravitino server can only operate as the specified Kerberos user
and doesn't support proxying to other Kerberos users now.
:::

