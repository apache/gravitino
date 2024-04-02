---
title: "Gravitino supported Catalogs"
slug: /trino-connector/supported-catalog
keyword: gravitino connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

The catalogs currently supported by the Gravitino connector are as follows:

- [Hive](catalog-hive.md)
- [Iceberg](catalog-iceberg.md)
- [MySQL](catalog-mysql.md)
- [PostgreSQL](catalog-postgresql.md)

## Create catalog

Trino itself does not support creating catalogs. 
Users can create catalogs through the Gravitino connector and then load them into Trino. 
The Gravitino connector provides the following stored procedures to create, delete, and alter catalogs.
User can also use the system table `catalog` to describe all the catalogs.

Create catalog:

```sql
create_catalog(CATALOG varchar, PROVIDER varchar, PROPERTIES MAP(VARCHAR, VARCHAR), IGNORE_EXIST boolean);
```

- CATALOG: The catalog name to be created.
- PROVIDER: The catalog provider, currently only supports `hive`, `lakehouse-iceberg`, `jdbc-mysql`, `jdbc-postgresql`.
- PROPERTIES: The properties of the catalog.
- IGNORE_EXIST: The flag to ignore the error if the catalog already exists. It's optional, the default value is `false`.

The type of catalog properties reference:
- [Hive catalog](../apache-hive-catalog.md#catalog-properties)
- [Iceberg catalog](../lakehouse-iceberg-catalog.md#catalog-properties)
- [MySQL catalog](../jdbc-mysql-catalog.md#catalog-properties)
- [PostgreSQL catalog](../jdbc-postgresql-catalog.md#catalog-properties)


Drop catalog:

```sql
drop_catalog(CATALOG varchar, IGNORE_NOT_EXIST boolean);
```

- CATALOG: The catalog name to be deleted.
- IGNORE_NOT_EXIST: The flag to ignore the error if the catalog does not exist. It's optional, the default value is `false`.


Alter catalog:

```sql
alter_catalog(CATALOG varchar, SET_PROPERTIES MAP(VARCHAR, VARCHAR), REMOVE_PROPERTIES ARRY[VARCHAR]);
```

- CATALOG: The catalog name to be altered.
- SET_PROPERTIES: The properties to be set.
- REMOVE_PROPERTIES: The properties to be removed.

These stored procedures are under the `gravitino` connector and the `system` schema.
So you need to use the following SQL to call them in the `trino-cli`:


Describe catalogs:

The system table `gravitino.system.catalog` is used to describe all the catalogs.

```sql
select * from gravitino.system.catalog;
```

The result is like:

```test
     name     | provider |                                                 properties
--------------+----------+-------------------------------------------------------------------------------------------------------------
 test.gt_hive | hive     | {gravitino.bypass.hive.metastore.client.capability.check=false, metastore.uris=thrift://trino-ci-hive:9083}
```

Example:
You can run the following SQL to create a catalog named `mysql` with `jdbc-mysql` provider.

```sql
-- Call stored procedures with position.
call gravitino.system.create_catalog(
    'mysql',
    'jdbc-mysql',
    Map(
        Array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        Array['jdbc:mysql:192.168.164.4:3306?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    )
)
call gravitino.system.drop_datalog('mysql');

-- Call stored procedures with name.
call gravitino.system.create_catalog(
    catalog =>'mysql',
    provider => 'jdbc-mysql',
    properties => Map(
        Array['jdbc-url', 'jdbc-user', 'jdbc-password', 'jdbc-driver'],
        Array['jdbc:mysql:192.168.164.4:3306?useSSL=false', 'trino', 'ds123', 'com.mysql.cj.jdbc.Driver']
    ),
    ignore_exist => true
)

call gravitino.system.drop_datalog(
    catalog => 'mysql'
    ignore_not_exist => true
);

call gravitino.system.alter_catalog(
    catalog => 'mysql',
    set_properties=> Map(
        Array['jdbc-url'],
        Array['jdbc:mysql:127.0.0.1:3306?useSSL=false']
    ),
    remove_properties => Array['jdbc-driver']
)
```

if you need more information about catalog, please refer to:
[Create a Catalog](../manage-relational-metadata-using-gravitino.md#create-a-catalog).

## Data type mapping between Trino and Gravitino

Gravitino connector supports the following data type conversions between Trino and Gravitino currently. Depending on the detailed catalog, Gravitino may not support some data types conversion for this specific catalog, for example,
Hive does not support `TIME` data type.

| Gravitino Type | Trino Type |
|----------------|------------|
| BooleanType    | BOOLEAN    |
| ByteType       | TINYINT    |
| ShortType      | SMALLINT   |
| IntegerType    | INTEGER    |
| LongType       | BIGINT     |
| FloatType      | REAL       |
| DoubleType     | DOUBLE     |
| DecimalType    | DECIMAL    |
| StringType     | VARCHAR    |
| VarcharType    | VARCHAR    |
| BinaryType     | VARBINARY  |
| DateType       | DATE       |
| TimeType       | TIME       |
| TimestampType  | TIMESTAMP  |
| ArrayType      | ARRAY      |
| MapType        | MAP        |
| StructType     | ROW        |

For more about Trino data types, please refer to [Trino data types](https://trino.io/docs/current/language/types.html) and Gravitino data types, please refer to [Gravitino data types](../manage-relational-metadata-using-gravitino.md#gravitino-table-column-type).
