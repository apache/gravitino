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
The Gravitino connector provides the following stored procedures to create and delete catalogs.

Create catalog:

```text
createCatalog(CATALOG varchar, PROVIDER varchar, PROPERTIES varchar, IGNORE EXIST boolean);
```

- CATALOG: The catalog name to be created.
- PROVIDER: The catalog provider, currently only supports `hive`, `lakehouse-iceberg`, `jdbc-mysql`, `jdbc-postgresql`.
- PROPERTIES: The properties of the catalog, the format is a json string. like `{"key1":"value1","key2":"value2"}`.
- IGNORE EXIST: The flag to ignore the error if the catalog already exists. It's optional, the default value is `false`.

Drop catalog:

```text
dropCatalog(CATALOG varchar, IGNORE NOT EXIST boolean);
```

- CATALOG: The catalog name to be created.
- IGNORE NOT EXIST: The flag to ignore the error if the catalog does not exist. It's optional, the default value is `false`.

The two stored procedures are under the `gravitino` connector, and the `system` schema.
So you need to use the following SQL to call them in the `trino-cli`:

Example:
You can run the following SQL to create a catalog named `mysql` with `jdbc-mysql` provider.

```text
call gravitino.system.createCatalog('mysql', 'jdbc-mysql', '{"jdbc-url":"jdbc:mysql://192.168.164.4:3306?useSSL=false","jdbc-user":"trino","jdbc-password":"ds123", "jdbc-driver":"com.mysql.cj.jdbc.Driver"}');
call gravitino.system.dropCatalog('mysql');
```

if you need more information about catalog, please refer to:
[Create a Catalog](../manage-metadata-using-gravitino.md#create-a-catalog).

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

For more about Trino data types, please refer to [Trino data types](https://trino.io/docs/current/language/types.html) and Gravitino data types, please refer to [Gravitino data types](../manage-metadata-using-gravitino.md#gravitino-table-column-type).