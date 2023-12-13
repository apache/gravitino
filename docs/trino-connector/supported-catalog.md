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

Trino currently does not support creating Gravitino managed catalogs. 
If you need to create a catalog, please refer to:
- [Create a Catalog](../manage-metadata-using-gravitino.md#create-a-catalog)

## Data type mapping between Trino and Gravitino

All types of catalogs support the following data type conversions between Trino and Gravitino:

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
| TimestampType  | TIMESTAMP  |
| DateType       | DATE       |
| ArrayType      | ARRAY      |
| MapType        | MAP        |
| StructType     | ROW        |