---
title: "Gravitino supported Catalogs"
slug: /trino-connector/catalogs
keyword: gravition connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

The catalogs currently supported by the Gravitino connector are as follows:

- [Hive](/docs/trino-connector/catalogs/hive)
- [Iceberg](/docs/trino-connector/catalogs/iceberg)
- [MySQL](/docs/trino-connector/catalogs/mysql)
- [PostgreSQL](/docs/trino-connector/catalogs/postgresql)

## Create catalog

Trino currently does not support creating Gravitino managed catalogs. 
If you need to create a catalog, please refer to:
- [Create Catalog](/docs)
- [Gravition Catalog](/docs)

## Data type mapping

All types of catalogs support the following data type conversions.
Data Type Conversion between Trino and Gravitino is as following:

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
| UnionType      | ROW        |