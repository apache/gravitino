---
title: "Manage Relational Metadata"
slug: "/manage-relational-metadata-using-gravitino"
keyword: "table management, table, column, relational metadata, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for tables. For what a table is, how columns and properties work,
the drop versus purge distinction, and how to work with tables in the UI, see [Tables and
Views](./tables-and-views.md). For creating the catalog and schema a table lives in, see [Manage
Catalogs and Schemas](./manage-catalogs-and-schemas.md). Views have their own page, [Manage View
Metadata](./manage-view-metadata-using-gravitino.md).

The examples below use a Hive catalog. Column types, table properties, and supported operations vary
by provider, and each catalog type documents its own: [Apache Hive](./apache-hive-catalog.md),
[MySQL](./jdbc-mysql-catalog.md), [PostgreSQL](./jdbc-postgresql-catalog.md), [Apache
Doris](./jdbc-doris-catalog.md), [StarRocks](./jdbc-starrocks-catalog.md),
[OceanBase](./jdbc-oceanbase-catalog.md), [Hologres](./jdbc-hologres-catalog.md),
[ClickHouse](./jdbc-clickhouse-catalog.md), [Apache Iceberg](./lakehouse-iceberg-catalog.md),
[Apache Paimon](./lakehouse-paimon-catalog.md), [Apache Hudi](./lakehouse-hudi-catalog.md), and
[Lakehouse generic](./lakehouse-generic-catalog.md).

## Table Operations

### Create a Table

A table needs a name and its columns. Partitioning, distribution, sort order, indexes, and
properties are all optional, and which of them a catalog accepts depends on the provider.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "customers",
  "comment": "Customer records",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "comment": "Primary key",
      "nullable": false,
      "autoIncrement": true
    },
    {
      "name": "name",
      "type": "varchar(500)",
      "comment": "Customer name",
      "nullable": true
    },
    {
      "name": "created_at",
      "type": "timestamp",
      "nullable": false,
      "defaultValue": {
        "type": "function",
        "funcName": "current_timestamp",
        "funcArgs": []
      }
    }
  ],
  "properties": {"format": "ORC"}
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("sales");

Column[] columns = new Column[] {
    Column.of("id", Types.IntegerType.get(), "Primary key", false, true, null),
    Column.of("name", Types.VarCharType.of(500), "Customer name"),
    Column.of("created_at", Types.TimestampType.withoutTimeZone(), null, false, false,
        FunctionExpression.of("current_timestamp"))
};

Table table = catalog.asTableCatalog().createTable(
    NameIdentifier.of("public", "customers"),
    columns,
    "Customer records",
    ImmutableMap.of("format", "ORC"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("sales")

columns = [
    Column.of("id", Types.IntegerType.get(), "Primary key", False, True, None),
    Column.of("name", Types.VarCharType.of(500), "Customer name"),
    Column.of("created_at", Types.TimestampType.without_time_zone(), None, False),
]

table = catalog.as_table_catalog().create_table(
    ident=NameIdentifier.of("public", "customers"),
    columns=columns,
    comment="Customer records",
    properties={"format": "ORC"})
```

</TabItem>
</Tabs>

For partitioning, distribution, sort order, and indexes, see [Table partitioning, distribution, sort
order, and indexes](./table-partitioning-distribution-sort-order-indexes.md).

### Load a Table

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/customers
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = catalog.asTableCatalog().loadTable(
    NameIdentifier.of("public", "customers"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = catalog.as_table_catalog().load_table(
    NameIdentifier.of("public", "customers"))
```

</TabItem>
</Tabs>

### Alter a Table

Changes are applied as a list in one request, and cover the table itself, its properties, and its
columns.

| Change                        | JSON                                                                                            | Java                                          |
|-------------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------|
| Rename the table              | `{"@type":"rename","newName":"table_renamed"}`                                                  | `TableChange.rename(...)`                     |
| Move to another schema        | `{"@type":"rename","newName":"table_renamed","newSchemaName":"new_schema"}`                     | `TableChange.rename(...)`                     |
| Update the comment            | `{"@type":"updateComment","newComment":"new_comment"}`                                          | `TableChange.updateComment(...)`              |
| Set a property                | `{"@type":"setProperty","property":"key1","value":"value1"}`                                    | `TableChange.setProperty(...)`                |
| Remove a property             | `{"@type":"removeProperty","property":"key1"}`                                                  | `TableChange.removeProperty(...)`             |
| Add a column                  | `{"@type":"addColumn","fieldName":["position"],"type":"varchar(20)","position":"FIRST"}`        | `TableChange.addColumn(...)`                  |
| Delete a column               | `{"@type":"deleteColumn","fieldName":["name"],"ifExists":true}`                                 | `TableChange.deleteColumn(...)`               |
| Rename a column               | `{"@type":"renameColumn","oldFieldName":["name_old"],"newFieldName":"name_new"}`                | `TableChange.renameColumn(...)`               |
| Update a column comment       | `{"@type":"updateColumnComment","fieldName":["name"],"newComment":"new comment"}`               | `TableChange.updateColumnComment(...)`        |
| Update a column type          | `{"@type":"updateColumnType","fieldName":["name"],"newType":"varchar(100)"}`                    | `TableChange.updateColumnType(...)`           |
| Update a column's nullability | `{"@type":"updateColumnNullability","fieldName":["name"],"nullable":true}`                      | `TableChange.updateColumnNullability(...)`    |
| Update a column position      | `{"@type":"updateColumnPosition","fieldName":["name"],"newPosition":"default"}`                 | `TableChange.updateColumnPosition(...)`       |
| Update a column default value | `{"@type":"updateColumnDefaultValue","fieldName":["name"],"newDefaultValue":{...}}`             | `TableChange.updateColumnDefaultValue(...)`   |

Not every provider accepts every change. Where one does not, the request is rejected rather than
silently ignored.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Customer records, curated"},
    {"@type": "addColumn", "fieldName": ["email"], "type": "varchar(320)", "nullable": true}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/customers
```

</TabItem>
<TabItem value="java" label="Java">

```java
Table table = catalog.asTableCatalog().alterTable(
    NameIdentifier.of("public", "customers"),
    TableChange.updateComment("Customer records, curated"),
    TableChange.addColumn(new String[] {"email"}, Types.VarCharType.of(320)));
```

</TabItem>
<TabItem value="python" label="Python">

```python
table = catalog.as_table_catalog().alter_table(
    NameIdentifier.of("public", "customers"),
    TableChange.update_comment("Customer records, curated"),
    TableChange.add_column(["email"], Types.VarCharType.of(320)))
```

</TabItem>
</Tabs>

### Drop or Purge a Table

Dropping removes the metadata, and for a managed table the underlying directory as well. For an
external table only the metadata goes. Purging removes the data completely and skips trash, is
rejected on external tables, and is not supported by every catalog.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  "http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables/customers?purge=false"
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = catalog.asTableCatalog().dropTable(
    NameIdentifier.of("public", "customers"));

boolean purged = catalog.asTableCatalog().purgeTable(
    NameIdentifier.of("public", "customers"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = catalog.as_table_catalog().drop_table(
    NameIdentifier.of("public", "customers"))

purged = catalog.as_table_catalog().purge_table(
    NameIdentifier.of("public", "customers"))
```

</TabItem>
</Tabs>

### List Tables

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = catalog.asTableCatalog().listTables(
    Namespace.of("public"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = catalog.as_table_catalog().list_tables(Namespace.of("public"))
```

</TabItem>
</Tabs>
