---
title: "Manage View Metadata"
slug: "/manage-view-metadata-using-gravitino"
keyword: "view management, view, SQL view, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for views. For how views relate to tables, see
[Tables and Views](./tables-and-views.md). The [Hive](./apache-hive-catalog.md#view-capabilities),
[Iceberg](./lakehouse-iceberg-catalog.md#view-capabilities), and
[Paimon](./lakehouse-paimon-catalog.md#view-capabilities) catalog guides describe which SQL dialects
and defaults each catalog supports.

To follow the examples, register the [`iceberg_rest` catalog](./lakehouse-iceberg-catalog.md#rest-catalog-backend)
in the metalake named `metalake` using an Iceberg REST backend that supports views. Then
[create a schema](./manage-catalogs-and-schemas.md#create-a-schema) named `db` in that catalog.

## View Operations

For the client examples, connect the [Java client](./how-to-use-gravitino-client.md) or
[Python client](./how-to-use-python-client.md) to the same metalake.

### Create a View

A view declares its output columns and one or more SQL representations, each with its own dialect.
The `customer_ids` view below supplies the query for both Spark and Trino. Its default catalog and
schema tell each engine where to resolve the unqualified table name `customers`.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "customer_ids",
  "comment": "Customer identifiers",
  "columns": [{"name": "id", "type": "integer", "nullable": true}],
  "representations": [
    {"type": "sql", "dialect": "spark", "sql": "SELECT id FROM customers"},
    {"type": "sql", "dialect": "trino", "sql": "SELECT id FROM customers"}
  ],
  "defaultCatalog": "iceberg_rest",
  "defaultSchema": "db"
}' http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_rest/schemas/db/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("iceberg_rest");
ViewCatalog views = catalog.asViewCatalog();
NameIdentifier viewId = NameIdentifier.of("db", "customer_ids");
Column[] columns = {Column.of("id", Types.IntegerType.get())};
String sql = "SELECT id FROM customers";
Representation[] representations = {
    SQLRepresentation.builder().withDialect(Dialects.SPARK).withSql(sql).build(),
    SQLRepresentation.builder().withDialect(Dialects.TRINO).withSql(sql).build()
};

View view = views.createView(
    viewId,
    "Customer identifiers",
    columns,
    representations,
    "iceberg_rest",
    "db",
    Collections.emptyMap());
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("iceberg_rest")
views = catalog.as_view_catalog()
view_id = NameIdentifier.of("db", "customer_ids")
sql = "SELECT id FROM customers"

view = views.create_view(
    view_id,
    columns=[Column.of("id", Types.IntegerType.get())],
    representations=[
        SQLRepresentation(Dialects.SPARK, sql),
        SQLRepresentation(Dialects.TRINO, sql),
    ],
    comment="Customer identifiers",
    default_catalog="iceberg_rest",
    default_schema="db",
)
```

</TabItem>
</Tabs>

Gravitino stores the definition and does not execute it, so whether the query resolves is a question
for the engine reading the view.

### Load a View

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_rest/schemas/db/views/customer_ids
```

</TabItem>
<TabItem value="java" label="Java">

```java
View current = views.loadView(viewId);
```

</TabItem>
<TabItem value="python" label="Python">

```python
current = views.load_view(view_id)
```

</TabItem>
</Tabs>

### Alter a View

| Change             | JSON                                                         | Java                                       | Python                                     |
|--------------------|--------------------------------------------------------------|--------------------------------------------|--------------------------------------------|
| Rename             | `{"@type":"rename","newName":"view_renamed"}`                | `ViewChange.rename("view_renamed")`        | `ViewChange.rename("view_renamed")`        |
| Set a property     | `{"@type":"setProperty","property":"key1","value":"value1"}` | `ViewChange.setProperty("key1", "value1")` | `ViewChange.set_property("key1", "value1")` |
| Remove a property  | `{"@type":"removeProperty","property":"key1"}`               | `ViewChange.removeProperty("key1")`        | `ViewChange.remove_property("key1")`       |
| Replace a view     | `{"@type":"replaceView",...}`                                | `ViewChange.replaceView(...)`              | `ViewChange.replace_view(...)`             |

`replaceView` replaces the columns, SQL representations, default catalog, default schema, and
comment together. To change just the comment, pass the other values from the view you loaded.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "replaceView",
      "columns": [{"name": "id", "type": "integer", "nullable": true}],
      "representations": [
        {"type": "sql", "dialect": "spark", "sql": "SELECT id FROM customers"},
        {"type": "sql", "dialect": "trino", "sql": "SELECT id FROM customers"}
      ],
      "defaultCatalog": "iceberg_rest",
      "defaultSchema": "db",
      "comment": "Customer IDs for reporting"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_rest/schemas/db/views/customer_ids
```

</TabItem>
<TabItem value="java" label="Java">

```java
View updated = views.alterView(
    viewId,
    ViewChange.replaceView(
        current.columns(),
        current.representations(),
        current.defaultCatalog(),
        current.defaultSchema(),
        "Customer IDs for reporting"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
updated = views.alter_view(
    view_id,
    ViewChange.replace_view(
        columns=current.columns(),
        representations=current.representations(),
        default_catalog=current.default_catalog(),
        default_schema=current.default_schema(),
        comment="Customer IDs for reporting",
    ),
)
```

</TabItem>
</Tabs>

### Drop a View

Dropping a view removes the definition. The tables it reads are untouched.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_rest/schemas/db/views/customer_ids
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = views.dropView(viewId);
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = views.drop_view(view_id)
```

</TabItem>
</Tabs>

### List Views

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_rest/schemas/db/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = views.listViews(Namespace.of("db"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = views.list_views(Namespace.of("db"))
```

</TabItem>
</Tabs>
