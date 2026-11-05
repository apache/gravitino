---
title: "Manage View Metadata"
slug: "/manage-view-metadata-using-gravitino"
keyword: "view management, view, SQL view, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for views. For what a view is, which catalogs support them, and
how they relate to tables, see [Tables and Views](./tables-and-views.md). For creating the catalog
and schema a view lives in, see [Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).

Views are supported by the Hive, Iceberg, and Paimon catalogs.

## View Operations

### Create a View

A view carries its columns and one or more representations, each holding a query and the dialect it
is written in. SQL is the only representation type today. A default catalog and schema can be set so
unqualified names in the query resolve.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "active_customers",
  "comment": "Customers with orders in the last year",
  "query": "SELECT * FROM customers WHERE last_order_at > current_date - interval 365 day",
  "dialect": "spark",
  "columns": [
    {"name": "id", "type": "integer", "nullable": false},
    {"name": "name", "type": "varchar(500)", "nullable": true}
  ],
  "properties": {}
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = client.loadCatalog("sales");
ViewCatalog views = catalog.asViewCatalog();

Column[] columns = new Column[] {
    Column.of("id", Types.IntegerType.get(), null, false, false, null),
    Column.of("name", Types.VarCharType.of(500))
};

Representation[] representations = new Representation[] {
    SQLRepresentation.builder()
        .withDialect("spark")
        .withSql("SELECT * FROM customers "
            + "WHERE last_order_at > current_date - interval 365 day")
        .build()
};

View view = views.createView(
    NameIdentifier.of("public", "active_customers"),
    "Customers with orders in the last year",
    columns,
    representations,
    "sales",
    "public",
    ImmutableMap.of());
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog = client.load_catalog("sales")
views = catalog.as_view_catalog()

columns = [
    Column.of("id", Types.IntegerType.get(), nullable=False),
    Column.of("name", Types.VarCharType.of(500)),
]

representations = [
    SQLRepresentation(
        Dialects.SPARK,
        "SELECT * FROM customers WHERE last_order_at > current_date - interval 365 day",
    )
]

view = views.create_view(
    NameIdentifier.of("public", "active_customers"),
    columns=columns,
    representations=representations,
    comment="Customers with orders in the last year",
    default_catalog="sales",
    default_schema="public",
    properties={},
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
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/views/active_customers
```

</TabItem>
<TabItem value="java" label="Java">

```java
View view = views.loadView(NameIdentifier.of("public", "active_customers"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
view = views.load_view(NameIdentifier.of("public", "active_customers"))
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

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "setProperty", "property": "key1", "value": "value1"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/views/active_customers
```

</TabItem>
<TabItem value="java" label="Java">

```java
View view = views.alterView(
    NameIdentifier.of("public", "active_customers"),
    ViewChange.setProperty("key1", "value1"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
view = views.alter_view(
    NameIdentifier.of("public", "active_customers"),
    ViewChange.set_property("key1", "value1"),
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
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/views/active_customers
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = views.dropView(NameIdentifier.of("public", "active_customers"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = views.drop_view(NameIdentifier.of("public", "active_customers"))
```

</TabItem>
</Tabs>

### List Views

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/public/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = views.listViews(Namespace.of("public"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = views.list_views(Namespace.of("public"))
```

</TabItem>
</Tabs>
