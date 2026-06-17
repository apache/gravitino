---
title: "Manage View Metadata"
slug: "/manage-view-metadata-using-gravitino"
date: 2026-5-17
keyword: "Gravitino view metadata manage"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page introduces how to manage view metadata by Apache Gravitino. A view stores a logical
query definition rather than physical data. Through Gravitino, you can create, load, alter,
drop, and list views in supported catalogs via unified REST APIs or the Java client.

When a catalog implements view management, Gravitino operates on the provider's native view
metadata rather than a separate Gravitino-only view type. View capabilities are implemented by
each catalog provider and may differ in operation coverage and SQL dialect behavior. For detailed
support, see:

- [Hive view capabilities](./apache-hive-catalog.md#view-capabilities)
- [Iceberg view capabilities](./lakehouse-iceberg-catalog.md#view-capabilities)
- [Paimon view capabilities](./lakehouse-paimon-catalog.md#view-capabilities)

Unlike tables, views define query output and one or more representations, but do not manage
partitions, sort orders, indexes, or physical storage locations. The representation model is
extensible, but only SQL representations are supported.

To use view management, make sure that:

 - Gravitino server has started, and the host and port are [http://localhost:8090](http://localhost:8090).
 - A metalake has been created and [enabled](./manage-metalake-using-gravitino.md#enable-a-metalake).
 - A relational catalog has been created within the metalake.
 - A schema has been created within the catalog.

## View Operations

:::tip
Users should create a metalake, a catalog, and a schema, and ensure that the metalake and
catalog are enabled before operating views.
:::

### Create a View

Create a view by sending a `POST` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views` endpoint or just use the
Gravitino Java client. The following is an example of creating a view:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_view",
  "comment": "This is an example view",
  "columns": [
    {
      "name": "id",
      "type": "long",
      "comment": "id column",
      "nullable": true
    }
  ],
  "representations": [
    {
      "type": "sql",
      "dialect": "trino",
      "sql": "SELECT id FROM example_table"
    },
    {
      "type": "sql",
      "dialect": "spark",
      "sql": "SELECT id FROM example_table"
    }
  ],
  "defaultCatalog": "iceberg_catalog",
  "defaultSchema": "schema",
  "properties": {
    "key": "value"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas/schema/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
ViewCatalog viewCatalog = catalog.asViewCatalog();

View view = viewCatalog.createView(
    NameIdentifier.of("schema", "example_view"),
    "This is an example view",
    new Column[] {
        Column.of("id", Types.LongType.get(), "id column")
    },
    new Representation[] {
        SQLRepresentation.builder()
            .withSql("SELECT id FROM example_table")
            .withDialect("trino")
            .build(),
        SQLRepresentation.builder()
            .withSql("SELECT id FROM example_table")
            .withDialect("spark")
            .build()
    },
    "iceberg_catalog",
    "schema",
    ImmutableMap.of("key", "value")
);
// ...
```

</TabItem>
</Tabs>

:::caution
The provided example uses an Iceberg catalog and includes `trino` and `spark` SQL
representations for the same logical view. Supported dialects, representation counts, and
default-resolution fields depend on the catalog provider.
:::

#### Create Request Fields

Use the following fields when creating a view:

- `columns`: Defines the output schema of the view.
- `representations`: Provides one or more SQL definitions. The API uses the extensible
  `Representation` model, but only the `sql` type is supported, which maps to
  `SQLRepresentation` in Java.
- `defaultCatalog` and `defaultSchema`: Optionally define how unqualified identifiers in the SQL
  text are resolved for dialects that use them. For the `hive` and `flink` dialects in Hive
  catalogs, both fields must be `null`.
- `properties`: Carries provider-specific metadata.

Column types use the same Gravitino type system as table columns. For the full type list and the
behavior of special types such as external and unparsed types, refer to [Apache Gravitino table column type](./manage-relational-metadata-using-gravitino.md#table-column-type).

### Load a View

Load a view by sending a `GET` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/{view}` endpoint or just use
the Gravitino Java client. The following is an example of loading a view:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas/schema/views/example_view
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
ViewCatalog viewCatalog = catalog.asViewCatalog();
View view = viewCatalog.loadView(NameIdentifier.of("schema", "example_view"));
// ...
```

</TabItem>
</Tabs>

:::note
- Loading a view returns the metadata exposed by the underlying catalog, including output columns,
stored representations, default catalog, default schema, comment, and properties.
- If the provider already exposes a native view through its metadata API, Gravitino can load it
directly without a separate import step.
:::

### Alter a View

Modify a view by sending a `PUT` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/{view}` endpoint or just use
the Gravitino Java client. The following is an example of modifying a view:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "updates": [
    {
      "@type": "rename",
      "newName": "new_view_name"
    }
  ]
}' http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas/schema/views/example_view
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
ViewCatalog viewCatalog = catalog.asViewCatalog();
View view = viewCatalog.alterView(
    NameIdentifier.of("schema", "example_view"),
    ViewChange.rename("new_view_name"),
    ViewChange.setProperty("key", "value")
);
// ...
```

</TabItem>
</Tabs>

Gravitino supports the following changes to a view:

| Supported modification                                 | REST / JSON                                                                                                    | Java                                                                                                  |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Rename view                                            | `{"@type":"rename","newName":"new_view_name"}`                                                          | `ViewChange.rename("new_view_name")`                                                                 |
| Set a view property                                    | `{"@type":"setProperty","property":"key","value":"value"}`                                           | `ViewChange.setProperty("key", "value")`                                                           |
| Remove a view property                                 | `{"@type":"removeProperty","property":"key"}`                                                            | `ViewChange.removeProperty("key")`                                                                   |
| Replace view definition (columns/representations/etc.) | Replace columns, representations, default catalog, default schema, and comment in one update request.         | `ViewChange.replaceView(columns, representations, defaultCatalog, defaultSchema, comment)`            |

`ViewChange.replaceView(...)` performs a full replacement of the view body. If you only want to
change one part of the body, load the current view first and pass the unchanged fields back in the
replacement request.

### Drop a View

Remove a view by sending a `DELETE` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views/{view}` endpoint or just use
the Gravitino Java client. The following is an example of dropping a view:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas/schema/views/example_view
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
ViewCatalog viewCatalog = catalog.asViewCatalog();
viewCatalog.dropView(NameIdentifier.of("schema", "example_view"));
// ...
```

</TabItem>
</Tabs>

Dropping a view removes the view metadata definition only. It does not remove underlying table
data because a view does not own storage.

### List All Views Under a Schema

List all views in a schema by sending a `GET` request to the
`/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views` endpoint or just use the
Gravitino Java client. The following is an example of listing all the views in a schema:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" \
http://localhost:8090/api/metalakes/metalake/catalogs/iceberg_catalog/schemas/schema/views
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created an Iceberg catalog named `iceberg_catalog`
Catalog catalog = gravitinoClient.loadCatalog("iceberg_catalog");
ViewCatalog viewCatalog = catalog.asViewCatalog();
NameIdentifier[] identifiers = viewCatalog.listViews(Namespace.of("schema"));
// ...
```

</TabItem>
</Tabs>