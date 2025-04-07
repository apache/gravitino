---
title: "Sorted tables"
slug: /manage-sorted-tables
keyword: Table sort order
license: This software is licensed under the Apache License version 2.
---

## Sort ordering

To define a sorted table, you will provide the following three parameters:

- <tt>sortTerm</tt>. The field or function Gravitino uses to sort the table.
  The parameter value  must be a valid [expression](../../metadata/expression.md).

- <tt>direction</tt>: The direction in which Gravitino sorts the table.
  See [sort directions](#sort-directions) for details. The default value is `ascending`.

- <tt>nullOrder</tt>: How to handle null values when ordering.
  See [null ordering](#null-ordering) for details.

### Defining table sort orders

<Tabs groupId='language' queryString>
<TabItem value="Json" label="Json">

```json
 {
  "direction": "asc",
  "nullOrder": "NULLS_LAST",
  "sortTerm":  {
    "type": "field",
    "fieldName": ["score"]
  }
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
SortOrders.of(NamedReference.field("score"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST);
```
</TabItem>
</Tabs>

### Sort directions

<table>
<thead>
<tr>
  <td>Direction</td>
  <td>Description</td>
  <td>JSON</td>
  <td>Java</td>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>ascending</tt></td>
  <td>Sort by a field or a function, ascending.</td>
  <td>`asc`</td>
  <td>`SortDirection.ASCENDING`</td>
</tr>
<tr>
  <td><tt>descending</tt></td>
  <td>Sort by a field or a function, descending.</td>
  <td>`desc`</td>
  <td>`SortDirection.DESCENDING`</td>
</tr>
</tbody>
</table>

### Null ordering

<table>
<thead>
<tr>
  <td>Null ordering type</td>
  <td>Description</td>
  <td>JSON</td>
  <td>Java</td>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>null_first</tt></td>
  <td>Puts the null value ahead of other values.</td>
  <td>`nulls_first`</td>
  <td>`NullOrdering.NULLS_FIRST`</td>
</tr>
<tr>
  <td><tt>null_last</tt></td>
  <td>Puts the null value behind all other values.</td>
  <td>`nulls_last`</td>
  <td>`NullOrdering.NULLS_LAST`</td>
</tr>
</tbody>
</table>

:::note
- If the <tt>direction</tt> is `ascending`, the default ordering value is `nulls_first`.
- If the <tt>direction</tt> is `descending`, the default ordering value is `nulls_last`.
:::

## Sorted table operations


:::tip
Not all catalogs may support those features.
Please refer to the related catalog documentation for more details.
:::

### Create a sorted table

The following example creates a [partitioned](./partitioned-table.md), bucketed table, and sorted order table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >table.json
{
  "name": "mytable",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": true,
      "comment": "Id of the user"
    },
    {
      "name": "name",
      "type": "varchar(2000)",
      "nullable": true,
      "comment": "Name of the user"
    },
    {
      "name": "age",
      "type": "short",
      "nullable": true,
      "comment": "Age of the user"
    },
    {
      "name": "score",
      "type": "double",
      "nullable": true,
      "comment": "Score of the user"
    }
  ],
  "comment": "Create a new Table",
  "properties": {
    "format": "ORC"
  },
  "partitioning": [
    {
      "strategy": "identity",
      "fieldName": ["score"]
    }
  ],
  "distribution": {
    "strategy": "hash",
    "number": 4,
    "funcArgs": [
      {
        "type": "field",
        "fieldName": ["score"]
      }
    ]
  },
  "sortOrders": [
    {
      "direction": "asc",
      "nullOrder": "NULLS_LAST",
      "sortTerm":  {
        "type": "field",
        "fieldName": ["name"]
      }
    }
  ]
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@table.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
tableCatalog.createTable(
    NameIdentifier.of("myschema", "mytable"),
    new Column[] {
        Column.of("id", Types.IntegerType.get(), "Id of the user", true, false, null),
        Column.of("name", Types.VarCharType.of(2000), "Name of the user", true, false, null),
        Column.of("age", Types.ShortType.get(), "Age of the user", true, false, null),
        Column.of("score", Types.DoubleType.get(), "Score of the user", false, false, null)
    },
    "My new sorted, partitioned table",
    tablePropertiesMap,
    new Transform[] {
        // Partition by score
        Transforms.identity("score")
    },
    // Clusterd by ID
    Distributions.of(Strategy.HASH, 4, NamedReference.field("id")),
    // Sorted by name in ascending order
    new SortOrder[] {
        SortOrders.of(
            NamedReference.field("age"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST
        ),
    });
```

</TabItem>
</Tabs>

