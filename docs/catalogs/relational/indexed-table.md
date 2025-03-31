---
title: "Indexed tables"
slug: /manage-indexed-tables
keyword: Table Index
license: This software is licensed under the Apache License version 2.
---

## Indexed tables

To define an indexed table, you should provide the following three parameters.

- <tt>indexType</tt> The type of index, such as *primary key* or *unique key*.
  For detailed list of supported index types, please check [index types](#index-types).
- <tt>name</tt>: The name of the index.
- <tt>fieldNames</tt>: The table fields Gravitino uses to index the table.

## Index types

<table>
<thead>
<tr>
  <th>Index type</th>
  <th>Description</th>
  <th>JSON</th>
  <th>Java</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>PRIMARY_KEY</tt></td>
  <td>
    A PRIMARY KEY is a column or set of columns that uniquely identifies each row in a table.
    It enforces uniqueness and ensures that no two rows have the same values in the specified columns.
    Additionally, the PRIMARY KEY constraint automatically creates a unique index on the specified columns.
  </td>
  <td>`PRIMARY_KEY`</td>
  <td>`IndexType.PRIMARY_KEY`</td>
</tr>
<tr>
  <td><tt>UNIQUE_KEY</tt></td>
  <td>
    The UNIQUE KEY constraint ensures that all values in a specified column
    or set of columns are unique across the entire table.
    Unlike the PRIMARY KEY constraint, a table can have multiple UNIQUE KEY constraints,
    allowing for unique values in multiple columns or sets of columns.
  </td>
  <td>`UNIQUE_KEY`</td>
  <td>`IndexType.UNIQUE_KEY`</td>
</tr>
</tbody>
</table>

The following snippets show how to define indice for an indexed table:

<Tabs groupId='language' queryString>
<TabItem value="Json" label="Json">

```json
 {
  "indexType": "PRIMARY_KEY",
  "name": "PRIMARY",
  "fieldNames": [["col_1"],["col_2"]]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"col_1"}, {"col_2"}});
```

</TabItem>
</Tabs>

## Indexed table operations

### Create an indexed table

The following is an example of creating an index table:

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
      "comment": "ID of the user"
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
  "indexes": [
    {
      "indexType": "PRIMARY_KEY",
      "name": "PRIMARY",
      "fieldNames": [["id"]]
    },
    {
      "indexType": "UNIQUE_KEY",
      "name": "name_age_score_uk",
      "fieldNames": [["name"],["age"],["score]]
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
      Column.of("id", Types.IntegerType.get(), "ID of the user", false, true, null),
      Column.of("name", Types.VarCharType.of(1000), "Name of the user", true, false, null),
      Column.of("age", Types.ShortType.get(), "Age of the user", true, false, null),
      Column.of("score", Types.DoubleType.get(), "Score of the user", true, false, null)
    },
    "Create a new Table",
    tablePropertiesMap,
    Transforms.EMPTY_TRANSFORM,
    Distributions.NONE,
    new SortOrder[0],
    new Index[] {
      Indexes.of(IndexType.PRIMARY_KEY, "PRIMARY", new String[][]{{"id"}}),
      Indexes.of(IndexType.UNIQUE_KEY, "name_age_score_uk", new String[][]{{"name"}, {"age"}, {"score"}})
    });
```
</TabItem>
</Tabs>


