---
title: "Table partitioning, distribution and sort ordering and indexes"
slug: /table-partitioning-distribution-sort-order-indexes
date: 2023-12-25
keyword: Table Partition Bucket Distribute Sort By
license: This software is licensed under the Apache License version 2.
last_update:
  date: 2024-02-02
  author: Clearvive
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Table partitioning

To create a partitioned table, you should provide the following two components to construct a valid partitioned table.

- Partitioning strategy. It defines how Gravitino distributes table data across partitions. Currently, Gravitino supports the following partitioning strategies.

:::note
The `score`, `createTime`, and `city` appearing in the table below refer to the field names in a table.
:::

| Partitioning strategy | Description                                                    | JSON example                                                                                            | Java example                                                                        | Equivalent SQL semantics              |
|-----------------------|----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|---------------------------------------|
| `identity`            | Source value, unmodified.                                      | `{"strategy":"identity","fieldName":["score"]}`                                                         | `Transforms.identity("score")`                                                      | `PARTITION BY score`                  |
| `hour`                | Extract a timestamp hour, as hours from '1970-01-01 00:00:00'. | `{"strategy":"hour","fieldName":["createTime"]}`                                                        | `Transforms.hour("createTime")`                                                     | `PARTITION BY hour(createTime)`       |
| `day`                 | Extract a date or timestamp day, as days from '1970-01-01'.    | `{"strategy":"day","fieldName":["createTime"]}`                                                         | `Transforms.day("createTime")`                                                      | `PARTITION BY day(createTime)`        |
| `month`               | Extract a date or timestamp month, as months from '1970-01-01' | `{"strategy":"month","fieldName":["createTime"]}`                                                       | `Transforms.month("createTime")`                                                    | `PARTITION BY month(createTime)`      |
| `year`                | Extract a date or timestamp year, as years from 1970.          | `{"strategy":"year","fieldName":["createTime"]}`                                                        | `Transforms.year("createTime")`                                                     | `PARTITION BY year(createTime)`       |
| `bucket[N]`           | Hash of value, mod N.                                          | `{"strategy":"bucket","numBuckets":10,"fieldNames":[["score"]]}`                                        | `Transforms.bucket(10, "score")`                                                    | `PARTITION BY bucket(10, score)`      |
| `truncate[W]`         | Value truncated to width W.                                    | `{"strategy":"truncate","width":20,"fieldName":["score"]}`                                              | `Transforms.truncate(20, "score")`                                                  | `PARTITION BY truncate(20, score)`    |
| `list`                | Partition the table by a list value.                           | `{"strategy":"list","fieldNames":[["createTime"],["city"]]}`                                            | `Transforms.list(new String[] {"createTime", "city"})`                              | `PARTITION BY list(createTime, city)` |
| `range`               | Partition the table by a range value.                          | `{"strategy":"range","fieldName":["createTime"]}`                                                       | `Transforms.range("createTime")`                                                    | `PARTITION BY range(createTime)`      |
| `function`            | Partition the table by function expression.                    | `{"strategy":"function","funcName":"toYYYYMM","funcArgs":[{"type":"field","fieldName":["VisitDate"]}]}` | `Transforms.apply("toYYYYMM", new Expression[]{NamedReference.field("VisitDate")})` | `PARTITION BY toYYYYMM(VisitDate)`    |

:::note
For function partitioning, you should provide the function name and the function arguments. The function arguments must be an [expression](./expression.md).
:::

- Field names: It defines which fields Gravitino uses to partition the table.

- In some cases, you require other information. For example, if the partitioning strategy is `bucket`, you should provide the number of buckets; if the partitioning strategy is `truncate`, you should provide the width of the truncate.

Once a partitioned table is created, you can [manage its partitions using Gravitino](./manage-table-partition-using-gravitino.md).

## Table distribution

To create a distribution(bucketed) table, you should use the following three components to construct a valid bucketed table.

- Strategy. It defines how Gravitino distributes table data across partitions.

| Distribution strategy | Description                                                                                                                                                                    | JSON    | Java             |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| hash                  | Distribution table using hash. Gravitino distributes table data into buckets based on the hash value of the key.                                                               | `hash`  | `Strategy.HASH`  |
| range                 | Distribution table using range. Gravitino distributes table data into buckets based on a specified range or interval of values.                                                | `range` | `Strategy.RANGE` |
| even                  | Distribution table using even. Gravitino distributes table data, ensuring an equal distribution of data. Currently we use `even` to implementation Doris `random` distribution | `even`  | `Strategy.EVEN`  |

- number. It defines how many buckets you use to distribution the table.
- funcArgs. It defines the arguments of the strategy, the argument must be an [expression](./expression.md).

<Tabs groupId='language' queryString>
<TabItem value="Json" label="Json">

```json
{
  "strategy": "hash",
  "number": 4,
  "funcArgs": [
    {
      "type": "field",
      "fieldName": ["score"]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Distributions.of(Strategy.HASH, 4, NamedReference.field("score"));

// if you want to use auto distribution, you can use the following code, it will set the number is -1.
// Auto distribution with strategy and fields
Distributions.auto(Strategy.HASH, NamedReference.field("score"));
```
</TabItem>

</Tabs>

## Sort ordering

To define a sorted order table, you should use the following three components to construct a valid sorted order table.

- Direction. It defines in which direction Gravitino sorts the table. The default value is `ascending`.

| Direction  | Description                                 | JSON   | Java                       |
|------------|---------------------------------------------|--------|----------------------------|
| ascending  | Sorted by a field or a function ascending.  | `asc`  | `SortDirection.ASCENDING`  |
| descending | Sorted by a field or a function descending. | `desc` | `SortDirection.DESCENDING` |

- Null ordering. It describes how to handle null values when ordering

| Null ordering Type | Description                             | JSON          | Java                       |
|--------------------|-----------------------------------------|---------------|----------------------------|
| null_first         | Puts the null value in the first place. | `nulls_first` | `NullOrdering.NULLS_FIRST` |
| null_last          | Puts the null value in the last place.  | `nulls_last`  | `NullOrdering.NULLS_LAST`  |

Note: If the direction value is `ascending`, the default ordering value is `nulls_first` and if the direction value is `descending`, the default ordering value is `nulls_last`.

- sortTerm. It shows which field or function Gravitino uses to sort the table, must be an [expression](./expression.md).

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


:::tip
**Not all catalogs may support those features**. Please refer to the related document for more details.
:::

The following is an example of creating a partitioned, bucketed table, and sorted order table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "table",
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
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
tableCatalog.createTable(
    NameIdentifier.of("schema", "table"),
    new Column[] {
      Column.of("id", Types.IntegerType.get(), "Id of the user", true, false, null),
      Column.of("name", Types.VarCharType.of(2000), "Name of the user", true, false, null),
      Column.of("age", Types.ShortType.get(), "Age of the user", true, false, null),
      Column.of("score", Types.DoubleType.get(), "Score of the user", false, false, null)
    },
    "Create a new Table",
    tablePropertiesMap,
    new Transform[] {
    // Partition by id
      Transforms.identity("score")
    },
    // CLUSTERED BY id
    Distributions.of(Strategy.HASH, 4, NamedReference.field("id")),
    // SORTED BY name asc
    new SortOrder[] {
      SortOrders.of(
        NamedReference.field("age"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
    });
```

</TabItem>
</Tabs>

## Indexes

To define an indexed table, you should utilize the following three components to construct a valid indexed table.

- IndexType. Represents the type of index, such as primary key or unique key.

| IndexType     | Description                                                                                                                                                                                                                                                                                            | JSON            | Java                      |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------------------------|
| PRIMARY_KEY   | The PRIMARY KEY is a column or set of columns that uniquely identifies each row in a table. It enforces uniqueness and ensures that no two rows have the same values in the specified columns. Additionally, the PRIMARY KEY constraint automatically creates a unique index on the specified columns. | `PRIMARY_KEY`   | `IndexType.PRIMARY_KEY`   |
| UNIQUE_KEY    | The UNIQUE KEY constraint ensures that all values in a specified column or set of columns are unique across the entire table. Unlike the PRIMARY KEY constraint, a table can have multiple UNIQUE KEY constraints, allowing for unique values in multiple columns or sets of columns.                  | `UNIQUE_KEY`    | `IndexType.UNIQUE_KEY`    |

- Name. It defines the name of the index.

- FieldNames. It defines which table fields Gravitino uses to index the table.

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

The following is an example of creating an index table:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "table",
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
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/tables
```

</TabItem>
<TabItem value="java" label="Java">

```java
tableCatalog.createTable(
    NameIdentifier.of("schema", "table"),
    new Column[] {
      Column.of("id", Types.IntegerType.get(), "Id of the user", false, true, null),
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
