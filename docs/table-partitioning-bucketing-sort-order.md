---
title: "Table partitioning, bucketing and sort ordering"
slug: /table-partitioning-bucketing-sort-order
date: 2023-12-25
keyword: Table Partition Bucket Distribute Sort By
license: Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Table partitioning

To create a partitioned table, you should provide the following two components to construct a valid partitioned table.

- Partitioning strategy. It defines how Gravitino distributes table data across partitions. Currently, Gravitino supports the following partitioning strategies.

:::note
The `score`, `createTime`, and `city` appearing in the table below refer to the field names in a table.
:::

| Partitioning strategy | Description                                                    | JSON example                                                     | Java example                                           | Equivalent SQL semantics              |
|-----------------------|----------------------------------------------------------------|------------------------------------------------------------------|--------------------------------------------------------|---------------------------------------|
| `identity`            | Source value, unmodified.                                      | `{"strategy":"identity","fieldName":["score"]}`                  | `Transforms.identity("score")`                         | `PARTITION BY score`                  |
| `hour`                | Extract a timestamp hour, as hours from '1970-01-01 00:00:00'. | `{"strategy":"hour","fieldName":["createTime"]}`                 | `Transforms.hour("createTime")`                        | `PARTITION BY hour(createTime)`       |
| `day`                 | Extract a date or timestamp day, as days from '1970-01-01'.    | `{"strategy":"day","fieldName":["createTime"]}`                  | `Transforms.day("createTime")`                         | `PARTITION BY day(createTime)`        |
| `month`               | Extract a date or timestamp month, as months from '1970-01-01' | `{"strategy":"month","fieldName":["createTime"]}`                | `Transforms.month("createTime")`                       | `PARTITION BY month(createTime)`      |
| `year`                | Extract a date or timestamp year, as years from 1970.          | `{"strategy":"year","fieldName":["createTime"]}`                 | `Transforms.year("createTime")`                        | `PARTITION BY year(createTime)`       |
| `bucket[N]`           | Hash of value, mod N.                                          | `{"strategy":"bucket","numBuckets":10,"fieldNames":[["score"]]}` | `Transforms.bucket(10, "score")`                       | `PARTITION BY bucket(10, score)`      |
| `truncate[W]`         | Value truncated to width W.                                    | `{"strategy":"truncate","width":20,"fieldName":["score"]}`       | `Transforms.truncate(20, "score")`                     | `PARTITION BY truncate(20, score)`    |
| `list`                | Partition the table by a list value.                           | `{"strategy":"list","fieldNames":[["createTime"],["city"]]}`     | `Transforms.list(new String[] {"createTime", "city"})` | `PARTITION BY list(createTime, city)` |
| `range`               | Partition the table by a range value.                          | `{"strategy":"range","fieldName":["createTime"]}`                | `Transforms.range("createTime")`                       | `PARTITION BY range(createTime)`      |

As well as the strategies mentioned before, you can use other functions strategies to partition the table, for example, the strategy can be `{"strategy":"functionName","fieldName":["score"]}`. The `functionName` can be any function name that you can use in SQL, for example, `{"strategy":"toDate","fieldName":["createTime"]}` is equivalent to `PARTITION BY toDate(createTime)` in SQL.
For complex functions, please refer to [FunctionPartitioningDTO](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/partitions/FunctionPartitioningDTO.java).

- Field names: It defines which fields Gravitino uses to partition the table.

- In some cases, you require other information. For example, if the partitioning strategy is `bucket`, you should provide the number of buckets; if the partitioning strategy is `truncate`, you should provide the width of the truncate.

The following is an example of creating a partitioned table:

<Tabs>
<TabItem value="Json" label="Json">

```json
[
  {
    "strategy": "identity",
    "fieldName": [
      "score"
    ]
  }
]
```

</TabItem>
<TabItem value="java" label="Java">

```java
new Transform[] {
    // Partition by score
    Transforms.identity("score")
    }
```

</TabItem>
</Tabs>


## Table bucketing

To create a bucketed table, you should use the following three components to construct a valid bucketed table.

- Strategy. It defines how Gravitino distributes table data across partitions.

| Bucket strategy | Description                                                                                                                   | JSON     | Java             |
|-----------------|-------------------------------------------------------------------------------------------------------------------------------|----------|------------------|
| hash            | Bucket table using hash. Gravitino distributes table data into buckets based on the hash value of the key.                | `hash`   | `Strategy.HASH`  |
| range           | Bucket table using range. Gravitino distributes table data into buckets based on a specified range or interval of values. | `range`  | `Strategy.RANGE` |
| even            | Bucket table using even. Gravitino distributes table data, ensuring an equal distribution of data.                       | `even`   | `Strategy.EVEN`  |

- Number. It defines how many buckets you use to bucket the table.
- Function arguments. It defines the arguments of the strategy, Gravitino supports the following three kinds of arguments, for more, you can refer to Java class [FunctionArg](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/expressions/FunctionArg.java) and [DistributionDTO](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/DistributionDTO.java) to use more complex function arguments.

| Expression type | JSON example                                                   | Java example                                                                              | Equivalent SQL semantics | Description                       |
|-----------------|----------------------------------------------------------------|-------------------------------------------------------------------------------------------|--------------------------|-----------------------------------|
| field           | `{"type":"field","fieldName":["score"]}`                       | `FieldReferenceDTO.of("score")`                                                           | `score`                  | The field reference value `score` |
| function        | `{"type":"function","functionName":"hour","fieldName":["dt"]}` | `new FuncExpressionDTO.Builder().withFunctionName("hour").withFunctionArgs("dt").build()` | `hour(dt)`               | The function value `hour(dt)`     |
| constant        | `{"type":"literal","value":10, "dataType": "integer"}`         | `new LiteralDTO.Builder().withValue("10").withDataType(Types.IntegerType.get()).build()`  | `10`                     | The integer literal `10`          |


<Tabs>
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
```

</TabItem>
</Tabs>


## Sort ordering

To define a sorted order table, you should use the following three components to construct a valid sorted order table.

- Direction. It defines in which direction Gravitino sorts the table. The default value is `ascending`.

| Direction  | Description                                 | JSON   | Java                       |
|------------|---------------------------------------------| ------ | -------------------------- |
| ascending  | Sorted by a field or a function ascending.  | `asc`  | `SortDirection.ASCENDING`  |
| descending | Sorted by a field or a function descending. | `desc` | `SortDirection.DESCENDING` |

- Null ordering. It describes how to handle null values when ordering

| Null ordering Type | Description                             | JSON          | Java                       |
|--------------------|-----------------------------------------| ------------- | -------------------------- |
| null_first         | Puts the null value in the first place. | `nulls_first` | `NullOrdering.NULLS_FIRST` |
| null_last          | Puts the null value in the last place.  | `nulls_last`  | `NullOrdering.NULLS_LAST`  |

Note: If the direction value is `ascending`, the default ordering value is `nulls_first` and if the direction value is `descending`, the default ordering value is `nulls_last`.

- Sort term. It shows which field or function Gravitino uses to sort the table, please refer to the `Function arguments` in the table bucketing section.

<Tabs>
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
SortOrders.of(FieldReferenceDTO.of("score"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST);
```

</TabItem>
</Tabs>


:::tip
**Not all catalogs may support those features.**. Please refer to the related document for more details.
:::

The following is an example of creating a partitioned, bucketed table, and sorted order table:

<Tabs>
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
    NameIdentifier.of("metalake", "hive_catalog", "schema", "table"),
    new ColumnDTO[] {
      ColumnDTO.builder()
        .withComment("Id of the user")
        .withName("id")
        .withDataType(Types.IntegerType.get())
        .withNullable(true)
      .build(),
      ColumnDTO.builder()
        .withComment("Name of the user")
        .withName("name")
        .withDataType(Types.VarCharType.of(1000))
        .withNullable(true)
        .build(),
      ColumnDTO.builder()
        .withComment("Age of the user")
        .withName("age")
        .withDataType(Types.ShortType.get())
        .withNullable(true)
        .build(),
      ColumnDTO.builder()
        .withComment("Score of the user")
        .withName("score")
        .withDataType(Types.DoubleType.get())
        .withNullable(true)
        .build(),
    },
    "Create a new Table",
    tablePropertiesMap,
    new Transform[] {
    // Partition by id
      Transforms.identity("score")
    },
    // CLUSTERED BY id
    Distributions.of(Strategy.HASH, 4, NamedReference.field("id")),,
    // SORTED BY name asc
    new SortOrder[] {
      SortOrders.of(
        NamedReference.field("age"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST),
    });
```

</TabItem>
</Tabs>
