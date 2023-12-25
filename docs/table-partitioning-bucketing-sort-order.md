

## Partitioned table

Currently, Gravitino supports the following partitioning strategies:

:::note
The `score`, `dt`, and `city` appearing in the table below refer to the field names in a table.
:::

| Partitioning strategy | Description                                                  | Json example                                                     | Java example                                    | Equivalent SQL semantics           |
|-----------------------|--------------------------------------------------------------| ---------------------------------------------------------------- | ----------------------------------------------- |-----------------------------------|
| `identity`            | Source value, unmodified                                     | `{"strategy":"identity","fieldName":["score"]}`                  | `Transforms.identity("score")`                  | `PARTITION BY score`               |
| `hour`                | Extract a timestamp hour, as hours from 1970-01-01 00:00:00  | `{"strategy":"hour","fieldName":["score"]}`                      | `Transforms.hour("score")`                      | `PARTITION BY hour(score)`         |
| `day`                 | Extract a date or timestamp day, as days from 1970-01-01     | `{"strategy":"day","fieldName":["score"]}`                       | `Transforms.day("score")`                       | `PARTITION BY day(score)`          |
| `month`               | Extract a date or timestamp month, as months from 1970-01-01 | `{"strategy":"month","fieldName":["score"]}`                     | `Transforms.month("score")`                     | `PARTITION BY month(score)`        |
| `year`                | Extract a date or timestamp year, as years from 1970         | `{"strategy":"year","fieldName":["score"]}`                      | `Transforms.year("score")`                      | `PARTITION BY year(score)`         |
| `bucket[N]`           | Hash of value, mod N                                         | `{"strategy":"bucket","numBuckets":10,"fieldNames":[["score"]]}` | `Transforms.bucket(10, "score")`                | `PARTITION BY bucket(10, score)`   |
| `truncate[W]`         | Value truncated to width W                                   | `{"strategy":"truncate","width":20,"fieldName":["score"]}`       | `Transforms.truncate(20, "score")`              | `PARTITION BY truncate(20, score)` |
| `list`                | Partition the table by a list value                          | `{"strategy":"list","fieldNames":[["dt"],["city"]]}`             | `Transforms.list(new String[] {"dt", "city"})`  | `PARTITION BY list(dt, city)`      |
| `range`               | Partition the table by a range value                         | `{"strategy":"range","fieldName":["dt"]}`                        | `Transforms.range(20, "score")`                 | `PARTITION BY range(score)`        |

Except the strategies above, you can use other functions strategies to partition the table, for example, the strategy can be `{"strategy":"functionName","fieldName":["score"]}`. The `functionName` can be any function name that you can use in SQL, for example, `{"strategy":"toDate","fieldName":["score"]}` is equivalent to `PARTITION BY toDate(score)` in SQL.
For complex function, please refer to [FunctionPartitioningDTO](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/partitions/FunctionPartitioningDTO.java).

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


## Bucketed table

- Strategy. It defines how your table data is distributed across partitions.

| Bucket strategy | Description                                                                                                          | Json     | Java             |
|-----------------|----------------------------------------------------------------------------------------------------------------------|----------|------------------|
| hash            | Bucket table using hash. The data will be distributed into buckets based on the hash value of the key.               | `hash`   | `Strategy.HASH`  |
| range           | Bucket table using range. The data will be divided into buckets based on a specified range or interval of values.    | `range`  | `Strategy.RANGE` |
| even            | Bucket table using even. The data will be evenly distributed into buckets, ensuring an equal distribution of data.   | `even`   | `Strategy.EVEN`  |

- Number. It defines how many buckets you use to bucket the table.
- Function arguments. It defines the arguments of the strategy above, Gravitino supports the following three kinds of arguments, for more, you can refer to Java class [FunctionArg](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/expressions/FunctionArg.java) and [DistributionDTO](https://github.com/datastrato/gravitino/blob/main/common/src/main/java/com/datastrato/gravitino/dto/rel/DistributionDTO.java) to use more complex function arguments.

| Expression type | Json example                                                      | Java example                                                                                            | Equivalent SQL semantics | Description                    | 
|-----------------|-------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|--------------------------|--------------------------------|
| field           | `{"type":"field","fieldName":["score"]}`                          | `FieldReferenceDTO.of("score")`                                                                         | `score`                  | field reference value `score`  |
| function        | `{"type":"function","functionName":"hour","fieldName":["score"]}` | `new FuncExpressionDTO.Builder()<br/>.withFunctionName("hour")<br/>.withFunctionArgs("score").build()`  | `hour(score)`            | function value `hour(score)`   |
| constant        | `{"type":"literal","value":10, "dataType": "integer"}`            | `new LiteralDTO.Builder()<br/>.withValue("10")<br/>.withDataType(Types.IntegerType.get())<br/>.build()` | `10`                     | Integer constant `10`          |


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
 new DistributionDTO.Builder()
    .withStrategy(Strategy.HASH)
    .withNumber(4)
    .withArgs(FieldReferenceDTO.of("score"))
    .build()
```

</TabItem>
</Tabs>


## Sorted order table

To define a sorted order table, you should use the following three components to construct a valid sorted order table.

- Direction.  It defines in which direction we sort the table.

| Direction  | Description                               | Json   | Java                       |
|------------|-------------------------------------------| ------ | -------------------------- |
| ascending  | Sorted by a field or a function ascending | `asc`  | `SortDirection.ASCENDING`  |
| descending | Sorted by a field or a function descending| `desc` | `SortDirection.DESCENDING` |

- Null ordering. It describes how to handle null value when ordering

| Null ordering Type | Description                       | Json          | Java                       |
|--------------------|-----------------------------------| ------------- | -------------------------- |
| null_first         | Put null value in the first place | `nulls_first` | `NullOrdering.NULLS_FIRST` |
| null_last          | Put null value in the last place  | `nulls_last`  | `NullOrdering.NULLS_LAST`  |

Note: If the direction value is `ascending`, the default ordering value is `nulls_first` and if the direction value is `descending`, the default ordering value is `nulls_last`.

- Sort term. It shows which field or function should be used to sort the table, please refer to the `Expression type` in the bucketed table chapter.

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
 new SortOrderDTO.Builder()
    .withDirection(SortDirection.ASCENDING)
    .withNullOrder(NullOrdering.NULLS_LAST)
    .withSortTerm(FieldReferenceDTO.of("score"))
    .build()
```

</TabItem>
</Tabs>


:::tip
**Not all catalogs may support those features.**. Please refer to the related document for more details.
:::

The following is an example of creating a partitioned, bucketed table and sorted order table:

<Tabs>
<TabItem value="bash" label="Bash">

```bash
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
    new DistributionDTO.Builder()
    .withStrategy(Strategy.HASH)
    .withNumber(4)
    .withArgs(FieldReferenceDTO.of("id"))
    .build(),
    // SORTED BY name asc
    new SortOrderDTO[] {
    new SortOrderDTO.Builder()
    .withDirection(SortDirection.ASCENDING)
    .withNullOrder(NullOrdering.NULLS_LAST)
    .withSortTerm(FieldReferenceDTO.of("name"))
    .build()
    }
    );
```

</TabItem>
</Tabs>