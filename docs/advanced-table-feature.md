---
title: "Advanced table feature"
slug: /advanced-table-feature
date: 2023-12-19
keyword: partitioning bucket distribution sort order
license: Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

#### Partitioned table

Currently, Gravitino supports the following partitioning strategies:

:::note
The `score`, `dt` and `city` are the field names in the table.
:::

| Partitioning strategy | Json                                                | Java                           | SQL syntax                 | Description                                                                                                                 |
|-----------------------|-----------------------------------------------------|--------------------------------|----------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Identity              | `{"strategy":"identity","fieldName":["score"]}`     | `Transforms.identity("score")` | `PARTITION BY score`       | Partition by a field or reference                                                                                           |
| Function              | `{"strategy":"functionName","fieldName":["score"]}` | `Transforms.hour("score")`     | `PARTITION BY hour(score)` | Partition by a function, currently, we support currently function, hour, year, day, bucket, month, truncate, list and range |

The detail of function strategies is as follows:

| Function strategy | Json                                                             | Java                                           | SQL syntax                         | Description                                            |
|-------------------|------------------------------------------------------------------|------------------------------------------------|------------------------------------|--------------------------------------------------------|
| Hour              | `{"strategy":"hour","fieldName":["score"]}`                      | `Transforms.hour("score")`                     | `PARTITION BY hour(score)`         | Partition by `hour` function in field `score`          |
| Day               | `{"strategy":"day","fieldName":["score"]}`                       | `Transforms.day("score")`                      | `PARTITION BY day(score)`          | Partition by `day` function in field `score`           |
| Month             | `{"strategy":"month","fieldName":["score"]}`                     | `Transforms.month("score")`                    | `PARTITION BY month(score)`        | Partition by `month` function in field `score`         |
| Year              | `{"strategy":"year","fieldName":["score"]}`                      | `Transforms.year("score")`                     | `PARTITION BY year(score)`         | Partition by `year` function in field `score`          |
| Bucket            | `{"strategy":"bucket","numBuckets":10,"fieldNames":[["score"]]}` | `Transforms.bucket(10, "score")`               | `PARTITION BY bucket(10, score)`   | Partition by `bucket` function in field `score`        |
| Truncate          | `{"strategy":"truncate","width":20,"fieldName":["score"]}`       | `Transforms.truncate(20, "score")`             | `PARTITION BY truncate(20, score)` | Partition by `truncate` function in field `score`      |
| List              | `{"strategy":"list","fieldNames":[["dt"],["city"]]}`             | `Transforms.list(new String[] {"dt", "city"})` | `PARTITION BY list(dt, city)`      | Partition by `list` function in fields `dt` and `city` |
| Range             | `{"strategy":"range","fieldName":["dt"]}`                        | `Transforms.range(20, "score")`                | `PARTITION BY range(score)`        | Partition by `range` function in field `score`         |

Except the strategies above, you can use other functions strategies to partition the table, for example, the strategy can be `{"strategy":"functionName","fieldName":["score"]}`. The `functionName` can be any function name that you can use in SQL, for example, `{"strategy":"functionName","fieldName":["score"]}` is equivalent to `PARTITION BY functionName(score)` in SQL. 
For complex function, please refer to `FunctionPartitioningDTO`. 

#### Bucketed table

- Strategy. It defines in which way we bucket the table.

| Bucket strategy | Json    | Java             | Description                                                                                 |
|-----------------|---------|------------------|---------------------------------------------------------------------------------------------|
| HASH            | `hash`  | `Strategy.HASH`  | Bucket table using hash                                                                     |
| RANGE           | `range` | `Strategy.RANGE` | Bucket table using range                                                                    |
| EVEN            | `even`  | `Strategy.EVEN`  | Bucket table using even, The data will be bucketed equally according to the amount of data. |

- Number. It defines how many buckets we use to bucket the table.
- Function arguments. It defines which field or function should be used to bucket the table. Gravitino supports the following three kinds of arguments, for more, you can refer to Java class `FunctionArg` and `DistributionDTO` to use more complex function arguments.

| Expression type | Json                                                              | Java                                                                                                      | SQL syntax      | Description                    | 
|-----------------|-------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|-----------------|--------------------------------|
| Field           | `{"type":"field","fieldName":["score"]}`                          | `FieldReferenceDTO.of("score")`                                                                           | `score`         | field reference value `score`  |
| Function        | `{"type":"function","functionName":"hour","fieldName":["score"]}` | `new FuncExpressionDTO.Builder()<br/>.withFunctionName("hour")<br/>.withFunctionArgs("score").build()`    | `hour(score)`   | function value `hour(score)`   |
| Constant        | `{"type":"constant","value":10, "dataType": "integer"}`           | `new LiteralDTO.Builder()<br/>.withValue("10")<br/>.withDataType(Types.IntegerType.get())<br/>.build()`   | `10`            | Integer constant `10`          |


#### Sorted order table

To define a sorted order table, you should use the following three components to construct a valid sorted order table.

- Direction.  It defines in which direction we sort the table.

| Direction  | Json   | Java                       | Description                               |
| ---------- | ------ | -------------------------- |-------------------------------------------|
| Ascending  | `asc`  | `SortDirection.ASCENDING`  | Sorted by a field or a function ascending |
| Descending | `desc` | `SortDirection.DESCENDING` | Sorted by a field or a function ascending |

- Null ordering. It describes how to handle null value when ordering

| Null ordering                     | Json          | Java                       | Description                       |
| --------------------------------- | ------------- | -------------------------- |-----------------------------------|
| Put null value in the first place | `nulls_first` | `NullOrdering.NULLS_FIRST` | Put null value in the first place |  
| Put null value int the last place | `nulls_last`  | `NullOrdering.NULLS_LAST`  | Put null value in the last place  |

- Sort term.  It shows which field or function should be used to sort the table, please refer to the `Expression type` in the bucketed table chapter.