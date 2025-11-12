---
title: "Expression system of Apache Gravitino"
slug: /expression
date: 2024-02-02
keyword: expression function field literal reference
license: This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces the expression system of Apache Gravitino. Expressions are vital component of metadata definition, through expressions, you can define [default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) for columns, function arguments for [function partitioning](./table-partitioning-distribution-sort-order-indexes.md#table-partitioning), [bucketing](./table-partitioning-distribution-sort-order-indexes.md#table-distribution), and sort term of [sort ordering](./table-partitioning-distribution-sort-order-indexes.md#sort-ordering) in tables.
Gravitino expression system divides expressions into three basic parts: field reference, literal, and function. Function expressions can contain field references, literals, and other function expressions.

## Field reference

Field reference is a reference to a field in a table.
The following is an example of creating a field reference expression, demonstrating how to create a reference for the `student` field.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
[
  {
    "type": "field",
    "fieldName": [
      "student"
    ]
  }
]
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
NamedReference field = NamedReference.field("student");
```

  </TabItem>
</Tabs>

## Literal

Literal is a constant value.
The following is an example of creating a literal expression, demonstrating how to create a `NULL` literal and three different data types of literal expressions for the value `1024`.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
[
  {
    "type": "literal",
    "dataType": "null",
    "value": "null"
  },
  {
    "type": "literal",
    "dataType": "integer",
    "value": "1024"
  },
  {
    "type": "literal",
    "dataType": "string",
    "value": "1024"
  },
  {
    "type": "literal",
    "dataType": "decimal(10,2)",
    "value": "1024"
  }
]
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
Literal<?>[] literals =
    new Literal[] {
    Literals.NULL,
    Literals.integerLiteral(1024),
    Literals.stringLiteral("1024"),
    Literals.decimalLiteral(Decimal.of("1024", 10, 2))
    };
```

  </TabItem>
</Tabs>

## Function expression

Function expression represents a function call with/without arguments. The arguments can be field references, literals, or other function expressions.
The following is an example of creating a function expression, demonstrating how to create function expressions for `rand()` and `date_trunc('year', birthday)`.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
[
  {
    "type": "function",
    "funcName": "rand",
    "funcArgs": []
  },
  {
    "type": "function",
    "funcName": "date_trunc",
    "funcArgs": [
      {
        "type": "literal",
        "dataType": "string",
        "value": "year"
      },
      {
        "type": "field",
        "fieldName": [
          "birthday"
        ]
      }
    ]
  }
]
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
FunctionExpression[] functionExpressions =
        new FunctionExpression[] {
          FunctionExpression.of("rand"),
          FunctionExpression.of("date_trunc", Literals.stringLiteral("year"), NamedReference.field("birthday"))
        };
```

  </TabItem>
</Tabs>

## Unparsed expression

Unparsed expression is a special type of expression, currently serves exclusively for presenting the default value of a column when it's unsolvable.
The following shows the data structure of an unparsed expression in JSON and Java, enabling easy retrieval of its value.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="Json">

```json
{
  "type": "unparsed",
  "unparsedExpression": "(curdate() + interval 1 year)"
}
```

  </TabItem>
  <TabItem value="java" label="Java">

```java
// The result of the following expression is a string "(curdate() + interval 1 year)"
String unparsedValue = ((UnparsedExpression) expressino).unparsedExpression();
```

  </TabItem>
</Tabs>
