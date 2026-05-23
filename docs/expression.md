---
title: "Expression System"
slug: "/expression"
date: 2024-02-02
keyword: "expression function field literal reference"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page introduces the expression system of Apache Gravitino. Expressions are a core part of metadata definition. They define [default values](./manage-relational-metadata-using-gravitino.md#table-column-default-value) for columns, function arguments for [function partitioning](./table-partitioning-distribution-sort-order-indexes.md#table-partitioning) and [bucketing](./table-partitioning-distribution-sort-order-indexes.md#table-distribution), and sort terms for [sort ordering](./table-partitioning-distribution-sort-order-indexes.md#sort-ordering).

Gravitino divides expressions into three basic kinds: field references, literals, and functions. A function expression can contain field references, literals, and other function expressions.

## Field Reference

A field reference points to a field in a table. The following example creates a field reference for the `student` field.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="JSON">

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

A literal is a constant value. The following example creates a `NULL` literal and three literals for the value `1024` in different data types.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="JSON">

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

## Function Expression

A function expression represents a function call, with or without arguments. Arguments can be field references, literals, or other function expressions. The following example creates function expressions for `rand()` and `date_trunc('year', birthday)`.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="JSON">

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

## Unparsed Expression

The unparsed expression is a special expression type used to preserve a column default value that Gravitino cannot parse. The following examples show the data structure of an unparsed expression in JSON and Java, including how to retrieve its value.

<Tabs groupId='language' queryString>
  <TabItem value="Json" label="JSON">

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
String unparsedValue = ((UnparsedExpression) expression).unparsedExpression();
```

  </TabItem>
</Tabs>
