---
title: "Expression system of Gravitino"
slug: /expression
date: 2024-02-02
keyword: expression function field literal reference
license: Copyright 2024 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This page introduces the expression system of Gravitino. Expressions are vital component of metadata definition, through expressions, you can define default values for columns, partitioning functions for tables, and so on.
Gravitino expression system divides expressions into three basic parts: field reference, literal, and function. Function expressions can contain field references, literals, and other function expressions.

## Field reference

Field reference is a reference to a field in a table.
The following is an example of creating a field reference expression, demonstrating how to create a reference for the `student` field.

<Tabs>
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
The following is an example of creating a literal expression, demonstrating how to create three different data types of literal expressions for the value `1024`.

<Tabs>
  <TabItem value="Json" label="Json">

```json
[
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

<Tabs>
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

