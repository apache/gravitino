---
title: "Spark connector - User-defined functions"
slug: /spark-connector/spark-connector-udf
keyword: spark connector UDF user-defined function
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Apache Gravitino Spark connector supports loading user-defined functions (UDFs) registered
in the Gravitino function registry. Once a function is
[registered in Gravitino](../manage-user-defined-function-using-gravitino.md), Spark can discover and
invoke it through standard Spark SQL syntax — no additional `CREATE FUNCTION` statement is needed.

:::note
Currently, only **Java implementations** with `RuntimeType.SPARK` are supported in the Spark
connector. SQL and Python implementations registered in Gravitino cannot yet be invoked
directly from Spark. Support for additional languages is planned for future releases.
:::

## Prerequisites

Before using Gravitino UDFs in Spark, ensure the following:

1. The **Spark connector is configured** and the catalog is accessible
   (see [Spark connector setup](spark-connector.md)).
2. The function has been **registered in Gravitino** with at least one definition that includes
   a Java implementation targeting `RuntimeType.SPARK`
   (see [Register a function](../manage-user-defined-function-using-gravitino.md#register-a-function)).
3. The **JAR containing the UDF class** is available on the Spark classpath (e.g. via
   `--jars` or `spark.jars` configuration).

## Java UDF requirements

The Java class specified in `className` of the function implementation must implement Spark's
`org.apache.spark.sql.connector.catalog.functions.UnboundFunction` interface. For details on
implementing custom Spark functions, refer to the
[Spark DataSource V2 Functions documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/functions/UnboundFunction.html).

Key points:

- The class must have a **public no-arg constructor**.
- The class must be on the **Spark driver and executor classpath**.
- Only functions with `RuntimeType.SPARK` are visible to the Spark connector; implementations
  targeting other runtimes (e.g. `TRINO`) are filtered out.

## Calling functions in Spark SQL

Use the fully qualified three-part name `catalog.schema.function_name` to call a
Gravitino-registered function:

```sql
-- Call a scalar function
SELECT my_catalog.my_schema.add_one(42);

-- Use in a query
SELECT id, my_catalog.my_schema.add_one(value) AS incremented
FROM my_catalog.my_schema.my_table;
```

:::tip
You can simplify the syntax by setting the default catalog and schema first:

```sql
USE my_catalog.my_schema;
SELECT add_one(42);
```
:::

## Discovering functions

The Spark connector only exposes functions that have at least one Java implementation with
`RuntimeType.SPARK`. Functions with only non-Spark implementations (e.g. `TRINO`) are not
listed or loadable.

```sql
-- List all available functions in a schema (includes Gravitino UDFs with Spark runtime)
SHOW FUNCTIONS IN my_catalog.my_schema;
```
