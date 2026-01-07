---
title: "Lance REST integration with Spark and Ray"
slug: /lance-rest-spark-ray-integration
keywords:
  - lance
  - lance-rest
  - spark
  - ray
  - integration
license: "This software is licensed under the Apache License version 2."
---

## Overview

This guide shows how to use the Lance REST service from Apache Gravitino with the Lance Spark connector (`lance-spark`) and the Lance Ray connector (`lance-ray`). It builds on the Lance REST service setup described in [Lance REST service](./lance-rest-service).

## Compatibility matrix

| Gravitino version (Lance REST) | Supported lance-spark versions | Supported lance-ray versions |
|--------------------------------|--------------------------------|------------------------------|
| 1.1.1                          | 0.0.10 – 0.0.15                | 0.0.6 – 0.0.8                |

:::note
- Update this matrix when newer Gravitino versions (for example 1.2.0) are released.
- Align connector versions with the Lance REST service bundled in the target release.
:::

## Prerequisites

- Gravitino server running with Lance REST service enabled (default endpoint: `http://localhost:9101/lance`).
- A Lance catalog created in Gravitino, for example `lance_catalog`.
- Downloaded `lance-spark` bundle JAR that matches your Spark version (set the absolute path in the examples below).
- Python environments with required packages:
  - Spark: `pyspark`
  - Ray: `ray`, `lance-namespace`, `lance-ray`

## Using Lance REST with Spark

The example below starts a local PySpark session that talks to Lance REST and creates a table through Spark SQL.

```python
from pyspark.sql import SparkSession
import os
import logging
logging.basicConfig(level=logging.INFO)

# Point to your downloaded lance-spark bundle.
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/lance-spark-bundle-3.5_2.12-0.0.15.jar --conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --master local[1] pyspark-shell"

# Create the Lance catalog named "lance_catalog" in Gravitino beforehand.
spark = SparkSession.builder \
    .appName("lance_rest_example") \
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "rest") \
    .config("spark.sql.catalog.lance.uri", "http://localhost:9101/lance") \
    .config("spark.sql.catalog.lance.parent", "lance_catalog") \
    .config("spark.sql.defaultCatalog", "lance") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

# Create schema and table, write, then read data.
spark.sql("create database schema")
spark.sql("""
create table schema.sample(id int, score float)
USING lance
LOCATION '/tmp/schema/sample.lance/'
TBLPROPERTIES ('format' = 'lance')
""")
spark.sql("""
insert into schema.sample values(1, 1.1)
""")
spark.sql("select * from schema.sample").show()
```

:::note
- Keep the Lance REST service reachable from Spark executors.
- Replace the JAR path with the actual location on your machine or cluster.
- Add your own JVM debugging flags only when needed.
:::

The storage location in the example above is local path, if you want to use cloud storage, please refer to the following Minio example:

```python
spark.sql("""
create table schema.sample(id int, score float)
USING lance
LOCATION 's3://bucket/tmp/schema/sample.lance/'
TBLPROPERTIES (
  'format' = 'lance',
  'lance.storage.access_key_id' = 'ak',
  'lance.storage.endpoint' = 'http://minio:9000',
  'lance.storage.secret_access_key' = 'sk',
  'lance.storage.allow_http'= 'true'
 )
```

## Using Lance REST with Ray

The snippet below writes and reads a Lance dataset through the Lance REST namespace.

```shell
pip install lance-ray
```
Please note that ray will also be installed if not already present. Currently lance-ray only tested with ray version 2.50.0, please
ensure ray version compatibility in your environment.

```python
import ray
import lance_namespace as ln
from lance_ray import read_lance, write_lance

ray.init()

namespace = ln.connect("rest", {"uri": "http://localhost:9101/lance"})

data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

write_lance(data, namespace=namespace, table_id=["lance_catalog", "schema", "my_table"])
ray_dataset = read_lance(namespace=namespace, table_id=["lance_catalog", "schema", "my_table"])

result = ray_dataset.filter(lambda row: row["value"] < 100).count()
print(f"Filtered count: {result}")
```

:::note
- Ensure the target Lance catalog (`lance_catalog`) and schema (`schema`) already exist in Gravitino.
- The table path is represented as `["catalog", "schema", "table"]` when using Lance Ray helpers.
:::
