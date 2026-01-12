---
title: "Lance REST integration with Spark and Ray"
slug: /lance-rest-integration
keywords:
  - lance
  - lance-rest
  - spark
  - ray
  - integration
license: "This software is licensed under the Apache License version 2."
---

## Overview

This guide shows how to use the Lance REST service from Apache Gravitino with the [Lance Spark connector](https://lance.org/integrations/spark/) (`lance-spark`) and the [Lance Ray connector](https://lance.org/integrations/ray/) (`lance-ray`). It builds on the Lance REST service setup described in [Lance REST service](./lance-rest-service).

## Compatibility matrix

| Gravitino version (Lance REST) | Supported lance-spark versions | Supported lance-ray versions |
|--------------------------------|--------------------------------|------------------------------|
| 1.1.1                          | 0.0.10 – 0.0.15                | 0.0.6 – 0.0.8                |

These version ranges represent combinations that are expected to be compatible. Only a subset of versions within each range may have been explicitly tested, so you should verify a specific connector version in your own environment.

Why does we need to maintain compatibility matrix? As Lance and Lance connectors are actively developed, some APIs and features may change over time. Gravitino's Lance REST service relies on specific versions of these connectors to ensure seamless integration and functionality. Using incompatible versions may lead to unexpected behavior or errors.


## Prerequisites

- Gravitino server running with Lance REST service enabled (default endpoint: `http://localhost:9101/lance`).
- A Lance catalog created in Gravitino via Lance REST namespace API(see `CreateNamespace` in [docs](./lance-rest-service.md)) or Gravitino REST API, for example `lance_catalog`.
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
TBLPROPERTIES ('format' = 'lance')
""")
spark.sql("""
insert into schema.sample values(1, 1.1)
""")
spark.sql("select * from schema.sample").show()
```

The storage location in the example above is local path, if you want to use cloud storage, please refer to the following MinIO example:

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
  'lance.storage.allow_http' = 'true'
 )""")
```

## Using Lance REST with Ray

The snippet below writes and reads a Lance dataset through the Lance REST namespace.

```shell
pip install lance-ray
```
Please note that Ray will also be installed if not already present. Currently lance-ray is only tested with Ray version 2.41.0 to 2.50.0, please ensure Ray version compatibility in your environment.

After installing `lance-ray`, you can run the following Ray script:

```python
import ray
import lance_namespace as ln
from lance_ray import read_lance, write_lance

ray.init()

namespace = ln.connect("rest", {"uri": "http://localhost:9101/lance"})

data = ray.data.range(1000).map(lambda row: {"id": row["id"], "value": row["id"] * 2})

# Please note that namespace `schema` should also be created via Lance REST API or Gravitino API beforehand.
write_lance(data, namespace=namespace, table_id=["lance_catalog", "schema", "my_table"])
ray_dataset = read_lance(namespace=namespace, table_id=["lance_catalog", "schema", "my_table"])

result = ray_dataset.filter(lambda row: row["value"] < 100).count()
print(f"Filtered count: {result}")
```

:::note
- Ensure the target Lance catalog (`lance_catalog`) and schema (`schema`) already exist in Gravitino.
- The table path is represented as `["catalog", "schema", "table"]` when using Lance Ray helpers.
:::


## Other engines

Lance REST can also be used with other engines that support Lance format, such as DuckDB and Pandas. Please refer to the respective [integration documentation](https://lance.org/integrations/datafusion/) for details on how to connect to Lance REST from those engines.