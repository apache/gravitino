---
title: Connect Ray via Iceberg REST
sidebar_label: Ray
---

# Connecting Ray via Iceberg REST

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible client can connect to directly. This page describes how to use Ray Data with
Gravitino's Iceberg REST (IRC) endpoint.

:::note
Ray Data only supports reading from and writing to existing Iceberg tables. It does not support
DDL operations such as creating, dropping, or altering tables, schemas, or catalogs. Use Spark
or PyIceberg to manage table metadata.
:::

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your Python environment. The default port is `9001`.
- Ray installed: `pip install ray[data]`

## Configuration

Ray Data connects to the Gravitino IRC endpoint via `catalog_kwargs` passed directly to the
read/write functions. No separate catalog registration is required.

### Without authentication

```python
catalog_kwargs = {
    "name": "default",
    "type": "rest",
    "uri": "http://<gravitino-host>:9001/iceberg/",
}
```

### With credential vending and Basic authentication

```python
catalog_kwargs = {
    "name": "default",
    "type": "rest",
    "uri": "http://<gravitino-host>:9001/iceberg/",
    "header.X-Iceberg-Access-Delegation": "vended-credentials",
    "auth": {
        "type": "basic",
        "basic": {"username": "<user>", "password": "<password>"}
    }
}
```

See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

## Usage examples

### Write to an Iceberg table

```python
import ray
import pandas as pd

docs = [{"id": i, "data": f"Doc {i}"} for i in range(4)]
ds = ray.data.from_pandas(pd.DataFrame(docs))
ds.write_iceberg(
    table_identifier="default.sample",
    catalog_kwargs=catalog_kwargs
)
```

### Read from an Iceberg table

```python
import ray

ds = ray.data.read_iceberg(
    table_identifier="default.sample",
    catalog_kwargs=catalog_kwargs
)
ds.show(limit=1)
```

## Gravitino connector vs Iceberg REST

| | Gravitino Engine Connector | Iceberg REST |
|---|---|---|
| Engine plugin required | Yes | No |
| Gravitino access control | Yes | Yes |
| Supported engines | Trino, Spark, Flink, Daft | Any Iceberg-compatible engine |
| Credential vending | Varies | Yes (S3, GCS, OSS, ADLS) |

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect PyIceberg via Iceberg REST](./pyiceberg.md)
- [Connect Spark via Iceberg REST](./spark.md)
- 
