---
title: "Connect PyIceberg to Iceberg REST"
sidebar_label: "PyIceberg"
---

## Introduction

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible client can connect to directly. This page describes how to use PyIceberg with
Gravitino's Iceberg REST (IRC) endpoint.

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your Python environment. The default port is `9001`.
- PyIceberg installed: `pip install pyiceberg`

## Configuration

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "gravitino_irc",
    **{
        "type": "rest",
        "uri":  "http://<gravitino-host>:9001/iceberg",
    }
)
```

### Credential Vending

```python
catalog = load_catalog(
    "gravitino_irc",
    **{
        "type":                            "rest",
        "uri":                             "http://<gravitino-host>:9001/iceberg",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }
)
```

### OAuth2 Authentication

```python
catalog = load_catalog(
    "gravitino_irc",
    **{
        "type":  "rest",
        "uri":   "http://<gravitino-host>:9001/iceberg",
        "token": "<your-token>",
    }
)
```

See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

## Examples

### List Namespaces

```python
catalog.list_namespaces()
```

### Load a Table

```python
table = catalog.load_table("db.table")
print(table.schema())
```

### Scan a Table

```python
df = table.scan().to_arrow()
print(df)
```

### Create a Namespace and Table

```python
catalog.create_namespace("db")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType

schema = Schema(
    NestedField(1, "id",   LongType(),   required=True),
    NestedField(2, "name", StringType(), required=False),
)
catalog.create_table("db.new_table", schema=schema)
```

## Gravitino Connector vs. Iceberg REST

| Feature                  | Gravitino Engine Connector  | Iceberg REST                  |
|:-------------------------|:----------------------------|:------------------------------|
| Engine plugin required   | Yes                         | No                            |
| Gravitino access control | Yes                         | Yes                           |
| Supported engines        | Trino, Spark, Flink, Daft   | Any Iceberg-compatible engine |
| Credential vending       | Varies                      | Yes (S3, GCS, OSS, ADLS)      |

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark via Iceberg REST](./spark.md)
- [Connect Flink via Iceberg REST](./flink.md)
