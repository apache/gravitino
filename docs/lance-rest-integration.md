---
title: "Lance REST Integration"
slug: "/lance-rest-integration"
keywords:
  - lance
  - lance-rest
  - spark
  - ray
  - integration
license: "This software is licensed under the Apache License version 2."
---

## Overview

Engines reach the Apache Gravitino Lance REST service through a Lance client library. Two of them drive the service directly: [lance-spark](https://lance.org/integrations/spark/) for Apache Spark and [lance-ray](https://lance.org/integrations/ray/) for Ray. Both speak the Lance REST Catalog protocol, so they treat a Gravitino metalake as their namespace without knowing anything about Gravitino.

Set up the service first, as described in [Lance REST Service](./lance-rest-service.md). The examples below assume it is running on `http://localhost:9101/lance` and that `{catalog_name}` exists, which is where that page's Quick Start finishes.

## Connecting an Engine

Gravitino builds against `lance-namespace-core` 0.7.5, which is what most version incompatibilities below trace back to. Test the exact versions in your own environment before relying on them.

### Spark

#### Supported Versions

| lance-spark Version | Status                                            |
|---------------------|---------------------------------------------------|
| 0.5.1               | Verified                                          |
| 0.4.0               | Verified                                          |
| 0.2.0               | Verified                                          |
| 0.1.1               | Not supported, `404` on every table-creation flow |
| 0.1.0               | Not supported, `404` on every table-creation flow |

`lance-spark` 0.1.0 and 0.1.1 create tables through `POST /v1/table/{id}/create-empty`, which Gravitino no longer exposes after the deprecated `createEmptyTable` API was removed and table declaration was consolidated onto `POST /v1/table/{id}/declare`. Every table-creation flow returns `404 Not Found`, while list and describe still work. Upgrade to 0.2.0 or later.

To re-verify or extend the list, run the bundled test driver once per version:

```shell
# Per-version reports land under
# lance/lance-rest-server/build/reports/lance-spark-matrix/{version}/
./gradlew :lance:lance-rest-server:lanceSparkMatrixTest \
    -PlanceSparkBundleVersions=0.2.0,0.4.0,0.5.1 \
    -PskipDockerTests=true
```

#### Session Setup

Pass the `lance-spark` bundle JAR matching your Spark version, and open the JDK internals the bundle needs.

```python
import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/lance-spark-bundle-3.5_2.12-{version}.jar "
    "--conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" "
    "--conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" "
    "--master local[1] pyspark-shell"
)

spark = SparkSession.builder \
    .appName("lance_rest_integration") \
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "rest") \
    .config("spark.sql.catalog.lance.uri", "http://localhost:9101/lance") \
    .config("spark.sql.catalog.lance.parent", "{catalog_name}") \
    .config("spark.sql.defaultCatalog", "lance") \
    .getOrCreate()
```

The `parent` setting names the Gravitino catalog the Spark catalog is rooted at. Spark databases map to Gravitino schemas beneath it.

#### Reading and Writing

```python
spark.sql("CREATE DATABASE IF NOT EXISTS sales")
spark.sql("""
    CREATE TABLE sales.orders (id INT, score FLOAT)
    USING lance
    TBLPROPERTIES ('format' = 'lance')
""")
spark.sql("INSERT INTO sales.orders VALUES (1, 1.1)")
spark.sql("SELECT * FROM sales.orders").show()
```

The database and table appear in Gravitino as a schema and a table under `{catalog_name}`.

#### Table Location

The `LOCATION` clause on `CREATE TABLE` is optional. When it is omitted, `lance-spark` derives a location from the catalog properties Gravitino returns.

Put the storage configuration on the catalog rather than in the Spark session. The Lance REST service resolves `lance.storage.*` from the catalog and then the table, and returns the result to the client in the `storageOptions` of `CreateTable` and `DescribeTable` responses, so Spark does not need to repeat it.

Set those properties when you create the catalog, by passing them to the Lance `CreateNamespace` call:

```shell
LANCE_URL=http://localhost:9101/lance

curl -X POST "${LANCE_URL}/v1/namespace/{catalog_name}/create" \
  -H 'Content-Type: application/json' \
  -d '{"id": ["{catalog_name}"], "mode": "create",
       "properties": {"location": "s3://{bucket}/{prefix}",
                      "lance.storage.endpoint": "http://{minio_host}:9000",
                      "lance.storage.access_key_id": "{access_key}",
                      "lance.storage.secret_access_key": "{secret_key}"}}'
```

See [Storage Options](./lakehouse-generic-catalog.md#storage-options) for the option names. A per-table override is available by setting them as table properties, which take precedence over the catalog values.

### Ray

#### Supported Versions

| lance-ray Version | Status                                         |
|-------------------|------------------------------------------------|
| 0.4.2             | Verified                                       |
| 0.3.0             | Verified                                       |
| 0.2.0             | Works only with `pylance` pinned to 3.x or 4.x |
| 0.1.0             | Not supported                                  |

`lance-ray` 0.1.0 takes a constructed namespace object rather than an implementation name, so calls raise `TypeError: write_lance() got an unexpected keyword argument 'namespace_impl'`. Upgrade to 0.3.0 or later.

`lance-ray` 0.2.0 works only with a pin. Its `lance_ray.utils.create_storage_options_provider` imports a symbol that no longer exists in the `pylance` 6.0.0 wheel that `lance-namespace` 0.7.5 pulls in, raising `ImportError: cannot import name 'LanceNamespaceStorageOptionsProvider' from 'lance'`. Pin `pylance` to 3.x or 4.x, or upgrade to 0.3.0.

To re-verify or extend the list:

```shell
# Provisions a venv per version under
# clients/client-python/build/lance-ray-matrix/.venv-{version}/
# and runs tests/integration/test_lance_ray.py against each.
# The Gradle wrapper starts and stops Gravitino automatically.
./gradlew :clients:client-python:lanceRayMatrixTest \
    -PlanceRayVersions=0.4.2,0.3.0
```

#### Reading and Writing

Install `lance-ray`. Ray is pulled in automatically if it is not already present.

```shell
pip install lance-ray
```

Pass the namespace as an implementation name plus properties.

```python
import ray
from lance_ray import read_lance, write_lance

ray.init()

ns_properties = {"uri": "http://localhost:9101/lance"}
table_id = ["{catalog_name}", "sales", "orders"]

data = ray.data.range(1000).map(
    lambda row: {"id": row["id"], "value": row["id"] * 2}
)

write_lance(
    data,
    namespace_impl="rest",
    namespace_properties=ns_properties,
    table_id=table_id,
)

ray_dataset = read_lance(
    namespace_impl="rest",
    namespace_properties=ns_properties,
    table_id=table_id,
)

print(ray_dataset.filter(lambda row: row["value"] < 100).count())
```

The catalog and schema named in `table_id` have to exist before the write. Create them through the Lance REST service or the Gravitino REST API.

### Engines Without Lance REST Support

Lance publishes integrations for Trino, DuckDB, DataFusion, PyTorch, TensorFlow, and pandas, among others. None of them speaks the Lance REST Catalog protocol today. They read a dataset once they have its location, which you obtain from `DescribeTable` on the Lance REST service or from the Gravitino REST API, and then open the files directly.

`DescribeTable` returns the storage options alongside the location, so the credentials are available, but you have to relay them into the engine's own configuration rather than the engine picking them up. The engine also sees a location rather than a catalog, so it has no view of the namespace hierarchy and no way to discover a table it was not given.

See the [Lance integration documentation](https://lance.org/integrations) for the engine-side setup.

### Presenting Credentials

The examples above connect without credentials, which works when the Lance REST service is left at its `simple` default. Once the service is configured to validate callers, clients have to present a token on every request. See [Authenticating Callers](./lance-rest-service.md#authenticating-callers).

## Related Pages

- [Lance REST Service](./lance-rest-service.md) for service configuration, the API reference, and authentication
- [Lance Tables](./lakehouse-generic-lance-table.md) for table properties, type mappings, and worked examples on both APIs
- [Lance documentation](https://lance.org/) for the format and the client libraries themselves
