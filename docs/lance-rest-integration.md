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

This guide provides comprehensive instructions for integrating the Apache Gravitino Lance REST service with data processing engines that support the Lance format, including Apache Spark via the [Lance Spark connector](https://lance.org/integrations/spark/) and Ray via the [Lance Ray connector](https://lance.org/integrations/ray/).

This documentation assumes familiarity with the Lance REST service setup as described in the [Lance REST Service](./lance-rest-service) documentation.

## Compatibility Matrix

The following table outlines the tested compatibility between Gravitino versions and Lance connector versions:

| Gravitino Version (Lance REST) | Supported lance-spark Versions | Supported lance-ray Versions                  |
|--------------------------------|--------------------------------|-----------------------------------------------|
| 1.1.1 - 1.2.1                  | 0.0.10 - 0.0.15                | 0.0.6 - 0.0.8                                 |
| 1.3.0                          | 0.2.0, 0.4.0, 0.5.1            | 0.3.0 - 0.4.2 (0.2.0 conditionally supported) |

:::note
- These version entries show which versions are expected to work together.
- For Gravitino 1.3.0, the explicitly verified release versions are
  `lance-spark` (0.2.0, 0.4.0, 0.5.1) and `lance-ray` (0.3.0, 0.4.2). `lance-ray`
  0.2.0 is conditionally supported only with the conditions described below.

- **`lance-spark` 0.1.0 and 0.1.1 are not supported on Gravitino 1.3.0.**
  Those bundles create tables by calling the legacy
  `POST /lance/v1/table/{id}/create-empty` endpoint, which 1.3.0 no longer
  exposes — the table-declaration path was consolidated onto
  `POST /lance/v1/table/{id}/declare` (`LanceTableOperations#declareTable`)
  when the deprecated `createEmptyTable` API was removed during the
  `lance-namespace-core` 0.7.5 upgrade. Running 0.1.x against 1.3.0 surfaces
  as `404 Not Found` on every table-creation flow; the small set of
  list/describe-only tests still works, but any write path will fail.
- **`lance-ray` 0.1.0 is not supported on Gravitino 1.3.0.** It exposes
  `write_lance(... namespace=<LanceNamespace>)`, whereas the 0.2.0+ test path
  uses the new `write_lance(... namespace_impl="rest",
  namespace_properties={...})` signature. Calling it on 0.1.0 raises
  `TypeError: write_lance() got an unexpected keyword argument 'namespace_impl'`.
- **`lance-ray` 0.2.0 is conditionally supported on Gravitino 1.3.0.** It
  matches the new signature, but at runtime
  `lance_ray.utils.create_storage_options_provider` does
  `from lance import LanceNamespaceStorageOptionsProvider`, which no longer
  exists in the `pylance` 6.0.0 wheel that `lance-namespace==0.7.5` pulls in,
  raising `ImportError: cannot import name
  'LanceNamespaceStorageOptionsProvider' from 'lance'`. You can use
  `lance-ray` 0.2.0 with Gravitino 1.3.0 by pinning `pylance` to 3.x or 4.x.
- Before using in production, test the exact connector versions in your own environment.
- The Lance ecosystem is changing quickly, so some versions may introduce breaking changes.
:::

### Reproducing the matrix locally

Both connectors ship with a multi-version integration test driver so the
matrix can be re-verified (and extended) without ad-hoc scripting:

```bash
# lance-spark — runs LanceSparkRESTServiceIT once per bundle version.
# The default list intentionally omits 0.1.0 / 0.1.1: those bundles call the
# removed /create-empty endpoint and will fail with 404 against 1.3.0+.
./gradlew :lance:lance-rest-server:lanceSparkMatrixTest \
    -PlanceSparkBundleVersions=0.2.0,0.4.0,0.5.1 \
    -PskipDockerTests=true
# Per-version JUnit reports land under
# lance/lance-rest-server/build/reports/lance-spark-matrix/<version>/.

# lance-ray — provisions a venv per version under
# clients/client-python/build/lance-ray-matrix/.venv-<version>/ and runs
# tests/integration/test_lance_ray.py against each. The Gradle wrapper
# below starts / stops Gravitino automatically.
./gradlew :clients:client-python:lanceRayMatrixTest \
    -PlanceRayVersions=0.4.2,0.3.0
```

### Rationale

The Lance ecosystem is under active development, with frequent updates to APIs and features. Gravitino's Lance REST service depends on specific connector behaviors to ensure reliable operation. Using incompatible versions may result in:

- Runtime errors or exceptions
- Data corruption or loss
- Unexpected behavior in query execution
- Performance degradation

## Prerequisites

Before proceeding, ensure the following requirements are met:

1. **Gravitino Server**: A running Gravitino server instance with the Lance REST service enabled
    - Default endpoint: `http://localhost:9101/lance`

2. **Lance Catalog**: A Lance catalog created in Gravitino using either:
    - Lance REST namespace API (`CreateNamespace` operation - see [Lance REST Service documentation](./lance-rest-service.md)
    - Gravitino REST API, for more, refer to [lakehouse-generic-catalog](./lakehouse-generic-catalog.md)
    - Example catalog name: `lance_catalog`

3. **Lance Spark Bundle** (for Spark integration):
    - Downloaded `lance-spark` bundle JAR matching your Apache Spark version
    - Note the absolute file path for configuration

4. **Python Dependencies**:
    - For Spark integration: `pyspark`
    - For Ray integration: `ray`, `lance-namespace`, `lance-ray`

## Spark Integration

### Configuration

The following example demonstrates how to configure a PySpark session to interact with Lance REST and perform table operations using Spark SQL.

```python
from pyspark.sql import SparkSession
import os
import logging

# Configure logging for debugging
logging.basicConfig(level=logging.INFO)

# Configure Spark to use the lance-spark bundle
# Replace /path/to/lance-spark-bundle-3.5_2.12-X.X.XX.jar with your actual JAR path and version;
# refer to the compatibility matrix for supported lance-spark versions.
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/lance-spark-bundle-3.5_2.12-0.4.0.jar "
    "--conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" "
    "--conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" "
    "--master local[1] pyspark-shell"
)

# Initialize Spark session with Lance REST catalog configuration
# Note: The catalog "lance_catalog" must exist in Gravitino before running this code, you can create
# it via Lance REST API `CreateNamespace` or Gravitino REST API `CreateCatalog`.
spark = SparkSession.builder \
    .appName("lance_rest_integration") \
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "rest") \
    .config("spark.sql.catalog.lance.uri", "http://localhost:9101/lance") \
    .config("spark.sql.catalog.lance.parent", "lance_catalog") \
    .config("spark.sql.defaultCatalog", "lance") \
    .getOrCreate()

# Enable debug logging for troubleshooting
spark.sparkContext.setLogLevel("DEBUG")

# Create schema (database)
spark.sql("CREATE DATABASE IF NOT EXISTS sales")

# Create Lance table with explicit location
spark.sql("""
    CREATE TABLE sales.orders (
        id INT,
        score FLOAT
    )
    USING lance
    LOCATION '/tmp/sales/orders.lance/'
    TBLPROPERTIES ('format' = 'lance')
""")

# Insert sample data
spark.sql("INSERT INTO sales.orders VALUES (1, 1.1)")

# Query data
spark.sql("SELECT * FROM sales.orders").show()
```

### Storage Location Configuration

The `LOCATION` clause in the `CREATE TABLE` statement is optional. When omitted, lance-spark automatically determines an appropriate storage location based on catalog properties.
For detailed information on location resolution logic, refer to the [Lakehouse Generic Catalog documentation](./lakehouse-generic-catalog.md#key-property-location).

For Gravitino-managed Lance catalogs, put the storage configuration in the Gravitino catalog properties so Spark does not need to repeat it.

For example, create the Gravitino catalog with catalog-level Lance storage properties:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
  "name": "lance_catalog",
  "type": "RELATIONAL",
  "provider": "lakehouse-generic",
  "comment": "catalog for Lance tables on MinIO",
  "properties": {
    "location": "s3://bucket/tmp",
    "lance.storage.endpoint": "http://minio:9000",
    "lance.storage.access_key_id": "ak",
    "lance.storage.secret_access_key": "sk",
    "lance.storage.allow_http": "true",
    "lance.storage.region": "us-east-1"
  }
}' http://localhost:8090/api/metalakes/test/catalogs
```

```python
spark.sql("""
    CREATE TABLE sales.orders (
        id INT,
        score FLOAT
    )
    USING lance
    LOCATION 's3://bucket/tmp/sales/orders.lance/'
    TBLPROPERTIES ('format' = 'lance')
""")
```

If you need a per-table override, `lance.storage.*` table properties are still supported and take precedence over catalog defaults.

## Ray Integration

### Installation

Install the required Ray integration packages:

```shell
pip install lance-ray
```

:::info
- Ray will be automatically installed if not already present
- For Gravitino 1.3.0, use a `lance-namespace` client compatible with
  server-side `lance-namespace-core` 0.7.5 or newer.
- Ensure Ray version compatibility in your environment before deployment
:::

### Example

The following example demonstrates reading and writing Lance datasets through the Lance REST namespace using Ray:

```python
import ray
import lance_namespace as ln
from lance_ray import read_lance, write_lance

# Initialize Ray runtime
ray.init()

# Connect to Lance REST namespace
namespace = ln.connect("rest", {"uri":  "http://localhost:9101/lance"})

# Create sample dataset
data = ray.data.range(1000).map(
    lambda row: {"id": row["id"], "value": row["id"] * 2}
)

# Write dataset to Lance table
# Note: Both the catalog "lance_catalog" and schema "sales" must exist in Gravitino, you can create
# them via Lance REST API `CreateNamespace` or Gravitino REST API `CreateCatalog` and `CreateSchema`.
write_lance(
    data, 
    namespace=namespace, 
    table_id=["lance_catalog", "sales", "orders"]
)

# Read dataset from Lance table
ray_dataset = read_lance(
    namespace=namespace, 
    table_id=["lance_catalog", "sales", "orders"]
)

# Perform filtering operation
result = ray_dataset.filter(lambda row: row["value"] < 100).count()
print(f"Filtered row count: {result}")
```

## Additional Engines

The Lance REST service is compatible with other data processing engines that support the Lance format, including:

- **DuckDB**: For analytical SQL queries
- **Pandas**: For Python-based data manipulation
- **DataFusion**: For Rust-based query execution

Note: These three engines do not support Lance REST natively yet, but can still interact with Lance datasets through table location paths retrieved from the Lance REST service.

For engine-specific integration instructions, consult the [Lance Integration Documentation](https://lance.org/integrations).

### General Integration Pattern

Most Lance-compatible engines follow this general pattern:

1. Establish connection to Lance REST service endpoint
2. Authenticate using appropriate credentials
3. Reference tables using the hierarchical namespace structure
4. Execute read/write operations using engine-native APIs

Refer to each engine's specific documentation for detailed configuration parameters and code examples.

## Related

- [Lance REST Service Documentation](./lance-rest-service)
- [Lance Format Specification](https://lance.org/)
- [Apache Gravitino Documentation](https://gravitino.apache.org/)
- [Lakehouse Generic Catalog Guide](./lakehouse-generic-catalog.md)
