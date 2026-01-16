---
title: "Lance REST Integration with Spark and Ray"
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

This guide provides comprehensive instructions for integrating the Apache Gravitino Lance REST service with data processing engines that support the Lance format, including Apache Spark via the [Lance Spark connector](https://lance.org/integrations/spark/) and Ray via the [Lance Ray connector](https://lance.org/integrations/ray/).

This documentation assumes familiarity with the Lance REST service setup as described in the [Lance REST Service](./lance-rest-service) documentation.

## Compatibility Matrix

The following table outlines the tested compatibility between Gravitino versions and Lance connector versions:

| Gravitino Version (Lance REST) | Supported lance-spark Versions | Supported lance-ray Versions |
|--------------------------------|--------------------------------|------------------------------|
| 1.1.1                          | 0.0.10 – 0.0.15                | 0.0.6 – 0.0.8                |

:::note
- These version ranges represent combinations expected to be compatible based on API stability and feature sets.
- While broad compatibility is anticipated within these ranges, only select versions have been explicitly tested.
- We strongly recommend validating specific connector versions in your development environment before production deployment.
- As the Lance ecosystem evolves rapidly, API changes may introduce breaking changes between versions.
:::

### Why Maintain a Compatibility Matrix?

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
    - Gravitino REST API, for more, please refer to [lakehouse-generic-catalog](./lakehouse-generic-catalog.md)
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
logging. basicConfig(level=logging.INFO)

# Configure Spark to use the lance-spark bundle
# Replace /path/to/lance-spark-bundle-3.5_2.12-0.0.15.jar with your actual JAR path
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/lance-spark-bundle-3.5_2.12-0.0.15.jar "
    "--conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" "
    "--conf \"spark.executor.extraJavaOptions=--add-opens=java. base/sun.nio.ch=ALL-UNNAMED\" "
    "--master local[1] pyspark-shell"
)

# Initialize Spark session with Lance REST catalog configuration
# Note: The catalog "lance_catalog" must exist in Gravitino before running this code, you can create
# it via Lance REST API `CreateNameSpace` or Gravitino REST API `CreateCatalog`.
spark = SparkSession.builder \
    .appName("lance_rest_integration") \
    .config("spark.sql.catalog.lance", "com.lancedb.lance. spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "rest") \
    .config("spark. sql.catalog.lance.uri", "http://localhost:9101/lance") \
    .config("spark.sql.catalog.lance. parent", "lance_catalog") \
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

#### Local Storage

The `LOCATION` clause in the `CREATE TABLE` statement is optional. When omitted, lance-spark automatically determines an appropriate storage location based on catalog properties.
For detailed information on location resolution logic, refer to the [Lakehouse Generic Catalog documentation](./lakehouse-generic-catalog.md#key-property-location).

#### Cloud Storage (S3-Compatible)

For cloud storage backends such as Amazon S3 or MinIO, specify credentials and endpoint configuration in the table properties:

```python
spark.sql("""
    CREATE TABLE sales.orders (
        id INT,
        score FLOAT
    )
    USING lance
    LOCATION 's3://bucket/tmp/sales/orders.lance/'
    TBLPROPERTIES (
        'format' = 'lance',
        'lance.storage.access_key_id' = 'your_access_key',
        'lance.storage.secret_access_key' = 'your_secret_key',
        'lance.storage.endpoint' = 'http://minio:9000',
        'lance.storage.allow_http' = 'true'
    )
""")
```

:::warning
Never hardcode credentials in production code. Use environment variables, secret management systems, or IAM roles for credential management.
:::

## Ray Integration

### Installation

Install the required Ray integration packages:

```shell
pip install lance-ray
```

:::info
- Ray will be automatically installed if not already present
- lance-ray is currently tested with Ray versions 2.41.0 to 2.50.0
- Ensure Ray version compatibility in your environment before deployment
:::

### Usage Example

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
# them via Lance REST API `CreateNameSpace` or Gravitino REST API `CreateCatalog` and `CreateSchema`.
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

### Important Considerations

- **Namespace Hierarchy**: The `table_id` parameter uses a hierarchical structure:  `["catalog_name", "sales", "orders"]`
- **Pre-requisites**: Both the target catalog (`lance_catalog`) and schema (`sales`) must be created in Gravitino before executing write operations
- **Error Handling**:  Implement appropriate error handling for network failures and authentication issues in production environments

## Additional Engine Support

The Lance REST service is compatible with other data processing engines that support the Lance format, including:

- **DuckDB**: For analytical SQL queries
- **Pandas**: For Python-based data manipulation
- **DataFusion**: For Rust-based query execution

Note: These three engines does not support Lance REST natively yet, but can still interact with Lance datasets through table location paths retrieved from the Lance REST service.

For engine-specific integration instructions, consult the [Lance Integration Documentation](https://lance.org/integrations/datafusion/).

### General Integration Pattern

Most Lance-compatible engines follow this general pattern:

1. Establish connection to Lance REST service endpoint
2. Authenticate using appropriate credentials
3. Reference tables using the hierarchical namespace structure
4. Execute read/write operations using engine-native APIs

Refer to each engine's specific documentation for detailed configuration parameters and code examples.

## Additional Resources

- [Lance REST Service Documentation](./lance-rest-service)
- [Lance Format Specification](https://lance.org/)
- [Apache Gravitino Documentation](https://gravitino.apache.org/)
- [Lakehouse Generic Catalog Guide](./lakehouse-generic-catalog.md)