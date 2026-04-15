---
title: Connect Spark via Iceberg REST
sidebar_label: Spark
---

# Connecting Apache Spark via Iceberg REST

Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly — without installing a Gravitino-specific
connector plugin. This page describes how to configure Apache Spark to use Gravitino's Iceberg REST
(IRC) endpoint.

:::note
This integration uses the standard Apache Iceberg REST catalog specification. Gravitino enforces
its full access-control model on all IRC requests. Per-user identity propagation from the engine is
planned for a future release; current requests are authorized using the credentials supplied in the
Spark configuration.
:::

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from your Spark environment. The default port is `9001`.
- The following JAR files available in your Spark environment:
  - `iceberg-spark-runtime-3.5_2.12-1.7.1.jar`
  - `hadoop-aws-3.3.4.jar`
  - `aws-bundle-2.29.38.jar`

This page uses **Spark 3.5.3** with **Iceberg 1.7.1**. For other versions, ensure compatibility
between Spark, Scala, and Iceberg runtime versions.

## Configuration

`spark-defaults.conf` is Spark's persistent configuration file. Properties set here are
automatically applied to every Spark session — no command-line flags needed. The file lives at:

```
$SPARK_HOME/conf/spark-defaults.conf
```

If the file doesn't exist yet, copy the template:

```bash
cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
```

### Simple authentication

Add the following to `$SPARK_HOME/conf/spark-defaults.conf`:

```properties
# Iceberg extensions
spark.sql.extensions                                    org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Gravitino IRC catalog
spark.sql.catalog.gravitino_irc                         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.gravitino_irc.type                    rest
spark.sql.catalog.gravitino_irc.uri                     http://<gravitino-host>:9001/iceberg

# S3 FileIO
spark.sql.catalog.gravitino_irc.io-impl                 org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.gravitino_irc.s3.region               us-east-1
spark.sql.catalog.gravitino_irc.s3.access-key-id        <access-key>
spark.sql.catalog.gravitino_irc.s3.secret-access-key    <secret-key>

# Hadoop S3A (for s3a:// paths)
spark.hadoop.fs.s3a.impl                                org.apache.hadoop.fs.s3a.S3AFileSystem

# Set as default catalog (optional)
spark.sql.defaultCatalog                                gravitino_irc
```

:::note
`gravitino_irc` is the catalog identifier used within Spark. It maps to the Gravitino IRC endpoint
via the `uri` property. You may use any identifier you prefer. S3 credentials can alternatively
be supplied via environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) or an IAM
instance profile, in which case the explicit credential lines can be omitted.
:::

### With OAuth2 authentication

If Gravitino is configured with OAuth2, add the auth properties to the same
`$SPARK_HOME/conf/spark-defaults.conf` file:

```properties
# Iceberg extensions
spark.sql.extensions                                    org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Gravitino IRC catalog
spark.sql.catalog.gravitino_irc                         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.gravitino_irc.type                    rest
spark.sql.catalog.gravitino_irc.uri                     http://<gravitino-host>:9001/iceberg

# OAuth2 authentication
spark.sql.catalog.gravitino_irc.rest.auth.type          oauth2
spark.sql.catalog.gravitino_irc.rest.auth.oauth2.token  <your-token>

# S3 FileIO
spark.sql.catalog.gravitino_irc.io-impl                 org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.gravitino_irc.s3.region               us-east-1
spark.sql.catalog.gravitino_irc.s3.access-key-id        <access-key>
spark.sql.catalog.gravitino_irc.s3.secret-access-key    <secret-key>

# Hadoop S3A (for s3a:// paths)
spark.hadoop.fs.s3a.impl                                org.apache.hadoop.fs.s3a.S3AFileSystem

# Set as default catalog (optional)
spark.sql.defaultCatalog                                gravitino_irc
```

See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

:::tip Local development
For local development, [MinIO](https://min.io) can be used as an S3-compatible storage backend.
Replace the S3 FileIO section with:

```properties
spark.sql.catalog.gravitino_irc.io-impl                 org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.gravitino_irc.s3.endpoint             http://<minio-host>:9000
spark.sql.catalog.gravitino_irc.s3.path-style-access    true
spark.sql.catalog.gravitino_irc.s3.access-key-id        <minio-access-key>
spark.sql.catalog.gravitino_irc.s3.secret-access-key    <minio-secret-key>
spark.hadoop.fs.s3a.impl                                org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint                            http://<minio-host>:9000
spark.hadoop.fs.s3a.path.style.access                   true
spark.hadoop.fs.s3a.connection.ssl.enabled              false
```

See [gravitino-irc-quickstart](https://github.com/markhoerth/gravitino-irc-quickstart) for a
complete local development environment using MinIO.
:::

### With credential vending

If Gravitino is configured with credential vending, add the following to enable it on the client side:

```properties
spark.sql.catalog.gravitino_irc.header.X-Iceberg-Access-Delegation    vended-credentials
```

See [Credential vending](../iceberg-rest-service.md#credential-vending) for server-side configuration.

:::note
For storage not managed by Gravitino, properties are not automatically transferred from the server
to the client. Pass custom properties to initialize FileIO explicitly:

```properties
spark.sql.catalog.gravitino_irc.<configuration-key>    <property-value>
```
:::

## Starting Spark

Once `spark-defaults.conf` is in place, start your Spark session normally. The Gravitino IRC
catalog is available immediately without any additional flags.

### spark-shell (Scala)

```bash
$SPARK_HOME/bin/spark-shell
```

### spark-sql

```bash
$SPARK_HOME/bin/spark-sql
```

### pyspark

```bash
$SPARK_HOME/bin/pyspark
```

## Usage examples

### List namespaces

```sql
SHOW NAMESPACES IN gravitino_irc;
```

### List tables

```sql
SHOW TABLES IN gravitino_irc.<namespace>;
```

### Query a table

```sql
SELECT * FROM gravitino_irc.<namespace>.<table> LIMIT 10;
```

### Create a table

```sql
CREATE TABLE gravitino_irc.<namespace>.new_table (
  id INT,
  name STRING,
  created_at TIMESTAMP
) USING iceberg;
```

### Insert data

```sql
INSERT INTO gravitino_irc.<namespace>.new_table VALUES (1, 'example', current_timestamp());
```

## Gravitino connector vs Iceberg REST

| Feature | Gravitino Engine Connector | Iceberg REST |
|:---|:---|:---|
| Engine plugin required | Yes | No |
| Gravitino access control | Yes | Yes |
| Supported engines | Trino, Spark, Flink, Daft | Any Iceberg-compatible engine |
| Credential vending | Varies | Yes (S3, GCS, OSS, ADLS) |

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Trino via Iceberg REST](./trino.md)
- [Connect Flink via Iceberg REST](./flink.md)
- [Spark Gravitino connector](../spark-connector/spark-connector.md)
