---
title: "Fileset Catalog with S3"
slug: "/fileset-catalog-with-s3"
keyword: "Fileset catalog S3"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This page shows how to store fileset data in Amazon S3 while Gravitino manages the metadata,
and how to read and write that data through the Gravitino Virtual File System (GVFS).

Everything on this page is specific to Amazon S3. The fileset model itself, the properties shared by
every storage backend, and the way properties are inherited from catalog to schema to fileset are
described in [Fileset Catalog](./fileset-catalog.md).

The examples run in order and use the same names throughout: metalake `metalake`, catalog
`s3_catalog`, schema `s3_schema`, fileset `example_fileset`, and `http://localhost:8090` as the
server URL. Replace them with your own values.

## Prerequisites

1. Download the [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle) file.
2. Place it in the fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server:

```bash
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

The catalog automatically loads the Amazon S3 filesystem provider once the bundle jar is on the
classpath. The deprecated `filesystem-providers` and `default-filesystem-provider` catalog
properties do not need to be set.

## Amazon S3 Properties

These properties are needed in addition to the shared
[catalog properties](./fileset-catalog.md#catalog-properties). Configure credentials on the
catalog; GVFS clients fetch them through [credential vending](./security/credential-vending.md)
and must not set cloud credentials in local configuration. Non-secret settings are inherited
from catalog, schema, and fileset metadata — override them in GVFS configuration only when
needed. The Python client spells property names with underscores while the catalog and the
Java client use hyphens.

| Catalog and Java client | Python client          | Description                                                                                                                                                                                                                                                                                                         | Required                                         |
|-------------------------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|
| `s3-endpoint`           | `s3_endpoint`          | Endpoint of the S3 service. S3-compatible storage such as MinIO always needs it.                                                                                                                                                                                                                                    | Yes, except for the Python client against AWS S3 |
| `s3-access-key-id`      | `s3_access_key_id`     | Access key of the S3 service.                                                                                                                                                                                                                                                                                       | Yes (catalog)                                    |
| `s3-secret-access-key`  | `s3_secret_access_key` | Secret key of the S3 service.                                                                                                                                                                                                                                                                                       | Yes (catalog)                                    |
| `credential-providers`  | (n/a)                  | The credential provider types, separated by comma. Possible values are `s3-token`, `s3-secret-key`, `aws-irsa`. When set explicitly, chooses how the server vends credentials. If omitted, Gravitino auto-detects a provider from the static credentials on the catalog. See [credential vending](./security/credential-vending.md#s3) for the extra properties each provider takes. | No                                               |

:::note
- The location must start with `s3a://`, not `s3://`. The `hadoop-aws` library does not support the
  `s3://` scheme.
- For MinIO and other S3-compatible services, set `s3-endpoint` to that service. If it requires
  path-style access, add `gravitino.bypass.fs.s3a.path.style.access=true` to
  `${GRAVITINO_HOME}/catalogs/fileset/conf/fileset.conf` for server-side operations. Also set
  `s3-path-style-access=true` on a GVFS Java client, or
  `spark.hadoop.s3-path-style-access=true` in Spark. For the Python GVFS client, pass
  `config_kwargs={"s3": {"addressing_style": "path"}}` to
  `GravitinoVirtualFileSystem`; for pandas, add the same `config_kwargs` entry at the top level
  of `storage_options`.
:::

Schema and fileset properties are documented on the shared page: see
[schema properties](./fileset-catalog.md#schema-properties) and
[fileset properties](./fileset-catalog.md#fileset-properties).

A fileset catalog stores its data under `location`, which for Amazon S3 looks like
`s3a://bucket/root`.

## Create the Catalog, Schema, and Fileset

### Step 1: Create the catalog

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "s3_catalog",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Amazon S3",
  "properties": {
    "location": "s3a://bucket/root",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "s3-access-key-id": "access_key",
    "s3-secret-access-key": "secret_key"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root")
    .put("s3-endpoint", "http://s3.ap-northeast-1.amazonaws.com")
    .put("s3-access-key-id", "access_key")
    .put("s3-secret-access-key", "secret_key")
    .build();

Catalog catalog = gravitinoClient.createCatalog("s3_catalog",
    Catalog.Type.FILESET,
    "A fileset catalog backed by Amazon S3",
    catalogProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(
    uri="http://localhost:8090", metalake_name="metalake")

catalog_properties = {
    "location": "s3a://bucket/root",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "s3-access-key-id": "access_key",
    "s3-secret-access-key": "secret_key",
}

catalog = gravitino_client.create_catalog(name="s3_catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider=None,
                                          comment="A fileset catalog backed by Amazon S3",
                                          properties=catalog_properties)
```

</TabItem>
</Tabs>

### Step 2: Create the schema

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "s3_schema",
  "comment": "A schema in the Amazon S3 fileset catalog",
  "properties": {
    "location": "s3a://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/s3_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("s3_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root/schema")
    .build();

Schema schema = supportsSchemas.createSchema("s3_schema",
    "A schema in the Amazon S3 fileset catalog",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="s3_catalog")
catalog.as_schemas().create_schema(name="s3_schema",
                                   comment="A schema in the Amazon S3 fileset catalog",
                                   properties={"location": "s3a://bucket/root/schema"})
```

</TabItem>
</Tabs>

### Step 3: Create the fileset

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "s3a://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/s3_catalog/schemas/s3_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("s3_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> filesetProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("s3_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "s3a://bucket/root/schema/example_fileset",
    filesetProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="s3_catalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("s3_schema", "example_fileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="s3a://bucket/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The fileset is now addressable as
`gvfs://fileset/s3_catalog/s3_schema/example_fileset` from any GVFS client.

## Access the Fileset

### Java client jars

Every Java or Hadoop-based client needs `gravitino-filesystem-hadoop3-runtime`, which is published
on Maven Central, plus the Amazon S3 filesystem implementation. Only the latter differs by
environment:

| Environment            | Jar providing the Amazon S3 filesystem                                                                                                                                             |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No Hadoop installed    | [`gravitino-aws-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle), a fat jar bundling the Amazon S3 filesystem implementation and the AWS SDK |
| Hadoop already present | `hadoop-aws-${hadoop-version}.jar` and `aws-java-sdk-bundle-1.12.262.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`                                       |

The artifacts in full:

- [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle):
  a "fat" jar that includes the `gravitino-aws` functionality together with every dependency it needs,
  such as `hadoop-aws` and the AWS SDK. Use it when the environment has no pre-existing Hadoop setup.
- [`gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-filesystem-hadoop3-runtime):
  a "fat" jar that bundles the Gravitino virtual filesystem client and already includes the
  `gravitino-aws` functionality. Java and Hadoop-based clients require it to access Gravitino
  filesets.
- `hadoop-aws-${hadoop-version}.jar` and `aws-java-sdk-bundle-1.12.262.jar`: the standard Hadoop
  dependencies for Amazon S3 access, shipped with Hadoop under
  `${HADOOP_HOME}/share/hadoop/tools/lib`. Supply them yourself when running inside an existing
  Hadoop environment.
- [`gravitino-aws-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws):
  a "thin" jar carrying only the AWS integration code. It is already contained in both jars above,
  so it is not needed as a direct dependency unless you prefer to manage all Hadoop and AWS
  dependencies yourself.

```xml
<!-- No Hadoop environment -->
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-aws-bundle</artifactId>
  <version>${GRAVITINO_VERSION}</version>
</dependency>
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
  <version>${GRAVITINO_VERSION}</version>
</dependency>
```

```xml
<!-- Existing Hadoop environment -->
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-common</artifactId>
  <version>${HADOOP_VERSION}</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-aws</artifactId>
  <version>${HADOOP_VERSION}</version>
</dependency>
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
  <version>${GRAVITINO_VERSION}</version>
</dependency>
```

:::note
The thin `gravitino-aws` jar is not needed. Its functionality is already included in both
`gravitino-aws-bundle` and `gravitino-filesystem-hadoop3-runtime`.
:::

### GVFS Java client

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration), configure the Gravitino
connection. Cloud credentials are fetched from the server; do not set access keys in the Hadoop
`Configuration`. Override non-secret properties such as `s3-endpoint` only when they are not
already set on the catalog.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");

Path filesetPath = new Path("gvfs://fileset/s3_catalog/s3_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

### Apache Spark

The example below uses PySpark 3.5.0 in an environment that already has Hadoop 3.3.4.

```bash
pip install pyspark==3.5.0
pip install apache-gravitino==${GRAVITINO_VERSION}
```

```python
import os
from pyspark.sql import SparkSession

# On JDK 17, also add:
#   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
#   --conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar,"
    "/path/to/hadoop-aws-3.3.4.jar,"
    "/path/to/aws-java-sdk-bundle-1.12.262.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("s3_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate())

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
spark_df = spark.createDataFrame(data, schema=["Name", "Age"])
gvfs_path = "gvfs://fileset/s3_catalog/s3_schema/example_fileset/people"

spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gvfs_path)
```

If Spark runs without a Hadoop environment, only the jar list changes:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-aws-bundle-${gravitino-version}.jar,"
    "/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar "
    "--master local[1] pyspark-shell"
)
```

:::note
Some Spark versions need a Hadoop environment in the driver and do not pick up filesystem
implementations passed with `--jars`. If that happens, add the jars to the Spark classpath directly.
:::

### Hadoop fs command

1. Add the following to `${HADOOP_HOME}/etc/hadoop/core-site.xml`:

```xml
<property>
  <name>fs.AbstractFileSystem.gvfs.impl</name>
  <value>org.apache.gravitino.filesystem.hadoop.Gvfs</value>
</property>
<property>
  <name>fs.gvfs.impl</name>
  <value>org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem</value>
</property>
<property>
  <name>fs.gravitino.server.uri</name>
  <value>http://localhost:8090</value>
</property>
<property>
  <name>fs.gravitino.client.metalake</name>
  <value>metalake</value>
</property>
```

2. Add these jars to the Hadoop classpath:

   - `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, from Maven Central.
   - `hadoop-aws-${hadoop-version}.jar` and `aws-java-sdk-bundle-1.12.262.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`.

3. Access the fileset:

```shell
${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/s3_catalog/s3_schema/example_fileset
${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/s3_catalog/s3_schema/example_fileset
```

### GVFS Python client

```bash
pip install apache-gravitino==${GRAVITINO_VERSION}
```

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration-1), pass Gravitino
connection settings in `options`. Do not pass cloud access keys; GVFS fetches credentials from the
server.

```python
from gravitino import gvfs

options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
}

fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
fs.ls("gvfs://fileset/s3_catalog/s3_schema/example_fileset/")
```

### pandas

pandas reaches the same paths through `storage_options`. Use the `fs` instance from the preceding
GVFS example to discover the generated Spark part file.

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090",
    "metalake_name": "metalake",
    "options": {
        "auth_type": "simple",
    }
}

csv_path = next(
    f"gvfs://{path}"
    for path in fs.ls(
        "gvfs://fileset/s3_catalog/s3_schema/example_fileset/people",
        detail=False,
    )
    if (
        path.rsplit("/", 1)[-1].startswith("part-")
        and path.endswith(".csv")
    )
)
ds = pd.read_csv(csv_path, storage_options=storage_options)
ds.head()
```

For further use cases, see [Gravitino Virtual File System](./how-to-use-gvfs.md).

## Credential Vending

GVFS always uses credential vending for cloud filesets: the catalog holds the Amazon S3
credentials and the Gravitino server hands out a credential per request, so clients never configure
cloud keys locally. See [Credential Vending](./security/credential-vending.md) for the general
mechanism and [S3 credentials](./security/credential-vending.md#s3) for the properties each
provider takes.

The supported providers are `s3-token`, which vends a short-lived STS token;
`s3-secret-key`, which vends the static access key configured on the catalog; and `aws-irsa`, which
vends credentials from an IAM role for service accounts and currently reads the web identity token
from a file. The example below uses `s3-token`.

### Configure the catalog, schema, and fileset

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "s3_catalog_with_vending",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Amazon S3 with credential vending",
  "properties": {
    "location": "s3a://bucket/root",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "s3-access-key-id": "access_key",
    "s3-secret-access-key": "secret_key",
    "credential-providers": "s3-token",
    "s3-region": "ap-northeast-1",
    "s3-role-arn": "arn:aws:iam::123456789012:role/gravitino-fileset"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

Create the schema and fileset in the credential-vending catalog:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "s3_schema",
  "comment": "A schema in the Amazon S3 credential-vending catalog",
  "properties": {
    "location": "s3a://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/s3_catalog_with_vending/schemas

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "s3a://bucket/root/schema/example_fileset",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/s3_catalog_with_vending/schemas/s3_schema/filesets
```

The `s3-token` provider needs two more catalog properties.

| Property Name | Description                                        |
|---------------|----------------------------------------------------|
| `s3-region`   | Region of the bucket, for example `ap-northeast-1` |
| `s3-role-arn` | ARN of the role that grants access to the data     |

### GVFS client configuration

Configure only the Gravitino connection on the client. Cloud credentials come from the server.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");

Path filesetPath = new Path(
    "gvfs://fileset/s3_catalog_with_vending/s3_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

```python
spark = (SparkSession.builder
    .appName("s3_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .getOrCreate())
```

```python
options = {
    "auth_type": "simple",
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
```
