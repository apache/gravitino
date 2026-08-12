---
title: "Fileset Catalog with Google Cloud Storage"
slug: "/fileset-catalog-with-gcs"
keyword: "Fileset catalog GCS"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This page shows how to store fileset data in Google Cloud Storage while Gravitino manages the metadata,
and how to read and write that data through the Gravitino Virtual File System (GVFS).

Everything on this page is specific to Google Cloud Storage. The fileset model itself, the properties shared by
every storage backend, and the way properties are inherited from catalog to schema to fileset are
described in [Fileset Catalog](./fileset-catalog.md).

The examples run in order and use the same names throughout: metalake `metalake`, catalog
`gcs_catalog`, schema `gcs_schema`, fileset `example_fileset`, and `http://localhost:8090` as the
server URL. Replace them with your own values.

## Prerequisites

1. Download the [`gravitino-gcp-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle) file.
2. Place it in the fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server:

```bash
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

The catalog automatically loads the Google Cloud Storage filesystem provider once the bundle jar is on the
classpath. The deprecated `filesystem-providers` and `default-filesystem-provider` catalog
properties do not need to be set.

## Google Cloud Storage Properties

These properties are needed in addition to the shared
[catalog properties](./fileset-catalog.md#catalog-properties). The same values are also needed by
the GVFS clients, so they are listed together here — note that the Python client spells them with
underscores while the catalog and the Java client use hyphens.

| Catalog and Java client | Python client | Description | Required |
|-------------------------|---------------|-------------|----------|
| `gcs-service-account-file` | `gcs_service_account_file` | Path of the GCS service account JSON file. | Yes |

:::note
The service account file must be readable by the Gravitino server process for the catalog, and by
each client process for GVFS.
:::

A fileset catalog stores its data under `location`, which for Google Cloud Storage looks like
`gs://bucket/root`.

## Create the Catalog, Schema, and Fileset

### Step 1: Create the catalog

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "gcs_catalog",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Google Cloud Storage",
  "properties": {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "/path/to/service-account.json"
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
    .put("location", "gs://bucket/root")
    .put("gcs-service-account-file", "/path/to/service-account.json")
    .build();

Catalog catalog = gravitinoClient.createCatalog("gcs_catalog",
    Catalog.Type.FILESET,
    "A fileset catalog backed by Google Cloud Storage",
    catalogProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(
    uri="http://localhost:8090", metalake_name="metalake")

catalog_properties = {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "/path/to/service-account.json",
}

catalog = gravitino_client.create_catalog(name="gcs_catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider=None,
                                          comment="A fileset catalog backed by Google Cloud Storage",
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
  "name": "gcs_schema",
  "comment": "A schema in the Google Cloud Storage fileset catalog",
  "properties": {
    "location": "gs://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/gcs_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("gcs_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "gs://bucket/root/schema")
    .build();

Schema schema = supportsSchemas.createSchema("gcs_schema",
    "A schema in the Google Cloud Storage fileset catalog",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="gcs_catalog")
catalog.as_schemas().create_schema(name="gcs_schema",
                                   comment="A schema in the Google Cloud Storage fileset catalog",
                                   properties={"location": "gs://bucket/root/schema"})
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
  "storageLocation": "gs://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/gcs_catalog/schemas/gcs_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("gcs_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> filesetProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("gcs_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "gs://bucket/root/schema/example_fileset",
    filesetProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="gcs_catalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("gcs_schema", "example_fileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="gs://bucket/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The fileset is now addressable as
`gvfs://fileset/gcs_catalog/gcs_schema/example_fileset` from any GVFS client.

## Access the Fileset

### Client jars

Every client needs `gravitino-filesystem-hadoop3-runtime`, plus the Google Cloud Storage filesystem
implementation. Which jar provides it depends on the environment:

| Environment | Jars to add |
|-------------|-------------|
| No Hadoop installed | [`gravitino-gcp-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle) — a fat jar bundling `gcs-connector` (hadoop3-2.2.22) |
| Hadoop already present | [`gcs-connector-hadoop3-2.2.22-shaded.jar`](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar) |

```xml
<!-- No Hadoop environment -->
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-gcp-bundle</artifactId>
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
  <groupId>com.google.cloud.bigdataoss</groupId>
  <artifactId>gcs-connector</artifactId>
  <version>hadoop3-2.2.22</version>
</dependency>
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
  <version>${GRAVITINO_VERSION}</version>
</dependency>
```

:::note
The thin `gravitino-gcp` jar is not needed. Its functionality is already included in both
`gravitino-gcp-bundle` and `gravitino-filesystem-hadoop3-runtime`.
:::

### GVFS Java client

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration), set the Google Cloud Storage
properties from the table above.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
conf.set("gcs-service-account-file", "/path/to/service-account.json");

Path filesetPath = new Path("gvfs://fileset/gcs_catalog/gcs_schema/example_fileset/new_dir");
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
    "/path/to/gcs-connector-hadoop3-2.2.22-shaded.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("gcs_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .config("spark.hadoop.gcs-service-account-file", "/path/to/service-account.json")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate())

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
spark_df = spark.createDataFrame(data, schema=["Name", "Age"])
gvfs_path = "gvfs://fileset/gcs_catalog/gcs_schema/example_fileset/people"

spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gvfs_path)
```

If Spark runs without a Hadoop environment, only the jar list changes:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-gcp-bundle-${gravitino-version}.jar,"
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
<property>
  <name>gcs-service-account-file</name>
  <value>/path/to/service-account.json</value>
</property>
```

2. Add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar` and [`gcs-connector-hadoop3-2.2.22-shaded.jar`](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar)
   to the Hadoop classpath.

3. Access the fileset:

```shell
${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/gcs_catalog/gcs_schema/example_fileset
${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/gcs_catalog/gcs_schema/example_fileset
```

### GVFS Python client

```bash
pip install apache-gravitino==${GRAVITINO_VERSION}
```

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration-1), pass the Google Cloud Storage
properties in `options`, spelled with underscores.

```python
from gravitino import gvfs

options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "gcs_service_account_file": "/path/to/service-account.json",
}

fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
fs.ls("gvfs://fileset/gcs_catalog/gcs_schema/example_fileset/")
```

### pandas

pandas reaches the same paths through `storage_options`.

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090",
    "metalake_name": "metalake",
    "options": {
        "gcs_service_account_file": "/path/to/service-account.json",
    }
}

ds = pd.read_csv("gvfs://fileset/gcs_catalog/gcs_schema/example_fileset/people/part-00000.csv",
                 storage_options=storage_options)
ds.head()
```

## Credential Vending

With credential vending the catalog holds the Google Cloud Storage credentials and the Gravitino server hands
out a credential per request, so clients never hold cloud keys of their own. See
[Credential Vending](./security/credential-vending.md) for the full provider reference.

The supported provider is `gcs-token`, which vends a short-lived token.

### Configure the catalog

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "gcs_catalog_with_vending",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Google Cloud Storage with credential vending",
  "properties": {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "/path/to/service-account.json",
    "credential-providers": "gcs-token"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

### Access without local credentials

Enable vending on the client and drop the credential properties.

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
// No need to set gcs-service-account-file

Path filesetPath = new Path("gvfs://fileset/gcs_catalog/gcs_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

```python
spark = (SparkSession.builder
    .appName("gcs_fileset")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    # No need to set gcs-service-account-file
    .getOrCreate())
```

```python
options = {
    "auth_type": "simple",
    "enable_credential_vending": True,
    # No need to set gcs-service-account-file
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
```
