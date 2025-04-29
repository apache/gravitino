---
title: "Hadoop catalog with GCS"
slug: /hadoop-catalog-with-gcs
date: 2024-01-03
keyword: Hadoop catalog GCS
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Hadoop catalog with GCS.

## Prerequisites

To set up a Hadoop catalog with OSS:

1. Download the [`gravitino-gcp-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle) file.
1. Place the downloaded file into the Gravitino Hadoop catalog classpath
   at `${GRAVITINO_HOME}/catalogs/hadoop/libs/`.
1. Start the Gravitino server by running the following command:

   ```bash
   ${GRAVITINO_HOME}/bin/gravitino-server.sh start
   ```

Once the server is up and running, you can proceed to configure the Hadoop catalog with GCS.
In the rest of this document we will use `http://localhost:8090` as the Gravitino server URL,
please replace it with your actual server URL.

## Configurations for creating a Hadoop catalog with GCS

### Configurations for a GCS Hadoop catalog

Apart from configurations mentioned in [Hadoop catalog configuration](./hadoop-catalog.md#catalog-properties),
the following properties are required to configure a Hadoop catalog with GCS:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>filesystem-providers</tt></td>
  <td>
    The file system providers to add.
    Set it to `gcs` if it's a GCS fileset.
    A comma-separated string that contains `gcs` like `gcs,s3` can be used
    to support multiple kinds of fileset including `gcs`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>default-filesystem-provider</tt></td>
  <td>
    The name default filesystem providers of this Hadoop catalog
    if users do not specify the scheme in the URI.
    The default value is `builtin-local` for GCS.
    If set, we can omit the prefix 'gs://' in the location.
  </td>
  <td>`builtin-local`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gcs-service-account-file</tt></td>
  <td>The path of GCS service account JSON file.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td>
    The credential provider types, separated by comma.
    Possible value can be `gcs-token`.
    As the default authentication type is using service account as the above,
    this configuration can enable credential vending provided by Gravitino server
    and the client will no longer need to provide authentication information
    like service account to access GCS by GVFS.
    When set, more configuration items are needed to make it works.
    Please see [gcs-credential-vending](../../../security/credential-vending.md#gcs-credentials)
    for more details.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### Configurations for a schema

Refer to [Schema configurations](./hadoop-catalog.md#schema-properties) for more details.

### Configurations for a fileset

Refer to [Fileset configurations](./hadoop-catalog.md#fileset-properties) for more details.

## Example of creating Hadoop catalog with GCS

This section will show you how to use the Hadoop catalog with GCS in Gravitino,
including detailed examples.

### Step1: Create a Hadoop catalog with GCS

First, you need to create a Hadoop catalog with GCS.
The following example shows how to create a Hadoop catalog with GCS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >catalog.json
{
  "name": "mycatalog",
  "type": "FILESET",
  "comment": "This is a GCS fileset catalog",
  "provider": "hadoop",
  "properties": {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "path_of_gcs_service_account_file",
    "filesystem-providers": "gcs"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Map<String, String> gcsProperties = ImmutableMap.<String, String>builder()
    .put("location", "gs://bucket/root")
    .put("gcs-service-account-file", "path_of_gcs_service_account_file")
    .put("filesystem-providers", "gcs")
    .build();

Catalog gcsCatalog = gravitinoClient.createCatalog(
    "mycatalog", 
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a GCS fileset catalog",
    gcsProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
gcs_properties = {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "path_of_gcs_service_account_file",
    "filesystem-providers": "gcs"
}

gcs_properties = client.create_catalog(
     name="test_catalog",
     catalog_type=Catalog.Type.FILESET,
     provider="hadoop",
     comment="This is a GCS fileset catalog",
     properties=gcs_properties)
```

</TabItem>
</Tabs>

### Step2: Create a schema

Once you have created a Hadoop catalog with GCS, you can create a schema.
The following example shows how to create a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >schema.json
{
  "name": "test_schema",
  "comment": "This is a GCS schema",
  "properties": {
    "location": "gs://bucket/root/schema"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@schema.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas
```
</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("mycatalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "gs://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema(
    "myschema",
    "This is a GCS schema",
    schemaProperties
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="metalake")
catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_schemas().create_schema(
    name="myschema",
    comment="This is a GCS schema",
    properties={"location": "gs://bucket/root/schema"})
```
</TabItem>
</Tabs>

### Step3: Create a fileset

After creating a schema, you can create a fileset.
The following example shows how to create a fileset:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >fileset.json
{
  "name": "myfileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "gs://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@fileset.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs/mycatalog/schemas/myschema/filesets
```
</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("mymetalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("mycatalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("myschema", "myfileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "gs://bucket/root/schema/myfileset",
    propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")

catalog = client.load_catalog(name="mycatalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("myschema", "myfileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="gs://bucket/root/schema/myfileset",
    properties={"k1": "v1"})
```
</TabItem>
</Tabs>

## Accessing a fileset with GCS

### Using the GVFS Java client to access the fileset

To access fileset with GCS using the GVFS Java client,
based on the [basic GVFS configurations](../gvfs/index.md#java-gvfs-configuration),
you need to add the following configurations:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gcs-service-account-file</tt></td>
  <td>The path of GCS service account JSON file.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::note
If the catalog has enabled [credential vending](../../../security/credential-vending.md),
the properties above can be omitted.
More details can be found in [fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");
conf.set("gcs-service-account-file", "/path/your-service-account-file.json");
Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/myfileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

Similar to Spark configurations, you need to add GCS (bundle) JARs
to the class path according to your environment.
If your wants to custom your hadoop version or there is already a hadoop version in your project,
you can add the following dependencies to your `pom.xml`:

```xml
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>
  <dependency>
    <groupId>com.google.cloud.bigdataoss</groupId>
    <artifactId>gcs-connector</artifactId>
    <version>${GCS_CONNECTOR_VERSION}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-gcp</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

Or use the bundle jar with Hadoop environment if there is no Hadoop environment:

```xml
  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-gcp-bundle</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

### Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.1.3 with Hadoop environment(Hadoop 3.2.0)**
to access the fileset.

Before running the following code, you need to install required packages:

```bash
pip install pyspark==3.1.3
pip install apache-gravitino==${GRAVITINO_VERSION}
```
Then you can run the following code:

```python
from pyspark.sql import SparkSession
import os

gravitino_url = "http://localhost:8090"
metalake_name = "mymetalake"

catalog_name = "your_gcs_catalog"
schema_name = "your_gcs_schema"
fileset_name = "your_gcs_fileset"

jar_list = ".".join([
    "/path/to/gravitino-gcp-{gravitino-version}.jar",
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar",
    "/path/to/gcs-connector-hadoop3-2.2.22-shaded.jar"])
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jar_list} --master local[1] pyspark-shell"
spark = SparkSession.builder
    .appName("gcs_fielset_test")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "mymetalake")
    .config("spark.hadoop.gcs-service-account-file", "/path/to/gcs-service-account-file.json")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
columns = ["Name", "Age"]
spark_df = spark.createDataFrame(data, schema=columns)
gvfs_path = f"gvfs://fileset/{catalog}/{schema}/{fileset}/people"

spark_df.coalesce(1).write
    .mode("overwrite")
    .option("header", "true")
    .csv(gvfs_path)
```

If your Spark **doesn't have a Hadoop environment**,
you can use the following code snippet to access the fileset:

```python
jar_list = ".".join([
    "/path/to/gravitino-gcp-{gravitino-version}.jar",
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar",
])

## The environment variables need to be adjusted.
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars " + jar_list + " --master local[1] pyspark-shell"
```
- [`gravitino-gcp-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle)
  is the Gravitino GCP jar with Hadoop environment(3.3.1) and `gcs-connector`.
- [`gravitino-gcp-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp)
  is a condensed version of the Gravitino GCP bundle jar without Hadoop environment and
  [`gcs-connector`](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar) 

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver.
Adding the bundle jars with '--jars' may not work.
If this is the case, you should add the JARs to the spark CLASSPATH directly.
:::

### Accessing a fileset using the Hadoop fs command

The following are examples of how to use the `hadoop fs` command
to access the fileset in Hadoop 3.1.3.

1. Adding the following contents to the `${HADOOP_HOME}/etc/hadoop/core-site.xml` file:

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
     <value>test</value>
   </property>
 
   <property>
     <name>gcs-service-account-file</name>
     <value>/path/your-service-account-file.json</value>
   </property>
   ```

1. Add the necessary jars to the Hadoop classpath.

   For GCS, you need to add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`,
   `gravitino-gcp-${gravitino-version}.jar` and
   [`gcs-connector-hadoop3-2.2.22-shaded.jar`](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar)
   to Hadoop classpath.

1. Run the following command to access the fileset:

   ```shell
   ./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/gcs_catalog/gcs_schema/gcs_example
   ./${HADOOP_HOME}/bin/hadoop dfs -put /path/to/local/file gvfs://fileset/gcs_catalog/gcs_schema/gcs_example
   ```

### Using the GVFS Python client to access a fileset

In order to access fileset with GCS using the GVFS Python client,
apart from [basic GVFS configurations](../gvfs/index.md#python-gvfs-configuration),
you need to add the following configurations:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gcs_service_account_file</tt></td>
  <td>The path of GCS service account JSON file.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::note
If the catalog has enabled [credential vending](../../../security/credential-vending.md),
the properties above can be omitted.
:::

Please install the `gravitino` package before running the following code:

```bash
pip install apache-gravitino==${GRAVITINO_VERSION}
```

```python
from gravitino import gvfs
options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "gcs_service_account_file": "path_of_gcs_service_account_file.json",
}
fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090",
    metalake_name="mymetalake",
    options=options)
fs.ls("gvfs://fileset/{catalog}/{schema}/{fileset}/")
```

### Using fileset with pandas

The following are examples of how to use the pandas library
to access the GCS fileset:

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
    "metalake_name": "test",
    "options": {
        "gcs_service_account_file": "path_of_gcs_service_account_file.json",
    }
}
ds = pd.read_csv(f"gvfs://fileset/${catalog}/${schema}/${fileset}/people/fileset1.csv",
                 storage_options=storage_options)
ds.head()
```

For other use cases, please refer to the
[Gravitino Virtual File System](../gvfs/index.md) document.

## Fileset with credential vending

Starting from *0.8.0-incubating*, Gravitino supports credential vending for GCS fileset.
If the catalog has been [configured with credential](../../../security/credential-vending.md),
you can access GCS fileset without providing authentication information like `gcs-service-account-file`
in the properties.

### How to create a GCS Hadoop catalog with credential vending

Apart from configuration method in [create-gcs-hadoop-catalog](#configurations-for-a-gcs-hadoop-catalog),
properties needed by [gcs-credential](../../../security/credential-vending.md#gcs-credentials)
should also be set to enable credential vending for GCS fileset.
Take `gcs-token` credential provider for example:

```shell
cat <<EOF >catalog.json
{
  "name": "gcs-catalog-with-token",
  "type": "FILESET",
  "comment": "This is a GCS fileset catalog",
  "provider": "hadoop",
  "properties": {
    "location": "gs://bucket/root",
    "gcs-service-account-file": "path_of_gcs_service_account_file",
    "filesystem-providers": "gcs",
    "credential-providers": "gcs-token"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

### How to access GCS fileset with credential vending

When the catalog is configured with credentials and client-side credential vending is enabled,
you can access GCS filesets directly using the GVFS Java/Python client or Spark
without providing authentication details.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");
// No need to set gcs-service-account-file
Path filesetPath = new Path("gvfs://fileset/gcs_test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

Spark:

```python
spark = SparkSession.builder
    .appName("gcs_fileset_test")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "mymetalake")
    # No need to set gcs-service-account-file
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.

