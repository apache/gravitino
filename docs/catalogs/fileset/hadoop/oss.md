---
title: "Hadoop catalog with OSS"
slug: /hadoop-catalog-with-oss
date: 2025-01-03
keyword: Hadoop catalog OSS
license: "This software is licensed under the Apache License version 2."
---

This document explains how to configure a Hadoop catalog
with Aliyun OSS (Object Storage Service) in Gravitino.

## Prerequisites

To set up a Hadoop catalog with OSS, follow these steps:

1. Download the `gravitino-aliyun-bundle-${gravitino-version}.jar` JAR
   from [maven repository](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle).

1. Place the downloaded file into the Gravitino Hadoop catalog class path
   at `${GRAVITINO_HOME}/catalogs/hadoop/libs/`.

1. Start the Gravitino server by running the following command:

   ```bash
   ${GRAVITINO_HOME}/bin/gravitino-server.sh start
   ```

Once the server is up and running, you can proceed to configure the Hadoop catalog with OSS.
In the rest of this document, we will use `http://localhost:8090` as the Gravitino server URL.
Pplease replace it with your actual server URL.

## Configurations for creating a Hadoop catalog with OSS

### Configuration for an OSS Hadoop catalog

In addition to the basic configurations mentioned in
[Hadoop-catalog-catalog-configuration](./hadoop-catalog.md#catalog-properties),
the following properties are required to configure a Hadoop catalog with OSS:

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
    Set it to `oss` if it's a OSS fileset, or a comma separated string that contains `oss`
    like `oss,gs,s3` to support multiple kinds of fileset including `oss`.
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
    The default value is `builtin-local` for OSS.
    If we set this value, we can omit the prefix 'oss://' in the <tt>location</tt>.
  </td>
  <td>`builtin-local`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-endpoint</tt></td>
  <td>The endpoint of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td>The access key of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td>The secret key of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td>
    The credential provider types, separated by comma.
    Valid values can be `oss-token`, `oss-secret-key`.
    As the default authentication type is using AKSK as the above,
    this configuration can enable credential vending provided
    by Gravitino server and client will no longer need
    to provide authentication information like AKSK
    to access OSS by GVFS.

    When set, more configuration items are needed to make it works.
    Please see [oss-credential-vending](../../../security/credential-vending.md#oss-credentials)
    for more details.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### Configurations for a schema

To create a schema, refer to [schema configurations](./hadoop-catalog.md#schema-properties).
for more details.

### Configurations for a fileset

For instructions on how to create a fileset,
refer to [fileset configurations](./hadoop-catalog.md#fileset-properties)
for more details.

## Example of creating Hadoop catalog/schema/fileset with OSS

This section will show you how to use the Hadoop catalog with OSS in Gravitino,
including detailed examples.

### Step1: Create a Hadoop catalog with OSS

First, you need to create a Hadoop catalog for OSS.
The following examples demonstrate how to create a Hadoop catalog with OSS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >catalog.json
{
  "name": "mycatalog",
  "type": "FILESET",
  "comment": "This is a OSS fileset catalog",
  "provider": "hadoop",
  "properties": {
    "location": "oss://bucket/root",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "filesystem-providers": "oss"
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

Map<String, String> ossProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root")
    .put("oss-access-key-id", "access_key")
    .put("oss-secret-access-key", "secret_key")
    .put("oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .put("filesystem-providers", "oss")
    .build();

Catalog ossCatalog = gravitinoClient.createCatalog(
    "mycatalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a OSS fileset catalog",
    ossProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
oss_properties = {
    "location": "oss://bucket/root",
    "oss-access-key-id": "access_key"
    "oss-secret-access-key": "secret_key",
    "oss-endpoint": "ossProperties",
    "filesystem-providers": "oss"
}

oss_catalog = client.create_catalog(
    name="test_catalog",
    catalog_type=Catalog.Type.FILESET,
    provider="hadoop",
    comment="This is a OSS fileset catalog",
    properties=oss_properties)
```

</TabItem>
</Tabs>

### Step 2: Create a Schema

Once the Hadoop catalog with OSS is created, you can create a schema inside that catalog.
Below are examples of how to do this:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >schema.json
{
  "name": "myschema",
  "comment": "This is a OSS schema",
  "properties": {
    "location": "oss://bucket/root/schema"
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
    .put("location", "oss://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema(
    "myschema",
    "This is a OSS schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
client = GravitinoClient(uri="http://localhost:8090",
                         metalake_name="mymetalake")
catalo = client.load_catalog(name="mycatalog")
catalog.as_schemas().create_schema(
    name="test_schema",
    comment="This is a OSS schema",
    properties={"location": "oss://bucket/root/schema"})
```
</TabItem>
</Tabs>

### Step3: Create a fileset

Now that the schema is created, you can create a fileset inside it.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
cat <<EOF >fileset.json
{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "oss://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@fileset.json'
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
    "oss://bucket/root/schema/example_fileset",
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
    storage_location="oss://bucket/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Accessing a fileset with OSS

### Using the GVFS Java client to access the fileset

To access fileset with OSS using the GVFS Java client,
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
  <td><tt>oss-endpoint</tt></td>
  <td>The endpoint of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td>The access key of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td>The secret key of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

:::note
If the catalog has enabled [credential vending](../../../security/credential-vending.md),
the properties above can be omitted.
More details can be found in [Fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");
conf.set("oss-endpoint", "http://localhost:8090");
conf.set("oss-access-key-id", "minio");
conf.set("oss-secret-access-key", "minio123"); 
Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/myfileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add OSS (bundle) jars
to the class path in your environment.
If you want to customize your Hadoop version or
there is already a Hadoop version in your project,
you can add the following dependencies to your `pom.xml`:

```xml
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aliyun</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-aliyun</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

Or use the bundle jar with Hadoop environment if there is no Hadoop environment:

```xml
  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-aliyun-bundle</artifactId>
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
to access the fileset:

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

catalog_name = "your_oss_catalog"
schema_name = "your_oss_schema"
fileset_name = "your_oss_fileset"

jars = ".".join([
    "/path/to/gravitino-aliyun-{gravitino-version}.jar",
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar",
    "/path/to/aliyun-sdk-oss-2.8.3.jar",
    "/path/to/hadoop-aliyun-3.2.0.jar",
    "/path/to/jdom-1.1.jar"])

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars " + jars + " --master local[1] pyspark-shell"
spark = SparkSession.builder
    .appName("oss_fileset_test")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "${_URL}")
    .config("spark.hadoop.fs.gravitino.client.metalake", "mymetalake")
    .config("spark.hadoop.oss-access-key-id", os.environ["OSS_ACCESS_KEY_ID"])
    .config("spark.hadoop.oss-secret-access-key", os.environ["OSS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
columns = ["Name", "Age"]
spark_df = spark.createDataFrame(data, schema=columns)
gvfs_path = f"gvfs://fileset/mycatalog/myschema/myfileset/people"

spark_df.coalesce(1).write
    .mode("overwrite")
    .option("header", "true")
    .csv(gvfs_path)
```

If your Spark **doesn't have a Hadoop environment**,
you can use the following code snippet to access the fileset:

```python
jars = ".".join([
    "/path/to/gravitino-aliyun-{gravitino-version}.jar",
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar",
])

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars " + jars + " --master local[1] pyspark-shell"
```

- The `gravitino-aliyun-bundle-${gravitino-version}.jar` file is 
  is the Gravitino Aliyun jar with Hadoop environment(3.3.1) and `hadoop-oss` jar.
  It can be downloaded from the [maven repository](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle).
 
- The `gravitino-aliyun-${gravitino-version}.jar` file is 
  a condensed version of the Gravitino Aliyun bundle JAR.
  It is available on the [mave repository](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun).
  It doesn't contain the Hadoop environment or the `hadoop-aliyun` JAR.

- `hadoop-aliyun-3.2.0.jar` and `aliyun-sdk-oss-2.8.3.jar` can be found
  in the Hadoop distribution under the `${HADOOP_HOME}/share/hadoop/tools/lib` directory.

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver.
Adding the bundle JARs with '--jars' may not work.
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
     <name>oss-endpoint</name>
     <value>http://oss-cn-hangzhou.aliyuncs.com</value>
   </property>
   
   <property>
     <name>oss-access-key-id</name>
     <value>access-key</value>
   </property>
   
   <property>
   <name>oss-secret-access-key</name>
     <value>secret-key</value>
   </property>
   ```

1. Add the necessary JARs to the Hadoop classpath.

   For OSS, you need to add
   `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`,
   `gravitino-aliyun-${gravitino-version}.jar` and
   `hadoop-aliyun-${hadoop-version}.jar` located at `${HADOOP_HOME}/share/hadoop/tools/lib/`
   to the class path for Hadoop. 

1. Run the following command to access the fileset:

   ```shell
   ./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/oss_catalog/oss_schema/oss_fileset
   ./${HADOOP_HOME}/bin/hadoop dfs -put /path/to/local/file gvfs://fileset/oss_catalog/schema/oss_fileset
   ```

### Using the GVFS Python client to access a fileset

In order to access fileset with OSS using the GVFS Python client,
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
  <td><tt>oss_endpoint</tt></td>
  <td>The endpoint of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss_access_key_id</tt></td>
  <td>The access key of the Aliyun OSS.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss_secret_access_key</tt></td>
  <td>The secret key of the Aliyun OSS.</td>
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
    "oss_endpoint": "http://localhost:8090",
    "oss_access_key_id": "minio",
    "oss_secret_access_key": "minio123"
}
fs = gvfs.GravitinoVirtualFileSystem(
        server_uri="http://localhost:8090",
        metalake_name="mymetalake",
        options=options)

fs.ls("gvfs://fileset/mycatalog/myschema/myfileset/")
```

### Using fileset with pandas

The following are examples of how to use the 'pandas' library
to access the OSS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
    "metalake_name": "mymeatalake",
    "options": {
        "oss_access_key_id": "access_key",
        "oss_secret_access_key": "secret_key",
        "oss_endpoint": "http://oss-cn-hangzhou.aliyuncs.com"
    }
}
ds = pd.read_csv("gvfs://fileset/mycatalog/myschema/myfileset/people/part235.csv",
                 storage_options=storage_options)
ds.head()
```
For other use cases, refer to the [Gravitino Virtual File System](../gvfs/index.md) document.

## Fileset with credential vending

Since *0.8.0-incubating*, Gravitino supports credential vending for OSS fileset.
If the catalog has been [configured with credential](../../../security/credential-vending.md),
you can access OSS fileset without providing authentication information
like `oss-access-key-id` and `oss-secret-access-key` in the properties.

### How to create an OSS Hadoop catalog with credential vending

Apart from configuration method in [create-oss-hadoop-catalog](#configuration-for-an-oss-hadoop-catalog),
properties needed by [oss-credential](../../../security/credential-vending.md#oss-credentials)
should also be set to enable credential vending for OSS fileset.
Take `oss-token` credential provider for example:

```shell
cat <<EOF >catalog.json
{
  "name": "oss-catalog-with-token",
  "type": "FILESET",
  "comment": "This is a OSS fileset catalog",
  "provider": "hadoop",
  "properties": {
    "location": "oss://bucket/root",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "filesystem-providers": "oss",
    "credential-providers": "oss-token",
    "oss-region":"oss-cn-hangzhou",
    "oss-role-arn":"The ARN of the role to access the OSS data"
  }
}
EOF

curl -X POST \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '@catalog.json' \
  http://localhost:8090/api/metalakes/mymetalake/catalogs
```

### How to access OSS fileset with credential vending

When the catalog is configured with credentials and client-side credential vending is enabled,
you can access OSS filesets directly using the GVFS Java/Python client or Spark
without providing authentication details.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "mymetalake");
// No need to set oss-access-key-id and oss-secret-access-key
Path filesetPath = new Path("gvfs://fileset/mycatalog/myschema/myfileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
    .appName("oss_fileset_test")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "mymetalake")
    # No need to set oss-access-key-id and oss-secret-access-key
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.

