---
title: "Fileset catalog with OSS"
slug: /fileset-catalog-with-oss
date: 2025-01-03
keyword: Fileset catalog OSS
license: "This software is licensed under the Apache License version 2."
---

This document explains how to configure a Fileset catalog with Aliyun OSS (Object Storage Service) in Gravitino.

## Prerequisites

To set up a Fileset catalog with OSS, follow these steps:

1. Download the [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) file.
2. Place the downloaded file into the Gravitino Fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server by running the following command:

```bash
$ ${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

Once the server is up and running, you can proceed to configure the Fileset catalog with OSS. In the rest of this document we will use `http://localhost:8090` as the Gravitino server URL, please replace it with your actual server URL.

## Configurations for creating a Fileset catalog with OSS

### Configuration for an OSS Fileset catalog

In addition to the basic configurations mentioned in [Fileset-catalog-catalog-configuration](./fileset-catalog.md#catalog-properties), the following properties are required to configure a Fileset catalog with OSS:

| Configuration item             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Default value   | Required | Since version    |
|--------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|------------------|
| `filesystem-providers`         | The file system providers to add. Set it to `oss` if it's a OSS fileset, or a comma separated string that contains `oss` like `oss,gs,s3` to support multiple kinds of fileset including `oss`.                                                                                                                                                                                                                                                                                                                     | (none)          | Yes      | 0.7.0-incubating |
| `default-filesystem-provider`  | The name default filesystem providers of this Fileset catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for OSS, if we set this value, we can omit the prefix 'oss://' in the location.                                                                                                                                                                                                                                                                                      | `builtin-local` | No       | 0.7.0-incubating |
| `oss-endpoint`                 | The endpoint of the Aliyun OSS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | (none)          | Yes      | 0.7.0-incubating |
| `oss-access-key-id`            | The access key of the Aliyun OSS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | (none)          | Yes      | 0.7.0-incubating |
| `oss-secret-access-key`        | The secret key of the Aliyun OSS.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | (none)          | Yes      | 0.7.0-incubating |
| `credential-providers`         | The credential provider types, separated by comma, possible value can be `oss-token`, `oss-secret-key`. As the default authentication type is using AKSK as the above, this configuration can enable credential vending provided by Gravitino server and client will no longer need to provide authentication information like AKSK to access OSS by GVFS. Once it's set, more configuration items are needed to make it works, please see [oss-credential-vending](security/credential-vending.md#oss-credentials) | (none)          | No       | 0.8.0-incubating |


### Configurations for a schema

To create a schema, refer to [Schema configurations](./fileset-catalog.md#schema-properties).

### Configurations for a fileset

For instructions on how to create a fileset, refer to [Fileset configurations](./fileset-catalog.md#fileset-properties) for more details.

## Example of creating Fileset catalog/schema/fileset with OSS

This section will show you how to use the Fileset catalog with OSS in Gravitino, including detailed examples.

### Step1: Create a Fileset catalog with OSS

First, you need to create a Fileset catalog for OSS. The following examples demonstrate how to create a Fileset catalog with OSS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_catalog",
  "type": "FILESET",
  "comment": "This is a OSS fileset catalog",
  "properties": {
    "location": "oss://bucket/root",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "filesystem-providers": "oss"
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

Map<String, String> ossProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root")
    .put("oss-access-key-id", "access_key")
    .put("oss-secret-access-key", "secret_key")
    .put("oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .put("filesystem-providers", "oss")
    .build();

Catalog ossCatalog = gravitinoClient.createCatalog("test_catalog",
    Type.FILESET,
    "This is a OSS fileset catalog",
    ossProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
oss_properties = {
    "location": "oss://bucket/root",
    "oss-access-key-id": "access_key"
    "oss-secret-access-key": "secret_key",
    "oss-endpoint": "ossProperties",
    "filesystem-providers": "oss"
}

oss_catalog = gravitino_client.create_catalog(name="test_catalog",
                                              catalog_type=Catalog.Type.FILESET,
                                              provider=None,
                                              comment="This is a OSS fileset catalog",
                                              properties=oss_properties)
```

</TabItem>
</Tabs>

### Step 2: Create a Schema

Once the Fileset catalog with OSS is created, you can create a schema inside that catalog. Below are examples of how to do this:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "This is a OSS schema",
  "properties": {
    "location": "oss://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("test_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("test_schema",
    "This is a OSS schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_schemas().create_schema(name="test_schema",
                                   comment="This is a OSS schema",
                                   properties={"location": "oss://bucket/root/schema"})
```

</TabItem>
</Tabs>


### Step3: Create a fileset

Now that the schema is created, you can create a fileset inside it. Here’s how:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "oss://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas/test_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("test_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

filesetCatalog.createFileset(
    NameIdentifier.of("test_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "oss://bucket/root/schema/example_fileset",
    propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("test_schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="oss://bucket/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Accessing a fileset with OSS

### Using the GVFS Java client to access the fileset

To access fileset with OSS using the GVFS Java client, based on the [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item      | Description                       | Default value | Required | Since version    |
|-------------------------|-----------------------------------|---------------|----------|------------------|
| `oss-endpoint`          | The endpoint of the Aliyun OSS.   | (none)        | Yes      | 0.7.0-incubating |
| `oss-access-key-id`     | The access key of the Aliyun OSS. | (none)        | Yes      | 0.7.0-incubating |
| `oss-secret-access-key` | The secret key of the Aliyun OSS. | (none)        | Yes      | 0.7.0-incubating |

:::note
If the catalog has enabled [credential vending](security/credential-vending.md), the properties above can be omitted. More details can be found in [Fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
conf.set("oss-endpoint", "http://localhost:8090");
conf.set("oss-access-key-id", "minio");
conf.set("oss-secret-access-key", "minio123"); 
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add OSS (bundle) jars to the classpath according to your environment.
If your wants to custom your hadoop version or there is already a hadoop version in your project, you can add the following dependencies to your `pom.xml`:

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
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
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
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

### Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.5.0 with Hadoop environment(Hadoop 3.3.4)** to access the fileset:

Before running the following code, you need to install required packages:

```bash
pip install pyspark==3.5.0
pip install apache-gravitino==${GRAVITINO_VERSION}
```
Then you can run the following code:

```python
from pyspark.sql import SparkSession
import os

gravitino_url = "http://localhost:8090"
metalake_name = "test"

catalog_name = "your_oss_catalog"
schema_name = "your_oss_schema"
fileset_name = "your_oss_fileset"

# JDK8 as follows, JDK17 will be slightly different, you need to add '--conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\"' to the submit args.
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-aliyun-{gravitino-version}.jar,"
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,"
    "/path/to/aliyun-sdk-oss-3.13.0.jar,"
    "/path/to/hadoop-aliyun-3.3.4.jar,"
    "/path/to/jdom2-2.0.6 "
    "--master local[1] pyspark-shell"
)
spark = SparkSession.builder
    .appName("oss_fileset_test")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "${_URL}")
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    .config("spark.hadoop.oss-access-key-id", os.environ["OSS_ACCESS_KEY_ID"])
    .config("spark.hadoop.oss-secret-access-key", os.environ["OSS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
columns = ["Name", "Age"]
spark_df = spark.createDataFrame(data, schema=columns)
gvfs_path = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people"

spark_df.coalesce(1).write
    .mode("overwrite")
    .option("header", "true")
    .csv(gvfs_path)
```

If your Spark **without Hadoop environment**, you can use the following code snippet to access the fileset:

```python
## Replace the following code snippet with the above code snippet with the same environment variables

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aliyun-bundle-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar, --master local[1] pyspark-shell"
```

- [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) is the Gravitino Aliyun jar with Hadoop environment(3.3.1) and `hadoop-oss` jar.
- [`gravitino-aliyun-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun) is a condensed version of the Gravitino Aliyun bundle jar without Hadoop environment and `hadoop-aliyun` jar.
-`hadoop-aliyun-3.3.4.jar`, `jdom2-2.0.6.jar`, and `aliyun-sdk-oss-3.13.0.jar` can be found in the Hadoop distribution in the `${HADOOP_HOME}/share/hadoop/tools/lib` directory.

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver, adding the bundle jars with '--jars' may not work. If this is the case, you should add the jars to the spark CLASSPATH directly.
:::

### Accessing a fileset using the Hadoop fs command

The following are examples of how to use the `hadoop fs` command to access the fileset in Hadoop 3.1.3.

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

2. Add the necessary jars to the Hadoop classpath.

For OSS, you need to add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, `gravitino-aliyun-${gravitino-version}.jar` and `hadoop-aliyun-${hadoop-version}.jar` located at `${HADOOP_HOME}/share/hadoop/tools/lib/` to Hadoop classpath. 

3. Run the following command to access the fileset:

```shell
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/oss_catalog/oss_schema/oss_fileset
./${HADOOP_HOME}/bin/hadoop dfs -put /path/to/local/file gvfs://fileset/oss_catalog/schema/oss_fileset
```

### Using the GVFS Python client to access a fileset

In order to access fileset with OSS using the GVFS Python client, apart from [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item      | Description                       | Default value | Required | Since version    |
|-------------------------|-----------------------------------|---------------|----------|------------------|
| `oss_endpoint`          | The endpoint of the Aliyun OSS.   | (none)        | Yes      | 0.7.0-incubating |
| `oss_access_key_id`     | The access key of the Aliyun OSS. | (none)        | Yes      | 0.7.0-incubating |
| `oss_secret_access_key` | The secret key of the Aliyun OSS. | (none)        | Yes      | 0.7.0-incubating |

:::note
If the catalog has enabled [credential vending](security/credential-vending.md), the properties above can be omitted.
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
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)

fs.ls("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/")
```


### Using fileset with pandas

The following are examples of how to use the pandas library to access the OSS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://mini.io:9000", 
    "metalake_name": "test",
    "options": {
        "oss_access_key_id": "access_key",
        "oss_secret_access_key": "secret_key",
        "oss_endpoint": "http://oss-cn-hangzhou.aliyuncs.com"
    }
}
ds = pd.read_csv(f"gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/people/part-00000-51d366e2-d5eb-448d-9109-32a96c8a14dc-c000.csv",
                 storage_options=storage_options)
ds.head()
```
For other use cases, please refer to the [Gravitino Virtual File System](./how-to-use-gvfs.md) document.

## Fileset with credential vending

Since 0.8.0-incubating, Gravitino supports credential vending for OSS fileset. If the catalog has been [configured with credential](./security/credential-vending.md), you can access OSS fileset without providing authentication information like `oss-access-key-id` and `oss-secret-access-key` in the properties.

### How to create an OSS Fileset catalog with credential vending

Apart from configuration method in [create-oss-fileset-catalog](#configuration-for-an-oss-fileset-catalog),
properties needed by [oss-credential](./security/credential-vending.md#oss-credentials)
should also be set to enable credential vending for OSS fileset. Take `oss-token` credential provider for example:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "oss-catalog-with-token",
  "type": "FILESET",
  "comment": "This is a OSS fileset catalog",
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
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

### How to access OSS fileset with credential vending

When the catalog is configured with credentials and client-side credential vending is enabled,
you can access OSS filesets directly using the GVFS Java/Python client or Spark without providing authentication details.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
// No need to set oss-access-key-id and oss-secret-access-key
Path filesetPath = new Path("gvfs://fileset/oss_test_catalog/test_schema/test_fileset/new_dir");
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
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    # No need to set oss-access-key-id and oss-secret-access-key
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.


