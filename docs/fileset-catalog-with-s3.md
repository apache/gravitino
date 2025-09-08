---
title: "Fileset catalog with S3"
slug: /fileset-catalog-with-s3
date: 2025-01-03
keyword: Fileset catalog S3
license: "This software is licensed under the Apache License version 2."
---

This document explains how to configure a Fileset catalog with S3 in Gravitino.

## Prerequisites

To create a Fileset catalog with S3, follow these steps:

1. Download the [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle) file.
2. Place this file in the Gravitino Fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server using the following command:

```bash
$ ${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

Once the server is up and running, you can proceed to configure the Fileset catalog with S3. In the rest of this document we will use `http://localhost:8090` as the Gravitino server URL, please replace it with your actual server URL.

## Configurations for creating a Fileset catalog with S3

### Configurations for S3 Fileset Catalog

In addition to the basic configurations mentioned in [Fileset-catalog-catalog-configuration](./fileset-catalog.md#catalog-properties), the following properties are necessary to configure a Fileset catalog with S3:

| Configuration item             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Default value   | Required | Since version    |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|------------------|
| `filesystem-providers`         | The file system providers to add. Set it to `s3` if it's a S3 fileset, or a comma separated string that contains `s3` like `gs,s3` to support multiple kinds of fileset including `s3`.                                                                                                                                                                                                                                                                                                                        | (none)          | Yes      | 0.7.0-incubating |
| `default-filesystem-provider`  | The name default filesystem providers of this Fileset catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for S3, if we set this value, we can omit the prefix 's3a://' in the location.                                                                                                                                                                                                                                                                                  | `builtin-local` | No       | 0.7.0-incubating |
| `s3-endpoint`                  | The endpoint of the AWS S3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | (none)          | Yes      | 0.7.0-incubating |
| `s3-access-key-id`             | The access key of the AWS S3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | (none)          | Yes      | 0.7.0-incubating |
| `s3-secret-access-key`         | The secret key of the AWS S3.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | (none)          | Yes      | 0.7.0-incubating |
| `credential-providers`         | The credential provider types, separated by comma, possible value can be `s3-token`, `s3-secret-key`. As the default authentication type is using AKSK as the above, this configuration can enable credential vending provided by Gravitino server and client will no longer need to provide authentication information like AKSK to access S3 by GVFS. Once it's set, more configuration items are needed to make it works, please see [s3-credential-vending](security/credential-vending.md#s3-credentials) | (none)          | No       | 0.8.0-incubating |

### Configurations for a schema

To learn how to create a schema, refer to [Schema configurations](./fileset-catalog.md#schema-properties).

### Configurations for a fileset

For more details on creating a fileset, Refer to [Fileset configurations](./fileset-catalog.md#fileset-properties).

## Using the Fileset catalog with S3

This section demonstrates how to use the Fileset catalog with S3 in Gravitino, with a complete example.

### Step1: Create a Fileset Catalog with S3

First of all, you need to create a Fileset catalog with S3. The following example shows how to create a Fileset catalog with S3:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_catalog",
  "type": "FILESET",
  "comment": "This is a S3 fileset catalog",
  "properties": {
    "location": "s3a://bucket/root",
    "s3-access-key-id": "access_key",
    "s3-secret-access-key": "secret_key",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "filesystem-providers": "s3"
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

Map<String, String> s3Properties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root")
    .put("s3-access-key-id", "access_key")
    .put("s3-secret-access-key", "secret_key")
    .put("s3-endpoint", "http://s3.ap-northeast-1.amazonaws.com")
    .put("filesystem-providers", "s3")
    .build();

Catalog s3Catalog = gravitinoClient.createCatalog("test_catalog",
    Type.FILESET,
    "This is a S3 fileset catalog",
    s3Properties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
s3_properties = {
    "location": "s3a://bucket/root",
    "s3-access-key-id": "access_key"
    "s3-secret-access-key": "secret_key",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "filesystem-providers": "s3"
}

s3_catalog = gravitino_client.create_catalog(name="test_catalog",
                                             catalog_type=Catalog.Type.FILESET,
                                             provider=None,
                                             comment="This is a S3 fileset catalog",
                                             properties=s3_properties)
```

</TabItem>
</Tabs>

:::note
When using S3, ensure that the location value starts with s3a:// (not s3://) for AWS S3. For example, use s3a://bucket/root, as the s3:// format is not supported by the hadoop-aws library.
:::

### Step2: Create a schema

Once your Fileset catalog with S3 is created, you can create a schema under the catalog. Here are examples of how to do that:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "This is a S3 schema",
  "properties": {
    "location": "s3a://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("test_schema",
    "This is a S3 schema",
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
                                   comment="This is a S3 schema",
                                   properties={"location": "s3a://bucket/root/schema"})
```

</TabItem>
</Tabs>

### Step3: Create a fileset

After creating the schema, you can create a fileset. Here are examples for creating a fileset:

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
    "s3a://bucket/root/schema/example_fileset",
    propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="s3a://bucket/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Accessing a fileset with S3

### Using the GVFS Java client to access the fileset

To access fileset with S3 using the GVFS Java client, based on the [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item     | Description                   | Default value | Required | Since version    |
|------------------------|-------------------------------|---------------|----------|------------------|
| `s3-endpoint`          | The endpoint of the AWS S3.   | (none)        | Yes      | 0.7.0-incubating |
| `s3-access-key-id`     | The access key of the AWS S3. | (none)        | Yes      | 0.7.0-incubating |
| `s3-secret-access-key` | The secret key of the AWS S3. | (none)        | Yes      | 0.7.0-incubating |

:::note
- If the catalog has enabled [credential vending](security/credential-vending.md), the properties above can be omitted. More details can be found in [Fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
conf.set("s3-endpoint", "http://localhost:8090");
conf.set("s3-access-key-id", "minio");
conf.set("s3-secret-access-key", "minio123");

Path filesetPath = new Path("gvfs://fileset/adls_catalog/adls_schema/adls_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add S3 (bundle) jars to the classpath according to your environment.

```xml
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

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-aws</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

Or use the bundle jar with Hadoop environment if there is no Hadoop environment:


```xml
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

### Using Spark to access the fileset

The following Python code demonstrates how to use **PySpark 3.5.0 with Hadoop environment(Hadoop 3.3.4)** to access the fileset:

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

catalog_name = "your_s3_catalog"
schema_name = "your_s3_schema"
fileset_name = "your_s3_fileset"

# JDK8 as follows, JDK17 will be slightly different, you need to add '--conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\"' to the submit args.
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aws-${gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}-SNAPSHOT.jar,/path/to/hadoop-aws-3.3.4.jar,/path/to/aws-java-sdk-bundle-1.12.262.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
    .appName("s3_fileset_test")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    .config("spark.hadoop.s3-access-key-id", os.environ["S3_ACCESS_KEY_ID"])
    .config("spark.hadoop.s3-secret-access-key", os.environ["S3_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.s3-endpoint", "http://s3.ap-northeast-1.amazonaws.com")
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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aws-bundle-${gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}-SNAPSHOT.jar --master local[1] pyspark-shell"
```

- [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle) is the Gravitino AWS jar with Hadoop environment(3.3.1) and `hadoop-aws` jar.
- [`gravitino-aws-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws) is a condensed version of the Gravitino AWS bundle jar without Hadoop environment and `hadoop-aws` jar.
- `hadoop-aws-3.3.4.jar` and `aws-java-sdk-bundle-1.12.262.jar` can be found in the Hadoop distribution in the `${HADOOP_HOME}/share/hadoop/tools/lib` directory.

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
    <name>s3-endpoint</name>
    <value>http://s3.ap-northeast-1.amazonaws.com</value>
  </property>

  <property>
    <name>s3-access-key-id</name>
    <value>access-key</value>
  </property>
  
  <property>
  <name>s3-secret-access-key</name>
    <value>secret-key</value>
  </property>
```

2. Add the necessary jars to the Hadoop classpath. 

For S3, you need to add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, `gravitino-aws-${gravitino-version}.jar` and `hadoop-aws-${hadoop-version}.jar` located at `${HADOOP_HOME}/share/hadoop/tools/lib/` to Hadoop classpath. 

3. Run the following command to access the fileset:

```shell
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/s3_catalog/s3_schema/s3_fileset
./${HADOOP_HOME}/bin/hadoop dfs -put /path/to/local/file gvfs://fileset/s3_catalog/s3_schema/s3_fileset
```

### Using the GVFS Python client to access a fileset

In order to access fileset with S3 using the GVFS Python client, apart from [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item     | Description                                                                                                                                  | Default value | Required | Since version    |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `s3_endpoint`          | The endpoint of the AWS S3. This configuration is optional for S3 service, but required for other S3-compatible storage services like MinIO. | (none)        | No       | 0.7.0-incubating |
| `s3_access_key_id`     | The access key of the AWS S3.                                                                                                                | (none)        | Yes      | 0.7.0-incubating |
| `s3_secret_access_key` | The secret key of the AWS S3.                                                                                                                | (none)        | Yes      | 0.7.0-incubating |

:::note
- `s3_endpoint` is an optional configuration for GVFS **Python** client but a required configuration for GVFS **Java** client to access Hadoop with AWS S3, and it is required for other S3-compatible storage services like MinIO.
- If the catalog has enabled [credential vending](security/credential-vending.md), the properties above can be omitted.
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
    "s3_endpoint": "http://localhost:8090",
    "s3_access_key_id": "minio",
    "s3_secret_access_key": "minio123"
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)
fs.ls("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/")                                                                         ")
```

### Using fileset with pandas

The following are examples of how to use the pandas library to access the S3 fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
    "metalake_name": "test",
    "options": {
        "s3_access_key_id": "access_key",
        "s3_secret_access_key": "secret_key",
        "s3_endpoint": "http://s3.ap-northeast-1.amazonaws.com"
    }
}
ds = pd.read_csv(f"gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/people/part-00000-51d366e2-d5eb-448d-9109-32a96c8a14dc-c000.csv",
                 storage_options=storage_options)
ds.head()
```

For more use cases, please refer to the [Gravitino Virtual File System](./how-to-use-gvfs.md) document.

## Fileset with credential vending

Since 0.8.0-incubating, Gravitino supports credential vending for S3 fileset. If the catalog has been [configured with credential](./security/credential-vending.md), you can access S3 fileset without providing authentication information like `s3-access-key-id` and `s3-secret-access-key` in the properties.

### How to create a S3 Fileset catalog with credential vending

Apart from configuration method in [create-s3-fileset-catalog](#configurations-for-s3-fileset-catalog),
properties needed by [s3-credential](./security/credential-vending.md#s3-credentials)
should also be set to enable credential vending for S3 fileset. Take `s3-token` credential provider for example:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "s3-catalog-with-token",
  "type": "FILESET",
  "comment": "This is a S3 fileset catalog",
  "properties": {
    "location": "s3a://bucket/root",
    "s3-access-key-id": "access_key",
    "s3-secret-access-key": "secret_key",
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com",
    "filesystem-providers": "s3",
    "credential-providers": "s3-token",
    "s3-region":"ap-northeast-1",
    "s3-role-arn":"The ARN of the role to access the S3 data"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

### How to access S3 fileset with credential vending

When the catalog is configured with credentials and client-side credential vending is enabled,
you can access S3 filesets directly using the GVFS Java/Python client or Spark without providing authentication details.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
// No need to set s3-access-key-id and s3-secret-access-key
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
    .appName("s3_fileset_test")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    # No need to set s3-access-key-id and s3-secret-access-key
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.


