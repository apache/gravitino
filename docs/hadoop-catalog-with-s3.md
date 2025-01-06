---
title: "Hadoop catalog with S3"
slug: /hadoop-catalog-with-s3
date: 2025-01-03
keyword: Hadoop catalog S3
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Hadoop catalog with S3. 

## Prerequisites

In order to create a Hadoop catalog with S3, you need to place [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle) in Gravitino Hadoop classpath located 
at `${HADOOP_HOME}/share/hadoop/common/lib/`. After that, start Gravitino server with the following command:

```bash
$ bin/gravitino-server.sh start
```

## Create a Hadoop Catalog with S3

### Catalog a S3 Hadoop catalog

Apart from configuration method in [Hadoop-catalog-catalog-configuration](./hadoop-catalog.md#catalog-properties), the following properties are required to configure a Hadoop catalog with S3:

| Configuration item            | Description                                                                                                                                                                                                                  | Default value   | Required                  | Since version    |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------------------------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `s3` if it's a S3 fileset, or a comma separated string that contains `s3` like `gs,s3` to support multiple kinds of fileset including `s3`.                                      | (none)          | Yes                       | 0.7.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for S3, if we set this value, we can omit the prefix 's3a://' in the location. | `builtin-local` | No                        | 0.7.0-incubating |
| `s3-endpoint`                 | The endpoint of the AWS S3.                                                                                                                                                                                                  | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |
| `s3-access-key-id`            | The access key of the AWS S3.                                                                                                                                                                                                | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |
| `s3-secret-access-key`        | The secret key of the AWS S3.                                                                                                                                                                                                | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |

### Create a schema

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

### Create a fileset

Refer to [Fileset operation](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.


## Using Hadoop catalog with S3

The rest of this document shows how to use the Hadoop catalog with S3 in Gravitino with a full example.

### Create a Hadoop catalog/schema/file set with S3

First of all, you need to create a Hadoop catalog with S3. The following example shows how to create a Hadoop catalog with S3:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
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

s3Properties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root")
    .put("s3-access-key-id", "access_key")
    .put("s3-secret-access-key", "secret_key")
    .put("s3-endpoint", "http://s3.ap-northeast-1.amazonaws.com")
    .put("filesystem-providers", "s3")
    .build();

Catalog s3Catalog = gravitinoClient.createCatalog("test_catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
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
    "s3-endpoint": "http://s3.ap-northeast-1.amazonaws.com"
}

s3_catalog = gravitino_client.create_catalog(name="test_catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a S3 fileset catalog",
                                             properties=s3_properties)

```

</TabItem>
</Tabs>

:::note
The value of location should always start with `s3a` NOT `s3` for AWS S3, for instance, `s3a://bucket/root`. Value like `s3://bucket/root` is not supported due to the limitation of the hadoop-aws library.
:::

Then create a schema and a fileset in the catalog created above. 

Using the following code to create a schema and fileset:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "comment",
  "properties": {
    "location": "s3a://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "s3a://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("test_schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_schemas().create_schema(name="test_schema",
                                   comment="This is a schema",
                                   properties={"location": "s3a://bucket/root/schema"})
```

</TabItem>
</Tabs>

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


### Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.1.3 with Hadoop environment(Hadoop 3.2.0)** to access the fileset:

```python
import logging
from gravitino import NameIdentifier, GravitinoClient, Catalog, Fileset, GravitinoAdminClient
from pyspark.sql import SparkSession
import os

gravitino_url = "http://localhost:8090"
metalake_name = "test"

catalog_name = "your_s3_catalog"
schema_name = "your_s3_schema"
fileset_name = "your_s3_fileset"

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aws-${gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}-SNAPSHOT.jar,/path/to/hadoop-aws-3.2.0.jar,/path/to/aws-java-sdk-bundle-1.11.375.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
  .appName("s3_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "${GRAVITINO_SERVER_URL}")
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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aws-${gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}-SNAPSHOT.jar,/path/to/hadoop-aws-3.2.0.jar,/path/to/aws-java-sdk-bundle-1.11.375.jar --master local[1] pyspark-shell"
```

- [`gravitino-aws-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle) is the Gravitino AWS jar with Hadoop environment and `hadoop-aws` jar.
- [`gravitino-aws-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws) is a condensed version of the Gravitino AWS bundle jar without Hadoop environment and `hadoop-aws` jar.

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver, adding the bundle jars with '--jars' may not work. If this is the case, you should add the jars to the spark CLASSPATH directly.
:::

### Using Gravitino virtual file system Java client to access the fileset

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","${GRAVITINO_SERVER_IP:PORT}");
conf.set("fs.gravitino.client.metalake","test_metalake");

conf.set("s3-endpoint", "${GRAVITINO_SERVER_IP:PORT}");
conf.set("s3-access-key-id", "minio");
conf.set("s3-secret-access-key", "minio123");

Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add S3 bundle jars to the classpath according to your environment.

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
    <value>${GRAVITINO_SERVER_IP:PORT}</value>
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

2. Copy the necessary jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

For S3, you need to copy `gravitino-aws-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directoryl,
then copy hadoop-aws-{version}.jar and related dependencies to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. Those jars can be found in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory, you can add all the jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.


3. Run the following command to access the fileset:

```shell
hadoop dfs -ls gvfs://fileset/s3_catalog/schema/example
hadoop dfs -put /path/to/local/file gvfs://fileset/s3_catalog/schema/example
```

### Using the Gravitino virtual file system Python client to access a fileset

```python
from gravitino import gvfs
options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "s3_endpoint": "${GRAVITINO_SERVER_IP:PORT}",
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
For other use cases, please refer to the [Gravitino Virtual File System](./how-to-use-gvfs.md) document.

## Fileset with credential

Since 0.8.0-incubating, Gravitino supports credential vending for S3 fileset. If the catalog has been configured with credential, you can access S3 fileset without providing authentication information like `s3-access-key-id` and `s3-secret-access-key` in the properties.

### How to create a S3 Hadoop catalog with credential enabled

Apart from configuration method in [create-s3-hadoop-catalog](#catalog-a-catalog), properties needed by [s3-credential](./security/credential-vending.md#s3-credentials) should also be set to enable credential vending for S3 fileset.

### How to access S3 fileset with credential

If the catalog has been configured with credential, you can access S3 fileset without providing authentication information via GVFS. Let's see how to access S3 fileset with credential:

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// No need to set s3-access-key-id and s3-secret-access-key
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
  .appName("s3_fielset_test")
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


