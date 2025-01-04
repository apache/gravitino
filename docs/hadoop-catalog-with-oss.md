---
title: "Hadoop catalog with OSS"
slug: /hadoop-catalog-with-oss
date: 2025-01-03
keyword: Hadoop catalog OSS
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Hadoop catalog with Aliyun OSS.

## Prerequisites

In order to create a Hadoop catalog with OSS, you need to place [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) in Gravitino Hadoop classpath located
at `${HADOOP_HOME}/share/hadoop/common/lib/`. After that, start Gravitino server with the following command:

```bash
$ bin/gravitino-server.sh start
```

## Create a Hadoop Catalog with OSS in Gravitino

### Catalog a catalog

Apart from configuration method in [Hadoop-catalog-catalog-configuration](./hadoop-catalog.md#catalog-properties), the following properties are required to configure a Hadoop catalog with OSS:

| Configuration item            | Description                                                                                                                                                                                                                   | Default value   | Required                   | Since version    |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------------------------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `oss` if it's a OSS fileset, or a comma separated string that contains `oss` like `oss,gs,s3` to support multiple kinds of fileset including `oss`.                               | (none)          | Yes                        | 0.7.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for OSS, if we set this value, we can omit the prefix 'oss://' in the location. | `builtin-local` | No                         | 0.7.0-incubating |
| `oss-endpoint`                | The endpoint of the Aliyun OSS.                                                                                                                                                                                               | (none)          | Yes if it's a OSS fileset. | 0.7.0-incubating |
| `oss-access-key-id`           | The access key of the Aliyun OSS.                                                                                                                                                                                             | (none)          | Yes if it's a OSS fileset. | 0.7.0-incubating |
| `oss-secret-access-key`       | The secret key of the Aliyun OSS.                                                                                                                                                                                             | (none)          | Yes if it's a OSS fileset. | 0.7.0-incubating |

### Create a schema

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

### Create a fileset

Refer to [Fileset operation](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.


## Using Hadoop catalog with OSS

### Create a Hadoop catalog/schema/file set with OSS

First, you need to create a Hadoop catalog with OSS. The following example shows how to create a Hadoop catalog with OSS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
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

ossProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root")
    .put("oss-access-key-id", "access_key")
    .put("oss-secret-access-key", "secret_key")
    .put("oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .put("filesystem-providers", "oss")
    .build();

Catalog ossCatalog = gravitinoClient.createCatalog("catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
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
    "oss-endpoint": "ossProperties"
}

oss_catalog = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a OSS fileset catalog",
                                             properties=oss_properties)

```

</TabItem>
</Tabs>

Then create a schema and fileset in the catalog created above.

Using the following code to create a schema and fileset:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "location": "oss://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Assuming you have just created a Hive catalog named `hive_catalog`
Catalog catalog = gravitinoClient.loadCatalog("hive_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://127.0.0.1:8090", metalake_name="metalake")
catalog: Catalog = gravitino_client.load_catalog(name="hive_catalog")
catalog.as_schemas().create_schema(name="schema",
                                   comment="This is a schema",
                                   properties={"location": "oss://bucket/root/schema"})
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
  "storageLocation": "oss://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas/schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

Catalog catalog = gravitinoClient.loadCatalog("catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> propertiesMap = ImmutableMap.<String, String>builder()
        .put("k1", "v1")
        .build();

filesetCatalog.createFileset(
  NameIdentifier.of("schema", "example_fileset"),
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

catalog: Catalog = gravitino_client.load_catalog(name="catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="oss://bucket/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.1.3 with Hadoop environment(Hadoop 3.2.0)** to access the fileset:

```python
import logging
from gravitino import NameIdentifier, GravitinoClient, Catalog, Fileset, GravitinoAdminClient
from pyspark.sql import SparkSession
import os

gravitino_url = "http://localhost:8090"
metalake_name = "test"

catalog_name = "your_oss_catalog"
schema_name = "your_oss_schema"
fileset_name = "your_oss_fileset"

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-aliyun-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,/path/to/aliyun-sdk-oss-2.8.3.jar,/path/to/hadoop-aliyun-3.2.0.jar,/path/to/jdom-1.1.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
.appName("oss_fielset_test")
.config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
.config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
.config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
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

- [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) is the Gravitino Aliyun jar with Hadoop environment and `hadoop-oss` jar.
- [`gravitino-aliyun-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun) is the Gravitino OSS jar without Hadoop environment and `hadoop-oss` jar.

Please choose the correct jar according to your environment.

:::note
In some Spark version, Hadoop environment is needed by the driver, adding the bundle jars with '--jars' may not work, in this case, you should add the jars to the spark classpath directly.
:::

## Using Gravitino virual file system Java client to access the fileset

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
conf.set("oss-endpoint", "http://localhost:9000");
conf.set("oss-access-key-id", "minio");
conf.set("oss-secret-access-key", "minio123"); 
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```


## Using fileset with hadoop fs command

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
    <value>http://192.168.50.188:8090</value>
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

2. Copy the necessary jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

Copy the corresponding jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory. For OSS, you need to copy `gravitino-aliyun-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.
then copy hadoop-aliyun-{version}.jar and related dependencies to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. Those jars can be found in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory, for simple you can add all the jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.


3. Run the following command to access the fileset:

```shell
hadoop dfs -ls gvfs://fileset/oss_catalog/schema/example
hadoop dfs -put /path/to/local/file gvfs://fileset/oss_catalog/schema/example
```


## Using Gravitino virtual file system Python client

```python
from gravitino import gvfs
options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "oss_endpoint": "http://localhost:9000",
    "oss_access_key_id": "minio",
    "oss_secret_access_key": "minio123"
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)

fs.ls("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/")
```


## Using fileset with pandas

The following are examples of how to use the pandas library to access the OSS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
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

## Fileset with credential

Since 0.8.0-incubating, Gravitino supports credential vending for OSS fileset. If the catalog has been configured with credential, you can access OSS fileset without providing authentication information like `oss-access-key-id` and `oss-secret-access-key` in the properties.

### How to create a OSS Hadoop catalog with credential enabled

Apart from configuration method in [create-oss-hadoop-catalog](#catalog-a-catalog), properties needed by [oss-credential](./security/credential-vending.md#oss-credentials) should also be set to enable credential vending for OSS fileset.

### How to access OSS fileset with credential

If the catalog has been configured with credential, you can access OSS fileset without providing authentication information via GVFS. Let's see how to access OSS fileset with credential:

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// No need to set oss-access-key-id and oss-secret-access-key
Path filesetPath = new Path("gvfs://fileset/oss_test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
  .appName("oss_fielset_test")
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


