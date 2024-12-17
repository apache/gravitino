---
title: "How to use cloud storage fileset"
slug: /how-to-use-cloud-storage-fileset
keyword: fileset S3 GCS ADLS OSS
license: "This software is licensed under the Apache License version 2."
---

This document aims to provide a comprehensive guide on how to use cloud storage fileset created by Gravitino, it usually contains the following sections:


## Start up Gravitino server

### Start up Gravitino server

Before running the Gravitino server, you need to put the following jars into the fileset class path located in `${GRAVITINO_HOME}/catalogs/hadoop/libs`. For example, if you are using S3, you need to put gravitino-aws-bundles-{version}.jar into the fileset class path.


| Storage type | Description                                                   | Jar file                                                                                                   | Since Version    |
|--------------|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|------------------|
| Local file   | The local file system.                                        | (none)                                                                                                     | 0.5.0            |
| HDFS         | HDFS file system.                                             | (none)                                                                                                     | 0.5.0            |
| S3           | AWS S3 storage.                                               | [gravitino-aws-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle)       | 0.7.0-incubating |
| GCS          | Google Cloud Storage.                                         | [gravitino-gcp-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle)       | 0.7.0-incubating |
| OSS          | Aliyun OSS storage.                                           | [gravitino-aliyun-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) | 0.7.0-incubating |
| ABS          | Azure Blob Storage (aka. ABS, or Azure Data Lake Storage (v2) | [gravitino-azure-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle)   | 0.8.0-incubating |

After putting the jars into the fileset class path, you can start up the Gravitino server by running the following command:

```shell
cd ${GRAVITINO_HOME}
bin/gravitino.sh start
```

### Bundle jars

`gravitino-{aws,gcp,aliyun,azure}-bundle` are the jars that contain all the necessary classes to access the corresponding cloud storages, for instance, gravitino-aws-bundle contains the all necessary classes like `hadoop-common`(hadoop-3.3.1) and `hadoop-aws` to access the S3 storage.
**They are used in the scenario where there is no hadoop environment in the runtime.**

**If there is already hadoop environment in the runtime, you can use the `gravitino-{aws,gcp,aliyun,azure}-core.jar` that does not contain the cloud storage classes (like hadoop-aws) and hadoop environment, you can manually add the necessary jars to the classpath.**

The following picture demonstrates what jars are necessary for different cloud storage filesets:

| Hadoop runtime version | S3                                                                                                                                   | GCS                                                                                                          | OSS                                                                                                                                           | ABS                                                                                                               |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| No Hadoop environment  | gravitino-aws-bundle-{gravitino-version}.jar                                                                                         | gravitino-gcp-bundle-{gravitino-version}.jar                                                                 | gravitino-aliyun-bundle-{gravitino-version}.jar                                                                                               | gravitino-azure-bundle-{gravitino-version}.jar                                                                    |
| 2.x, 3.x               | gravitino-aws-core-{gravitino-version}.jar, hadoop-aws-{hadoop-version}.jar, aws-sdk-java-{version} and other necessary dependencies | gravitino-gcp-core-{gravitino-version}.jar, gcs-connector-{hadoop-version}.jar, other necessary dependencies | gravitino-aliyun-core-{gravitino-version}.jar, hadoop-aliyun-{hadoop-version}.jar, aliyun-sdk-java-{version} and other necessary dependencies | gravitino-azure-core-{gravitino-version}.jar, hadoop-azure-{hadoop-version}.jar, and other necessary dependencies |

For `hadoop-aws-{version}.jar`, `hadoop-azure-{version}.jar` and `hadoop-aliyun-{version}.jar` and related dependencies, you can get it from ${HADOOP_HOME}/share/hadoop/tools/lib/ directory.
For `gcs-connector`, you can download it from the [GCS connector](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22-shaded.jar) for hadoop2 or hadoop3. 

If there still have some issues, please report it to the Gravitino community and create an issue. 

:::note
Gravitino server uses Hadoop 3.3.1, and you only need to put the corresponding bundle jars into the fileset class path. 
:::


## Create fileset catalogs

Once the Gravitino server is started, you can create the corresponding fileset by the following sentence:


### Create a S3 fileset catalog

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

Catalog s3Catalog = gravitinoClient.createCatalog("catalog",
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

s3_catalog = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a S3 fileset catalog",
                                             properties=s3_properties)

```

</TabItem>
</Tabs>

:::note
The value of location should always start with `s3a` NOT `s3`, for instance, `s3a://bucket/root`. Value like `s3://bucket/root` is not supported.
:::

### Create a GCS fileset catalog

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
    "location": "gs://bucket/root",
    "gcs-service-account-file": "path_of_gcs_service_account_file",
    "filesystem-providers": "gcs"
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

gcsProperties = ImmutableMap.<String, String>builder()
    .put("location", "gs://bucket/root")
    .put("gcs-service-account-file", "path_of_gcs_service_account_file")
    .put("filesystem-providers", "gcs")
    .build();

Catalog gcsCatalog = gravitinoClient.createCatalog("catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a GCS fileset catalog",
    gcsProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

gcs_properties = {
    "location": "gcs://bucket/root",
    "gcs_service_account_file": "path_of_gcs_service_account_file"
}

s3_catalog = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a GCS fileset catalog",
                                             properties=gcs_properties)

```

</TabItem>
</Tabs>

### Create an OSS fileset catalog

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

Catalog ossProperties = gravitinoClient.createCatalog("catalog",
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
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com"
}

oss_catalog = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a OSS fileset catalog",
                                             properties=oss_properties)

```

### Create an ABS (Azure Blob Storage or ADLS) fileset catalog

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
    "location": "abfss://container/root",
    "abs-account-name": "The account name of the Azure Blob Storage",
    "abs-account-key": "The account key of the Azure Blob Storage",
    "filesystem-providers": "abs"
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

absProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container/root")
    .put("abs-account-name", "The account name of the Azure Blob Storage")
    .put("abs-account-key", "The account key of the Azure Blob Storage")
    .put("filesystem-providers", "abs")
    .build();

Catalog gcsCatalog = gravitinoClient.createCatalog("catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a Azure Blob storage fileset catalog",
    absProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

abs_properties = {
    "location": "gcs://bucket/root",
    "abs_account_name": "The account name of the Azure Blob Storage",
    "abs_account_key": "The account key of the Azure Blob Storage"  
}

abs_catalog = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a Azure Blob Storage fileset catalog",
                                             properties=abs_properties)

```

</TabItem>
</Tabs>

note:::
- The prefix of an ABS (Azure Blob Storage or ADLS (v2)) location should always start with `abfss` NOT `abfs`, for instance, `abfss://container/root`. Value like `abfs://container/root` is not supported.
- The prefix of an AWS S3 location should always start with `s3a` NOT `s3`, for instance, `s3a://bucket/root`. Value like `s3://bucket/root` is not supported.
- The prefix of an Aliyun OSS location should always start with `oss` for instance, `oss://bucket/root`.
- The prefix of a GCS location should always start with `gs` for instance, `gs://bucket/root`.
:::


## Create fileset schema

This part is the same for all cloud storage filesets, you can create the schema by the following sentence:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "schema",
  "comment": "comment",
  "properties": {
    "location": "file:///tmp/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("http://localhost:8090")
    .withMetalake("metalake")
    .build();

// Assuming you have just created a Hadoop catalog named `catalog`
Catalog catalog = gravitinoClient.loadCatalog("catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    // Property "location" is optional, if specified all the managed fileset without
    // specifying storage location will be stored under this location.
    .put("location", "file:///tmp/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("schema",
    "This is a schema",
    schemaProperties
);
// ...
```

</TabItem>
<TabItem value="python" label="Python">

You can change the value of property `location` according to which catalog you are using, moreover, if we have set the `location` property in the catalog, we can omit the `location` property in the schema.

## Create filesets

The following sentences can be used to create a fileset in the schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "file:///tmp/root/schema/example_fileset",
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
  "file:///tmp/root/schema/example_fileset",
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
                                            storage_location="/tmp/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

Similar to schema, the `storageLocation` is optional if you have set the `location` property in the schema or catalog. Please change the value of 
`location` as the actual location you want to store the fileset. 


## Using Spark to access the fileset

The following code snippet shows how to use **Spark 3.1.3 with hadoop environment** to access the fileset

```python
import logging
from gravitino import NameIdentifier, GravitinoClient, Catalog, Fileset, GravitinoAdminClient
from pyspark.sql import SparkSession
import os

gravitino_url = "http://localhost:8090"
metalake_name = "test"

catalog_name = "s3_catalog"
schema_name = "schema"
fileset_name = "example"

## this is for S3
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aws-core/build/libs/gravitino-aws-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-aws-3.2.0.jar,/Users/yuqi/Downloads/aws-java-sdk-bundle-1.11.375.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
  .appName("s3_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  .config("spark.hadoop.s3-access-key-id", os.environ["S3_ACCESS_KEY_ID"])
  .config("spark.hadoop.s3-secret-access-key", os.environ["S3_SECRET_ACCESS_KEY"])
  .config("spark.hadoop.s3-endpoint", "http://s3.ap-northeast-1.amazonaws.com")
  .config("spark.hadoop.fs.gvfs.filesystem.providers", "s3")
  .config("spark.driver.memory", "2g")
  .config("spark.driver.port", "2048")
  .getOrCreate()

### this is for GCS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/gcp-core/build/libs/gravitino-gcp-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/gcs-connector-hadoop3-2.2.22-shaded.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
  .appName("s3_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  .config("spark.hadoop.gcs-service-account-file", "/Users/yuqi/Downloads/silken-physics-431108-g3-30ab3d97bb60.json")
  .config("spark.hadoop.fs.gvfs.filesystem.providers", "gcs")
  .config("spark.driver.memory", "2g")
  .config("spark.driver.port", "2048")
  .getOrCreate()

### this is for OSS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aliyun-core/build/libs/gravitino-aliyun-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/aliyun-sdk-oss-2.8.3.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-aliyun-3.2.0.jar,/Users/yuqi/Downloads/hadoop-jars/jdom-1.1.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
  .appName("s3_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  .config("spark.hadoop.oss-access-key-id", os.environ["OSS_ACCESS_KEY_ID"])
  .config("spark.hadoop.oss-secret-access-key", os.environ["S3_SECRET_ACCESS_KEY"])
  .config("spark.hadoop.oss-endpoint", "https://oss-cn-shanghai.aliyuncs.com")
  .config("spark.hadoop.fs.gvfs.filesystem.providers", "oss")
  .config("spark.driver.memory", "2g")
  .config("spark.driver.port", "2048")
  .getOrCreate()
spark.sparkContext.setLogLevel("DEBUG")

### this is for ABS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/azure-core/build/libs/gravitino-azure-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-azure-3.2.0.jar,/Users/yuqi/Downloads/hadoop-jars/azure-storage-7.0.0.jar,/Users/yuqi/Downloads/hadoop-jars/wildfly-openssl-1.0.4.Final.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
  .appName("s3_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  .config("spark.hadoop.abs-account-name", "xiaoyu456")
  .config("spark.hadoop.abs-account-key", "account_key")
  .config("spark.hadoop.fs.gvfs.filesystem.providers", "abs")
  .config("spark.hadoop.fs.azure.skipUserGroupMetadataDuringInitialization", "true")
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

If your spark has no hadoop environment, you can use the following code snippet to access the fileset:

```python
## replace the env PYSPARK_SUBMIT_ARGS variable in the code above with the following content:
### S3
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aws-bundle/build/libs/gravitino-aws-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar --master local[1] pyspark-shell"
### GCS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/gcp-bundle/build/libs/gravitino-gcp-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar, --master local[1] pyspark-shell"
### OSS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aliyun-bundle/build/libs/gravitino-aliyun-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar, --master local[1] pyspark-shell"

#### Azure Blob Storage
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/azure-bundle/build/libs/gravitino-azure-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar --master local[1] pyspark-shell"
```

:::note
**In some spark version, hadoop environment is needed by the driver, adding the bundles jars with '--jars' may not work, in this case, you should add the jars to the spark classpath directly.**
:::

## Using fileset with hadoop fs command

The following are examples of how to use the `hadoop fs` command to access the fileset in hadoop 3.1.3.

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

  <!-- Optional. It's only for S3 catalog.-->
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
  <property>
    <name>fs.gvfs.filesystem.providers</name>
    <value>s3</value>
  </property>

  <!-- Optional. It's only for OSS catalog.-->
  <property>
    <name>oss-endpoint</name>
    <value>https://oss-cn-shanghai.aliyuncs.com</value>
  </property>
  <property>
    <name>oss-access-key-id</name>
    <value>access_key</value>
  </property>
  <property>
    <name>oss-secret-access-key</name>
    <value>secret_key</value>
  </property>
   <property>
    <name>fs.gvfs.filesystem.providers</name>
    <value>oss</value>
  </property>

  <!-- Optional. It's only for GCS -->
  <property>
    <name>gcs-service-account-file</name>
    <value>/root/silken-physics-431108-g3-30ab3d97bb60.json</value>
  </property>
  <property>
    <name>fs.gvfs.filesystem.providers</name>
  <value>gcs</value>
  
  <!-- Optional. It's only for Azure Blob Storage -->
  <property>
    <name>abs-account-name</name>
    <value>account_name</value>
  </property>
  <property>
    <name>abs-account-key</name>
    <value>account_key</value>
  </property>
  <property>
    <name>fs.gvfs.filesystem.providers</name>
    <value>abs</value>
  </property>
</property>

```

2. Copy the necessary jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

Copy the corresponding core jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory. For example, if you are using S3, you need to copy `gravitino-aws-core-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.
then copy hadoop-aws-{version}.jar and related dependencies to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. Those jars can be found in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory, for simple you can add all the jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

For GCS, please download the corresponding the `gcs-connector-hadoop{2,3}-2.2.22-shaded.jar` from the [GCS connector](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22-shaded.jar) and add it to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. 


3. Run the following command to access the fileset:

```shell
hadoop dfs -ls gvfs://fileset/s3_catalog/schema/example
hadoop dfs -put /path/to/local/file gvfs://fileset/s3_catalog/schema/example
```

### Using fileset with pandas

The following are examples of how to use the pandas library to access the fileset in hadoop 3.1.3.

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
ds = pd.read_csv(f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people/part-00000-51d366e2-d5eb-448d-9109-32a96c8a14dc-c000.csv",
                 storage_options=storage_options)
ds.head()
```


