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
| ABS          | Azure Blob Storage (aka. ABS, or Azure Data Lake Storage (v2) | [gravitino-azure-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle)   | 0.7.0-incubating |

After putting the jars into the fileset class path, you can start up the Gravitino server by running the following command:
```shell
cd ${GRAVITINO_HOME}
bin/gravitino.sh start
```

### Bundle jars

`gravitino-{aws,gcp,aliyun,azure}-bundle` are the jars that contain the necessary classes to access the corresponding cloud storages, and it's compiled with Hadoop 3.3.1. Due to the compatibility issue, Those jars may not work 
in the environment with Hadoop 2.x or below 3.3.1. In order to solve this issue, Gravitino also provide the `gravitino-{aws,gcp,aliyun,azure}-core` that do not contain the cloud storage classes, users can manually add the necessary jars to the classpath.

| Hadoop runtime version | S3                                                                                                                         | GCS                                                                                                | OSS                                                                                                                                 | ABS                                                                                                     |
|------------------------|----------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| 3.3.1+(including)      | gravitino-aws-bundle-{version}.jar                                                                                         | gravitino-gcp-bundle-{version}.jar                                                                 | gravitino-aliyun-bundle-{version}.jar                                                                                               | gravitino-azure-bundle-{version}.jar                                                                    |
| 2.x, 3.3.1-            | gravitino-aws-core-{version}.jar, hadoop-aws-{hadoop-version}.jar, aws-sdk-java-{version} and other necessary dependencies | gravitino-gcp-core-{version}.jar, gcs-connector-{hadoop-version}.jar, other necessary dependencies | gravitino-aliyun-core-{version}.jar, hadoop-aliyun-{hadoop-version}.jar, aliyun-sdk-java-{version} and other necessary dependencies | gravitino-azure-core-{version}.jar, hadoop-azure-{hadoop-version}.jar, and other necessary dependencies |

## Create filesets

Once the Gravitino server is started, you can create the corresponding fileset by the following sentence:


### Create a S3 fileset

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

### Create a GCS fileset

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

### Create an OSS fileset

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

### Create an ABS (Azure Blob Storage or ADLS) fileset

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


## Using Spark to access the fileset

The following code snippet shows how to use Spark to access the fileset

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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aws-bundle/build/libs/gravitino-aws-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar --master local[1] pyspark-shell"
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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/gcp-bundle/build/libs/gravitino-gcp-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar, --master local[1] pyspark-shell"
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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aliyun-bundle/build/libs/gravitino-aliyun-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar, --master local[1] pyspark-shell"
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
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/azure-bundle/build/libs/gravitino-azure-bundle-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar --master local[1] pyspark-shell"
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

The above examples are only applicable for Spark 3.2.x+, if you are using Spark 3.1.x or below, you need to make the following changes according to [bundle jars](#bundle-jars) to adapt to different hadoop runtime versions:
Take Spark 3.1.3 for example, as the hadoop runtime of spark 3.1.3 is 3.2.0, we need to do the following changes:

```python

## replace the env variable with the following content:

### S3
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aws-core/build/libs/gravitino-aws-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-aws-3.2.0.jar,/Users/yuqi/Downloads/aws-java-sdk-bundle-1.11.375.jar --master local[1] pyspark-shell"
### GCS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/gcp-core/build/libs/gravitino-gcp-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/gcs-connector-hadoop3-2.2.22-shaded.jar --master local[1] pyspark-shell"
### OSS
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/aliyun-core/build/libs/gravitino-aliyun-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/aliyun-sdk-oss-2.8.3.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-aliyun-3.2.0.jar,/Users/yuqi/Downloads/hadoop-jars/jdom-1.1.jar --master local[1] pyspark-shell"

#### Azure Blob Storage
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /Users/yuqi/project/gravitino/bundles/azure-core/build/libs/gravitino-azure-core-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/project/gravitino/clients/filesystem-hadoop3-runtime/build/libs/gravitino-filesystem-hadoop3-runtime-0.8.0-incubating-SNAPSHOT.jar,/Users/yuqi/Downloads/hadoop-jars/hadoop-azure-3.2.0.jar,/Users/yuqi/Downloads/hadoop-jars/azure-storage-7.0.0.jar,/Users/yuqi/Downloads/hadoop-jars/wildfly-openssl-1.0.4.Final.jar --master local[1] pyspark-shell"  

```

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

If the Hadoop version is 3.3.1+, please copy the corresponding bundle jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory. For example, if you are using S3, you need to copy `gravitino-aws-bundle-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.
If the version is below 3.3.1, for S3, GCS, OSS, please add all jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` to the Hadoop class path or use the `hadoop fs` command with the `--jars` option to specify the necessary jars. For GCS, please download the corresponding the `gcs-connector-hadoop{2,3}-2.2.22-shaded.jar` from the [GCS connector](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22-shaded.jar) and add it to the Hadoop class path. 

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


