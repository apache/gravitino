---
title: "Fileset Catalog with COS"
slug: "/fileset-catalog-with-cos"
date: 2026-6-17
keyword: "Fileset catalog COS Tencent"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This document explains how to configure a Fileset catalog with Tencent Cloud COS (Cloud Object Storage) in Gravitino.

## Prerequisites

To set up a Fileset catalog with COS, follow these steps:

1. Download the [`gravitino-tencent-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle) file.
2. Place the downloaded file into the Gravitino Fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server by running the following command:

```bash
$ ${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

Once the server is up and running, you can proceed to configure the Fileset catalog with COS. In the rest of this document we will use `http://localhost:8090` as the Gravitino server URL, replace with your actual server URL.

## COS Catalog Configuration

### COS Fileset Catalog Configuration

In addition to the basic configurations mentioned in [Fileset catalog properties](./fileset-catalog.md#catalog-properties), the following properties are required to configure a Fileset catalog with COS:

| Configuration item            | Description                                                                                                                                                                                                                                                                                                                                                                                                                    | Default value | Required | Since version |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `cos-region`                  | The region of the Tencent Cloud COS bucket, e.g. `ap-guangzhou`, `ap-shanghai`.                                                                                                                                                                                                                                                                                                                                                | (none)        | Yes      | 2.0.0         |
| `cos-endpoint`                | The endpoint *suffix* of the Tencent Cloud COS service (mapped to `fs.cosn.bucket.endpoint_suffix`). It is a host suffix, not a full URL — e.g. `cos.ap-guangzhou.myqcloud.com`, **not** `https://cos.ap-guangzhou.myqcloud.com`. Optional; when not set, hadoop-cos derives it from `cos-region` (`cos.${region}.myqcloud.com`). Set this only if you need to point to a non-public endpoint (e.g. an internal/VPC endpoint). | (none)        | No       | 2.0.0         |
| `cos-access-key-id`           | The static access key ID (Tencent Cloud `SecretId`) used to access COS data.                                                                                                                                                                                                                                                                                                                                                   | (none)        | Yes      | 2.0.0         |
| `cos-secret-access-key`       | The static secret access key (Tencent Cloud `SecretKey`) used to access COS data.                                                                                                                                                                                                                                                                                                                                              | (none)        | Yes      | 2.0.0         |
| `credential-providers`        | The credential provider types, separated by comma. The currently supported value is `cos-secret-key`. Setting this enables credential vending provided by the Gravitino server, so the GVFS client no longer needs `cos-access-key-id` / `cos-secret-access-key` locally. See [cos-credential-vending](#fileset-with-credential-vending) below for details.                                                                    | (none)        | No       | 2.0.0         |

:::note
The fileset catalog automatically loads filesystem providers on the classpath. The COS provider
is registered when the `gravitino-tencent-bundle` jar is present, so `default-filesystem-provider`
and `filesystem-providers` do not need to be set.

`cos-region` is mandatory for hadoop-cos: signing requests, building the default endpoint and
selecting the right CAM scope all require the region. Even if you also set `cos-endpoint`, please
keep `cos-region` set.
:::

### Schema Configuration

To create a schema, refer to [Schema configurations](./fileset-catalog.md#schema-properties).

### Fileset Configuration

For instructions on how to create a fileset, refer to [Fileset configurations](./fileset-catalog.md#fileset-properties) for more details.

## Create the Catalog, Schema, and Fileset

This section will show you how to use the Fileset catalog with COS in Gravitino, including detailed examples.

### Step 1: Create a Fileset Catalog with COS

First, you need to create a Fileset catalog for COS. The following examples demonstrate how to create a Fileset catalog with COS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_catalog",
  "type": "FILESET",
  "comment": "This is a COS fileset catalog",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-access-key-id": "access_key",
    "cos-secret-access-key": "secret_key"
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

Map<String, String> cosProperties = ImmutableMap.<String, String>builder()
    .put("location", "cosn://my-bucket-1250000000/root")
    .put("cos-region", "ap-guangzhou")
    .put("cos-access-key-id", "access_key")
    .put("cos-secret-access-key", "secret_key")
    .build();

Catalog cosCatalog = gravitinoClient.createCatalog("test_catalog",
    Type.FILESET,
    "This is a COS fileset catalog",
    cosProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
cos_properties = {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-access-key-id": "access_key",
    "cos-secret-access-key": "secret_key"
}

cos_catalog = gravitino_client.create_catalog(name="test_catalog",
                                              catalog_type=Catalog.Type.FILESET,
                                              provider=None,
                                              comment="This is a COS fileset catalog",
                                              properties=cos_properties)
```

</TabItem>
</Tabs>

:::note
Tencent Cloud COS bucket names always end with the appid suffix, for example `my-bucket-1250000000`. Use that full bucket name in `location` and in any `cosn://` URI.
:::

### Step 2: Create a Schema

Once the Fileset catalog with COS is created, you can create a schema inside that catalog. Below are examples of how to do this:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "This is a COS schema",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("test_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "cosn://my-bucket-1250000000/root/schema")
    .build();
Schema schema = supportsSchemas.createSchema("test_schema",
    "This is a COS schema",
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
                                   comment="This is a COS schema",
                                   properties={"location": "cosn://my-bucket-1250000000/root/schema"})
```

</TabItem>
</Tabs>

### Step 3: Create a Fileset

Now that the schema is created, you can create a fileset inside it. Here's how:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "cosn://my-bucket-1250000000/root/schema/example_fileset",
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
    "cosn://my-bucket-1250000000/root/schema/example_fileset",
    propertiesMap);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("test_schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="cosn://my-bucket-1250000000/root/schema/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Access a Fileset with COS

### Access the Fileset with the GVFS Java Client

To access fileset with COS using the GVFS Java client, based on the [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item      | Description                                                                                                              | Default value | Required | Since version |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `cos-region`            | The region of the Tencent Cloud COS bucket.                                                                              | (none)        | Yes      | 2.0.0         |
| `cos-endpoint`          | The endpoint *suffix* of the Tencent Cloud COS service (e.g. `cos.ap-guangzhou.myqcloud.com`, not a full URL). Optional. | (none)        | No       | 2.0.0         |
| `cos-access-key-id`     | The access key ID (Tencent Cloud SecretId) for COS data.                                                                 | (none)        | Yes      | 2.0.0         |
| `cos-secret-access-key` | The secret access key (Tencent Cloud SecretKey) for COS data.                                                            | (none)        | Yes      | 2.0.0         |

:::note
If the catalog has enabled [credential vending](security/credential-vending.md), the AK/SK properties above can be omitted. More details can be found in [Fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
conf.set("cos-region", "ap-guangzhou");
conf.set("cos-access-key-id", "access_key");
conf.set("cos-secret-access-key", "secret_key");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add COS (bundle) jars to the classpath according to your environment.
If you want to customise your hadoop version or there is already a hadoop version in your project, you can add the following dependencies to your `pom.xml`:

```xml
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>

  <!-- hadoop-cos is published by Tencent Cloud, not by Apache Hadoop. -->
  <dependency>
    <groupId>com.qcloud.cos</groupId>
    <artifactId>hadoop-cos</artifactId>
    <version>3.3.0-8.3.23</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

:::note
Unlike the S3, OSS, GCS and Azure connectors, COS does **not** ship with Apache Hadoop. The HCFS adapter for COS is published by Tencent Cloud as `com.qcloud.cos:hadoop-cos`. Make sure the version you pick is compatible with your Hadoop version (the `<hadoop>-<sdk>` form encodes both, e.g. `3.3.0-8.3.23` targets Hadoop 3.3.0).
:::

Or use the bundle jar with Hadoop environment if there is no Hadoop environment:

```xml
  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-tencent-bundle</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

### Access the Fileset with Spark

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

catalog_name = "your_cos_catalog"
schema_name = "your_cos_schema"
fileset_name = "your_cos_fileset"

# JDK8 as follows. JDK17 will be slightly different, you need to add
# '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
#  --conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"'
# to the submit args.
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars "
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,"
    "/path/to/hadoop-cos-3.3.0-8.3.23.jar,"
    "/path/to/cos_api-bundle-5.6.227.jar "
    "--master local[1] pyspark-shell"
)
spark = SparkSession.builder \
    .appName("cos_fileset_test") \
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs") \
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem") \
    .config("spark.hadoop.fs.gravitino.server.uri", gravitino_url) \
    .config("spark.hadoop.fs.gravitino.client.metalake", "test") \
    .config("spark.hadoop.cos-region", "ap-guangzhou") \
    .config("spark.hadoop.cos-access-key-id", os.environ["COS_ACCESS_KEY_ID"]) \
    .config("spark.hadoop.cos-secret-access-key", os.environ["COS_SECRET_ACCESS_KEY"]) \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.port", "2048") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
columns = ["Name", "Age"]
spark_df = spark.createDataFrame(data, schema=columns)
gvfs_path = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people"

spark_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(gvfs_path)
```

If your Spark is **without Hadoop environment**, you can use the following code snippet to access the fileset:

```python
## Only the PYSPARK_SUBMIT_ARGS line below changes; keep all the SparkSession.builder.config(...)
## calls (including spark.hadoop.cos-region / cos-access-key-id / cos-secret-access-key) above as-is.

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-tencent-bundle-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar --master local[1] pyspark-shell"
```

- [`gravitino-tencent-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle): A "fat" JAR that includes `gravitino-tencent` functionality and all necessary dependencies like `hadoop-cos` and the Tencent Cloud COS Java SDK. Use this if your Spark environment doesn't have a pre-existing Hadoop setup.
- [`gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-filesystem-hadoop3-runtime): A "fat" JAR that bundles Gravitino's virtual filesystem client and includes the functionality of `gravitino-tencent`. It is required for accessing Gravitino filesets.

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver, adding the bundle jars with `--jars` may not work. If this is the case, you should add the jars to the spark CLASSPATH directly.
:::

### Access a Fileset Using the Hadoop Fs Command

The following are examples of how to use the `hadoop fs` command to access the fileset in Hadoop 3.1.3:

1. Add the following contents to the `${HADOOP_HOME}/etc/hadoop/core-site.xml` file:

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
    <name>cos-region</name>
    <value>ap-guangzhou</value>
  </property>

  <property>
    <name>cos-access-key-id</name>
    <value>access-key</value>
  </property>

  <property>
    <name>cos-secret-access-key</name>
    <value>secret-key</value>
  </property>
```

2. Add the necessary jars to the Hadoop classpath.

For COS, you need to add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, `hadoop-cos-3.3.0-8.3.23.jar` and `cos_api-bundle-5.6.227.jar` to the Hadoop classpath. Unlike `hadoop-aws` or `hadoop-aliyun`, these jars are *not* part of the Apache Hadoop distribution; download them from Maven Central and place them under `${HADOOP_HOME}/share/hadoop/tools/lib/`.

3. Run the following command to access the fileset:

```shell
./${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/cos_catalog/cos_schema/cos_fileset
./${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/cos_catalog/cos_schema/cos_fileset
```

### Access the Fileset with the GVFS Python Client / Pandas

:::note
The GVFS **Python** client does not yet ship a COS storage handler, so reading
and writing COS-backed filesets via the Python `gvfs.GravitinoVirtualFileSystem`
or pandas `read_csv("gvfs://...")` is **not supported in this release**. Only
the GVFS **Java** client (and Spark / `hadoop fs` on top of it) can do fileset
I/O against COS today.

The Python `GravitinoClient` itself is not affected: you can still create,
inspect, update and delete COS catalogs, schemas and filesets through the
metadata API exactly as shown in the [Step 1 example above](#step-1-create-a-fileset-catalog-with-cos) and the following Step 2 / Step 3 sections.
What is missing is purely the *data plane* (`gvfs.ls`, `gvfs.open`,
pandas `read_csv("gvfs://...")`, etc.) for COS, which will be added in a
follow-up release together with a `COSStorageHandler` and the corresponding
fsspec adapter.
:::

For other use cases of the Java GVFS client, refer to the [Gravitino Virtual File System](./how-to-use-gvfs.md) document.

## Fileset with Credential Vending

Since 2.0.0, Gravitino supports credential vending for the COS fileset. If the catalog has been [configured with credential](./security/credential-vending.md), you can access COS fileset without providing authentication information like `cos-access-key-id` and `cos-secret-access-key` in the GVFS client configuration.

The currently supported credential providers are listed below:

| Credential provider | Description                                                                                                                                                                | Vended credential type |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
| `cos-secret-key`    | The Gravitino server hands out the static `cos-access-key-id` / `cos-secret-access-key` configured on the catalog. Useful for centralising credentials on the server side. | Static AK/SK           |

:::note
STS-token-based credential vending (`cos-token`) for COS, including role-arn / assume-role configuration, will be added in a follow-up release. Until then, only the static `cos-secret-key` provider is available.
:::

### Create a COS Fileset Catalog with Credential Vending

In addition to the configuration described in [COS fileset catalog configuration](#cos-fileset-catalog-configuration), you only need to set `credential-providers` to `cos-secret-key` to enable credential vending:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "cos-catalog-with-credential-vending",
  "type": "FILESET",
  "comment": "This is a COS fileset catalog with credential vending",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-access-key-id": "access_key",
    "cos-secret-access-key": "secret_key",
    "credential-providers": "cos-secret-key"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

### Access a COS Fileset with Credential Vending

When the catalog is configured with credentials and client-side credential vending is enabled,
you can access COS filesets directly using the GVFS Java client or Spark without providing AK/SK in the client.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
// No need to set cos-access-key-id and cos-secret-access-key
Path filesetPath = new Path("gvfs://fileset/cos_test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder \
    .appName("cos_fileset_test") \
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true") \
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs") \
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem") \
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090") \
    .config("spark.hadoop.fs.gravitino.client.metalake", "test") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.port", "2048") \
    .getOrCreate()
# Note: no need to set spark.hadoop.cos-access-key-id / cos-secret-access-key here —
# credential vending will fetch them from the Gravitino server.
```

The Hadoop `fs` command is similar to the above examples. The GVFS **Python** client cannot access COS-backed filesets yet — see the note in [Access the Fileset with the GVFS Python Client / Pandas](#access-the-fileset-with-the-gvfs-python-client--pandas).
