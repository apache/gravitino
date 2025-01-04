---
title: "Hadoop catalog with GCS"
slug: /hadoop-catalog-with-gcs
date: 2024-01-03
keyword: Hadoop catalog GCS
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Hadoop catalog with GCS.

## Prerequisites

In order to create a Hadoop catalog with GCS, you need to place [`gravitino-gcp-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle) in Gravitino Hadoop classpath located
at `${HADOOP_HOME}/share/hadoop/common/lib/`. After that, start Gravitino server with the following command:

```bash
$ bin/gravitino-server.sh start
```

## Create a Hadoop Catalog with GCS in Gravitino

### Catalog a catalog

Apart from configuration method in [Hadoop-catalog-catalog-configuration](./hadoop-catalog.md#catalog-properties), the following properties are required to configure a Hadoop catalog with GCS:

| Configuration item            | Description                                                                                                                                                                                                                  | Default value   | Required                   | Since version    |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------------------------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `gs` if it's a GCS fileset, a comma separated string that contains `gs` like `gs,s3` to support multiple kinds of fileset including `gs`.                                        | (none)          | Yes                        | 0.7.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for GCS, if we set this value, we can omit the prefix 'gs://' in the location. | `builtin-local` | No                         | 0.7.0-incubating |
| `gcs-service-account-file`    | The path of GCS service account JSON file.                                                                                                                                                                                   | (none)          | Yes if it's a GCS fileset. | 0.7.0-incubating |

### Create a schema

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

### Create a fileset

Refer to [Fileset operation](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.


## Using Hadoop catalog with GCS

### Create a Hadoop catalog/schema/file set with GCS

First, you need to create a Hadoop catalog with GCS. The following example shows how to create a Hadoop catalog with GCS:

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
    "location": "gs://bucket/root",
    "gcs-service-account-file": "path_of_gcs_service_account_file"
}

gcs_properties = gravitino_client.create_catalog(name="catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a GCS fileset catalog",
                                             properties=gcs_properties)

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
    "location": "gs://bucket/root/schema"
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
    .put("location", "gs://bucket/root/schema")
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
                                   properties={"location": "gs://bucket/root/schema"})
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
  "storageLocation": "gs://bucket/root/schema/example_fileset",
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
  "gs://bucket/root/schema/example_fileset",
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
                                            storage_location="gs://bucket/root/schema/example_fileset",
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

catalog_name = "your_gcs_catalog"
schema_name = "your_gcs_schema"
fileset_name = "your_gcs_fileset"

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-gcp-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,/path/to/gcs-connector-hadoop3-2.2.22-shaded.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
.appName("gcs_fielset_test")
.config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
.config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
.config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
.config("spark.hadoop.fs.gravitino.client.metalake", "test")
.config("spark.hadoop.gcs-service-account-file", "/path/to/gcs-service-account-file.json")
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

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-gcp-bundle-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar, --master local[1] pyspark-shell"
```

- [`gravitino-gcp-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle) is the Gravitino GCS jar with Hadoop environment and `gcs-connector` jar.
- [`gravitino-gcp-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp) is the Gravitino GCS jar without Hadoop environment and `gcs-connector` jar.

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
conf.set("gcs-service-account-file", "/path/your-service-account-file.json");
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
    <name>gcs-service-account-file</name>
    <value>/path/your-service-account-file.json</value>
  </property>
```

2. Copy the necessary jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

Copy the corresponding jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory. For GCS, you need to copy `gravitino-gcp-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.
then copy `hadoop-gcp-${version}.jar` and related dependencies to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. Those jars can be found in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory, for simple you can add all the jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.


3. Run the following command to access the fileset:

```shell
hadoop dfs -ls gvfs://fileset/gcs_catalog/schema/example
hadoop dfs -put /path/to/local/file gvfs://fileset/gcs_catalog/schema/example
```


## Using Gravitino virtual file system Python client

```python
from gravitino import gvfs
options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "gcs_service_account_file": "path_of_gcs_service_account_file.json",
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)
fs.ls("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/")
```

## Using fileset with pandas

The following are examples of how to use the pandas library to access the GCS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
    "metalake_name": "test",
    "options": {
        "gcs_service_account_file": "path_of_gcs_service_account_file.json",
    }
}
ds = pd.read_csv(f"gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/people/part-00000-51d366e2-d5eb-448d-9109-32a96c8a14dc-c000.csv",
                 storage_options=storage_options)
ds.head()
```

## Fileset with credential

Since 0.8.0-incubating, Gravitino supports credential vending for GCS fileset. If the catalog has been configured with credential, you can access GCS fileset without providing authentication information like `gcs-service-account-file` in the properties.

### How to create a GCS Hadoop catalog with credential enabled

Apart from configuration method in [create-gcs-hadoop-catalog](#catalog-a-catalog), properties needed by [gcs-credential](./security/credential-vending.md#gcs-credentials) should also be set to enable credential vending for GCS fileset.

### How to access GCS fileset with credential

If the catalog has been configured with credential, you can access GCS fileset without providing authentication information via GVFS. Let's see how to access GCS fileset with credential:

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","http://localhost:8090");
conf.set("fs.gravitino.client.metalake","test_metalake");
// No need to set gcs-service-account-file
Path filesetPath = new Path("gvfs://fileset/gcs_test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
  .appName("gcs_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  # No need to set gcs-service-account-file
  .config("spark.driver.memory", "2g")
  .config("spark.driver.port", "2048")
  .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.
