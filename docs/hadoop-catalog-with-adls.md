---
title: "Hadoop catalog with ADLS"
slug: /hadoop-catalog-with-adls
date: 2025-01-03
keyword: Hadoop catalog ADLS
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Hadoop catalog with ADLS (Azure Blob Storage).

## Prerequisites

To set up a Hadoop catalog with ADLS, follow these steps:

1. Download the [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle) file.
2. Place the downloaded file into the Gravitino Hadoop catalog classpath at `${GRAVITINO_HOME}/catalogs/hadoop/libs/`.
3. Start the Gravitino server by running the following command:

```bash
$ bin/gravitino-server.sh start
```
Once the server is up and running, you can proceed to configure the Hadoop catalog with ADLS.


```bash
$ bin/gravitino-server.sh start
```

## Create a Hadoop Catalog with ADLS

The rest of this document shows how to use the Hadoop catalog with ADLS in Gravitino with a full example.

###  Configuration for a ADLS Hadoop catalog

Apart from configurations mentioned in [Hadoop-catalog-catalog-configuration](./hadoop-catalog.md#catalog-properties), the following properties are required to configure a Hadoop catalog with ADLS:

| Configuration item                | Description                                                                                                                                                                                                                                    | Default value   | Required                                  | Since version    |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|-------------------------------------------|------------------|
| `filesystem-providers`            | The file system providers to add. Set it to `abs` if it's a Azure Blob Storage fileset, or a comma separated string that contains `abs` like `oss,abs,s3` to support multiple kinds of fileset including `abs`.                                | (none)          | Yes                                       | 0.8.0-incubating |
| `default-filesystem-provider`     | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for Azure Blob Storage, if we set this value, we can omit the prefix 'abfss://' in the location. | `builtin-local` | No                                        | 0.8.0-incubating |
| `azure-storage-account-name `     | The account name of Azure Blob Storage.                                                                                                                                                                                                        | (none)          | Yes if it's a Azure Blob Storage fileset. | 0.8.0-incubating |
| `azure-storage-account-key`       | The account key of Azure Blob Storage.                                                                                                                                                                                                         | (none)          | Yes if it's a Azure Blob Storage fileset. | 0.8.0-incubating |

### Configuration for a schema

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

### Configuration for a fileset

Refer to [Fileset operation](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.


## Using Hadoop catalog with ADLS

This section demonstrates how to use the Hadoop catalog with ADLS in Gravitino, with a complete example.

### Step1: Create a Hadoop catalog with ADLS

First, you need to create a Hadoop catalog with ADLS. The following example shows how to create a Hadoop catalog with ADLS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_catalog",
  "type": "FILESET",
  "comment": "comment",
  "provider": "hadoop",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/path",
    "azure-storage-account-name": "The account name of the Azure Blob Storage",
    "azure-storage-account-key": "The account key of the Azure Blob Storage",
    "filesystem-providers": "abs"
  }
}' ${GRAVITINO_SERVER_IP:PORT}/api/metalakes/metalake/catalogs
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("${GRAVITINO_SERVER_IP:PORT}")
    .withMetalake("metalake")
    .build();

adlsProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container@account-name.dfs.core.windows.net/path")
    .put("azure-storage-account-name", "azure storage account name")
    .put("azure-storage-account-key", "azure storage account key")
    .put("filesystem-providers", "abs")
    .build();

Catalog adlsCatalog = gravitinoClient.createCatalog("example_catalog",
    Type.FILESET,
    "hadoop", // provider, Gravitino only supports "hadoop" for now.
    "This is a ADLS fileset catalog",
    adlsProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="${GRAVITINO_SERVER_IP:PORT}", metalake_name="metalake")
adls_properties = {
    "location": "abfss://container@account-name.dfs.core.windows.net/path",
    "azure_storage_account_name": "azure storage account name",
    "azure_storage_account_key": "azure storage account key"
}

adls_properties = gravitino_client.create_catalog(name="example_catalog",
                                             type=Catalog.Type.FILESET,
                                             provider="hadoop",
                                             comment="This is a ADLS fileset catalog",
                                             properties=adls_properties)

```

</TabItem>
</Tabs>

### Step2: Create a schema

Once the catalog is created, you can create a schema. The following example shows how to create a schema:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "test_schema",
  "comment": "comment",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/path"
  }
}' ${GRAVITINO_SERVER_IP:PORT}/api/metalakes/metalake/catalogs/test_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("test_catalog");

SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container@account-name.dfs.core.windows.net/path")
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
                                   properties={"location": "abfss://container@account-name.dfs.core.windows.net/path"})
```

</TabItem>
</Tabs>

### Step3: Create a fileset

After creating the schema, you can create a fileset. The following example shows how to create a fileset:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "abfss://container@account-name.dfs.core.windows.net/path/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' ${GRAVITINO_SERVER_IP:PORT}/api/metalakes/metalake/catalogs/test_catalog/schemas/test_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
GravitinoClient gravitinoClient = GravitinoClient
    .builder("${GRAVITINO_SERVER_IP:PORT}")
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
  "abfss://container@account-name.dfs.core.windows.net/path/example_fileset",
  propertiesMap,
);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="${GRAVITINO_SERVER_IP:PORT}", metalake_name="metalake")

catalog: Catalog = gravitino_client.load_catalog(name="test_catalog")
catalog.as_fileset_catalog().create_fileset(ident=NameIdentifier.of("test_schema", "example_fileset"),
                                            type=Fileset.Type.MANAGED,
                                            comment="This is an example fileset",
                                            storage_location="abfss://container@account-name.dfs.core.windows.net/path/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Accessing a fileset with ADLS

### Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.1.3 with Hadoop environment(Hadoop 3.2.0)** to access the fileset:

```python
import logging
from gravitino import NameIdentifier, GravitinoClient, Catalog, Fileset, GravitinoAdminClient
from pyspark.sql import SparkSession
import os

gravitino_url = "${GRAVITINO_SERVER_IP:PORT}"
metalake_name = "test"

catalog_name = "your_adls_catalog"
schema_name = "your_adls_schema"
fileset_name = "your_adls_fileset"

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-azure-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,/path/to/hadoop-azure-3.2.0.jar,/path/to/azure-storage-7.0.0.jar,/path/to/wildfly-openssl-1.0.4.Final.jar --master local[1] pyspark-shell"
spark = SparkSession.builder
.appName("adls_fileset_test")
.config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
.config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
.config("spark.hadoop.fs.gravitino.server.uri", "${GRAVITINO_SERVER_URL}")
.config("spark.hadoop.fs.gravitino.client.metalake", "test")
.config("spark.hadoop.azure-storage-account-name", "azure_account_name")
.config("spark.hadoop.azure-storage-account-key", "azure_account_name")
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

If your Spark **without Hadoop environment**, you can use the following code snippet to access the fileset:

```python
## Replace the following code snippet with the above code snippet with the same environment variables

os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-azure-bundle-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar --master local[1] pyspark-shell"
```

- [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle) is the Gravitino ADLS jar with Hadoop environment and `hadoop-azure` jar.
- [`gravitino-azure-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure) is a condensed version of the Gravitino ADLS bundle jar without Hadoop environment and `hadoop-azure` jar.
- `hadoop-azure-3.2.0.jar` and `azure-storage-7.0.0.jar` can be found in the Hadoop distribution in the `${HADOOP_HOME}/share/hadoop/tools/lib` directory.


Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is needed by the driver, adding the bundle jars with '--jars' may not work. If this is the case, you should add the jars to the spark CLASSPATH directly.
:::

### Using Gravitino virtual file system Java client to access the fileset

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","${GRAVITINO_SERVER_URL}");
conf.set("fs.gravitino.client.metalake","test_metalake");
conf.set("azure-storage-account-name", "account_name_of_adls");
conf.set("azure-storage-account-key", "account_key_of_adls");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add ADLS bundle jars to the classpath according to your environment.

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
    <name>azure-storage-account-name</name>
    <value>account_name</value>
  </property>
  <property>
    <name>azure-storage-account-key</name>
    <value>account_key</value>
  </property>
```

2. Copy the necessary jars to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.

For ADLS, you need to copy `gravitino-azure-{version}.jar` to the `${HADOOP_HOME}/share/hadoop/common/lib` directory,
then copy `hadoop-azure-${version}.jar` and related dependencies to the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory. Those jars can be found in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory, you can add all the jars in the `${HADOOP_HOME}/share/hadoop/tools/lib/` directory to the `${HADOOP_HOME}/share/hadoop/common/lib` directory.


3. Run the following command to access the fileset:

```shell
hadoop dfs -ls gvfs://fileset/adls_catalog/adls_schema/adls_fileset
hadoop dfs -put /path/to/local/file gvfs://fileset/adls_catalog/adls_schema/adls_fileset
```

### Using the Gravitino virtual file system Python client to access a fileset

```python
from gravitino import gvfs
options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "azure_storage_account_name": "azure_account_name",
    "azure_storage_account_key": "azure_account_key"
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="${GRAVITINO_SERVER_IP:PORT}", metalake_name="test_metalake", options=options)
fs.ls("gvfs://fileset/{adls_catalog}/{adls_schema}/{adls_fileset}/")
```


### Using fileset with pandas

The following are examples of how to use the pandas library to access the ADLS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "${GRAVITINO_SERVER_IP:PORT}", 
    "metalake_name": "test",
    "options": {
        "azure_storage_account_name": "azure_account_name",
        "azure_storage_account_key": "azure_account_key"
    }
}
ds = pd.read_csv(f"gvfs://fileset/${catalog_name}/${schema_name}/${fileset_name}/people/part-00000-51d366e2-d5eb-448d-9109-32a96c8a14dc-c000.csv",
                 storage_options=storage_options)
ds.head()
```

For other use cases, please refer to the [Gravitino Virtual File System](./how-to-use-gvfs.md) document.

## Fileset with credential

Since 0.8.0-incubating, Gravitino supports credential vending for ADLS fileset. If the catalog has been configured with credential, you can access ADLS fileset without providing authentication information like `azure-storage-account-name` and `azure-storage-account-key` in the properties.

### How to create an ADLS Hadoop catalog with credential enabled

Apart from configuration method in [create-adls-hadoop-catalog](#catalog-a-catalog), properties needed by [adls-credential](./security/credential-vending.md#adls-credentials) should also be set to enable credential vending for ADLSfileset.

### How to access ADLS fileset with credential

If the catalog has been configured with credential, you can access ADLS fileset without providing authentication information via GVFS. Let's see how to access ADLS fileset with credential:

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl","org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl","org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri","${GRAVITINO_SERVER_IP:PORT}");
conf.set("fs.gravitino.client.metalake","test_metalake");
// No need to set azure-storage-account-name and azure-storage-account-name
Path filesetPath = new Path("gvfs://fileset/adls_test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Spark:

```python
spark = SparkSession.builder
  .appName("adls_fielset_test")
  .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
  .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
  .config("spark.hadoop.fs.gravitino.server.uri", "${GRAVITINO_SERVER_IP:PORT}")
  .config("spark.hadoop.fs.gravitino.client.metalake", "test")
  # No need to set azure-storage-account-name and azure-storage-account-name
  .config("spark.driver.memory", "2g")
  .config("spark.driver.port", "2048")
  .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.

