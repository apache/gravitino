---
title: "Fileset catalog with ADLS"
slug: /fileset-catalog-with-adls
date: 2025-01-03
keyword: Fileset catalog ADLS
license: "This software is licensed under the Apache License version 2."
---

This document describes how to configure a Fileset catalog with ADLS (aka. Azure Blob Storage (ABS), or Azure Data Lake Storage (v2)).

## Prerequisites

To set up a Fileset catalog with ADLS, follow these steps:

1. Download the [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle) file.
2. Place the downloaded file into the Gravitino Fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server by running the following command:

```bash
$ ${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

Once the server is up and running, you can proceed to configure the Fileset catalog with ADLS. In the rest of this document we will use `http://localhost:8090` as the Gravitino server URL, please replace it with your actual server URL.

## Configurations for creating a Fileset catalog with ADLS

### Configuration for a ADLS Fileset catalog

Apart from configurations mentioned in [fileset-catalog-catalog-configuration](./fileset-catalog.md#catalog-properties), the following properties are required to configure a Fileset catalog with ADLS:

| Configuration item            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Default value   | Required | Since version    |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `abs` if it's a Azure Blob Storage fileset, or a comma separated string that contains `abs` like `oss,abs,s3` to support multiple kinds of fileset including `abs`.                                                                                                                                                                                                                                                                                                                                                        | (none)          | Yes      | 0.8.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Fileset catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for Azure Blob Storage, if we set this value, we can omit the prefix 'abfss://' in the location.                                                                                                                                                                                                                                                                                                                        | `builtin-local` | No       | 0.8.0-incubating |
| `azure-storage-account-name ` | The account name of Azure Blob Storage.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | (none)          | Yes      | 0.8.0-incubating |
| `azure-storage-account-key`   | The account key of Azure Blob Storage.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | (none)          | Yes      | 0.8.0-incubating |
| `credential-providers`        | The credential provider types, separated by comma, possible value can be `adls-token`, `azure-account-key`. As the default authentication type is using account name and account key as the above, this configuration can enable credential vending provided by Gravitino server and client will no longer need to provide authentication information like account_name/account_key to access ADLS by GVFS. Once it's set, more configuration items are needed to make it works, please see [adls-credential-vending](security/credential-vending.md#adls-credentials) | (none)          | No       | 0.8.0-incubating |


### Configurations for a schema

Refer to [Schema configurations](./fileset-catalog.md#schema-properties) for more details.

### Configurations for a fileset

Refer to [Fileset configurations](./fileset-catalog.md#fileset-properties) for more details.

## Example of creating Fileset catalog with ADLS

This section demonstrates how to create the Fileset catalog with ADLS in Gravitino, with a complete example.

### Step1: Create a Fileset catalog with ADLS

First, you need to create a Fileset catalog with ADLS. The following example shows how to create a Fileset catalog with ADLS:

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_catalog",
  "type": "FILESET",
  "comment": "This is a ADLS fileset catalog",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/path",
    "azure-storage-account-name": "The account name of the Azure Blob Storage",
    "azure-storage-account-key": "The account key of the Azure Blob Storage",
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

Map<String, String> adlsProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container@account-name.dfs.core.windows.net/path")
    .put("azure-storage-account-name", "azure storage account name")
    .put("azure-storage-account-key", "azure storage account key")
    .put("filesystem-providers", "abs")
    .build();

Catalog adlsCatalog = gravitinoClient.createCatalog("example_catalog",
    Type.FILESET,
    "This is a ADLS fileset catalog",
    adlsProperties);
// ...

```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(uri="http://localhost:8090", metalake_name="metalake")
adls_properties = {
    "location": "abfss://container@account-name.dfs.core.windows.net/path",
    "azure-storage-account-name": "azure storage account name",
    "azure-storage-account-key": "azure storage account key",
    "filesystem-providers": "abs"
}

adls_properties = gravitino_client.create_catalog(name="example_catalog",
                                                  catalog_type=Catalog.Type.FILESET,
                                                  provider=None,
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
  "comment": "This is a ADLS schema",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/path"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/test_catalog/schemas
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
    "This is a ADLS schema",
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
                                   comment="This is a ADLS schema",
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
    "abfss://container@account-name.dfs.core.windows.net/path/example_fileset",
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
                                            storage_location="abfss://container@account-name.dfs.core.windows.net/path/example_fileset",
                                            properties={"k1": "v1"})
```

</TabItem>
</Tabs>

## Accessing a fileset with ADLS

### Using the GVFS Java client to access the fileset

To access fileset with Azure Blob Storage(ADLS) using the GVFS Java client, based on the [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item           | Description                             | Default value | Required | Since version    |
|------------------------------|-----------------------------------------|---------------|----------|------------------|
| `azure-storage-account-name` | The account name of Azure Blob Storage. | (none)        | Yes      | 0.8.0-incubating |
| `azure-storage-account-key`  | The account key of Azure Blob Storage.  | (none)        | Yes      | 0.8.0-incubating |

:::note
If the catalog has enabled [credential vending](security/credential-vending.md), the properties above can be omitted. More details can be found in [Fileset with credential vending](#fileset-with-credential-vending).
:::

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
conf.set("azure-storage-account-name", "account_name_of_adls");
conf.set("azure-storage-account-key", "account_key_of_adls");
Path filesetPath = new Path("gvfs://fileset/test_catalog/test_schema/test_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
...
```

Similar to Spark configurations, you need to add ADLS (bundle) jars to the classpath according to your environment.

If your wants to custom your hadoop version or there is already a hadoop version in your project, you can add the following dependencies to your `pom.xml`:

```xml
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-azure</artifactId>
    <version>${HADOOP_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-azure</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

Or use the bundle jar with Hadoop environment if there is no Hadoop environment:

```xml
  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-azure-bundle</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gravitino</groupId>
    <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
    <version>${GRAVITINO_VERSION}</version>
  </dependency>
```

### Using Spark to access the fileset

The following code snippet shows how to use **PySpark 3.1.3 with Hadoop environment(Hadoop 3.2.0)** and JDK8 to access the fileset:

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
metalake_name = "test"

catalog_name = "your_adls_catalog"
schema_name = "your_adls_schema"
fileset_name = "your_adls_fileset"
# JDK8
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-azure-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,/path/to/hadoop-azure-3.2.0.jar,/path/to/azure-storage-7.0.0.jar,/path/to/wildfly-openssl-1.0.4.Final.jar --master local[1] pyspark-shell"
# JDK17
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /path/to/gravitino-azure-{gravitino-version}.jar,/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar,/path/to/hadoop-azure-3.2.0.jar,/path/to/azure-storage-7.0.0.jar,/path/to/wildfly-openssl-1.0.4.Final.jar --conf \"spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --conf \"spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" --master local[1] pyspark-shell"
spark = SparkSession.builder
    .appName("adls_fileset_test")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    .config("spark.hadoop.azure-storage-account-name", "azure_account_name")
    .config("spark.hadoop.azure-storage-account-key", "azure_account_key")
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
If Spark can't start with the above configuration (no Hadoop environment available and use bundle jar), you can try to set the jars to the classpath directly:

```python
jars_path = (
    "/path/to/gravitino-azure-bundle-{gravitino-version}.jar:"
    "/path/to/gravitino-filesystem-hadoop3-runtime-{gravitino-version}.jar"
)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f'--conf "spark.driver.extraClassPath={jars_path}" '
    f'--conf "spark.executor.extraClassPath={jars_path}" '
    '--master local[1] pyspark-shell'
)
```

- [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle) is the Gravitino ADLS jar with Hadoop environment(3.3.1), `hadoop-azure.jar` and all packages needed to access ADLS.
- [`gravitino-azure-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure) is a condensed version of the Gravitino ADLS bundle jar without Hadoop environment and `hadoop-azure.jar`.
- `hadoop-azure-3.2.0.jar` and `azure-storage-7.0.0.jar` can be found in the Hadoop distribution in the `${HADOOP_HOME}/share/hadoop/tools/lib` directory.

Please choose the correct jar according to your environment.

:::note
In some Spark versions, a Hadoop environment is necessary for the driver, adding the bundle jars with '--jars' may not work. If this is the case, you should add the jars to the spark CLASSPATH directly.
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
    <name>azure-storage-account-name</name>
    <value>account_name</value>
  </property>
  <property>
    <name>azure-storage-account-key</name>
    <value>account_key</value>
  </property>
```

2. Add the necessary jars to the Hadoop classpath.

For ADLS, you need to add `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, `gravitino-azure-${gravitino-version}.jar` and `hadoop-azure-${hadoop-version}.jar` located at `${HADOOP_HOME}/share/hadoop/tools/lib/` to the Hadoop classpath. 

3. Run the following command to access the fileset:

```shell
./${HADOOP_HOME}/bin/hadoop dfs -ls gvfs://fileset/adls_catalog/adls_schema/adls_fileset
./${HADOOP_HOME}/bin/hadoop dfs -put /path/to/local/file gvfs://fileset/adls_catalog/adls_schema/adls_fileset
```

### Using the GVFS Python client to access a fileset

In order to access fileset with Azure Blob storage (ADLS) using the GVFS Python client, apart from [basic GVFS configurations](./how-to-use-gvfs.md#configuration-1), you need to add the following configurations:

| Configuration item           | Description                            | Default value | Required | Since version    |
|------------------------------|----------------------------------------|---------------|----------|------------------|
| `azure_storage_account_name` | The account name of Azure Blob Storage | (none)        | Yes      | 0.8.0-incubating |
| `azure_storage_account_key`  | The account key of Azure Blob Storage  | (none)        | Yes      | 0.8.0-incubating |

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
    "azure_storage_account_name": "azure_account_name",
    "azure_storage_account_key": "azure_account_key"
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090", metalake_name="test_metalake", options=options)
fs.ls("gvfs://fileset/{adls_catalog}/{adls_schema}/{adls_fileset}/")
```


### Using fileset with pandas

The following are examples of how to use the pandas library to access the ADLS fileset

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090", 
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

## Fileset with credential vending

Since 0.8.0-incubating, Gravitino supports credential vending for ADLS fileset. If the catalog has been [configured with credential](./security/credential-vending.md), you can access ADLS fileset without providing authentication information like `azure-storage-account-name` and `azure-storage-account-key` in the properties.

### How to create an ADLS Fileset catalog with credential vending

Apart from configuration method in [create-adls-fileset-catalog](#configuration-for-a-adls-fileset-catalog),
properties needed by [adls-credential](./security/credential-vending.md#adls-credentials) should
also be set to enable credential vending for ADLS fileset. Take `adls-token` credential provider for example:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "adls-catalog-with-token",
  "type": "FILESET",
  "comment": "This is a ADLS fileset catalog",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/path",
    "azure-storage-account-name": "The account name of the Azure Blob Storage",
    "azure-storage-account-key": "The account key of the Azure Blob Storage",
    "filesystem-providers": "abs",
    "credential-providers": "adls-token",
    "azure-tenant-id":"The Azure tenant id",
    "azure-client-id":"The Azure client id",
    "azure-client-secret":"The Azure client secret key"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

### How to access ADLS fileset with credential vending

When the catalog is configured with credentials and client-side credential vending is enabled, 
you can access ADLS filesets directly using the GVFS Java/Python client or Spark without providing authentication details.

GVFS Java client:

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "test_metalake");
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
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "test")
    # No need to set azure-storage-account-name and azure-storage-account-name
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate()
```

Python client and Hadoop command are similar to the above examples.
