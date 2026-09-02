---
title: "Fileset Catalog with ADLS"
slug: "/fileset-catalog-with-adls"
keyword: "Fileset catalog ADLS Azure Blob Storage"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This page shows how to store fileset data in Azure Data Lake Storage while Gravitino manages the metadata,
and how to read and write that data through the Gravitino Virtual File System (GVFS).

Everything on this page is specific to Azure Data Lake Storage. The fileset model itself, the properties shared by
every storage backend, and the way properties are inherited from catalog to schema to fileset are
described in [Fileset Catalog](./fileset-catalog.md).

The examples run in order and use the same names throughout: metalake `metalake`, catalog
`adls_catalog`, schema `adls_schema`, fileset `example_fileset`, and `http://localhost:8090` as the
server URL. Replace them with your own values.

## Prerequisites

1. Download the [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle) file.
2. Place it in the fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server:

```bash
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

The catalog automatically loads the Azure Data Lake Storage filesystem provider once the bundle jar is on the
classpath. The deprecated `filesystem-providers` and `default-filesystem-provider` catalog
properties do not need to be set.

## Azure Data Lake Storage Properties

These properties are needed in addition to the shared
[catalog properties](./fileset-catalog.md#catalog-properties). Configure credentials on the
catalog; GVFS clients fetch them through [credential vending](./security/credential-vending.md)
and must not set cloud credentials in local configuration. Non-secret settings are inherited
from catalog, schema, and fileset metadata — override them in GVFS configuration only when
needed. The Python client spells property names with underscores while the catalog and the
Java client use hyphens.

| Catalog and Java client      | Python client                | Description                                                                                                                                                                                                                                                                                                     | Required      |
|------------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `azure-storage-account-name` | `azure_storage_account_name` | Account name of the Azure Blob Storage.                                                                                                                                                                                                                                                                         | Yes (catalog) |
| `azure-storage-account-key`  | `azure_storage_account_key`  | Account key of the Azure Blob Storage.                                                                                                                                                                                                                                                                          | Yes (catalog) |
| `credential-providers`       | (n/a)                        | The credential provider types, separated by comma. Possible values are `adls-token`, `azure-account-key`. When set explicitly, chooses how the server vends credentials. If omitted, Gravitino auto-detects a provider from the static credentials on the catalog. See [credential vending](./security/credential-vending.md#adls) for the extra properties each provider takes. | No            |

:::note
Azure Data Lake Storage is also known as Azure Blob Storage (ABS). The location uses the `abfss://`
scheme.
:::

Schema and fileset properties are documented on the shared page: see
[schema properties](./fileset-catalog.md#schema-properties) and
[fileset properties](./fileset-catalog.md#fileset-properties).

A fileset catalog stores its data under `location`, which for Azure Data Lake Storage looks like
`abfss://container@account-name.dfs.core.windows.net/root`.

## Create the Catalog, Schema, and Fileset

### Step 1: Create the catalog

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "adls_catalog",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Azure Data Lake Storage",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/root",
    "azure-storage-account-name": "account_name",
    "azure-storage-account-key": "account_key"
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

Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container@account-name.dfs.core.windows.net/root")
    .put("azure-storage-account-name", "account_name")
    .put("azure-storage-account-key", "account_key")
    .build();

Catalog catalog = gravitinoClient.createCatalog("adls_catalog",
    Catalog.Type.FILESET,
    "A fileset catalog backed by Azure Data Lake Storage",
    catalogProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(
    uri="http://localhost:8090", metalake_name="metalake")

catalog_properties = {
    "location": "abfss://container@account-name.dfs.core.windows.net/root",
    "azure-storage-account-name": "account_name",
    "azure-storage-account-key": "account_key",
}

catalog = gravitino_client.create_catalog(name="adls_catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider=None,
                                          comment="A fileset catalog backed by Azure Data Lake Storage",
                                          properties=catalog_properties)
```

</TabItem>
</Tabs>

### Step 2: Create the schema

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "adls_schema",
  "comment": "A schema in the Azure Data Lake Storage fileset catalog",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/adls_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("adls_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "abfss://container@account-name.dfs.core.windows.net/root/schema")
    .build();

Schema schema = supportsSchemas.createSchema("adls_schema",
    "A schema in the Azure Data Lake Storage fileset catalog",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="adls_catalog")
catalog.as_schemas().create_schema(name="adls_schema",
                                   comment="A schema in the Azure Data Lake Storage fileset catalog",
                                   properties={"location": "abfss://container@account-name.dfs.core.windows.net/root/schema"})
```

</TabItem>
</Tabs>

### Step 3: Create the fileset

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "abfss://container@account-name.dfs.core.windows.net/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/adls_catalog/schemas/adls_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("adls_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> filesetProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("adls_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "abfss://container@account-name.dfs.core.windows.net/root/schema/example_fileset",
    filesetProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="adls_catalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("adls_schema", "example_fileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="abfss://container@account-name.dfs.core.windows.net/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The fileset is now addressable as
`gvfs://fileset/adls_catalog/adls_schema/example_fileset` from any GVFS client.

## Access the Fileset

### Java client jars

Every Java or Hadoop-based client needs `gravitino-filesystem-hadoop3-runtime`, which is published
on Maven Central, plus the Azure Data Lake Storage filesystem implementation. Only the latter
differs by environment:

| Environment            | Jar providing the Azure Data Lake Storage filesystem                                                                                                                                                      |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No Hadoop installed    | [`gravitino-azure-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle), a fat jar bundling the Azure Data Lake Storage filesystem implementation and its dependencies |
| Hadoop already present | `hadoop-azure-${hadoop-version}.jar`, `azure-storage-7.0.1.jar` and `wildfly-openssl-1.0.7.Final.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`                                  |

The artifacts in full:

- [`gravitino-azure-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle):
  a "fat" jar that includes the `gravitino-azure` functionality together with every dependency it needs,
  such as `hadoop-azure` and the packages it needs to reach ADLS. Use it when the environment has no pre-existing Hadoop setup.
- [`gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-filesystem-hadoop3-runtime):
  a "fat" jar that bundles the Gravitino virtual filesystem client and already includes the
  `gravitino-azure` functionality. Java and Hadoop-based clients require it to access Gravitino
  filesets.
- `hadoop-azure-${hadoop-version}.jar`, `azure-storage-7.0.1.jar` and
  `wildfly-openssl-1.0.7.Final.jar`: the standard Hadoop dependencies for Azure Data Lake Storage
  access, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`. Supply them yourself
  when running inside an existing Hadoop environment.
- [`gravitino-azure-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure):
  a "thin" jar carrying only the Azure integration code. It is already contained in both jars above,
  so it is not needed as a direct dependency unless you prefer to manage all Hadoop and Azure
  dependencies yourself.

```xml
<!-- No Hadoop environment -->
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

```xml
<!-- Existing Hadoop environment -->
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
```

:::note
The thin `gravitino-azure` jar is not needed. Its functionality is already included in both
`gravitino-azure-bundle` and `gravitino-filesystem-hadoop3-runtime`.
:::

### GVFS Java client

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration), configure the Gravitino
connection. Cloud credentials are fetched from the server; do not set account keys in the Hadoop
`Configuration`.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");

Path filesetPath = new Path("gvfs://fileset/adls_catalog/adls_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

### Apache Spark

The example below uses PySpark 3.5.0 in an environment that already has Hadoop 3.3.4.

```bash
pip install pyspark==3.5.0
pip install apache-gravitino==${GRAVITINO_VERSION}
```

```python
import os
from pyspark.sql import SparkSession

# On JDK 17, also add:
#   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
#   --conf "spark.executor.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar,"
    "/path/to/hadoop-azure-3.3.4.jar,"
    "/path/to/azure-storage-7.0.1.jar,"
    "/path/to/wildfly-openssl-1.0.7.Final.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("adls_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate())

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
spark_df = spark.createDataFrame(data, schema=["Name", "Age"])
gvfs_path = "gvfs://fileset/adls_catalog/adls_schema/example_fileset/people"

spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gvfs_path)
```

If Spark runs without a Hadoop environment, only the jar list changes:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-azure-bundle-${gravitino-version}.jar,"
    "/path/to/gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar "
    "--master local[1] pyspark-shell"
)
```

:::note
Some Spark versions need a Hadoop environment in the driver and do not pick up filesystem
implementations passed with `--jars`. If that happens, add the jars to the Spark classpath directly.
:::

### Hadoop fs command

1. Add the following to `${HADOOP_HOME}/etc/hadoop/core-site.xml`:

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
  <value>metalake</value>
</property>
```

2. Add these jars to the Hadoop classpath:

   - `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, from Maven Central.
   - `hadoop-azure-${hadoop-version}.jar`, `azure-storage-7.0.1.jar` and `wildfly-openssl-1.0.7.Final.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`.

3. Access the fileset:

```shell
${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/adls_catalog/adls_schema/example_fileset
${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/adls_catalog/adls_schema/example_fileset
```

### GVFS Python client

```bash
pip install apache-gravitino==${GRAVITINO_VERSION}
```

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration-1), pass Gravitino
connection settings in `options`. Do not pass cloud access keys; GVFS fetches credentials from the
server.

```python
from gravitino import gvfs

options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
}

fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
fs.ls("gvfs://fileset/adls_catalog/adls_schema/example_fileset/")
```

### pandas

pandas reaches the same paths through `storage_options`. Use the `fs` instance from the preceding
GVFS example to discover the generated Spark part file.

```python
import pandas as pd

storage_options = {
    "server_uri": "http://localhost:8090",
    "metalake_name": "metalake",
    "options": {
        "auth_type": "simple",
    }
}

csv_path = next(
    f"gvfs://{path}"
    for path in fs.ls(
        "gvfs://fileset/adls_catalog/adls_schema/example_fileset/people",
        detail=False,
    )
    if (
        path.rsplit("/", 1)[-1].startswith("part-")
        and path.endswith(".csv")
    )
)
ds = pd.read_csv(csv_path, storage_options=storage_options)
ds.head()
```

For further use cases, see [Gravitino Virtual File System](./how-to-use-gvfs.md).

## Credential Vending

GVFS always uses credential vending for cloud filesets: the catalog holds the Azure Data Lake Storage
credentials and the Gravitino server hands out a credential per request, so clients never configure
cloud keys locally. See [Credential Vending](./security/credential-vending.md) for the general
mechanism and [ADLS credentials](./security/credential-vending.md#adls) for the properties each
provider takes.

The supported providers are `adls-token`, which vends a short-lived token, and
`azure-account-key`, which vends the static account key configured on the catalog. The example below
uses `adls-token`.

### Configure the catalog, schema, and fileset

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "adls_catalog_with_vending",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Azure Data Lake Storage with credential vending",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/root",
    "azure-storage-account-name": "account_name",
    "azure-storage-account-key": "account_key",
    "credential-providers": "adls-token",
    "azure-tenant-id": "The Azure tenant id",
    "azure-client-id": "The Azure client id",
    "azure-client-secret": "The Azure client secret key"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

Create the schema and fileset in the credential-vending catalog:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "adls_schema",
  "comment": "A schema in the Azure Data Lake Storage credential-vending catalog",
  "properties": {
    "location": "abfss://container@account-name.dfs.core.windows.net/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/adls_catalog_with_vending/schemas

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "abfss://container@account-name.dfs.core.windows.net/root/schema/example_fileset",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/adls_catalog_with_vending/schemas/adls_schema/filesets
```

The `adls-token` provider needs three more catalog properties.

| Property Name         | Description             |
|-----------------------|-------------------------|
| `azure-tenant-id`     | Azure tenant id         |
| `azure-client-id`     | Azure client id         |
| `azure-client-secret` | Azure client secret key |

### GVFS client configuration

Configure only the Gravitino connection on the client. Cloud credentials come from the server.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");

Path filesetPath = new Path(
    "gvfs://fileset/adls_catalog_with_vending/adls_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

```python
spark = (SparkSession.builder
    .appName("adls_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .getOrCreate())
```

```python
options = {
    "auth_type": "simple",
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
```
