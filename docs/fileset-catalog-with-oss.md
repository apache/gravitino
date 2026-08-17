---
title: "Fileset Catalog with OSS"
slug: "/fileset-catalog-with-oss"
keyword: "Fileset catalog OSS"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This page shows how to store fileset data in Alibaba Cloud OSS while Gravitino manages the metadata,
and how to read and write that data through the Gravitino Virtual File System (GVFS).

Everything on this page is specific to Alibaba Cloud OSS. The fileset model itself, the properties shared by
every storage backend, and the way properties are inherited from catalog to schema to fileset are
described in [Fileset Catalog](./fileset-catalog.md).

The examples run in order and use the same names throughout: metalake `metalake`, catalog
`oss_catalog`, schema `oss_schema`, fileset `example_fileset`, and `http://localhost:8090` as the
server URL. Replace them with your own values.

## Prerequisites

1. Download the [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle) file.
2. Place it in the fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server:

```bash
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

The catalog automatically loads the Alibaba Cloud OSS filesystem provider once the bundle jar is on the
classpath. The deprecated `filesystem-providers` and `default-filesystem-provider` catalog
properties do not need to be set.

## Alibaba Cloud OSS Properties

These properties are needed in addition to the shared
[catalog properties](./fileset-catalog.md#catalog-properties). The same values are also needed by
the GVFS clients, so they are listed together here — note that the Python client spells them with
underscores while the catalog and the Java client use hyphens.

| Catalog and Java client | Python client           | Description                                                                                                                                                                                                                                                                                                | Required |
|-------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `oss-endpoint`          | `oss_endpoint`          | Endpoint of the Aliyun OSS service.                                                                                                                                                                                                                                                                        | Yes      |
| `oss-access-key-id`     | `oss_access_key_id`     | Access key of the Aliyun OSS service.                                                                                                                                                                                                                                                                      | Yes      |
| `oss-secret-access-key` | `oss_secret_access_key` | Secret key of the Aliyun OSS service.                                                                                                                                                                                                                                                                      | Yes      |
| `credential-providers`  | (n/a)                   | The credential provider types, separated by comma. Possible values are `oss-token`, `oss-secret-key`. Setting it enables credential vending, so clients no longer need the credentials above. See [credential vending](./security/credential-vending.md#oss) for the extra properties each provider takes. | No       |

Schema and fileset properties are documented on the shared page: see
[schema properties](./fileset-catalog.md#schema-properties) and
[fileset properties](./fileset-catalog.md#fileset-properties).

A fileset catalog stores its data under `location`, which for Alibaba Cloud OSS looks like
`oss://bucket/root`.

## Create the Catalog, Schema, and Fileset

### Step 1: Create the catalog

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "oss_catalog",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Alibaba Cloud OSS",
  "properties": {
    "location": "oss://bucket/root",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key"
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
    .put("location", "oss://bucket/root")
    .put("oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .put("oss-access-key-id", "access_key")
    .put("oss-secret-access-key", "secret_key")
    .build();

Catalog catalog = gravitinoClient.createCatalog("oss_catalog",
    Catalog.Type.FILESET,
    "A fileset catalog backed by Alibaba Cloud OSS",
    catalogProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(
    uri="http://localhost:8090", metalake_name="metalake")

catalog_properties = {
    "location": "oss://bucket/root",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key",
}

catalog = gravitino_client.create_catalog(name="oss_catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider=None,
                                          comment="A fileset catalog backed by Alibaba Cloud OSS",
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
  "name": "oss_schema",
  "comment": "A schema in the Alibaba Cloud OSS fileset catalog",
  "properties": {
    "location": "oss://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/oss_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("oss_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "oss://bucket/root/schema")
    .build();

Schema schema = supportsSchemas.createSchema("oss_schema",
    "A schema in the Alibaba Cloud OSS fileset catalog",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="oss_catalog")
catalog.as_schemas().create_schema(name="oss_schema",
                                   comment="A schema in the Alibaba Cloud OSS fileset catalog",
                                   properties={"location": "oss://bucket/root/schema"})
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
  "storageLocation": "oss://bucket/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/oss_catalog/schemas/oss_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("oss_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> filesetProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("oss_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "oss://bucket/root/schema/example_fileset",
    filesetProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="oss_catalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("oss_schema", "example_fileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="oss://bucket/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The fileset is now addressable as
`gvfs://fileset/oss_catalog/oss_schema/example_fileset` from any GVFS client.

## Access the Fileset

### Java client jars

Every Java or Hadoop-based client needs `gravitino-filesystem-hadoop3-runtime`, which is published
on Maven Central, plus the Alibaba Cloud OSS filesystem implementation. Only the latter differs by
environment:

| Environment            | Jar providing the Alibaba Cloud OSS filesystem                                                                                                                                                        |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No Hadoop installed    | [`gravitino-aliyun-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle), a fat jar bundling the Alibaba Cloud OSS filesystem implementation and its dependencies |
| Hadoop already present | `hadoop-aliyun-${hadoop-version}.jar`, `aliyun-sdk-oss-3.13.0.jar` and `jdom2-2.0.6.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`                                           |

The artifacts in full:

- [`gravitino-aliyun-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle):
  a "fat" jar that includes the `gravitino-aliyun` functionality together with every dependency it needs,
  such as `hadoop-aliyun` and `aliyun-sdk-oss`. Use it when the environment has no pre-existing Hadoop setup.
- [`gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-filesystem-hadoop3-runtime):
  a "fat" jar that bundles the Gravitino virtual filesystem client and already includes the
  `gravitino-aliyun` functionality. Java and Hadoop-based clients require it to access Gravitino
  filesets.
- `hadoop-aliyun-${hadoop-version}.jar`, `aliyun-sdk-oss-3.13.0.jar` and `jdom2-2.0.6.jar`: the
  standard Hadoop dependencies for Alibaba Cloud OSS access, shipped with Hadoop under
  `${HADOOP_HOME}/share/hadoop/tools/lib`. Supply them yourself when running inside an existing
  Hadoop environment.
- [`gravitino-aliyun-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun):
  a "thin" jar carrying only the Aliyun integration code. It is already contained in both jars
  above, so it is not needed as a direct dependency unless you prefer to manage all Hadoop and
  Aliyun dependencies yourself.

```xml
<!-- No Hadoop environment -->
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-aliyun-bundle</artifactId>
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
  <artifactId>hadoop-aliyun</artifactId>
  <version>${HADOOP_VERSION}</version>
</dependency>
<dependency>
  <groupId>org.apache.gravitino</groupId>
  <artifactId>gravitino-filesystem-hadoop3-runtime</artifactId>
  <version>${GRAVITINO_VERSION}</version>
</dependency>
```

:::note
The thin `gravitino-aliyun` jar is not needed. Its functionality is already included in both
`gravitino-aliyun-bundle` and `gravitino-filesystem-hadoop3-runtime`.
:::

### GVFS Java client

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration), set the Alibaba Cloud OSS
properties from the table above.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
conf.set("oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com");
conf.set("oss-access-key-id", "access_key");
conf.set("oss-secret-access-key", "secret_key");

Path filesetPath = new Path("gvfs://fileset/oss_catalog/oss_schema/example_fileset/new_dir");
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
    "/path/to/hadoop-aliyun-3.3.4.jar,"
    "/path/to/aliyun-sdk-oss-3.13.0.jar,"
    "/path/to/jdom2-2.0.6.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("oss_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .config("spark.hadoop.oss-endpoint", "http://oss-cn-hangzhou.aliyuncs.com")
    .config("spark.hadoop.oss-access-key-id", "access_key")
    .config("spark.hadoop.oss-secret-access-key", "secret_key")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate())

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
spark_df = spark.createDataFrame(data, schema=["Name", "Age"])
gvfs_path = "gvfs://fileset/oss_catalog/oss_schema/example_fileset/people"

spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gvfs_path)
```

If Spark runs without a Hadoop environment, only the jar list changes:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-aliyun-bundle-${gravitino-version}.jar,"
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
<property>
  <name>oss-endpoint</name>
  <value>http://oss-cn-hangzhou.aliyuncs.com</value>
</property>
<property>
  <name>oss-access-key-id</name>
  <value>access_key</value>
</property>
<property>
  <name>oss-secret-access-key</name>
  <value>secret_key</value>
</property>
```

2. Add these jars to the Hadoop classpath:

   - `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, from Maven Central.
   - `hadoop-aliyun-${hadoop-version}.jar`, `aliyun-sdk-oss-3.13.0.jar` and `jdom2-2.0.6.jar`, shipped with Hadoop under `${HADOOP_HOME}/share/hadoop/tools/lib`.

3. Access the fileset:

```shell
${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/oss_catalog/oss_schema/example_fileset
${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/oss_catalog/oss_schema/example_fileset
```

### GVFS Python client

```bash
pip install apache-gravitino==${GRAVITINO_VERSION}
```

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration-1), pass the Alibaba Cloud OSS
properties in `options`, spelled with underscores.

```python
from gravitino import gvfs

options = {
    "cache_size": 20,
    "cache_expired_time": 3600,
    "auth_type": "simple",
    "oss_endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "oss_access_key_id": "access_key",
    "oss_secret_access_key": "secret_key",
}

fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
fs.ls("gvfs://fileset/oss_catalog/oss_schema/example_fileset/")
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
        "oss_endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
        "oss_access_key_id": "access_key",
        "oss_secret_access_key": "secret_key",
    }
}

csv_path = next(
    f"gvfs://{path}"
    for path in fs.ls(
        "gvfs://fileset/oss_catalog/oss_schema/example_fileset/people",
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

With credential vending the catalog holds the Alibaba Cloud OSS credentials and the Gravitino server hands
out a credential per request, so clients never hold cloud keys of their own. See
[Credential Vending](./security/credential-vending.md) for the general mechanism and
[OSS credentials](./security/credential-vending.md#oss) for the properties
each provider takes.

The supported providers are `oss-token`, which vends a short-lived STS token, and
`oss-secret-key`, which vends the static access key configured on the catalog. The example below uses
`oss-token`.

### Configure the catalog, schema, and fileset

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "oss_catalog_with_vending",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Alibaba Cloud OSS with credential vending",
  "properties": {
    "location": "oss://bucket/root",
    "oss-endpoint": "http://oss-cn-hangzhou.aliyuncs.com",
    "oss-access-key-id": "access_key",
    "oss-secret-access-key": "secret_key",
    "credential-providers": "oss-token",
    "oss-region": "oss-cn-hangzhou",
    "oss-role-arn": "The ARN of the role that grants access to the OSS data"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

Create the schema and fileset in the credential-vending catalog:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "oss_schema",
  "comment": "A schema in the Alibaba Cloud OSS credential-vending catalog",
  "properties": {
    "location": "oss://bucket/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/oss_catalog_with_vending/schemas

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "oss://bucket/root/schema/example_fileset",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/oss_catalog_with_vending/schemas/oss_schema/filesets
```

The `oss-token` provider needs two more catalog properties.

| Property Name  | Description                                         |
|----------------|-----------------------------------------------------|
| `oss-region`   | Region of the bucket, for example `oss-cn-hangzhou` |
| `oss-role-arn` | ARN of the role that grants access to the data      |

### Access without local credentials

Enable vending on the client and drop the credential properties.

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
// No need to set oss-access-key-id or oss-secret-access-key

Path filesetPath = new Path(
    "gvfs://fileset/oss_catalog_with_vending/oss_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

```python
spark = (SparkSession.builder
    .appName("oss_fileset")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    # No need to set oss-access-key-id or oss-secret-access-key
    .getOrCreate())
```

```python
options = {
    "auth_type": "simple",
    "enable_credential_vending": True,
    # No need to set oss-access-key-id or oss-secret-access-key
}
fs = gvfs.GravitinoVirtualFileSystem(server_uri="http://localhost:8090",
                                     metalake_name="metalake",
                                     options=options)
```
