---
title: "Fileset Catalog with COS"
slug: "/fileset-catalog-with-cos"
keyword: "Fileset catalog COS"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

This page shows how to store fileset data in Tencent Cloud COS while Gravitino manages the metadata,
and how to read and write that data through the Gravitino Virtual File System (GVFS).

Everything on this page is specific to Tencent Cloud COS. The fileset model itself, the properties shared by
every storage backend, and the way properties are inherited from catalog to schema to fileset are
described in [Fileset Catalog](./fileset-catalog.md).

The examples run in order and use the same names throughout: metalake `metalake`, catalog
`cos_catalog`, schema `cos_schema`, fileset `example_fileset`, and `http://localhost:8090` as the
server URL. Replace them with your own values.

## Prerequisites

1. Download the [`gravitino-tencent-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle) file.
2. Place it in the fileset catalog classpath at `${GRAVITINO_HOME}/catalogs/fileset/libs/`.
3. Start the Gravitino server:

```bash
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

The catalog automatically loads the Tencent Cloud COS filesystem provider once the bundle jar is on the
classpath. The deprecated `filesystem-providers` and `default-filesystem-provider` catalog
properties do not need to be set.

## Tencent Cloud COS Properties

These properties are needed in addition to the shared
[catalog properties](./fileset-catalog.md#catalog-properties). The same values are also needed by
the GVFS clients, so they are listed together here — note that the Python client spells them with
underscores while the catalog and the Java client use hyphens.

| Catalog and Java client    | Python client              | Description                                                                                                                                                                                                                                                                                                                                                                                       | Required |
|----------------------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `cos-region`               | `cos_region`               | Region of the COS bucket, for example `ap-guangzhou` or `ap-shanghai`.                                                                                                                                                                                                                                                                                                                            | Yes      |
| `cos-endpoint`             | `cos_endpoint`             | Endpoint *suffix* of the COS service, mapped to `fs.cosn.bucket.endpoint_suffix`. It is a host suffix, not a URL — `cos.ap-guangzhou.myqcloud.com`, not `https://cos.ap-guangzhou.myqcloud.com`. When unset, hadoop-cos derives it from `cos-region`. Set it only to reach a non-public endpoint such as a VPC endpoint.                                                                          | No       |
| `cos-access-key-id`        | `cos_access_key_id`        | Static access key id, the Tencent Cloud `SecretId`.                                                                                                                                                                                                                                                                                                                                               | Yes      |
| `cos-secret-access-key`    | `cos_secret_access_key`    | Static secret access key, the Tencent Cloud `SecretKey`.                                                                                                                                                                                                                                                                                                                                          | Yes      |
| `credential-providers`     | (n/a)                      | The credential provider types, separated by comma. Supported values are `cos-secret-key` (static AK/SK vended by the server) and `cos-token` (short-lived STS token issued via CAM `AssumeRole`). Setting it enables credential vending, so clients no longer need the credentials above. See [credential vending](./security/credential-vending.md) for the extra properties each provider takes.| No       |
| `cos-role-arn`             | `cos_role_arn`             | The CAM role ARN that the Gravitino server assumes when issuing STS temporary credentials, e.g. `qcs::cam::uin/100012345678:roleName/GravitinoCOSAccess`. Required only when `credential-providers` includes `cos-token`.                                                                                                                                                                         | No       |
| `cos-app-id`               | `cos_app_id`               | The numeric Tencent Cloud AppId of the bucket owner (the trailing segment of the bucket name, e.g. `1250000000`). Required only when `credential-providers` includes `cos-token`; used to build the resource ARN in the STS session policy.                                                                                                                                                       | No       |
| `cos-external-id`          | `cos_external_id`          | Optional `ExternalId` propagated to the STS `AssumeRole` call to lock the role's trust policy to Gravitino. Only meaningful when `credential-providers` includes `cos-token`.                                                                                                                                                                                                                     | No       |
| `cos-token-expire-in-secs` | `cos_token_expire_in_secs` | The COS STS token expire time in seconds. Must not exceed the role's max session duration. Only meaningful when `credential-providers` includes `cos-token`. Defaults to `3600`.                                                                                                                                                                                                                  | No       |

:::note
`default-filesystem-provider` and `filesystem-providers` are deprecated. The fileset catalog
automatically loads the filesystem providers found on the classpath, including the built-in
providers and the cloud providers carried by a bundle jar such as `gravitino-tencent-bundle`.
:::

:::note
`cos-region` is mandatory for hadoop-cos: signing requests, building the default endpoint and
selecting the right CAM scope all need the region. Keep it set even when `cos-endpoint` is also set.
:::

Schema and fileset properties are documented on the shared page: see
[schema properties](./fileset-catalog.md#schema-properties) and
[fileset properties](./fileset-catalog.md#fileset-properties).

A fileset catalog stores its data under `location`, which for Tencent Cloud COS looks like
`cosn://my-bucket-1250000000/root`.

## Create the Catalog, Schema, and Fileset

### Step 1: Create the catalog

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "cos_catalog",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Tencent Cloud COS",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-endpoint": "cos.ap-guangzhou.myqcloud.com",
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

Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
    .put("location", "cosn://my-bucket-1250000000/root")
    .put("cos-region", "ap-guangzhou")
    .put("cos-endpoint", "cos.ap-guangzhou.myqcloud.com")
    .put("cos-access-key-id", "access_key")
    .put("cos-secret-access-key", "secret_key")
    .build();

Catalog catalog = gravitinoClient.createCatalog("cos_catalog",
    Catalog.Type.FILESET,
    "A fileset catalog backed by Tencent Cloud COS",
    catalogProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
gravitino_client: GravitinoClient = GravitinoClient(
    uri="http://localhost:8090", metalake_name="metalake")

catalog_properties = {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-endpoint": "cos.ap-guangzhou.myqcloud.com",
    "cos-access-key-id": "access_key",
    "cos-secret-access-key": "secret_key",
}

catalog = gravitino_client.create_catalog(name="cos_catalog",
                                          catalog_type=Catalog.Type.FILESET,
                                          provider=None,
                                          comment="A fileset catalog backed by Tencent Cloud COS",
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
  "name": "cos_schema",
  "comment": "A schema in the Tencent Cloud COS fileset catalog",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/cos_catalog/schemas
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("cos_catalog");
SupportsSchemas supportsSchemas = catalog.asSchemas();

Map<String, String> schemaProperties = ImmutableMap.<String, String>builder()
    .put("location", "cosn://my-bucket-1250000000/root/schema")
    .build();

Schema schema = supportsSchemas.createSchema("cos_schema",
    "A schema in the Tencent Cloud COS fileset catalog",
    schemaProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="cos_catalog")
catalog.as_schemas().create_schema(name="cos_schema",
                                   comment="A schema in the Tencent Cloud COS fileset catalog",
                                   properties={"location": "cosn://my-bucket-1250000000/root/schema"})
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
  "storageLocation": "cosn://my-bucket-1250000000/root/schema/example_fileset",
  "properties": {
    "k1": "v1"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/cos_catalog/schemas/cos_schema/filesets
```

</TabItem>
<TabItem value="java" label="Java">

```java
Catalog catalog = gravitinoClient.loadCatalog("cos_catalog");
FilesetCatalog filesetCatalog = catalog.asFilesetCatalog();

Map<String, String> filesetProperties = ImmutableMap.<String, String>builder()
    .put("k1", "v1")
    .build();

filesetCatalog.createFileset(
    NameIdentifier.of("cos_schema", "example_fileset"),
    "This is an example fileset",
    Fileset.Type.MANAGED,
    "cosn://my-bucket-1250000000/root/schema/example_fileset",
    filesetProperties);
```

</TabItem>
<TabItem value="python" label="Python">

```python
catalog: Catalog = gravitino_client.load_catalog(name="cos_catalog")
catalog.as_fileset_catalog().create_fileset(
    ident=NameIdentifier.of("cos_schema", "example_fileset"),
    type=Fileset.Type.MANAGED,
    comment="This is an example fileset",
    storage_location="cosn://my-bucket-1250000000/root/schema/example_fileset",
    properties={"k1": "v1"})
```

</TabItem>
</Tabs>

The fileset is now addressable as
`gvfs://fileset/cos_catalog/cos_schema/example_fileset` from any GVFS client.

## Access the Fileset

### Java client jars

Every Java or Hadoop-based client needs `gravitino-filesystem-hadoop3-runtime`, which is published
on Maven Central, plus the Tencent Cloud COS filesystem implementation. Only the latter differs by
environment:

| Environment            | Jar providing the Tencent Cloud COS filesystem                                                                                                                                                      |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| No Hadoop installed    | [`gravitino-tencent-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle), a fat jar bundling `hadoop-cos` and the Tencent Cloud COS Java SDK                  |
| Hadoop already present | `hadoop-cos-3.3.0-8.3.23.jar` and `cos_api-bundle-5.6.227.jar`, published by Tencent Cloud on Maven Central and, unlike `hadoop-aws` or `hadoop-aliyun`, not part of the Apache Hadoop distribution |

The artifacts in full:

- [`gravitino-tencent-bundle-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle):
  a "fat" jar that includes the `gravitino-tencent` functionality together with every dependency it needs,
  such as `hadoop-cos` and the Tencent Cloud COS Java SDK. Use it when the environment has no pre-existing Hadoop setup.
- [`gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-filesystem-hadoop3-runtime):
  a "fat" jar that bundles the Gravitino virtual filesystem client and already includes the
  `gravitino-tencent` functionality. Java and Hadoop-based clients require it to access Gravitino
  filesets.
- `hadoop-cos-3.3.0-8.3.23.jar` and `cos_api-bundle-5.6.227.jar`: the standard Hadoop dependencies
  for Tencent Cloud COS access, published by Tencent Cloud on Maven Central and, unlike `hadoop-aws`
  or `hadoop-aliyun`, not part of the Apache Hadoop distribution. Supply them yourself when running
  inside an existing Hadoop environment.

```xml
<!-- No Hadoop environment -->
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

```xml
<!-- Existing Hadoop environment -->
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
The thin `gravitino-tencent` jar is not needed. Its functionality is already included in both
`gravitino-tencent-bundle` and `gravitino-filesystem-hadoop3-runtime`.
:::

### GVFS Java client

On top of the [base GVFS configuration](./how-to-use-gvfs.md#configuration), set the Tencent Cloud COS
properties from the table above.

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
conf.set("cos-region", "ap-guangzhou");
conf.set("cos-endpoint", "cos.ap-guangzhou.myqcloud.com");
conf.set("cos-access-key-id", "access_key");
conf.set("cos-secret-access-key", "secret_key");

Path filesetPath = new Path("gvfs://fileset/cos_catalog/cos_schema/example_fileset/new_dir");
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
    "/path/to/hadoop-cos-3.3.0-8.3.23.jar,"
    "/path/to/cos_api-bundle-5.6.227.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("cos_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    .config("spark.hadoop.cos-region", "ap-guangzhou")
    .config("spark.hadoop.cos-endpoint", "cos.ap-guangzhou.myqcloud.com")
    .config("spark.hadoop.cos-access-key-id", "access_key")
    .config("spark.hadoop.cos-secret-access-key", "secret_key")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.port", "2048")
    .getOrCreate())

data = [("Alice", 25), ("Bob", 30), ("Cathy", 45)]
spark_df = spark.createDataFrame(data, schema=["Name", "Age"])
gvfs_path = "gvfs://fileset/cos_catalog/cos_schema/example_fileset/people"

spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gvfs_path)
```

If Spark runs without a Hadoop environment, only the jar list changes:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-tencent-bundle-${gravitino-version}.jar,"
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
  <name>cos-region</name>
  <value>ap-guangzhou</value>
</property>
<property>
  <name>cos-endpoint</name>
  <value>cos.ap-guangzhou.myqcloud.com</value>
</property>
<property>
  <name>cos-access-key-id</name>
  <value>access_key</value>
</property>
<property>
  <name>cos-secret-access-key</name>
  <value>secret_key</value>
</property>
```

2. Add these jars to the Hadoop classpath:

   - `gravitino-filesystem-hadoop3-runtime-${gravitino-version}.jar`, from Maven Central.
   - `hadoop-cos-3.3.0-8.3.23.jar` and `cos_api-bundle-5.6.227.jar`, published by Tencent Cloud on Maven Central and, unlike `hadoop-aws` or `hadoop-aliyun`, not part of the Apache Hadoop distribution.

3. Access the fileset:

```shell
${HADOOP_HOME}/bin/hadoop fs -ls gvfs://fileset/cos_catalog/cos_schema/example_fileset
${HADOOP_HOME}/bin/hadoop fs -put /path/to/local/file gvfs://fileset/cos_catalog/cos_schema/example_fileset
```

### GVFS Python client and pandas

:::note
The GVFS Python client does not yet ship a COS storage handler. It cannot read or write
COS-backed filesets through `gvfs.GravitinoVirtualFileSystem` or pandas
`read_csv("gvfs://...")`. Use the GVFS Java client, Spark, or `hadoop fs` for COS data access.

This limitation does not affect the Python `GravitinoClient` metadata API. It can still create,
inspect, update, and delete COS catalogs, schemas, and filesets as shown in
[Create the Catalog, Schema, and Fileset](#create-the-catalog-schema-and-fileset).
:::

For further Java client use cases, see
[Gravitino Virtual File System](./how-to-use-gvfs.md).

## Credential Vending

With credential vending the catalog holds the Tencent Cloud COS credentials and the Gravitino
server hands out a credential per request, so clients never hold cloud keys of their own. See
[Credential Vending](./security/credential-vending.md) for the general mechanism.

The currently supported credential providers are listed below:

| Credential provider | Description                                                                                                                                                                                                                                                                                                                                                                                        | Vended credential type |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
| `cos-secret-key`    | The Gravitino server hands out the static `cos-access-key-id` / `cos-secret-access-key` configured on the catalog. Useful for centralising credentials on the server side.                                                                                                                                                                                                                         | Static AK/SK           |
| `cos-token`         | The Gravitino server calls Tencent Cloud CAM `AssumeRole` and hands out a short-lived STS triple (`TmpSecretId` / `TmpSecretKey` / `SessionToken`), scoped down to the fileset paths that the client requested. Requires `cos-role-arn` and `cos-app-id` on the catalog and CAM permissions (`sts:AssumeRole` / `cam:GetFederationToken`) on the account whose AK/SK is configured on the catalog. | Short-lived STS token  |

### Configure the catalog, schema, and fileset

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "cos_catalog_with_vending",
  "type": "FILESET",
  "comment": "A fileset catalog backed by Tencent Cloud COS with credential vending",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-endpoint": "cos.ap-guangzhou.myqcloud.com",
    "cos-access-key-id": "access_key",
    "cos-secret-access-key": "secret_key",
    "credential-providers": "cos-secret-key"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

To enable STS-based credential vending instead (recommended for production, since the AK/SK on the catalog never leaves the server), switch `credential-providers` to `cos-token` and add the CAM role wiring. The AK/SK below is the *server-side* account used to call `sts:AssumeRole`; the vended clients will only ever see the short-lived session token issued for the requested fileset paths:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "cos-catalog-with-sts-vending",
  "type": "FILESET",
  "comment": "This is a COS fileset catalog with STS credential vending",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root",
    "cos-region": "ap-guangzhou",
    "cos-access-key-id": "server_access_key",
    "cos-secret-access-key": "server_secret_key",
    "credential-providers": "cos-token",
    "cos-role-arn": "qcs::cam::uin/100012345678:roleName/GravitinoCOSAccess",
    "cos-app-id": "1250000000",
    "cos-token-expire-in-secs": "1800"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs
```

:::note
When using `cos-token`, the CAM role referenced by `cos-role-arn` must (1) trust the Gravitino server principal (or use `cos-external-id` for stricter matching) and (2) have COS read/write permissions on the bucket paths you plan to expose. Gravitino narrows the vended token further via a session policy that only allows the fileset's own read/write locations, so the role can be as broad as your organisation's policies allow — the effective permission is the intersection.
:::

Create the schema and fileset in the credential-vending catalog:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "cos_schema",
  "comment": "A schema in the Tencent Cloud COS credential-vending catalog",
  "properties": {
    "location": "cosn://my-bucket-1250000000/root/schema"
  }
}' http://localhost:8090/api/metalakes/metalake/catalogs/cos_catalog_with_vending/schemas

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
-H "Content-Type: application/json" -d '{
  "name": "example_fileset",
  "comment": "This is an example fileset",
  "type": "MANAGED",
  "storageLocation": "cosn://my-bucket-1250000000/root/schema/example_fileset",
  "properties": {}
}' http://localhost:8090/api/metalakes/metalake/catalogs/cos_catalog_with_vending/schemas/cos_schema/filesets
```

### Access without local credentials

Enable vending on the client and drop the credential properties.

```java
Configuration conf = new Configuration();
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "metalake");
// No need to set cos-access-key-id or cos-secret-access-key

Path filesetPath = new Path(
    "gvfs://fileset/cos_catalog_with_vending/cos_schema/example_fileset/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

```python
spark = (SparkSession.builder
    .appName("cos_fileset")
    .config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "metalake")
    # No need to set cos-access-key-id or cos-secret-access-key
    .getOrCreate())
```

The GVFS Python client cannot access COS-backed filesets; see
[GVFS Python client and pandas](#gvfs-python-client-and-pandas).
