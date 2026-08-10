---
title: "Fileset Catalog with Tencent Cloud COS"
slug: "/fileset-catalog-with-cos"
keyword: "Fileset catalog COS"
license: "This software is licensed under the Apache License version 2."
---

## Overview

A fileset catalog backed by Tencent Cloud COS stores fileset data in COS while Gravitino manages the metadata. Clients reach the data through the Gravitino Virtual File System (GVFS) using a `gvfs://` path, so the storage backend stays behind the catalog.

Everything here is specific to COS. For the fileset model, the shared catalog, schema, and fileset properties, and property inheritance, see [Fileset Catalog](./fileset-catalog.md).

## Quick Start

**1. Install the bundle.** Download [`gravitino-tencent-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-tencent-bundle), place it in `${GRAVITINO_HOME}/catalogs/fileset/libs/`, and start the server.

```shell
${GRAVITINO_HOME}/bin/gravitino-server.sh start
```

**2. Create the catalog.**

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "cosn://{bucket}/{prefix}",
          "cos-region": "{cos_region}",
          "cos-access-key-id": "{cos_access_key_id}",
          "cos-secret-access-key": "{cos_secret_access_key}",
          "cos-endpoint": "{cos_endpoint}",
        }
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs
```

**3. Create a schema.**

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "{schema_name}", "comment": "", "properties": {}}' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs/{catalog_name}/schemas
```

**4. Create a fileset.**

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "{fileset_name}", "type": "MANAGED", "comment": "", "properties": {}}' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}/filesets
```

**5. Read the fileset.** The fileset is now addressable as `gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}` from any GVFS client.

## Catalog Properties

These properties are needed in addition to the shared [catalog properties](./fileset-catalog.md#catalog-properties).

| Property Name           | Description                                         | Required |
|-------------------------|-----------------------------------------------------|----------|
| `cos-region`            | Bucket region, for example `ap-guangzhou`           | Yes      |
| `cos-access-key-id`     | Access key, the Tencent Cloud `SecretId`            | Yes      |
| `cos-secret-access-key` | Secret key, the Tencent Cloud `SecretKey`           | Yes      |
| `cos-endpoint`          | Endpoint host suffix, only for non-public endpoints | No       |

`cos-endpoint` takes a host suffix rather than a URL, so it is set to `cos.ap-guangzhou.myqcloud.com` and not `https://cos.ap-guangzhou.myqcloud.com`. When it is unset the endpoint is derived from `cos-region`, which is what you want unless you are pointing at an internal or VPC endpoint.

## Accessing the Fileset

Every client needs `gravitino-filesystem-hadoop3-runtime` on its classpath, plus either `gravitino-tencent-bundle` or the matching Hadoop dependencies for the environment it runs in. See [How to Use GVFS](./how-to-use-gvfs.md) for the base GVFS configuration these examples build on.

### Java

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "{metalake}");
conf.set("cos-region", "{cos_region}");
conf.set("cos-access-key-id", "{cos_access_key_id}");
conf.set("cos-secret-access-key", "{cos_secret_access_key}");
conf.set("cos-endpoint", "{cos_endpoint}");

Path filesetPath = new Path("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

In a Maven build, depend on `gravitino-filesystem-hadoop3-runtime` plus `gravitino-tencent-bundle`. In an environment that already has Hadoop, depend on `hadoop-common` and the Hadoop connector for COS instead of the bundle.

### Spark

```python
import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-filesystem-hadoop3-runtime.jar,/path/to/gravitino-tencent-bundle.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("cos_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "{metalake}")
    .config("spark.hadoop.cos-region", "{cos_region}")
    .config("spark.hadoop.cos-access-key-id", "{cos_access_key_id}")
    .config("spark.hadoop.cos-secret-access-key", "{cos_secret_access_key}")
    .config("spark.hadoop.cos-endpoint", "{cos_endpoint}")
    .getOrCreate())

df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.write.mode("overwrite").option("header", "true").csv(
    "gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people")
```

Where Spark already provides Hadoop, use `hadoop-cos-{hadoop_version}.jar` and `cos_api-bundle-{sdk_version}.jar` in place of the bundle jar. Some Spark versions do not pick up filesystem implementations passed with `--jars`, in which case the jars go on the Spark classpath directly.

### Hadoop Command Line

Add the GVFS implementation classes, the server URI, the metalake, and the COS properties above to `${HADOOP_HOME}/etc/hadoop/core-site.xml`, then place the same jars in the Hadoop classpath.

```shell
hadoop fs -ls gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
hadoop fs -put /path/to/local/file gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
```

### Python

The Python client uses underscores where the Java client and the catalog use hyphens.

| Configuration Item      | Description   | Required |
|-------------------------|---------------|----------|
| `cos_region`            | Bucket region | Yes      |
| `cos_access_key_id`     | Access key    | Yes      |
| `cos_secret_access_key` | Secret key    | Yes      |

```python
from gravitino import gvfs

options = {
    "auth_type": "simple",
    "cos_region": "{cos_region}",
    "cos_access_key_id": "{cos_access_key_id}",
    "cos_secret_access_key": "{cos_secret_access_key}",
}
fs = gvfs.GravitinoVirtualFileSystem(
    server_uri="http://localhost:8090",
    metalake_name="{metalake}",
    options=options,
)
fs.ls("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/")
```

pandas reads the same paths through the `storage_options` argument, passing `server_uri`, `metalake_name`, and the same `options` dictionary.

## Credential Vending

With credential vending the catalog holds the COS credentials and Gravitino issues short-lived credentials per request, so clients hold no cloud keys. Set `credential-providers` on the catalog to one of `cos-secret-key`, and set `fs.gravitino.enableCredentialVending` to `true` on the client.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "cosn://{bucket}/{prefix}",
          "cos-region": "{cos_region}",
          "cos-access-key-id": "{cos_access_key_id}",
          "cos-secret-access-key": "{cos_secret_access_key}",
          "cos-endpoint": "{cos_endpoint}",
          "credential-providers": "cos-secret-key",

        }
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs
```

On the client, enable vending and drop the credential properties entirely.

```java
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
```

```python
.config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
```

See [Credential Vending](./security/credential-vending.md) for the full provider configuration.
