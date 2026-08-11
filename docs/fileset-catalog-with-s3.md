---
title: "Fileset Catalog with Amazon S3"
slug: "/fileset-catalog-with-s3"
keyword: "Fileset catalog S3"
license: "This software is licensed under the Apache License version 2."
---

## Overview

A fileset catalog backed by Amazon S3 stores fileset data in S3 while Gravitino manages the metadata. Clients reach the data through the Gravitino Virtual File System (GVFS) using a `gvfs://` path, so the storage backend stays behind the catalog.

Everything here is specific to S3. For the fileset model, the shared catalog, schema, and fileset properties, and property inheritance, see [Fileset Catalog](./fileset-catalog.md).

## Quick Start

**1. Install the bundle.** Download [`gravitino-aws-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle), place it in `${GRAVITINO_HOME}/catalogs/fileset/libs/`, and start the server.

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
          "location": "s3a://{bucket}/{prefix}",
          "s3-endpoint": "{s3_endpoint}",
          "s3-access-key-id": "{s3_access_key_id}",
          "s3-secret-access-key": "{s3_secret_access_key}"
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

| Property Name          | Description                | Required |
|------------------------|----------------------------|----------|
| `s3-endpoint`          | Endpoint of the S3 service | Yes      |
| `s3-access-key-id`     | Access key                 | Yes      |
| `s3-secret-access-key` | Secret key                 | Yes      |

S3-compatible storage such as MinIO uses the same properties with its own endpoint.

## Accessing the Fileset

Every client needs `gravitino-filesystem-hadoop3-runtime` on its classpath, plus either `gravitino-aws-bundle` or the matching Hadoop dependencies for the environment it runs in. See [How to Use GVFS](./how-to-use-gvfs.md) for the base GVFS configuration these examples build on.

### Java

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "{metalake}");
conf.set("s3-endpoint", "{s3_endpoint}");
conf.set("s3-access-key-id", "{s3_access_key_id}");
conf.set("s3-secret-access-key", "{s3_secret_access_key}");

Path filesetPath = new Path("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

In a Maven build, depend on `gravitino-filesystem-hadoop3-runtime` plus `gravitino-aws-bundle`. In an environment that already has Hadoop, depend on `hadoop-common` and the Hadoop connector for S3 instead of the bundle.

### Spark

```python
import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-filesystem-hadoop3-runtime.jar,/path/to/gravitino-aws-bundle.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("s3_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "{metalake}")
    .config("spark.hadoop.s3-endpoint", "{s3_endpoint}")
    .config("spark.hadoop.s3-access-key-id", "{s3_access_key_id}")
    .config("spark.hadoop.s3-secret-access-key", "{s3_secret_access_key}")
    .getOrCreate())

df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.write.mode("overwrite").option("header", "true").csv(
    "gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people")
```

Where Spark already provides Hadoop, use `hadoop-aws-{hadoop_version}.jar` and `aws-java-sdk-bundle-{sdk_version}.jar` in place of the bundle jar. Some Spark versions do not pick up filesystem implementations passed with `--jars`, in which case the jars go on the Spark classpath directly.

### Hadoop Command Line

Add the GVFS implementation classes, the server URI, the metalake, and the S3 properties above to `${HADOOP_HOME}/etc/hadoop/core-site.xml`, then place the same jars in the Hadoop classpath.

```shell
hadoop fs -ls gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
hadoop fs -put /path/to/local/file gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
```

### Python

The Python client uses underscores where the Java client and the catalog use hyphens.

| Configuration Item     | Description                                                                                        | Required |
|------------------------|----------------------------------------------------------------------------------------------------|----------|
| `s3_endpoint`          | Endpoint of the S3 service. Optional against AWS, required for S3-compatible storage such as MinIO | No       |
| `s3_access_key_id`     | Access key                                                                                         | Yes      |
| `s3_secret_access_key` | Secret key                                                                                         | Yes      |

```python
from gravitino import gvfs

options = {
    "auth_type": "simple",
    "s3_endpoint": "{s3_endpoint}",
    "s3_access_key_id": "{s3_access_key_id}",
    "s3_secret_access_key": "{s3_secret_access_key}",
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

With credential vending the catalog holds the S3 credentials and Gravitino issues short-lived credentials per request, so clients hold no cloud keys. Set `credential-providers` on the catalog to one of `s3-token`, `s3-secret-key`, and set `fs.gravitino.enableCredentialVending` to `true` on the client.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "s3a://{bucket}/{prefix}",
          "s3-endpoint": "{s3_endpoint}",
          "s3-access-key-id": "{s3_access_key_id}",
          "s3-secret-access-key": "{s3_secret_access_key}",
          "credential-providers": "s3-token",
          "s3-region": "{s3_region}",
          "s3-role-arn": "{s3_role_arn}"
        }
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs
```

The `s3-token` provider needs these additional catalog properties.

| Property Name | Description                                        |
|---------------|----------------------------------------------------|
| `s3-region`   | Region of the bucket, for example `ap-northeast-1` |
| `s3-role-arn` | ARN of the role that grants access to the data     |

On the client, enable vending and drop the credential properties entirely.

```java
conf.setBoolean("fs.gravitino.enableCredentialVending", true);
```

```python
.config("spark.hadoop.fs.gravitino.enableCredentialVending", "true")
```

See [Credential Vending](./security/credential-vending.md) for the full provider configuration.
