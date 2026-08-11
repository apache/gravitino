---
title: "Fileset Catalog with Google Cloud Storage"
slug: "/fileset-catalog-with-gcs"
keyword: "Fileset catalog GCS"
license: "This software is licensed under the Apache License version 2."
---

## Overview

A fileset catalog backed by Google Cloud Storage stores fileset data in GCS while Gravitino manages the metadata. Clients reach the data through the Gravitino Virtual File System (GVFS) using a `gvfs://` path, so the storage backend stays behind the catalog.

Everything here is specific to GCS. For the fileset model, the shared catalog, schema, and fileset properties, and property inheritance, see [Fileset Catalog](./fileset-catalog.md).

## Quick Start

**1. Install the bundle.** Download [`gravitino-gcp-bundle`](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle), place it in `${GRAVITINO_HOME}/catalogs/fileset/libs/`, and start the server.

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
          "location": "gs://{bucket}/{prefix}",
          "gcs-service-account-file": "{gcs_service_account_file}"
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

| Property Name              | Description                           | Required |
|----------------------------|---------------------------------------|----------|
| `gcs-service-account-file` | Path to the service account JSON file | Yes      |

The service account file is read wherever it is configured, so it must exist on the Gravitino server for the catalog, and on the client machine for any client not using vended credentials.

## Accessing the Fileset

Every client needs `gravitino-filesystem-hadoop3-runtime` on its classpath, plus either `gravitino-gcp-bundle` or the matching Hadoop dependencies for the environment it runs in. See [How to Use GVFS](./how-to-use-gvfs.md) for the base GVFS configuration these examples build on.

### Java

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
conf.set("fs.gravitino.server.uri", "http://localhost:8090");
conf.set("fs.gravitino.client.metalake", "{metalake}");
conf.set("gcs-service-account-file", "{gcs_service_account_file}");

Path filesetPath = new Path("gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/new_dir");
FileSystem fs = filesetPath.getFileSystem(conf);
fs.mkdirs(filesetPath);
```

In a Maven build, depend on `gravitino-filesystem-hadoop3-runtime` plus `gravitino-gcp-bundle`. In an environment that already has Hadoop, depend on `hadoop-common` and the Hadoop connector for GCS instead of the bundle.

### Spark

```python
import os
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /path/to/gravitino-filesystem-hadoop3-runtime.jar,/path/to/gravitino-gcp-bundle.jar "
    "--master local[1] pyspark-shell"
)

spark = (SparkSession.builder
    .appName("gcs_fileset")
    .config("spark.hadoop.fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs")
    .config("spark.hadoop.fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem")
    .config("spark.hadoop.fs.gravitino.server.uri", "http://localhost:8090")
    .config("spark.hadoop.fs.gravitino.client.metalake", "{metalake}")
    .config("spark.hadoop.gcs-service-account-file", "{gcs_service_account_file}")
    .getOrCreate())

df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.write.mode("overwrite").option("header", "true").csv(
    "gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}/people")
```

Where Spark already provides Hadoop, use `gcs-connector-hadoop3-{connector_version}-shaded.jar` in place of the bundle jar. Some Spark versions do not pick up filesystem implementations passed with `--jars`, in which case the jars go on the Spark classpath directly.

### Hadoop Command Line

Add the GVFS implementation classes, the server URI, the metalake, and the GCS properties above to `${HADOOP_HOME}/etc/hadoop/core-site.xml`, then place the same jars in the Hadoop classpath.

```shell
hadoop fs -ls gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
hadoop fs -put /path/to/local/file gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
```

### Python

The Python client uses underscores where the Java client and the catalog use hyphens.

| Configuration Item         | Description                           | Required |
|----------------------------|---------------------------------------|----------|
| `gcs_service_account_file` | Path to the service account JSON file | Yes      |

```python
from gravitino import gvfs

options = {
    "auth_type": "simple",
    "gcs_service_account_file": "{gcs_service_account_file}",
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

With credential vending the catalog holds the GCS credentials and Gravitino issues short-lived credentials per request, so clients hold no cloud keys. Set `credential-providers` on the catalog to one of `gcs-token`, and set `fs.gravitino.enableCredentialVending` to `true` on the client.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "gs://{bucket}/{prefix}",
          "gcs-service-account-file": "{gcs_service_account_file}",
          "credential-providers": "gcs-token"
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
