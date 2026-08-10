---
title: "Fileset Catalog"
slug: "/fileset-catalog"
keywords:
  - fileset
  - catalog
  - storage
  - s3
  - gcs
  - adls
  - oss
  - cos
license: "This software is licensed under the Apache License version 2."
---

## Overview

A fileset catalog manages filesets over a Hadoop Compatible File System. Gravitino owns the catalog rather than federating an external one, so no provider is needed when creating it, and the same catalog, schema, and fileset model works over HDFS, a local filesystem, or object storage.

What changes per storage system is small: a bundle jar on the classpath, the URI scheme in the location, and a few credential properties. Creating and managing the objects is covered in [Manage Fileset Metadata](./manage-fileset-metadata-using-gravitino.md), and reading and writing the files in [How to Use GVFS](./how-to-use-gvfs.md). Neither changes because the data sits in S3 rather than HDFS, which is the point of the indirection described in [Filesets](./filesets.md).

The catalog is built against Hadoop 3 but uses no Hadoop 3 features, so Hadoop 2.x should also work. Report any incompatibility as an [issue](https://github.com/apache/gravitino/issues).

## Quick Start

**1. Create the catalog.** No provider is needed. Give it a base `location` and, for object
storage, the credential properties for that backend.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "hdfs://{cluster}/{path}"
        }
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs
```

**2. Create a schema.** The directory is created under the catalog location unless
`disable-filesystem-ops` is set.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{"name": "{schema_name}", "comment": "", "properties": {}}' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs/{catalog_name}/schemas
```

**3. Create a fileset.**

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{fileset_name}",
        "type": "MANAGED",
        "comment": "",
        "properties": {}
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs/{catalog_name}/schemas/{schema_name}/filesets
```

**4. Read and write the files.** The fileset is addressable as
`gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}` from the Java client, the Python
client, Spark, and the Hadoop shell. See [How to Use GVFS](./how-to-use-gvfs.md).

Only step 1 changes per storage backend, and only in the `location` scheme and the credential
properties. Steps 2 through 4 are the same over HDFS, S3, GCS, ADLS, OSS, and COS.

## Catalog Properties

These apply in addition to the [common catalog properties](./gravitino-server-config.md#catalog-properties-configuration).

| Property Name                        | Description                                                                                                                            | Default Value |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `location`                           | Base storage location, named `unknown`. Always a directory or path prefix, never a single file                                         | (none)        |
| `location-`                          | Prefix for named locations, as `location-{name}={path}`                                                                                | (none)        |
| `credential-providers`               | Credential provider types, separated by commas                                                                                         | (none)        |
| `config.resources`                   | Configuration files to load, separated by commas, such as `hdfs-site.xml,core-site.xml`                                                | (none)        |
| `filesystem-conn-timeout-secs`       | Timeout when obtaining a filesystem client, in seconds                                                                                 | `6`           |
| `disable-filesystem-ops`             | Stops the server creating and removing directories when schemas and filesets are created and dropped                                   | `false`       |
| `fileset-cache-eviction-interval-ms` | Fileset cache eviction interval, where `-1` never evicts                                                                               | `3600000`     |
| `fileset-cache-max-size`             | Maximum filesets held in the cache, where `-1` is unlimited                                                                            | `200000`      |
| `fs.path.config.<n>`                 | A logical location entry set to a base URI such as `hdfs://cluster1/`. Keys sharing the prefix are forwarded to that filesystem client | (none)        |

`default-filesystem-provider` and `filesystem-providers` are deprecated and no longer needed. The catalog loads filesystem providers from the classpath, including cloud providers whenever the matching bundle jar is present.

## Storage Backends

HDFS and local filesystems need no bundle jar and no credential properties. Object storage needs a jar in `${GRAVITINO_HOME}/catalogs/fileset/libs/` and a server restart, plus the properties below.

| Storage System          | Bundle Jar                 | URI Scheme           | Credential Providers              |
|-------------------------|----------------------------|----------------------|-----------------------------------|
| Amazon S3               | `gravitino-aws-bundle`     | `s3a://`             | `s3-token`, `s3-secret-key`       |
| Google Cloud Storage    | `gravitino-gcp-bundle`     | `gs://`              | `gcs-token`                       |
| Azure Data Lake Storage | `gravitino-azure-bundle`   | `abfss://`           | `adls-token`, `azure-account-key` |
| Alibaba Cloud OSS       | `gravitino-aliyun-bundle`  | `oss://`             | `oss-token`, `oss-secret-key`     |
| Tencent Cloud COS       | `gravitino-tencent-bundle` | `cosn://`            | `cos-secret-key`                  |
| HDFS and local          | None, built in             | `hdfs://`, `file://` | None                              |

Bundle jars are published on [Maven Central](https://mvnrepository.com/artifact/org.apache.gravitino) and versioned with the server.

The GVFS client takes the same property names as the catalog, so a client reading an S3 fileset sets `s3-endpoint`, `s3-access-key-id`, and `s3-secret-access-key` alongside its base GVFS configuration. Setting `credential-providers` on the catalog and `fs.gravitino.enableCredentialVending=true` on the client removes that requirement, since Gravitino then issues short-lived credentials per request and the client holds no cloud keys at all. See [Credential Vending](./security/credential-vending.md).

### Amazon S3

| Property Name          | Description                | Required |
|------------------------|----------------------------|----------|
| `s3-endpoint`          | Endpoint of the S3 service | Yes      |
| `s3-access-key-id`     | Access key                 | Yes      |
| `s3-secret-access-key` | Secret key                 | Yes      |

S3-compatible storage such as MinIO uses the same properties with its own endpoint.

```shell
curl -X POST -H "Content-Type: application/json" \
  -d '{
        "name": "{catalog_name}",
        "type": "FILESET",
        "comment": "",
        "properties": {
          "location": "s3a://{bucket}/{prefix}",
          "s3-endpoint": "{endpoint}",
          "s3-access-key-id": "{access_key_id}",
          "s3-secret-access-key": "{secret_access_key}"
        }
      }' \
  http://localhost:8090/api/metalakes/{metalake}/catalogs
```

### Google Cloud Storage

| Property Name              | Description                           | Required |
|----------------------------|---------------------------------------|----------|
| `gcs-service-account-file` | Path to the service account JSON file | Yes      |

The path is read wherever it is configured, so the file must exist on the server for the catalog, and on the client machine for a client not using vended credentials.

### Azure Data Lake Storage

| Property Name                | Description          | Required |
|------------------------------|----------------------|----------|
| `azure-storage-account-name` | Storage account name | Yes      |
| `azure-storage-account-key`  | Storage account key  | Yes      |

### Alibaba Cloud OSS

| Property Name           | Description                 | Required |
|-------------------------|-----------------------------|----------|
| `oss-endpoint`          | Endpoint of the OSS service | Yes      |
| `oss-access-key-id`     | Access key                  | Yes      |
| `oss-secret-access-key` | Secret key                  | Yes      |

### Tencent Cloud COS

| Property Name           | Description                                         | Required |
|-------------------------|-----------------------------------------------------|----------|
| `cos-region`            | Bucket region, for example `ap-guangzhou`           | Yes      |
| `cos-access-key-id`     | Access key, the Tencent Cloud `SecretId`            | Yes      |
| `cos-secret-access-key` | Secret key, the Tencent Cloud `SecretKey`           | Yes      |
| `cos-endpoint`          | Endpoint host suffix, only for non-public endpoints | No       |

`cos-endpoint` is a host suffix rather than a URL, so it takes `cos.ap-guangzhou.myqcloud.com` and not `https://cos.ap-guangzhou.myqcloud.com`. When unset it is derived from `cos-region`, which is what you want unless you are pointing at an internal or VPC endpoint.

### Multiple Storage Systems

One catalog can carry the properties for several storage systems at once, and Gravitino selects among them by the URI scheme of the object being accessed.

## Accessing a Fileset

A fileset is addressable as `gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}` from the Java client, the Python client, Spark, and the Hadoop shell. The address does not change with the storage backend, but the client needs the bundle jar for that backend and its credential properties, exactly as the catalog does.

Every client needs `gravitino-filesystem-hadoop3-runtime-{version}.jar` plus the backend bundle jar on its classpath, and these properties.

| Property Name                     | Value                                                               |
|-----------------------------------|---------------------------------------------------------------------|
| `fs.AbstractFileSystem.gvfs.impl` | `org.apache.gravitino.filesystem.hadoop.Gvfs`                       |
| `fs.gvfs.impl`                    | `org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem` |
| `fs.gravitino.server.uri`         | Gravitino server address                                            |
| `fs.gravitino.client.metalake`    | Metalake name                                                       |

Add the credential properties for the backend on top of those, using the same names the catalog uses.

| Storage System          | Client Credential Properties                                 |
|-------------------------|--------------------------------------------------------------|
| Amazon S3               | `s3-endpoint`, `s3-access-key-id`, `s3-secret-access-key`    |
| Google Cloud Storage    | `gcs-service-account-file`                                   |
| Azure Data Lake Storage | `azure-storage-account-name`, `azure-storage-account-key`    |
| Alibaba Cloud OSS       | `oss-endpoint`, `oss-access-key-id`, `oss-secret-access-key` |
| Tencent Cloud COS       | `cos-region`, `cos-access-key-id`, `cos-secret-access-key`   |
| HDFS and local          | None                                                         |

When the catalog sets `credential-providers`, replace the credential properties with `fs.gravitino.enableCredentialVending=true`. Gravitino then issues short-lived credentials per request and the client holds no cloud keys at all.

```shell
hadoop fs -ls gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
```

The client APIs, including the Java and Python examples and the Spark session configuration, are covered in [How to Use GVFS](./how-to-use-gvfs.md).

## Accessing a Fileset

A fileset is addressable as `gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}` from the Java client, the Python client, Spark, and the Hadoop shell. The address does not change with the storage backend, but the client needs the bundle jar for that backend and its credential properties, exactly as the catalog does.

Every client needs `gravitino-filesystem-hadoop3-runtime-{version}.jar` plus the backend bundle jar on its classpath, and these properties.

| Property Name                     | Value                                                               |
|-----------------------------------|---------------------------------------------------------------------|
| `fs.AbstractFileSystem.gvfs.impl` | `org.apache.gravitino.filesystem.hadoop.Gvfs`                       |
| `fs.gvfs.impl`                    | `org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem` |
| `fs.gravitino.server.uri`         | Gravitino server address                                            |
| `fs.gravitino.client.metalake`    | Metalake name                                                       |

Add the credential properties for the backend on top of those, using the same names the catalog uses.

| Storage System          | Client Credential Properties                                 |
|-------------------------|--------------------------------------------------------------|
| Amazon S3               | `s3-endpoint`, `s3-access-key-id`, `s3-secret-access-key`    |
| Google Cloud Storage    | `gcs-service-account-file`                                   |
| Azure Data Lake Storage | `azure-storage-account-name`, `azure-storage-account-key`    |
| Alibaba Cloud OSS       | `oss-endpoint`, `oss-access-key-id`, `oss-secret-access-key` |
| Tencent Cloud COS       | `cos-region`, `cos-access-key-id`, `cos-secret-access-key`   |
| HDFS and local          | None                                                         |

When the catalog sets `credential-providers`, replace the credential properties with `fs.gravitino.enableCredentialVending=true`. Gravitino then issues short-lived credentials per request and the client holds no cloud keys at all.

```shell
hadoop fs -ls gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}
```

The client APIs, including the Java and Python examples and the Spark session configuration, are covered in [How to Use GVFS](./how-to-use-gvfs.md).

## HDFS and Kerberos

A secured HDFS cluster needs these on the catalog, and they can be narrowed on a schema or fileset.

| Property Name                                      | Description                                              | Default Value |
|----------------------------------------------------|----------------------------------------------------------|---------------|
| `authentication.type`                              | `simple` or `kerberos`                                   | `simple`      |
| `authentication.impersonation-enable`              | Whether the catalog impersonates the calling user        | `false`       |
| `authentication.kerberos.principal`                | Kerberos principal, required when the type is `kerberos` | (none)        |
| `authentication.kerberos.keytab-uri`               | URI of the keytab, required when the type is `kerberos`  | (none)        |
| `authentication.kerberos.check-interval-sec`       | Credential check interval                                | `60`          |
| `authentication.kerberos.keytab-fetch-timeout-sec` | Timeout when retrieving the keytab                       | `60`          |

The HDFS client itself is configured through the files named in `config.resources`, where Gravitino recognizes three additional keys: `hadoop.security.authentication.kerberos.principal`, `hadoop.security.authentication.kerberos.keytab`, and `hadoop.security.authentication.kerberos.krb5.conf`.

## Schema Properties

Schemas inherit every catalog property and can override these.

| Property Name                         | Description                                           | Default Value |
|---------------------------------------|-------------------------------------------------------|---------------|
| `location`                            | Base storage location for the schema, named `unknown` | (none)        |
| `location-`                           | Prefix for named locations                            | (none)        |
| `credential-providers`                | Credential provider types, separated by commas        | (none)        |
| `config.resources`                    | Configuration files to load                           | (none)        |
| `authentication.type`                 | `simple` or `kerberos`                                | Catalog value |
| `authentication.impersonation-enable` | Whether to impersonate the calling user               | Catalog value |
| `authentication.kerberos.principal`   | Kerberos principal for this schema                    | Catalog value |
| `authentication.kerberos.keytab-uri`  | Keytab URI for this schema                            | Catalog value |

Creating or dropping a schema creates or removes the matching directories, except when `disable-filesystem-ops` is `true` or the location contains [placeholders](./manage-fileset-metadata-using-gravitino.md#placeholder).

## Fileset Properties

Filesets inherit every schema property, including those the schema inherited from the catalog.

| Property Name                         | Description                                                                                        | Default Value                        | Immutable |
|---------------------------------------|----------------------------------------------------------------------------------------------------|--------------------------------------|-----------|
| `location`                            | Storage location for the fileset, named `unknown`                                                  | (none)                               | No        |
| `default-location-name`               | Which location GVFS uses when none is named. Required when the fileset has several named locations | The only location, when there is one | Yes       |
| `placeholder-`                        | Values substituted into placeholders in the location                                               | (none)                               | Yes       |
| `credential-providers`                | Credential provider types, separated by commas                                                     | (none)                               | No        |
| `config.resources`                    | Configuration files to load                                                                        | (none)                               | No        |
| `authentication.type`                 | `simple` or `kerberos`                                                                             | Schema value                         | No        |
| `authentication.impersonation-enable` | Whether to impersonate the calling user                                                            | Schema value                         | Yes       |
| `authentication.kerberos.principal`   | Kerberos principal for this fileset                                                                | Schema value                         | No        |
| `authentication.kerberos.keytab-uri`  | Keytab URI for this fileset                                                                        | Schema value                         | No        |

Three placeholders are supplied by Gravitino and cannot be set: `placeholder-catalog`, `placeholder-schema`, and `placeholder-fileset`, which resolve to the names of the objects themselves.

## Property Inheritance

Authentication and credential settings resolve from the nearest level outward, so a fileset value beats a schema value, which beats a catalog value. A catalog can therefore set a default that individual filesets override without repeating the rest of the configuration.

## Implementing a Custom Filesystem Provider

A storage system Gravitino does not ship support for can be added by implementing `FileSystemProvider` from [gravitino-hadoop-common](https://repo1.maven.org/maven2/org/apache/gravitino/gravitino-hadoop-common/):

```java
// Build a FileSystem from the properties set when the catalog was created.
FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
    throws IOException;

// URI scheme, such as 'file', 'hdfs', 's3a', 'gs', 'oss', or 'cosn'.
String scheme();

// Provider name, such as 'builtin-local', 'builtin-hdfs', 's3', 'gcs', 'oss', or 'cos'.
String name();
```

The provider is discovered through Java SPI, so the jar needs a `META-INF/services/org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider` file naming the implementing class. Place the jar in `${GRAVITINO_HOME}/catalogs/fileset/libs/` and restart the server.

## Further Reading

- [Filesets](./filesets.md) for the fileset model itself
- [How to Use GVFS](./how-to-use-gvfs.md) for reading and writing fileset data
- [Manage Fileset Metadata](./manage-fileset-metadata-using-gravitino.md) for the API
- [Credential Vending](./security/credential-vending.md) for issuing short-lived storage credentials
