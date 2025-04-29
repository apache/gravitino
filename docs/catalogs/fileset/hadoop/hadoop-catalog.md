---
title: "Hadoop catalog"
slug: /hadoop-catalog
date: 2024-4-2
keyword: hadoop catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Hadoop catalog is a fileset catalog that using Hadoop Compatible File System (HCFS)
to manage the storage location of the fileset.
Currently, it supports the local filesystem and HDFS.
Starting from *0.7.0-incubating*, Gravitino Hadoop catalog supports
[S3](./s3.md), [GCS](./gcs.md), [OSS](./oss.md) and [Azure Blob Storage](./adls.md).

The rest of this document will use HDFS or local file as an example
to illustrate how to use the Hadoop catalog.
For S3, GCS, OSS and Azure Blob Storage, the configurations are similar to HDFS,
please refer to the corresponding document for more details.

Note that Gravitino uses Hadoop 3 dependencies to build Hadoop catalog.
Theoretically, it should be compatible with both Hadoop 2.x and 3.x,
since Gravitino doesn't leverage any new features in Hadoop 3.
If there's any compatibility issue, please file an [issue](https://github.com/apache/gravitino/issues).

## Catalog

### Catalog properties

Besides the [common catalog properties](../../../admin/server-config.md#apache-gravitino-catalog-properties-configuration),
the Hadoop catalog has the following properties:

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>location</tt></td>
  <td>The storage location managed by Hadoop catalog.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>default-filesystem-provider</tt></td>
  <td>
    The default filesystem provider of this Hadoop catalog
    if users do not specify the scheme in the URI.
    Valid values are `builtin-local`, `builtin-hdfs`, `s3`, `gcs`, `abs` and `oss`.
    The default value is `builtin-local`.
    For S3, if we set this value to 's3', we can omit the prefix 's3a://' in the <tt>location</tt>.
  </td>
  <td>`builtin-local`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>filesystem-providers</tt></td>
  <td>
    The file system providers to add.
    Users need to set this configuration to support cloud storage or custom HCFS.
    For instance, set it to `s3` or a comma separated string that contains `s3`
    like `gs,s3` to support multiple kinds of fileset including `s3`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td>The credential provider types, separated by comma.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>filesystem-conn-timeout-secs</tt></td>
  <td>
    The timeout of getting the file system using Hadoop FileSystem client instance.
    Time unit: seconds.
  </td>
  <td>`6`</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>disable-filesystem-ops</tt></td>
  <td>
    The configuration to disable file system operations at the server side.
    If set to true, the Hadoop catalog at the server side will not create, drop files or folder
    when the schema, fileset is created, dropped.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>fileset-cache-eviction-interval-ms</tt></td>
  <td>The interval in milliseconds to evict the fileset cache, -1 means never evict.</td>
  <td>`3600000`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>fileset-cache-max-size</tt></td>
  <td>The maximum number of filesets the cache may contain, -1 means no limit.</td>
  <td>`200000`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

Please refer to [Credential vending](../../../security/credential-vending.md)
for more details about credential vending.

### HDFS fileset 

Apart from the above properties, to access fileset like HDFS fileset,
you need to configure the following extra properties.

<table>
<thead>
<tr>
  <th>Property Name</th>
  <th>Description</th>
  <th>Default Value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>authentication.impersonation-enable</tt></td>
  <td>Whether to enable impersonation for the Hadoop catalog.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>authentication.type</tt></td>
  <td>
    The type of authentication for Hadoop catalog.
    Currently we only support `kerberos`, `simple`.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.principal</tt></td>
  <td>
    The principal of the Kerberos authentication.

    This is required if the value of `authentication.type` is Kerberos.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-uri</tt></td>
  <td>
    The URI of The keytab for the Kerberos authentication.

    This is required if the value of `authentication.type` is Kerberos.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.check-interval-sec</tt></td>
  <td>
    The check interval of Kerberos credential for Hadoop catalog,
    in seconds.
  </td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-fetch-timeout-sec</tt></td>
  <td>
    The fetch timeout of retrieving Kerberos keytab from
    `authentication.kerberos.keytab-uri`, in seconds.
  </td>
  <td>`60`</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
</tbody>
</table>

### Hadoop catalog with Cloud Storage

- For S3, refer to [Hadoop-catalog-with-s3](./s3.md) for more details.
- For GCS, refer to [Hadoop-catalog-with-gcs](./gcs.md) for more details.
- For OSS, refer to [Hadoop-catalog-with-oss](./oss.md) for more details.
- For Azure Blob Storage, refer to [Hadoop-catalog-with-adls](./adls.md) for more details.

### How to custom your own HCFS file system fileset?

Developers and users can custom their own HCFS file system fileset
by implementing the `FileSystemProvider` interface
in the [Gravitino hadoop catalog](https://repo1.maven.org/maven2/org/apache/gravitino/catalog-hadoop/).
The `FileSystemProvider` interface is defined as follows:

```java
// Create a FileSystem instance using the properties you set when creating the catalog. 
FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
    throws IOException;

// The schema name of the file system provider. Valid values are:
// - 'file' for local file system,
// - 'hdfs' for HDFS
// - 's3' for AWS S3
// - 'gs' for GCS
// - 'oss' for Aliyun OSS. 
String scheme();

// Name of the file system provider. Valid values are:
// - 'builtin-local' for local file system
// - 'builtin-hdfs' for HDFS, 
// - 's3' for AWS S3
// - 'gcs' for GCS
// - 'oss' for Aliyun OSS.
// You need to set catalog properties `filesystem-providers` to support this file system.
String name();
```

`FileSystemProvider` uses Java SPI to load the custom file system provider.
You need to create a file named `org.apache.gravitino.catalog.fs.FileSystemProvider`
in the `META-INF/services` directory of the JAR file.
The content of the file is the full class name of the custom file system provider. 
For example, the content of `S3FileSystemProvider` is as follows:

![img.png](../../../assets/fileset/custom-filesystem-provider.png)

After implementing the `FileSystemProvider` interface, you need to put the JAR file
into the `$GRAVITINO_HOME/catalogs/hadoop/libs` directory.
You can then set the `filesystem-providers` property to use your custom file system provider.

### Authentication for Hadoop Catalog

The Hadoop catalog supports multi-level authentication to control access,
allowing different authentication settings for the catalog, schema, and fileset.
The priority of authentication settings is as follows:

catalog < schema < fileset. Specifically:

- **Catalog**: The default authentication is `simple`.

- **Schema**: Inherits the authentication setting from the catalog if not explicitly set.
  For more information about schema settings, refer to [Schema properties](#schema-properties).

- **Fileset**: Inherits the authentication setting from the schema if not explicitly set.
  For more information about fileset settings, refer to [Fileset properties](#fileset-properties).

The default value of `authentication.impersonation-enable` is false.
The default value for catalogs about this configuration is false.
For schemas and filesets, the default value is inherited from their parent.
Value set by the user will override the parent value.
The priority mechanism is the same as authentication.

### Catalog operations

Refer to [catalog operations](../../../metadata/fileset.md#catalog-operations)
for more details.

## Schema

### Schema capabilities

The Hadoop catalog supports creating, updating, deleting, and listing schema.

### Schema properties

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>location</tt></td>
  <td>The storage location managed by Hadoop schema.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.5.0`</td>
</tr>
<tr>
  <td><tt>authentication.impersonation-enable</tt></td>
  <td>
    Whether to enable impersonation for this schema of the Hadoop catalog.
  </td>
  <td>The parent (catalog) value</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.type</tt></td>
  <td>
    The type of authentication for this schema of Hadoop catalog.
    Currently we only support `kerberos` and `simple`.
  </td>
  <td>The parent(catalog) value</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.principal</tt></td>
  <td>The principal of the Kerberos authentication for this schema.</td>
  <td>The parent(catalog) value</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-uri</tt></td>
  <td>The URI of The keytab for the Kerberos authentication for this schema.</td>
  <td>The parent(catalog) value</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td>The credential provider types, separated by comma.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### Schema operations

Refer to [Schema operation](../../../metadata/fileset.md#schema-operations)
for more details.

## Fileset

### Fileset capabilities

- The Hadoop catalog supports creating, updating, deleting, and listing filesets.

### Fileset properties

<table>
<thead>
<tr>
  <th>Property name</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Immutable</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>authentication.impersonation-enable</tt></td>
  <td>Whether to enable impersonation for the Hadoop catalog fileset.</td>
  <td>The parent(schema) value</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.type</tt></td>
  <td>
    The type of authentication for Hadoop catalog fileset.
    Currently we only support `kerberos` and `simple`.
  </td>
  <td>The parent(schema) value</td>
  <td>No</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.principal</tt></td>
  <td>The principal of the Kerberos authentication for the fileset.</td>
  <td>The parent(schema) value</td>
  <td>No</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>authentication.kerberos.keytab-uri</tt></td>
  <td>The URI of The keytab for the Kerberos authentication for the fileset.</td>
  <td>The parent(schema) value</td>
  <td>No</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td>The credential provider types, separated by comma.</td>
  <td>(none)</td>
  <td>No</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>placehoder-&#42;</tt></td>
  <td>
    Properties that start with `placeholder-` are used to replace placehoders
    in the <tt>location</tt>.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>Yes</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

Some properties are reserved and cannot be set by users:

<table>
<thead>
<tr>
  <td>Property name</td>
  <td>Description</td>
  <td>Default value</td>
  <td>Since version</td>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>placeholder-catalog</tt></td>
  <td>The paceholder for the catalog name.</td>
  <td>Catalog name for the fileset</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>placehoder-schema</tt></td>
  <td>The placeholder for the schema name.</td>
  <td>Schema name for the fileset</td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>placehoder-fileset</tt></td>
  <td>The placeholder for the fileset name.</td>
  <td>Fileset name</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

Credential providers can be specified in several places, as listed below.
Gravitino checks the `credential-providers` setting in the following order:

1. Fileset properties
1. Schema properties
1. Catalog properties

### Fileset operations

Refer to [Fileset operations](../../../metadata/fileset.md#fileset-operations)
for more details.

