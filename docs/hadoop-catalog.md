---
title: "Hadoop catalog"
slug: /hadoop-catalog
date: 2024-4-2
keyword: hadoop catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Hadoop catalog is a fileset catalog that using Hadoop Compatible File System (HCFS) to manage
the storage location of the fileset. Currently, it supports local filesystem and HDFS. For
object storage like S3, GCS, and Azure Blob Storage, you can put the hadoop object store jar like
hadoop-aws into the `$GRAVITINO_HOME/catalogs/hadoop/libs` directory to enable the support.
Gravitino itself hasn't yet tested the object storage support, so if you have any issue,
please create an [issue](https://github.com/apache/gravitino/issues).

Note that Gravitino uses Hadoop 3 dependencies to build Hadoop catalog. Theoretically, it should be
compatible with both Hadoop 2.x and 3.x, since Gravitino doesn't leverage any new features in
Hadoop 3. If there's any compatibility issue, please create an [issue](https://github.com/apache/gravitino/issues).

## Catalog

### Catalog properties

Besides the [common catalog properties](./gravitino-server-config.md#gravitino-catalog-properties-configuration), the Hadoop catalog has the following properties:

| Property Name | Description                                     | Default Value | Required | Since Version |
|---------------|-------------------------------------------------|---------------|----------|---------------|
| `location`    | The storage location managed by Hadoop catalog. | (none)        | No       | 0.5.0         |

Apart from the above properties, to access fileset like HDFS, S3, GCS, OSS or custom fileset, you need to configure the following extra properties.

#### HDFS fileset 

| Property Name                                      | Description                                                                                    | Default Value | Required                                                   | Since Version  |
|----------------------------------------------------|------------------------------------------------------------------------------------------------|---------------|------------------------------------------------------------|----------------|
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Hadoop catalog.                                        | `false`       | No                                                         | 0.5.1          |
| `authentication.type`                              | The type of authentication for Hadoop catalog, currently we only support `kerberos`, `simple`. | `simple`      | No                                                         | 0.5.1          |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                   | (none)        | required if the value of `authentication.type` is Kerberos.| 0.5.1          |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                         | (none)        | required if the value of `authentication.type` is Kerberos.| 0.5.1          |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Hadoop catalog.                                  | 60            | No                                                         | 0.5.1          |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.     | 60            | No                                                         | 0.5.1          |

#### S3 fileset

| Configuration item             | Description                                                                                                                                                                                                                 | Default value   | Required                  | Since version    |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------------------------|------------------|
| `filesystem-providers`         | The file system providers to add. Set it to `s3` if it's a S3 fileset, or a comma separated string that contains `s3` like `gs,s3` to support multiple kinds of fileset including `s3`.                                     | (none)          | Yes                       | 0.7.0-incubating |
| `default-filesystem-provider`  | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for S3, if we set this value, we can omit the prefix 's3a://' in the location.| `builtin-local` | No                        | 0.7.0-incubating |
| `s3-endpoint`                  | The endpoint of the AWS S3.                                                                                                                                                                                                 | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |
| `s3-access-key-id`             | The access key of the AWS S3.                                                                                                                                                                                               | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |
| `s3-secret-access-key`         | The secret key of the AWS S3.                                                                                                                                                                                               | (none)          | Yes if it's a S3 fileset. | 0.7.0-incubating |

At the same time, you need to place the corresponding bundle jar [`gravitino-aws-bundle-${version}.jar`](https://repo1.maven.org/maven2/org/apache/gravitino/aws-bundle/) in the directory `${GRAVITINO_HOME}/catalogs/hadoop/libs`.

#### GCS fileset

| Configuration item            | Description                                                                                                                                                                                                                 | Default value   | Required                  | Since version    |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------------------------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `gs` if it's a GCS fileset, a comma separated string that contains `gs` like `gs,s3` to support multiple kinds of fileset including `gs`.                                       | (none)          | Yes                       | 0.7.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for GCS, if we set this value, we can omit the prefix 'gs://' in the location.| `builtin-local` | No                        | 0.7.0-incubating |
| `gcs-service-account-file`    | The path of GCS service account JSON file.                                                                                                                                                                                  | (none)          | Yes if it's a GCS fileset.| 0.7.0-incubating |

In the meantime, you need to place the corresponding bundle jar [`gravitino-gcp-bundle-${version}.jar`](https://repo1.maven.org/maven2/org/apache/gravitino/gcp-bundle/) in the directory `${GRAVITINO_HOME}/catalogs/hadoop/libs`.

#### OSS fileset

| Configuration item            | Description                                                                                                                                                                                                                  | Default value   | Required                  | Since version    |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|---------------------------|------------------|
| `filesystem-providers`        | The file system providers to add. Set it to `oss` if it's a OSS fileset, or a comma separated string that contains `oss` like `oss,gs,s3` to support multiple kinds of fileset including `oss`.                              | (none)          | Yes                       | 0.7.0-incubating |
| `default-filesystem-provider` | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`, for OSS, if we set this value, we can omit the prefix 'oss://' in the location.| `builtin-local` | No                        | 0.7.0-incubating |
| `oss-endpoint`                | The endpoint of the Aliyun OSS.                                                                                                                                                                                              | (none)          | Yes if it's a OSS fileset.| 0.7.0-incubating |
| `oss-access-key-id`           | The access key of the Aliyun OSS.                                                                                                                                                                                            | (none)          | Yes if it's a OSS fileset.| 0.7.0-incubating |
| `oss-secret-access-key`       | The secret key of the Aliyun OSS.                                                                                                                                                                                            | (none)          | Yes if it's a OSS fileset.| 0.7.0-incubating |

In the meantime, you need to place the corresponding bundle jar [`gravitino-aliyun-bundle-${version}.jar`](https://repo1.maven.org/maven2/org/apache/gravitino/aliyun-bundle/) in the directory `${GRAVITINO_HOME}/catalogs/hadoop/libs`.

:::note
- Gravitino contains builtin file system providers for local file system(`builtin-local`) and HDFS(`builtin-hdfs`), that is to say if `filesystem-providers` is not set, Gravitino will still support local file system and HDFS. Apart from that, you can set the `filesystem-providerss` to support other file systems like S3, GCS, OSS or custom file system.
- `default-filesystem-provider` is used to set the default file system provider for the Hadoop catalog. If the user does not specify the scheme in the URI, Gravitino will use the default file system provider to access the fileset. For example, if the default file system provider is set to `builtin-local`, the user can omit the prefix `file://` in the location. 
:::

#### How to custom your own HCFS file system fileset?

Developers and users can custom their own HCFS file system fileset by implementing the `FileSystemProvider` interface in the jar [gravitino-catalog-hadoop](https://repo1.maven.org/maven2/org/apache/gravitino/catalog-hadoop/) . The `FileSystemProvider` interface is defined as follows:

```java
  
  // Create a FileSystem instance by the properties you have set when creating the catalog. 
  FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException;
  
  // The schema name of the file system provider. 'file' for Local file system,
  // 'hdfs' for HDFS, 's3a' for AWS S3, 'gs' for GCS, 'oss' for Aliyun OSS. 
  String scheme();

  // Name of the file system provider. 'builtin-local' for Local file system, 'builtin-hdfs' for HDFS, 
  // 's3' for AWS S3, 'gcs' for GCS, 'oss' for Aliyun OSS.
  
  // You need to set catalog properties `filesystem-providers` to support this file system.
  String name();
```

After implementing the `FileSystemProvider` interface, you need to put the jar file into the `$GRAVITINO_HOME/catalogs/hadoop/libs` directory. Then you can set the `filesystem-providers` property to use your custom file system provider.


### Authentication for Hadoop Catalog

The Hadoop catalog supports multi-level authentication to control access, allowing different authentication settings for the catalog, schema, and fileset. The priority of authentication settings is as follows: catalog < schema < fileset. Specifically:

- **Catalog**: The default authentication is `simple`.
- **Schema**: Inherits the authentication setting from the catalog if not explicitly set. For more information about schema settings, please refer to [Schema properties](#schema-properties).
- **Fileset**: Inherits the authentication setting from the schema if not explicitly set. For more information about fileset settings, please refer to [Fileset properties](#fileset-properties).

The default value of `authentication.impersonation-enable` is false, and the default value for catalogs about this configuration is false, for
schemas and filesets, the default value is inherited from the parent. Value set by the user will override the parent value, and the priority mechanism is the same as authentication.

### Catalog operations

Refer to [Catalog operations](./manage-fileset-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema capabilities

The Hadoop catalog supports creating, updating, deleting, and listing schema.

### Schema properties

| Property name                         | Description                                                                                                    | Default value             | Required | Since Version    |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------|---------------------------|----------|------------------|
| `location`                            | The storage location managed by Hadoop schema.                                                                 | (none)                    | No       | 0.5.0            |
| `authentication.impersonation-enable` | Whether to enable impersonation for this schema of the Hadoop catalog.                                         | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.type`                 | The type of authentication for this schema of Hadoop catalog , currently we only support `kerberos`, `simple`. | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.kerberos.principal`   | The principal of the Kerberos authentication for this schema.                                                  | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`  | The URI of The keytab for the Kerberos authentication for this scheam.                                         | The parent(catalog) value | No       | 0.6.0-incubating |

### Schema operations

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

## Fileset

### Fileset capabilities

- The Hadoop catalog supports creating, updating, deleting, and listing filesets.

### Fileset properties

| Property name                         | Description                                                                                            | Default value            | Required | Since Version    |
|---------------------------------------|--------------------------------------------------------------------------------------------------------|--------------------------|----------|------------------|
| `authentication.impersonation-enable` | Whether to enable impersonation for the Hadoop catalog fileset.                                        | The parent(schema) value | No       | 0.6.0-incubating |
| `authentication.type`                 | The type of authentication for Hadoop catalog fileset, currently we only support `kerberos`, `simple`. | The parent(schema) value | No       | 0.6.0-incubating |
| `authentication.kerberos.principal`   | The principal of the Kerberos authentication for the fileset.                                          | The parent(schema) value | No       | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`  | The URI of The keytab for the Kerberos authentication for the fileset.                                 | The parent(schema) value | No       | 0.6.0-incubating |

### Fileset operations

Refer to [Fileset operations](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.
