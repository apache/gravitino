---
title: "Hadoop catalog"
slug: /hadoop-catalog
date: 2024-4-2
keyword: hadoop catalog
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Hadoop catalog is a fileset catalog that using Hadoop Compatible File System (HCFS) to manage
the storage location of the fileset. Currently, it supports the local filesystem and HDFS. Since 0.7.0-incubating, Gravitino supports [S3](hadoop-catalog-with-s3.md), [GCS](hadoop-catalog-with-gcs.md), [OSS](hadoop-catalog-with-oss.md) and [Azure Blob Storage](hadoop-catalog-with-adls.md) through Hadoop catalog. 

The rest of this document will use HDFS or local file as an example to illustrate how to use the Hadoop catalog. For S3, GCS, OSS and Azure Blob Storage, the configuration is similar to HDFS, please refer to the corresponding document for more details.

Note that Gravitino uses Hadoop 3 dependencies to build Hadoop catalog. Theoretically, it should be
compatible with both Hadoop 2.x and 3.x, since Gravitino doesn't leverage any new features in
Hadoop 3. If there's any compatibility issue, please create an [issue](https://github.com/apache/gravitino/issues).

## Catalog

### Catalog properties

Besides the [common catalog properties](./gravitino-server-config.md#apache-gravitino-catalog-properties-configuration), the Hadoop catalog has the following properties:

| Property Name                  | Description                                                                                                                                                                                                                                                                                                        | Default Value   | Required | Since Version    |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|------------------|
| `location`                     | The storage location managed by Hadoop catalog.                                                                                                                                                                                                                                                                    | (none)          | No       | 0.5.0            |
| `default-filesystem-provider`  | The default filesystem provider of this Hadoop catalog if users do not specify the scheme in the URI. Candidate values are 'builtin-local', 'builtin-hdfs', 's3', 'gcs', 'abs' and 'oss'. Default value is `builtin-local`. For S3, if we set this value to 's3', we can omit the prefix 's3a://' in the location. | `builtin-local` | No       | 0.7.0-incubating |
| `filesystem-providers`         | The file system providers to add. Users need to set this configuration to support cloud storage or custom HCFS. For instance, set it to `s3` or a comma separated string that contains `s3` like `gs,s3` to support multiple kinds of fileset including `s3`.                                                     | (none)          | Yes      | 0.7.0-incubating |
| `credential-providers`         | The credential provider types, separated by comma.                                                                                                                                                                                                                                                                 | (none)          | No       | 0.8.0-incubating |
| `filesystem-conn-timeout-secs` | The timeout of getting the file system using Hadoop FileSystem client instance. Time unit: seconds.                                                                                                                                                                                                                | 6               | No       | 0.8.0-incubating |

Please refer to [Credential vending](./security/credential-vending.md) for more details about credential vending.

### HDFS fileset 

Apart from the above properties, to access fileset like HDFS fileset, you need to configure the following extra properties.

| Property Name                                      | Description                                                                                    | Default Value | Required                                                    | Since Version |
|----------------------------------------------------|------------------------------------------------------------------------------------------------|---------------|-------------------------------------------------------------|---------------|
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Hadoop catalog.                                        | `false`       | No                                                          | 0.5.1         |
| `authentication.type`                              | The type of authentication for Hadoop catalog, currently we only support `kerberos`, `simple`. | `simple`      | No                                                          | 0.5.1         |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                   | (none)        | required if the value of `authentication.type` is Kerberos. | 0.5.1         |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                         | (none)        | required if the value of `authentication.type` is Kerberos. | 0.5.1         |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Hadoop catalog.                                  | 60            | No                                                          | 0.5.1         |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.     | 60            | No                                                          | 0.5.1         |

### Hadoop catalog with Cloud Storage
- For S3, please refer to [Hadoop-catalog-with-s3](./hadoop-catalog-with-s3.md) for more details.
- For GCS, please refer to [Hadoop-catalog-with-gcs](./hadoop-catalog-with-gcs.md) for more details.
- For OSS, please refer to [Hadoop-catalog-with-oss](./hadoop-catalog-with-oss.md) for more details.
- For Azure Blob Storage, please refer to [Hadoop-catalog-with-adls](./hadoop-catalog-with-adls.md) for more details.

### How to custom your own HCFS file system fileset?

Developers and users can custom their own HCFS file system fileset by implementing the `FileSystemProvider` interface in the jar [gravitino-catalog-hadoop](https://repo1.maven.org/maven2/org/apache/gravitino/catalog-hadoop/). The `FileSystemProvider` interface is defined as follows:

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

In the meantime, `FileSystemProvider` uses Java SPI to load the custom file system provider. You need to create a file named `org.apache.gravitino.catalog.fs.FileSystemProvider` in the `META-INF/services` directory of the jar file. The content of the file is the full class name of the custom file system provider. 
For example, the content of `S3FileSystemProvider` is as follows:
![img.png](assets/fileset/custom-filesystem-provider.png)

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
| `authentication.kerberos.keytab-uri`  | The URI of The keytab for the Kerberos authentication for this schema.                                         | The parent(catalog) value | No       | 0.6.0-incubating |
| `credential-providers`                | The credential provider types, separated by comma.                                                             | (none)                    | No       | 0.8.0-incubating |

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
| `credential-providers`                | The credential provider types, separated by comma.                                                     | (none)                   | No       | 0.8.0-incubating |
| `placeholder-`                        | Properties that start with `placeholder-` are used to replace placeholders in the location.            | (none)                   | No       | 0.9.0-incubating |

Some properties are reserved and cannot be set by users:

| Property name         | Description                           | Default value               | Since Version    |
|-----------------------|---------------------------------------|-----------------------------|------------------|
| `placeholder-catalog` | The placeholder for the catalog name. | catalog name of the fileset | 0.9.0-incubating |
| `placeholder-schema`  | The placeholder for the schema name.  | schema name of the fileset  | 0.9.0-incubating |
| `placeholder-fileset` | The placeholder for the fileset name. | fileset name                | 0.9.0-incubating |

Credential providers can be specified in several places, as listed below. Gravitino checks the `credential-providers` setting in the following order of precedence:

1. Fileset properties
2. Schema properties
3. Catalog properties

### Fileset operations

Refer to [Fileset operations](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.
