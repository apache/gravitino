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

| Property Name                                      | Description                                                                                                                                                                                                                                                                                                    | Default Value   | Required                                                    | Since Version    |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|-------------------------------------------------------------|------------------|
| `location`                                         | The storage location managed by Hadoop catalog.                                                                                                                                                                                                                                                                | (none)          | No                                                          | 0.5.0            |
| `filesystem-providers`                             | The names (split by comma) of filesystem providers for the Hadoop catalog. Gravitino already support built-in `builtin-local`(`local file`) and `builtin-hdfs`(`hdfs`). If users want to support more file system and add it to Gravitino, they custom more file system by implementing `FileSystemProvider`.  | (none)          | No                                                          | 0.7.0-incubating |
| `default-filesystem-provider`                      | The name default filesystem providers of this Hadoop catalog if users do not specify the scheme in the URI. Default value is `builtin-local`                                                                                                                                                                   | `builtin-local` | No                                                          | 0.7.0-incubating |
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Hadoop catalog.                                                                                                                                                                                                                                                        | `false`         | No                                                          | 0.5.1            |
| `authentication.type`                              | The type of authentication for Hadoop catalog, currently we only support `kerberos`, `simple`.                                                                                                                                                                                                                 | `simple`        | No                                                          | 0.5.1            |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                                                                                                                                                                                                                                   | (none)          | required if the value of `authentication.type` is Kerberos. | 0.5.1            |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                                                                                                                                                                                                                                         | (none)          | required if the value of `authentication.type` is Kerberos. | 0.5.1            |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Hadoop catalog.                                                                                                                                                                                                                                                  | 60              | No                                                          | 0.5.1            |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.                                                                                                                                                                                                                     | 60              | No                                                          | 0.5.1            |

For more about `filesystem-providers`, please refer to `HadoopFileSystemProvider` or `LocalFileSystemProvider` in the source code. Furthermore, you also need to place the jar of the file system provider into the `$GRAVITINO_HOME/catalogs/hadoop/libs` directory if it's not in the classpath.

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
