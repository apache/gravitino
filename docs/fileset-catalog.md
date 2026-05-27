---
title: "Fileset Catalog"
slug: "/fileset-catalog"
keywords:
  - fileset
  - hcfs
  - metadata
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Fileset catalog enables Apache Gravitino to manage collections of storage locations (filesets) through the Hadoop Compatible File System (HCFS) interface. Use it to register, govern, and address dataset directories that live on HDFS, the local filesystem, or cloud object storage (Amazon S3, Google Cloud Storage, Alibaba OSS, Azure Blob Storage), so engines and applications can discover and access them through a single Gravitino-managed surface alongside relational and lakehouse catalogs.

The rest of this document uses HDFS or the local filesystem as the running example. Configuration for S3, GCS, OSS, and Azure Blob Storage follows the same shape; see the per-storage guides linked from [Requirements and Limitations](#requirements-and-limitations) below.

Gravitino builds the Fileset catalog against Hadoop 3 dependencies. Open an [issue](https://github.com/apache/gravitino/issues) if you hit a compatibility problem.

### Requirements and Limitations

- **Hadoop Compatible File System backend.** The Fileset catalog manages storage locations through the HCFS interface. Out of the box, Gravitino supports the local filesystem and HDFS.
- **Cloud storage requires bundle JARs.** Since 0.7.0, the Fileset catalog also supports Amazon S3, Google Cloud Storage, Alibaba OSS, and Azure Blob Storage (ADLS Gen2). Each requires the corresponding Gravitino bundle JAR (`gravitino-aws-bundle`, `gravitino-gcp-bundle`, `gravitino-aliyun-bundle`, or `gravitino-azure-bundle`) on the Gravitino server. See the per-storage guides: [S3](fileset-catalog-with-s3.md), [GCS](fileset-catalog-with-gcs.md), [OSS](fileset-catalog-with-oss.md), [ADLS](fileset-catalog-with-adls.md).
- **Filesets are directories or path prefixes, not single files.** The `location` value must be a directory (HDFS, local) or a path prefix (cloud storage). Single-file filesets are not supported.
- **Authentication.** `simple` and `Kerberos` are supported and can be configured independently at the catalog, schema, and fileset levels, with schema and fileset values inheriting from the parent unless overridden.
- **Credential vending.** Optional. Configure `credential-providers` at the catalog, schema, or fileset level. See [Credential vending](./security/credential-vending.md).

## Quick Start

Create a minimum-viable Fileset catalog backed by the local filesystem and confirm it is reachable. The example uses a local path so the walkthrough runs against a default Gravitino installation without any cloud bundle JARs. For HDFS or cloud-backed filesets, see the [Catalog Properties](#catalog-properties) section and the per-storage guides linked above. The walkthrough assumes a Gravitino server at `http://localhost:8090` and a metalake named `test`.

### Create the Catalog

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "fileset_catalog",
    "type": "FILESET",
    "comment": "Local fileset catalog",
    "provider": "fileset",
    "properties": {
      "location": "file:///tmp/gravitino-filesets"
    }
  }' \
  http://localhost:8090/api/metalakes/test/catalogs
```

The response is a JSON object describing the created catalog. The directory at `/tmp/gravitino-filesets` is created automatically when the first schema is added (unless `disable-filesystem-ops` is `true`).

### Verify the Catalog

```bash
# List catalogs in the metalake. fileset_catalog should appear.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs" | jq

# Load the catalog directly and inspect its properties.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/fileset_catalog" | jq

# List schemas. The response is an empty array on a freshly created catalog until a schema is added.
curl -sS "http://localhost:8090/api/metalakes/test/catalogs/fileset_catalog/schemas" | jq
```

**Success check:** the catalog-list response includes `fileset_catalog`, the load-catalog response shows `"type":"FILESET"`, `"provider":"fileset"`, and `"location":"file:///tmp/gravitino-filesets"` in the properties. The schema-list response is a JSON array; an empty array on a fresh catalog is expected. If the load-catalog response shows the catalog but later schema creation fails with a permissions error, confirm that the Gravitino server process has write access to `/tmp/gravitino-filesets`.

## Catalog

### Catalog Capabilities

The Fileset catalog:

- Manages storage locations through the Hadoop Compatible File System (HCFS) interface.
- Supports filesets backed by directories (HDFS, local filesystem) or path prefixes (S3, GCS, OSS, ADLS).
- Supports multi-level authentication inheritance from catalog through schema to fileset.
- Supports credential vending through pluggable credential providers configured at the catalog, schema, or fileset level.
- Caches loaded filesets to reduce filesystem-client overhead; cache eviction is configurable through `fileset-cache-eviction-interval-ms` and `fileset-cache-max-size`.

### Catalog Properties

Besides the [common catalog properties](./gravitino-server-config.md#catalog-properties),
the Fileset catalog has the following properties:

| Property Name                        | Description                                                                                                                                                                                                                                                                                                                     | Default Value   | Required | Since Version    |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|----------|------------------|
| `location`                           | Storage location managed by the fileset catalog. The location name is `unknown`. The value must always be a directory (HDFS) or path prefix (cloud storage such as S3 or GCS); single files are not supported.                                                                                                                  | (none)          | No       | 0.5.0            |
| `location-`                          | The property prefix. User can use `location-{name}={path}` to set multiple locations with different names for the catalog.                                                                                                                                                                                                      | (none)          | No       | 0.9.0-incubating |
| `default-filesystem-provider`        | (deprecated) The default filesystem provider of this Fileset catalog if users do not specify the scheme in the URI. Candidate values are 'builtin-local', 'builtin-hdfs', 's3', 'gcs', 'abs' and 'oss'. Default value is `builtin-local`. For S3, if we set this value to 's3', we can omit the prefix 's3a://' in the location. | `builtin-local` | No       | 0.7.0-incubating |
| `filesystem-providers`               | (deprecated) The file system providers to add. Users need to set this configuration to support cloud storage or custom HCFS. For instance, set it to `s3` or a comma separated string that contains `s3` like `gs,s3` to support multiple kinds of fileset including `s3`.                                                       | (none)          | No       | 0.7.0-incubating |
| `credential-providers`               | The credential provider types, separated by comma.                                                                                                                                                                                                                                                                              | (none)          | No       | 0.8.0-incubating |
| `filesystem-conn-timeout-secs`       | The timeout of getting the file system using Hadoop FileSystem client instance. Time unit: seconds.                                                                                                                                                                                                                             | 6               | No       | 0.8.0-incubating |
| `disable-filesystem-ops`             | Disables filesystem operations on the server side. When `true`, the fileset catalog does not create or delete files and folders as schemas and filesets are created and dropped.                                                                                                                                                | false           | No       | 0.9.0-incubating |
| `fileset-cache-eviction-interval-ms` | The interval in milliseconds to evict the fileset cache, -1 means never evict.                                                                                                                                                                                                                                                  | 3600000         | No       | 0.9.0-incubating |
| `fileset-cache-max-size`             | The maximum number of the filesets the cache may contain, -1 means no limit.                                                                                                                                                                                                                                                    | 200000          | No       | 0.9.0-incubating |
| `config.resources`                   | The configuration resources, separated by comma. For example, `hdfs-site.xml,core-site.xml`.                                                                                                                                                                                                                                    | (none)          | No       | 1.1.0            |
| `fs.path.config.<name>`              | Defines a logical location entry. Set `fs.path.config.<name>` to the real base URI (for example, `hdfs://cluster1/`). Any key that starts with the same prefix (such as `fs.path.config.<name>.config.resource`) is treated as a location-scoped property and will be forwarded to the underlying filesystem client.            | (none)          | No       | 1.2.0            |

:::note
`default-filesystem-provider` and `filesystem-providers` are deprecated as of 1.2.0. The fileset catalog automatically loads filesystem providers from the classpath, including the built-in providers and cloud providers when the corresponding bundle JAR is present (for example, `gravitino-aws-bundle`, `gravitino-azure-bundle`, `gravitino-aliyun-bundle`, or `gravitino-gcp-bundle`).
:::

Refer to [Credential vending](./security/credential-vending.md) for more details about credential vending.

### HDFS Fileset

Apart from the above properties, to access fileset like HDFS fileset, you need to configure the following extra
properties.

| Property Name                                      | Description                                                                                     | Default Value | Required                                                    | Since Version |
|----------------------------------------------------|-------------------------------------------------------------------------------------------------|---------------|-------------------------------------------------------------|---------------|
| `authentication.impersonation-enable`              | Whether to enable impersonation for the Fileset catalog.                                        | `false`       | No                                                          | 0.5.1         |
| `authentication.type`                              | Authentication type for the fileset catalog. Supported values are `kerberos` and `simple`.      | `simple`      | No                                                          | 0.5.1         |
| `authentication.kerberos.principal`                | The principal of the Kerberos authentication                                                    | (none)        | required if the value of `authentication.type` is Kerberos. | 0.5.1         |
| `authentication.kerberos.keytab-uri`               | The URI of The keytab for the Kerberos authentication.                                          | (none)        | required if the value of `authentication.type` is Kerberos. | 0.5.1         |
| `authentication.kerberos.check-interval-sec`       | The check interval of Kerberos credential for Fileset catalog.                                  | 60            | No                                                          | 0.5.1         |
| `authentication.kerberos.keytab-fetch-timeout-sec` | The fetch timeout of retrieving Kerberos keytab from `authentication.kerberos.keytab-uri`.      | 60            | No                                                          | 0.5.1         |

The `config.resources` property allows users to specify custom configuration files.

The Gravitino Fileset extends the following properties in the `xxx-site.xml`:

| Property Name                                     | Description                                                             | Default Value | Required                                                    | Since Version |
|---------------------------------------------------|-------------------------------------------------------------------------|---------------|-------------------------------------------------------------|---------------|
| hadoop.security.authentication.kerberos.principal | The principal of the Kerberos authentication for HDFS client.           | (none)        | required if the value of `authentication.type` is Kerberos. | 1.1.0         |
| hadoop.security.authentication.kerberos.keytab    | The keytab file path of the Kerberos authentication for HDFS client.    | (none)        | required if the value of `authentication.type` is Kerberos. | 1.1.0         |
| hadoop.security.authentication.kerberos.krb5.conf | The krb5.conf file path of the Kerberos authentication for HDFS client. | (none)        | No                                                          | 1.1.0         |

### Fileset Catalog with Cloud Storage

In the current implementation, the fileset uses the HDFS protocol to access its location. If users use S3, GCS, OSS,
or Azure Blob Storage, they can also configure the `config.resources` to specify custom configuration
files.

- For S3, refer to [Fileset-catalog-with-s3](./fileset-catalog-with-s3.md) for more details.
- For GCS, refer to [Fileset-catalog-with-gcs](./fileset-catalog-with-gcs.md) for more details.
- For OSS, refer to [Fileset-catalog-with-oss](./fileset-catalog-with-oss.md) for more details.
- For Azure Blob Storage, refer to [Fileset-catalog-with-adls](./fileset-catalog-with-adls.md) for more details.

### Plug in a Custom HCFS Filesystem

Implement the `FileSystemProvider` interface from the [gravitino-hadoop-common](https://repo1.maven.org/maven2/org/apache/gravitino/gravitino-hadoop-common/) JAR. The interface is defined as follows:

```java
  
  // Create a FileSystem instance by the properties you have set when creating the catalog. 
  FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException;
  
  // The schema name of the file system provider. 'file' for Local file system,
  // 'hdfs' for HDFS, 's3a' for AWS S3, 'gs' for GCS, 'oss' for Aliyun OSS. 
  String scheme();

  // Name of the file system provider. 'builtin-local' for Local file system, 'builtin-hdfs' for HDFS, 
  // 's3' for AWS S3, 'gcs' for GCS, 'oss' for Aliyun OSS.
  String name();
```

`FileSystemProvider` is loaded through Java SPI. Create a file named `org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider` in the `META-INF/services` directory of the JAR; its contents are the fully qualified class name of the custom provider. For example, the contents for `S3FileSystemProvider`:

![img.png](assets/fileset/custom-filesystem-provider.png)

Place the JAR in `$GRAVITINO_HOME/catalogs/fileset/libs`. The custom provider is then available to the fileset catalog.

### Fileset Catalog Authentication

The Fileset catalog supports multi-level authentication to control access, allowing different authentication settings
for the catalog, schema, and fileset. The priority of authentication settings is as follows: catalog < schema < fileset.
Specifically:

- **Catalog**: The default authentication is `simple`.
- **Schema**: Inherits the authentication setting from the catalog if not explicitly set. For more information about
  schema settings, refer to [Schema properties](#schema-properties).
- **Fileset**: Inherits the authentication setting from the schema if not explicitly set. For more information about
  fileset settings, refer to [Fileset properties](#fileset-properties).

`authentication.impersonation-enable` defaults to `false` at the catalog level. Schemas and filesets inherit the value from their parent; any value set explicitly overrides the inherited one, following the same precedence as the rest of the authentication settings.

### Catalog Operations

Refer to [Catalog operations](./manage-fileset-metadata-using-gravitino.md#catalog-operations) for more details.

## Schema

### Schema Capabilities

The fileset catalog supports creating, updating, deleting, and listing schemas.

### Schema Properties

Schemas inherit all catalog properties. The fileset catalog also defines the following schema properties:

| Property name                         | Description                                                                                                               | Default value             | Required | Since Version    |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------|---------------------------|----------|------------------|
| `location`                            | Storage location managed by the schema. The location name is `unknown`. The value must be a directory or path prefix.    | (none)                    | No       | 0.5.0            |
| `location-`                           | The property prefix. User can use `location-{name}={path}` to set multiple locations with different names for the schema. | (none)                    | No       | 0.9.0-incubating |
| `authentication.impersonation-enable` | Whether to enable impersonation for this schema of the Fileset catalog.                                                   | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.type`                 | Authentication type for this schema of the fileset catalog. Supported values are `kerberos` and `simple`.                | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.kerberos.principal`   | The principal of the Kerberos authentication for this schema.                                                             | The parent(catalog) value | No       | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`  | The URI of The keytab for the Kerberos authentication for this schema.                                                    | The parent(catalog) value | No       | 0.6.0-incubating |
| `credential-providers`                | The credential provider types, separated by comma.                                                                        | (none)                    | No       | 0.8.0-incubating |
| `config.resources`                    | The configuration resources, separated by comma. For example, `hdfs-site.xml,core-site.xml`.                              | (none)                    | No       | 1.1.0            |

### Schema Operations

Refer to [Schema operation](./manage-fileset-metadata-using-gravitino.md#schema-operations) for more details.

:::note
During schema creation or deletion, Gravitino automatically creates or removes the corresponding filesystem directories
for the schema locations.
This behavior is skipped in either of these cases:

1. When the catalog property `disable-filesystem-ops` is set to `true`
2. When the location contains [placeholders](./manage-fileset-metadata-using-gravitino.md#placeholder)
:::

## Fileset

### Fileset Capabilities

- A Gravitino fileset corresponds to a single storage location, or a named multi-location set, within a schema.
- Supports creating, listing, loading, updating, and dropping filesets.
- A fileset can be backed by a directory (HDFS, local filesystem) or a path prefix (S3, GCS, OSS, ADLS); single-file filesets are not supported.
- Inherits authentication settings from the schema (which in turn inherits from the catalog); any value set explicitly at the fileset level overrides the inherited one. See [Fileset Catalog Authentication](#fileset-catalog-authentication).
- Supports credential vending through `credential-providers` set at the fileset, schema, or catalog level. Fileset-level providers take precedence.

### Fileset Properties

Filesets inherit all schema properties, which in turn include the properties inherited from the catalog. The fileset catalog also defines the following fileset properties:

| Property name                         | Description                                                                                                             | Default value                                                                                                  | Required                                   | Immutable | Since Version    |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|--------------------------------------------|-----------|------------------|
| `location`                            | Storage location managed by the fileset. The location name is `unknown`. The value must be a directory or path prefix. | (none)                                                                                                         | No                                         | Yes       | 0.5.0            |
| `authentication.impersonation-enable` | Whether to enable impersonation for the Fileset catalog fileset.                                                        | The parent(schema) value                                                                                       | No                                         | Yes       | 0.6.0-incubating |
| `authentication.type`                 | Authentication type for the fileset. Supported values are `kerberos` and `simple`.                                     | The parent(schema) value                                                                                       | No                                         | No        | 0.6.0-incubating |
| `authentication.kerberos.principal`   | The principal of the Kerberos authentication for the fileset.                                                           | The parent(schema) value                                                                                       | No                                         | No        | 0.6.0-incubating |
| `authentication.kerberos.keytab-uri`  | The URI of The keytab for the Kerberos authentication for the fileset.                                                  | The parent(schema) value                                                                                       | No                                         | No        | 0.6.0-incubating |
| `credential-providers`                | The credential provider types, separated by comma.                                                                      | (none)                                                                                                         | No                                         | No        | 0.8.0-incubating |
| `placeholder-`                        | Properties that start with `placeholder-` are used to replace placeholders in the location.                             | (none)                                                                                                         | No                                         | Yes       | 0.9.0-incubating |
| `default-location-name`               | The name of the default location of the fileset, mainly used for GVFS operations without specifying a location name.    | When the fileset has only one location, its location name will be automatically selected as the default value. | Yes, if the fileset has multiple locations | Yes       | 0.9.0-incubating |
| `config.resources`                    | The configuration resources, separated by comma. For example, `hdfs-site.xml,core-site.xml`.                            | (none)                                                                                                         | No                                         | No        | 1.1.0            |

Some properties are reserved and cannot be set by users:

| Property name         | Description                           | Default value               | Since Version    |
|-----------------------|---------------------------------------|-----------------------------|------------------|
| `placeholder-catalog` | The placeholder for the catalog name. | catalog name of the fileset | 0.9.0-incubating |
| `placeholder-schema`  | The placeholder for the schema name.  | schema name of the fileset  | 0.9.0-incubating |
| `placeholder-fileset` | The placeholder for the fileset name. | fileset name                | 0.9.0-incubating |

Credential providers can be specified in several places, as listed below. Gravitino checks the `credential-providers`
setting in the following order of precedence:

1. Fileset properties
2. Schema properties
3. Catalog properties

### Fileset Operations

Refer to [Fileset operations](./manage-fileset-metadata-using-gravitino.md#fileset-operations) for more details.
