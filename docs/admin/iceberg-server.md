---
title: Iceberg REST catalog service
slug: /iceberg-rest-service
keywords:
  - Iceberg REST catalog
license: "This software is licensed under the Apache License version 2."
---

## Background

The Apache Gravitino Iceberg REST Server follows the
[Apache Iceberg REST API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
and acts as an Iceberg REST catalog server,
you could access the Iceberg REST endpoint at `http://$ip:$port/iceberg/`.

### Capabilities

- Supports the Apache Iceberg REST API defined in Iceberg 1.5, and supports all namespace and table interfaces.
  The following interfaces are not implemented yet:
  - multi-table transaction
  - pagination
- Works as a catalog proxy, supporting `Hive` and `JDBC` as catalog backend.
- Supports credential vending for `S3`、`GCS`、`OSS` and `ADLS`.
- Supports different storages like `S3`, `HDFS`, `OSS`, `GCS`, `ADLS`.
- Capable of supporting other storages.
- Supports event listener.
- Supports Audit log.
- Supports OAuth2 and HTTPS.
- Provides a pluggable metrics store interface to store and delete Iceberg metrics.

## Server management

There are three deployment scenarios for Gravitino Iceberg REST server:

- A standalone server in a standalone Gravitino Iceberg REST server package, the CLASSPATH is `libs`.
- A standalone server in the Gravitino server package, the CLASSPATH is `iceberg-rest-server/libs`.
- An auxiliary service embedded in the Gravitino server, the CLASSPATH is `iceberg-rest-server/libs`.

For detailed instructions on how to build and install the Gravitino server package,
please refer to [the build guide](../develop/how-to-build.md) and [the installation guide](../install/install.md).
To build the Gravitino Iceberg REST server package, use the command `./gradlew compileIcebergRESTServer -x test`.
Alternatively, to create the corresponding compressed package in the distribution directory,
use `./gradlew assembleIcebergRESTServer -x test`.
The Gravitino Iceberg REST server package includes the following files:

```text
├─ ...
└─ distribution/gravitino-iceberg-rest-server
    ├─ bin/
    │  └─ gravitino-iceberg-rest-server.sh    # Launching scripts.
    ├─ conf/                                   # All configurations.
    │  ├─ gravitino-iceberg-rest-server.conf  # Server configuration.
    │  ├─ gravitino-env.sh                    # Environment variables, e.g. JAVA_HOME, GRAVITINO_HOME, etc.
    │  ├─ log4j2.properties                   # log4j configurations.
    │  └─ hdfs-site.xml & core-site.xml       # HDFS configuration files.
    ├─ libs/                                   # Dependencies libraries.
    └─ logs/                                   # Logs directory. Auto-created after the server starts.
```

## Server configuration

There are distinct configuration files for standalone and auxiliary server:

- `gravitino-iceberg-rest-server.conf` is used for the standalone server;
- `gravitino.conf` is for the auxiliary server.

Although the configuration files differ, the configuration items remain the same.

Starting with version `0.6.0-incubating`, the prefix `gravitino.auxService.iceberg-rest.`
for auxiliary server configurations has been deprecated.
If both `gravitino.auxService.iceberg-rest.key` and `gravitino.iceberg-rest.key` are present,
the latter will take precedence.
The configurations listed below use the `gravitino.iceberg-rest.` prefix.

### Configuration to enable Iceberg REST service in Gravitino server.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.auxService.names</tt></td>
  <td>
    The auxiliary service name of the Gravitino Iceberg REST catalog service.
    Use `iceberg-rest`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.classpath</tt></td>
  <td>
    The CLASSPATH of the Gravitino Iceberg REST catalog service,
    including the directory containing JARs and configuration.
    It supports both absolute and relative paths.
    For example, `iceberg-rest-server/libs,iceberg-rest-server/conf`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
</tbody>
</table>

:::note
These configurations only are only effective in `gravitino.conf`.
You don't need to specify them if the Iceberg server is started
as a standalone server.
:::

### HTTP server configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.host</tt></td>
  <td>The host of the Gravitino Iceberg REST catalog service.</td>
  <td>`0.0.0.0`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.httpPort</tt></td>
  <td>The port of the Gravitino Iceberg REST catalog service.</td>
  <td>`9001`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.minThreads</tt></td>
  <td>
    The minimum number of threads in the thread pool used by the Jetty Web server.
    `minThreads` is 8 if the value is less than 8.
  </td>
  <td>`Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 100), 8)`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.maxThreads</tt></td>
  <td>
    The maximum number of threads in the thread pool used by the Jetty Web server.
    `maxThreads` is 8 if the value is less than 8, and the value must be greater than or equal to `minThreads`.
  </td>
  <td>`Math.max(Runtime.getRuntime().availableProcessors() * 4, 400)`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.threadPoolWorkQueueSize</tt></td>
  <td>
    The size of the queue in the thread pool used by Gravitino Iceberg REST catalog service.
  </td>
  <td>`100`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.stopTimeout</tt></td>
  <td>
    The amount of time in ms for the Gravitino Iceberg REST catalog service to stop gracefully.
    For more information, see `org.eclipse.jetty.server.Server#setStopTimeout`.
  </td>
  <td>`30000`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.idleTimeout</tt></td>
  <td>The timeout in ms of idle connections.</td>
  <td>`30000`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.requestHeaderSize</tt></td>
  <td>The maximum size in bytes for a HTTP request.</td>
  <td>`131072`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.responseHeaderSize</tt></td>
  <td>The maximum size in bytes for a HTTP response.</td>
  <td>`131072`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.customFilters</tt></td>
  <td>
    Comma-separated list of filter class names to apply to the APIs.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>

The filter in `customFilters` should be a standard javax servlet filter.
You can also specify filter parameters by setting configuration entries
in the format `gravitino.iceberg-rest.<filter class name>.param.<name>=<value>`.

### Security

Gravitino Iceberg REST server supports OAuth2 and HTTPS,
please refer to [security documentation](../security/security.md) for more details.

#### Backend authentication

For JDBC backend, you can use the `gravitino.iceberg-rest.jdbc.user` and `gravitino.iceberg-rest.jdbc.password`
to authenticate the JDBC connection.
For Hive backend, you can use the `gravitino.iceberg-rest.authentication.type`
to specify the authentication type, and use the `gravitino.iceberg-rest.authentication.kerberos.principal`
and `gravitino.iceberg-rest.authentication.kerberos.keytab-uri`
to authenticate the Kerberos connection.
The detailed configuration items are as follows:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.type</tt></td>
  <td>
    The type of authentication for Iceberg rest catalog backend.
    This configuration only applicable for for Hive backend,
    and only supports `Kerberos`, `simple` currently.
    As for JDBC backend, only username/password authentication is supported now.
  </td>
  <td>`simple`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.impersonation-enable</tt></td>
  <td>Whether impersonation is enabled for the Iceberg catalog service.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.hive.metastore.sasl.enabled</tt></td>
  <td>
    Whether SASL authentication protocol is enabled when connecting to Kerberos Hive metastore.

    This value should be `true` in most case
    when  the value of `gravitino.iceberg-rest.authentication.type` is Kerberos.
    In some very rare cases, the SSL protocol is used.
  </td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.kerberos.principal</tt></td>
  <td>
    The principal of the Kerberos authentication.

    This field required if the value of `gravitino.iceberg-rest.authentication.type` is `Kerberos`.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.kerberos.keytab-uri</tt></td>
  <td>
    The URI of the keytab for the Kerberos authentication.
    This field required if the value of `gravitino.iceberg-rest.authentication.type` is `Kerberos`.
  </td>
  <td>(none)</td>
  <td>Yes|No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.kerberos.check-interval-sec</tt></td>
  <td>The check interval in seconds of Kerberos credential for Iceberg catalog.</td>
  <td>60</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.authentication.kerberos.keytab-fetch-timeout-sec</tt></td>
  <td>
    The fetch timeout in seconds when retrieving Kerberos keytab
    from `authentication.kerberos.keytab-uri`.
  </td>
  <td>60</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

### Credential vending

Please refer to [credential vending](../security/credential-vending.md) for more details.

### Storage

#### S3 configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.aws.s3.S3FileIO` for S3.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.s3-endpoint</tt></td>
  <td>
    An alternative endpoint of the S3 service.
    This could be used for S3FileIO with any s3-compatible object storage service
    that has a different endpoint, or access a private S3 endpoint
    in a virtual private cloud.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.s3-region</tt></td>
  <td>The region of the S3 service, like `us-west-2`.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.s3-path-style-access</tt></td>
  <td>Whether to use path style access for S3.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.9.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg s3 properties not managed by Gravitino like `s3.sse.type`,
you could config it directly by `gravitino.iceberg-rest.s3.sse.type`.

Please refer to [S3 credentials](../security/credential-vending.md#s3-credentials)
for credential related configurations.

:::info
To configure the JDBC catalog backend, set the `gravitino.iceberg-rest.warehouse` parameter
to `s3://{bucket_name}/${prefix_name}`.
For the Hive catalog backend, set `gravitino.iceberg-rest.warehouse`
to `s3a://{bucket_name}/${prefix_name}`.
Additionally, download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle)
and place it in the CLASSPATH of Iceberg REST server.
:::

#### OSS configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.aliyun.oss.OSSFileIO` for OSS.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.oss-endpoint</tt></td>
  <td>The endpoint of Aliyun OSS service.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.oss-region</tt></td>
  <td>
    The region of the OSS service, like `oss-cn-hangzhou`.
    Only used when `credential-providers` is `oss-token`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg OSS properties not managed by Gravitino like `client.security-token`,
you could config it directly by `gravitino.iceberg-rest.client.security-token`.

Please refer to [OSS credentials](../security/credential-vending.md#oss-credentials)
for credential related configurations.

Additionally, Iceberg doesn't provide Iceberg Aliyun bundle JARs which contains OSS packages,
there are two alternatives to use OSS packages:

1. Use [Gravitino Aliyun bundle JAR with Hadoop packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle).
1. Use [Aliyun JAVA SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip)
   and extract `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar`.

Please place the above jars in the CLASSPATH of Iceberg REST server.
Refer to [server management](#server-management) for CLASSPATH details.

:::info
You need to set the `gravitino.iceberg-rest.warehouse` parameter
to `oss://{bucket_name}/${prefix_name}`. 
:::

#### GCS

Supports using static GCS credential file or generating GCS token to access GCS data.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg GCS properties not managed by Gravitino like `gcs.project-id`,
you can config it directly using `gravitino.iceberg-rest.gcs.project-id`.

Please refer to [GCS credentials](../security/credential-vending.md#gcs-credentials)
for credential related configurations.

:::note
Please ensure that the credential file can be accessed by the Gravitino server.
For example, if the server is running on a GCE machine, or you can set the environment variable
`export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`,
even when the `gcs-service-account-file` has already been configured.
:::

:::info
Please set `gravitino.iceberg-rest.warehouse` to `gs://{bucket_name}/${prefix_name}`,
and download [Iceberg gcp bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle)
and place it into the CLASSPATH of Gravitino Iceberg REST server,
`iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

#### ADLS

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Use `org.apache.iceberg.azure.adlsv2.ADLSFileIO` for ADLS.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

For other Iceberg ADLS properties not managed by Gravitino like `adls.read.block-size-bytes`,
you could config it directly using `gravitino.iceberg-rest.adls.read.block-size-bytes`.

Please refer to [ADLS credentials](../security/credential-vending.md#adls-credentials)
for credential related configurations.

:::info
Please set `gravitino.iceberg-rest.warehouse` to
`abfs[s]://{container-name}@{storage-account-name}.dfs.core.windows.net/{path}`,
and download the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle)
and place it into the CLASSPATH of the Iceberg REST server.
:::

#### HDFS configuration

You should place HDFS configuration file to the CLASSPATH of the Iceberg REST server,
`iceberg-rest-server/conf` for Gravitino server package,
`conf` for standalone Gravitino Iceberg REST server package.
When writing to HDFS, the Gravitino Iceberg REST catalog service can only operate
as the specified HDFS user and doesn't support proxying to other HDFS users.
See [How to access Apache Hadoop](./server-config.md#how-to-access-apache-hadoop)
for more details.

:::info
Builds with Hadoop 2.10.x. There may be compatibility issues when accessing Hadoop 3.x clusters.
:::

#### Other storages

For other storages that are not managed by Gravitino directly,
you can manage them through custom catalog properties.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>
    The I/O implementation for `FileIO` in Iceberg.
    Please use the full qualified classname.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

To pass custom properties such as `security-token` to your custom `FileIO`,
you can directly configure it using `gravitino.iceberg-rest.security-token`.
`security-token` will be included in the properties when `FileIO` is initialized.

:::info
Please set the `gravitino.iceberg-rest.warehouse` parameter to
`{storage_prefix}://{bucket}/{prefix}`.
Additionally, download corresponding jars in the CLASSPATH of Iceberg REST server,
`iceberg-rest-server/libs` for the auxiliary server, `libs` for the standalone server.
:::

### Catalog backend configuration

:::info
The Gravitino Iceberg REST catalog service uses the memory catalog backend by default.
You can specify a Hive or JDBC catalog backend for production environment.
:::

#### Hive backend configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend</tt></td>
  <td>
    The catalog backend for the Gravitino Iceberg REST catalog service.
    Use the value **`hive`** for a Hive catalog.
  </td>
  <td>`memory`</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.uri</tt></td>
  <td>The Hive metadata address, such as `thrift://127.0.0.1:9083`.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.warehouse</tt></td>
  <td>The warehouse directory of the Hive catalog, such as `/user/hive/warehouse-hive/`.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend-name</tt></td>
  <td>
    The catalog backend name passed to underlying Iceberg catalog backend.
    Catalog name in JDBC backend is used to isolate namespace and tables.

    The default value is `hive` for Hive backend, `jdbc` for JDBC backend,
    or `memory` for memory backend.
  </td>
  <td>`hive`|`jdbc`|`memory`</td>
  <td>No</td>
  <td>`0.5.2`</td>
</tr>
</tbody>
</table>

#### JDBC backend configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend</tt></td>
  <td>
    The catalog backend of the Gravitino Iceberg REST catalog service.
    Use the value **`jdbc`** for a JDBC catalog.
  </td>
  <td>`memory`</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.uri</tt></td>
  <td>
    The JDBC connection address, such as `jdbc:postgresql://127.0.0.1:5432` for Postgres,
    or `jdbc:mysql://127.0.0.1:3306/` for MySQL.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.warehouse</tt></td>
  <td>
    The warehouse directory of JDBC catalog.
    Set the HDFS prefix if using HDFS, such as
    `hdfs://127.0.0.1:9000/user/hive/warehouse-jdbc`.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend-name</tt></td>
  <td>
    The catalog name passed to underlying Iceberg catalog backend.
    Catalog name in JDBC backend is used to isolate namespace and tables.

    For JDBC backend, the default value is `jdbc`.
  </td>
  <td>`jdbc`</td>
  <td>No</td>
  <td>`0.5.2`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.jdbc.user</tt></td>
  <td>
    The username of the JDBC connection.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.jdbc.password</tt></td>
  <td>The password of the JDBC connection.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.jdbc-initialize</tt></td>
  <td>
    Whether to initialize the meta tables when creating the JDBC catalog.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.jdbc-driver</tt></td>
  <td>
    The default value is `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver` for MySQL;
    `org.postgresql.Driver` for PostgreSQL.
  </td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.3.0`</td>
</tr>
</tbody>
</table>

If you have a JDBC Iceberg catalog prior, you must set `catalog-backend-name`
to keep consistent with your Jdbc Iceberg catalog name to operate the prior namespace and tables.

:::caution
You must download the corresponding JDBC driver to the `iceberg-rest-server/libs` directory.
:::

#### Custom backend configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend</tt></td>
  <td>
    The catalog backend of the Gravitino Iceberg REST catalog service.
    Use the value **`custom`** for a Custom catalog.
  </td>
  <td>`memory`</td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-backend-impl</tt></td>
  <td>
    The fully-qualified class name of a custom catalog implementation.
    Only applicable when `catalog-backend` is `custom`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

If you want to use a custom Iceberg Catalog as `catalog-backend`,
you can add a corresponding JAR file to the CLASSPATH
and load a custom Iceberg Catalog implementation
by setting the `catalog-backend-impl` property.

### View support

You could access the view interface if using JDBC backend and enable `jdbc.schema-version` property.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.jdbc.schema-version</tt></td>
  <td>
    The schema version of JDBC catalog backend.
    Setting this to `V1` if supporting view operations.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

### Multi catalog support

The Gravitino Iceberg REST server supports multiple catalogs.
You can manage the catalog in different ways.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-config-provider</tt></td>
  <td>
    The `className` of catalog configuration provider.
    Gravitino provides built-in `static-config-provider` and `dynamic-config-provider`.
    You can also develop a custom class that implements `apache.gravitino.iceberg.service.provider.IcebergConfigProvider`,
    and add the corresponding JAR file to the Iceberg REST service CLASSPATH directory.
  </td>
  <td>`static-config-provider`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

#### Static catalog configuration provider

The static catalog configuration provider retrieves the catalog configuration
from the configuration file of the Gravitino Iceberg REST server.
You can select the default catalog to use by setting `gravitino.iceberg-rest.<param name>=<value>`.
For some specific catalogs, use the format
`gravitino.iceberg-rest.catalog.<catalog name>.<param name>=<value>`.

For instance, you can configure three different catalogs,
the default catalog and two separate, specific catalogs:
`hive_backend` and `jdbc_backend`.

```text
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:postgresql://127.0.0.1:5432
gravitino.iceberg-rest.warehouse = hdfs://127.0.0.1:9000/user/hive/warehouse-postgresql
...
gravitino.iceberg-rest.catalog.hive_backend.catalog-backend = hive
gravitino.iceberg-rest.catalog.hive_backend.uri = thrift://127.0.0.1:9084
gravitino.iceberg-rest.catalog.hive_backend.warehouse = /user/hive/warehouse-hive/
...
gravitino.iceberg-rest.catalog.jdbc_backend.catalog-backend = jdbc
gravitino.iceberg-rest.catalog.jdbc_backend.uri = jdbc:mysql://127.0.0.1:3306/
gravitino.iceberg-rest.catalog.jdbc_backend.warehouse = hdfs://127.0.0.1:9000/user/hive/warehouse-mysql
...
```

In the Iceberg REST client configuration, you can choose different catalogs to access
by using the catalog-specific `prefix` values.

The default catalog will be used if you do not provide a `prefix`.
For instance, consider the case of SparkSQL.

```shell
./bin/spark-sql -v \
...
--conf spark.sql.catalog.default_rest_catalog.type=rest  \
--conf spark.sql.catalog.default_rest_catalog.uri=http://127.0.0.1:9001/iceberg/ \
...
--conf spark.sql.catalog.hive_backend_catalog.type=rest  \
--conf spark.sql.catalog.hive_backend_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.hive_backend_catalog.prefix=hive_backend \
...
--conf spark.sql.catalog.jdbc_backend_catalog.type=rest  \
--conf spark.sql.catalog.jdbc_backend_catalog.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.jdbc_backend_catalog.prefix=jdbc_backend \
...
```

#### Dynamic catalog configuration provider

The dynamic catalog configuration provider retrieves the catalog configuration from the Gravitino server.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.gravitino-uri</tt></td>
  <td>
    The URI of the Gravitino server address.
    This is only applicable when `catalog-config-provider` is `dynamic-config-provider`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.gravitino-metalake</tt></td>
  <td>
    The metalake name that `dynamic-config-provider` used to request to Gravitino.
    Only applicable when `catalog-config-provider` is `dynamic-config-provider`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.catalog-cache-eviction-interval-ms</tt></td>
  <td>Catalog cache eviction interval.</td>
  <td>`3600000`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

```text
gravitino.iceberg-rest.catalog-cache-eviction-interval-ms = 300000
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-uri = http://127.0.0.1:8090
gravitino.iceberg-rest.gravitino-metalake = test
```

### Other Apache Iceberg catalog properties

You can add other properties defined in
[Iceberg catalog properties](https://iceberg.apache.org/docs/1.6.1/configuration/#catalog-properties).
The `clients` property for example:

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.clients</tt></td>
  <td>The client pool size of the catalog.</td>
  <td>`2`</td>
  <td>No</td>
</tr>
</tbody>
</table>

:::info
`catalog-impl` has no effect.
:::

### Event listener

Gravitino generates pre-event and post-event for table operations
and provide a pluggable event listener to allow you to inject custom logic.
For more details, please refer to [Event listener configuration](./server-config.md#event-listener-configuration).

### Audit log

Gravitino provides a pluggable audit logging mechanism, please refer to
[Audit log configuration](./server-config.md#audit-log-configuration).

### Apache Iceberg metrics store configuration

Gravitino provides a pluggable metrics store interface to store and delete Iceberg metrics.
You can develop a class that implements `org.apache.gravitino.iceberg.service.metrics.IcebergMetricsStore`
and add the corresponding jar file to the Iceberg REST service CLASSPATH directory.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.metricsStore</tt></td>
  <td>The Iceberg metrics storage class name.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.metricsStoreRetainDays</tt></td>
  <td>
    The days to keep Iceberg metrics in store.
    The value not greater than 0 means retain forever.
   </td>
  <td>`-1`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.metricsQueueCapacity</tt></td>
  <td>
    The size of queue to store metrics temporally before storing to the persistent storage.
    Metrics will be dropped when the queue is full.
  </td>
  <td>`1000`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>

### Misc configurations

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.extension-packages</tt></td>
  <td>
    Comma-separated list of Iceberg REST API packages to expand.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

## Starting the Iceberg REST server

To start as an auxiliary service with Gravitino server:

```shell
./bin/gravitino.sh start 
```

To start a standalone Gravitino Iceberg REST catalog server:

```shell
./bin/gravitino-iceberg-rest-server.sh start
```

To verify whether the service has started:

```shell
curl  http://127.0.0.1:9001/iceberg/v1/config
```

Normally you will see the output like `{"defaults":{},"overrides":{}}`.

## Exploring the Apache Gravitino Iceberg REST catalog service with Apache Spark

### Deploying Apache Spark with Apache Iceberg support

Follow the [Spark Iceberg start guide](https://iceberg.apache.org/docs/1.6.1/spark-getting-started/)
to set up Apache Spark's and Apache Iceberg's environment.

### Starting the Apache Spark client with the Apache Iceberg REST catalog

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>spark.sql.catalog.&lt;catalog-name&gt;.type</tt></td>
  <td>The Spark catalog type. This should be set to `rest`.</td>
</tr>
<tr>
  <td><tt>spark.sql.catalog.&lt;catalog-name&gt;.uri</tt></td>
  <td>Spark Iceberg REST catalog URI, such as `http://127.0.0.1:9001/iceberg/`.</td>
</tr>
</tbody>
</table>

For example, we can set Spark catalog options to use Gravitino Iceberg REST catalog
with the catalog name `rest`.

```shell
./bin/spark-sql -v \
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog  \
--conf spark.sql.catalog.rest.type=rest  \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/
```

You may need to adjust the Iceberg Spark runtime JAR file name based on the real version number in your environment.
If you want to access the data stored in cloud, you need to download corresponding JARs
(please refer to the cloud storage part) and place it in the CLASSPATH of Spark.
If you want to enable credential vending, please set `credential-providers` to a proper value at the server side,
set `spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation` = `vended-credentials` at the client side.

For other storages not managed by Gravitino, the properties wouldn't transfer from the server to client automatically.
If you want to pass custom properties to initialize `FileIO`, you could add it by setting
`spark.sql.catalog.${iceberg_catalog_name}.${configuration_key}` = `{property_value}`.

### Exploring Apache Iceberg with Apache Spark SQL

```sql
// Switch to use the `rest` catalog
USE rest;
CREATE DATABASE IF NOT EXISTS dml;
CREATE TABLE dml.test (id bigint COMMENT 'unique id') using iceberg;
DESCRIBE TABLE EXTENDED dml.test;
INSERT INTO dml.test VALUES (1), (2);
SELECT * FROM dml.test;
```

## Docker instructions

You could run Gravitino Iceberg REST server though docker container:

```shell
docker run -d -p 9001:9001 apache/gravitino-iceberg-rest:0.8.0-incubating
```

Gravitino Iceberg REST server in docker image could access local storage by default,
you could set the following environment variables if the storage is cloud/remote storage like S3.
Please refer to the [storage section](#storage) for more details.

<table>
<thead>
<tr>
  <th>Environment Variable</th>
  <th>Configuration item</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>GRAVITINO_IO_IMPL</tt></td>
  <td><tt>gravitino.iceberg-rest.io-impl</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_URI</tt></td>
  <td><tt>gravitino.iceberg-rest.uri</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_JDBC_DRIVER</tt></td>
  <td><tt>gravitino.iceberg-rest.jdbc-driver</tt></td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_JDBC_USER</tt></td>
  <td><tt>gravitino.iceberg-rest.jdbc-user</tt></td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_JDBC_PASSWORD</tt></td>
  <td><tt>gravitino.iceberg-rest.jdbc-password</tt></td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_WAREHOUSE</tt></td>
  <td><tt>gravitino.iceberg-rest.warehouse</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_CREDENTIAL_PROVIDERS</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_GCS_SERVICE_ACCOUNT_FILE</tt></td>
  <td><tt>gravitino.iceberg-rest.gcs-service-account-file</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_ACCESS_KEY</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-access-key-id</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_SECRET_KEY</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-secret-access-key</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_ENDPOINT</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-endpoint</tt></td>
  <td>`0.9.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_REGION</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-region</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_ROLE_ARN</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-role-arn</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_EXTERNAL_ID</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-external-id</tt></td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_S3_TOKEN_SERVICE_ENDPOINT</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-token-service-endpoint</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-name</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-key</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_AZURE_TENANT_ID</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-tenant-id</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_AZURE_CLIENT_ID</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-client-id</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_AZURE_CLIENT_SECRET</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-client-secret</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_ACCESS_KEY</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-access-key-id</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_SECRET_KEY</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-secret-access-key</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_ENDPOINT</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-endpoint</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_REGION</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-region</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_ROLE_ARN</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-role-arn</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_OSS_EXTERNAL_ID</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-external-id</tt></td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

The environment variables below are deprecated.
Please use their equivalent new configuration items instead.

<table>
<thead>
<tr>
  <th>Deprecated Variable</th>
  <th>New Variable</th>
  <th>Since Version</th>
  <th>Deprecated Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>GRAVITINO_CREDENTIAL_PROVIDER_TYPE</tt></td>
  <td><tt>GRAVITINO_CREDENTIAL_PROVIDERS</tt></td>
  <td>`0.7.0-incubating`</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>GRAVITINO_GCS_CREDENTIAL_FILE_PATH</tt></td>
  <td><tt>GRAVITINO_GCS_SERVICE_ACCOUNT_FILE</tt></td>
  <td>`0.7.0-incubating`</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

Or build it manually to add custom configuration or logics:

```shell
sh ./dev/docker/build-docker.sh \
  --platform linux/arm64 \
  --type iceberg-rest-server \
  --image apache/gravitino-iceberg-rest \
  --tag 0.7.0-incubating
```

You could try Spark with Gravitino REST catalog service in our
[playground](../playground/using-the-playground.md#using-apache-iceberg-rest-service).

