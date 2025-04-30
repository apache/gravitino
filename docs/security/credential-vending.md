---
title: "Gravitino credential vending"
slug: /security/credential-vending
keyword: security credential vending
license: "This software is licensed under the Apache License version 2."
---

## Background

Gravitino *credential vending* is used to generate temporary or static credentials for accessing data.
With credential vending, Gravitino provides an unified way to control the access
to diverse data sources on different platforms.

### Capabilities

- Supports Gravitino Iceberg REST server.
- Supports Gravitino server, Hadoop catalog only.
- Supports pluggable credentials with built-in credentials:
  - S3: `S3TokenCredential`, `S3SecretKeyCredential`
  - GCS: `GCSTokenCredential`
  - ADLS: `ADLSTokenCredential`, `AzureAccountKeyCredential`
  - OSS: `OSSTokenCredential`, `OSSSecretKeyCredential`
- No support for Spark/Trino/Flink connector yet.

## General configurations

<table>
<thead>
<tr>
  <th>
    Gravitino server<br/>catalog properties
  </th>
  <th>
    Gravitino Iceberg REST<br/>server configurations
  </th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-provider-type</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-provider-type</tt></td>
  <td>Deprecated, please use `credential-providers` instead.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>The credential provider types, separated by comma.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-cache-expire-ratio</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-cache-expire-ratio</tt></td>
  <td>Ratio of the credential's expiration time when Gravitino remove credential from the cache.</td>
  <td>`0.15`</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>credential-cache-max-size</tt></td>
  <td><tt>gravitino.iceberg-rest.cache-max-size</tt></td>
  <td>Max size for the credential cache.</td>
  <td>10000</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

## Build-in credentials configurations

### S3 credentials

#### S3 secret key credential

A credential with static S3 access key id and secret access key.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`s3-secret-key` for S3 secret key credential provider.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-access-key-id</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-access-key-id</tt></td>
  <td>The static access key ID used to access S3 data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-secret-access-key</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-secret-access-key</tt></td>
  <td>The static secret access key used to access S3 data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

#### S3 token credential

An S3 token is a token credential with scoped privileges.
It leverages the STS [Assume Role](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html).
To use an S3 token credential, you should create a role and grant it proper privileges.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`s3-token` for S3 token credential provider.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-access-key-id</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-access-key-id</tt></td>
  <td>The static access key ID used to access S3 data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-secret-access-key</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-secret-access-key</tt></td>
  <td>The static secret access key used to access S3 data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-role-arn</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-role-arn</tt></td>
  <td>The ARN of the role to access the S3 data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-region</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-region</tt></td>
  <td>The region of the S3 service, like `us-west-2`.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-external-id</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-external-id</tt></td>
  <td>The S3 external id to generate token.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-token-expire-in-secs</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-token-expire-in-secs</tt></td>
  <td>
    The S3 session token expire time in seconds.
    The value must be less thanthe max session time of the assumed role.
  </td>
  <td>`3600`</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>s3-token-service-endpoint</tt></td>
  <td><tt>gravitino.iceberg-rest.s3-token-service-endpoint</tt></td>
  <td>
    An alternative endpoint of the S3 token service.
    This could be used with s3-compatible object storage service
    like MINIO that has a different STS endpoint.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### OSS credentials

#### OSS secret key credential

A credential with static OSS access key id and secret access key.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`oss-secret-key` for OSS secret credential.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-access-key-id</tt></td>
  <td>The static access key ID used to access OSS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-secret-access-key</tt></td>
  <td>The static secret access key used to access OSS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
</tbody>
</table>

#### OSS token credential

An OSS token is a token credential with scoped privileges
by leveraging STS [Assume Role](https://www.alibabacloud.com/help/en/oss/developer-reference/use-temporary-access-credentials-provided-by-sts-to-access-oss).
To use an OSS token credential, you should create a role and grant it proper privileges.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`oss-token` for s3 token credential.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-access-key-id</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-access-key-id</tt></td>
  <td>The static access key ID used to access OSS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-secret-access-key</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-secret-access-key</tt></td>
  <td>The static secret access key used to access OSS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-role-arn</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-role-arn</tt></td>
  <td>The ARN of the role to access the OSS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-region</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-region</tt></td>
  <td>
    The region of the OSS service, like `oss-cn-hangzhou`.
    Only applicable when `credential-providers` is `oss-token`.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-external-id</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-external-id</tt></td>
  <td>The OSS external id to generate token.</td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>oss-token-expire-in-secs</tt></td>
  <td><tt>gravitino.iceberg-rest.oss-token-expire-in-secs</tt></td>
  <td>The OSS security token expire time in secs.</td>
  <td>3600</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### ADLS credentials

#### Azure account key credential

A credential with static Azure storage account name and key.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`azure-account-key` for Azure account key credential.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-name</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-name</tt></td>
  <td>The static storage account name used to access ADLS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-key</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-key</tt></td>
  <td>The static storage account key used to access ADLS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

#### ADLS token credential

An ADLS token is a token credential with scoped privileges.
It leverages the Azure [User Delegation Sas](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas).
To use an ADLS token credential, you need to create a Microsoft Entra ID service principal
and grant it proper privileges.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`adls-token` for ADLS token credential.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-name</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-name</tt></td>
  <td>The static storage account name used to access ADLS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-storage-account-key</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-storage-account-key</tt></td>
  <td>The static storage account key used to access ADLS data.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-tenant-id</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-tenant-id</tt></td>
  <td>Azure Active Directory (AAD) tenant ID.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-client-id</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-client-id</tt></td>
  <td>Azure Active Directory (AAD) client ID used for authentication.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>azure-client-secret</tt></td>
  <td><tt>gravitino.iceberg-rest.azure-client-secret</tt></td>
  <td>Azure Active Directory (AAD) client secret used for authentication.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>adls-token-expire-in-secs</tt></td>
  <td><tt>gravitino.iceberg-rest.adls-token-expire-in-secs</tt></td>
  <td>The ADLS SAS token expire time in seconds.</td>
  <td>3600</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

### GCS credentials

#### GCS token credential

An GCS token is a token credential with scoped privileges.
It leverages the GCS [Credential Access Boundaries](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials).
To use an GCS token credential, you should create an GCS service account
and grant it proper privileges.

<table>
<thead>
<tr>
  <th>Gravitino server<br/>catalog properties</th>
  <th>Gravitino Iceberg REST<br/>server configurations</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>credential-providers</tt></td>
  <td><tt>gravitino.iceberg-rest.credential-providers</tt></td>
  <td>`gcs-token` for GCS token credential.</td>
  <td>(none)</td>
  <td>Yes</td>
  <td>`0.8.0-incubating`</td>
</tr>
<tr>
  <td><tt>gcs-credential-file-path</tt></td>
  <td><tt>gravitino.iceberg-rest.gcs-credential-file-path</tt></td>
  <td>Deprecated, please use `gcs-service-account-file` instead.</td>
  <td>GCS Application default credential.</td>
  <td>No</td>
  <td>`0.7.0-incubating`</td>
</tr>
<tr>
  <td><tt>gcs-service-account-file</tt></td>
  <td><tt>gravitino.iceberg-rest.gcs-service-account-file</tt></td>
  <td>The location of GCS credential file.</td>
  <td>GCS Application default credential.</td>
  <td>No</td>
  <td>`0.8.0-incubating`</td>
</tr>
</tbody>
</table>

:::note
For the Gravitino Iceberg REST server, please ensure that the credential file can be accessed by the server.
For example, if the server is running on a GCE machine,
or you can set the environment variable like
`export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`,
even when the `gcs-service-account-file` has already been configured.
:::

## Custom credentials

Gravitino supports custom credentials. To support custom credentials,
you can implement the `org.apache.gravitino.credential.CredentialProvider` interface.
The corresponding JARs should be placed into the class paths
for the Iceberg catalog server or the Hadoop catalog.

## Deployment

Besides setting credentials related configuration, please download Gravitino cloud bundle JAR
and place it into the CLASSPATH for the Iceberg REST server or Hadoop catalog.

For Hadoop catalog, please use the *Gravitino cloud bundle JAR with Hadoop and cloud packages*.
Gravitino provides packages for
the [AWS](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle),
the [Aliyun](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle),
the [GCP](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle),
and the [Azure](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle)
cloud platforms.

For Iceberg REST catalog server, please use _the Gravitino cloud bundle JAR without Hadoop and cloud packages_.
In addition to this, you need to download the corresponding Iceberg cloud packages, i.e.

- The [AWS bundle JAR](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws)
  without Hadoop and cloud packages.
- The [Aliyun bundle JAR](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun)
  without Hadoop and cloud packages.
- The [GCP bundle JAR](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp)
  without Hadoop and cloud packages.
- The [Azure bundle JAR](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure)
  without Hadoop and cloud packages.

:::note
For OSS, Iceberg doesn't provide Iceberg Aliyun bundle JAR which contains OSS packages.
You can provision the OSS JAR by yourself or use the Gravitino Aliyun bundle JAR with Hadoop
and cloud packages from the [maven repository](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle).
Please refer to [OSS configuration](../admin/iceberg-server.md#oss-configuration)
for more details.
:::

The class paths for the servers:

- For the Iceberg REST server, the class paths differs in different deploy modes.
  Please refer to [Iceberg server management](../admin/iceberg-server.md#server-management)
  for more details.
- For the Hadoop catalog, the class path is `catalogs/hadoop/libs/`.

## Usage example

### Credential vending for Iceberg REST server

Suppose the Iceberg table data is stored in S3, follow the steps below:

1. Download the Gravitino AWS bundle JAR without hadoop packages
   from [maven repository](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws),
   and place it to the classpath of Iceberg REST server.

1. Add s3 token credential configurations.

   ```
   gravitino.iceberg-rest.warehouse = s3://{bucket_name}/{warehouse_path}
   gravitino.iceberg-rest.io-impl= org.apache.iceberg.aws.s3.S3FileIO
   gravitino.iceberg-rest.credential-providers = s3-token
   gravitino.iceberg-rest.s3-access-key-id = xxx
   gravitino.iceberg-rest.s3-secret-access-key = xxx
   gravitino.iceberg-rest.s3-region = {region_name}
   gravitino.iceberg-rest.s3-role-arn = {role_arn}
   ```

1. Explore the Iceberg table with Spark client with credential vending enabled.

   ```shell
   ./bin/spark-sql -v \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1 \
     --conf spark.jars={path}/iceberg-aws-bundle-1.5.2.jar \
     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
     --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog  \
     --conf spark.sql.catalog.rest.type=rest  \
     --conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/ \
     --conf spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation=vended-credentials
   ```
