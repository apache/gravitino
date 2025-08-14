---
title: "Gravitino credential vending"
slug: /security/credential-vending
keyword: security credential vending
license: "This software is licensed under the Apache License version 2."
---

## Background

Gravitino credential vending is used to generate temporary or static credentials for accessing data. With credential vending, Gravitino provides an unified way to control the access to diverse data sources in different platforms.

### Capabilities

- Supports Gravitino Iceberg REST server.
- Supports Gravitino server, only support Hadoop catalog.
- Supports pluggable credentials with build-in credentials:
  - S3: `S3TokenCredential`, `S3SecretKeyCredential`, `AwsIrsaCredential`
  - GCS: `GCSTokenCredential`
  - ADLS: `ADLSTokenCredential`, `AzureAccountKeyCredential`
  - OSS: `OSSTokenCredential`, `OSSSecretKeyCredential`
- No support for Spark/Trino/Flink connector yet.

## General configurations

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations           | Description                                                                                | Default value | Required | Since Version    |
|-------------------------------------|--------------------------------------------------------|--------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `credential-provider-type`          | `gravitino.iceberg-rest.credential-provider-type`      | Deprecated, please use `credential-providers` instead.                                     | (none)        | Yes      | 0.7.0-incubating |
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`          | The credential provider types, separated by comma.                                         | (none)        | Yes      | 0.8.0-incubating |
| `credential-cache-expire-ratio`     | `gravitino.iceberg-rest.credential-cache-expire-ratio` | Ratio of the credential's expiration time when Gravitino remove credential from the cache. | 0.15          | No       | 0.8.0-incubating |
| `credential-cache-max-size`         | `gravitino.iceberg-rest.cache-max-size`                | Max size for the credential cache.                                                         | 10000         | No       | 0.8.0-incubating |

## Build-in credentials configurations

### S3 credentials

#### S3 IRSA credential

A credential using AWS IAM Roles for Service Accounts (IRSA) to access S3 with temporary credentials, typically used in EKS environments. This provider supports both basic IRSA credentials and fine-grained path-based access control with dynamically generated IAM policies.

**Features:**
- **Basic IRSA mode**: Returns credentials with full permissions of the associated IAM role (for non-path-based contexts)
- **Fine-grained mode**: Generates path-specific credentials with minimal required permissions (for table access with `X-Iceberg-Access-Delegation: vended-credentials`)
- **Automatic policy generation**: Creates custom IAM policies scoped to specific table paths including data, metadata, and write locations
- **EKS integration**: Leverages existing IRSA setup while providing enhanced security through path-based restrictions


| Gravitino server catalog properties | Gravitino Iceberg REST server configurations       | Description                                                                                               | Default value | Required | Since Version |
|-------------------------------------|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------|---------------|----------|---------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`      | `aws-irsa` for AWS IRSA credential provider.                                                              | (none)        | Yes      | 1.0.0         |
| `s3-role-arn`                       | `gravitino.iceberg-rest.s3-role-arn`               | The ARN of the IAM role to assume. Required for fine-grained path-based access control.                   | (none)        | Yes*     | 1.0.0         |
| `s3-region`                         | `gravitino.iceberg-rest.s3-region`                 | The AWS region for STS operations. Used for fine-grained access control.                                  | (none)        | No       | 1.0.0         |
| `s3-token-expire-in-secs`           | `gravitino.iceberg-rest.s3-token-expire-in-secs`   | Token expiration time in seconds for fine-grained credentials. Cannot exceed role's max session duration. | 3600          | No       | 1.0.0         |
| `s3-token-service-endpoint`         | `gravitino.iceberg-rest.s3-token-service-endpoint` | Alternative STS endpoint for fine-grained credential generation. Useful for S3-compatible services.       | (none)        | No       | 1.0.0         |

**Note**: `s3-role-arn` is required only when using fine-grained path-based access control with vended credentials. For basic IRSA usage without path restrictions, only `credential-providers=aws-irsa` is needed.

**Prerequisites for fine-grained mode:**
- EKS cluster with IRSA properly configured
- `AWS_WEB_IDENTITY_TOKEN_FILE` environment variable pointing to the service account token
- IAM role with permissions to assume the target role specified in `s3-role-arn`
- Target IAM role with necessary S3 permissions for the data locations

#### S3 secret key credential

A credential with static S3 access key id and secret access key.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations      | Description                                            | Default value | Required | Since Version    |
|-------------------------------------|---------------------------------------------------|--------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`     | `s3-secret-key` for S3 secret key credential provider. | (none)        | Yes      | 0.8.0-incubating |
| `s3-access-key-id`                  | `gravitino.iceberg-rest.s3-access-key-id`         | The static access key ID used to access S3 data.       | (none)        | Yes      | 0.6.0-incubating |
| `s3-secret-access-key`              | `gravitino.iceberg-rest.s3-secret-access-key`     | The static secret access key used to access S3 data.   | (none)        | Yes      | 0.6.0-incubating |

#### S3 token credential

An S3 token is a token credential with scoped privileges, by leveraging STS [Assume Role](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html). To use an S3 token credential, you should create a role and grant it proper privileges.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations       | Description                                                                                                                                                 | Default value | Required | Since Version    |
|-------------------------------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`      | `s3-token` for S3 token credential provider.                                                                                                                | (none)        | Yes      | 0.8.0-incubating |
| `s3-access-key-id`                  | `gravitino.iceberg-rest.s3-access-key-id`          | The static access key ID used to access S3 data.                                                                                                            | (none)        | Yes      | 0.6.0-incubating |
| `s3-secret-access-key`              | `gravitino.iceberg-rest.s3-secret-access-key`      | The static secret access key used to access S3 data.                                                                                                        | (none)        | Yes      | 0.6.0-incubating |
| `s3-role-arn`                       | `gravitino.iceberg-rest.s3-role-arn`               | The ARN of the role to access the S3 data.                                                                                                                  | (none)        | Yes      | 0.7.0-incubating |
| `s3-region`                         | `gravitino.iceberg-rest.s3-region`                 | The region of the S3 service, like `us-west-2`.                                                                                                             | (none)        | No       | 0.6.0-incubating |
| `s3-external-id`                    | `gravitino.iceberg-rest.s3-external-id`            | The S3 external id to generate token.                                                                                                                       | (none)        | No       | 0.7.0-incubating |
| `s3-token-expire-in-secs`           | `gravitino.iceberg-rest.s3-token-expire-in-secs`   | The S3 session token expire time in secs, it couldn't exceed the max session time of the assumed role.                                                      | 3600          | No       | 0.7.0-incubating |
| `s3-token-service-endpoint`         | `gravitino.iceberg-rest.s3-token-service-endpoint` | An alternative endpoint of the S3 token service, This could be used with s3-compatible object storage service like MINIO that has a different STS endpoint. | (none)        | No       | 0.8.0-incubating |

### OSS credentials

#### OSS secret key credential

A credential with static OSS access key id and secret access key.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations      | Description                                                                   | Default value | Required | Since Version    |
|-------------------------------------|---------------------------------------------------|-------------------------------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`     | `oss-secret-key` for OSS secret credential.                                   | (none)        | Yes      | 0.8.0-incubating |
| `oss-access-key-id`                 | `gravitino.iceberg-rest.oss-access-key-id`        | The static access key ID used to access OSS data.                             | (none)        | Yes      | 0.7.0-incubating |
| `oss-secret-access-key`             | `gravitino.iceberg-rest.oss-secret-access-key`    | The static secret access key used to access OSS data.                         | (none)        | Yes      | 0.7.0-incubating |

#### OSS token credential

An OSS token is a token credential with scoped privileges, by leveraging STS [Assume Role](https://www.alibabacloud.com/help/en/oss/developer-reference/use-temporary-access-credentials-provided-by-sts-to-access-oss). To use an OSS token credential, you should create a role and grant it proper privileges.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations      | Description                                                                                                  | Default value | Required | Since Version    |
|-------------------------------------|---------------------------------------------------|--------------------------------------------------------------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`     | `oss-token` for s3 token credential.                                                                         | (none)        | Yes      | 0.8.0-incubating |
| `oss-access-key-id`                 | `gravitino.iceberg-rest.oss-access-key-id`        | The static access key ID used to access OSS data.                                                            | (none)        | Yes      | 0.7.0-incubating |
| `oss-secret-access-key`             | `gravitino.iceberg-rest.oss-secret-access-key`    | The static secret access key used to access OSS data.                                                        | (none)        | Yes      | 0.7.0-incubating |
| `oss-role-arn`                      | `gravitino.iceberg-rest.oss-role-arn`             | The ARN of the role to access the OSS data.                                                                  | (none)        | Yes      | 0.8.0-incubating |
| `oss-region`                        | `gravitino.iceberg-rest.oss-region`               | The region of the OSS service, like `oss-cn-hangzhou`, only used when `credential-providers` is `oss-token`. | (none)        | No       | 0.8.0-incubating |
| `oss-external-id`                   | `gravitino.iceberg-rest.oss-external-id`          | The OSS external id to generate token.                                                                       | (none)        | No       | 0.8.0-incubating |
| `oss-token-expire-in-secs`          | `gravitino.iceberg-rest.oss-token-expire-in-secs` | The OSS security token expire time in secs.                                                                  | 3600          | No       | 0.8.0-incubating |

### ADLS credentials

#### Azure account key credential

A credential with static Azure storage account name and key.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations        | Description                                               | Default value | Required | Since Version    |
|-------------------------------------|-----------------------------------------------------|-----------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`       | `azure-account-key` for Azure account key credential.     | (none)        | Yes      | 0.8.0-incubating |
| `azure-storage-account-name`        | `gravitino.iceberg-rest.azure-storage-account-name` | The static storage account name used to access ADLS data. | (none)        | Yes      | 0.8.0-incubating |
| `azure-storage-account-key`         | `gravitino.iceberg-rest.azure-storage-account-key`  | The static storage account key used to access ADLS data.  | (none)        | Yes      | 0.8.0-incubating |

#### ADLS token credential

An ADLS token is a token credential with scoped privileges, by leveraging Azure [User Delegation Sas](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas). To use an ADLS token credential, you should create a Microsoft Entra ID service principal and grant it proper privileges.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations        | Description                                                         | Default value | Required | Since Version    |
|-------------------------------------|-----------------------------------------------------|---------------------------------------------------------------------|---------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`       | `adls-token` for ADLS token credential.                             | (none)        | Yes      | 0.8.0-incubating |
| `azure-storage-account-name`        | `gravitino.iceberg-rest.azure-storage-account-name` | The static storage account name used to access ADLS data.           | (none)        | Yes      | 0.8.0-incubating |
| `azure-storage-account-key`         | `gravitino.iceberg-rest.azure-storage-account-key`  | The static storage account key used to access ADLS data.            | (none)        | Yes      | 0.8.0-incubating |
| `azure-tenant-id`                   | `gravitino.iceberg-rest.azure-tenant-id`            | Azure Active Directory (AAD) tenant ID.                             | (none)        | Yes      | 0.8.0-incubating |
| `azure-client-id`                   | `gravitino.iceberg-rest.azure-client-id`            | Azure Active Directory (AAD) client ID used for authentication.     | (none)        | Yes      | 0.8.0-incubating |
| `azure-client-secret`               | `gravitino.iceberg-rest.azure-client-secret`        | Azure Active Directory (AAD) client secret used for authentication. | (none)        | Yes      | 0.8.0-incubating |
| `adls-token-expire-in-secs`         | `gravitino.iceberg-rest.adls-token-expire-in-secs`  | The ADLS SAS token expire time in secs.                             | 3600          | No       | 0.8.0-incubating | 

### GCS credentials

#### GCS token credential

An GCS token is a token credential with scoped privileges, by leveraging GCS [Credential Access Boundaries](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials). To use an GCS token credential, you should create an GCS service account and grant it proper privileges.

| Gravitino server catalog properties | Gravitino Iceberg REST server configurations      | Description                                                | Default value                       | Required | Since Version    |
|-------------------------------------|---------------------------------------------------|------------------------------------------------------------|-------------------------------------|----------|------------------|
| `credential-providers`              | `gravitino.iceberg-rest.credential-providers`     | `gcs-token` for GCS token credential.                      | (none)                              | Yes      | 0.8.0-incubating |
| `gcs-credential-file-path`          | `gravitino.iceberg-rest.gcs-credential-file-path` | Deprecated, please use `gcs-service-account-file` instead. | GCS Application default credential. | No       | 0.7.0-incubating |
| `gcs-service-account-file`          | `gravitino.iceberg-rest.gcs-service-account-file` | The location of GCS credential file.                       | GCS Application default credential. | No       | 0.8.0-incubating |

:::note
For Gravitino Iceberg REST server, please ensure that the credential file can be accessed by the server. For example, if the server is running on a GCE machine, or you can set the environment variable as `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`, even when the `gcs-service-account-file` has already been configured.
:::

## Custom credentials

Gravitino supports custom credentials, you can implement the `org.apache.gravitino.credential.CredentialProvider` interface to support custom credentials, and place the corresponding jar to the classpath of Iceberg catalog server or Hadoop catalog.

## Deployment

Besides setting credentials related configuration, please download Gravitino cloud bundle jar and place it in the classpath of Iceberg REST server or Hadoop catalog.

For Hadoop catalog, please use Gravitino cloud bundle jar with Hadoop and cloud packages:

- [Gravitino AWS bundle jar with Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle)
- [Gravitino Aliyun bundle jar with Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle)
- [Gravitino GCP bundle jar with Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle)
- [Gravitino Azure bundle jar with Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle)

For Iceberg REST catalog server, please use the Gravitino cloud bundle jar without Hadoop and cloud packages. Additionally, download the corresponding Iceberg cloud packages.

- [Gravitino AWS bundle jar without Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws)
- [Gravitino Aliyun bundle jar without Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun)
- [Gravitino GCP bundle jar without Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp)
- [Gravitino Azure bundle jar without Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure)

:::note
For OSS, Iceberg doesn't provide Iceberg Aliyun bundle jar which contains OSS packages, you could provide the OSS jar by yourself or use [Gravitino Aliyun bundle jar with Hadoop and cloud packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle), please refer to [OSS configuration](../iceberg-rest-service.md#oss-configuration) for more details.
:::

The classpath of the server:

- Iceberg REST server: the classpath differs in different deploy mode, please refer to [Server management](../iceberg-rest-service.md#server-management) part.
- Hadoop catalog: `catalogs/hadoop/libs/`

## Usage example

### Credential vending for Iceberg REST server

Suppose the Iceberg table data is stored in S3, follow the steps below:

1. Download the [Gravitino AWS bundle jar without hadoop packages](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws), and place it to the classpath of Iceberg REST server.

2. Add s3 token credential configurations.

```
gravitino.iceberg-rest.warehouse = s3://{bucket_name}/{warehouse_path}
gravitino.iceberg-rest.io-impl= org.apache.iceberg.aws.s3.S3FileIO
gravitino.iceberg-rest.credential-providers = s3-token
gravitino.iceberg-rest.s3-access-key-id = xxx
gravitino.iceberg-rest.s3-secret-access-key = xxx
gravitino.iceberg-rest.s3-region = {region_name}
gravitino.iceberg-rest.s3-role-arn = {role_arn}
```

3. Exploring the Iceberg table with Spark client with credential vending enabled.

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
