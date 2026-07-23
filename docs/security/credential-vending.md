---
title: "Credential Vending"
slug: "/security/credential-vending"
keyword: "security credential vending"
license: "This software is licensed under the Apache License version 2."
---

## Background

Gravitino credential vending is used to generate temporary or static credentials for accessing data. With credential vending, Gravitino provides a unified way to control access to diverse data sources across different platforms.

## Supported Catalogs

| Catalog type | Vends                           |
|--------------|---------------------------------|
| Fileset      | S3, OSS, GCS, ADLS              |
| Hive         | S3, OSS, GCS, ADLS              |
| Iceberg      | S3, OSS, GCS, ADLS              |
| Glue         | S3                              |
| JDBC         | JDBC user and password          |
| Paimon       | S3, OSS, JDBC user and password |

S3 is Amazon S3, OSS is Alibaba Cloud OSS, GCS is Google Cloud Storage, and ADLS is Azure Data Lake Storage. The Gravitino Spark, Flink, and Trino connectors consume vended credentials automatically for these catalogs.

## Quick Start

Vend scoped S3 credentials to Spark through the IRC. Create the catalog through the Gravitino REST catalog API:

```shell
curl -X POST http://localhost:8090/api/metalakes/{metalake}/catalogs \
-H "Content-Type: application/json" \
-d '{
  "name": "iceberg_catalog",
  "type": "RELATIONAL",
  "provider": "lakehouse-iceberg",
  "properties": {
    "catalog-backend": "jdbc",
    "uri": "jdbc:postgresql://{postgres_host}:5432/{database}",
    "jdbc-driver": "org.postgresql.Driver",
    "jdbc-user": "{jdbc_user}",
    "jdbc-password": "{jdbc_password}",
    "jdbc-initialize": "true",
    "warehouse": "s3://{bucket_name}/{warehouse_path}",
    "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "credential-providers": "s3-token",
    "s3-access-key-id": "{access_key_id}",
    "s3-secret-access-key": "{secret_access_key}",
    "s3-region": "{region_name}",
    "s3-role-arn": "{role_arn}"
  }
}'
```

Point Spark at the IRC and request vended credentials with the delegation header:

```shell
./bin/spark-sql -v \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.rest.type=rest \
--conf spark.sql.catalog.rest.uri=http://127.0.0.1:9001/iceberg/ \
--conf spark.sql.catalog.rest.prefix=iceberg_catalog \
--conf spark.sql.catalog.rest.header.X-Iceberg-Access-Delegation=vended-credentials
```

The role in `s3-role-arn` needs a trust policy and S3 permissions before this works. See [`s3-token`](#s3-token).

For Trino instead of Spark:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://127.0.0.1:9001/iceberg/
iceberg.rest-catalog.prefix=iceberg_catalog
iceberg.rest-catalog.vended-credentials-enabled=true
fs.native-s3.enabled=true
s3.region={region_name}
```

For the full Trino setup, see [Connect Trino to the IRC](../iceberg-rest-engine/trino.md).

## Setting Properties

Credential vending properties go in the catalog's `properties` map when you create it through the Gravitino REST catalog API, alongside the warehouse location and the other catalog settings.

The Gravitino Iceberg REST Catalog (IRC) is the exception. It can also read a catalog straight from `gravitino.conf`, using the same property names prefixed with `gravitino.iceberg-rest.`:

```properties
s3-role-arn                             # as a catalog property
gravitino.iceberg-rest.s3-role-arn      # in gravitino.conf
```

Catalogs defined in `gravitino.conf` are not registered in a metalake, so Gravitino access control does not apply to them. Privileges are granted on catalogs in a metalake, and there is nothing to grant them on.

## General Configurations

| Property                        | Description                                                                                                                       | Default value | Required             |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|---------------|----------------------|
| `credential-providers`          | The credential provider types, separated by comma. If omitted, Gravitino infers some providers from the other properties present. | (none)        | Yes, unless inferred |
| `credential-cache-expire-ratio` | Ratio of the credential's expiration time when Gravitino removes the credential from the cache.                                   | 0.15          | No                   |
| `credential-cache-max-size`     | Max size for the credential cache.                                                                                                | 10000         | No                   |

### Values for `credential-providers`

| Value                | Storage | Vends                                                      |
|----------------------|---------|------------------------------------------------------------|
| `s3-token`           | S3      | Temporary STS credentials, scoped to the table path        |
| `aws-irsa`           | S3      | Credentials from an IAM role for service accounts, for EKS |
| `s3-secret-key`      | S3      | The configured static access key and secret                |
| `oss-token`          | OSS     | Temporary STS credentials, scoped to the table path        |
| `oss-secret-key`     | OSS     | The configured static access key and secret                |
| `adls-token`         | ADLS    | A user delegation SAS token                                |
| `azure-account-key`  | ADLS    | The configured static storage account key                  |
| `gcs-token`          | GCS     | A downscoped access token                                  |
| `jdbc-user-password` | JDBC    | The configured JDBC username and password                  |

Each value has its own properties, listed in the sections below. To vend for more than one storage type on a catalog, separate values with a comma. Custom providers can be added by implementing `CredentialProvider`, described under [Custom Credentials](#custom-credentials).

### When `credential-providers` Is Omitted

If a catalog does not set `credential-providers`, Gravitino infers providers from the credential properties present:

| Properties present                                           | Provider enabled    |
|--------------------------------------------------------------|---------------------|
| `s3-access-key-id` and `s3-secret-access-key`                | `s3-secret-key`     |
| `oss-access-key-id` and `oss-secret-access-key`              | `oss-secret-key`    |
| `azure-storage-account-name` and `azure-storage-account-key` | `azure-account-key` |
| `gcs-service-account-file`                                   | `gcs-token`         |

JDBC catalogs additionally infer `jdbc-user-password` from `jdbc-user` and `jdbc-password`.

Four providers have no inference rule and must always be set explicitly: `s3-token`, `oss-token`, `adls-token`, and `aws-irsa`. In particular, setting `s3-role-arn` without `credential-providers` does not enable `s3-token`. The catalog falls back to `s3-secret-key` and vends the static access key instead, which is long-lived and not scoped to the table path. Set `credential-providers` explicitly whenever you want token-based vending.

## S3

### `s3-token`

Gravitino calls STS [AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html) and returns temporary credentials scoped to the table path.

| Property                    | Description                                                                                    | Default value | Required |
|-----------------------------|------------------------------------------------------------------------------------------------|---------------|----------|
| `s3-role-arn`               | ARN of the role Gravitino assumes, in the form `arn:aws:iam::{account_id}:role/{role_name}`.   | (none)        | Yes      |
| `s3-region`                 | Region of the S3 service, like `us-west-2`.                                                    | (none)        | No       |
| `s3-token-expire-in-secs`   | Session lifetime of the vended credentials. Cannot exceed the role's maximum session duration. | 3600          | No       |
| `s3-external-id`            | External ID passed on AssumeRole, for cross-account trust policies that require one.           | (none)        | No       |
| `s3-token-service-endpoint` | Alternative STS endpoint, for S3-compatible storage such as MinIO.                             | (none)        | No       |

Also set `s3-access-key-id` and `s3-secret-access-key`. Gravitino uses them to call AssumeRole, not to reach data, and they are never sent to the engine.

#### Trust Policy on the Role

The role in `s3-role-arn` must allow the `s3-access-key-id` principal to assume it. Without this, AssumeRole is rejected and no credential is vended.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::{account_id}:user/{gravitino_user}" },
    "Action": "sts:AssumeRole"
  }]
}
```

#### Permission Policy on the Role

The vended credentials inherit this policy, narrowed to the table path. Without S3 access to the warehouse prefix, the credentials are vended but cannot read or write.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::{bucket_name}/{warehouse_path}/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::{bucket_name}"
    }
  ]
}
```

### `aws-irsa`

For Gravitino running on EKS. Instead of an access key, Gravitino uses its pod's IAM role to call AssumeRole, so no static keys exist anywhere in the setup.

| Property                    | Description                                                                                    | Default value | Required                    |
|-----------------------------|------------------------------------------------------------------------------------------------|---------------|-----------------------------|
| `s3-role-arn`               | ARN of the role to assume, in the form `arn:aws:iam::{account_id}:role/{role_name}`.           | (none)        | For path-scoped credentials |
| `s3-region`                 | AWS region for STS operations.                                                                 | (none)        | No                          |
| `s3-token-expire-in-secs`   | Session lifetime of the vended credentials. Cannot exceed the role's maximum session duration. | 3600          | No                          |
| `s3-token-service-endpoint` | Alternative STS endpoint, for S3-compatible storage.                                           | (none)        | No                          |

Set `s3-role-arn` to get credentials scoped to the table path, with an IAM policy generated per table covering its data, metadata, and write locations. Without it, the vended credentials carry the full permissions of the pod's role.

The role in `s3-role-arn` needs the same two policies as `s3-token` above, except the trust policy names the pod's IAM role rather than an IAM user.

IRSA itself must already be configured on the pod's Kubernetes service account. When it is, EKS injects a signed service account token into the pod and sets `AWS_WEB_IDENTITY_TOKEN_FILE` to its path, which is what the AWS SDK uses to obtain credentials. If vending fails, check that this variable is present in the pod.

### `s3-secret-key`

Returns the catalog's configured access key and secret to the client, unchanged, in the `loadTable` response.

The key is long-lived, carries whatever permissions its IAM user has, and is not scoped to the table path. Any client that can load a table receives it, and it stays valid after the query finishes. Prefer `s3-token`, which returns temporary credentials scoped to the table path. Use `s3-secret-key` to confirm the vending path works before configuring a role.

| Property               | Description                                          | Default value | Required |
|------------------------|------------------------------------------------------|---------------|----------|
| `s3-access-key-id`     | The static access key ID used to access S3 data.     | (none)        | Yes      |
| `s3-secret-access-key` | The static secret access key used to access S3 data. | (none)        | Yes      |

## OSS

### `oss-token`

Gravitino calls Alibaba Cloud STS [AssumeRole](https://www.alibabacloud.com/help/en/oss/developer-reference/use-temporary-access-credentials-provided-by-sts-to-access-oss) and returns temporary credentials scoped to the table path.

Also set `oss-access-key-id` and `oss-secret-access-key`. Gravitino uses them to call AssumeRole, not to reach data, and they are never sent to the engine.

| Property                   | Description                                                                                                  | Default value | Required |
|----------------------------|--------------------------------------------------------------------------------------------------------------|---------------|----------|
| `oss-access-key-id`        | The static access key ID used to access OSS data.                                                            | (none)        | Yes      |
| `oss-secret-access-key`    | The static secret access key used to access OSS data.                                                        | (none)        | Yes      |
| `oss-role-arn`             | The ARN of the role to access the OSS data.                                                                  | (none)        | Yes      |
| `oss-region`               | The region of the OSS service, like `oss-cn-hangzhou`, only used when `credential-providers` is `oss-token`. | (none)        | No       |
| `oss-external-id`          | The OSS external id to generate the token.                                                                   | (none)        | No       |
| `oss-token-expire-in-secs` | The OSS security token expire time in secs.                                                                  | 3600          | No       |

#### Trust Policy on the RAM Role

The role in `oss-role-arn` must allow the `oss-access-key-id` principal to assume it.

```json
{
  "Version": "1",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sts:AssumeRole",
    "Principal": { "RAM": ["acs:ram::{account_id}:user/{gravitino_user}"] }
  }]
}
```

#### Permission Policy on the RAM Role

The vended credentials inherit this policy, narrowed to the table path.

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["oss:GetObject", "oss:PutObject", "oss:DeleteObject"],
      "Resource": "acs:oss:*:*:{bucket_name}/{warehouse_path}/*"
    },
    {
      "Effect": "Allow",
      "Action": ["oss:ListObjects", "oss:GetBucketInfo"],
      "Resource": "acs:oss:*:*:{bucket_name}"
    }
  ]
}
```

### `oss-secret-key`

Returns the catalog's configured access key and secret to the client, unchanged.

The key is long-lived, carries whatever permissions its RAM user has, and is not scoped to the table path. Any client that can load a table receives it, and it stays valid after the query finishes. Prefer `oss-token`. Use `oss-secret-key` to confirm the vending path works before configuring a role.

| Property                | Description                                           | Default value | Required |
|-------------------------|-------------------------------------------------------|---------------|----------|
| `oss-access-key-id`     | The static access key ID used to access OSS data.     | (none)        | Yes      |
| `oss-secret-access-key` | The static secret access key used to access OSS data. | (none)        | Yes      |

## ADLS

### `adls-token`

Gravitino requests an Azure [user delegation SAS](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas) and returns it to the client, scoped to the table path.

Azure grants access through role assignments rather than policy documents. The Microsoft Entra ID service principal identified by `azure-tenant-id`, `azure-client-id`, and `azure-client-secret` needs two roles:

- **Storage Blob Delegator**, assigned at the storage account, which allows it to request the user delegation key that signs the SAS.
- **Storage Blob Data Contributor**, assigned on the container or the warehouse path, which determines what the vended SAS can do. Use **Storage Blob Data Reader** for read-only access.

Without the delegator role the SAS cannot be issued at all. Without a data role the SAS is issued but grants nothing.

| Property                     | Description                                                         | Default value | Required |
|------------------------------|---------------------------------------------------------------------|---------------|----------|
| `azure-storage-account-name` | The static storage account name used to access ADLS data.           | (none)        | Yes      |
| `azure-tenant-id`            | Azure Active Directory (AAD) tenant ID.                             | (none)        | Yes      |
| `azure-client-id`            | Azure Active Directory (AAD) client ID used for authentication.     | (none)        | Yes      |
| `azure-client-secret`        | Azure Active Directory (AAD) client secret used for authentication. | (none)        | Yes      |
| `adls-token-expire-in-secs`  | The ADLS SAS token expire time in secs.                             | 3600          | No       |

### `azure-account-key`

Returns the catalog's configured storage account key to the client, unchanged.

A storage account key grants full access to every container in the storage account, not just the warehouse path, and it does not expire. Any client that can load a table receives it. Prefer `adls-token`, which is scoped and time-limited. Use `azure-account-key` to confirm the vending path works before configuring a service principal.

| Property                     | Description                                               | Default value | Required |
|------------------------------|-----------------------------------------------------------|---------------|----------|
| `azure-storage-account-name` | The static storage account name used to access ADLS data. | (none)        | Yes      |
| `azure-storage-account-key`  | The static storage account key used to access ADLS data.  | (none)        | Yes      |

## GCS

### `gcs-token`

Gravitino downscopes its own credentials using GCS [credential access boundaries](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials) and returns a token scoped to the table path.

There is no role to assume. The identity is the service account in `gcs-service-account-file`, or the application default credentials when that is unset. Grant that service account **Storage Object User** (`roles/storage.objectUser`) on the warehouse bucket, or **Storage Object Viewer** for read-only access. Downscoping narrows from those permissions, so the vended token can never exceed what the service account itself holds.

| Property                   | Description                              | Default value                       | Required |
|----------------------------|------------------------------------------|-------------------------------------|----------|
| `gcs-service-account-file` | The location of the GCS credential file. | GCS Application default credential. | No       |

For the IRC, ensure that the credential file is accessible by that server. For example, the server may be running on a GCE machine, or you may set the environment variable `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json` even when `gcs-service-account-file` is already configured.

## Requesting Vended Credentials

How a client asks depends on which interface it uses.

### Over the IRC

Credentials are vended only when the client asks for them. Spark, Flink, and other IRC clients ask with a header:

```
X-Iceberg-Access-Delegation: vended-credentials
```

In Spark, set it as a catalog config key:

```properties
spark.sql.catalog.{name}.header.X-Iceberg-Access-Delegation=vended-credentials
```

Trino asks with a catalog property instead, and sends the header for you:

```properties
iceberg.rest-catalog.vended-credentials-enabled=true
```

### Over the Gravitino REST Catalog API

Hive, Glue, JDBC, Paimon, and Fileset catalogs are reached through the Gravitino REST catalog API, which has no delegation header. Credential properties are hidden from the catalog GET response, so clients fetch them from the Gravitino credential endpoint instead. It works for any metadata object:

```
GET /api/metalakes/{metalake}/objects/{type}/{full_name}/credentials
```

For a catalog, `{type}` is `catalog` and `{full_name}` is the catalog name:

```
GET /api/metalakes/{metalake}/objects/catalog/{catalog}/credentials
```

The Gravitino Spark and Flink connectors call this for you and inject the returned credentials, so no client configuration is needed.

## Custom Credentials

Gravitino supports custom credentials. You can implement the `org.apache.gravitino.credential.CredentialProvider` interface to support custom credentials, and place the corresponding jar in the classpath of the IRC or the Fileset catalog.

## Deployment

The credential provider implementations ship in separate jars. Whichever component vends the credentials needs the right jar on its classpath, or the provider cannot be created and no credentials are vended.

| Vending component | Jar                                                | Classpath                                                                              |
|-------------------|----------------------------------------------------|----------------------------------------------------------------------------------------|
| IRC               | `gravitino-iceberg-{cloud}-bundle`                 | See [Deployment](../iceberg-rest-service.md#deployment); it differs by deployment mode |
| Iceberg catalog   | `gravitino-iceberg-{cloud}-bundle`                 | `catalogs/lakehouse-iceberg/libs/`                                                     |
| Fileset catalog   | `gravitino-{cloud}-bundle`                         | `catalogs/fileset/libs/`                                                               |
| Hive catalog      | `gravitino-{cloud}`                                | `catalogs/hive/libs/`                                                                  |
| Glue catalog      | `gravitino-aws`                                    | `catalogs/glue/libs/`                                                                  |
| Paimon catalog    | `gravitino-aws` for S3, `gravitino-aliyun` for OSS | `catalogs/lakehouse-paimon/libs/`                                                      |

Substitute `{cloud}` with `aws`, `gcp`, `aliyun`, or `azure`. Note the two jar families: the `-bundle` variants also carry Hadoop and cloud SDK packages, which the Fileset catalog and the IRC need. The Hive, Glue, and Paimon catalogs only vend credentials, so they take the plain `gravitino-{cloud}` jar.

Since Gravitino 1.1.0, the Gravitino Iceberg cloud bundle jars already include the Iceberg cloud bundle jars, so there is no need to download and include those separately.

Vending JDBC user and password requires no additional jar.

Bundle jars on Maven Central:

- [gravitino-aws-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws-bundle), [gravitino-gcp-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-gcp-bundle), [gravitino-aliyun-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun-bundle), [gravitino-azure-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-azure-bundle)
- [gravitino-iceberg-aws-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aws-bundle), [gravitino-iceberg-gcp-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-gcp-bundle), [gravitino-iceberg-aliyun-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-aliyun-bundle), [gravitino-iceberg-azure-bundle](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-iceberg-azure-bundle)
- [gravitino-aws](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aws), [gravitino-aliyun](https://mvnrepository.com/artifact/org.apache.gravitino/gravitino-aliyun)

## Upgrading From a Release Earlier Than 1.3.0

Since 1.3.0, sensitive catalog properties such as `s3-access-key-id`, `s3-secret-access-key`, `jdbc-user`, and `jdbc-password` are excluded from `GET /api/metalakes/{metalake}/catalogs/{catalog}`. Clients written against earlier releases that read those properties directly lose access to them.

For a zero-downtime migration, set the following in `gravitino.conf`:

```properties
gravitino.catalog.credential.backfillToProperties = true
```

The Gravitino server then re-includes the hidden properties in its catalog GET responses. Turn it off once all clients use the Gravitino credential endpoint, since it exposes credentials in plaintext.
