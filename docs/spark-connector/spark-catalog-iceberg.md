---
title: "Spark Connector: Iceberg Catalog"
slug: "/spark-connector/spark-catalog-iceberg"
keyword: "spark connector iceberg catalog"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

The Apache Gravitino Spark connector offers the capability to read and write Iceberg tables, with the metadata managed by the Gravitino server.

## Preparation

1. Set `spark.sql.gravitino.enableIcebergSupport` to `true` in Spark configuration.
2. Download the Iceberg Spark runtime JAR and the Gravitino Spark connector runtime JAR that match your Spark minor version and Scala version, and place them in the Spark classpath.

Spark clients use a different Iceberg version than the Gravitino server (1.11.0). Use the table below to choose the correct JARs for your Spark version.

| Spark version | Scala          | Iceberg version | Iceberg client runtime artifact                         | Gravitino connector runtime artifact                                              |
|---------------|----------------|-----------------|---------------------------------------------------------|-----------------------------------------------------------------------------------|
| 3.3           | 2.12 or 2.13   | 1.8.1           | `iceberg-spark-runtime-3.3_${scala-version}-1.8.1.jar`  | `gravitino-spark-connector-runtime-3.3_${scala-version}-${gravitino-version}.jar` |
| 3.4           | 2.12 or 2.13   | 1.11.0          | `iceberg-spark-runtime-3.4_${scala-version}-1.11.0.jar` | `gravitino-spark-connector-runtime-3.4_${scala-version}-${gravitino-version}.jar` |
| 3.5           | 2.12 or 2.13   | 1.11.0          | `iceberg-spark-runtime-3.5_${scala-version}-1.11.0.jar` | `gravitino-spark-connector-runtime-3.5_${scala-version}-${gravitino-version}.jar` |

Replace `${scala-version}` with `2.12` or `2.13`, and `${gravitino-version}` with your Gravitino release version.

:::caution
Use only the JARs from the matching table row. Mixing Iceberg JARs from different versions on the client classpath is not compatible and may cause runtime errors.
:::

## Capabilities

### DML and DDL Operations

- `CREATE TABLE`

Doesn't support distribution and sort orders.

- `DROP TABLE`
- `ALTER TABLE`
- `INSERT INTO&OVERWRITE`
- `SELECT`
- `MERGE INTO`
- `DELETE FROM`
- `UPDATE`
- `CALL`
- `TIME TRAVEL QUERY`
- `DESCRIBE TABLE`

### Unsupported Operations

- View operations.
- Metadata tables, like:
  - `{iceberg_catalog}.{iceberg_database}.{iceberg_table}.snapshots`
- Other Iceberg extension SQLs, like:
  - `ALTER TABLE prod.db.sample ADD PARTITION FIELD xx`
  - `ALTER TABLE ... WRITE ORDERED BY`
  - `ALTER TABLE prod.db.sample CREATE BRANCH branchName`
  - `ALTER TABLE prod.db.sample CREATE TAG tagName`
- AtomicCreateTableAsSelect&AtomicReplaceTableAsSelect

## SQL Example

```sql
-- Suppose iceberg_a is the Iceberg catalog name managed by Gravitino
USE iceberg_a;

CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employee (
  id bigint,
  name string,
  department string,
  hire_date timestamp
) USING iceberg
PARTITIONED BY (days(hire_date));
DESC TABLE EXTENDED employee;

INSERT INTO employee
VALUES
(1, 'Alice', 'Engineering', TIMESTAMP '2021-01-01 09:00:00'),
(2, 'Bob', 'Marketing', TIMESTAMP '2021-02-01 10:30:00'),
(3, 'Charlie', 'Sales', TIMESTAMP '2021-03-01 08:45:00');

SELECT * FROM employee WHERE date(hire_date) = '2021-01-01';

UPDATE employee SET department = 'Jenny' WHERE id = 1;

DELETE FROM employee WHERE id < 2;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO employee
USING (SELECT 4 as id, 'David' as name, 'Engineering' as department, TIMESTAMP '2021-04-01 09:00:00' as hire_date) as new_employee
ON employee.id = new_employee.id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT *;

-- Suppose that the first snapshotId of employee is 1L and the second snapshotId is 2L
-- Rollback the snapshot for iceberg_a.mydatabase.employee to 1L
CALL iceberg_a.system.rollback_to_snapshot('iceberg_a.mydatabase.employee', 1);
-- Set the snapshot for iceberg_a.mydatabase.employee to 2L
CALL iceberg_a.system.set_current_snapshot('iceberg_a.mydatabase.employee', 2);

-- Suppose that the commit timestamp of the first snapshot is older than '2024-05-27 01:01:00'
-- Time travel to '2024-05-27 01:01:00'
SELECT * FROM employee TIMESTAMP AS OF '2024-05-27 01:01:00';
SELECT * FROM employee FOR SYSTEM_TIME AS OF '2024-05-27 01:01:00';

-- Show the details of employee, such as schema and reserved properties(like location, current-snapshot-id, provider, format, format-version, etc)
DESC EXTENDED employee;
```

For more details about `CALL`, refer to the [Spark Procedures description](https://iceberg.apache.org/docs/1.5.2/spark-procedures/#spark-procedures) in Iceberg official document.

## Catalog Properties

Gravitino spark connector will transform below property names which are defined in catalog properties to Spark Iceberg connector configuration.

| Gravitino catalog property name | Spark Iceberg connector configuration | Description                                                                                                                                                                                                         |
|---------------------------------|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `catalog-backend`               | `type`                                | Catalog backend type.Supports `hive` or `jdbc` or `rest` or `custom`                                                                                                                                                |
| `catalog-backend-impl`          | `catalog-impl`                        | The fully-qualified class name of a custom catalog implementation, only worked if `catalog-backend` is `custom`                                                                                                     |
| `uri`                           | `uri`                                 | Catalog backend uri                                                                                                                                                                                                 |
| `warehouse`                     | `warehouse`                           | Catalog backend warehouse                                                                                                                                                                                           |
| `jdbc-user`                     | `jdbc.user`                           | JDBC user name                                                                                                                                                                                                      |
| `jdbc-password`                 | `jdbc.password`                       | JDBC password                                                                                                                                                                                                       |
| `io-impl`                       | `io-impl`                             | The io implementation for `FileIO` in Iceberg.                                                                                                                                                                      |
| `s3-endpoint`                   | `s3.endpoint`                         | An alternative endpoint of the S3 service, This could be used for S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. |
| `s3-region`                     | `client.region`                       | The region of the S3 service, like `us-west-2`.                                                                                                                                                                     |
| `s3-access-key-id`              | `s3.access-key-id`                    | The static access key ID used to access S3 data.                                                                                                                                                                    |
| `s3-secret-access-key`          | `s3.secret-access-key`                | The static secret access key used to access S3 data.                                                                                                                                                                |
| `s3-path-style-access`          | `s3.path-style-access`                | Whether to use path style access for S3.                                                                                                                                                                            |
| `oss-endpoint`                  | `oss.endpoint`                        | The endpoint of Aliyun OSS service.                                                                                                                                                                                 |
| `oss-access-key-id`             | `client.access-key-id`                | The static access key ID used to access OSS data.                                                                                                                                                                   |
| `oss-secret-access-key`         | `client.access-key-secret`            | The static secret access key used to access OSS data.                                                                                                                                                               |
| `azure-storage-account-name`    | `adls.auth.shared-key.account.name`   | The static storage account name used to access ADLS data.                                                                                                                                                           |
| `azure-storage-account-key`     | `adls.auth.shared-key.account.key`    | The static storage account key used to access ADLS data..                                                                                                                                                           |

Gravitino catalog property names with the prefix `spark.bypass.` are passed to Spark Iceberg connector. For example, using `spark.bypass.clients` to pass the `clients` to the Spark Iceberg connector.

:::info
Iceberg catalog property `cache-enabled` is setting to `false` internally and not allowed to change.
:::

## Routing Through the Gravitino Iceberg REST Server

If the Gravitino server exposes an [Iceberg REST catalog](../iceberg-rest-service.md) (IRC) endpoint for the
current metalake, the Spark connector automatically routes `hive` and `jdbc` backed Iceberg catalogs through
that endpoint instead of talking to the Hive metastore or JDBC database directly. This has no effect on
catalogs whose `catalog-backend` is already `rest` or `custom`.

Routing through the IRC server is the only way to receive short-lived, per-table **vended credentials**
that Iceberg's native REST protocol refreshes automatically. The non-REST path can still inject a single
vended credential fetched once at catalog initialization (see
[Credential vending](../security/credential-vending.md)), but it is not refreshed per table access.

REST routing is enabled by default. The endpoint is discovered once per Spark application — for the life
of the Gravitino Spark plugin, not per `SparkSession` — and is not re-checked afterward.

- If no discoverable endpoint is found (for example, the `iceberg-rest` auxiliary service is disabled or
  not configured with `catalog-config-provider=dynamic-config-provider`) and routing was left at its
  default, the connector falls back to the native Hive/JDBC backend and logs a warning. Set
  `spark.sql.gravitino.iceberg.rest-routing-enabled=true` explicitly to require Iceberg REST routing and
  fail catalog initialization instead.
- A catalog whose warehouse uses a scheme with a native Iceberg FileIO (`s3://`, `gs://`, `abfs://`, etc.)
  must have [credential vending](../security/credential-vending.md) configured (`credential-providers`)
  before it can be routed: routing replaces any static storage credentials for that FileIO with vended
  ones, and without credential vending the catalog would lose storage access. Catalog initialization
  fails with an actionable error if this is not configured. Set `rest-routing-enabled=false` for that
  catalog to keep using the legacy Hive/JDBC backend instead.

To force a specific endpoint instead of relying on auto-discovery, set:

```properties
spark.sql.gravitino.iceberg.rest-uri    http://<gravitino-host>:9001/iceberg
```

To retain the legacy Hive/JDBC translation and skip endpoint discovery, disable routing explicitly:

```properties
spark.sql.gravitino.iceberg.rest-routing-enabled    false
```

If Gravitino requires authentication on the IRC endpoint, pass the Iceberg REST client's own auth
properties using the `spark.sql.gravitino.iceberg.rest.` prefix. For example, for Basic authentication:

```properties
spark.sql.gravitino.iceberg.rest.rest.auth.type            basic
spark.sql.gravitino.iceberg.rest.rest.auth.basic.username  <username>
spark.sql.gravitino.iceberg.rest.rest.auth.basic.password  <password>
```

See [Connect Spark to Iceberg REST](../iceberg-rest-engine/spark.md) for the full set of supported
`rest.auth.*` properties and how to configure them when connecting directly to the IRC endpoint.

When the Gravitino client uses OAuth2, the connector reuses its OAuth2 client configuration for IRC by
default. This avoids duplicating configuration when both endpoints accept the same client identity. The
equivalent explicit setting is:

```properties
spark.sql.gravitino.iceberg.reuseOAuth2    true
```

Gravitino and IRC may use different OAuth2 clients even when IRC runs as a Gravitino auxiliary service.
Override any reused value with an IRC-specific Iceberg REST property; unspecified values continue to come
from the Gravitino client configuration:

```properties
# Gravitino metadata API client
spark.sql.gravitino.authType                  oauth2
spark.sql.gravitino.oauth2.serverUri           https://identity.example.com
spark.sql.gravitino.oauth2.tokenPath           /oauth/token
spark.sql.gravitino.oauth2.credential          <gravitino-client-id>:<gravitino-client-secret>
spark.sql.gravitino.oauth2.scope               gravitino

# IRC data-plane client override
spark.sql.gravitino.iceberg.rest.credential   <irc-client-id>:<irc-client-secret>
spark.sql.gravitino.iceberg.rest.scope        iceberg
```

The IRC properties take precedence field by field. This validation also applies when reusing the Gravitino
configuration by default with no IRC-specific override at all: if the reused configuration itself is
incomplete, catalog initialization fails and identifies the missing properties. Set
`spark.sql.gravitino.iceberg.reuseOAuth2=false` when supplying a complete, independent IRC authentication
configuration or when IRC does not require OAuth2.

:::caution
Spark's UI redacts environment values whose property name matches `secret|password|token`, which does not
match `credential`. Both `spark.sql.gravitino.oauth2.credential` and `spark.sql.gravitino.iceberg.rest.credential`
are shown in plain text on the Spark UI's environment page; set `spark.redaction.regex` to also match
`credential` if this is a concern.
:::

Because vended credentials are only consumed by Iceberg's native `FileIO` implementations, make sure the
warehouse storage jars listed under [Storage](#storage) below are on the Spark classpath; the connector
derives `io-impl` automatically from the warehouse location's scheme (`s3`/`s3a`/`s3n`, `gs`,
`abfs`/`abfss`/`wasb`/`wasbs`, `oss`) unless `io-impl` is already set explicitly on the catalog.

## Storage

Spark connector could convert storage properties in the Gravitino catalog to Spark Iceberg connector automatically, No extra configuration is needed for `S3`, `ADLS`, `OSS`, `GCS`.

### S3

Download the [Iceberg AWS bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle)
that matches the Iceberg runtime version and place it on the Spark driver and executor classpaths. This is
required for `S3FileIO` even when the Spark image already includes the AWS SDK v1 used by Hadoop S3A;
`S3FileIO` uses AWS SDK v2 from `iceberg-aws-bundle`. If the bundle is absent, initialization can fail with
a `NoClassDefFoundError` that does not identify the missing bundle directly.

### OSS

Please downloading the [Aliyun OSS SDK](https://gosspublic.alicdn.com/sdks/java/aliyun_java_sdk_3.10.2.zip) and copy `aliyun-sdk-oss-3.10.2.jar`, `hamcrest-core-1.1.jar`, `jdom2-2.0.6.jar` in the classpath of Spark.

### GCS

Please make sure the credential file is accessible by Spark, like using `export GOOGLE_APPLICATION_CREDENTIALS=/xx/application_default_credentials.json`, and download [Iceberg GCP bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-gcp-bundle) and place it to the classpath of Spark.

### ADLS

Please downloading the [Iceberg Azure bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-azure-bundle) and place it in the classpath of Spark.

### Other Storage

Add custom configurations with the format `spark.sql.catalog.${iceberg_catalog_name}.{configuration_key}`. Additionally, place corresponding jars which implement `FileIO` in the classpath of Spark.
