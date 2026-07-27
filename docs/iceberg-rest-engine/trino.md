---
title: "Connect Trino to the Iceberg REST Catalog"
sidebar_label: "Trino"
---

## Introduction

Apache Gravitino exposes an Iceberg REST Catalog (IRC) endpoint that any Iceberg-compatible engine
can connect to directly, without installing a Gravitino-specific connector plugin. This page
describes how to configure Trino to use it.

## Quick Start

A complete Trino catalog file using vended credentials with OAuth2 authentication. Place it in
`etc/catalog/` and restart Trino. The sections below explain each part and the alternatives.

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://{gravitino_host}:9001/iceberg
iceberg.rest-catalog.prefix={catalog}

# Vended credentials
iceberg.rest-catalog.vended-credentials-enabled=true
fs.native-s3.enabled=true
s3.region={region_name}

# OAuth2
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential={client_id}:{client_secret}
iceberg.rest-catalog.oauth2.server-uri={oauth_server_uri}
iceberg.rest-catalog.oauth2.scope={scope}

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The IRC endpoint reachable from the Trino coordinator and all workers. The default port is `9001`.
- Trino 430 or later, required for the native S3 filesystem. Verified on Trino 478. Some features
  below need a higher version, noted where they apply.

## Configuration

Create a catalog properties file in your Trino `etc/catalog/` directory. The filename determines the
catalog name in Trino, so `gravitino_irc.properties` creates a catalog named `gravitino_irc`.

The Quick Start above is a complete working file. The rest of this page explains each part and the
alternatives. Storage credentials and authentication are independent choices, so pick one option from
each of the two sections below.

`iceberg.rest-catalog.prefix` selects which Gravitino catalog to use and must match the catalog name
in Gravitino. Confirm the expected value against the server:

```bash
curl -s "http://{gravitino_host}:9001/iceberg/v1/config?warehouse={catalog}"
```

Gravitino returns the prefix under `defaults`, so setting it explicitly in Trino matches rather than
overrides it.

The `warehouse` property is managed by the IRC and does not need to be set in the Trino catalog file.

## Storage Credentials

Trino needs credentials to read and write the underlying object storage. Choose one of the two
options below. Do not configure both.

### Vended Credentials

Used in the Quick Start above. Gravitino mints short-lived, path-scoped credentials at query time and
returns them to Trino, so no long-lived storage keys live in the Trino configuration. See
[Credential vending](../security/credential-vending.md) for the catalog-side configuration this
requires.

Do not set `s3.aws-access-key` or `s3.aws-secret-key` alongside vended credentials. When static keys
are present, Trino uses them and ignores the vended credentials. Queries still succeed, so the
catalog appears correctly configured while vending is not actually in use.

`fs.native-s3.enabled=true` is required. The native S3 filesystem performs the request signing that
consumes vended credentials. Without it, metadata operations succeed and data reads fail with
`ICEBERG_FILESYSTEM_ERROR`.

Trino implements vended credentials for S3 only. Vended credential support for GCS and Azure are
open Trino feature requests ([trinodb/trino#24518](https://github.com/trinodb/trino/issues/24518),
[trinodb/trino#23238](https://github.com/trinodb/trino/issues/23238)), so use static credentials for
those backends.

### Static Credentials

Configure storage keys directly in Trino. Simpler to set up, but the keys are long-lived, are not
scoped to a table path, and are managed outside Gravitino.

```properties
fs.native-s3.enabled=true
s3.region={region_name}
s3.aws-access-key={access_key_id}
s3.aws-secret-key={secret_access_key}
```

For local development against MinIO:

```properties
fs.native-s3.enabled=true
s3.endpoint=http://{minio_host}:9000
s3.path-style-access=true
s3.aws-access-key={minio_access_key}
s3.aws-secret-key={minio_secret_key}
s3.region=us-east-1
```

## Authentication

How Trino identifies itself to Gravitino. Independent of the storage credential choice above.

### No Authentication

Add nothing. Suitable only when Gravitino authentication is disabled.

### Basic Authentication

Requires Trino 481 or later. Trino has no native Basic mode for Iceberg REST, so pass the
`Authorization` header directly. On earlier releases, `iceberg.rest-catalog.http-headers` is not
available and Basic authentication cannot be used.

```bash
echo -n '{username}:{password}' | base64
```

```properties
iceberg.rest-catalog.http-headers=Authorization: Basic {base64_credentials}
```

### OAuth2 Authentication

The Quick Start above uses the client credentials flow, which obtains and renews tokens rather than
carrying a static one that eventually expires. Prefer it.

To carry a static token instead:

```properties
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token={token}
```

On Trino 479 and later, add the following to avoid token-exchange behavior that can cause repeated
token requests:

```properties
iceberg.rest-catalog.session=NONE
iceberg.rest-catalog.oauth2.token-exchange-enabled=false
```

`iceberg.rest-catalog.session=NONE` is already the default and can be omitted.

See [How to authenticate](../security/how-to-authenticate.md) for the Gravitino side of this
configuration.

## Starting Trino

Trino is a server process, and the catalog is picked up when Trino starts. After placing
`gravitino_irc.properties` in `etc/catalog/`, restart Trino:

```bash
$TRINO_HOME/bin/launcher restart
```

Trino needs roughly 20 seconds to accept queries after a restart, which is long enough to produce
misleading connection errors in scripted runs.

Once Trino is running, connect using the Trino CLI:

```bash
trino --server http://{trino_host}:8080 --catalog gravitino_irc
```

Or connect without a default catalog and qualify queries fully:

```bash
trino --server http://{trino_host}:8080
```

## Verifying Credential Vending

Confirm the server vends credentials before assuming Trino is using them. The `storage-credentials`
block in the `loadTable` response is the direct evidence:

```bash
curl -s -H "X-Iceberg-Access-Delegation: vended-credentials" \
  -H "Authorization: Bearer {token}" \
  http://{gravitino_host}:9001/iceberg/v1/{catalog}/namespaces/{namespace}/tables/{table}
```

Three markers distinguish genuine vending from static credentials passed through: the access key
begins with `ASIA` rather than `AKIA`, a session token is present, and the prefix is scoped to the
table path rather than the whole bucket.

## Known Issues

### Vended Credentials Are Not Refreshed During Long-Running Queries

**Cause:** Gravitino advertises a refresh endpoint in the `loadTable` response as
`client.refresh-credentials-endpoint`, but Trino does not call it when vended credentials expire
mid-query ([trinodb/trino#25827](https://github.com/trinodb/trino/issues/25827)). Scans running past
the STS session lifetime fail.

**Solution:** Raise `s3-token-expire-in-secs` on the Gravitino catalog, together with the maximum
session duration on the IAM role, or keep individual queries shorter than the session lifetime.

### `SHOW SCHEMAS` Fails With OAuth2 and Nested Namespaces

**Cause:** With `iceberg.rest-catalog.security=OAUTH2`,
`iceberg.rest-catalog.nested-namespace-enabled=true`, and `iceberg.rest-catalog.session=NONE` (the
default), `SHOW SCHEMAS` recursively calls Iceberg REST `listNamespaces`. On Trino releases before
482, each recursive call creates a separate OAuth session, which can trigger excessive token requests
and cause errors such as `Connection pool shut down` or `StackOverflowError`.

**Solution:** Upgrade to Trino 482 or later.

### `TIMESTAMP WITH TIME ZONE` Values Are Not Adjusted to the Client Session Time Zone

**Cause:** Trino does not adjust `TIMESTAMP WITH TIME ZONE` results according to the client session
time zone. Unlike Spark and Flink, it displays these values based on the stored
timestamp-with-time-zone value.

**Solution:** Convert with `at_timezone` and `current_timezone()`:

```sql
SELECT
  id,
  at_timezone(timestamp_with_timezone_column, current_timezone())
FROM {catalog}.{namespace}.{table};
```

### Trino Identifiers Are Not Case Sensitive

**Cause:** Trino identifiers are not treated as case sensitive, so metadata names that differ only by
letter case cannot be distinguished. See [Trino identifier
documentation](https://trino.io/docs/current/language/reserved.html#language-identifiers). The
limitation comes from Trino itself and is not specific to Gravitino.

**Solution:** Use lowercase metadata names, and avoid creating objects whose names differ only by
letter case.

## Gravitino Connector vs. the IRC

| Feature                  | Gravitino engine connector | IRC                           |
|:-------------------------|:---------------------------|:------------------------------|
| Engine plugin required   | Yes                        | No                            |
| Gravitino access control | Yes                        | Yes, for API-created catalogs |
| Supported engines        | Trino, Spark, Flink, Daft  | Any Iceberg-compatible engine |
| Credential vending       | Varies                     | Yes, S3 only in Trino         |

Catalogs created through the Gravitino REST catalog API are registered in a metalake, so privileges
can be granted on them and Gravitino access control applies to queries that reach them over the IRC.
Catalogs defined instead in the Iceberg REST service configuration file are not registered in a
metalake, so there is nothing to grant privileges on.

## Related

- [Credential vending](../security/credential-vending.md)
- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark to Iceberg REST](./spark.md)
- [Connect Flink to Iceberg REST](./flink.md)
- [Trino Gravitino connector](../trino-connector/trino-connector.md)
