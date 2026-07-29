---
title: "Connect Trino to Iceberg REST"
sidebar_label: "Trino"
---

## Introduction

<<<<<<< HEAD
Apache Gravitino exposes an [Iceberg REST catalog](../iceberg-rest-service.md) endpoint that any
Iceberg-compatible engine can connect to directly — without installing a Gravitino-specific
connector plugin. This page describes how to configure Trino to use Gravitino's Iceberg REST
(IRC) endpoint.
=======
Apache Gravitino exposes an Iceberg REST Catalog (IRC) endpoint that any Iceberg-compatible engine
can connect to directly, without installing a Gravitino-specific connector plugin. The sections
below describe how to configure Trino to use it.

Most of the configuration is on the Trino side and is covered here. The storage credential setup
lives on the Gravitino catalog and is covered in [Credential
vending](../security/credential-vending.md), which this page links to at each point where it
applies.
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

:::note
This integration uses the standard Apache Iceberg REST catalog specification. Gravitino enforces
its full access-control model on all IRC requests.
:::

<<<<<<< HEAD
## Prerequisites

- Apache Gravitino running with the Iceberg REST service enabled. See
  [Iceberg REST catalog service](../iceberg-rest-service.md) for setup instructions.
- The Gravitino IRC endpoint is accessible from the Trino coordinator and all workers. The default
  port is `9001`.
- Trino 469 or later recommended.

## Configuration

Create a catalog properties file in your Trino `etc/catalog/` directory. The filename determines
the catalog name in Trino — `gravitino_irc.properties` creates a catalog named `gravitino_irc`.

:::note
The `warehouse` property is managed by the Gravitino IRC server and does not need to be set in
the Trino catalog configuration.
:::

### No Authentication
=======
Create a catalog properties file in your Trino `etc/catalog/` directory. The filename determines the
catalog name in Trino, so `gravitino_irc.properties` creates a catalog named `gravitino_irc`. Some
distributions place the directory at `etc/trino/catalog/`. Either way, these are per-catalog
properties and do not belong in `config.properties`.

The file below is complete, using vended credentials with OAuth2 authentication. Place it in
`etc/catalog/` and restart Trino. Each commented group has a matching section below, covering what
the properties do and what the alternatives are.
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

```properties
# Connection properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

<<<<<<< HEAD
# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>
=======
# Storage access
iceberg.rest-catalog.vended-credentials-enabled=true
fs.s3.enabled=true
s3.region={region_name}

# Authentication
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential={client_id}:{client_secret}
iceberg.rest-catalog.oauth2.server-uri={token_endpoint_uri}
iceberg.rest-catalog.oauth2.scope={scope}
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

<<<<<<< HEAD
### Basic Authentication

Requires Trino **481+**. Trino has no native Basic mode for Iceberg REST; pass `Authorization`
via HTTP headers.
=======
On Trino 480 and earlier, `fs.s3.enabled` is named `fs.native-s3.enabled`. See [Storage
Access](#storage-access).

## Prerequisites

On the Gravitino side:

- Gravitino running with the Iceberg REST service enabled. See [Iceberg REST catalog
  service](../iceberg-rest-service.md) for setup.
- The IRC endpoint reachable from the Trino coordinator and all workers. The default port is `9001`.
- For vended credentials, three pieces of catalog-side setup, all described in [Credential
  vending](../security/credential-vending.md):
  - A credential provider, and the role it assumes, configured on the catalog. See
    [`s3-token`](../security/credential-vending.md#s3-token).
  - The cloud bundle jar on the IRC classpath. See
    [Deployment](../security/credential-vending.md#deployment).
  - The trust policy and the permission policy on the vending role, in AWS IAM.

Of everything vending needs, only the Trino properties in [Storage Access](#storage-access) are
configured on this page. A Trino catalog file that is correct on its own vends nothing when any of
the pieces above is missing, and the resulting failures surface in Trino rather than in Gravitino.
See [Troubleshooting](#troubleshooting) for the symptoms each one produces.

On the Trino side, the release requirements differ by feature:

| Capability                                                       | Minimum Trino release |
|:-----------------------------------------------------------------|:----------------------|
| Native S3 file system, which consumes vended credentials         | 419                   |
| Explicit file system activation in each catalog file             | 458                   |
| Vended credentials for Azure                                     | 481                   |
| Refreshable vended credentials for S3, GCS, and Azure            | 481                   |
| Basic authentication through `iceberg.rest-catalog.http-headers` | 481                   |
| `SHOW SCHEMAS` with OAuth2 and nested namespaces                 | 482                   |

Verified against Gravitino 1.3.0 and Trino 478 with AWS S3. Property names and behavior at Trino 481
and later come from the Trino documentation and release notes rather than from that run.

## Which Gravitino Catalogs Are Reachable

Two settings in `conf/gravitino.conf` decide what a Trino catalog file can reach:

```properties
gravitino.iceberg-rest.catalog-config-provider = dynamic-config-provider
gravitino.iceberg-rest.gravitino-metalake = {metalake}
```

The metalake is fixed at startup. `gravitino-metalake` names exactly one metalake, and the IRC
serves catalogs from that metalake only. Creating another metalake through the Gravitino REST API
does not make it reachable on this endpoint. Pointing the IRC at a different metalake means editing
`gravitino.conf` and restarting, and serving two metalakes at once means running two Iceberg REST
services. The metalake does not have to exist when the server starts, so naming it in the
configuration first and creating it afterwards works.

Catalogs are not fixed. The dynamic config provider polls the Gravitino server for the catalogs in
that metalake, so a catalog created through the REST catalog API becomes reachable without a
restart. Select one from Trino with `iceberg.rest-catalog.prefix`, described in [Connection
Properties](#connection-properties). Setting `gravitino.iceberg-rest.default-catalog-name` decides
which catalog answers when a client sends no prefix at all.

The static config provider instead serves catalogs defined directly in `gravitino.conf` with
`gravitino.iceberg-rest.` prefixed keys, loaded once when the server starts. Catalogs defined that
way are not registered in a metalake, so there is nothing to grant privileges on. Most published
examples use the static form, so take care not to mix the two.

Gravitino access control over the IRC needs three things together, not the dynamic provider alone:

- The Iceberg REST service running as an auxiliary service inside the Gravitino server. Standalone
  Iceberg REST deployments do not support access control. See [Deployment
  modes](../iceberg-rest-service.md#deployment-modes), where the mode table carries the access
  control column, and [Access control](../iceberg-rest-service.md#access-control).
- `gravitino.authorization.enable = true` on the Gravitino server. See [Access
  control](../security/access-control.md).
- The dynamic config provider. See [Dynamic catalog configuration
  provider](../iceberg-rest-service.md#dynamic-catalog-configuration-provider).

For an end-to-end walkthrough that enables all three and then grants privileges on a catalog reached
over the IRC, see [Access control tutorial](../iceberg-rest-service.md#access-control-tutorial).

See [Setting properties](../security/credential-vending.md#setting-properties) for how credential
properties differ between the two providers, and [Iceberg REST catalog
service](../iceberg-rest-service.md) for the full provider configuration.

## Connection Properties

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://{gravitino_host}:9001/iceberg
iceberg.rest-catalog.prefix={catalog}
```

| Property                     | Purpose                                | Required |
|:-----------------------------|:---------------------------------------|:---------|
| `connector.name`             | Always `iceberg` for this path         | Yes      |
| `iceberg.catalog.type`       | Always `rest` for this path            | Yes      |
| `iceberg.rest-catalog.uri`   | The Gravitino IRC endpoint             | Yes      |
| `iceberg.rest-catalog.prefix`| Selects which Gravitino catalog to use | Yes      |

Trino documents the full set in [Iceberg REST catalog configuration
properties](https://trino.io/docs/current/object-storage/metastores.html#rest-catalog).

`iceberg.rest-catalog.prefix` must match the catalog name in Gravitino. Confirm the expected value
against the server:

```bash
curl -s "http://{gravitino_host}:9001/iceberg/v1/config?warehouse={catalog}"
```

Gravitino returns the prefix under `defaults`, so setting it explicitly in Trino matches rather than
overrides it.

Storage access and authentication are independent choices. Pick one option from each of the two
sections below.

## Storage Access

Trino needs credentials to read and write the underlying object storage. Choose vended credentials
or static credentials. Do not configure both.

Whichever you choose, the native S3 file system must be enabled. It performs the request signing for
S3 access, and without it metadata operations succeed while data reads fail with
`ICEBERG_FILESYSTEM_ERROR`.

```properties
fs.s3.enabled=true
```

Trino 481 removed legacy object storage support, leaving `fs.hadoop.enabled` for HDFS only, and the
native file system properties lost the `native-` segment at the same time. Releases through 480
document `fs.native-s3.enabled`, and 481 and later document `fs.s3.enabled`. Use the name documented
for the release you run.

### How Credential Vending Works

1. Trino calls `loadTable` on the Gravitino IRC endpoint. With
   `iceberg.rest-catalog.vended-credentials-enabled=true`, Trino sends the
   `X-Iceberg-Access-Delegation: vended-credentials` header for you.
2. Gravitino calls STS `AssumeRole` against the IAM role configured on the catalog.
3. Gravitino returns a `storage-credentials` block in the `loadTable` response, holding temporary
   credentials scoped to the table's S3 prefix.
4. Trino uses those credentials for the S3 reads and writes in that query.

One property of the design is worth stating explicitly, because it is commonly assumed otherwise.
The vended credentials are scoped to the table path, not to the calling user. Gravitino mints them
by assuming a fixed role configured on the catalog, so every caller that reaches a given table
receives credentials with the same storage permissions. Per-user restriction comes from Gravitino
access control deciding who reaches the table, not from the credentials themselves.

### Vended Credentials

Used in the Quick Start above. Gravitino mints short-lived, path-scoped credentials at query time,
so no long-lived storage keys live in the Trino configuration.

```properties
iceberg.rest-catalog.vended-credentials-enabled=true
fs.s3.enabled=true
s3.region={region_name}
```

The catalog-side configuration this requires, covering the credential provider, the role it assumes,
and the IAM policies behind it, is described in [Credential
vending](../security/credential-vending.md). None of it is set from the Trino catalog file.

If `s3.aws-access-key` and `s3.aws-secret-key` are also set in the file, Trino requests vended
credentials, receives them, and then signs with the static keys anyway. No warning appears and every
query works, so the file reads as though vending is in use when it is not. Check that neither is
present, and see [Verification](#verification) for how to confirm which credentials are actually
reaching S3.

Backend coverage varies by Trino release. S3 is supported throughout. Trino 481 added Azure
([trinodb/trino#23238](https://github.com/trinodb/trino/issues/23238)) and refreshable vended
credentials, which its release notes describe as covering S3, GCS, and Azure
([trinodb/trino#28998](https://github.com/trinodb/trino/issues/28998)). Check the release notes for
the version you run before relying on GCS or Azure, and use static credentials where vending is not
yet available.

### Static Credentials

Configure storage keys directly in Trino. Simpler to set up, but the keys are long-lived, are not
scoped to a table path, and are managed outside Gravitino.

Relative to the Quick Start, remove `iceberg.rest-catalog.vended-credentials-enabled` and configure
the keys:

```properties
fs.s3.enabled=true
s3.region={region_name}
s3.aws-access-key={access_key_id}
s3.aws-secret-key={secret_access_key}
```

Leaving `iceberg.rest-catalog.vended-credentials-enabled=true` in place alongside the keys is not an
error and produces no warning. Trino requests vended credentials and then signs with the static keys
anyway, for the reason given under [Vended Credentials](#vended-credentials).

For local development against MinIO. The same precedence applies, so remove
`iceberg.rest-catalog.vended-credentials-enabled` here too:

```properties
fs.s3.enabled=true
s3.endpoint=http://{minio_host}:9000
s3.path-style-access=true
s3.aws-access-key={minio_access_key}
s3.aws-secret-key={minio_secret_key}
s3.region=us-east-1
```

## Authentication

How Trino identifies itself to Gravitino. Independent of the storage credential choice above. If
Gravitino requires an identity, Trino must present one here or requests are rejected before any
vending occurs. See [How to authenticate](../security/how-to-authenticate.md) for the Gravitino side.

### No Authentication

Omit the authentication block entirely. Relative to the Quick Start, that means dropping these four
lines:

```properties
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential={client_id}:{client_secret}
iceberg.rest-catalog.oauth2.server-uri={token_endpoint_uri}
iceberg.rest-catalog.oauth2.scope={scope}
```

With none of them set, `iceberg.rest-catalog.security` stays at its default of `NONE` and Trino
sends no credentials to the IRC. See [Iceberg REST catalog configuration
properties](https://trino.io/docs/current/object-storage/metastores.html#rest-catalog) for the
property and its other values.

Use this for local development and isolated test environments only. With no identity on the request
there is nothing for Gravitino to authorize, so privileges granted on a catalog have no effect on
queries arriving over the IRC and every caller that can reach the port has the same access. Adding
vended credentials extends that past metadata. Since the IRC mints credentials for any caller that
asks, as described under [How Credential Vending Works](#how-credential-vending-works), anyone who
can reach the endpoint can obtain working storage credentials for the warehouse.

If the server does require an identity and Trino presents none, requests are rejected before vending
is reached, which surfaces as a 403 rather than as a storage error.

### Basic Authentication

Requires Trino 481 or later.

`iceberg.rest-catalog.security` has no Basic value, so Trino cannot be configured to authenticate to
a REST catalog with a username and password. Gravitino's IRC does accept HTTP Basic, so the way
across is to construct the header yourself and have Trino attach it to every REST catalog request.
Trino 481 added `iceberg.rest-catalog.http-headers`
([trinodb/trino#24236](https://github.com/trinodb/trino/issues/24236)) for sending arbitrary headers,
and Basic authentication is one use of it rather than a feature in its own right.

Encode the credentials:
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

```shell
echo -n '<username>:<password>' | base64
```

Then set the header:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

# Basic authentication
iceberg.rest-catalog.http-headers=Authorization: Basic <base64-credentials>

# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

<<<<<<< HEAD
Replace `<base64-credentials>` with the output of `echo -n '<username>:<password>' | base64`.

### OAuth2 Authentication

=======
On releases before 481 there is no way to send the header, so the choice there is OAuth2 or no
authentication.

Trino treats the value as an opaque header rather than as credentials, so nothing renews or rotates
it and it goes out unchanged on every request. Base64 is encoding rather than encryption, so anyone
who can read the catalog file recovers the password. Trino also documents the property as carrying
additional non-sensitive headers, so putting credentials in it works but runs against its stated
purpose. Prefer OAuth2 where the Gravitino server supports it.

### OAuth2 Authentication

Two options, differing in who obtains the token. Both put the same `Authorization: Bearer` header on
the wire, so the Gravitino side of the configuration is the same for either.

#### Client Credentials Flow

Used in the Quick Start above. Trino holds a client ID and secret, requests a token itself, and
obtains a fresh one when the current token expires. Prefer it.

```properties
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential={client_id}:{client_secret}
iceberg.rest-catalog.oauth2.server-uri={token_endpoint_uri}
iceberg.rest-catalog.oauth2.scope={scope}
```

`iceberg.rest-catalog.oauth2.server-uri` is how Trino locates the identity provider. It takes the
provider's token endpoint rather than its issuer or realm URL. On Keycloak that is:

```
https://{keycloak_host}/realms/{realm}/protocol/openid-connect/token
```

Trino then presents the token it receives to Gravitino as a bearer token, in an HTTP header on every
Iceberg REST request:

```
Authorization: Bearer {access_token}
```

#### Pre-Issued Token

Trino sends one fixed token, obtained out of band from your identity provider, on every request to
the IRC. Nothing renews it, so when the token expires every request fails with 401 until someone
edits the catalog file and restarts Trino.

>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://<gravitino-host>:9001/iceberg

# OAuth2 authentication
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<your-token>

# Native S3 filesystem (Trino 430+)
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>

# Table defaults
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

<<<<<<< HEAD
See [How to authenticate](../security/how-to-authenticate.md) for Gravitino authentication
configuration options.

:::tip Local development
For local development with MinIO, replace the S3 section with:
=======
Reach for it when Trino cannot reach the OAuth2 server, or for a short-lived test. Note that the
token sits in plaintext in the catalog file and is directly replayable against the IRC by anyone who
can read that file, which a client secret is not.

#### Token Exchange

On Trino 479 and later, add the following to both options to avoid token-exchange behavior that can
cause repeated token requests:
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

```properties
fs.native-s3.enabled=true
s3.endpoint=http://<minio-host>:9000
s3.path-style-access=true
s3.aws-access-key=<minio-access-key>
s3.aws-secret-key=<minio-secret-key>
s3.region=us-east-1
```

See [gravitino-irc-quickstart](https://github.com/markhoerth/gravitino-irc-quickstart) for a
complete local development environment using MinIO.
:::

<<<<<<< HEAD
## Start Trino
=======
## Table Defaults

Optional, and independent of everything else on this page. These set the defaults Trino uses when it
creates tables through the IRC:

```properties
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

Trino is a server process — the catalog is picked up automatically when Trino starts. After
placing `gravitino_irc.properties` in `etc/catalog/`, restart Trino:

```bash
$TRINO_HOME/bin/launcher restart
```

Once Trino is running, connect using the Trino CLI:

```bash
trino --server http://<trino-host>:8080 --catalog gravitino_irc
```

Or connect without specifying a default catalog and qualify queries fully:

```bash
trino --server http://<trino-host>:8080
```

<<<<<<< HEAD
## Examples

Once connected, use the Trino CLI or any Trino-compatible client.

### List Schemas

```sql
SHOW SCHEMAS FROM gravitino_irc;
=======
## Verification

### Confirm the Server Vends Credentials

Check the server before assuming Trino is using vended credentials. The `storage-credentials` block
in the `loadTable` response is the direct evidence:

```bash
curl -s -H "X-Iceberg-Access-Delegation: vended-credentials" \
  -H "Authorization: Bearer {token}" \
  http://{gravitino_host}:9001/iceberg/v1/{catalog}/namespaces/{namespace}/tables/{table} \
  | python3 -m json.tool | grep -A8 storage-credentials
```

Expected output:

```json
"storage-credentials": [
  {
    "prefix": "s3://{bucket_name}/{warehouse_path}/{namespace}/{table}",
    "config": {
      "s3.access-key-id": "ASIA...",
      "s3.secret-access-key": "...",
      "s3.session-token": "...",
      "s3.session-token-expires-at-ms": "..."
    }
  }
]
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))
```

### List Tables

```sql
SHOW TABLES FROM gravitino_irc.<namespace>;
```

### Query a Table

```sql
SELECT * FROM gravitino_irc.<namespace>.<table> LIMIT 10;
```

### Create a Schema

When creating a schema in Trino, a storage location must be specified:

```sql
CREATE SCHEMA gravitino_irc.<namespace>
WITH (location = 's3://<bucket>/<namespace>/');
```

### Create a Table

```sql
CREATE TABLE gravitino_irc.<namespace>.new_table (
  id INTEGER,
  name VARCHAR,
  created_at TIMESTAMP
)
WITH (
  format         = 'PARQUET',
  format_version = 2
);
```

### Confirm the Engine Path

```bash
trino --execute "SELECT * FROM {catalog}.{namespace}.{table}"
```

A successful query alone does not prove vending is in use, since static keys or an instance profile
can satisfy the same reads. For independent confirmation at the AWS layer, CloudTrail shows the
`AssumeRole` call against the vending role, followed by S3 operations attributed to the assumed-role
session rather than to the base IAM user. CloudTrail is the authoritative check in environments
where an EC2 instance profile could otherwise satisfy the S3 reads.

## Known Issues

### `SHOW SCHEMAS` Fails on Gravitino IRC (OAuth2, Nested Namespaces)

<<<<<<< HEAD
**Cause:** When Trino connects to Gravitino IRC with `iceberg.rest-catalog.security=OAUTH2`,
`iceberg.rest-catalog.nested-namespace-enabled=true`, and `iceberg.rest-catalog.session=NONE`
(the default), `SHOW SCHEMAS` recursively calls Iceberg REST `listNamespaces`. On Trino releases
before 482, each recursive call creates a separate OAuth session, which can trigger excessive token
requests and cause errors such as `Connection pool shut down` or `StackOverflowError`.

**Solution:** Upgrade to Trino 482+.
=======
Applies to Trino releases before 481.

**Cause:** Gravitino advertises a refresh endpoint in the `loadTable` response as
`client.refresh-credentials-endpoint`, but Trino does not call it when vended credentials expire
mid-query ([trinodb/trino#25827](https://github.com/trinodb/trino/issues/25827)). Scans running past
the STS session lifetime fail. The gap is client-side rather than a catalog limitation.

**Solution:** Upgrade to Trino 481 or later, which adds refreshable vended credentials
([trinodb/trino#28998](https://github.com/trinodb/trino/issues/28998)). On earlier releases, raise
[`s3-token-expire-in-secs`](../security/credential-vending.md#s3-token) on the Gravitino catalog,
together with the maximum session duration on the IAM role, or keep individual queries shorter than
the session lifetime.

### Storage Credentials Are Exposed in Query JSON

**Cause:** Trino serializes storage credentials into query JSON for write and table maintenance
operations, where any user with write privilege can read them through the Trino UI or the query API
([GHSA-x27p-5f68-m644](https://github.com/trinodb/trino/security/advisories/GHSA-x27p-5f68-m644)).
The exposure applies to static credentials as well as vended ones.

**Solution:** Review the current advisory status against your deployed Trino version. Short
`s3-token-expire-in-secs` values limit the window during which an exposed vended credential is
usable, which static keys do not offer.

### Trino Returns Valid Credentials but Does Not Honor Them

**Cause:** At least one report describes a Trino version receiving a correct `storage-credentials`
block from the server yet failing on S3 access, where the same endpoint works from Spark
([trinodb/trino#27416](https://github.com/trinodb/trino/issues/27416), reported on Trino 474).

**Solution:** If the verification curl shows a valid credentials block and Trino still fails, treat
the Trino version as a suspect before revisiting configuration.

### `SHOW SCHEMAS` Fails With OAuth2 and Nested Namespaces

**Cause:** With `iceberg.rest-catalog.security=OAUTH2`,
`iceberg.rest-catalog.nested-namespace-enabled=true`, and `iceberg.rest-catalog.session=NONE` (the
default), `SHOW SCHEMAS` recursively calls Iceberg REST `listNamespaces`. On Trino releases before
482, each recursive call creates a separate OAuth session, which can trigger excessive token
requests and cause errors such as `Connection pool shut down` or `StackOverflowError`.

**Solution:** Upgrade to Trino 482 or later.
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

### `TIMESTAMP WITH TIME ZONE` Values Are Not Adjusted to the Client Session Time Zone

**Cause:** For `TIMESTAMP WITH TIME ZONE` values, Trino does not adjust query results according to
the client session time zone. Unlike Spark and Flink, Trino displays these values based on the
stored timestamp-with-time-zone value.

**Solution:** To convert a `TIMESTAMP WITH TIME ZONE` value to the current client session time
zone, use `at_timezone` together with `current_timezone()`:

```sql
SELECT
  id,
  at_timezone(timestamp_with_timezone_column, current_timezone())
FROM <catalog>.<namespace>.<table>;
```

## Gravitino Connector vs. Iceberg REST

<<<<<<< HEAD
| Feature                  | Gravitino Engine Connector  | Iceberg REST                  |
|:-------------------------|:----------------------------|:------------------------------|
| Engine plugin required   | Yes                         | No                            |
| Gravitino access control | Yes                         | Yes                           |
| Supported engines        | Trino, Spark, Flink, Daft   | Any Iceberg-compatible engine |
| Credential vending       | Varies                      | Yes (S3, GCS, OSS, ADLS)      |

### Trino Identifiers Are Not Treated as Case Sensitive

Trino identifiers are not treated as case sensitive. As a result, metadata names that differ
only by letter case cannot be distinguished. See [Trino identifier
documentation](https://trino.io/docs/current/language/reserved.html#language-identifiers). This
limitation comes from Trino itself and is not specific to Gravitino.

For the best compatibility with Trino:

- Use lowercase metadata names.
- Avoid creating objects whose names differ only by letter case.
=======
**Cause:** Trino identifiers are not treated as case sensitive, so metadata names that differ only
by letter case cannot be distinguished. See [Trino identifier
documentation](https://trino.io/docs/current/language/reserved.html#language-identifiers). The
limitation comes from Trino itself and is not specific to Gravitino.

**Solution:** Use lowercase metadata names, and avoid creating objects whose names differ only by
letter case. Where mixed-case names already exist in Gravitino,
`iceberg.rest-catalog.case-insensitive-name-matching=true`, off by default, lets Trino resolve them.
It does not make names that differ only by case distinguishable.

## Troubleshooting

Failures in this path tend to surface far from their cause. The table maps symptoms back to the
configuration that produces them.

| Symptom                                                          | Likely cause                                                                                               |
|:-----------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| Catalog not visible in `SHOW CATALOGS`                           | Trino not restarted, or a parse error in the catalog file. Check the Trino server log                      |
| `Failed to list namespaces`                                      | `iceberg.rest-catalog.prefix` does not match a Gravitino catalog name, or the identity was rejected        |
| 403 `ForbiddenException`, principal not in metalake              | Identity rejected before vending is reached. A token principal must be a member of the metalake            |
| `storage-credentials` absent from the `loadTable` response       | `credential-providers` not set on the catalog, or the cloud bundle jar missing from the IRC classpath      |
| `ICEBERG_FILESYSTEM_ERROR` on data reads, metadata fine          | Native S3 file system not enabled, wrong property name for the release, or static keys overriding          |
| S3 `AccessDenied` despite a valid `storage-credentials` block    | Permission policy on the vending role, or static keys taking precedence in the Trino catalog file          |
| STS `AccessDenied` on `AssumeRole`                               | Trust policy does not allow the `s3-access-key-id` principal to assume the vending role                    |
| Access key in the response begins with `AKIA`                    | The catalog is using `s3-secret-key` rather than `s3-token`, so static keys are vended unchanged           |
| Long queries fail after about an hour                            | STS session expiry with no client-side refresh. See Known Issues                                           |
| Verification curl shows valid credentials but Trino fails on S3  | Possible version-specific consumption bug. See Known Issues                                                |

The catalog-side causes are described in [Credential
vending](../security/credential-vending.md).

## Gravitino Connector vs. the IRC

| Feature                  | Gravitino engine connector | IRC                           |
|:-------------------------|:---------------------------|:------------------------------|
| Engine plugin required   | Yes                        | No                            |
| Gravitino access control | Yes                        | Yes, for API-created catalogs |
| Supported engines        | Trino, Spark, Flink, Daft  | Any Iceberg-compatible engine |
| Credential vending       | Varies                     | Yes, see Trino release notes  |

Catalogs created through the Gravitino REST catalog API are registered in a metalake, so privileges
can be granted on them and Gravitino access control applies to queries that reach them over the IRC.
Catalogs defined instead in the Iceberg REST service configuration file are not registered in a
metalake, so there is nothing to grant privileges on.
>>>>>>> 4d60cc71c ([MINOR] docs: update the Trino Iceberg REST engine page (#12242))

## Related

- [Iceberg REST catalog service](../iceberg-rest-service.md)
- [Connect Spark to Iceberg REST](./spark.md)
- [Connect Flink to Iceberg REST](./flink.md)
- [Trino Gravitino connector](../trino-connector/trino-connector.md)
