---
title: "Lance REST Service"
slug: "/lance-rest-service"
keywords:
  - Lance
  - Lance REST
  - Lance datasets
  - REST API
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Lance REST service lets Lance clients read and write Lance tables while Apache Gravitino holds the catalog metadata. Gravitino implements the Lance REST Catalog protocol, so any tool built on a Lance SDK can treat a Gravitino metalake as its namespace.

### About Lance

[Lance](https://lance.org/format/) is an open table and file format for multimodal AI data. A Lance table is a complete format in its own right, with its own file layout, manifests, and version history. It does not build on Parquet, and it does not store data in Iceberg format.

Where Iceberg over Parquet is built to scan analytical tables, Lance is built for the work that surrounds a model:

| Capability               | What It Provides                                                                                   |
|--------------------------|----------------------------------------------------------------------------------------------------|
| Fast random access       | Point lookups of individual rows, for training loops and retrieval rather than full scans          |
| Vectors as columns       | Embeddings stored as fixed-size list columns next to the rest of the row                           |
| Indexes inside the table | Vector, scalar, and full-text indexes stored with the data instead of in a separate service        |
| Multimodal values        | Images, audio, and video stored alongside structured columns, with a blob API to skip them on scan |
| Cheap schema evolution   | Adding a column, such as a new embedding, without rewriting the dataset                            |

Schemas are Apache Arrow schemas, and Arrow IPC is the wire format for data operations.

### Choosing an API

Lance tables in Gravitino always live in a `lakehouse-generic` catalog. There are two ways to get one. Configure the Lance REST service, and it creates the catalog itself when a client first connects. Or create the catalog through the Gravitino REST API and skip the service entirely. The first has a server configuration step and no catalog step; the second has a catalog step and no server configuration.

| Path               | Choose It When                                                                                     | Documented In                                                       |
|--------------------|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| Lance REST service | Lance-native tools drive the workload, such as `lance-spark`, `lance-ray`, or any Lance SDK client | This page and [Lance REST Integration](./lance-rest-integration.md) |
| Gravitino REST API | Gravitino is the system of record and Lance is one table format among several                      | [Lance Tables](./lakehouse-generic-lance-table.md)                  |

Both produce the same catalog, schema, and table objects, and either can read what the other wrote. Property names differ between the paths for the same concepts, so read both pages before mixing them in one deployment. See [Lakehouse Generic Catalog](./lakehouse-generic-catalog.md) for the catalog itself.

For the protocol itself, including the full operation list and request models, see the [Lance REST Catalog specification](https://lance.org/format/catalog/rest/). For the models the SDKs use, see the [Lance Namespace client spec](https://lance.org/format/namespace/).

## Quick Start

These steps run the Lance REST service inside the Gravitino server, which is how it is normally deployed.

**1. Enable the service in `${GRAVITINO_HOME}/conf/gravitino.conf`.**

```properties
gravitino.auxService.names = lance-rest
gravitino.lance-rest.classpath = lance-rest-server/libs
gravitino.lance-rest.httpPort = 9101
gravitino.lance-rest.namespace-backend = gravitino
gravitino.lance-rest.gravitino-uri = http://localhost:8090
gravitino.lance-rest.gravitino-metalake = {metalake_name}
gravitino.lance-rest.gravitino-auth-type = simple
```

`simple` accepts requests that arrive without credentials, which suits a local trial. For a deployment, see [Authenticating Callers](#authenticating-callers).

**2. Start the Gravitino server.** The Lance REST service starts with it.

```shell
${GRAVITINO_HOME}/bin/gravitino.sh start
```

**3. Create the metalake named in the configuration.** The service resolves it on its first call rather than at startup, so it does not have to exist before this point.

```shell
GRAVITINO_URL=http://localhost:8090

curl -X POST "${GRAVITINO_URL}/api/metalakes" \
  -H "Accept: application/vnd.gravitino.v1+json" \
  -H 'Content-Type: application/json' \
  -d '{"name": "{metalake_name}", "comment": "Lance metalake"}'
```

**4. Create a namespace.** A first-level namespace is a `lakehouse-generic` catalog. Set `location` and the `lance.storage.*` credentials here, so that tables inherit them and Lance clients receive them.

```shell
LANCE_URL=http://localhost:9101/lance

curl -X POST "${LANCE_URL}/v1/namespace/{catalog_name}/create" \
  -H 'Content-Type: application/json' \
  -d '{"id": ["{catalog_name}"], "mode": "create",
       "properties": {"location": "s3://{bucket}/{prefix}",
                      "lance.storage.access_key_id": "{access_key}",
                      "lance.storage.secret_access_key": "{secret_key}",
                      "lance.storage.region": "us-east-1"}}'
```

**5. Confirm it reached Gravitino.**

```shell
curl -H "Accept: application/vnd.gravitino.v1+json" \
  "${GRAVITINO_URL}/api/metalakes/{metalake_name}/catalogs/{catalog_name}"

curl "${LANCE_URL}/health/ready"
```

The namespace created through the Lance API appears as a catalog in `{metalake_name}`, which is the whole point of pointing Lance clients at Gravitino.

Readiness returns 200 from this point on. The service connects to the Gravitino server on its first namespace or table call rather than at startup, so `/health/ready` returns 503 until step 4 runs. A 503 after that means initialization failed, and the body names the failing check.

## Configuration

Every property on this page goes in `${GRAVITINO_HOME}/conf/gravitino.conf`, alongside the rest of the Gravitino server configuration. Changes take effect on server restart. When the service runs as a separate process the same property names apply, but they go in `gravitino-lance-rest-server.conf` instead, as described in [Running as a Separate Process](#running-as-a-separate-process).

### Service Properties

| Configuration Property                             | Description                                                                        | Default Value         | Required          |
|----------------------------------------------------|------------------------------------------------------------------------------------|-----------------------|-------------------|
| `gravitino.auxService.names`                       | Auxiliary services to run. Include `lance-rest` to enable this service             | (none)                | Yes               |
| `gravitino.lance-rest.classpath`                   | Classpath for the service, relative to the Gravitino home directory                | (none)                | Yes               |
| `gravitino.lance-rest.namespace-backend`           | Namespace metadata backend. Only `gravitino` is supported                          | `gravitino`           | Yes               |
| `gravitino.lance-rest.gravitino-uri`               | Gravitino server URI, required in all deployments                                  | http://localhost:8090 | Yes               |
| `gravitino.lance-rest.gravitino-metalake`          | Gravitino metalake the service exposes as its root                                 | (none)                | Yes               |
| `gravitino.lance-rest.gravitino-auth-type`         | Auth type used to reach the Gravitino server. Supported values: `simple`, `oauth2` | `simple`              | No                |
| `gravitino.lance-rest.gravitino-simple.user-name`  | User name presented when the auth type is `simple`                                 | `lance-rest-server`   | No                |
| `gravitino.lance-rest.gravitino-oauth2.server-uri` | OAuth2 server URI                                                                  | (none)                | Yes, for `oauth2` |
| `gravitino.lance-rest.gravitino-oauth2.credential` | Credential used to request the OAuth2 token                                        | (none)                | Yes, for `oauth2` |
| `gravitino.lance-rest.gravitino-oauth2.token-path` | Path on the OAuth2 server used to request the token                                | (none)                | Yes, for `oauth2` |
| `gravitino.lance-rest.gravitino-oauth2.scope`      | Scope of the requested OAuth2 token                                                | (none)                | Yes, for `oauth2` |
| `gravitino.lance-rest.host`                        | Hostname the service binds to                                                      | `0.0.0.0`             | No                |
| `gravitino.lance-rest.httpPort`                    | Port the service listens on                                                        | `9101`                | No                |

### Authenticating to the Gravitino Server

The Quick Start needs no authentication setup. Both the Gravitino server and the Lance REST service default to `simple`, which treats a request arriving without an `Authorization` header as the anonymous user.

Two settings have to agree. `gravitino.authenticators` decides how the Gravitino server validates callers, and `gravitino.lance-rest.gravitino-auth-type` decides how the Lance REST service identifies itself to that server. Setting one without the other breaks the service.

### Authenticating Callers

An external OAuth 2.0 server is required. Configure it as described in [How to authenticate](./security/how-to-authenticate.md), then set both sides:

```properties
# How the Gravitino server validates caller tokens
gravitino.authenticators = oauth
gravitino.authenticator.oauth.serverUri = https://{oauth_host}
gravitino.authenticator.oauth.tokenPath = /oauth/token
gravitino.authenticator.oauth.defaultSignKey = {sign_key}

# How the Lance REST service identifies itself to the Gravitino server
gravitino.lance-rest.gravitino-auth-type = oauth2
gravitino.lance-rest.gravitino-oauth2.server-uri = https://{oauth_host}
gravitino.lance-rest.gravitino-oauth2.credential = {client_id}:{client_secret}
gravitino.lance-rest.gravitino-oauth2.token-path = /oauth/token
gravitino.lance-rest.gravitino-oauth2.scope = {scope}
```

Callers then present a bearer token:

```shell
curl -H "Authorization: Bearer ${TOKEN}" "${LANCE_URL}/v1/namespace/list"
```

Health endpoints bypass the authentication filter, so `/health/ready` answers without a token in every configuration.

JWKS validation is the alternative to a static signing key, and Basic and Kerberos are also supported. All are covered in [How to authenticate](./security/how-to-authenticate.md).

## Lance REST API

Gravitino implements the Lance REST Catalog protocol. For request and response models, see the [Lance REST Catalog specification](https://lance.org/format/catalog/rest/).

### Identifiers

Gravitino uses a three-level hierarchy of catalog, schema, and table. The Lance REST service maps namespaces onto the first two levels, so a namespace identifier holds at most two elements and tables are created beneath a schema.

```
{metalake_name}
└── {catalog_name}          namespace level 1
    └── {schema_name}       namespace level 2
        └── {table_name}    table
```

A three-level namespace identifier is rejected, and tables cannot be created directly under a catalog.

Levels are joined into a single path element using `$` by default. Because `$` is reserved in URIs it must be percent-encoded as `%24`.

| Form        | Example                                               |
|-------------|-------------------------------------------------------|
| Identifier  | `["{catalog_name}", "{schema_name}", "{table_name}"]` |
| Joined      | `{catalog_name}${schema_name}${table_name}`           |
| URL encoded | `{catalog_name}%24{schema_name}%24{table_name}`       |

Pass a different separator with the `delimiter` query parameter.

### Operations

Paths are relative to the service base URL, `http://{host}:9101/lance`.

| Operation              | Method | Path                            |
|------------------------|--------|---------------------------------|
| ListNamespaces         | GET    | `/v1/namespace/list` ¹          |
| ListNamespaces         | GET    | `/v1/namespace/{id}/list`       |
| CreateNamespace        | POST   | `/v1/namespace/{id}/create`     |
| DescribeNamespace      | POST   | `/v1/namespace/{id}/describe`   |
| NamespaceExists        | POST   | `/v1/namespace/{id}/exists`     |
| DropNamespace          | POST   | `/v1/namespace/{id}/drop`       |
| ListTables             | GET    | `/v1/namespace/{id}/table/list` |
| CreateTable            | POST   | `/v1/table/{id}/create`         |
| DeclareTable           | POST   | `/v1/table/{id}/declare`        |
| RegisterTable          | POST   | `/v1/table/{id}/register`       |
| DescribeTable          | POST   | `/v1/table/{id}/describe`       |
| TableExists            | POST   | `/v1/table/{id}/exists`         |
| DropTable              | POST   | `/v1/table/{id}/drop`           |
| DeregisterTable        | POST   | `/v1/table/{id}/deregister`     |
| AlterTableDropColumns  | POST   | `/v1/table/{id}/drop_columns`   |
| AlterTableAlterColumns | POST   | `/v1/table/{id}/alter_columns`  |

¹ A Gravitino extension. The specification has no root-level list, so a portable Lance client cannot rely on it. It lists the top level and takes no identifier, where `/v1/namespace/{id}/list` lists the children of `{id}`.

### Calling the Operations

Details that are easy to get wrong when issuing these calls by hand.

| Item               | Detail                                                                                                                                                               |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CreateTable body   | An Arrow IPC stream sent as `application/vnd.apache.arrow.stream`, not JSON. The schema is read from the stream                                                      |
| CreateTable inputs | Location is passed in the `x-lance-table-location` header and properties in `x-lance-table-properties`. `mode` is a query parameter, defaulting to `create`          |
| DropNamespace      | `behavior` defaults to `restrict`. Under `cascade`, child namespaces and tables are dropped along with their Lance dataset files, and the operation cannot be undone |
| Pagination         | List operations accept `page_token` and `limit` query parameters                                                                                                     |

### Limitations

Gravitino implements a subset of the Lance REST Catalog specification. The operations above are the ones it serves; a client calling anything else gets a 404.

Three gaps are worth naming.

| Limitation       | Detail                                                                                                                                                                                            |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Identifier depth | The specification allows arbitrary namespace nesting. Gravitino allows one or two levels, and a table identifier of exactly three, because they map onto its catalog, schema, and table hierarchy |
| Column changes   | `alter_columns` supports rename only. The specification also defines nullability, data type, and virtual column alterations                                                                       |
| Indexes          | The specification defines `create_index`, `create_scalar_index`, and `index`. None are served. Indexes are created through the Gravitino REST API instead                                         |

Data-plane operations are outside the implemented set entirely, including `insert`, `update`, `delete`, `merge_insert`, and `query`. Clients read and write the dataset files themselves.

One behavior is worth knowing separately. `DropTable` removes the dataset files, even though every table this service creates is marked `external`. Use `DeregisterTable` to drop the metadata and keep the files.

### Storage Options

Lance clients open dataset files directly rather than reading them through the service, so they need the storage credentials themselves. Gravitino hands those credentials over.

Set `lance.storage.*` properties on the catalog, or on an individual table to override the catalog. Schema properties are ignored. On `CreateTable` and `DescribeTable`, the service gathers those properties, strips the `lance.storage.` prefix, and returns what is left to the client as `storageOptions`.

A catalog property of `lance.storage.region = us-east-1` therefore reaches the client as `region: us-east-1`, which is the key name the Lance SDK expects.

See [Storage Options](./lakehouse-generic-catalog.md#storage-options) for the option names, and [Lance Tables](./lakehouse-generic-lance-table.md#lance-rest-api) for worked create and register examples.

## Running as a Separate Process

The service can also run as its own process, alongside a Gravitino server rather than inside it. A Gravitino server is still required, since `namespace-backend` accepts only `gravitino`. Access control is not available in this configuration. Docker and Kubernetes deployments use it.

Configuration lives in `${GRAVITINO_HOME}/conf/gravitino-lance-rest-server.conf` and uses the same property names as the Quick Start.

Callers to a separate process are authenticated by the `gravitino.authenticators` setting in `gravitino-lance-rest-server.conf`, not by the Gravitino server's. Nothing requires the two to match, so set both deliberately.

### From the Command Line

```shell
${GRAVITINO_HOME}/bin/gravitino-lance-rest-server.sh start
```

### With Docker

Start the Gravitino server first, then:

```shell
docker run -d --name lance-rest-service -p 9101:9101 \
  -e LANCE_REST_GRAVITINO_METALAKE_NAME={metalake_name} \
  -e LANCE_REST_GRAVITINO_URI=http://{gravitino_host}:8090 \
  apache/gravitino-lance-rest:latest
```

The image accepts five environment variables, which it writes into `gravitino-lance-rest-server.conf` on startup:

| Environment Variable                 | Configuration Property                    |
|--------------------------------------|-------------------------------------------|
| `LANCE_REST_GRAVITINO_METALAKE_NAME` | `gravitino.lance-rest.gravitino-metalake` |
| `LANCE_REST_GRAVITINO_URI`           | `gravitino.lance-rest.gravitino-uri`      |
| `LANCE_REST_NAMESPACE_BACKEND`       | `gravitino.lance-rest.namespace-backend`  |
| `LANCE_REST_HOST`                    | `gravitino.lance-rest.host`               |
| `LANCE_REST_PORT`                    | `gravitino.lance-rest.httpPort`           |

Any other property, including the authentication properties, must be set in a `gravitino-lance-rest-server.conf` mounted into the container. Values already present in that file are preserved.

### On Kubernetes

See [Install Lance REST Server on Kubernetes](./lance-rest-server-chart.md) for the Helm chart.

## Related Pages

- [Lance REST Integration](./lance-rest-integration.md) for `lance-spark` and `lance-ray` versions and examples
- [Lance Tables](./lakehouse-generic-lance-table.md) for table properties, type mappings, and worked examples on both APIs
- [Lakehouse Generic Catalog](./lakehouse-generic-catalog.md) for the catalog this service creates and its properties
- [Install Lance REST Server on Kubernetes](./lance-rest-server-chart.md) for the Helm chart
