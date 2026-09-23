<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Gravitino Trino Connector

Copy this plugin into an existing Trino or Starburst installation, then
configure it against an Apache Gravitino server.

Image: `apache/gravitino-trino-connector:{version}`

This image is not a Trino server. Use it as a Kubernetes init container, or
unpack it on a VM, to place one version band under Trino's `plugin/`
directory.

## Build

Build locally from the repository root with the shared image build script:

```bash
./dev/docker/build-docker.sh \
  --platform linux/amd64 \
  --type trino-connectors \
  --image apache/gravitino-trino-connector \
  --tag dev
```

The script builds the connector bands (`trino-connectors-dependency.sh`),
stages the repository-root `LICENSE`/`NOTICE` into `licenses/`, injects
`IMAGE_VERSION` from `gradle.properties`, and runs the multi-arch buildx build.

This image contains open source software only. The Apache Gravitino connector
code is licensed under the Apache License 2.0; each plugin band also bundles
third-party open source dependency jars under their own licenses. See the
`LICENSE`, `NOTICE` and `THIRD_PARTY_LICENSES.txt` files under `/licenses` in
the image.

## Server compatibility

The connector must not be newer than the Gravitino server it connects to.
`GravitinoClientBase` checks the version on the first metadata poll, and a
connector newer than the server fails that check inside `loadMetalake`; the
failure is swallowed, so no catalog appears and nothing is logged. Match the
connector image version to the server version, or keep it lower.

On an unsupported Trino version, set
`gravitino.trino.skip-version-validation=true` to load anyway (untested).

## Supported versions

Each Trino/Starburst version band ships as its own plugin directory under
`/connectors`. The exact set of bands baked into an image depends on the
Gravitino source branch it was built from; list them with:

```bash
docker run --rm apache/gravitino-trino-connector:{version}
```

Each band is a full plugin directory: the connector JAR, its dependencies,
`LICENSE`, `NOTICE`, and this README. Install **exactly one** band.

## JDBC drivers

This image ships **no** JDBC drivers, by design. A Gravitino JDBC catalog
(for example MySQL or PostgreSQL) is served by Trino's own `mysql` /
`postgresql` connector, and the JDBC driver comes from that Trino plugin
(`plugin/mysql`, `plugin/postgresql`) — not from this image. Standard Trino and
Starburst distributions already include those plugins, so no driver setup is
needed.

If you run your own Trino and have removed the `mysql` or `postgresql` plugin,
reinstall it (that is where the driver lives); a driver placed in the Gravitino
plugin directory would not be used, as Trino isolates each plugin's classpath.

## Install

Mount `/target` and set `TRINO_VERSION`. The entrypoint copies the matching
band into that directory.

| Variable        | Default | Description                                             |
|-----------------|---------|---------------------------------------------------------|
| `TRINO_VERSION` | `478`   | Trino server version. Resolved to a band automatically. |

### Kubernetes

Mount an empty volume at `/target` in the init container, and at the plugin
path on the coordinator and every worker. The last path component must be
`gravitino`.

The usual plugin path on the official Helm chart is
`/usr/lib/trino/plugin/gravitino`.

```yaml
volumes:
  - name: gravitino-plugin
    emptyDir: {}
initContainers:
  - name: install-gravitino-connector
    image: apache/gravitino-trino-connector:{version}
    env:
      - name: TRINO_VERSION
        value: "477"
    volumeMounts:
      - name: gravitino-plugin
        mountPath: /target
containers:
  - name: trino
    volumeMounts:
      - name: gravitino-plugin
        mountPath: /usr/lib/trino/plugin/gravitino
```

To pick up a new connector image or catalog properties, roll the coordinator
and worker pods so the init container runs again.

### VM / on-premises

Run the image once against the target plugin directory, or unpack a band by
hand:

```bash
docker run --rm \
  -e TRINO_VERSION=477 \
  -v /usr/lib/trino/plugin/gravitino:/target \
  apache/gravitino-trino-connector:{version}
```

The directory name under `plugin/` must be `gravitino`. Repeat on the
coordinator and every worker, then restart each Trino process.

### Trino server settings

On the coordinator:

```properties
catalog.management=dynamic
```

On Kubernetes, when catalogs are stored on disk, also set on the coordinator:

```properties
catalog.store=file
```

### Logging

The connector emits nothing at any level unless the coordinator JVM is
started with a Log4j 2 configuration file:

```
-Dlog4j.configurationFile=/etc/trino/log4j2.properties
```

Without it, the version and registration failures above are invisible.

## Configure

Create the Trino catalog file
`/etc/trino/catalog/gravitino.properties`. On the official Helm chart, set
the same keys under `catalogs.gravitino`; the chart writes that file. The
file name (`gravitino`) is the Trino catalog that hosts the connector. It is
not the plugin directory under `/usr/lib/trino/plugin/gravitino`.

Set `gravitino.uri` to the server REST URL the engine can reach, then
configure Basic or OAuth2. In the cluster this is usually
`http://{service}.{namespace}.svc.cluster.local:8090`. A published HTTPS URL
works the same way; it is not tied to the auth type.

`gravitino.metalake` is optional. Omit it (or leave it empty) to load catalogs
from every metalake. Set it when you want a single metalake only.

`gravitino.client.authType` accepts `simple`, `basic`, `oauth2`, or
`kerberos`. The OAuth2 token path key is `gravitino.client.oauth2.path`.

### Basic

On a multi-node Trino cluster, also map the password to an env var (see
[Worker credentials](#worker-credentials-on-a-distributed-cluster)) and set
that env var on every pod.

```properties
connector.name=gravitino
gravitino.uri=http://gravitino.example.svc.cluster.local:8090
# Optional: omit gravitino.metalake to load every metalake
gravitino.use-single-metalake=true
gravitino.client.authType=basic
gravitino.client.basic.username=admin
gravitino.client.basic.password={password}
gravitino.dynamic-catalog.environment-variable.gravitino.client.basic.password=GRAVITINO_BASIC_PASSWORD
gravitino.iceberg.rest-uri=http://gravitino.example.svc.cluster.local:9001/iceberg/
```

### OAuth2

The connector authenticates with the client-credentials grant against any
OAuth2 server. The example below uses Azure AD; a Keycloak realm token
endpoint works the same way. On a distributed cluster, map the credentials
to env vars as shown.

```properties
connector.name=gravitino
gravitino.uri=http://gravitino.example.svc.cluster.local:8090
# Optional: omit gravitino.metalake to load every metalake
gravitino.use-single-metalake=true
gravitino.client.authType=oauth2
gravitino.client.oauth2.serverUri=https://login.microsoftonline.com
gravitino.client.oauth2.credential={client_id}:{client_secret}
gravitino.client.oauth2.path={tenant_id}/oauth2/v2.0/token
gravitino.client.oauth2.scope={client_id}/.default
gravitino.dynamic-catalog.environment-variable.gravitino.client.oauth2.credential=GRAVITINO_CLIENT_CREDENTIAL
gravitino.iceberg.rest-uri=http://gravitino.example.svc.cluster.local:9001/iceberg/
gravitino.iceberg.rest-catalog.security=OAUTH2
gravitino.iceberg.rest-catalog.oauth2.server-uri=https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
gravitino.iceberg.rest-catalog.oauth2.credential={client_id}:{client_secret}
gravitino.iceberg.rest-catalog.oauth2.scope={client_id}/.default
gravitino.dynamic-catalog.environment-variable.gravitino.iceberg.rest-catalog.oauth2.credential=IRC_CLIENT_CREDENTIAL
```

### Worker credentials on a distributed cluster

The connector registers catalogs by issuing `CREATE CATALOG` over JDBC to the
coordinator. Secret values are stripped from the catalog definition sent to
workers, so a literal password or credential leaves distributed queries
failing with `REMOTE_TASK_ERROR` or missing OAuth credential. Reference
secrets by environment variable instead: map a property to an env var with
the `gravitino.dynamic-catalog.environment-variable.` prefix, and the
connector writes `${ENV:...}` into the worker catalog definition.

```properties
# Property -> env var. The connector emits the property as '${ENV:VAR}' in the worker catalog.
gravitino.dynamic-catalog.environment-variable.gravitino.client.basic.password=GRAVITINO_BASIC_PASSWORD
gravitino.dynamic-catalog.environment-variable.gravitino.client.oauth2.credential=GRAVITINO_CLIENT_CREDENTIAL
gravitino.dynamic-catalog.environment-variable.gravitino.iceberg.rest-catalog.oauth2.credential=IRC_CLIENT_CREDENTIAL
```

Set the matching env vars (`GRAVITINO_BASIC_PASSWORD`,
`GRAVITINO_CLIENT_CREDENTIAL`, `IRC_CLIENT_CREDENTIAL`, etc.) on the
coordinator and every worker. On Kubernetes with the official Trino Helm
chart, put them in a Secret and mount with `envFrom` / `secretRef` so every
pod receives the same values.

### Connecting to a TLS-enabled coordinator

The internal JDBC connection to the coordinator must trust the coordinator's
certificate, or `CREATE CATALOG` fails with a PKIX error and no catalogs
appear. Configure the `trino.jdbc.*` properties in the same catalog file:

```properties
trino.jdbc.user=admin
trino.jdbc.password={trino_password}
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.jks
trino.jdbc.ssl.truststore.password={truststore_password}
# Required when the deployment only allows CREATE CATALOG with a privileged role.
trino.jdbc.roles=system:sysadmin
```

`trino.jdbc.ssl.enabled` may be omitted when `discovery.uri` uses `https`; it
is derived from that scheme. The `trino.jdbc.*` values are used by the
coordinator only and are never copied into the catalogs the connector
creates.

### Iceberg REST routing

`lakehouse-iceberg` catalogs are routed through the Gravitino Iceberg REST
server (IRC), enabled by default. The IRC endpoint is discovered from the
server automatically.

Override `gravitino.iceberg.rest-uri` when the discovered address is wrong for
the Trino network — for example when Gravitino advertises
`http://...:9001` but Trino must use a Service DNS name, Ingress, or another
reachable base URL ending in `/iceberg/`. Without a reachable URI, Iceberg
queries fail with connection refused.

When IRC itself requires OAuth2, pass settings under
`gravitino.iceberg.rest-catalog.` (rewritten to `iceberg.rest-catalog.`) and
map the credential with
`gravitino.dynamic-catalog.environment-variable.gravitino.iceberg.rest-catalog.oauth2.credential`
so workers can resolve it.

| Property                                 | Default      | Notes                                                                                                           |
|------------------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------|
| `gravitino.iceberg.rest-routing-enabled` | `true`       | Route non-REST Iceberg catalogs through IRC. Set `false` for legacy catalog-backend translation.                |
| `gravitino.iceberg.rest-uri`             | (discovered) | Override the discovered IRC endpoint. In multi-metalake mode use `gravitino.iceberg.rest-uri.{metalake}`.       |
| `gravitino.iceberg.rest-catalog.`        | (none)       | Passthrough prefix rewritten to `iceberg.rest-catalog.`, e.g. `gravitino.iceberg.rest-catalog.security=OAUTH2`. |

### Identity forwarding

To have the Gravitino server authorize each end user instead of the shared
service identity, forward the Trino session user:

| Property                                          | Default | Notes                                                                       |
|---------------------------------------------------|---------|-----------------------------------------------------------------------------|
| `gravitino.client.session.forwardUser`            | `false` | Per-session Gravitino client. Supported with `authType=simple` or `oauth2`. |
| `gravitino.client.session.userTokenCredentialKey` | `token` | Extra-credential key carrying the forwarded OAuth2 token.                   |

### Properties

| Property                                                                 | Required | Notes                                                     |
|--------------------------------------------------------------------------|----------|-----------------------------------------------------------|
| `connector.name`                                                         | Yes      | Must be `gravitino`.                                      |
| `gravitino.metalake`                                                     | No       | One metalake. Omit to load every metalake.                |
| `gravitino.uri`                                                          | Yes      | Server REST URL.                                          |
| `gravitino.use-single-metalake`                                          | No       | Default `true`. See catalog names.                        |
| `gravitino.client.authType`                                              | Yes      | `simple`, `basic`, `oauth2`, or `kerberos`.               |
| `gravitino.client.basic.username` / `.password`                          | Basic    | Basic credentials.                                        |
| `gravitino.client.oauth2.serverUri` / `.path` / `.credential` / `.scope` | OAuth2   | OAuth2 client-credentials settings.                       |
| `gravitino.dynamic-catalog.environment-variable.`                        | No       | Map secret properties to worker env vars.                 |
| `trino.jdbc.user` / `.password`                                          | No       | Internal JDBC connection to the coordinator.              |
| `trino.jdbc.ssl.enabled`                                                 | No       | Derived from `discovery.uri` scheme when unset.           |
| `trino.jdbc.ssl.truststore.path` / `.password` / `.type`                 | No       | Coordinator certificate trust.                            |
| `trino.jdbc.ssl.keystore.path` / `.password` / `.type`                   | No       | Client certificate for mutual TLS.                        |
| `trino.jdbc.ssl.verification`                                            | No       | `FULL` (default), `CA`, or `NONE` (troubleshooting only). |
| `trino.jdbc.roles`                                                       | No       | Session roles, e.g. `system:sysadmin`.                    |
| `trino.jdbc.properties.`                                                 | No       | Passthrough prefix for arbitrary JDBC driver properties.  |
| `gravitino.iceberg.rest-routing-enabled`                                 | No       | Default `true`.                                           |
| `gravitino.iceberg.rest-uri`                                             | No       | Override discovered IRC endpoint when unreachable.        |
| `gravitino.iceberg.rest-catalog.`                                        | No       | IRC passthrough prefix.                                   |
| `gravitino.client.session.forwardUser`                                   | No       | Forward the session user.                                 |
| `gravitino.trino.skip-version-validation`                                | No       | Default `false`.                                          |

Optional client settings use the prefix `gravitino.client.`, for example
`gravitino.client.socketTimeoutMs`.

New metalake catalogs can appear after the plugin is loaded, on the refresh
interval (`gravitino.metadata.refresh-interval-seconds`, default `10`).

## Catalog names

- `gravitino.use-single-metalake=true` (default): `<catalog_name>`, for
  example `hive`.
- `gravitino.use-single-metalake=false`: `<metalake_name>.<catalog_name>`,
  for example `test.hive`.

`gravitino` is the connector catalog. Metalake catalogs are registered
separately.

```sql
SHOW CATALOGS;
```
