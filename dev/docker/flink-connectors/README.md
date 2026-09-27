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

# Apache Gravitino Flink Connector

Copy this runtime JAR onto an existing Flink classpath, then configure Flink
against an Apache Gravitino server.

Image: `apache/gravitino-flink-connector:{version}`

This image is not a Flink runtime. Use it as a Kubernetes init container, or
unpack it on a VM, to copy one versioned JAR.

## Build

Build locally from the repository root with the shared image build script:

```bash
./dev/docker/build-docker.sh \
  --platform linux/amd64 \
  --type flink-connectors \
  --image apache/gravitino-flink-connector \
  --tag dev
```

The script builds the connector runtime jars (`flink-connectors-dependency.sh`),
stages the repository-root `LICENSE`/`NOTICE` into `licenses/`, injects
`IMAGE_VERSION` from `gradle.properties`, and runs the multi-arch buildx build.

This image contains open source software only. The Apache Gravitino connector
code is licensed under the Apache License 2.0; the shaded runtime jar also
bundles third-party open source components under their own licenses. See the
`LICENSE`, `NOTICE` and `THIRD_PARTY_LICENSES.txt` files under `/licenses` in
the image.

## Server compatibility

The connector must not be newer than the Gravitino server it connects to.
`GravitinoClientBase` checks the version on the first metadata call, and a
connector newer than the server fails that check; the failure surfaces as a
catalog store that never loads. Match the connector image version to the
server version, or keep it lower.

## Supported versions

Each Flink minor version ships as its own directory under `/connectors`
(Scala 2.12; Flink does not support Scala 2.13). The exact set baked into an
image depends on the Gravitino source branch it was built from; list them
with:

```bash
docker run --rm apache/gravitino-flink-connector:{version}
```

Each directory contains one shaded runtime JAR. Do not mix JARs from
different Flink minor versions. `LICENSE` and `NOTICE` are in `/licenses` in
the image.

## Install

Mount `/target` and set `FLINK_VERSION`. The entrypoint copies the matching
JAR into that directory.

| Variable        | Default  | Description                        |
|-----------------|----------|------------------------------------|
| `FLINK_VERSION` | `1.20`   | Flink minor version, e.g. `1.20`.  |

### Kubernetes

Mount an empty volume at `/target` in the init container, and at
`/opt/flink/usrlib` on the Flink main container. Do not mount over
`/opt/flink/lib`. Match `FLINK_VERSION` to `spec.image` /
`spec.flinkVersion`.

```yaml
volumes:
  - name: flink-jars
    emptyDir: {}
initContainers:
  - name: copy-gravitino-jars
    image: apache/gravitino-flink-connector:{version}
    env:
      - name: FLINK_VERSION
        value: "1.20"
    volumeMounts:
      - name: flink-jars
        mountPath: /target
containers:
  - name: flink-main-container
    volumeMounts:
      - name: flink-jars
        mountPath: /opt/flink/usrlib
```

To pick up a new connector image or settings, redeploy the `FlinkDeployment`
so the init container runs again.

### VM / on-premises

Run the image once against the Flink `lib` directory, or unpack the JAR by
hand:

```bash
docker run --rm \
  -e FLINK_VERSION=1.20 \
  -v /opt/flink/lib:/target \
  apache/gravitino-flink-connector:{version}
```

Then restart JobManager and TaskManager so the catalog store loads.

### Extra JARs

This image ships only the connector runtime. JDBC catalogs also need, on the
same classpath:

- the Flink JDBC connector for that Flink minor, for example
  `flink-connector-jdbc-3.3.0-1.20.jar` for Flink 1.20
- the database JDBC driver

For PostgreSQL catalogs, set `jdbc-database` and
`flink.bypass.default-database` so `USE CATALOG` has a default database.

## Configure

Set these in `flink-conf.yaml`, `FlinkDeployment.spec.flinkConfiguration`, or
`TableEnvironment`.

Set `table.catalog-store.gravitino.gravitino.uri` to the server REST URL the
engine can reach, then configure Basic or OAuth2. In the cluster this is
usually `http://{service}.{namespace}.svc.cluster.local:8090`. A published
HTTPS URL works the same way; it is not tied to the auth type.

Auth type is `client.auth.type`. The OAuth2 token path key is `tokenPath`.

### Basic

```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.uri: http://gravitino.example.svc.cluster.local:8090
table.catalog-store.gravitino.gravitino.metalake: test
table.catalog-store.gravitino.gravitino.client.auth.type: basic
table.catalog-store.gravitino.gravitino.client.basic.username: admin
table.catalog-store.gravitino.gravitino.client.basic.password: {password}
```

### OAuth2

The connector authenticates with the client-credentials grant against any
OAuth2 server. The example below uses Azure AD; a Keycloak realm token
endpoint works the same way.

```yaml
table.catalog-store.kind: gravitino
table.catalog-store.gravitino.gravitino.uri: http://gravitino.example.svc.cluster.local:8090
table.catalog-store.gravitino.gravitino.metalake: test
table.catalog-store.gravitino.gravitino.client.auth.type: oauth2
table.catalog-store.gravitino.gravitino.client.oauth2.serverUri: https://login.microsoftonline.com
table.catalog-store.gravitino.gravitino.client.oauth2.tokenPath: {tenant_id}/oauth2/v2.0/token
table.catalog-store.gravitino.gravitino.client.oauth2.credential: {client_id}:{client_secret}
table.catalog-store.gravitino.gravitino.client.oauth2.scope: {client_id}/.default
```

### Properties

| Property                                                                                                    | Required | Notes                               |
|-------------------------------------------------------------------------------------------------------------|----------|-------------------------------------|
| `table.catalog-store.kind`                                                                                  | Yes      | Must be `gravitino`.                |
| `table.catalog-store.gravitino.gravitino.uri`                                                               | Yes      | Server REST URL.                    |
| `table.catalog-store.gravitino.gravitino.metalake`                                                          | Yes      | Metalake name.                      |
| `table.catalog-store.gravitino.gravitino.client.auth.type`                                                  | Yes      | `basic` or `oauth2`.                |
| `table.catalog-store.gravitino.gravitino.client.basic.username` / `.password`                               | Basic    | Basic credentials.                  |
| `table.catalog-store.gravitino.gravitino.client.oauth2.serverUri` / `.tokenPath` / `.credential` / `.scope` | OAuth2   | OAuth2 client-credentials settings. |

Optional client settings use the prefix
`table.catalog-store.gravitino.gravitino.client.`, for example
`table.catalog-store.gravitino.gravitino.client.socketTimeoutMs`.

## Catalog names

Use the metalake catalog name directly, for example `catalog_postgres`. There
is no `<metalake>.<catalog>` form. Names must not start with a digit.

```sql
SHOW CATALOGS;
USE CATALOG catalog_postgres;
SHOW DATABASES;
SHOW TABLES FROM `public`;
SELECT * FROM catalog_postgres.`public`.my_table LIMIT 20;
```

Session-only catalogs
(`table.catalog-store.gravitino.gravitino.enableSessionCatalogSupport=true`)
do not survive a redeploy.
