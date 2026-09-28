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

# Apache Gravitino Spark Connector

Copy this runtime JAR onto an existing Spark classpath, then configure Spark
against an Apache Gravitino server.

Image: `apache/gravitino-spark-connector:{version}`

This image is not a Spark runtime. Use it as a Kubernetes init container, or
unpack it on a VM, to copy one versioned JAR.

## Build

Build locally from the repository root with the shared image build script:

```bash
./dev/docker/build-docker.sh \
  --platform linux/amd64 \
  --type spark-connectors \
  --image apache/gravitino-spark-connector \
  --tag dev
```

The script builds the connector runtime jars (`spark-connectors-dependency.sh`),
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
catalog that never loads. Match the connector image version to the server
version, or keep it lower.

## Supported versions

Each Spark version / Scala variant ships as its own directory under
`/connectors` (named `spark-<major>_<scala>`). Across the project Spark 3.3 is
Scala 2.12 only; Spark 3.4 and later add Scala 2.13; Spark 4.0 is Scala 2.13
only. The exact set baked into an image depends on the Gravitino source branch
it was built from; list them with:

```bash
docker run --rm apache/gravitino-spark-connector:{version}
```

Each directory contains one shaded runtime JAR. `LICENSE` and `NOTICE` are in
`/licenses` in the image.

## Install

Mount `/target` and set the Spark and Scala versions. The entrypoint copies
the matching JAR into that directory.

| Variable        | Default | Description                                       |
|-----------------|---------|---------------------------------------------------|
| `SPARK_VERSION` | `3.5`   | Spark major version, e.g. `3.5`.                  |
| `SCALA_VERSION` | `2.12`  | `2.12` or `2.13`, depending on the Spark version. |

### Kubernetes

Mount an empty volume at `/target` in the init container, and at a path
**outside** `/opt/spark/jars` on both driver and executor. Put that path on
the classpath. Do not mount over `/opt/spark/jars` or `/opt/spark/conf`.

```yaml
volumes:
  - name: spark-jars
    emptyDir: {}
driver:
  volumeMounts:
    - name: spark-jars
      mountPath: /opt/gravitino/jars
  initContainers:
    - name: copy-gravitino-jars
      image: apache/gravitino-spark-connector:{version}
      env:
        - name: SPARK_VERSION
          value: "3.5"
        - name: SCALA_VERSION
          value: "2.12"
      volumeMounts:
        - name: spark-jars
          mountPath: /target
# Repeat the same volumeMount and initContainer on the executor.
sparkConf:
  "spark.plugins": "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin"
  "spark.driver.extraClassPath": "/opt/gravitino/jars/*"
  "spark.executor.extraClassPath": "/opt/gravitino/jars/*"
  "spark.sql.gravitino.uri": "http://gravitino.example.svc.cluster.local:8090"
  "spark.sql.gravitino.metalake": "test"
  "spark.sql.gravitino.authType": "basic"
  "spark.sql.gravitino.basic.username": "admin"
  "spark.sql.gravitino.basic.password": "{password}"
```

To pick up a new connector image or settings, submit a new job or roll the
pods so the init container runs again.

### VM / on-premises

Run the image once against a classpath directory, or unpack the JAR by hand:

```bash
docker run --rm \
  -e SPARK_VERSION=3.5 -e SCALA_VERSION=2.12 \
  -v /opt/gravitino/jars:/target \
  apache/gravitino-spark-connector:{version}
```

Then add that directory with `--jars` or via `spark.driver.extraClassPath`
and `spark.executor.extraClassPath`.

### The `spark.plugins` value is appended, not replaced

Packaged Spark distributions often set `spark.plugins` already. A bare
assignment silently disables the platform's own plugins. Append
`GravitinoSparkPlugin` to any existing value (comma-separated) rather than
overwriting it.

### Extra JARs

This image ships only the connector runtime.

- JDBC catalogs need the database driver on the same classpath.
- Iceberg catalogs need a matching `iceberg-spark-runtime` JAR and
  `spark.sql.gravitino.enableIcebergSupport=true`.
- Iceberg on S3 also needs `iceberg-aws-bundle` (matching the Iceberg
  version) on the driver and executor classpath. `S3FileIO` uses AWS SDK v2
  from that bundle; without it, the first use of vended credentials fails
  with `NoClassDefFoundError`. Images that reach S3 only through Hadoop S3A
  do not ship it.

## Configure

Set these in `spark-defaults.conf`, `spark-submit --conf`, or
`SparkApplication.spec.sparkConf`.

Set `spark.sql.gravitino.uri` to the server REST URL the engine can reach,
then configure Basic or OAuth2. In the cluster this is usually
`http://{service}.{namespace}.svc.cluster.local:8090`. A published HTTPS URL
works the same way; it is not tied to the auth type.

Note the two prefixes: auth settings are `spark.sql.gravitino.authType`,
`spark.sql.gravitino.basic.*`, and `spark.sql.gravitino.oauth2.*` (no
`client.` segment), while optional Gravitino client tuning uses
`spark.sql.gravitino.client.`. Both match `GravitinoSparkConfig`. The OAuth2
token path key is `spark.sql.gravitino.oauth2.tokenPath`.

### Basic

```properties
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://gravitino.example.svc.cluster.local:8090
spark.sql.gravitino.metalake=test
spark.sql.gravitino.authType=basic
spark.sql.gravitino.basic.username=admin
spark.sql.gravitino.basic.password={password}
```

### OAuth2

The connector authenticates with the client-credentials grant against any
OAuth2 server. The example below uses Azure AD; a Keycloak realm token
endpoint works the same way.

```properties
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://gravitino.example.svc.cluster.local:8090
spark.sql.gravitino.metalake=test
spark.sql.gravitino.authType=oauth2
spark.sql.gravitino.oauth2.serverUri=https://login.microsoftonline.com
spark.sql.gravitino.oauth2.tokenPath={tenant_id}/oauth2/v2.0/token
spark.sql.gravitino.oauth2.credential={client_id}:{client_secret}
spark.sql.gravitino.oauth2.scope={client_id}/.default
```

### Iceberg REST routing

`hive` and `jdbc` backed Iceberg catalogs are routed through the Gravitino
Iceberg REST server (IRC), enabled by default. The IRC endpoint is discovered
from the server automatically.

| Property                                           | Default      | Notes                                                                                                                        |
|----------------------------------------------------|--------------|------------------------------------------------------------------------------------------------------------------------------|
| `spark.sql.gravitino.iceberg.rest-routing-enabled` | `true`       | Route Iceberg catalogs through IRC. Set `false` for legacy native-backend translation.                                       |
| `spark.sql.gravitino.iceberg.rest-uri`             | (discovered) | Override the discovered IRC endpoint.                                                                                        |
| `spark.sql.gravitino.iceberg.reuseOAuth2`          | `true`       | Reuse the Gravitino OAuth2 client config for IRC. Set `false` to supply an independent IRC config or when IRC is not OAuth2. |
| `spark.sql.gravitino.iceberg.rest.`                | (none)       | Passthrough prefix for IRC client config, e.g. `rest.auth.type`.                                                             |

### Properties

| Property                                                                         | Required | Notes                                                                        |
|----------------------------------------------------------------------------------|----------|------------------------------------------------------------------------------|
| `spark.plugins`                                                                  | Yes      | `org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin` (append). |
| `spark.sql.gravitino.uri`                                                        | Yes      | Server REST URL.                                                             |
| `spark.sql.gravitino.metalake`                                                   | Yes      | Metalake name.                                                               |
| `spark.sql.gravitino.authType`                                                   | Yes      | `simple`, `basic`, or `oauth2`.                                              |
| `spark.sql.gravitino.basic.username` / `.password`                               | Basic    | Basic credentials.                                                           |
| `spark.sql.gravitino.oauth2.serverUri` / `.tokenPath` / `.credential` / `.scope` | OAuth2   | OAuth2 client-credentials settings.                                          |
| `spark.sql.gravitino.enableIcebergSupport`                                       | No       | Set `true` for Iceberg catalogs.                                             |
| `spark.sql.gravitino.iceberg.rest-routing-enabled`                               | No       | Default `true`.                                                              |
| `spark.sql.gravitino.iceberg.reuseOAuth2`                                        | No       | Default `true`.                                                              |

Optional client settings use the prefix `spark.sql.gravitino.client.`, for
example `spark.sql.gravitino.client.socketTimeoutMs`.

## Catalog names

Use the metalake catalog name directly, for example `catalog_postgres`. There
is no `<metalake>.<catalog>` form.

`SHOW CATALOGS` lists only `spark_catalog` until you `USE` another catalog.
Prefer fully qualified names:

```sql
SHOW DATABASES IN catalog_postgres;
SHOW TABLES IN catalog_postgres.public;
SELECT * FROM catalog_postgres.public.my_table LIMIT 20;
```
