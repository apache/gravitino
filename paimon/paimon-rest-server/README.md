<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Gravitino Paimon REST Catalog Service

Flink and Spark use the native Paimon REST catalog client (tested with 1.4.2, 2.0.0 and 2.1.0) to manage tables
and commit snapshots. The engines read and write data files directly in the warehouse.

## Run the service

Use Java 17 or newer. From the root of an extracted standalone distribution, edit
`conf/gravitino-paimon-rest-server.conf` and run:

```sh
bin/gravitino-paimon-rest-server.sh run
```

The default endpoint is `http://127.0.0.1:9002/paimon/`. Check readiness at
`/paimon/health/ready`. For local testing, set `gravitino.paimon-rest.token` to a
test token and use the same value in the client. For OAuth deployments, configure the
Gravitino OAuth authenticator and provide the engine with an access token from the
configured identity provider.

```properties
metastore = rest
uri = http://127.0.0.1:9002/paimon/
warehouse = default_catalog
token.provider = bear
token = <access-token>
```

Set the warehouse location in the server configuration. For distributed execution, all
workers and the REST server need access to the same storage. Enable checkpointing
for Flink streaming writes.

## Serve registered Gravitino catalogs

Set `gravitino.paimon-rest.catalog-config-provider = dynamic-config-provider` and
`gravitino.paimon-rest.gravitino-metalake` to the source metalake. Use the registered
catalog name as the engine's `warehouse`.

Auxiliary mode uses in-process catalog leases and Gravitino metadata authorization.
Standalone mode connects through an authenticated `GravitinoClient`, configured with
`gravitino-uri` and Simple or OAuth client credentials. Dynamic entries have a fixed
cache lifetime. S3/OSS credentials use their actual expiry and refresh before Paimon's
one-hour safety window. Built-in S3/OSS providers issue read/write tokens for the configured
warehouse prefix in both deployment modes. The [service guide](../../docs/paimon-rest-service.md#registered-gravitino-catalogs)
contains deployment, permission and credential lifetime settings.

## Build and test from source

```sh
./gradlew :paimon:paimon-common:check :paimon:paimon-rest-server:check
./gradlew assemblePaimonRESTServer
```

The archive and its checksum are written to `distribution/`. The test suite includes
Flink checkpoint commits, Spark appends, and a Flink batch read of the combined result.
The source repository's [Paimon REST service guide](../../docs/paimon-rest-service.md)
covers auxiliary deployment, catalog backends, and engine configuration.

### Packaged catalog regression tests

Build the main and standalone distributions, then run the following from the repository root
using Python 3.12 or later and JDK 17.

```sh
python3 paimon/paimon-rest-server/src/test/python/test_packaged_catalog.py \
  distribution/gravitino-2.0.0-SNAPSHOT-bin.tar.gz \
  distribution/gravitino-paimon-rest-server-2.0.0-SNAPSHOT-bin.tar.gz
```

The test registers STS catalogs through the Gravitino API, exercises S3 token vending through
both launchers, checks OSS FileIO in both archives, and verifies DDL behavior with the main API.
The S3 fixture checks each request against the corresponding issued session policy, including
actions, resources, `StringLike`/`StringEquals` conditions and explicit denies. Missing-object
probes require both object-read and bucket-list permission for the probed key. Negative tests
remove each grant, change the allowed list prefix, or add an explicit deny. Bucket-root
warehouses are tested with and without a trailing slash, including descendant listings.
DDL tests cover invalid schema and column names, stale schema and table registrations,
schema copying and identity-property protection. The base service tests reject UUID changes
before applying an alteration batch, keep existing writers usable, and cover tag snapshots,
empty-table responses and snapshot file statistics.
Cloud requests use local HTTP fixtures.
