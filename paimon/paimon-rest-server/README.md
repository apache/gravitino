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

## Build and test from source

```sh
./gradlew :paimon:paimon-common:check :paimon:paimon-rest-server:check
./gradlew assemblePaimonRESTServer
```

The archive and its checksum are written to `distribution/`. The test suite includes
Flink checkpoint commits, Spark appends, and a Flink batch read of the combined result.
The source repository's [Paimon REST service guide](../../docs/paimon-rest-service.md)
covers auxiliary deployment, catalog backends, and engine configuration.
