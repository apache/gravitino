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

# External test endpoints

Integration tests can reach a Hive, Kafka or MySQL that runs elsewhere, such as on a Kubernetes
cluster, instead of starting a Testcontainers container. A service is external when the endpoints
file sets `containers: external` (every service) or `<service>.external: true`.

The endpoints file is YAML, bound to the records in `TestEndpoints`, which are the only declaration
of the keys. Pass it with `-Pgravitino.test.endpoints.file=<path>` (a relative path is resolved
against the repository root), or keep it as `dev/test-endpoints/<name>.yaml` and pass
`-Pgravitino.test.profile=<name>`:

```shell
docker run -d --rm -p 9092:9092 apache/kafka:3.7.0
./gradlew :catalogs:catalog-kafka:test -PskipDockerTests=false -Pgravitino.test.profile=local-kafka
```

A flat key overrides the file, so one value of a shared file can change on the command line. In
order of precedence: the system property `gravitino.test.<service>[.<instance>].<key>` (Gradle
forwards every `-Pgravitino.test.*` and `-Dgravitino.test.*` to the test JVMs), then the
environment variable `GRAVITINO_TEST_<SERVICE>[_<INSTANCE>]_<KEY>` (upper case, `.` and `-` as
`_`), then the file. A list value is comma-separated and a map value is `k1=v1,k2=v2`:

```shell
./gradlew :catalogs:catalog-kafka:test -PskipDockerTests=false \
  -Pgravitino.test.kafka.external=true -Pgravitino.test.kafka.bootstrap-servers=localhost:9092
```

The run fails before any container starts when the file or a flat key has a key that
`TestEndpoints` does not declare (the error names the closest declared key), or when an external
service misses a required key. A test that asks for a named instance reads that instance's block
under `<service>.instances` whole; any other instance reads the service's own keys.

[`external-endpoints.example.yaml`](../../../integration-test-common/src/test/resources/external-endpoints.example.yaml)
sets every key. Its first line points editors with the YAML language server (VS Code, IntelliJ) at
[`test-endpoints.schema.json`](../../../integration-test-common/src/test/resources/test-endpoints.schema.json),
for completion and inline errors; add the same line to your own file.

The schema and the table below are generated from `TestEndpoints`. `TestExternalEndpoints` fails
when either is stale; rewrite them with
`./gradlew :integration-test-common:test --tests '*TestExternalEndpoints' -Pgravitino.test.updateGenerated=true`.

<!-- BEGIN GENERATED: external keys -->
| YAML key | Property | Environment variable | Required | Description |
| --- | --- | --- | --- | --- |
| `containers` | `gravitino.test.containers` | `GRAVITINO_TEST_CONTAINERS` | no | external makes every service external, so the run needs no Docker; default managed |
| `hive.external` | `gravitino.test.hive.external` | `GRAVITINO_TEST_HIVE_EXTERNAL` | no | true to reach the service instead of starting a container |
| `hive.metastore-uri` | `gravitino.test.hive.metastore-uri` | `GRAVITINO_TEST_HIVE_METASTORE_URI` | yes | thrift:// address of the Hive metastore |
| `hive.hdfs-uri` | `gravitino.test.hive.hdfs-uri` | `GRAVITINO_TEST_HIVE_HDFS_URI` | yes | hdfs:// address of the HDFS NameNode the metastore uses |
| `hive.hdfs-client-conf` | `gravitino.test.hive.hdfs-client-conf` | `GRAVITINO_TEST_HIVE_HDFS_CLIENT_CONF` | no | Hadoop client settings for the test JVM, such as dfs.client.use.datanode.hostname: "true" |
| `hive.hadoop-user` | `gravitino.test.hive.hadoop-user` | `GRAVITINO_TEST_HIVE_HADOOP_USER` | no | HDFS user that tests act as; default root |
| `kafka.external` | `gravitino.test.kafka.external` | `GRAVITINO_TEST_KAFKA_EXTERNAL` | no | true to reach the service instead of starting a container |
| `kafka.bootstrap-servers` | `gravitino.test.kafka.bootstrap-servers` | `GRAVITINO_TEST_KAFKA_BOOTSTRAP_SERVERS` | yes | host:port of each broker |
| `mysql.external` | `gravitino.test.mysql.external` | `GRAVITINO_TEST_MYSQL_EXTERNAL` | no | true to reach the service instead of starting a container |
| `mysql.jdbc-url` | `gravitino.test.mysql.jdbc-url` | `GRAVITINO_TEST_MYSQL_JDBC_URL` | yes | jdbc:mysql://host:port of the server, without a database |
| `mysql.user` | `gravitino.test.mysql.user` | `GRAVITINO_TEST_MYSQL_USER` | yes | user that tests connect and create databases as |
| `mysql.password` | `gravitino.test.mysql.password` | `GRAVITINO_TEST_MYSQL_PASSWORD` | yes | password of that user |
<!-- END GENERATED: external keys -->
