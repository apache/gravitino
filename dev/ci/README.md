<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# CI test shards

Fork CI keeps one reusable workflow per suite and splits long suites into independent runners:

| Suite | Shards | Coverage |
| --- | --- | --- |
| Build | `docker`, `others` | Existing non-core build checks and unit tests; core keeps its four dedicated lanes |
| Backend IT | `hive`, `lakehouse`, `others` | Each shard runs on H2 embedded, MySQL deploy, and PostgreSQL deploy |
| Flink IT | `embedded`, `deploy` | Each mode runs every supported Flink minor version |
| Access Control IT | `h2`, `mysql`, `postgresql` | H2 embedded and both deploy backends, retaining the existing unit tests |

`test-shards.sh` owns the task and case selections. Build and Backend IT use a catch-all `others`
shard, so new projects remain included. Backend IT assigns Iceberg's `experimentalTest` to the
lakehouse shard. To inspect the selected Gradle arguments or integration cases:

```bash
dev/ci/test-shards.sh backend-it --list
dev/ci/test-shards.sh backend-it lakehouse
dev/ci/test-shards.sh flink embedded
```

To reproduce a Flink or Access Control shard from a checkout with Docker and the required JDK:

```bash
dev/ci/run-integration-shard.sh flink embedded
dev/ci/run-integration-shard.sh access-control mysql
```

The `all` selector preserves the complete, unsharded integration suite. Cron/manual integration
callers use it explicitly and retain their existing timeout budgets. The shorter-job target applies
to the sharded Fork CI workflows; it does not change those full-suite callers. Gradle's shared test
environment lock remains enabled within each runner because integration projects share servers,
containers, databases, and configuration files.

Run the shard contract tests with `python3 dev/ci/test_test_shards.py`. They execute workflow commands
and the integration runner against a recording Gradle stub, compare Flink cases with the project
registry, and check failure propagation, task ownership, exclusions, and complete-suite selectors.
They do not execute the Java/Docker tests; a real CI run must verify their runtime results.

Measure queue time and execution time separately when rebalancing. Compare the union of test
results for the same backend/mode before and after a split, and record cache hits, setup/build/test
times, total runner-minutes, and end-to-end completion time. More shards duplicate setup and can
increase queueing. This layout adds ten test jobs compared with the unsharded workflows, without
adding top-level Fork CI suites. Do not remove a supported backend or version to meet a time target,
or infer a faster complete run solely from a shorter individual job.
