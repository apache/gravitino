---
title: "Connect Daft to Iceberg REST"
sidebar_label: "Daft"
---

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

# Connect Daft to Iceberg REST

Daft can read and append to existing Iceberg tables exposed by Gravitino's
[Iceberg REST service](../iceberg-rest-service.md). PyIceberg loads the table
through REST; Daft scans data files and commits append writes using that table.
This path uses Daft's native Iceberg APIs rather than `Catalog.from_gravitino()`.

## Contract test

Run from the repository root with JDK 17:

```bash
./gradlew compileDistribution -PskipWeb=true -x test
./gradlew -PpythonVersion=3.12 -PskipDockerTests=true -PskipWeb=true \
  :clients:client-python:daftIcebergIT
```

The task installs the versions pinned in
`clients/client-python/requirements-daft-iceberg.txt` in an isolated environment.
The environment does not install the Gravitino Python client, whose dependencies
conflict with the supported PyIceberg version. Default Python client dependencies
are unaffected.

The fixture starts an owned Gravitino process with the dynamic Iceberg REST
configuration provider, a memory-backed catalog, unique local ports, and a
temporary warehouse. PyIceberg creates a namespace and an empty table. Daft
reads the empty table, appends two batches including a NULL value, and reloads
the result through REST. Assertions cover field order, all row values, filtering,
and independent PyIceberg readback.

The fixture removes its metadata, stops its process, restores the original
configuration, and removes its temporary warehouse. Startup and cleanup errors
fail the test. Logs are retained under
`clients/client-python/build/daft-iceberg-it-logs/`.

The dedicated task requires all optional dependencies. Default Python integration
discovery defers this owned-server fixture to the dedicated task. This test covers the Native
runner and existing-table reads and append writes; it does not certify Daft Catalog
discovery, GVFS, credential vending, or distributed execution.
