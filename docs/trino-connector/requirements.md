---
title: "Trino Connector Requirements"
slug: "/trino-connector/requirements"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

To install and deploy the Apache Gravitino Trino connector:

- Use a Trino server between Trino-server-435 and Trino-server-478. Examples in this documentation use Trino `469`.
- For an unsupported Trino version, set `gravitino.trino.skip-version-validation` to `true`. Unsupported versions are not thoroughly tested.
- All nodes running Trino must be able to reach the Gravitino server port (default 8090).
- All nodes running Trino must be able to reach the real catalog resources (Hive, Iceberg, MySQL, PostgreSQL, and so on).
- The Hive, Iceberg, MySQL, and PostgreSQL connectors must be installed in Trino.
- The Trino coordinator must have `catalog.management` set to `dynamic`.
