---
title: "Gravitino connector requirements"
slug: /trino-connector/requried
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---

# Rquirements


To install and deploy the Gravitino connector, the following environment setup is required:

- Trino server version should be higher than Trino-server-360, ideally using Trino-server-426. 
  There are multiple Trino versions, and we have not tested with all versions.
- Ensure that all nodes running Trino can access the Gravitino server's port, which defaults to 8090.
- Ensure that all nodes running Trino can access the real catalogs resources, such as Hive, Iceberg, MySQL, PostgreSQL, etc.
- Ensure the following connectors are installed in Trino: Hive, Iceberg, MySQL, PostgreSQL.
- Ensure that the `catalog.management` is configured as `static` in the Trino configuration.

