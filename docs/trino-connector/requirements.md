---
title: "Apache Gravitino Trino connector requirements"
slug: /trino-connector/requirements
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

To install and deploy the Apache Gravitino Trino connector, The following environmental setup is necessary:

- Trino server version should be between Trino-server-435 and Trino-server-439. 
  If you use other versions of Trino, you can set the `gravitino.trino.skip-version-validation` to `true`.
  Other versions of Trino have not undergone thorough testing.
- Ensure that all nodes running Trino can access the Gravitino server's port, which defaults to 8090.
- Ensure that all nodes running Trino can access the real catalogs resources, such as Hive, Iceberg, MySQL, PostgreSQL, etc.
- Ensure that you have installed the following connectors in Trino: Hive, Iceberg, MySQL, PostgreSQL.
- Ensure that you have set the `catalog.management` to `dynamic` in the Trino coordinator configuration.
