---
title: "Gravitino connector"
slug: /trino-connector
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---
# Trino connector for Gravitino

Trino can manage and access data using the Trino connector provided by `Gravitino`, referred to as the `Gravitino connector`.
After configuring the Gravitino connector in Trino, it can automatically load catalog metadata from Gravitino, allowing users to directly access these catalogs in Trino.
Once integrated with Gravitino, Trino can operate on all Gravitino data without requiring additional configuration.

When metadata such as catalogs, schemas, or tables change in Gravitino, Trino can also update itself through Gravitino.


The loading of Gravitino's catalogs into Trino follows the naming convention:
```text
{metalake}.{catalog}
```
Regarding `metalake` and `catalog`, you can refer to [Gravition Metalake](/metalake), [Gravition Catalog](/catalog).

Usage in queries is as follows:
```text
SELECT * from "metalake.catalog".dbname.tabname
```
