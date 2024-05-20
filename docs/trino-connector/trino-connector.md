---
title: "Gravitino connector"
slug: /trino-connector/trino-connector
keyword: gravitino connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

Trino can manage and access data using the Trino connector provided by `Gravitino`, commonly referred to as the `Gravitino connector`.
After configuring the Gravitino connector in Trino, Trino can automatically load catalog metadata from Gravitino, allowing users to directly access these catalogs in Trino.
Once integrated with Gravitino, Trino can operate on all Gravitino data without requiring additional configuration.

:::node
Once metadata such as catalogs, schemas, or tables are changed in Gravitino, Trino can update itself through Gravitino, this process usually takes 
about 3~10 seconds. 
:::

By default, the loading of Gravitino's catalogs into Trino follows the naming convention:

```text
{catalog}
```

Usage in queries is as follows:

```text
SELECT * from catalog.dbname.tabname
```
