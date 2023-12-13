---
title: "Gravitino connector"
slug: /trino-connector/trino-connector
keyword: gravitino connector trino
license: "Copyright 2023 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2."
---

Trino can manage and access data using the Trino connector provided by `Gravitino`, commonly referred to as the `Gravitino connector`.
After configuring the Gravitino connector in Trino, it can automatically load catalog metadata from Gravitino, allowing users to directly access these catalogs in Trino.
Once integrated with Gravitino, Trino can operate on all Gravitino data without requiring additional configuration.

When metadata such as catalogs, schemas, or tables change in Gravitino, Trino can also update itself through Gravitino.


The loading of Gravitino's catalogs into Trino follows the naming convention:

```text
{metalake}.{catalog}
```

Regarding `metalake` and `catalog`, 
you can refer to [Create a Metalake](../manage-metadata-using-gravitino.md#create-a-metalake), [Create a Catalog](../manage-metadata-using-gravitino.md#create-a-catalog).

Usage in queries is as follows:

```text
SELECT * from "metalake.catalog".dbname.tabname
```