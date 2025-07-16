---
title: "Apache Gravitino Trino connector"
slug: /trino-connector/trino-connector
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

Trino can manage and access data using the Trino connector provided by `Apache Gravitino`, commonly referred to as the `Gravitino Trino connector`.
After configuring the Gravitino Trino connector in Trino, Trino can automatically load catalog metadata from Gravitino, allowing users to directly access these catalogs in Trino.
Once integrated with Gravitino, Trino can operate on all Gravitino data without requiring additional configuration. 
The Gravitino Trino connector uses the [Trino dynamic catalog managed mechanism](https://trino.io/docs/current/admin/properties-catalog.html) to load catalogs.
When the Gravitino Trino connector retrieves catalogs from the Gravitino server, it generates a `CREATE CATALOG` statement and executes
the statement on the current Trino server to register the catalogs with Trino

:::note
Once metadata such as catalogs are changed in Gravitino, Trino can update itself through Gravitino, this process usually takes 
about 3~10 seconds. 
:::

By default, the loading of Gravitino's catalogs into Trino follows the naming convention:

```text
{catalog}
```

Usage in queries is as follows:

```text
SELECT * from catalog.dbname.tablename
```


