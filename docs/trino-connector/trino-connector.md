---
title: "Trino Connector"
slug: "/trino-connector/trino-connector"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

Trino can manage and access data through the Apache Gravitino Trino connector (the "Gravitino Trino connector"). Once it is configured in Trino, Trino automatically loads catalog metadata from Gravitino, and users can access those catalogs directly. Trino can then operate on all Gravitino data without further configuration.

The Gravitino Trino connector uses the [Trino dynamic catalog management mechanism](https://trino.io/docs/current/admin/properties-catalog.html) to load catalogs. When it retrieves a catalog from the Gravitino server, it generates a `CREATE CATALOG` statement and executes it on the Trino server to register the catalog.

The connector supports multiple Trino versions. For supported version ranges, see [Requirements](requirements.md). Examples in this documentation use Trino `469`.

:::note
When metadata such as catalogs is changed in Gravitino, Trino picks up the change through Gravitino in about 3 to 10 seconds.
:::

By default, the loading of Gravitino's catalogs into Trino follows the naming convention:

```text
{catalog}
```

Usage in queries is as follows:

```text
SELECT * from catalog.dbname.tablename
```

