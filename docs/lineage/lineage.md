---
title: "Apache Gravitino Lineage support"
slug: /lineage/lineage
keyword: Gravitino Open Lineage
license: "This software is licensed under the Apache License version 2."
---

## Overview

Lineage information is critical for metadata systems, Gravitino supports data lineage by leveraging [OpenLineage](https://openlineage.io/). Gravitino provides a specific Spark jar to collect lineage information with Gravitino identifier, please refer to [Gravitino Spark lineage page](./gravitino-spark-lineage.md). Additional, Gravitino server provides lineage process framework to receive, process and sink Open lineage events to other systems.

## Capabilities

- Supports column lineages.
- Supports lineage across diverse Gravitino catalogs like fileset, Iceberg, Hudi, Paimon, Hive, Model, etc.
- Supports Spark.
