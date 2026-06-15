---
title: "Lineage"
slug: "/lineage/lineage"
keyword: "Gravitino Open Lineage"
license: "This software is licensed under the Apache License version 2."
---

## Overview

Lineage information is critical for metadata systems. Gravitino supports data lineage by leveraging [OpenLineage](https://openlineage.io/) and provides a specific Spark JAR to collect lineage information with the Gravitino identifier. For details, see the [Gravitino Spark lineage page](./gravitino-spark-lineage.md). Additionally, the Gravitino server provides a lineage process framework to receive, process, and sink OpenLineage events to other systems.

## Capabilities

- Supports column lineages.
- Supports lineage across diverse Gravitino catalogs like fileset, Iceberg, Hudi, Paimon, Hive, Model, etc.
- Supports Spark.
