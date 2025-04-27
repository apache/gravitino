---
title: "Gravitino Spark Lineage support"
slug: /lineage/gravitino-spark-lineage
keyword: Gravitino Spark OpenLineage
license: "This software is licensed under the Apache License version 2."
---

## Overview

By leveraging OpenLineage Spark plugin, Gravitino provides a separate Spark plugin to extract data lineage and transform the dataset identifier to Gravitino identifier.

## Capabilities

- Supports column lineage.
- Supports lineage across different catalogs like like fileset, Iceberg, Hudi, Paimon, Hive, Model, etc.
- Supports extract Gravitino dataset from GVFS.
- Supports Gravitino spark connector and non Gravitino Spark connector.

## Gravitino dataset

The Gravitino OpenLineage Spark plugin transforms the Gravitino metalake name into the dataset namespace. The dataset name varies by dataset type when generating lineage information.

When using the [Gravitino Spark connector](/spark-connector/spark-connector.md) to access tables managed by Gravitino, the dataset name follows this format:

| Dataset Type    | Dataset name                                         | Example                    | Since Version    |
|-----------------|------------------------------------------------------|----------------------------|------------------|
| Hive catalog    | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `hive_catalog.db.student`  | 0.9.0-incubating |
| Iceberg catalog | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `iceberg_catalog.db.score` | 0.9.0-incubating |
| Paimon catalog  | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `paimon_catalog.db.detail` | 0.9.0-incubating |
| JDBC catalog    | `${GravitinoCatalogName}.${schemaName}.${tableName}` | `jdbc_catalog.db.score`    | 0.9.0-incubating |

For datasets not managed by Gravitino, the dataset name is as follows:

| Dataset Type | Dataset name                                | Example                               | Since Version    |
|--------------|---------------------------------------------|---------------------------------------|------------------|
| Hive         | `spark_catalog.${schemaName}.${tableName}`  | `spark_catalog.db.table`              | 0.9.0-incubating |
| Iceberg      | `${catalogName}.${schemaName}.${tableName}` | `iceberg_catalog.db.table`            | 0.9.0-incubating |
| JDBC v2      | `${catalogName}.${schemaName}.${tableName}` | `jdbc_catalog.db.table`               | 0.9.0-incubating |
| JDBC v1      | `spark_catalog.${schemaName}.${tableName}`  | `spark_catalog.postgres.public.table` | 0.9.0-incubating |

When accessing datasets by location (e.g., `SELECT * FROM parquet.${dataset_path}`), the name is derived from the physical path:

| Location Type  | Dataset name                                  | Example                               | Since Version    |
|----------------|-----------------------------------------------|---------------------------------------|------------------|
| GVFS location  | `${catalogName}.${schemaName}.${filesetName}` | `fileset_catalog.schema.fileset_a`    | 0.9.0-incubating |
| Other location | location path                                 | `hdfs://127.0.0.1:9000/tmp/a/student` | 0.9.0-incubating |

For GVFS location, this plugin adds `fileset-location` facets which contains the location path.

```json
"fileset-location" :
{
"location":"${gvfs-virutal-location}",
"_producer":"https://github.com/datastrato/...",
"_schemaURL":"https://raw.githubusercontent...."
}
```

## How to use 

1. Download [Gravitino OpenLineage plugin jar](https://github.com/datastrato/gravitino-openlineage-plugins/tree/main/spark-plugin/) and place it to the classpath of Spark.
2. Add configuration to the Spark to enable lineage collection.

Configuration example For Spark shell:

```shell
./bin/spark-sql -v \
--jars /${path}/openlineage-spark_2.12-${gravitino-specific-version}.jar,/${path}/gravitino-spark-connector-runtime-3.5_2.12-${version}.jar \
--conf spark.plugins="org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin" \
--conf spark.sql.gravitino.uri=http://localhost:8090 \
--conf spark.sql.gravitino.metalake=${metalakeName} \
--conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
--conf spark.openlineage.transport.type=http \
--conf spark.openlineage.transport.url=http://localhost:8090 \
--conf spark.openlineage.transport.endpoint=/api/lineage \
--conf spark.openlineage.namespace=${metalakeName} \
--conf spark.openlineage.appName=${appName} \
--conf spark.openlineage.columnLineage.datasetLineageEnabled=true 
```

Please refer to [OpenLineage Spark guides](https://openlineage.io/docs/guides/spark/) and [Gravitino Spark connector](/spark-connector/spark-connector.md) for more details. Additionally, Gravitino provides following configurations for lineage. 

<table>
  <thead>
    <tr>
      <th>Configuration item</th>
      <th>Description</th>
      <th>Default value</th>
      <th>Required</th>
      <th>Since Version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>spark.sql.gravitino.useGravitinoIdentifier</code></td>
      <td>Whether to use Gravitino identifier for the dataset not managed by Gravitino. If setting to false, will use origin OpenLineage dataset identifier, like <code>hdfs://localhost:9000</code> as namespace and <code>/path/xx</code> as name for hive table.</td>
      <td>True</td>
      <td>No</td>
      <td>0.9.0-incubating</td>
    </tr>
    <tr>
      <td><code>spark.sql.gravitino.catalogMappings</code></td>
      <td>Catalog name mapping roles for the dataset not managed by Gravitino. For example <code>spark_catalog:catalog1,iceberg_catalog:catalog2</code> maps <code>spark_catalog</code> to <code>catalog1</code> and <code>iceberg_catalog</code> to <code>catalog2</code>, the other catalogs will not be mapped.</td>
      <td>None</td>
      <td>No</td>
      <td>0.9.0-incubating</td>
    </tr>
  </tbody>
</table>
