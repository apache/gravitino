---
title: "Spark Connector Integration Tests"
slug: "/spark-connector/spark-integration-test"
keyword: "spark connector integration test"
license: "This software is licensed under the Apache License version 2."
---

## Overview

The Spark connector has two kinds of integration tests: normal integration tests (such as `SparkXXCatalogIT`) and golden-file integration tests.

## Normal Integration Tests

Normal integration tests check the correctness of the metadata. They run in GitHub CI. To run them against a specific Spark version:

```
./gradlew :spark-connector:spark-3.3:test --tests "org.apache.gravitino.spark.connector.integration.test.hive.SparkHiveCatalogIT33.testCreateHiveFormatPartitionTable"
```

## Golden-file Integration Tests

Golden-file integration tests check the correctness of SQL results against larger datasets. They are disabled in GitHub CI. Run them with:

```
./gradlew :spark-connector:spark-3.3:test --tests  "org.apache.gravitino.spark.connector.integration.test.sql.SparkSQLRegressionTest33" -PenableSparkSQLITs
```

Change the Spark version number to test other versions. To change test behavior, modify `spark-connector/spark-common/src/test/resources/spark-test.conf`.

| Configuration item                         | Description                                                                                                                                                                            | Default value                                        | Required | Since Version    |
|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|----------|------------------|
| `gravitino.spark.test.dir`                 | The Spark SQL test base dir, include `test-sqls` and `data`.                                                                                                                           | `spark-connector/spark-common/src/test/resources/`   | No       | 0.6.0-incubating |
| `gravitino.spark.test.sqls`                | Specify the test SQLs, using directory to specify group of SQLs like `test-sqls/hive`, using file path to specify one SQL like `test-sqls/hive/basic.sql`, use `,` to split multi part | run all SQLs                                         | No       | 0.6.0-incubating |
| `gravitino.spark.test.generateGoldenFiles` | Whether generate golden files which are used to check the correctness of the SQL result                                                                                                | false                                                | No       | 0.6.0-incubating |
| `gravitino.spark.test.metalake`            | The metalake name to run the test                                                                                                                                                      | `test`                                               | No       | 0.6.0-incubating |
| `gravitino.spark.test.setupEnv`            | Whether to setup Gravitino and Hive environment                                                                                                                                        | `false`                                              | No       | 0.6.0-incubating |
| `gravitino.spark.test.uri`                 | Gravitino uri address, only available when `gravitino.spark.test.setupEnv` is false                                                                                                    | http://127.0.0.1:8090                                | No       | 0.6.0-incubating |
| `gravitino.spark.test.iceberg.warehouse`   | The warehouse location, only available when `gravitino.spark.test.setupEnv` is false                                                                                                   | hdfs://127.0.0.1:9000/user/hive/warehouse-spark-test | No       | 0.6.0-incubating |

The test SQL files are located in `spark-connector/spark-common/src/test/resources/` by default. There are three directories:
- `hive`, SQL tests for Hive catalog.
- `lakehouse-iceberg`, SQL tests for Iceberg catalog.
- `tpcds`, SQL tests for `tpcds` in Hive catalog.

You could create a simple SQL file, like `hive/catalog.sql`, the program will check the output with `hive/catalog.sql.out`. For complex cases like `tpcds`, you could do some prepare work like create table&load data in `prepare.sql`.
