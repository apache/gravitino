---
title: "Apache Gravitino Spark connector integration test"
slug: /spark-connector/spark-connector-integration-test
keyword: spark connector integration test
license: "This software is licensed under the Apache License version 2."
---

## Overview

There are two types of integration tests in spark connector, normal integration test like `SparkXXCatalogIT`, and the golden file integration test.

## Normal integration test

Normal integration test are mainly used to test the correctness of the metadata, it's enabled in the GitHub CI. You could run tests with specific Spark version like:

```
./gradlew :spark-connector:spark3.3:test --tests "org.apache.gravitino.spark.connector.integration.test.hive.SparkHiveCatalogIT33.testCreateHiveFormatPartitionTable"
```

## Golden file integration test

Golden file integration test are mainly to test the correctness of the SQL result with massive data, it's disabled in the GitHub CI, you could run tests with following command:

```
./gradlew :spark-connector:spark-3.3:test --tests  "org.apache.gravitino.spark.connector.integration.test.sql.SparkSQLRegressionTest33" -PenableSparkSQLITs
```

Please change the Spark version number if you want to test other Spark versions.
If you want to change the test behaviour, please modify `spark-connector/spark-common/src/test/resources/spark-test.conf`.

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
