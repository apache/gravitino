---
title: "Apache Gravitino Spark connector integration test"
slug: /spark-connector/spark-connector-integration-test
keyword: spark connector integration test
license: "This software is licensed under the Apache License version 2."
---

## Overview

There are two types of integration tests for the Spark connector:

- Normal integration test like `SparkXXCatalogIT`
- Golden file integration test

## Normal integration test

Normal integration test are mainly used to test the correctness of the metadata.
It's enabled in the GitHub CI.
You can run tests with specific Spark version. For example:

```shell
./gradlew :spark-connector:spark3.3:test \
  --tests "org.apache.gravitino.spark.connector.integration.test.hive.SparkHiveCatalogIT33.testCreateHiveFormatPartitionTable"
```

## Golden file integration test

A golden file integration test is mainly about testing the correctness of the SQL result with massive data.
It's by default disabled in the GitHub CI.
You can manually trigger this test with following command:

```shell
./gradlew :spark-connector:spark-3.3:test \
    --tests "org.apache.gravitino.spark.connector.integration.test.sql.SparkSQLRegressionTest33" -PenableSparkSQLITs
```

Please change the Spark version number if you want to test against other Spark versions.
If you want to change the test behaviour, please modify
`spark-connector/spark-common/src/test/resources/spark-test.conf`.

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.spark.test.dir</tt></td>
  <td>The Spark SQL test base dir, include `test-sqls` and `data`.</td>
  <td>`spark-connector/spark-common/src/test/resources/`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.sqls</tt></td>
  <td>
    The test SQL(s).
    You can use directory to specify group of SQLs like `test-sqls/hive`.
    You can use file path to specify one SQL like `test-sqls/hive/basic.sql`.
    You can use `,` to join multiple items.

    The default behavior is to run all SQL tests.
  </td>
  <td>(none)</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.generateGoldenFiles</tt></td>
  <td>
    Whether generate golden files that are used for validating the SQL results.
  </td>
  <td>false</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.metalake</tt></td>
  <td>The metalake name to run the test against</td>
  <td>`test`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.setupEnv</tt></td>
  <td>Whether setting up the Gravitino and Hive environment is needed</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.uri</tt></td>
  <td>
    The URI for the Gravitino server.
    Applicable when `gravitino.spark.test.setupEnv` is `false`.
  </td>
  <td>http://127.0.0.1:8090</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
<tr>
  <td><tt>gravitino.spark.test.iceberg.warehouse</tt></td>
  <td>
    The warehouse location.
    Only applicable when `gravitino.spark.test.setupEnv` is `false`.
  </td>
  <td>hdfs://127.0.0.1:9000/user/hive/warehouse-spark-test</td>
  <td>No</td>
  <td>`0.6.0-incubating`</td>
</tr>
</tbody>
</table>

The test SQL files are located in `spark-connector/spark-common/src/test/resources/` by default.
There are three directories:

- `hive`, SQL tests for Hive catalog.
- `lakehouse-iceberg`, SQL tests for Iceberg catalog.
- `tpcds`, SQL tests for `tpcds` in Hive catalog.

You can create a simple SQL file, like `hive/catalog.sql`.
The program will check the output with `hive/catalog.sql.out`.
For complex cases like `tpcds`, you can do some preparation work
like creating tables and loading them with data in `prepare.sql`.


