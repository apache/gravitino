/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

// Requires a running Gravitino server and Spark environment; enable with GRAVITINO_ENV_IT=true.
@EnabledIfEnvironmentVariable(named = "GRAVITINO_ENV_IT", matches = "true")
public class TestBuiltinIcebergRewriteDataFiles {

  private static final String SERVER_URI = "http://localhost:8090";
  private static final String METALAKE_NAME = "test";
  private static final String ICEBERG_REST_URI = "http://localhost:9001/iceberg";
  private static final String JOB_TEMPLATE_NAME = "builtin-iceberg-rewrite-data-files";
  private static final String SPARK_CATALOG_NAME = "rest_catalog";
  // Optional local overrides for GRAVITINO_ENV_IT (do not hardcode secrets in source).
  private static final String WAREHOUSE_LOCATION = envOrDefault("GRAVITINO_IT_WAREHOUSE", "");
  private static final String BASIC_AUTH_USER = envOrDefault("GRAVITINO_IT_USERNAME", "");
  private static final String BASIC_AUTH_PASSWORD = envOrDefault("GRAVITINO_IT_PASSWORD", "");

  @Test
  void testSubmitRewriteDataFilesJobByConfig() throws Exception {
    String tableName = "rewrite_table1";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;
    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);
          Map<String, String> jobConf = buildManualJobConfig(tableName);
          submitCompactionJob(metalake, jobConf);
        });
  }

  @Test
  void testSubmitBuiltinIcebergRewriteDataFilesJobFromAdapterAndOptimizerConfig() throws Exception {
    String tableName = "rewrite_table2";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;
    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);
          OptimizerConfig optimizerConfig = createOptimizerConfig();
          Map<String, String> jobOptions =
              Map.of("min-input-files", "1", "target-file-size-bytes", "1048576");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig,
                  tableName,
                  jobOptions,
                  new Column[0],
                  new Transform[0],
                  Collections.emptyList());
          submitCompactionJob(metalake, jobConf);
        });
  }

  @Test
  void testCompactNonPartitionTable() throws Exception {
    String tableName = "rewrite_non_partition_table";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createTableAndInsertData(spark, fullTableName);

          long beforeFiles = countDataFiles(spark, fullTableName);
          Assertions.assertTrue(beforeFiles > 1, "Expected multiple data files before compaction");

          OptimizerConfig optimizerConfig = createOptimizerConfig();
          Map<String, String> jobOptions = Map.of("min-input-files", "1");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig,
                  tableName,
                  jobOptions,
                  new Column[0],
                  new Transform[0],
                  Collections.emptyList());
          submitCompactionJob(metalake, jobConf);

          refreshTable(spark, fullTableName);
          long afterFiles = countDataFiles(spark, fullTableName);
          Assertions.assertTrue(
              afterFiles < beforeFiles,
              String.format(
                  "Expected fewer data files after compaction after:%d, before:%d",
                  afterFiles, beforeFiles));
        });
  }

  @Test
  void testCompactPartitionTable() throws Exception {
    String tableName = "rewrite_partition_table";
    String fullTableName = SPARK_CATALOG_NAME + ".db." + tableName;

    runWithSparkAndMetalake(
        (spark, metalake) -> {
          createPartitionTableAndInsertData(spark, fullTableName);

          Map<String, Long> beforeCounts = countDataFilesByPartition(spark, fullTableName);
          Map<String, Long> beforeMaxSizes = maxFileSizeByPartition(spark, fullTableName);
          String targetPartition1 = partitionKey(2024, "1");
          String targetPartition2 = partitionKey(2024, "2");
          String otherPartition1 = partitionKey(2025, "1");
          String otherPartition2 = partitionKey(2025, "2");

          Assertions.assertTrue(
              beforeCounts.getOrDefault(targetPartition1, 0L) > 1,
              "Expected multiple data files in " + targetPartition1 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(targetPartition2, 0L) > 1,
              "Expected multiple data files in " + targetPartition2 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(otherPartition1, 0L) > 1,
              "Expected multiple data files in " + otherPartition1 + " before compaction");
          Assertions.assertTrue(
              beforeCounts.getOrDefault(otherPartition2, 0L) > 1,
              "Expected multiple data files in " + otherPartition2 + " before compaction");

          OptimizerConfig optimizerConfig = createOptimizerConfig();

          List<PartitionPath> partitions =
              Arrays.asList(
                  PartitionPath.of(
                      Arrays.asList(
                          new PartitionEntryImpl("year", "2024"),
                          new PartitionEntryImpl("month", "1"))),
                  PartitionPath.of(
                      Arrays.asList(
                          new PartitionEntryImpl("year", "2024"),
                          new PartitionEntryImpl("month", "2"))));
          Column[] columns =
              new Column[] {
                column("year", Types.IntegerType.get()), column("month", Types.StringType.get())
              };
          Transform[] partitioning =
              new Transform[] {Transforms.identity("year"), Transforms.identity("month")};

          Map<String, String> jobOptions = Map.of("min-input-files", "1");
          Map<String, String> jobConf =
              buildCompactionJobConfig(
                  optimizerConfig, tableName, jobOptions, columns, partitioning, partitions);
          submitCompactionJob(metalake, jobConf);

          refreshTable(spark, fullTableName);
          Map<String, Long> afterCounts = countDataFilesByPartition(spark, fullTableName);
          Map<String, Long> afterMaxSizes = maxFileSizeByPartition(spark, fullTableName);
          Assertions.assertTrue(
              afterMaxSizes.getOrDefault(targetPartition1, 0L)
                  > beforeMaxSizes.getOrDefault(targetPartition1, 0L),
              "Expected larger data files after compaction for " + targetPartition1);
          Assertions.assertTrue(
              afterMaxSizes.getOrDefault(targetPartition2, 0L)
                  > beforeMaxSizes.getOrDefault(targetPartition2, 0L),
              "Expected larger data files after compaction for " + targetPartition2);
          Assertions.assertEquals(
              beforeCounts.getOrDefault(otherPartition1, 0L),
              afterCounts.getOrDefault(otherPartition1, 0L),
              "Expected no compaction for " + otherPartition1);
          Assertions.assertEquals(
              beforeCounts.getOrDefault(otherPartition2, 0L),
              afterCounts.getOrDefault(otherPartition2, 0L),
              "Expected no compaction for " + otherPartition2);
          Assertions.assertEquals(
              beforeMaxSizes.getOrDefault(otherPartition1, 0L),
              afterMaxSizes.getOrDefault(otherPartition1, 0L),
              "Expected no size changes for " + otherPartition1);
          Assertions.assertEquals(
              beforeMaxSizes.getOrDefault(otherPartition2, 0L),
              afterMaxSizes.getOrDefault(otherPartition2, 0L),
              "Expected no size changes for " + otherPartition2);
        });
  }

  @Test
  void testPartitionUtilsWhereClausesAgainstIcebergRewriteCall() {
    // Only needs Iceberg REST + Spark CALL; no Gravitino metalake / job submit.
    SparkSession spark = createSparkSession();
    try {
      spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
      // Iceberg Spark system functions are resolved inside rewrite_data_files where parsing
      // (SparkV2Filters), not always as free-standing Spark SQL routines in this classpath.

      for (boolean ansi : new boolean[] {false, true}) {
        spark.conf().set("spark.sql.ansi.doubleQuotedIdentifiers", Boolean.toString(ansi));
        assertRewriteCallForIdentityString(spark, ansi);
        assertRewriteCallForIdentityStringWithSingleQuote(spark, ansi);
        assertRewriteCallForIdentityStringWithQuoteAndBackslash(spark, ansi);
        assertRewriteCallForIdentityDate(spark, ansi);
        assertRewriteCallForIdentityTimestamp(spark, ansi);
        assertRewriteCallForDays(spark, ansi);
        assertRewriteCallForYears(spark, ansi);
        assertRewriteCallForMonths(spark, ansi);
        assertRewriteCallForHours(spark, ansi);
        assertRewriteCallForBucket(spark, ansi);
        assertRewriteCallForTruncate(spark, ansi);
        assertRewriteCallForStructDay(spark, ansi);
        assertRewriteCallForStructYear(spark, ansi);
        assertRewriteCallForStructMonth(spark, ansi);
        assertRewriteCallForStructHour(spark, ansi);
        assertRewriteCallForStructBucket(spark, ansi);
        assertRewriteCallForStructTruncate(spark, ansi);
        assertRewriteCallForStructIdentityTimestamp(spark, ansi);
        assertRewriteCallForStructIdentityInt(spark, ansi);
      }
    } finally {
      spark.stop();
    }
  }

  private static void assertRewriteCallForIdentityString(SparkSession spark, boolean ansi) {
    String table = fullTable("id_str_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, region STRING)",
        "region",
        "INSERT INTO " + table + " SELECT id, 'usa' FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("region", "usa")))),
            new Column[] {column("region", Types.StringType.get())},
            new Transform[] {Transforms.identity("region")});
    Assertions.assertEquals("(region = \"usa\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForIdentityStringWithSingleQuote(
      SparkSession spark, boolean ansi) {
    String table = fullTable("id_str_sq_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, region STRING)",
        "region",
        "INSERT INTO " + table + " SELECT id, 'O''Brien' FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("region", "O'Brien")))),
            new Column[] {column("region", Types.StringType.get())},
            new Transform[] {Transforms.identity("region")});
    Assertions.assertEquals("(region = \"O'Brien\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForIdentityStringWithQuoteAndBackslash(
      SparkSession spark, boolean ansi) {
    String table = fullTable("id_str_qb_" + ansiFlag(ansi));
    String partitionValue = "a\"b\\c";
    recreateTable(
        spark,
        table,
        "(id INT, region STRING)",
        "region",
        // Spark SQL string: \" → ", \\ → \
        "INSERT INTO " + table + " SELECT id, 'a\\\"b\\\\c' FROM range(0, 20)");
    String stored = spark.sql("SELECT region FROM " + table + " LIMIT 1").first().getString(0);
    Assertions.assertEquals(
        partitionValue, stored, "INSERT must store the intended partition value");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("region", partitionValue)))),
            new Column[] {column("region", Types.StringType.get())},
            new Transform[] {Transforms.identity("region")});
    Assertions.assertEquals("(region = \"a\\\\\"b\\\\\\\\c\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForIdentityDate(SparkSession spark, boolean ansi) {
    String table = fullTable("id_date_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, dt DATE)",
        "dt",
        "INSERT INTO " + table + " SELECT id, DATE '2024-01-01' FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("dt", "2024-01-01")))),
            new Column[] {column("dt", Types.DateType.get())},
            new Transform[] {Transforms.identity("dt")});
    Assertions.assertEquals("(dt = DATE \"2024-01-01\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForIdentityTimestamp(SparkSession spark, boolean ansi) {
    String table = fullTable("id_ts_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, ts TIMESTAMP)",
        "ts",
        "INSERT INTO " + table + " SELECT id, TIMESTAMP '2024-01-01 00:00:00' FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("ts", "2024-01-01 00:00:00")))),
            new Column[] {column("ts", Types.TimestampType.withoutTimeZone())},
            new Transform[] {Transforms.identity("ts")});
    Assertions.assertEquals("(ts = TIMESTAMP \"2024-01-01 00:00:00\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForDays(SparkSession spark, boolean ansi) {
    String table = fullTable("days_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, ts TIMESTAMP)",
        "days(ts)",
        "INSERT INTO " + table + " SELECT id, TIMESTAMP '2026-03-18 12:00:00' FROM range(0, 20)");
    Column[] columns = new Column[] {column("ts", Types.TimestampType.withoutTimeZone())};
    Transform[] partitioning = new Transform[] {Transforms.day("ts")};

    String isoWhere =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("ts_day", "2026-03-18")))),
            columns,
            partitioning);
    Assertions.assertEquals("(days(ts) = DATE \"2026-03-18\")", isoWhere);
    callRewrite(spark, table, isoWhere);

    String epochWhere =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("ts_day", "20530")))),
            columns,
            partitioning);
    Assertions.assertEquals("(days(ts) = DATE \"2026-03-18\")", epochWhere);
    callRewrite(spark, table, epochWhere);
  }

  private static void assertRewriteCallForYears(SparkSession spark, boolean ansi) {
    assertTemporalIntRewrite(spark, "years_" + ansiFlag(ansi), "years(ts)", Transforms.year("ts"));
  }

  private static void assertRewriteCallForMonths(SparkSession spark, boolean ansi) {
    assertTemporalIntRewrite(
        spark, "months_" + ansiFlag(ansi), "months(ts)", Transforms.month("ts"));
  }

  private static void assertRewriteCallForHours(SparkSession spark, boolean ansi) {
    assertTemporalIntRewrite(spark, "hours_" + ansiFlag(ansi), "hours(ts)", Transforms.hour("ts"));
  }

  private static void assertTemporalIntRewrite(
      SparkSession spark, String tableSuffix, String partitionBy, Transform transform) {
    String table = fullTable(tableSuffix);
    recreateTable(
        spark,
        table,
        "(id INT, ts TIMESTAMP)",
        partitionBy,
        "INSERT INTO " + table + " SELECT id, TIMESTAMP '2026-03-18 12:00:00' FROM range(0, 20)");
    Row partition = spark.sql("SELECT partition FROM " + table + ".partitions LIMIT 1").first();
    Row partitionStruct = partition.getStruct(0);
    String fieldName = partitionStruct.schema().fields()[0].name();
    String value = String.valueOf(partitionStruct.get(0));
    String functionName = partitionBy.substring(0, partitionBy.indexOf('('));
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl(fieldName, value)))),
            new Column[] {column("ts", Types.TimestampType.withoutTimeZone())},
            new Transform[] {transform});
    Assertions.assertEquals("(" + functionName + "(ts) = " + value + ")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForBucket(SparkSession spark, boolean ansi) {
    String table = fullTable("bucket_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, user_id BIGINT)",
        "bucket(16, user_id)",
        "INSERT INTO " + table + " SELECT id, 42 FROM range(0, 20)");
    Row partitionStruct =
        spark.sql("SELECT partition FROM " + table + ".partitions LIMIT 1").first().getStruct(0);
    String value = String.valueOf(partitionStruct.get(0));
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(
                PartitionPath.of(
                    List.of(
                        new PartitionEntryImpl(
                            partitionStruct.schema().fields()[0].name(), value)))),
            new Column[] {column("user_id", Types.LongType.get())},
            new Transform[] {Transforms.bucket(16, new String[] {"user_id"})});
    Assertions.assertEquals("(bucket(16, user_id) = " + value + ")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForTruncate(SparkSession spark, boolean ansi) {
    String table = fullTable("trunc_" + ansiFlag(ansi));
    recreateTable(
        spark,
        table,
        "(id INT, country STRING)",
        "truncate(3, country)",
        "INSERT INTO " + table + " SELECT id, 'usaxxx' FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("country_trunc", "usa")))),
            new Column[] {column("country", Types.StringType.get())},
            new Transform[] {Transforms.truncate(3, "country")});
    Assertions.assertEquals("(truncate(3, country) = \"usa\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForStructDay(SparkSession spark, boolean ansi) {
    assertStructTemporalRewrite(
        spark,
        "struct_day_" + ansiFlag(ansi),
        "days(event.ts)",
        Transforms.day(new String[] {"event", "ts"}),
        "(days(event.ts) = DATE \"2026-03-18\")",
        "2026-03-18");
  }

  private static void assertRewriteCallForStructYear(SparkSession spark, boolean ansi) {
    assertStructTemporalIntRewrite(
        spark,
        "struct_year_" + ansiFlag(ansi),
        "years(event.ts)",
        Transforms.year(new String[] {"event", "ts"}));
  }

  private static void assertRewriteCallForStructMonth(SparkSession spark, boolean ansi) {
    assertStructTemporalIntRewrite(
        spark,
        "struct_month_" + ansiFlag(ansi),
        "months(event.ts)",
        Transforms.month(new String[] {"event", "ts"}));
  }

  private static void assertRewriteCallForStructHour(SparkSession spark, boolean ansi) {
    assertStructTemporalIntRewrite(
        spark,
        "struct_hour_" + ansiFlag(ansi),
        "hours(event.ts)",
        Transforms.hour(new String[] {"event", "ts"}));
  }

  private static void assertStructTemporalRewrite(
      SparkSession spark,
      String tableSuffix,
      String partitionBy,
      Transform transform,
      String expectedWhere,
      String partitionValue) {
    String table = fullTable(tableSuffix);
    recreateStructTsTable(spark, table, partitionBy);
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.ts", partitionValue)))),
            new Column[] {eventStructColumn()},
            new Transform[] {transform});
    Assertions.assertEquals(expectedWhere, where);
    callRewrite(spark, table, where);
  }

  private static void assertStructTemporalIntRewrite(
      SparkSession spark, String tableSuffix, String partitionBy, Transform transform) {
    String table = fullTable(tableSuffix);
    recreateStructTsTable(spark, table, partitionBy);
    Row partitionStruct =
        spark.sql("SELECT partition FROM " + table + ".partitions LIMIT 1").first().getStruct(0);
    String value = String.valueOf(partitionStruct.get(0));
    String functionName = partitionBy.substring(0, partitionBy.indexOf('('));
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.ts", value)))),
            new Column[] {eventStructColumn()},
            new Transform[] {transform});
    Assertions.assertEquals("(" + functionName + "(event.ts) = " + value + ")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForStructBucket(SparkSession spark, boolean ansi) {
    String table = fullTable("struct_bucket_" + ansiFlag(ansi));
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT, event STRUCT<uid: BIGINT>) USING iceberg "
            + "PARTITIONED BY (bucket(8, event.uid))");
    spark.sql("INSERT INTO " + table + " SELECT id, named_struct('uid', 42L) FROM range(0, 20)");
    Row partitionStruct =
        spark.sql("SELECT partition FROM " + table + ".partitions LIMIT 1").first().getStruct(0);
    String value = String.valueOf(partitionStruct.get(0));
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.uid_bucket", value)))),
            new Column[] {
              column(
                  "event",
                  Types.StructType.of(
                      Types.StructType.Field.notNullField("uid", Types.LongType.get())))
            },
            new Transform[] {Transforms.bucket(8, new String[][] {new String[] {"event", "uid"}})});
    Assertions.assertEquals("(bucket(8, event.uid) = " + value + ")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForStructTruncate(SparkSession spark, boolean ansi) {
    String table = fullTable("struct_trunc_" + ansiFlag(ansi));
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT, event STRUCT<loc: STRING>) USING iceberg "
            + "PARTITIONED BY (truncate(2, event.loc))");
    spark.sql("INSERT INTO " + table + " SELECT id, named_struct('loc', 'abxx') FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.loc_trunc", "ab")))),
            new Column[] {
              column(
                  "event",
                  Types.StructType.of(
                      Types.StructType.Field.notNullField("loc", Types.StringType.get())))
            },
            new Transform[] {Transforms.truncate(2, new String[] {"event", "loc"})});
    Assertions.assertEquals("(truncate(2, event.loc) = \"ab\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForStructIdentityTimestamp(
      SparkSession spark, boolean ansi) {
    String table = fullTable("struct_id_ts_" + ansiFlag(ansi));
    recreateStructTsTable(spark, table, "event.ts");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(
                PartitionPath.of(
                    List.of(new PartitionEntryImpl("event.ts", "2026-03-18 12:00:00")))),
            new Column[] {eventStructColumn()},
            new Transform[] {Transforms.identity(new String[] {"event", "ts"})});
    Assertions.assertEquals("(event.ts = TIMESTAMP \"2026-03-18 12:00:00\")", where);
    callRewrite(spark, table, where);
  }

  private static void assertRewriteCallForStructIdentityInt(SparkSession spark, boolean ansi) {
    String table = fullTable("struct_id_int_" + ansiFlag(ansi));
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT, event STRUCT<code: INT>) USING iceberg "
            + "PARTITIONED BY (event.code)");
    spark.sql("INSERT INTO " + table + " SELECT id, named_struct('code', 7) FROM range(0, 20)");
    String where =
        PartitionUtils.getWhereClauseForPartitions(
            List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.code", "7")))),
            new Column[] {
              column(
                  "event",
                  Types.StructType.of(
                      Types.StructType.Field.notNullField("code", Types.IntegerType.get())))
            },
            new Transform[] {Transforms.identity(new String[] {"event", "code"})});
    Assertions.assertEquals("(event.code = 7)", where);
    callRewrite(spark, table, where);
  }

  private static void recreateStructTsTable(SparkSession spark, String table, String partitionBy) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " (id INT, event STRUCT<ts: TIMESTAMP>) USING iceberg "
            + "PARTITIONED BY ("
            + partitionBy
            + ")");
    spark.sql(
        "INSERT INTO "
            + table
            + " SELECT id, named_struct('ts', TIMESTAMP '2026-03-18 12:00:00') FROM range(0, 20)");
  }

  private static Column eventStructColumn() {
    return column(
        "event",
        Types.StructType.of(
            Types.StructType.Field.notNullField("ts", Types.TimestampType.withoutTimeZone())));
  }

  private static String fullTable(String suffix) {
    return SPARK_CATALOG_NAME + ".db.pu_" + suffix;
  }

  private static String ansiFlag(boolean ansi) {
    return ansi ? "ansi" : "plain";
  }

  private static void recreateTable(
      SparkSession spark, String table, String columns, String partitionBy, String insertSql) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + table);
    spark.sql(
        "CREATE TABLE "
            + table
            + " "
            + columns
            + " USING iceberg PARTITIONED BY ("
            + partitionBy
            + ")");
    spark.sql(insertSql);
  }

  private static void callRewrite(SparkSession spark, String fullTableName, String whereClause) {
    String tableArg = fullTableName.substring(SPARK_CATALOG_NAME.length() + 1);
    // Mirror IcebergRewriteDataFilesJob.buildProcedureCall / IcebergJobUtils.escapeSqlString.
    String sql =
        "CALL "
            + SPARK_CATALOG_NAME
            + ".system.rewrite_data_files(table => '"
            + escapeSqlString(tableArg)
            + "', strategy => 'binpack', where => '"
            + escapeSqlString(whereClause)
            + "')";
    Assertions.assertDoesNotThrow(() -> spark.sql(sql).collect());
  }

  /** Same as {@code IcebergJobUtils.escapeSqlString}: double single quotes only. */
  private static String escapeSqlString(String value) {
    return value == null ? null : value.replace("'", "''");
  }

  private static GravitinoMetalake loadOrCreateMetalake(
      GravitinoAdminClient client, String metalakeName) {
    try {
      return client.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException ignored) {
      return client.createMetalake(metalakeName, "IT metalake", Map.of());
    }
  }

  @FunctionalInterface
  private interface SparkMetalakeConsumer {
    void accept(SparkSession spark, GravitinoMetalake metalake) throws Exception;
  }

  private static void runWithSparkAndMetalake(SparkMetalakeConsumer consumer) throws Exception {
    SparkSession spark = createSparkSession();
    try (GravitinoAdminClient client = createGravitinoClient()) {
      GravitinoMetalake metalake = loadOrCreateMetalake(client, METALAKE_NAME);
      consumer.accept(spark, metalake);
    } finally {
      spark.stop();
    }
  }

  private static GravitinoAdminClient createGravitinoClient() {
    if (StringUtils.isNotBlank(BASIC_AUTH_USER) && StringUtils.isNotBlank(BASIC_AUTH_PASSWORD)) {
      return GravitinoAdminClient.builder(SERVER_URI)
          .withBasicAuth(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD)
          .build();
    }
    return GravitinoAdminClient.builder(SERVER_URI).build();
  }

  private static String envOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    return value == null ? defaultValue : value;
  }

  private static void submitCompactionJob(GravitinoMetalake metalake, Map<String, String> jobConf) {
    submitJob(metalake, JOB_TEMPLATE_NAME, jobConf);
  }

  private static void submitJob(
      GravitinoMetalake metalake, String jobTemplateName, Map<String, String> jobConf) {
    JobHandle jobHandle = metalake.runJob(jobTemplateName, jobConf);
    Assertions.assertTrue(StringUtils.isNotBlank(jobHandle.jobId()), "Job id should not be blank");

    Awaitility.await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofSeconds(2))
        .until(
            () -> {
              JobHandle.Status status = metalake.getJob(jobHandle.jobId()).jobStatus();
              return status == JobHandle.Status.SUCCEEDED
                  || status == JobHandle.Status.FAILED
                  || status == JobHandle.Status.CANCELLED;
            });

    JobHandle.Status finalStatus = metalake.getJob(jobHandle.jobId()).jobStatus();
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, finalStatus, "Job should succeed");
  }

  private static Map<String, String> buildManualJobConfig(String tableName) {
    Map<String, String> jobConf = new HashMap<>();
    jobConf.put("table_identifier", "db." + tableName);
    jobConf.put("where_clause", "");
    jobConf.put("sort_order", "");
    jobConf.put("strategy", "binpack");
    jobConf.put("options", "{\"min-input-files\":\"1\"}");
    jobConf.putAll(createOptimizerConfig().jobSubmitterConfigs());
    return jobConf;
  }

  private static Map<String, String> buildCompactionJobConfig(
      OptimizerConfig optimizerConfig,
      String tableName,
      Map<String, String> jobOptions,
      Column[] columns,
      Transform[] partitioning,
      List<PartitionPath> partitions) {
    CompactionJobContext jobContext =
        new CompactionJobContext(
            NameIdentifier.of(SPARK_CATALOG_NAME, "db", tableName),
            jobOptions,
            JOB_TEMPLATE_NAME,
            "binpack",
            "",
            columns,
            partitioning,
            partitions);
    GravitinoCompactionJobAdapter adapter = new GravitinoCompactionJobAdapter();
    return GravitinoJobSubmitter.buildJobConfig(optimizerConfig, jobContext, adapter);
  }

  private static long countDataFiles(SparkSession spark, String fullTableName) {
    Row row =
        spark
            .sql(
                "SELECT count(*) AS data_file_cnt FROM "
                    + fullTableName
                    + ".files WHERE content = 0")
            .first();
    return row.getLong(0);
  }

  private static void refreshTable(SparkSession spark, String fullTableName) {
    spark.sql("REFRESH TABLE " + fullTableName);
  }

  private static Map<String, Long> countDataFilesByPartition(
      SparkSession spark, String fullTableName) {
    List<Row> rows =
        spark
            .sql(
                "SELECT partition.year AS year, partition.month AS month, COUNT(*) AS data_file_cnt "
                    + "FROM "
                    + fullTableName
                    + ".files WHERE content = 0 "
                    + "GROUP BY partition.year, partition.month")
            .collectAsList();
    Map<String, Long> counts = new HashMap<>();
    for (Row row : rows) {
      int year = row.getInt(0);
      String month = row.getString(1);
      long count = row.getLong(2);
      counts.put(partitionKey(year, month), count);
    }
    return counts;
  }

  private static Map<String, Long> maxFileSizeByPartition(
      SparkSession spark, String fullTableName) {
    List<Row> rows =
        spark
            .sql(
                "SELECT partition.year AS year, partition.month AS month, "
                    + "MAX(file_size_in_bytes) AS max_size "
                    + "FROM "
                    + fullTableName
                    + ".files WHERE content = 0 "
                    + "GROUP BY partition.year, partition.month")
            .collectAsList();
    Map<String, Long> sizes = new HashMap<>();
    for (Row row : rows) {
      int year = row.getInt(0);
      String month = row.getString(1);
      long maxSize = row.getLong(2);
      sizes.put(partitionKey(year, month), maxSize);
    }
    return sizes;
  }

  private static String partitionKey(int year, String month) {
    return "year=" + year + ",month=" + month;
  }

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return ColumnDTO.builder().withName(name).withDataType(type).build();
  }

  private static OptimizerConfig createOptimizerConfig() {
    Map<String, String> optimizerConfigProps = new HashMap<>();
    optimizerConfigProps.put(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_type", "rest");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_uri", ICEBERG_REST_URI);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "warehouse_location", WAREHOUSE_LOCATION);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_name", SPARK_CATALOG_NAME);
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_master", "local[2]");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_instances", "1");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_cores", "1");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_executor_memory", "1g");
    optimizerConfigProps.put(
        OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_driver_memory", "1g");
    optimizerConfigProps.put(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "spark_conf", "{}");
    return new OptimizerConfig(optimizerConfigProps);
  }

  private static SparkSession createSparkSession() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("builtin-iceberg-rewrite-data-files-it")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                "spark.sql.catalog." + SPARK_CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".type", "rest")
            .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".cache-enabled", "false")
            .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".uri", ICEBERG_REST_URI)
            .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".warehouse", WAREHOUSE_LOCATION)
            // Iceberg years/months/days/hours resolve via the Iceberg FunctionCatalog.
            .config("spark.sql.defaultCatalog", SPARK_CATALOG_NAME);
    if (StringUtils.isNotBlank(BASIC_AUTH_USER) && StringUtils.isNotBlank(BASIC_AUTH_PASSWORD)) {
      builder
          .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".rest.auth.type", "basic")
          .config(
              "spark.sql.catalog." + SPARK_CATALOG_NAME + ".rest.auth.basic.username",
              BASIC_AUTH_USER)
          .config(
              "spark.sql.catalog." + SPARK_CATALOG_NAME + ".rest.auth.basic.password",
              BASIC_AUTH_PASSWORD);
    }
    return builder.getOrCreate();
  }

  private static void createPartitionTableAndInsertData(SparkSession spark, String fullTableName) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    spark.sql(
        "CREATE TABLE "
            + fullTableName
            + " (id INT, data STRING, year INT, month STRING) USING iceberg "
            + "PARTITIONED BY (year, month)");
    spark.sql(
        "ALTER TABLE "
            + fullTableName
            + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024000')");
    int id = 0;
    int[] years = new int[] {2024, 2025};
    String[] months = new String[] {"1", "2"};
    int rowsPerPartition = 20;
    for (int year : years) {
      for (String month : months) {
        for (int i = 0; i < rowsPerPartition; i++) {
          spark.sql(
              "INSERT INTO "
                  + fullTableName
                  + " VALUES ("
                  + id
                  + ", 'value_"
                  + id
                  + "', "
                  + year
                  + ", '"
                  + month
                  + "'"
                  + ")");
          id++;
        }
      }
    }
  }

  private static void createTableAndInsertData(SparkSession spark, String fullTableName) {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + ".db");
    spark.sql("DROP TABLE IF EXISTS " + fullTableName);
    spark.sql("CREATE TABLE " + fullTableName + " (id INT, data STRING) USING iceberg");
    spark.sql(
        "ALTER TABLE "
            + fullTableName
            + " SET TBLPROPERTIES ('write.target-file-size-bytes'='1024000')");
    for (int i = 0; i < 10; i++) {
      spark.sql("INSERT INTO " + fullTableName + " VALUES (" + i + ", 'value_" + i + "')");
    }
  }
}
