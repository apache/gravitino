/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkMetadataColumn;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.commons.util.StringUtils;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogIT extends SparkCommonIT {

  protected List<SparkTableInfo.SparkColumnInfo> getIcebergSimpleTableColumn() {
    return Arrays.asList(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkTableInfo.SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkTableInfo.SparkColumnInfo.of("ts", DataTypes.TimestampType, null));
  }

  protected SparkMetadataColumn[] getIcebergSimpleTableColumnWithPartition() {
    return new SparkMetadataColumn[] {
      new SparkMetadataColumn("_spec_id", DataTypes.IntegerType, false),
      new SparkMetadataColumn(
          "_partition",
          DataTypes.createStructType(
              new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)}),
          true),
      new SparkMetadataColumn("_file", DataTypes.StringType, false),
      new SparkMetadataColumn("_pos", DataTypes.LongType, false),
      new SparkMetadataColumn("_deleted", DataTypes.BooleanType, false)
    };
  }

  private String getCreateIcebergSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', ts TIMESTAMP)",
        tableName);
  }

  private void createIcebergTableWithTabProperties(
      String tableName, boolean isPartitioned, ImmutableMap<String, String> tblProperties) {
    String partitionedClause = isPartitioned ? " PARTITIONED BY (id) " : "";
    String tblPropertiesStr =
        tblProperties.entrySet().stream()
            .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
            .collect(Collectors.joining(","));
    String createSql =
        String.format(
            "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', age INT) %s TBLPROPERTIES(%s)",
            tableName, partitionedClause, tblPropertiesStr);
    sql(createSql);
  }

  @Override
  protected String getCatalogName() {
    return "iceberg";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-iceberg";
  }

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return true;
  }

  @Override
  protected boolean supportsDelete() {
    return true;
  }

  @Test
  void testIcebergFileLevelDeleteOperation() {
    String tableName = "test_delete_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    sql(
        String.format(
            "INSERT INTO %s VALUES (1, '1', 1),(2, '2', 2),(3, '3', 3),(4, '4', 4),(5, '5', 5)",
            tableName));
    List<String> queryResult1 = getTableData(tableName);
    Assertions.assertEquals(5, queryResult1.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", queryResult1));
    sql(getDeleteSql(tableName, "id < 10"));
    List<String> queryResult2 = getTableData(tableName);
    Assertions.assertEquals(0, queryResult2.size());
  }

  @Test
  void testCreateIcebergBucketPartitionTable() {
    String tableName = "iceberg_bucket_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (bucket(16, id));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withBucket(16, Collections.singletonList("id"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "id_bucket=4";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergHourPartitionTable() {
    String tableName = "iceberg_hour_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (hours(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withHour(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_hour=12";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergDayPartitionTable() {
    String tableName = "iceberg_day_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (days(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withDay(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_day=2024-01-01";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergMonthPartitionTable() {
    String tableName = "iceberg_month_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (months(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withMonth(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_month=2024-01";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergYearPartitionTable() {
    String tableName = "iceberg_year_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (years(ts));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withYear(Collections.singletonList("ts"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "ts_year=2024";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testCreateIcebergTruncatePartitionTable() {
    String tableName = "iceberg_truncate_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateIcebergSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (truncate(1, name));";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getIcebergSimpleTableColumn())
            .withTruncate(1, Collections.singletonList("name"));
    checker.check(tableInfo);

    String insertData =
        String.format(
            "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00.000' as timestamp));",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,2024-01-01 12:00:00.000", queryResult.get(0));
    String location = tableInfo.getTableLocation() + File.separator + "data";
    String partitionExpression = "name_trunc=a";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }

  @Test
  void testMetadataColumns() {
    String tableName = "test_metadata_columns";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);
  }

  @Test
  void testSpecAndPartitionMetadataColumns() {
    String tableName = "test_spec_partition";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);
    checkTableReadWrite(tableInfo);

    String expectedMetadata = "0,{a}";
    String getMetadataSQL =
        String.format("SELECT _spec_id, _partition FROM %s ORDER BY _spec_id", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals(expectedMetadata, queryResult.get(0));
  }

  @Test
  public void testPositionMetadataColumnWithMultipleRowGroups() throws NoSuchTableException {
    String tableName = "test_position_metadata_column_with_multiple_row_groups";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    List<Integer> ids = new ArrayList<>();
    for (int id = 0; id < 200; id++) {
      ids.add(id);
    }
    Dataset<Row> df =
        getSparkSession()
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("name", new Column(Literal.create("a", DataTypes.StringType)))
            .withColumn("age", new Column(Literal.create(1, DataTypes.IntegerType)));
    df.coalesce(1).writeTo(tableName).append();

    Assertions.assertEquals(200, getSparkSession().table(tableName).count());

    String getMetadataSQL = String.format("SELECT _pos FROM %s", tableName);
    List<String> expectedRows = ids.stream().map(String::valueOf).collect(Collectors.toList());
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(expectedRows.size(), queryResult.size());
    Assertions.assertArrayEquals(expectedRows.toArray(), queryResult.toArray());
  }

  @Test
  public void testPositionMetadataColumnWithMultipleBatches() throws NoSuchTableException {
    String tableName = "test_position_metadata_column_with_multiple_batches";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    List<Integer> ids = new ArrayList<>();
    for (int id = 0; id < 7500; id++) {
      ids.add(id);
    }
    Dataset<Row> df =
        getSparkSession()
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("name", new Column(Literal.create("a", DataTypes.StringType)))
            .withColumn("age", new Column(Literal.create(1, DataTypes.IntegerType)));
    df.coalesce(1).writeTo(tableName).append();

    Assertions.assertEquals(7500, getSparkSession().table(tableName).count());

    String getMetadataSQL = String.format("SELECT _pos FROM %s", tableName);
    List<String> expectedRows = ids.stream().map(String::valueOf).collect(Collectors.toList());
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(expectedRows.size(), queryResult.size());
    Assertions.assertArrayEquals(expectedRows.toArray(), queryResult.toArray());
  }

  @Test
  public void testPartitionMetadataColumnWithUnPartitionedTable() {
    String tableName = "test_position_metadata_column_with_multiple_batches";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);
    checkTableReadWrite(tableInfo);

    String getMetadataSQL = String.format("SELECT _partition FROM %s", tableName);
    Assertions.assertEquals(1, getSparkSession().sql(getMetadataSQL).count());
    // _partition value is null for unPartitioned table
    Assertions.assertThrows(NullPointerException.class, () -> getTableMetadata(getMetadataSQL));
  }

  @Test
  public void testFileMetadataColumn() {
    String tableName = "test_file_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);
    checkTableReadWrite(tableInfo);

    String getMetadataSQL = String.format("SELECT _file FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertTrue(queryResult.get(0).contains(tableName));
  }

  @Test
  void testDeleteMetadataColumn() {
    String tableName = "test_file_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumn[] metadataColumns = getIcebergSimpleTableColumnWithPartition();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);
    checkTableReadWrite(tableInfo);

    String getMetadataSQL = String.format("SELECT _deleted FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("false", queryResult.get(0));

    sql(getDeleteSql(tableName, "1 = 1"));

    List<String> queryResult1 = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(0, queryResult1.size());
  }

  @Test
  void testInjectSparkExtensions() {
    SparkSession sparkSession = getSparkSession();
    SparkConf conf = sparkSession.sparkContext().getConf();
    Assertions.assertTrue(conf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
    String extensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
    Assertions.assertTrue(StringUtils.isNotBlank(extensions));
    Assertions.assertEquals(
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", extensions);
  }

  @Test
  void testCopyOnWriteDeleteInUnPartitionedTable() {
    String tableName = "test_copy_on_write_delete";
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testCopyOnWriteDeleteInPartitionedTable() {
    String tableName = "test_copy_on_write_delete";
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testMergeOnReadDeleteInUnPartitionedTable() {
    String tableName = "test_merge_on_read_delete";
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testMergeOnReadDeleteInPartitionedTable() {
    String tableName = "test_merge_on_read_delete";
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testCopyOnWriteUpdateInUnPartitionedTable() {
    String tableName = "test_copy_on_write_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testCopyOnWriteUpdateInPartitionedTable() {
    String tableName = "test_copy_on_write_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testMergeOnReadUpdateInUnPartitionedTable() {
    String tableName = "test_merge_on_read_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testMergeOnReadUpdateInPartitionedTable() {
    String tableName = "test_merge_on_read_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.UPDATE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testCopyOnWriteMergeUpdateInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testCopyOnWriteMergeUpdateInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testMergeOnReadMergeUpdateInUnPartitionedTable() {
    String tableName = "test_merge_on_read_merge_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testMergeOnReadMergeUpdateInPartitionedTable() {
    String tableName = "test_merge_on_read_merge_update";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testCopyOnWriteInMergeDeleteInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_delete";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testCopyOnWriteInMergeDeleteInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_delete";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testMergeOnReadInMergeDeleteInUnPartitionedTable() {
    String tableName = "test_merge_on_read_merge_delete";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testMergeOnReadInMergeDeleteInPartitionedTable() {
    String tableName = "test_merge_on_read_merge_delete";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testCopyOnWriteInsertInMergeInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testCopyOnWriteInsertInMergeInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testMergeOnReadInsertInMergeInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testMergeOnReadInsertInMergeInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.MERGE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName()));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }
}
