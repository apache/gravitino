/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.commons.util.StringUtils;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkIcebergCatalogIT extends SparkCommonIT {

  private void createIcebergV2SimpleTable(String tableName) {
    String createSql =
        String.format(
            "CREATE TABLE %s (id INT NOT NULL COMMENT 'id comment', name STRING COMMENT '', age INT) TBLPROPERTIES('format.version'='2')",
            tableName);
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
    return false;
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
  void testUpdateOperations() {
    String tableName = "test_update_v2_table";
    dropTableIfExists(tableName);
    createIcebergV2SimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos =
        new ArrayList<>(getSimpleTableColumn());
    simpleTableColumnInfos.remove(0);
    List<SparkTableInfo.SparkColumnInfo> realTableColumnInfos = new ArrayList<>();
    realTableColumnInfos.add(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment", false));
    realTableColumnInfos.addAll(simpleTableColumnInfos);
    checkTableColumns(tableName, realTableColumnInfos, table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testRowLevelUpdateOperations() {
    String tableName = "test_merge_update_v2_table";
    dropTableIfExists(tableName);
    createIcebergV2SimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos =
        new ArrayList<>(getSimpleTableColumn());
    simpleTableColumnInfos.remove(0);
    List<SparkTableInfo.SparkColumnInfo> realTableColumnInfos = new ArrayList<>();
    realTableColumnInfos.add(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment", false));
    realTableColumnInfos.addAll(simpleTableColumnInfos);
    checkTableColumns(tableName, realTableColumnInfos, table);
    checkTableRowLevelUpdate(table);
  }

  @Test
  void testRowLevelDeleteOperations() {
    String tableName = "test_merge_delete_v2_table";
    dropTableIfExists(tableName);
    createIcebergV2SimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos =
        new ArrayList<>(getSimpleTableColumn());
    simpleTableColumnInfos.remove(0);
    List<SparkTableInfo.SparkColumnInfo> realTableColumnInfos = new ArrayList<>();
    realTableColumnInfos.add(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment", false));
    realTableColumnInfos.addAll(simpleTableColumnInfos);
    checkTableColumns(tableName, realTableColumnInfos, table);
    checkTableRowLevelDelete(table);
  }

  @Test
  void testRowLevelInsertOperations() {
    String tableName = "test_merge_insert_v2_table";
    dropTableIfExists(tableName);
    createIcebergV2SimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos =
        new ArrayList<>(getSimpleTableColumn());
    simpleTableColumnInfos.remove(0);
    List<SparkTableInfo.SparkColumnInfo> realTableColumnInfos = new ArrayList<>();
    realTableColumnInfos.add(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment", false));
    realTableColumnInfos.addAll(simpleTableColumnInfos);
    checkTableColumns(tableName, realTableColumnInfos, table);
    checkTableRowLevelInsert(table);
  }

  @Test
  void testInsertForV1Table() {
    String tableName = "test_insert_v1_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadWrite(table);
  }
}
