/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

  @Test
  void testUpdateOperations() {
    String tableName = "test_update_v2_table";
    dropTableIfExists(tableName);
    createIcebergV2SimpleTable(tableName);

    SparkTableInfo table = getTableInfo(tableName);

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos = getSimpleTableColumn();
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

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos = getSimpleTableColumn();
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

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos = getSimpleTableColumn();
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

    List<SparkTableInfo.SparkColumnInfo> simpleTableColumnInfos = getSimpleTableColumn();
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
