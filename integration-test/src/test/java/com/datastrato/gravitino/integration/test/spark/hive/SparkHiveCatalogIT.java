/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.hive;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkHiveCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "hive";
  }

  @Override
  protected String getProvider() {
    return "hive";
  }

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return true;
  }

  @Test
  public void testCreateHiveFormatPartitionTable() {
    String tableName = "hive_partition_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + "PARTITIONED BY (age_p1 INT, age_p2 STRING)";
    sql(createTableSQL);

    List<SparkColumnInfo> columns = new ArrayList<>(getSimpleTableColumn());
    columns.add(SparkColumnInfo.of("age_p1", DataTypes.IntegerType));
    columns.add(SparkColumnInfo.of("age_p2", DataTypes.StringType));

    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(columns)
            .withIdentifyPartition(Arrays.asList("age_p1", "age_p2"));
    checker.check(tableInfo);
    // write to static partition
    checkTableReadWrite(tableInfo);
    checkPartitionDirExists(tableInfo);
  }

  @Test
  public void testWriteHiveDynamicPartition() {
    String tableName = "hive_dynamic_partition_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + "PARTITIONED BY (age_p1 INT, age_p2 STRING)";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    // write data to dynamic partition
    String insertData =
        String.format(
            "INSERT OVERWRITE %s PARTITION(age_p1=1, age_p2) values(1,'a',3,'b');", tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertTrue(queryResult.size() == 1);
    Assertions.assertEquals("1,a,3,1,b", queryResult.get(0));
    String location = tableInfo.getTableLocation();
    String partitionExpression = "age_p1=1/age_p2=b";
    Path partitionPath = new Path(location, partitionExpression);
    checkDirExists(partitionPath);
  }
}
