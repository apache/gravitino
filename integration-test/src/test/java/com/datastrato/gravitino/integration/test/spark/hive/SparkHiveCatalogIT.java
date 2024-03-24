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
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.commons.util.StringUtils;

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
  protected String getUsingClause() {
    return "USING PARQUET";
  }

  @Test
  void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName);
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertTrue(StringUtils.isBlank(properties));

    testDatabaseName = "t_create2";
    dropDatabaseIfExists(testDatabaseName);
    String testDatabaseLocation = "/tmp/" + testDatabaseName;
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '%s'\n" + " WITH DBPROPERTIES (ID=001);",
            testDatabaseName, testDatabaseLocation));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    String comment = databaseMeta.get("Comment");
    Assertions.assertEquals("comment", comment);
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    // underlying catalog may change /tmp/t_create2 to file:/tmp/t_create2
    Assertions.assertTrue(databaseMeta.get("Location").contains(testDatabaseLocation));
    properties = databaseMeta.get("Properties");
    Assertions.assertEquals("((ID,001))", properties);
  }

  @Test
  void testAlterSchema() {
    String testDatabaseName = "t_alter";
    sql("CREATE DATABASE " + testDatabaseName);
    Assertions.assertTrue(
        StringUtils.isBlank(getDatabaseMetadata(testDatabaseName).get("Properties")));

    sql(String.format("ALTER DATABASE %s SET DBPROPERTIES ('ID'='001')", testDatabaseName));
    Assertions.assertEquals("((ID,001))", getDatabaseMetadata(testDatabaseName).get("Properties"));

    // Hive metastore doesn't support alter database location, therefore this test method
    // doesn't verify ALTER DATABASE database_name SET LOCATION 'new_location'.

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class,
        () -> sql("ALTER DATABASE notExists SET DBPROPERTIES ('ID'='001')"));
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
