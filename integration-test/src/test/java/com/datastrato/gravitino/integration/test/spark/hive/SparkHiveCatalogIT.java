/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.hive;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.datastrato.gravitino.spark.connector.hive.HivePropertiesConstants;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.TableCatalog;
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

  @Override
  protected boolean supportsPartition() {
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

  @Test
  public void testInsertHiveFormatPartitionTableAsSelect() {
    String tableName = "insert_hive_partition_table";
    String newTableName = "new_" + tableName;

    // create source table
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    checkTableReadWrite(tableInfo);

    // insert into partition ((name = %s, age = %s) select xx
    dropTableIfExists(newTableName);
    String createTableSql =
        String.format(
            "CREATE TABLE %s (id INT) PARTITIONED BY (name STRING, age INT)", newTableName);
    sql(createTableSql);
    String insertPartitionSql =
        String.format(
            "INSERT OVERWRITE TABLE %s PARTITION (name = %s, age = %s) SELECT id FROM %s",
            newTableName,
            typeConstant.get(DataTypes.StringType),
            typeConstant.get(DataTypes.IntegerType),
            tableName);
    sql(insertPartitionSql);

    SparkTableInfo newTableInfo = getTableInfo(newTableName);
    checkPartitionDirExists(newTableInfo);
    String expectedData = getExpectedTableData(newTableInfo);
    List<String> tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(expectedData, tableData.get(0));

    // insert into partition ((name = %s, age) select xx
    dropTableIfExists(newTableName);
    sql(createTableSql);
    insertPartitionSql =
        String.format(
            "INSERT OVERWRITE TABLE %s PARTITION (name = %s, age) SELECT id, age FROM %s",
            newTableName, typeConstant.get(DataTypes.StringType), tableName);
    sql(insertPartitionSql);

    newTableInfo = getTableInfo(newTableName);
    checkPartitionDirExists(newTableInfo);
    tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(expectedData, tableData.get(0));

    // insert into partition ((name,  age) select xx
    dropTableIfExists(newTableName);
    sql(createTableSql);
    insertPartitionSql =
        String.format(
            "INSERT OVERWRITE TABLE %s PARTITION (name , age) SELECT * FROM %s",
            newTableName, tableName);
    sql(insertPartitionSql);

    newTableInfo = getTableInfo(newTableName);
    checkPartitionDirExists(newTableInfo);
    tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(expectedData, tableData.get(0));
  }

  @Test
  void testHiveDefaultFormat() {
    String tableName = "hive_default_format_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(
                    HivePropertiesConstants.SPARK_HIVE_INPUT_FORMAT,
                    HivePropertiesConstants.TEXT_INPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_OUTPUT_FORMAT,
                    HivePropertiesConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_SERDE_LIB,
                    HivePropertiesConstants.LAZY_SIMPLE_SERDE_CLASS));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  void testHiveFormatWithStoredAs() {
    String tableName = "test_hive_format_stored_as_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    createTableSql += "STORED AS PARQUET";
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(
                    HivePropertiesConstants.SPARK_HIVE_INPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_INPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_OUTPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_OUTPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_SERDE_LIB,
                    HivePropertiesConstants.PARQUET_SERDE_CLASS));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
    checkParquetFile(tableInfo);
  }

  @Test
  void testHiveFormatWithExternalTable() {
    String tableName = "test_hive_format_with_external_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName, true);
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(HivePropertiesConstants.SPARK_HIVE_EXTERNAL, "true"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);

    dropTableIfExists(tableName);
    Path tableLocation = new Path(tableInfo.getTableLocation());
    checkDataFileExists(tableLocation);
  }

  @Test
  void testHiveFormatWithLocationTable() {
    String tableName = "test_hive_format_with_location_table";
    String location = "/user/hive/external_db";
    Boolean[] isExternals = {Boolean.TRUE, Boolean.FALSE};

    Arrays.stream(isExternals)
        .forEach(
            isExternal -> {
              dropTableIfExists(tableName);
              deleteDirIfExists(location);
              String createTableSql = getCreateSimpleTableString(tableName, isExternal);
              createTableSql = createTableSql + "LOCATION '" + location + "'";
              sql(createTableSql);

              SparkTableInfo tableInfo = getTableInfo(tableName);
              checkTableReadWrite(tableInfo);
              Assertions.assertTrue(tableInfo.getTableLocation().equals(hdfs.getUri() + location));
            });
  }

  @Test
  void testHiveFormatWithUsing() {
    String tableName = "test_hive_format_using_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    createTableSql += "USING PARQUET";
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(
                    HivePropertiesConstants.SPARK_HIVE_INPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_INPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_OUTPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_OUTPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_SERDE_LIB,
                    HivePropertiesConstants.PARQUET_SERDE_CLASS));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
    checkParquetFile(tableInfo);
  }

  @Test
  void testHivePropertiesWithSerdeRowFormat() {
    String tableName = "test_hive_row_serde_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    createTableSql =
        String.format(
            "%s ROW FORMAT SERDE '%s' WITH SERDEPROPERTIES ('serialization.format'='@', 'field.delim' = ',') STORED AS INPUTFORMAT '%s' OUTPUTFORMAT '%s'",
            createTableSql,
            HivePropertiesConstants.PARQUET_SERDE_CLASS,
            HivePropertiesConstants.PARQUET_INPUT_FORMAT_CLASS,
            HivePropertiesConstants.PARQUET_OUTPUT_FORMAT_CLASS);
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(
                    TableCatalog.OPTION_PREFIX + "serialization.format",
                    "@",
                    TableCatalog.OPTION_PREFIX + "field.delim",
                    ",",
                    HivePropertiesConstants.SPARK_HIVE_INPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_INPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_OUTPUT_FORMAT,
                    HivePropertiesConstants.PARQUET_OUTPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_SERDE_LIB,
                    HivePropertiesConstants.PARQUET_SERDE_CLASS));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
    checkParquetFile(tableInfo);
  }

  /*
  | DELIMITED [ FIELDS TERMINATED BY fields_terminated_char [ ESCAPED BY escaped_char ] ]
      [ COLLECTION ITEMS TERMINATED BY collection_items_terminated_char ]
      [ MAP KEYS TERMINATED BY map_key_terminated_char ]
      [ LINES TERMINATED BY row_terminated_char ]
      [ NULL DEFINED AS null_char ]
   */
  @Test
  void testHivePropertiesWithDelimitedRowFormat() {
    String tableName = "test_hive_row_format_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    createTableSql +=
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY ';' "
            + "COLLECTION ITEMS TERMINATED BY '@' "
            + "MAP KEYS TERMINATED BY ':' "
            + "NULL DEFINED AS 'n' "
            + "STORED AS TEXTFILE";
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(
                ImmutableMap.of(
                    TableCatalog.OPTION_PREFIX + "field.delim",
                    ",",
                    TableCatalog.OPTION_PREFIX + "escape.delim",
                    ";",
                    TableCatalog.OPTION_PREFIX + "mapkey.delim",
                    ":",
                    TableCatalog.OPTION_PREFIX + "serialization.format",
                    ",",
                    TableCatalog.OPTION_PREFIX + "colelction.delim",
                    "@",
                    HivePropertiesConstants.SPARK_HIVE_INPUT_FORMAT,
                    HivePropertiesConstants.TEXT_INPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_OUTPUT_FORMAT,
                    HivePropertiesConstants.IGNORE_KEY_OUTPUT_FORMAT_CLASS,
                    HivePropertiesConstants.SPARK_HIVE_SERDE_LIB,
                    HivePropertiesConstants.LAZY_SIMPLE_SERDE_CLASS));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);

    // check it's a text file and field.delim take effects
    List<Object[]> rows =
        rowsToJava(
            getSparkSession()
                .read()
                .option("delimiter", ",")
                .csv(tableInfo.getTableLocation())
                .collectAsList());
    Assertions.assertTrue(rows.size() == 1);
    Object[] row = rows.get(0);
    Assertions.assertEquals(3, row.length);
    Assertions.assertEquals("2", row[0]);
    Assertions.assertEquals("gravitino_it_test", (String) row[1]);
    Assertions.assertEquals("2", row[2]);
  }
}
