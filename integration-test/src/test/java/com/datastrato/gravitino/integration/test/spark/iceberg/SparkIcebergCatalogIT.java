/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.iceberg;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import com.datastrato.gravitino.integration.test.util.spark.SparkMetadataColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

public abstract class SparkIcebergCatalogIT extends SparkCommonIT {

  private static final String ICEBERG_FORMAT_VERSION = "format-version";
  private static final String ICEBERG_DELETE_MODE = "write.delete.mode";
  private static final String ICEBERG_UPDATE_MODE = "write.update.mode";
  private static final String ICEBERG_MERGE_MODE = "write.merge.mode";
  private static final String ICEBERG_COPY_ON_WRITE = "copy-on-write";
  private static final String ICEBERG_MERGE_ON_READ = "merge-on-read";

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

  @Override
  protected String getTableLocation(SparkTableInfo table) {
    return String.join(File.separator, table.getTableLocation(), "data");
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
  void testIcebergListAndLoadFunctions() throws NoSuchNamespaceException, NoSuchFunctionException {
    String[] empty_namespace = new String[] {};
    String[] system_namespace = new String[] {"system"};
    String[] default_namespace = new String[] {getDefaultDatabase()};
    String[] non_exists_namespace = new String[] {"non_existent"};
    List<String> functions =
        Arrays.asList("iceberg_version", "years", "months", "days", "hours", "bucket", "truncate");

    CatalogPlugin catalogPlugin =
        getSparkSession().sessionState().catalogManager().catalog(getCatalogName());
    Assertions.assertInstanceOf(FunctionCatalog.class, catalogPlugin);
    FunctionCatalog functionCatalog = (FunctionCatalog) catalogPlugin;

    for (String[] namespace : ImmutableList.of(empty_namespace, system_namespace)) {
      Arrays.stream(functionCatalog.listFunctions(namespace))
          .map(Identifier::name)
          .forEach(function -> Assertions.assertTrue(functions.contains(function)));
    }
    Arrays.stream(functionCatalog.listFunctions(default_namespace))
        .map(Identifier::name)
        .forEach(function -> Assertions.assertFalse(functions.contains(function)));
    Assertions.assertThrows(
        NoSuchNamespaceException.class, () -> functionCatalog.listFunctions(non_exists_namespace));

    for (String[] namespace : ImmutableList.of(empty_namespace, system_namespace)) {
      for (String function : functions) {
        Identifier identifier = Identifier.of(namespace, function);
        UnboundFunction func = functionCatalog.loadFunction(identifier);
        Assertions.assertEquals(function, func.name());
      }
    }
    functions.forEach(
        function -> {
          Identifier identifier = Identifier.of(new String[] {getDefaultDatabase()}, function);
          Assertions.assertThrows(
              NoSuchFunctionException.class, () -> functionCatalog.loadFunction(identifier));
        });
  }

  @Test
  void testIcebergFunction() {
    String[] catalogAndNamespaces = new String[] {getCatalogName() + ".system", getCatalogName()};
    Arrays.stream(catalogAndNamespaces)
        .forEach(
            catalogAndNamespace -> {
              List<String> bucket =
                  getQueryData(String.format("SELECT %s.bucket(2, 100)", catalogAndNamespace));
              Assertions.assertEquals(1, bucket.size());
              Assertions.assertEquals("0", bucket.get(0));
            });

    Arrays.stream(catalogAndNamespaces)
        .forEach(
            catalogAndNamespace -> {
              List<String> bucket =
                  getQueryData(
                      String.format("SELECT %s.truncate(2, 'abcdef')", catalogAndNamespace));
              Assertions.assertEquals(1, bucket.size());
              Assertions.assertEquals("ab", bucket.get(0));
            });
  }

  @Test
  void testIcebergPartitions() {
    Map<String, String> partitionPaths = new HashMap<>();
    partitionPaths.put("years", "name=a/name_trunc=a/id_bucket=4/ts_year=2024");
    partitionPaths.put("months", "name=a/name_trunc=a/id_bucket=4/ts_month=2024-01");
    partitionPaths.put("days", "name=a/name_trunc=a/id_bucket=4/ts_day=2024-01-01");
    partitionPaths.put("hours", "name=a/name_trunc=a/id_bucket=4/ts_hour=2024-01-01-12");

    partitionPaths
        .keySet()
        .forEach(
            func -> {
              String tableName = String.format("test_iceberg_%s_partition_table", func);
              dropTableIfExists(tableName);
              String createTableSQL = getCreateIcebergSimpleTableString(tableName);
              createTableSQL =
                  createTableSQL
                      + String.format(
                          " PARTITIONED BY (name, truncate(1, name), bucket(16, id), %s(ts));",
                          func);
              sql(createTableSQL);
              SparkTableInfo tableInfo = getTableInfo(tableName);
              SparkTableInfoChecker checker =
                  SparkTableInfoChecker.create()
                      .withName(tableName)
                      .withColumns(getIcebergSimpleTableColumn())
                      .withIdentifyPartition(Collections.singletonList("name"))
                      .withTruncatePartition(1, "name")
                      .withBucketPartition(16, Collections.singletonList("id"));
              switch (func) {
                case "years":
                  checker.withYearPartition("ts");
                  break;
                case "months":
                  checker.withMonthPartition("ts");
                  break;
                case "days":
                  checker.withDayPartition("ts");
                  break;
                case "hours":
                  checker.withHourPartition("ts");
                  break;
                default:
                  throw new IllegalArgumentException("UnSupported partition function: " + func);
              }
              checker.check(tableInfo);

              String insertData =
                  String.format(
                      "INSERT into %s values(2,'a',cast('2024-01-01 12:00:00' as timestamp));",
                      tableName);
              sql(insertData);
              List<String> queryResult = getTableData(tableName);
              Assertions.assertEquals(1, queryResult.size());
              Assertions.assertEquals("2,a,2024-01-01 12:00:00", queryResult.get(0));
              String partitionExpression = partitionPaths.get(func);
              Path partitionPath = new Path(getTableLocation(tableInfo), partitionExpression);
              checkDirExists(partitionPath);
            });
  }

  @Test
  void testIcebergMetadataColumns() throws NoSuchTableException {
    testMetadataColumns();
    testSpecAndPartitionMetadataColumns();
    testPositionMetadataColumn();
    testPartitionMetadataColumnWithUnPartitionedTable();
    testFileMetadataColumn();
    testDeleteMetadataColumn();
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
    String tableName = "test_copy_on_write_delete_unpartitioned";
    createIcebergTableWithTabProperties(
        tableName, false, ImmutableMap.of(ICEBERG_DELETE_MODE, ICEBERG_COPY_ON_WRITE));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testCopyOnWriteDeleteInPartitionedTable() {
    String tableName = "test_copy_on_write_delete_partitioned";
    createIcebergTableWithTabProperties(
        tableName, true, ImmutableMap.of(ICEBERG_DELETE_MODE, ICEBERG_COPY_ON_WRITE));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testMergeOnReadDeleteInUnPartitionedTable() {
    String tableName = "test_merge_on_read_delete_unpartitioned";
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_DELETE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testMergeOnReadDeleteInPartitionedTable() {
    String tableName = "test_merge_on_read_delete_partitioned";
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_DELETE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableRowLevelDelete(tableName);
  }

  @Test
  void testCopyOnWriteUpdateInUnPartitionedTable() {
    String tableName = "test_copy_on_write_update_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, false, ImmutableMap.of(ICEBERG_UPDATE_MODE, ICEBERG_COPY_ON_WRITE));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testCopyOnWriteUpdateInPartitionedTable() {
    String tableName = "test_copy_on_write_update_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, true, ImmutableMap.of(ICEBERG_UPDATE_MODE, ICEBERG_COPY_ON_WRITE));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testMergeOnReadUpdateInUnPartitionedTable() {
    String tableName = "test_merge_on_read_update_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_UPDATE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testMergeOnReadUpdateInPartitionedTable() {
    String tableName = "test_merge_on_read_update_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_UPDATE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableReadAndUpdate(table);
  }

  @Test
  void testCopyOnWriteMergeUpdateInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_update_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, false, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testCopyOnWriteMergeUpdateInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_update_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, true, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testMergeOnReadMergeUpdateInUnPartitionedTable() {
    String tableName = "test_merge_on_read_merge_update_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testMergeOnReadMergeUpdateInPartitionedTable() {
    String tableName = "test_merge_on_read_merge_update_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));
    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableUpdateInMerge(table);
  }

  @Test
  void testCopyOnWriteInMergeDeleteInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_delete_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, false, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testCopyOnWriteInMergeDeleteInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_delete_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, true, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testMergeOnReadInMergeDeleteInUnPartitionedTable() {
    String tableName = "test_merge_on_read_merge_delete_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testMergeOnReadInMergeDeleteInPartitionedTable() {
    String tableName = "test_merge_on_read_merge_delete_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableDeleteInMerge(table);
  }

  @Test
  void testCopyOnWriteInsertInMergeInUnPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, false, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testCopyOnWriteInsertInMergeInPartitionedTable() {
    String tableName = "test_copy_on_write_merge_insert_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName, true, ImmutableMap.of(ICEBERG_MERGE_MODE, ICEBERG_COPY_ON_WRITE));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testMergeOnReadInsertInMergeInUnPartitionedTable() {
    String tableName = "test_merge_on_read_merge_insert_unpartitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  @Test
  void testMergeOnReadInsertInMergeInPartitionedTable() {
    String tableName = "test_merge_on_read_merge_insert_partitioned";
    dropTableIfExists(tableName);
    createIcebergTableWithTabProperties(
        tableName,
        true,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_MERGE_MODE, ICEBERG_MERGE_ON_READ));

    SparkTableInfo table = getTableInfo(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), table);
    checkTableInsertInMerge(table);
  }

  private void testMetadataColumns() {
    String tableName = "test_metadata_columns";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);
  }

  private void testSpecAndPartitionMetadataColumns() {
    String tableName = "test_spec_partition";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);

    String expectedMetadata = "0,a";
    String getMetadataSQL =
        String.format("SELECT _spec_id, _partition FROM %s ORDER BY _spec_id", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals(expectedMetadata, queryResult.get(0));
  }

  private void testPositionMetadataColumn() throws NoSuchTableException {
    String tableName = "test_position_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
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

  private void testPartitionMetadataColumnWithUnPartitionedTable() {
    String tableName = "test_position_metadata_column_in_unpartitioned_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
    metadataColumns[1] =
        new SparkMetadataColumnInfo(
            "_partition", DataTypes.createStructType(new StructField[] {}), true);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);

    String getMetadataSQL = String.format("SELECT _partition FROM %s", tableName);
    Assertions.assertEquals(1, getSparkSession().sql(getMetadataSQL).count());
    Row row = getSparkSession().sql(getMetadataSQL).collectAsList().get(0);
    Assertions.assertNotNull(row);
    Assertions.assertNull(row.get(0));
  }

  private void testFileMetadataColumn() {
    String tableName = "test_file_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);

    String getMetadataSQL = String.format("SELECT _file FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertTrue(queryResult.get(0).contains(tableName));
  }

  private void testDeleteMetadataColumn() {
    String tableName = "test_delete_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getIcebergMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 1);", tableName);
    sql(insertData);

    String getMetadataSQL = String.format("SELECT _deleted FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("false", queryResult.get(0));

    sql(getDeleteSql(tableName, "1 = 1"));

    List<String> queryResult1 = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(0, queryResult1.size());
  }

  private List<SparkTableInfo.SparkColumnInfo> getIcebergSimpleTableColumn() {
    return Arrays.asList(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkTableInfo.SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkTableInfo.SparkColumnInfo.of("ts", DataTypes.TimestampType, null));
  }

  /**
   * Here we build a new `createIcebergSql` String for creating a table with a field of timestamp
   * type to create the year/month,etc partitions
   */
  private String getCreateIcebergSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', ts TIMESTAMP)",
        tableName);
  }

  private SparkMetadataColumnInfo[] getIcebergMetadataColumns() {
    return new SparkMetadataColumnInfo[] {
      new SparkMetadataColumnInfo("_spec_id", DataTypes.IntegerType, false),
      new SparkMetadataColumnInfo(
          "_partition",
          DataTypes.createStructType(
              new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)}),
          true),
      new SparkMetadataColumnInfo("_file", DataTypes.StringType, false),
      new SparkMetadataColumnInfo("_pos", DataTypes.LongType, false),
      new SparkMetadataColumnInfo("_deleted", DataTypes.BooleanType, false)
    };
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
}
