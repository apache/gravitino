/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.integration.test.iceberg;

import com.datastrato.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import com.datastrato.gravitino.spark.connector.iceberg.SparkIcebergTable;
import com.datastrato.gravitino.spark.connector.integration.test.SparkCommonIT;
import com.datastrato.gravitino.spark.connector.integration.test.util.SparkMetadataColumnInfo;
import com.datastrato.gravitino.spark.connector.integration.test.util.SparkTableInfo;
import com.datastrato.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class SparkIcebergCatalogIT extends SparkCommonIT {

  private static final String ICEBERG_FORMAT_VERSION = "format-version";
  private static final String ICEBERG_DELETE_MODE = "write.delete.mode";
  private static final String ICEBERG_UPDATE_MODE = "write.update.mode";
  private static final String ICEBERG_MERGE_MODE = "write.merge.mode";

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

  @ParameterizedTest
  @MethodSource("getIcebergTablePropertyValues")
  void testIcebergTableRowLevelOperations(IcebergTableWriteProperties icebergTableWriteProperties) {
    testIcebergDeleteOperation(icebergTableWriteProperties);
    testIcebergUpdateOperation(icebergTableWriteProperties);
    testIcebergMergeIntoDeleteOperation(icebergTableWriteProperties);
    testIcebergMergeIntoUpdateOperation(icebergTableWriteProperties);
  }

  @Test
  void testIcebergCallOperations() throws NoSuchTableException {
    testIcebergCallRollbackToSnapshot();
    testIcebergCallSetCurrentSnapshot();
    testIcebergCallRewriteDataFiles();
    testIcebergCallRewriteManifests();
    testIcebergCallRewritePositionDeleteFiles();
  }

  @Test
  void testIcebergTimeTravelQuery() throws NoSuchTableException {
    String tableName = "test_iceberg_as_of_query";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));

    sql(String.format("INSERT INTO %s VALUES (1, '1', 1)", tableName));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    SparkIcebergTable sparkIcebergTable = getSparkIcebergTableInstance(tableName);
    long snapshotId = getCurrentSnapshotId(tableName);
    sparkIcebergTable.table().manageSnapshots().createBranch("test_branch", snapshotId).commit();
    sparkIcebergTable.table().manageSnapshots().createTag("test_tag", snapshotId).commit();
    long snapshotTimestamp = getCurrentSnapshotTimestamp(tableName);
    long timestamp = waitUntilAfter(snapshotTimestamp + 1000);
    long timestampInSeconds = TimeUnit.MILLISECONDS.toSeconds(timestamp);

    // create a second snapshot
    sql(String.format("INSERT INTO %s VALUES (2, '2', 2)", tableName));
    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(2, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2", String.join(";", tableData));

    tableData =
        getQueryData(
            String.format("SELECT * FROM %s TIMESTAMP AS OF %s", tableName, timestampInSeconds));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
    tableData =
        getQueryData(
            String.format(
                "SELECT * FROM %s FOR SYSTEM_TIME AS OF %s", tableName, timestampInSeconds));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    tableData =
        getQueryData(String.format("SELECT * FROM %s VERSION AS OF %d", tableName, snapshotId));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
    tableData =
        getQueryData(
            String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF %d", tableName, snapshotId));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    tableData =
        getQueryData(String.format("SELECT * FROM %s VERSION AS OF 'test_branch'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
    tableData =
        getQueryData(
            String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_branch'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    tableData = getQueryData(String.format("SELECT * FROM %s VERSION AS OF 'test_tag'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
    tableData =
        getQueryData(
            String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_tag'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
  }

  @Test
  void testIcebergReservedProperties() throws NoSuchTableException {
    String tableName = "test_reserved_properties";
    dropTableIfExists(tableName);
    sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL COMMENT 'id comment', name STRING COMMENT '', age INT)",
            tableName));

    SparkTableInfo tableInfo = getTableInfo(tableName);
    Map<String, String> tableProperties = tableInfo.getTableProperties();
    Assertions.assertNotNull(tableProperties);
    Assertions.assertEquals(
        "iceberg", tableProperties.get(IcebergPropertiesConstants.ICEBERG_PROVIDER));
    Assertions.assertEquals(
        "iceberg/parquet", tableProperties.get(IcebergPropertiesConstants.ICEBERG_FILE_FORMAT));
    Assertions.assertTrue(
        StringUtils.isNotBlank(IcebergPropertiesConstants.ICEBERG_LOCATION)
            && tableProperties
                .get(IcebergPropertiesConstants.ICEBERG_LOCATION)
                .contains(tableName));
    Assertions.assertTrue(
        tableProperties.containsKey(IcebergPropertiesConstants.ICEBERG_FORMAT_VERSION));

    Assertions.assertEquals(
        "none", tableProperties.get(IcebergPropertiesConstants.ICEBERG_CURRENT_SNAPSHOT_ID));
    Assertions.assertFalse(
        tableProperties.containsKey(IcebergPropertiesConstants.ICEBERG_SORT_ORDER));
    Assertions.assertFalse(
        tableProperties.containsKey(IcebergPropertiesConstants.ICEBERG_IDENTIFIER_FIELDS));

    // create a new snapshot
    sql(String.format("INSERT INTO %s VALUES(1, '1', 1)", tableName));

    SparkIcebergTable sparkIcebergTable = getSparkIcebergTableInstance(tableName);
    // set Identifier fields
    sparkIcebergTable.table().updateSchema().setIdentifierFields("id").commit();

    // set sort orders
    ReplaceSortOrder orderBuilder = sparkIcebergTable.table().replaceSortOrder();
    orderBuilder.asc("id");
    orderBuilder.commit();

    sparkIcebergTable.table().refresh();

    tableInfo = getTableInfo(tableName);
    tableProperties = tableInfo.getTableProperties();
    Assertions.assertEquals(
        String.valueOf(sparkIcebergTable.table().currentSnapshot().snapshotId()),
        tableProperties.get(IcebergPropertiesConstants.ICEBERG_CURRENT_SNAPSHOT_ID));
    Assertions.assertEquals(
        "[id]", tableProperties.get(IcebergPropertiesConstants.ICEBERG_IDENTIFIER_FIELDS));
    Assertions.assertEquals(
        "id ASC NULLS FIRST", tableProperties.get(IcebergPropertiesConstants.ICEBERG_SORT_ORDER));
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

  private void testIcebergDeleteOperation(IcebergTableWriteProperties icebergTableWriteProperties) {
    String tableName =
        String.format(
            "test_iceberg_%s_%s_delete_operation",
            icebergTableWriteProperties.isPartitionedTable,
            icebergTableWriteProperties.formatVersion);
    dropTableIfExists(tableName);
    createIcebergTableWithTableProperties(
        tableName,
        icebergTableWriteProperties.isPartitionedTable,
        ImmutableMap.of(
            ICEBERG_FORMAT_VERSION,
            String.valueOf(icebergTableWriteProperties.formatVersion),
            ICEBERG_DELETE_MODE,
            icebergTableWriteProperties.writeMode));
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));
    checkRowLevelDelete(tableName);
  }

  private void testIcebergUpdateOperation(IcebergTableWriteProperties icebergTableWriteProperties) {
    String tableName =
        String.format(
            "test_iceberg_%s_%s_update_operation",
            icebergTableWriteProperties.isPartitionedTable,
            icebergTableWriteProperties.formatVersion);
    dropTableIfExists(tableName);
    createIcebergTableWithTableProperties(
        tableName,
        icebergTableWriteProperties.isPartitionedTable,
        ImmutableMap.of(
            ICEBERG_FORMAT_VERSION,
            String.valueOf(icebergTableWriteProperties.formatVersion),
            ICEBERG_UPDATE_MODE,
            icebergTableWriteProperties.writeMode));
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));
    checkRowLevelUpdate(tableName);
  }

  private void testIcebergMergeIntoDeleteOperation(
      IcebergTableWriteProperties icebergTableWriteProperties) {
    String tableName =
        String.format(
            "test_iceberg_%s_%s_mergeinto_delete_operation",
            icebergTableWriteProperties.isPartitionedTable,
            icebergTableWriteProperties.formatVersion);
    dropTableIfExists(tableName);
    createIcebergTableWithTableProperties(
        tableName,
        icebergTableWriteProperties.isPartitionedTable,
        ImmutableMap.of(
            ICEBERG_FORMAT_VERSION,
            String.valueOf(icebergTableWriteProperties.formatVersion),
            ICEBERG_MERGE_MODE,
            icebergTableWriteProperties.writeMode));
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));
    checkDeleteByMergeInto(tableName);
  }

  private void testIcebergMergeIntoUpdateOperation(
      IcebergTableWriteProperties icebergTableWriteProperties) {
    String tableName =
        String.format(
            "test_iceberg_%s_%s_mergeinto_update_operation",
            icebergTableWriteProperties.isPartitionedTable,
            icebergTableWriteProperties.formatVersion);
    dropTableIfExists(tableName);
    createIcebergTableWithTableProperties(
        tableName,
        icebergTableWriteProperties.isPartitionedTable,
        ImmutableMap.of(
            ICEBERG_FORMAT_VERSION,
            String.valueOf(icebergTableWriteProperties.formatVersion),
            ICEBERG_MERGE_MODE,
            icebergTableWriteProperties.writeMode));
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));
    checkTableUpdateByMergeInto(tableName);
  }

  private void testIcebergCallRollbackToSnapshot() throws NoSuchTableException {
    String fullTableName =
        String.format(
            "%s.%s.test_iceberg_call_rollback_to_snapshot", getCatalogName(), getDefaultDatabase());
    String tableName = "test_iceberg_call_rollback_to_snapshot";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    sql(String.format("INSERT INTO %s VALUES(1, '1', 1)", tableName));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    long snapshotId = getCurrentSnapshotId(tableName);

    sql(String.format("INSERT INTO %s VALUES(2, '2', 2)", tableName));
    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(2, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2", String.join(";", tableData));

    sql(
        String.format(
            "CALL %s.system.rollback_to_snapshot('%s', %d)",
            getCatalogName(), fullTableName, snapshotId));
    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
  }

  private void testIcebergCallSetCurrentSnapshot() throws NoSuchTableException {
    String fullTableName =
        String.format(
            "%s.%s.test_iceberg_call_set_current_snapshot", getCatalogName(), getDefaultDatabase());
    String tableName = "test_iceberg_call_set_current_snapshot";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    sql(String.format("INSERT INTO %s VALUES(1, '1', 1)", tableName));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    long snapshotId = getCurrentSnapshotId(tableName);

    sql(String.format("INSERT INTO %s VALUES(2, '2', 2)", tableName));
    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(2, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2", String.join(";", tableData));

    sql(
        String.format(
            "CALL %s.system.set_current_snapshot('%s', %d)",
            getCatalogName(), fullTableName, snapshotId));
    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
  }

  private void testIcebergCallRewriteDataFiles() {
    String fullTableName =
        String.format(
            "%s.%s.test_iceberg_call_rewrite_data_files", getCatalogName(), getDefaultDatabase());
    String tableName = "test_iceberg_call_rewrite_data_files";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    IntStream.rangeClosed(1, 5)
        .forEach(
            i -> sql(String.format("INSERT INTO %s VALUES(%d, '%d', %d)", tableName, i, i, i)));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(5, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", tableData));

    List<Row> callResult =
        getSparkSession()
            .sql(
                String.format(
                    "CALL %s.system.rewrite_data_files(table => '%s', strategy => 'sort', sort_order => 'id DESC NULLS LAST', where => 'id < 10')",
                    getCatalogName(), fullTableName))
            .collectAsList();
    Assertions.assertEquals(1, callResult.size());
    Assertions.assertEquals(5, callResult.get(0).getInt(0));
    Assertions.assertEquals(1, callResult.get(0).getInt(1));
  }

  private void testIcebergCallRewriteManifests() {
    String fullTableName =
        String.format("%s.%s.rewrite_manifests", getCatalogName(), getDefaultDatabase());
    String tableName = "rewrite_manifests";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);

    IntStream.rangeClosed(1, 5)
        .forEach(
            i -> sql(String.format("INSERT INTO %s VALUES(%d, '%d', %d)", tableName, i, i, i)));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(5, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", tableData));

    List<Row> callResult =
        getSparkSession()
            .sql(
                String.format(
                    "CALL %s.system.rewrite_manifests(table => '%s', use_caching => false)",
                    getCatalogName(), fullTableName))
            .collectAsList();
    Assertions.assertEquals(1, callResult.size());
    Assertions.assertEquals(5, callResult.get(0).getInt(0));
    Assertions.assertEquals(1, callResult.get(0).getInt(1));
  }

  private void testIcebergCallRewritePositionDeleteFiles() {
    String fullTableName =
        String.format(
            "%s.%s.rewrite_position_delete_files", getCatalogName(), getDefaultDatabase());
    String tableName = "rewrite_position_delete_files";
    dropTableIfExists(tableName);
    createIcebergTableWithTableProperties(
        tableName,
        false,
        ImmutableMap.of(ICEBERG_FORMAT_VERSION, "2", ICEBERG_DELETE_MODE, "merge-on-read"));

    sql(
        String.format(
            "INSERT INTO %s VALUES(1, '1', 1), (2, '2', 2), (3, '3', 3), (4, '4', 4), (5, '5', 5)",
            tableName));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(5, tableData.size());
    Assertions.assertEquals("1,1,1;2,2,2;3,3,3;4,4,4;5,5,5", String.join(";", tableData));

    sql(String.format("DELETE FROM %s WHERE id = 1", tableName));
    sql(String.format("DELETE FROM %s WHERE id = 2", tableName));

    tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(3, tableData.size());
    Assertions.assertEquals("3,3,3;4,4,4;5,5,5", String.join(";", tableData));

    List<Row> callResult =
        getSparkSession()
            .sql(
                String.format(
                    "CALL %s.system.rewrite_position_delete_files(table => '%s', options => map('rewrite-all','true'))",
                    getCatalogName(), fullTableName))
            .collectAsList();
    Assertions.assertEquals(1, callResult.size());
    Assertions.assertEquals(2, callResult.get(0).getInt(0));
    Assertions.assertEquals(1, callResult.get(0).getInt(1));
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

  private List<IcebergTableWriteProperties> getIcebergTablePropertyValues() {
    return Arrays.asList(
        IcebergTableWriteProperties.of(false, 1, "copy-on-write"),
        IcebergTableWriteProperties.of(false, 2, "merge-on-read"),
        IcebergTableWriteProperties.of(true, 1, "copy-on-write"),
        IcebergTableWriteProperties.of(true, 2, "merge-on-read"));
  }

  private void createIcebergTableWithTableProperties(
      String tableName, boolean isPartitioned, ImmutableMap<String, String> tblProperties) {
    String partitionedClause = isPartitioned ? " PARTITIONED BY (name) " : "";
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

  @Data
  private static class IcebergTableWriteProperties {

    private boolean isPartitionedTable;
    private int formatVersion;
    private String writeMode;

    private IcebergTableWriteProperties(
        boolean isPartitionedTable, int formatVersion, String writeMode) {
      this.isPartitionedTable = isPartitionedTable;
      this.formatVersion = formatVersion;
      this.writeMode = writeMode;
    }

    static IcebergTableWriteProperties of(
        boolean isPartitionedTable, int formatVersion, String writeMode) {
      return new IcebergTableWriteProperties(isPartitionedTable, formatVersion, writeMode);
    }
  }

  private SparkIcebergTable getSparkIcebergTableInstance(String tableName)
      throws NoSuchTableException {
    CatalogPlugin catalogPlugin =
        getSparkSession().sessionState().catalogManager().catalog(getCatalogName());
    Assertions.assertInstanceOf(TableCatalog.class, catalogPlugin);
    TableCatalog catalog = (TableCatalog) catalogPlugin;
    Table table = catalog.loadTable(Identifier.of(new String[] {getDefaultDatabase()}, tableName));
    return (SparkIcebergTable) table;
  }

  private long getCurrentSnapshotTimestamp(String tableName) throws NoSuchTableException {
    SparkIcebergTable sparkIcebergTable = getSparkIcebergTableInstance(tableName);
    return sparkIcebergTable.table().currentSnapshot().timestampMillis();
  }

  private long getCurrentSnapshotId(String tableName) throws NoSuchTableException {
    SparkIcebergTable sparkIcebergTable = getSparkIcebergTableInstance(tableName);
    return sparkIcebergTable.table().currentSnapshot().snapshotId();
  }

  private long waitUntilAfter(Long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }
}
