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
package org.apache.gravitino.spark.connector.integration.test.paimon;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.apache.gravitino.spark.connector.integration.test.util.SparkMetadataColumnInfo;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfo;
import org.apache.gravitino.spark.connector.integration.test.util.SparkTableInfoChecker;
import org.apache.gravitino.spark.connector.paimon.PaimonPropertiesConstants;
import org.apache.gravitino.spark.connector.paimon.SparkPaimonTable;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class SparkPaimonCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "paimon";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
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
    return false;
  }

  @Override
  protected boolean supportsSchemaEvolution() {
    return true;
  }

  @Override
  protected boolean supportsReplaceColumns() {
    // Paimon doesn't support replace columns, because it doesn't support drop all fields in table.
    // And `ALTER TABLE REPLACE COLUMNS` statement will remove all existing columns at first and
    // then adds the new set of columns.
    return false;
  }

  @Override
  protected String getTableLocation(SparkTableInfo table) {
    Map<String, String> tableProperties = table.getTableProperties();
    return tableProperties.get(PaimonPropertiesConstants.PAIMON_TABLE_LOCATION);
  }

  @Test
  void testPaimonPartitions() {
    String partitionPathString = "name=a/address=beijing";

    String tableName = "test_paimon_partition_table";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name, address);";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getPaimonSimpleTableColumn())
            .withIdentifyPartition(Collections.singletonList("name"))
            .withIdentifyPartition(Collections.singletonList("address"));
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a','beijing');", tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals("2,a,beijing", queryResult.get(0));
    Path partitionPath = new Path(getTableLocation(tableInfo), partitionPathString);
    checkDirExists(partitionPath);
  }

  @Test
  void testPaimonPartitionManagement() {
    // replace, add and load partition operations are unsupported in Paimon now.
    // Therefore, Paimon spark runtime only supports list and drop partition operations.
    testPaimonListAndDropPartition();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPaimonMetadataColumns(boolean isPartitioned) {
    testMetadataColumns();
    testFilePathMetadataColumn(isPartitioned);
    testRowIndexMetadataColumn(isPartitioned);
  }

  @Test
  void testPaimonListAndLoadFunctions() throws NoSuchNamespaceException, NoSuchFunctionException {
    String[] empty_namespace = new String[] {};
    String[] system_namespace = new String[] {"sys"};
    String[] default_namespace = new String[] {getDefaultDatabase()};
    String[] non_exists_namespace = new String[] {"non_existent"};
    // Paimon Spark connector only support bucket function now.
    List<String> functions = Collections.singletonList("bucket");

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
  void testShowPaimonFunctions() {
    AnalysisException analysisException =
        Assertions.assertThrows(AnalysisException.class, () -> sql("SHOW FUNCTIONS;"));
    Assertions.assertTrue(
        analysisException.getMessage().contains("Catalog paimon does not support functions"));
  }

  @Test
  void testPaimonTimeTravelQuery() throws NoSuchTableException {
    String tableName = "test_paimon_as_of_query";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    checkTableColumns(tableName, getSimpleTableColumn(), getTableInfo(tableName));

    sql(String.format("INSERT INTO %s VALUES (1, '1', 1)", tableName));
    List<String> tableData = getQueryData(getSelectAllSqlWithOrder(tableName, "id"));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));

    SparkPaimonTable sparkPaimonTable = getSparkPaimonTableInstance(tableName);
    long snapshotId = getCurrentSnapshotId(tableName);
    sparkPaimonTable.table().createTag("test_tag", snapshotId);
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

    tableData = getQueryData(String.format("SELECT * FROM %s VERSION AS OF 'test_tag'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
    tableData =
        getQueryData(
            String.format("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_tag'", tableName));
    Assertions.assertEquals(1, tableData.size());
    Assertions.assertEquals("1,1,1", tableData.get(0));
  }

  private void testMetadataColumns() {
    String tableName = "test_metadata_columns";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getPaimonMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getPaimonSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);
  }

  private void testFilePathMetadataColumn(boolean isPartitioned) {
    String tableName = "test_file_path_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    if (isPartitioned) {
      createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    }
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getPaimonMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getPaimonSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 'beijing');", tableName);
    sql(insertData);

    String getMetadataSQL = String.format("SELECT __paimon_file_path FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertTrue(queryResult.get(0).contains(tableName));
  }

  private void testRowIndexMetadataColumn(boolean isPartitioned) {
    String tableName = "test_row_index_metadata_column";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    if (isPartitioned) {
      createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    }
    sql(createTableSQL);

    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkMetadataColumnInfo[] metadataColumns = getPaimonMetadataColumns();
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getPaimonSimpleTableColumn())
            .withMetadataColumns(metadataColumns);
    checker.check(tableInfo);

    String insertData = String.format("INSERT into %s values(2,'a', 'beijing');", tableName);
    sql(insertData);

    String getMetadataSQL = String.format("SELECT __paimon_row_index FROM %s", tableName);
    List<String> queryResult = getTableMetadata(getMetadataSQL);
    Assertions.assertEquals(1, queryResult.size());
    Assertions.assertEquals(0, Integer.parseInt(queryResult.get(0)));
  }

  private void testPaimonListAndDropPartition() {
    String tableName = "test_paimon_drop_partition";
    dropTableIfExists(tableName);
    String createTableSQL = getCreatePaimonSimpleTableString(tableName);
    createTableSQL = createTableSQL + " PARTITIONED BY (name);";
    sql(createTableSQL);

    String insertData =
        String.format(
            "INSERT into %s values(1,'a','beijing'), (2,'b','beijing'), (3,'c','beijing');",
            tableName);
    sql(insertData);
    List<String> queryResult = getTableData(tableName);
    Assertions.assertEquals(3, queryResult.size());

    List<String> partitions = getQueryData(String.format("show partitions %s", tableName));
    Assertions.assertEquals(3, partitions.size());
    Assertions.assertEquals("name=a;name=b;name=c", String.join(";", partitions));

    sql(String.format("ALTER TABLE %s DROP PARTITION (`name`='a')", tableName));
    partitions = getQueryData(String.format("show partitions %s", tableName));
    Assertions.assertEquals(2, partitions.size());
    Assertions.assertEquals("name=b;name=c", String.join(";", partitions));
  }

  private String getCreatePaimonSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', address STRING COMMENT '') USING paimon",
        tableName);
  }

  private List<SparkTableInfo.SparkColumnInfo> getPaimonSimpleTableColumn() {
    return Arrays.asList(
        SparkTableInfo.SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkTableInfo.SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkTableInfo.SparkColumnInfo.of("address", DataTypes.StringType, ""));
  }

  private SparkMetadataColumnInfo[] getPaimonMetadataColumns() {
    return new SparkMetadataColumnInfo[] {
      new SparkMetadataColumnInfo("__paimon_file_path", DataTypes.StringType, true),
      new SparkMetadataColumnInfo("__paimon_row_index", DataTypes.LongType, true)
    };
  }

  private SparkPaimonTable getSparkPaimonTableInstance(String tableName)
      throws NoSuchTableException {
    CatalogPlugin catalogPlugin =
        getSparkSession().sessionState().catalogManager().catalog(getCatalogName());
    Assertions.assertInstanceOf(TableCatalog.class, catalogPlugin);
    TableCatalog catalog = (TableCatalog) catalogPlugin;
    Table table = catalog.loadTable(Identifier.of(new String[] {getDefaultDatabase()}, tableName));
    return (SparkPaimonTable) table;
  }

  private long getCurrentSnapshotTimestamp(String tableName) throws NoSuchTableException {
    SparkPaimonTable sparkPaimonTable = getSparkPaimonTableInstance(tableName);
    long latestSnapshotId = getCurrentSnapshotId(tableName);
    return sparkPaimonTable.table().snapshot(latestSnapshotId).timeMillis();
  }

  private long getCurrentSnapshotId(String tableName) throws NoSuchTableException {
    SparkPaimonTable sparkPaimonTable = getSparkPaimonTableInstance(tableName);
    return sparkPaimonTable.table().latestSnapshotId().getAsLong();
  }
}
