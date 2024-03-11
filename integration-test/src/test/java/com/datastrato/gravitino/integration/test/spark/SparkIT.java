/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.platform.commons.util.StringUtils;

@Tag("gravitino-docker-it")
@TestInstance(Lifecycle.PER_CLASS)
public class SparkIT extends SparkEnvIT {

  private static final String SELECT_ALL_TEMPLATE = "SELECT * FROM %s";
  private static final String INSERT_WITHOUT_PARTITION_TEMPLATE = "INSERT INTO %s VALUES (%s)";

  // To generate test data for write&read table.
  private static final Map<DataType, String> typeConstant =
      ImmutableMap.of(DataTypes.IntegerType, "2", DataTypes.StringType, "'gravitino_it_test'");

  // Use a custom database not the original default database because SparkIT couldn't read&write
  // data to tables in default database. The main reason is default database location is
  // determined by `hive.metastore.warehouse.dir` in hive-site.xml which is local HDFS address
  // not real HDFS address. The location of tables created under default database is like
  // hdfs://localhost:9000/xxx which couldn't read write data from SparkIT. Will use default
  // database after spark connector support Alter database xx set location command.
  @BeforeAll
  void initDefaultDatabase() {
    sql("USE " + hiveCatalogName);
    createDatabaseIfNotExists(getDefaultDatabase());
  }

  @BeforeEach
  void init() {
    sql("USE " + hiveCatalogName);
    sql("USE " + getDefaultDatabase());
  }

  private String getDefaultDatabase() {
    return "default_db";
  }

  @Test
  void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(hiveCatalogName));
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

  @Test
  void testDropSchema() {
    String testDatabaseName = "t_drop";
    Set<String> databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    sql("CREATE DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertTrue(databases.contains(testDatabaseName));

    sql("DROP DATABASE " + testDatabaseName);
    databases = getDatabases();
    Assertions.assertFalse(databases.contains(testDatabaseName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE notExists"));
  }

  @Test
  void testCreateSimpleTable() {
    String tableName = "simple_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withComment(null);
    checker.check(tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  void testCreateTableWithDatabase() {
    // test db.table as table identifier
    String databaseName = "db1";
    String tableName = "table1";
    createDatabaseIfNotExists(databaseName);
    String tableIdentifier = String.join(".", databaseName, tableName);

    createSimpleTable(tableIdentifier);
    SparkTableInfo tableInfo = getTableInfo(tableIdentifier);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create().withName(tableName).withColumns(getSimpleTableColumn());
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);

    // use db then create table with table name
    databaseName = "db2";
    tableName = "table2";
    createDatabaseIfNotExists(databaseName);

    sql("USE " + databaseName);
    createSimpleTable(tableName);
    tableInfo = getTableInfo(tableName);
    checker =
        SparkTableInfoChecker.create().withName(tableName).withColumns(getSimpleTableColumn());
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  void testCreateTableWithComment() {
    String tableName = "comment_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    String tableComment = "tableComment";
    createTableSql = String.format("%s COMMENT '%s'", createTableSql, tableComment);
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withComment(tableComment);
    checker.check(tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  void testDropTable() {
    String tableName = "drop_table";
    createSimpleTable(tableName);
    Assertions.assertEquals(true, tableExists(tableName));

    dropTableIfExists(tableName);
    Assertions.assertEquals(false, tableExists(tableName));

    Assertions.assertThrowsExactly(NoSuchTableException.class, () -> sql("DROP TABLE not_exists"));
  }

  @Test
  void testRenameTable() {
    String tableName = "rename1";
    String newTableName = "rename2";
    dropTableIfExists(tableName);
    dropTableIfExists(newTableName);

    createSimpleTable(tableName);
    Assertions.assertTrue(tableExists(tableName));
    Assertions.assertFalse(tableExists(newTableName));

    sql(String.format("ALTER TABLE %s RENAME TO %s", tableName, newTableName));
    Assertions.assertTrue(tableExists(newTableName));
    Assertions.assertFalse(tableExists(tableName));

    // rename to an existing table
    createSimpleTable(tableName);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> sql(String.format("ALTER TABLE %s RENAME TO %s", tableName, newTableName)));

    // rename a not existing tables
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("ALTER TABLE not_exists1 RENAME TO not_exist2"));
  }

  @Test
  void testListTable() {
    String table1 = "list1";
    String table2 = "list2";
    createSimpleTable(table1);
    createSimpleTable(table2);
    Set<String> tables = listTableNames();
    Assertions.assertTrue(tables.contains(table1));
    Assertions.assertTrue(tables.contains(table2));

    // show tables from not current db
    String database = "db_list";
    String table3 = "list3";
    String table4 = "list4";
    createDatabaseIfNotExists(database);
    createSimpleTable(String.join(".", database, table3));
    createSimpleTable(String.join(".", database, table4));
    tables = listTableNames(database);

    Assertions.assertTrue(tables.contains(table3));
    Assertions.assertTrue(tables.contains(table4));

    Assertions.assertThrows(NoSuchNamespaceException.class, () -> listTableNames("not_exists_db"));
  }

  @Test
  void testAlterTableSetAndRemoveProperty() {
    String tableName = "test_property";
    dropTableIfExists(tableName);

    createSimpleTable(tableName);
    sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES('key1'='value1', 'key2'='value2')", tableName));
    Map<String, String> oldProperties = getTableInfo(tableName).getTableProperties();
    Assertions.assertTrue(oldProperties.containsKey("key1") && oldProperties.containsKey("key2"));

    sql(String.format("ALTER TABLE %s UNSET TBLPROPERTIES('key1')", tableName));
    Map<String, String> newProperties = getTableInfo(tableName).getTableProperties();
    Assertions.assertFalse(newProperties.containsKey("key1"));
    Assertions.assertTrue(newProperties.containsKey("key2"));
  }

  @Test
  void testAlterTableAddAndDeleteColumn() {
    String tableName = "test_column";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();

    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 string)", tableName));
    ArrayList<SparkColumnInfo> addColumns = new ArrayList<>(simpleTableColumns);
    addColumns.add(SparkColumnInfo.of("col1", DataTypes.StringType, null));
    checkTableColumns(tableName, addColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S DROP COLUMNS (col1)", tableName));
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));
  }

  private void checkTableColumns(
      String tableName, List<SparkColumnInfo> columnInfos, SparkTableInfo tableInfo) {
    SparkTableInfoChecker.create()
        .withName(tableName)
        .withColumns(columnInfos)
        .withComment(null)
        .check(tableInfo);
  }

  private void checkTableReadWrite(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    String insertValues =
        table.getColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    sql(String.format(INSERT_WITHOUT_PARTITION_TEMPLATE, name, insertValues));

    // remove "'" from values, such as 'a' is trans to a
    String checkValues =
        table.getColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .map(
                s -> {
                  String tmp = org.apache.commons.lang3.StringUtils.removeEnd(s, "'");
                  tmp = org.apache.commons.lang3.StringUtils.removeStart(tmp, "'");
                  return tmp;
                })
            .collect(Collectors.joining(","));

    List<String> queryResult =
        sql(String.format(SELECT_ALL_TEMPLATE, name)).stream()
            .map(
                line ->
                    Arrays.stream(line)
                        .map(item -> item.toString())
                        .collect(Collectors.joining(",")))
            .collect(Collectors.toList());
    Assertions.assertTrue(
        queryResult.size() == 1, "Should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  private String getCreateSimpleTableString(String tableName) {
    return String.format(
        "CREATE TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', age INT)",
        tableName);
  }

  private List<SparkColumnInfo> getSimpleTableColumn() {
    return Arrays.asList(
        SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkColumnInfo.of("age", DataTypes.IntegerType, null));
  }

  // Helper method to create a simple table, and could use corresponding
  // getSimpleTableColumn to check table column.
  private void createSimpleTable(String identifier) {
    String createTableSql = getCreateSimpleTableString(identifier);
    sql(createTableSql);
  }
}
