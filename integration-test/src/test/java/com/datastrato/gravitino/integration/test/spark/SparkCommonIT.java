/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfo.SparkColumnInfo;
import com.datastrato.gravitino.integration.test.util.spark.SparkTableInfoChecker;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkCommonIT extends SparkEnvIT {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCommonIT.class);

  // To generate test data for write&read table.
  protected static final Map<DataType, String> typeConstant =
      ImmutableMap.of(
          DataTypes.IntegerType,
          "2",
          DataTypes.StringType,
          "'gravitino_it_test'",
          DataTypes.createArrayType(DataTypes.IntegerType),
          "array(1, 2, 3)",
          DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
          "map('a', 1)",
          DataTypes.createStructType(
              Arrays.asList(
                  DataTypes.createStructField("col1", DataTypes.IntegerType, true),
                  DataTypes.createStructField("col2", DataTypes.StringType, true))),
          "struct(1, 'a')");

  private static String getInsertWithoutPartitionSql(String tableName, String values) {
    return String.format("INSERT INTO %s VALUES (%s)", tableName, values);
  }

  private static String getInsertWithPartitionSql(
      String tableName, String partitionString, String values) {
    return String.format(
        "INSERT OVERWRITE %s PARTITION (%s) VALUES (%s)", tableName, partitionString, values);
  }

  protected static String getDeleteSql(String tableName, String condition) {
    return String.format("DELETE FROM %s where %s", tableName, condition);
  }

  // Whether supports [CLUSTERED BY col_name3 SORTED BY col_name INTO num_buckets BUCKETS]
  protected abstract boolean supportsSparkSQLClusteredBy();

  protected abstract boolean supportsPartition();

  // Use a custom database not the original default database because SparkCommonIT couldn't
  // read&write data to tables in default database. The main reason is default database location is
  // determined by `hive.metastore.warehouse.dir` in hive-site.xml which is local HDFS address
  // not real HDFS address. The location of tables created under default database is like
  // hdfs://localhost:9000/xxx which couldn't read write data from SparkCommonIT. Will use default
  // database after spark connector support Alter database xx set location command.
  @BeforeAll
  void initDefaultDatabase() throws IOException {
    // In embedded mode, derby acts as the backend database for the hive metastore
    // and creates a directory named metastore_db to store metadata,
    // supporting only one connection at a time.
    // Previously, only SparkHiveCatalogIT accessed derby without any exceptions.
    // Now, SparkIcebergCatalogIT exists at the same time.
    // This exception about `ERROR XSDB6: Another instance of Derby may have already
    // booted  the database {GRAVITINO_HOME}/integration-test/metastore_db` will occur when
    // SparkIcebergCatalogIT is initialized after the Sparkhivecatalogit is executed.
    // The main reason is that the lock file in the metastore_db directory is not cleaned so that a
    // new connection cannot be created,
    // so a clean operation is done here to ensure that a new connection can be created.
    File hiveLocalMetaStorePath = new File("metastore_db");
    try {
      if (hiveLocalMetaStorePath.exists()) {
        FileUtils.deleteDirectory(hiveLocalMetaStorePath);
      }
    } catch (IOException e) {
      LOG.error(String.format("delete director %s failed.", hiveLocalMetaStorePath), e);
      throw e;
    }
    sql("USE " + getCatalogName());
    createDatabaseIfNotExists(getDefaultDatabase());
  }

  @BeforeEach
  void init() {
    sql("USE " + getCatalogName());
    sql("USE " + getDefaultDatabase());
  }

  @AfterAll
  void cleanUp() {
    sql("USE " + getCatalogName());
    getDatabases()
        .forEach(database -> sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", database)));
  }

  @Test
  void testListTables() {
    String tableName = "t_list";
    dropTableIfExists(tableName);
    Set<String> tableNames = listTableNames();
    Assertions.assertFalse(tableNames.contains(tableName));
    createSimpleTable(tableName);
    tableNames = listTableNames();
    Assertions.assertTrue(tableNames.contains(tableName));
    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("SHOW TABLES IN nonexistent_schema"));
  }

  @Test
  void testLoadCatalogs() {
    Set<String> catalogs = getCatalogs();
    Assertions.assertTrue(catalogs.contains(getCatalogName()));
  }

  @Test
  void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName + " WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    String properties = databaseMeta.get("Properties");
    Assertions.assertTrue(properties.contains("(ID,001)"));

    testDatabaseName = "t_create2";
    dropDatabaseIfExists(testDatabaseName);
    String testDatabaseLocation = "/tmp/" + testDatabaseName;
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '%s'\n" + " WITH DBPROPERTIES (ID=002);",
            testDatabaseName, testDatabaseLocation));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    String comment = databaseMeta.get("Comment");
    Assertions.assertEquals("comment", comment);
    Assertions.assertEquals("datastrato", databaseMeta.get("Owner"));
    // underlying catalog may change /tmp/t_create2 to file:/tmp/t_create2
    Assertions.assertTrue(databaseMeta.get("Location").contains(testDatabaseLocation));
    properties = databaseMeta.get("Properties");
    Assertions.assertTrue(properties.contains("(ID,002)"));
  }

  @Test
  void testAlterSchema() {
    String testDatabaseName = "t_alter";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName + " WITH DBPROPERTIES (ID=001);");
    Assertions.assertTrue(
        getDatabaseMetadata(testDatabaseName).get("Properties").contains("(ID,001)"));

    sql(String.format("ALTER DATABASE %s SET DBPROPERTIES ('ID'='002')", testDatabaseName));
    Assertions.assertFalse(
        getDatabaseMetadata(testDatabaseName).get("Properties").contains("(ID,001)"));
    Assertions.assertTrue(
        getDatabaseMetadata(testDatabaseName).get("Properties").contains("(ID,002)"));

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

    dropTableIfExists(tableIdentifier);
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
    dropTableIfExists(tableName);
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
    dropTableIfExists(table1);
    dropTableIfExists(table2);
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
    dropTableIfExists(String.join(".", database, table3));
    dropTableIfExists(String.join(".", database, table4));
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

  @Test
  void testAlterTableUpdateColumnType() {
    String tableName = "test_column_type";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();

    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 int)", tableName));
    sql(String.format("ALTER TABLE %S CHANGE COLUMN col1 col1 bigint", tableName));
    ArrayList<SparkColumnInfo> updateColumns = new ArrayList<>(simpleTableColumns);
    updateColumns.add(SparkColumnInfo.of("col1", DataTypes.LongType, null));
    checkTableColumns(tableName, updateColumns, getTableInfo(tableName));
  }

  @Test
  void testAlterTableRenameColumn() {
    String tableName = "test_rename_column";
    dropTableIfExists(tableName);
    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();
    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    String oldColumnName = "col1";
    String newColumnName = "col2";

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 int)", tableName));
    sql(
        String.format(
            "ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, oldColumnName, newColumnName));
    ArrayList<SparkColumnInfo> renameColumns = new ArrayList<>(simpleTableColumns);
    renameColumns.add(SparkColumnInfo.of(newColumnName, DataTypes.IntegerType, null));
    checkTableColumns(tableName, renameColumns, getTableInfo(tableName));
  }

  @Test
  void testUpdateColumnPosition() {
    String tableName = "test_column_position";
    dropTableIfExists(tableName);

    List<SparkColumnInfo> simpleTableColumns =
        Arrays.asList(
            SparkColumnInfo.of("id", DataTypes.StringType, ""),
            SparkColumnInfo.of("name", DataTypes.StringType, ""),
            SparkColumnInfo.of("age", DataTypes.StringType, ""));

    sql(
        String.format(
            "CREATE TABLE %s (id STRING COMMENT '', name STRING COMMENT '', age STRING COMMENT '')",
            tableName));
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col1 STRING COMMENT '')", tableName));
    List<SparkColumnInfo> updateColumnPositionCol1 = new ArrayList<>(simpleTableColumns);
    updateColumnPositionCol1.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    checkTableColumns(tableName, updateColumnPositionCol1, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S CHANGE COLUMN col1 col1 STRING FIRST", tableName));
    List<SparkColumnInfo> updateColumnPositionFirst = new ArrayList<>();
    updateColumnPositionFirst.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionFirst.addAll(simpleTableColumns);
    checkTableColumns(tableName, updateColumnPositionFirst, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S ADD COLUMNS (col2 STRING COMMENT '')", tableName));
    List<SparkColumnInfo> updateColumnPositionCol2 = new ArrayList<>();
    updateColumnPositionCol2.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionCol2.addAll(simpleTableColumns);
    updateColumnPositionCol2.add(SparkColumnInfo.of("col2", DataTypes.StringType, ""));
    checkTableColumns(tableName, updateColumnPositionCol2, getTableInfo(tableName));

    sql(String.format("ALTER TABLE %S CHANGE COLUMN col2 col2 STRING AFTER col1", tableName));
    List<SparkColumnInfo> updateColumnPositionAfter = new ArrayList<>();
    updateColumnPositionAfter.add(SparkColumnInfo.of("col1", DataTypes.StringType, ""));
    updateColumnPositionAfter.add(SparkColumnInfo.of("col2", DataTypes.StringType, ""));
    updateColumnPositionAfter.addAll(simpleTableColumns);
    checkTableColumns(tableName, updateColumnPositionAfter, getTableInfo(tableName));
  }

  @Test
  void testAlterTableUpdateColumnComment() {
    String tableName = "test_update_column_comment";
    dropTableIfExists(tableName);
    List<SparkColumnInfo> simpleTableColumns = getSimpleTableColumn();
    createSimpleTable(tableName);
    checkTableColumns(tableName, simpleTableColumns, getTableInfo(tableName));

    String oldColumnComment = "col1_comment";
    String newColumnComment = "col1_new_comment";

    sql(
        String.format(
            "ALTER TABLE %S ADD COLUMNS (col1 int comment '%s')", tableName, oldColumnComment));
    sql(
        String.format(
            "ALTER TABLE %S CHANGE COLUMN col1 col1 int comment '%s'",
            tableName, newColumnComment));
    ArrayList<SparkColumnInfo> updateCommentColumns = new ArrayList<>(simpleTableColumns);
    updateCommentColumns.add(SparkColumnInfo.of("col1", DataTypes.IntegerType, newColumnComment));
    checkTableColumns(tableName, updateCommentColumns, getTableInfo(tableName));
  }

  @Test
  void testComplexType() {
    String tableName = "complex_type_table";
    dropTableIfExists(tableName);

    sql(
        String.format(
            "CREATE TABLE %s (col1 ARRAY<INT> COMMENT 'array', col2 MAP<STRING, INT> COMMENT 'map', col3 STRUCT<col1: INT, col2: STRING> COMMENT 'struct')",
            tableName));
    SparkTableInfo tableInfo = getTableInfo(tableName);
    List<SparkColumnInfo> expectedSparkInfo =
        Arrays.asList(
            SparkColumnInfo.of("col1", DataTypes.createArrayType(DataTypes.IntegerType), "array"),
            SparkColumnInfo.of(
                "col2",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                "map"),
            SparkColumnInfo.of(
                "col3",
                DataTypes.createStructType(
                    Arrays.asList(
                        DataTypes.createStructField("col1", DataTypes.IntegerType, true),
                        DataTypes.createStructField("col2", DataTypes.StringType, true))),
                "struct"));
    checkTableColumns(tableName, expectedSparkInfo, tableInfo);

    checkTableReadWrite(tableInfo);
  }

  @Test
  @EnabledIf("supportsPartition")
  void testCreateDatasourceFormatPartitionTable() {
    String tableName = "datasource_partition_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + " USING PARQUET PARTITIONED BY (name, age)";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withIdentifyPartition(Arrays.asList("name", "age"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
    checkPartitionDirExists(tableInfo);
  }

  @Test
  @EnabledIf("supportsSparkSQLClusteredBy")
  void testCreateBucketTable() {
    String tableName = "bucket_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL = createTableSQL + "CLUSTERED BY (id, name) INTO 4 buckets;";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withBucket(4, Arrays.asList("id", "name"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  @Test
  @EnabledIf("supportsSparkSQLClusteredBy")
  void testCreateSortBucketTable() {
    String tableName = "sort_bucket_table";

    dropTableIfExists(tableName);
    String createTableSQL = getCreateSimpleTableString(tableName);
    createTableSQL =
        createTableSQL + "CLUSTERED BY (id, name) SORTED BY (name, id) INTO 4 buckets;";
    sql(createTableSQL);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withColumns(getSimpleTableColumn())
            .withBucket(4, Arrays.asList("id", "name"), Arrays.asList("name", "id"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  // Spark CTAS doesn't copy table properties and partition schema from source table.
  @Test
  void testCreateTableAsSelect() {
    String tableName = "ctas_table";
    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    checkTableReadWrite(tableInfo);

    String newTableName = "new_" + tableName;
    dropTableIfExists(newTableName);
    createTableAsSelect(tableName, newTableName);

    SparkTableInfo newTableInfo = getTableInfo(newTableName);
    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create().withName(newTableName).withColumns(getSimpleTableColumn());
    checker.check(newTableInfo);

    List<String> tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(getExpectedTableData(newTableInfo), tableData.get(0));
  }

  @Test
  void testInsertTableAsSelect() {
    String tableName = "insert_select_table";
    String newTableName = "new_" + tableName;

    dropTableIfExists(tableName);
    createSimpleTable(tableName);
    SparkTableInfo tableInfo = getTableInfo(tableName);
    checkTableReadWrite(tableInfo);

    dropTableIfExists(newTableName);
    createSimpleTable(newTableName);
    insertTableAsSelect(tableName, newTableName);

    SparkTableInfo newTableInfo = getTableInfo(newTableName);
    String expectedTableData = getExpectedTableData(newTableInfo);
    List<String> tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(expectedTableData, tableData.get(0));
  }

  @Test
  @EnabledIf("supportsPartition")
  void testInsertDatasourceFormatPartitionTableAsSelect() {
    String tableName = "insert_select_partition_table";
    String newTableName = "new_" + tableName;
    dropTableIfExists(tableName);
    dropTableIfExists(newTableName);

    createSimpleTable(tableName);
    String createTableSql = getCreateSimpleTableString(newTableName);
    createTableSql += "PARTITIONED BY (name, age)";
    sql(createTableSql);

    SparkTableInfo tableInfo = getTableInfo(tableName);
    checkTableReadWrite(tableInfo);

    insertTableAsSelect(tableName, newTableName);

    SparkTableInfo newTableInfo = getTableInfo(newTableName);
    checkPartitionDirExists(newTableInfo);
    String expectedTableData = getExpectedTableData(newTableInfo);
    List<String> tableData = getTableData(newTableName);
    Assertions.assertTrue(tableData.size() == 1);
    Assertions.assertEquals(expectedTableData, tableData.get(0));
  }

  protected void checkPartitionDirExists(SparkTableInfo table) {
    Assertions.assertTrue(table.isPartitionTable(), "Not a partition table");
    String tableLocation = table.getTableLocation();
    String partitionExpression = getPartitionExpression(table, "/").replace("'", "");
    Path partitionPath = new Path(tableLocation, partitionExpression);
    checkDirExists(partitionPath);
  }

  protected void checkDirExists(Path dir) {
    try {
      Assertions.assertTrue(hdfs.exists(dir), "HDFS directory not exists," + dir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void checkDataFileExists(Path dir) {
    Boolean isExists = false;
    try {
      for (FileStatus fileStatus : hdfs.listStatus(dir)) {
        if (fileStatus.isFile()) {
          isExists = true;
          break;
        }
      }
      Assertions.assertTrue(isExists);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void deleteDirIfExists(String path) {
    try {
      Path dir = new Path(path);
      if (hdfs.exists(dir)) {
        hdfs.delete(dir, true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testTableOptions() {
    String tableName = "options_table";
    dropTableIfExists(tableName);
    String createTableSql = getCreateSimpleTableString(tableName);
    createTableSql += " OPTIONS('a'='b')";
    sql(createTableSql);
    SparkTableInfo tableInfo = getTableInfo(tableName);

    SparkTableInfoChecker checker =
        SparkTableInfoChecker.create()
            .withName(tableName)
            .withTableProperties(ImmutableMap.of(TableCatalog.OPTION_PREFIX + "a", "b"));
    checker.check(tableInfo);
    checkTableReadWrite(tableInfo);
  }

  protected void checkTableReadWrite(SparkTableInfo table) {
    String name = table.getTableIdentifier();
    boolean isPartitionTable = table.isPartitionTable();
    String insertValues =
        table.getUnPartitionedColumns().stream()
            .map(columnInfo -> typeConstant.get(columnInfo.getType()))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    String insertDataSQL = "";
    if (isPartitionTable) {
      String partitionExpressions = getPartitionExpression(table, ",");
      insertDataSQL = getInsertWithPartitionSql(name, partitionExpressions, insertValues);
    } else {
      insertDataSQL = getInsertWithoutPartitionSql(name, insertValues);
    }
    sql(insertDataSQL);

    String checkValues = getExpectedTableData(table);

    List<String> queryResult = getTableData(name);
    Assertions.assertTrue(
        queryResult.size() == 1, "Should just one row, table content: " + queryResult);
    Assertions.assertEquals(checkValues, queryResult.get(0));
  }

  protected String getExpectedTableData(SparkTableInfo table) {
    // Do something to match the query result:
    // 1. remove "'" from values, such as 'a' is trans to a
    // 2. remove "array" from values, such as array(1, 2, 3) is trans to [1, 2, 3]
    // 3. remove "map" from values, such as map('a', 1, 'b', 2) is trans to {a=1, b=2}
    // 4. remove "struct" from values, such as struct(1, 'a') is trans to 1,a
    return table.getColumns().stream()
        .map(columnInfo -> typeConstant.get(columnInfo.getType()))
        .map(Object::toString)
        .map(
            s -> {
              String tmp = org.apache.commons.lang3.StringUtils.remove(s, "'");
              if (org.apache.commons.lang3.StringUtils.isEmpty(tmp)) {
                return tmp;
              } else if (tmp.startsWith("array")) {
                return tmp.replace("array", "").replace("(", "[").replace(")", "]");
              } else if (tmp.startsWith("map")) {
                return tmp.replace("map", "")
                    .replace("(", "{")
                    .replace(")", "}")
                    .replace(", ", "=");
              } else if (tmp.startsWith("struct")) {
                return tmp.replace("struct", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace(", ", ",");
              }
              return tmp;
            })
        .collect(Collectors.joining(","));
  }

  protected String getCreateSimpleTableString(String tableName) {
    return getCreateSimpleTableString(tableName, false);
  }

  protected String getCreateSimpleTableString(String tableName, boolean isExternal) {
    String external = "";
    if (isExternal) {
      external = "EXTERNAL";
    }
    return String.format(
        "CREATE %s TABLE %s (id INT COMMENT 'id comment', name STRING COMMENT '', age INT)",
        external, tableName);
  }

  protected List<SparkColumnInfo> getSimpleTableColumn() {
    return Arrays.asList(
        SparkColumnInfo.of("id", DataTypes.IntegerType, "id comment"),
        SparkColumnInfo.of("name", DataTypes.StringType, ""),
        SparkColumnInfo.of("age", DataTypes.IntegerType, null));
  }

  protected String getDefaultDatabase() {
    return "default_db";
  }

  // Helper method to create a simple table, and could use corresponding
  // getSimpleTableColumn to check table column.
  protected void createSimpleTable(String identifier) {
    String createTableSql = getCreateSimpleTableString(identifier);
    sql(createTableSql);
  }

  protected void checkTableColumns(
      String tableName, List<SparkColumnInfo> columns, SparkTableInfo tableInfo) {
    SparkTableInfoChecker.create()
        .withName(tableName)
        .withColumns(columns)
        .withComment(null)
        .check(tableInfo);
  }

  // partition expression may contain "'", like a='s'/b=1
  private String getPartitionExpression(SparkTableInfo table, String delimiter) {
    return table.getPartitionedColumns().stream()
        .map(column -> column.getName() + "=" + typeConstant.get(column.getType()))
        .collect(Collectors.joining(delimiter));
  }

  protected void checkParquetFile(SparkTableInfo tableInfo) {
    String location = tableInfo.getTableLocation();
    Assertions.assertDoesNotThrow(() -> getSparkSession().read().parquet(location).printSchema());
  }
}
