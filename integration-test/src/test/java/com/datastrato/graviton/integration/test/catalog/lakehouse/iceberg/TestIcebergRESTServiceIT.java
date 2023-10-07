/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.integration.test.catalog.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergRESTServiceIT extends TestIcebergRESTServiceBaseIT {

  @BeforeEach
  void initEnv() {
    // use rest catalog
    sql("USE rest");
    sql("CREATE DATABASE IF NOT EXISTS table_test");
  }

  @Test
  void testCreateNamespace() {
    sql(
        "CREATE DATABASE db_create COMMENT 'This is customer database' WITH DBPROPERTIES (ID=001, Name='John')");
    Map<String, String> databaseInfo =
        convertToStringMap(sql("DESCRIBE DATABASE EXTENDED db_create"));
    Assertions.assertEquals("This is customer database", databaseInfo.get("Comment"));
    Assertions.assertEquals("db_create", databaseInfo.get("Namespace Name"));
    Assertions.assertEquals("((ID,001), (Name,John))", databaseInfo.get("Properties"));

    Assertions.assertThrowsExactly(
        NamespaceAlreadyExistsException.class, () -> sql("CREATE DATABASE db_create"));
  }

  @Test
  void testListNamespace() {
    sql("CREATE DATABASE list_foo1");
    sql("CREATE DATABASE list_foo2");
    List<Object[]> databases = sql("SHOW DATABASES like 'list_foo*'");
    Set<String> databasesString = convertToStringSet(databases, 0);
    Assertions.assertEquals(ImmutableSet.of("list_foo1", "list_foo2"), databasesString);
  }

  @Test
  void testDropNameSpace() {
    sql("CREATE DATABASE IF NOT EXISTS drop_foo1");
    sql("DESC DATABASE drop_foo1");
    sql(
        "CREATE TABLE IF NOT EXISTS drop_foo1.test (id bigint COMMENT 'unique id',data string) using iceberg");

    // seems a bug in Iceberg REST client, should be NamespaceNotEmptyException
    Assertions.assertThrowsExactly(BadRequestException.class, () -> sql("DROP DATABASE drop_foo1"));
    sql("DROP TABLE drop_foo1.test");
    sql("DROP DATABASE drop_foo1");

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DESC DATABASE drop_foo1"));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE drop_foo1"));
  }

  @Test
  void testNameSpaceProperties() {
    sql("DROP DATABASE if exists alter_foo1");
    sql("CREATE DATABASE if not exists alter_foo1");
    sql("ALTER DATABASE alter_foo1 SET PROPERTIES(id = 2)");
    List<Object[]> datas = sql("DESC DATABASE EXTENDED alter_foo1");
    Map<String, String> m = convertToStringMap(datas);
    Assertions.assertEquals("((id,2))", m.getOrDefault("Properties", ""));
  }

  @Test
  void testDML() {
    sql("CREATE DATABASE IF NOT EXISTS dml");
    sql("DROP TABLE IF EXISTS dml.test");
    sql("CREATE TABLE dml.test (id bigint COMMENT 'unique id',data string) using iceberg");
    sql(" INSERT INTO dml.test VALUES (1, 'a'), (2, 'b');");
    sql(" INSERT INTO dml.test VALUES (3, 'c'), (4, 'd');");
    Map<String, String> m = convertToStringMap(sql("SELECT * FROM dml.test"));
    Assertions.assertEquals(m, ImmutableMap.of("1", "a", "2", "b", "3", "c", "4", "d"));
  }

  @Test
  void testCreateTable() {
    sql(
        "CREATE TABLE table_test.create_foo1( id bigint, data string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (bucket(16, id), days(ts))");
    Map<String, String> tableInfo = getTableInfo("table_test.create_foo1");
    Map<String, String> m =
        ImmutableMap.of(
            "id", "bigint",
            "data", "string",
            "ts", "timestamp",
            "Part 0", "bucket(16, id)",
            "Part 1", "days(ts)");

    checkMapContains(m, tableInfo);

    Assertions.assertThrowsExactly(
        TableAlreadyExistsException.class, () -> sql("CREATE TABLE table_test.create_foo1"));
  }

  @Test
  void testDropTable() {
    sql(
        "CREATE TABLE table_test.drop_foo1(id bigint COMMENT 'unique id',data string) using iceberg");
    sql("DROP TABLE table_test.drop_foo1");
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("DESC TABLE table_test.drop_foo1"));

    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> sql("DROP TABLE table_test.drop_foo1"));
  }

  @Test
  void testListTable() {
    sql("CREATE DATABASE if not exists list_db");
    sql("CREATE TABLE list_db.list_foo1(id bigint COMMENT 'unique id',data string) using iceberg");
    sql("CREATE TABLE list_db.list_foo2(id bigint COMMENT 'unique id',data string) using iceberg");

    Set<String> tables = convertToStringSet(sql("show tables in list_db"), 1);
    Assertions.assertEquals(ImmutableSet.of("list_foo1", "list_foo2"), tables);
  }

  @Test
  void testRenameTable() {
    sql(
        "CREATE TABLE table_test.rename_foo1(id bigint COMMENT 'unique id',data string) using iceberg");
    sql("ALTER TABLE table_test.rename_foo1 RENAME TO table_test.rename_foo2");
    sql("desc table table_test.rename_foo2");
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("desc table table_test.rename_foo1"));

    sql(
        "CREATE TABLE table_test.rename_foo1(id bigint COMMENT 'unique id',data string) using iceberg");
    Assertions.assertThrowsExactly(
        TableAlreadyExistsException.class,
        () -> sql("ALTER TABLE table_test.rename_foo2 RENAME TO table_test.rename_foo1"));
  }

  @Test
  void testSetTableProperties() {
    sql(
        "CREATE TABLE table_test.set_foo1 (id bigint COMMENT 'unique id',data string) using iceberg");
    sql("ALTER TABLE table_test.set_foo1 SET TBLPROPERTIES ('read.split.target-size'='268435456')");
    Map<String, String> m = getTableInfo("table_test.set_foo1");
    Assertions.assertTrue(
        m.getOrDefault("Table Properties", "").contains("read.split.target-size=268435456"));

    sql("ALTER TABLE table_test.set_foo1 UNSET TBLPROPERTIES ('read.split.target-size')");
    m = getTableInfo("table_test.set_foo1");
    Assertions.assertFalse(
        m.getOrDefault("Table Properties", "read.split.target-size")
            .contains("read.split.target-size"));

    sql("ALTER TABLE table_test.set_foo1 SET TBLPROPERTIES ('comment'='a')");
    m = getTableInfo("table_test.set_foo1");
    // comment is hidden
    Assertions.assertFalse(m.getOrDefault("Table Properties", "").contains("comment=a"));
  }

  @Test
  void testAddColumns() {
    sql(
        "CREATE TABLE table_test.add_foo1 (id bigint COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () -> sql("ALTER TABLE table_test.add_foo1 ADD COLUMNS foo_after String After not_exits"));

    sql("ALTER TABLE table_test.add_foo1 ADD COLUMNS foo_after String After id");
    List<String> columns = getTableColumns("table_test.add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data"), columns);

    sql("ALTER TABLE table_test.add_foo1 ADD COLUMNS foo_last String");
    columns = getTableColumns("table_test.add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data", "foo_last"), columns);

    sql("ALTER TABLE table_test.add_foo1 ADD COLUMNS foo_first String FIRST");
    columns = getTableColumns("table_test.add_foo1");
    Assertions.assertEquals(
        Arrays.asList("foo_first", "id", "foo_after", "data", "foo_last"), columns);
  }

  @Test
  void testRenameColumns() {
    sql(
        "CREATE TABLE table_test.renameC_foo1 (id bigint COMMENT 'unique id',data string) using iceberg");
    sql("ALTER TABLE table_test.renameC_foo1 RENAME COLUMN data TO data1");

    Map<String, String> tableInfo = getTableInfo("table_test.renameC_foo1");
    Map<String, String> m =
        ImmutableMap.of(
            "id", "bigint",
            "data1", "string");
    checkMapContains(m, tableInfo);
    Assertions.assertFalse(m.containsKey("data"));
  }

  @Test
  void testDropColumns() {
    sql(
        "CREATE TABLE table_test.dropC_foo1 (id bigint COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () -> sql("ALTER TABLE table_test.dropC_foo1 DROP COLUMNS not_exits"));

    sql("ALTER TABLE table_test.dropC_foo1 DROP COLUMNS data");
    Map<String, String> tableInfo = getTableInfo("table_test.dropC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
    Assertions.assertFalse(m.containsKey("data"));
  }

  @Test
  void testUpdateColumnType() {
    sql(
        "CREATE TABLE table_test.updateC_foo1 (id int COMMENT 'unique id',data string) using iceberg");
    Map<String, String> tableInfo = getTableInfo("table_test.updateC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "int");
    checkMapContains(m, tableInfo);

    sql("ALTER TABLE table_test.updateC_foo1 ALTER COLUMN id TYPE bigint");
    tableInfo = getTableInfo("table_test.updateC_foo1");
    m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
  }

  @Test
  void testUpdateColumnPosition() {
    sql(
        "CREATE TABLE table_test.updateP_foo1 (id int COMMENT 'unique id',data string) using iceberg");
    List<String> columns = getTableColumns("table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);

    sql("ALTER TABLE table_test.updateP_foo1 ALTER COLUMN id AFTER data");
    columns = getTableColumns("table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("data", "id"), columns);

    sql("ALTER TABLE table_test.updateP_foo1 ALTER COLUMN id FIRST");
    columns = getTableColumns("table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);
  }

  @Test
  void testAlterPartitions() {
    sql("CREATE TABLE table_test.part_foo1( id bigint, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE table_test.part_foo1 ADD PARTITION FIELD bucket(16, id)");
    sql("ALTER TABLE table_test.part_foo1 ADD PARTITION FIELD truncate(4, data)");
    sql("ALTER TABLE table_test.part_foo1 ADD PARTITION FIELD years(ts)");

    Map<String, String> tableInfo = getTableInfo("table_test.part_foo1");
    Map<String, String> partitions =
        ImmutableMap.of(
            "Part 0", "bucket(16, id)",
            "Part 1", "truncate(4, data)",
            "Part 2", "years(ts)");
    checkMapContains(partitions, tableInfo);
    Assertions.assertFalse(tableInfo.containsKey("Part 3"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> sql("ALTER TABLE table_test.part_foo1 DROP PARTITION FIELD bucket(8, id)"));
    sql("ALTER TABLE table_test.part_foo1 DROP PARTITION FIELD bucket(16, id)");
    tableInfo = getTableInfo("table_test.part_foo1");
    partitions =
        ImmutableMap.of(
            "Part 0", "truncate(4, data)",
            "Part 1", "years(ts)");
    checkMapContains(partitions, tableInfo);
    Assertions.assertFalse(tableInfo.containsKey("Part 2"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            sql(
                "ALTER TABLE table_test.part_foo1 REPLACE PARTITION FIELD months(ts) WITH days(ts)"));
    sql("ALTER TABLE table_test.part_foo1 REPLACE PARTITION FIELD years(ts) WITH days(ts)");
    tableInfo = getTableInfo("table_test.part_foo1");
    partitions =
        ImmutableMap.of(
            "Part 0", "truncate(4, data)",
            "Part 1", "days(ts)");
    checkMapContains(partitions, tableInfo);
    Assertions.assertFalse(tableInfo.containsKey("Part 3"));
  }

  @Test
  void testAlterSortBy() {
    sql("CREATE TABLE table_test.sort_foo1( id bigint, data string, ts timestamp) USING iceberg");
    Assertions.assertThrowsExactly(
        ValidationException.class,
        () -> sql("ALTER TABLE table_test.sort_foo1 WRITE ORDERED BY xx, id"));
    sql(
        "ALTER TABLE table_test.sort_foo1 WRITE ORDERED BY data ASC NULLS FIRST, id ASC NULLS FIRST");
    Map<String, String> tableInfo = getTableInfo("table_test.sort_foo1");
    Assertions.assertTrue(
        tableInfo
            .get("Table Properties")
            .contains("sort-order=data ASC NULLS FIRST, id ASC NULLS FIRST,"));

    // replace with new one
    sql("ALTER TABLE table_test.sort_foo1 WRITE ORDERED BY ts ASC NULLS FIRST");
    tableInfo = getTableInfo("table_test.sort_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("sort-order=ts ASC NULLS FIRST,"));
  }

  @Test
  void testAlterPartitionBy() {
    sql("CREATE TABLE table_test.partby_foo1( id bigint, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE table_test.partby_foo1 WRITE DISTRIBUTED BY PARTITION");
    Map<String, String> tableInfo = getTableInfo("table_test.partby_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("write.distribution-mode=hash"));
  }

  @Test
  void testAlterIdentifier() {
    sql(
        "CREATE TABLE table_test.identifier_foo1( id bigint NOT NULL, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE table_test.identifier_foo1 SET IDENTIFIER FIELDS id");
    Map<String, String> tableInfo = getTableInfo("table_test.identifier_foo1");
    Assertions.assertTrue(tableInfo.get("Table Properties").contains("identifier-fields=[id]"));

    sql("ALTER TABLE table_test.identifier_foo1 DROP IDENTIFIER FIELDS id");
    tableInfo = getTableInfo("table_test.identifier_foo1");
    Assertions.assertFalse(tableInfo.get("Table Properties").contains("identifier-fields"));

    // java.lang.IllegalArgumentException: Cannot add field id as an identifier field: not a
    // required field
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> sql("ALTER TABLE table_test.identifier_foo1 SET IDENTIFIER FIELDS data"));
  }

  @Test
  // todo: MemoryCatalog doesn't support snapshot operations, will be supported after hive catalog
  // is merged
  void testSnapshot() {
    sql(
        "CREATE TABLE table_test.snapshot_foo1 (id bigint COMMENT 'unique id',data string) using iceberg");
    sql(" INSERT INTO table_test.snapshot_foo1 VALUES (1, 'a'), (2, 'b');");
    sql(" INSERT INTO table_test.snapshot_foo1 VALUES (3, 'c'), (4, 'd');");
    printObjects(sql("desc table_test.snapshot_foo1"));

    // org.apache.iceberg.exceptions.NotFoundException: File does not exist:
    // /tmp/table_test/snapshot_foo1/metadata/00002-c7516f8e-ef6b-406a-8d78-9dda825dd762.metadata.json
    // printObjects(sql("SELECT * FROM table_test.snapshot_foo1.snapshots"));
  }
}
