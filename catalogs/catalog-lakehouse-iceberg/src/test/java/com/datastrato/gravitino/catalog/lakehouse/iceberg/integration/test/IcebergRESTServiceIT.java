/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

@SuppressWarnings("FormatStringAnnotation")
@TestInstance(Lifecycle.PER_CLASS)
public class IcebergRESTServiceIT extends IcebergRESTServiceBaseIT {

  private static final String ICEBERG_REST_NS_PREFIX = "iceberg_rest_";

  @BeforeAll
  void prepareSQLContext() {
    // use rest catalog
    sql("USE rest");
    purgeAllIcebergTestNamespaces();
    sql("CREATE DATABASE IF NOT EXISTS iceberg_rest_table_test");
  }

  @AfterAll
  void cleanup() {
    purgeAllIcebergTestNamespaces();
  }

  @Override
  void initEnv() {}

  @Override
  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_BACKEND.getKey(),
        IcebergCatalogBackend.MEMORY.toString().toLowerCase());

    configMap.put(
        AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX
            + IcebergRESTService.SERVICE_NAME
            + "."
            + IcebergConfig.CATALOG_WAREHOUSE.getKey(),
        "/tmp/");
    return configMap;
  }

  private void purgeTable(String namespace, String table) {
    sql(String.format("DROP TABLE %s.%s PURGE", namespace, table));
  }

  private void purgeNameSpace(String namespace) {
    Set<String> tables = convertToStringSet(sql("SHOW TABLES IN " + namespace), 1);
    tables.forEach(table -> purgeTable(namespace, table));
    sql("DROP database " + namespace);
  }

  private void purgeAllIcebergTestNamespaces() {
    List<Object[]> databases =
        sql(String.format("SHOW DATABASES like '%s*'", ICEBERG_REST_NS_PREFIX));
    Set<String> databasesString = convertToStringSet(databases, 0);
    databasesString.stream()
        .filter(ns -> ns.startsWith(ICEBERG_REST_NS_PREFIX))
        .forEach(ns -> purgeNameSpace(ns));
  }

  @Test
  void testCreateNamespace() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "create";
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'This is customer database' "
                + "WITH DBPROPERTIES (ID=001, Name='John')",
            namespaceName));
    Map<String, String> databaseInfo =
        convertToStringMap(sql("DESCRIBE DATABASE EXTENDED " + namespaceName));
    Assertions.assertEquals("This is customer database", databaseInfo.get("Comment"));
    Assertions.assertEquals(namespaceName, databaseInfo.get("Namespace Name"));
    String properties = databaseInfo.getOrDefault("Properties", "");
    switch (catalogType) {
      case HIVE:
        //  hive add more properties, like:
        //  ((hive.metastore.database.owner,hive), (hive.metastore.database.owner-type,USER))
        Assertions.assertTrue(properties.contains("(ID,001), (Name,John)"));
        break;
      default:
        Assertions.assertEquals("((ID,001), (Name,John))", properties);
        break;
    }

    Assertions.assertThrowsExactly(
        NamespaceAlreadyExistsException.class, () -> sql("CREATE DATABASE " + namespaceName));
  }

  @Test
  void testListNamespace() {
    sql(String.format("CREATE DATABASE %slist_foo1", ICEBERG_REST_NS_PREFIX));
    sql(String.format("CREATE DATABASE %slist_foo2", ICEBERG_REST_NS_PREFIX));
    List<Object[]> databases =
        sql(String.format("SHOW DATABASES like '%slist_foo*'", ICEBERG_REST_NS_PREFIX));
    Set<String> databasesString = convertToStringSet(databases, 0);
    Assertions.assertEquals(
        ImmutableSet.of(ICEBERG_REST_NS_PREFIX + "list_foo1", ICEBERG_REST_NS_PREFIX + "list_foo2"),
        databasesString);
  }

  @Test
  void testDropNameSpace() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "foo1";
    sql("CREATE DATABASE IF NOT EXISTS " + namespaceName);
    sql("DESC DATABASE " + namespaceName);
    sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.test "
                + "(id bigint COMMENT 'unique id',data string) using iceberg",
            namespaceName));

    // seems a bug in Iceberg REST client, should be NamespaceNotEmptyException
    Assertions.assertThrowsExactly(
        BadRequestException.class, () -> sql("DROP DATABASE " + namespaceName));
    sql(String.format("DROP TABLE %s.test", namespaceName));
    sql("DROP DATABASE " + namespaceName);

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DESC DATABASE " + namespaceName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE " + namespaceName));
  }

  @Test
  void testNameSpaceProperties() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "alter_foo1";
    sql("DROP DATABASE if exists " + namespaceName);
    sql("CREATE DATABASE if not exists " + namespaceName);
    sql(String.format("ALTER DATABASE %s SET PROPERTIES(id = 2)", namespaceName));
    List<Object[]> datas = sql("DESC DATABASE EXTENDED " + namespaceName);
    Map<String, String> m = convertToStringMap(datas);
    String properties = m.getOrDefault("Properties", "");
    switch (catalogType) {
      case MEMORY:
        Assertions.assertEquals("((id,2))", properties);
        break;
      default:
        // ((hive.metastore.database.owner,hive), (hive.metastore.database.owner-type,USER), (id,2))
        Assertions.assertTrue(properties.contains("(id,2)"));
    }
  }

  @Test
  void testDML() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "dml";
    String tableName = namespaceName + ".test";
    sql("CREATE DATABASE IF NOT EXISTS " + namespaceName);
    sql(
        String.format(
            "CREATE TABLE %s (id bigint COMMENT 'unique id',data string, ts timestamp) USING iceberg "
                + "PARTITIONED BY (bucket(2, id), days(ts))",
            tableName));
    sql(
        String.format(
            " INSERT INTO %s VALUES (1, 'a', cast('2023-10-01 01:00:00' as timestamp));",
            tableName));
    sql(
        String.format(
            " INSERT INTO %s VALUES (2, 'b', cast('2023-10-02 01:00:00' as timestamp));",
            tableName));
    sql(
        String.format(
            " INSERT INTO %s VALUES (3, 'c', cast('2023-10-03 01:00:00' as timestamp));",
            tableName));
    sql(
        String.format(
            " INSERT INTO %s VALUES (4, 'd', cast('2023-10-04 01:00:00' as timestamp));",
            tableName));
    Map<String, String> m =
        convertToStringMap(sql("SELECT * FROM " + tableName + " WHERE ts > '2023-10-03 00:00:00'"));
    Assertions.assertEquals(m, ImmutableMap.of("3", "c", "4", "d"));
  }

  @Test
  void testCreateTable() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.create_foo1"
            + "( id bigint, data string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (bucket(16, id), days(ts))");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.create_foo1");
    Map<String, String> m =
        ImmutableMap.of(
            "id", "bigint",
            "data", "string",
            "ts", "timestamp",
            "Part 0", "bucket(16, id)",
            "Part 1", "days(ts)");

    checkMapContains(m, tableInfo);

    Assertions.assertThrowsExactly(
        TableAlreadyExistsException.class,
        () -> sql("CREATE TABLE iceberg_rest_table_test.create_foo1"));
  }

  @Test
  void testDropTable() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.drop_foo1"
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql("DROP TABLE iceberg_rest_table_test.drop_foo1");
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("DESC TABLE iceberg_rest_table_test.drop_foo1"));

    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> sql("DROP TABLE iceberg_rest_table_test.drop_foo1"));
  }

  @Test
  void testListTable() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "list_db";
    sql("CREATE DATABASE if not exists " + namespaceName);
    sql(
        String.format(
            "CREATE TABLE %s.list_foo1(id bigint COMMENT 'unique id',data string) using iceberg",
            namespaceName));
    sql(
        String.format(
            "CREATE TABLE %s.list_foo2(id bigint COMMENT 'unique id',data string) using iceberg",
            namespaceName));

    Set<String> tables = convertToStringSet(sql("show tables in " + namespaceName), 1);
    Assertions.assertEquals(ImmutableSet.of("list_foo1", "list_foo2"), tables);
  }

  @Test
  void testRenameTable() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.rename_foo1"
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        "ALTER TABLE iceberg_rest_table_test.rename_foo1 "
            + "RENAME TO iceberg_rest_table_test.rename_foo2");
    sql("desc table iceberg_rest_table_test.rename_foo2");
    Assertions.assertThrowsExactly(
        AnalysisException.class, () -> sql("desc table iceberg_rest_table_test.rename_foo1"));

    sql(
        "CREATE TABLE iceberg_rest_table_test.rename_foo1"
            + "(id bigint COMMENT 'unique id',data string) using iceberg");

    Class exception =
        catalogType == IcebergCatalogBackend.HIVE
            ? ServiceFailureException.class
            : TableAlreadyExistsException.class;

    Assertions.assertThrowsExactly(
        exception,
        () ->
            sql(
                "ALTER TABLE iceberg_rest_table_test.rename_foo2 "
                    + "RENAME TO iceberg_rest_table_test.rename_foo1"));
  }

  @Test
  void testSetTableProperties() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.set_foo1"
            + " (id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        "ALTER TABLE iceberg_rest_table_test.set_foo1 SET TBLPROPERTIES "
            + "('read.split.target-size'='268435456')");
    Map<String, String> m = getTableInfo("iceberg_rest_table_test.set_foo1");
    Assertions.assertTrue(
        m.getOrDefault("Table Properties", "").contains("read.split.target-size=268435456"));

    sql(
        "ALTER TABLE iceberg_rest_table_test.set_foo1 "
            + "UNSET TBLPROPERTIES ('read.split.target-size')");
    m = getTableInfo("iceberg_rest_table_test.set_foo1");
    Assertions.assertFalse(
        m.getOrDefault("Table Properties", "read.split.target-size")
            .contains("read.split.target-size"));

    sql("ALTER TABLE iceberg_rest_table_test.set_foo1 SET TBLPROPERTIES ('comment'='a')");
    m = getTableInfo("iceberg_rest_table_test.set_foo1");
    // comment is hidden
    Assertions.assertFalse(m.getOrDefault("Table Properties", "").contains("comment=a"));
  }

  @Test
  void testAddColumns() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.add_foo1"
            + " (id string COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () ->
            sql(
                "ALTER TABLE iceberg_rest_table_test.add_foo1 "
                    + "ADD COLUMNS foo_after String After not_exits"));

    sql("ALTER TABLE iceberg_rest_table_test.add_foo1 ADD COLUMNS foo_after String After id");
    List<String> columns = getTableColumns("iceberg_rest_table_test.add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data"), columns);

    sql("ALTER TABLE iceberg_rest_table_test.add_foo1 ADD COLUMNS foo_last String");
    columns = getTableColumns("iceberg_rest_table_test.add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data", "foo_last"), columns);

    sql("ALTER TABLE iceberg_rest_table_test.add_foo1 ADD COLUMNS foo_first String FIRST");
    columns = getTableColumns("iceberg_rest_table_test.add_foo1");
    Assertions.assertEquals(
        Arrays.asList("foo_first", "id", "foo_after", "data", "foo_last"), columns);
  }

  @Test
  void testRenameColumns() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.renameC_foo1"
            + " (id bigint COMMENT 'unique id',data string) using iceberg");
    sql("ALTER TABLE iceberg_rest_table_test.renameC_foo1 RENAME COLUMN data TO data1");

    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.renameC_foo1");
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
        "CREATE TABLE iceberg_rest_table_test.dropC_foo1 "
            + "(id bigint COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () -> sql("ALTER TABLE iceberg_rest_table_test.dropC_foo1 DROP COLUMNS not_exits"));

    sql("ALTER TABLE iceberg_rest_table_test.dropC_foo1 DROP COLUMNS data");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.dropC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
    Assertions.assertFalse(m.containsKey("data"));
  }

  @Test
  void testUpdateColumnType() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.updateC_foo1 "
            + "(id int COMMENT 'unique id',data string) using iceberg");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.updateC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "int");
    checkMapContains(m, tableInfo);

    sql("ALTER TABLE iceberg_rest_table_test.updateC_foo1 ALTER COLUMN id TYPE bigint");
    tableInfo = getTableInfo("iceberg_rest_table_test.updateC_foo1");
    m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
  }

  @Test
  void testUpdateColumnPosition() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.updateP_foo1 "
            + "(id string COMMENT 'unique id',data string) using iceberg");
    List<String> columns = getTableColumns("iceberg_rest_table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);

    sql("ALTER TABLE iceberg_rest_table_test.updateP_foo1 ALTER COLUMN id AFTER data");
    columns = getTableColumns("iceberg_rest_table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("data", "id"), columns);

    sql("ALTER TABLE iceberg_rest_table_test.updateP_foo1 ALTER COLUMN id FIRST");
    columns = getTableColumns("iceberg_rest_table_test.updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);
  }

  @Test
  void testAlterPartitions() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.part_foo1"
            + "( id bigint, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE iceberg_rest_table_test.part_foo1 ADD PARTITION FIELD bucket(16, id)");
    sql("ALTER TABLE iceberg_rest_table_test.part_foo1 ADD PARTITION FIELD truncate(4, data)");
    sql("ALTER TABLE iceberg_rest_table_test.part_foo1 ADD PARTITION FIELD years(ts)");

    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.part_foo1");
    Map<String, String> partitions =
        ImmutableMap.of(
            "Part 0", "bucket(16, id)",
            "Part 1", "truncate(4, data)",
            "Part 2", "years(ts)");
    checkMapContains(partitions, tableInfo);
    Assertions.assertFalse(tableInfo.containsKey("Part 3"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            sql(
                "ALTER TABLE iceberg_rest_table_test.part_foo1 "
                    + "DROP PARTITION FIELD bucket(8, id)"));
    sql("ALTER TABLE iceberg_rest_table_test.part_foo1 DROP PARTITION FIELD bucket(16, id)");
    tableInfo = getTableInfo("iceberg_rest_table_test.part_foo1");
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
                "ALTER TABLE iceberg_rest_table_test.part_foo1 "
                    + "REPLACE PARTITION FIELD months(ts) WITH days(ts)"));
    sql(
        "ALTER TABLE iceberg_rest_table_test.part_foo1 "
            + "REPLACE PARTITION FIELD years(ts) WITH days(ts)");
    tableInfo = getTableInfo("iceberg_rest_table_test.part_foo1");
    partitions =
        ImmutableMap.of(
            "Part 0", "truncate(4, data)",
            "Part 1", "days(ts)");
    checkMapContains(partitions, tableInfo);
    Assertions.assertFalse(tableInfo.containsKey("Part 3"));
  }

  @Test
  void testAlterSortBy() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.sort_foo1"
            + "( id bigint, data string, ts timestamp) USING iceberg");
    Assertions.assertThrowsExactly(
        ValidationException.class,
        () -> sql("ALTER TABLE iceberg_rest_table_test.sort_foo1 WRITE ORDERED BY xx, id"));
    sql(
        "ALTER TABLE iceberg_rest_table_test.sort_foo1 "
            + "WRITE ORDERED BY data ASC NULLS FIRST, id ASC NULLS FIRST");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.sort_foo1");
    Assertions.assertTrue(
        tableInfo
            .get("Table Properties")
            .contains("sort-order=data ASC NULLS FIRST, id ASC NULLS FIRST,"));

    // replace with new one
    sql("ALTER TABLE iceberg_rest_table_test.sort_foo1 WRITE ORDERED BY ts ASC NULLS FIRST");
    tableInfo = getTableInfo("iceberg_rest_table_test.sort_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("sort-order=ts ASC NULLS FIRST,"));
  }

  @Test
  void testAlterPartitionBy() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.partby_foo1"
            + "( id bigint, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE iceberg_rest_table_test.partby_foo1 WRITE DISTRIBUTED BY PARTITION");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.partby_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("write.distribution-mode=hash"));
  }

  @Test
  void testAlterIdentifier() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.identifier_foo1"
            + "( id bigint NOT NULL, data string, ts timestamp) USING iceberg");
    sql("ALTER TABLE iceberg_rest_table_test.identifier_foo1 SET IDENTIFIER FIELDS id");
    Map<String, String> tableInfo = getTableInfo("iceberg_rest_table_test.identifier_foo1");
    Assertions.assertTrue(tableInfo.get("Table Properties").contains("identifier-fields=[id]"));

    sql("ALTER TABLE iceberg_rest_table_test.identifier_foo1 DROP IDENTIFIER FIELDS id");
    tableInfo = getTableInfo("iceberg_rest_table_test.identifier_foo1");
    Assertions.assertFalse(tableInfo.get("Table Properties").contains("identifier-fields"));

    // java.lang.IllegalArgumentException: Cannot add field id as an identifier field: not a
    // required field
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            sql("ALTER TABLE iceberg_rest_table_test.identifier_foo1 SET IDENTIFIER FIELDS data"));
  }

  @Test
  // MemoryCatalog doesn't support snapshot operations, error is:
  // org.apache.iceberg.exceptions.NotFoundException: File does not exist:
  // /tmp/iceberg_rest_table_test/snapshot_foo1/metadata/00002-c7516f8e-ef6b-406a-8d78-9dda825dd762.metadata.json
  // sql("SELECT * FROM table_test.snapshot_foo1.snapshots");
  @EnabledIf("catalogTypeNotMemory")
  void testSnapshot() {
    sql(
        "CREATE TABLE iceberg_rest_table_test.snapshot_foo1 "
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql(" INSERT INTO iceberg_rest_table_test.snapshot_foo1 VALUES (1, 'a'), (2, 'b');");
    sql(" INSERT INTO iceberg_rest_table_test.snapshot_foo1 VALUES (3, 'c'), (4, 'd');");
    List<String> snapshots =
        convertToStringList(
            sql("SELECT * FROM iceberg_rest_table_test.snapshot_foo1.snapshots"), 1);

    Assertions.assertEquals(2, snapshots.size());
    String oldSnapshotId = snapshots.get(0);
    sql(
        String.format(
            "CALL rest.system.rollback_to_snapshot('iceberg_rest_table_test.snapshot_foo1', %s)",
            oldSnapshotId));
    Map<String, String> result =
        convertToStringMap(sql("select * from iceberg_rest_table_test.snapshot_foo1"));
    Assertions.assertEquals(ImmutableMap.of("1", "a", "2", "b"), result);
  }
}
