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
package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;

// Don't add @Tag("gravitino-docker-test"), because IcebergRESTMemoryCatalogIT don't need it.
@SuppressWarnings("FormatStringAnnotation")
@TestInstance(Lifecycle.PER_CLASS)
public abstract class IcebergRESTServiceIT extends IcebergRESTServiceBaseIT {

  protected boolean supportsNestedNamespaces() {
    return true;
  }

  protected String getTestNamespace() {
    return getTestNamespace(null);
  }

  protected String getTestNamespace(@Nullable String childNamespace) {
    String separator;
    String parentNamespace;

    if (supportsNestedNamespaces()) {
      parentNamespace = "iceberg_rest.nested.table_test";
      separator = ".";
    } else {
      parentNamespace = "iceberg_rest";
      separator = "_";
    }

    if (childNamespace != null) {
      return parentNamespace + separator + childNamespace;
    } else {
      return parentNamespace;
    }
  }

  @BeforeAll
  void prepareSQLContext() {
    // use rest catalog
    sql("USE rest");
    purgeAllIcebergTestNamespaces();
    if (supportsNestedNamespaces()) {
      // create all parent namespaces
      final String[] parentNamespace = {""};
      Arrays.stream(getTestNamespace().split("\\."))
          .forEach(
              ns -> {
                sql("CREATE DATABASE IF NOT EXISTS " + parentNamespace[0] + ns);
                parentNamespace[0] += ns + ".";
              });
    }

    sql(String.format("CREATE DATABASE IF NOT EXISTS %s", getTestNamespace()));
  }

  @AfterAll
  void cleanup() {
    purgeAllIcebergTestNamespaces();
  }

  private boolean namespaceExists(String namespace) {
    try {
      sql(String.format("DESCRIBE DATABASE %s ", namespace));
      return true;
    } catch (Exception e) {
      // can't directly catch NoSuchNamespaceException because it's a checked exception,
      // and it's not thrown by sparkSession.sql
      if (e instanceof NoSuchNamespaceException) {
        // ignore non existing namespace
        return false;
      } else {
        throw e;
      }
    }
  }

  private void purgeTable(String namespace, String table) {
    sql(String.format("DROP TABLE %s.%s PURGE", namespace, table));
  }

  private void purgeNamespace(String namespace) {
    if (!namespaceExists(namespace)) {
      return;
    }

    if (supportsNestedNamespaces()) {
      // list and purge child namespaces
      List<Object[]> childNamespaces = sql(String.format("SHOW DATABASES IN %s ", namespace));
      Set<String> childNamespacesString = convertToStringSet(childNamespaces, 0);
      childNamespacesString.forEach(this::purgeNamespace);
    }

    Set<String> tables = convertToStringSet(sql("SHOW TABLES IN " + namespace), 1);
    tables.forEach(table -> purgeTable(namespace, table));
    sql("DROP database " + namespace);
  }

  private void purgeAllIcebergTestNamespaces() {
    if (supportsNestedNamespaces()) {
      purgeNamespace(getTestNamespace());
    } else {
      List<Object[]> databases =
          sql(String.format("SHOW DATABASES like '%s*'", getTestNamespace()));
      Set<String> databasesString = convertToStringSet(databases, 0);
      databasesString.stream()
          .filter(ns -> ns.startsWith(getTestNamespace() + '.'))
          .forEach(this::purgeNamespace);
    }
  }

  @Test
  void testCreateNamespace() {
    String namespaceName = getTestNamespace("create");
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
        //  Hive add more properties, like:
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
    String separator = supportsNestedNamespaces() ? "." : "_";
    sql(String.format("CREATE DATABASE %s%slist_foo1", getTestNamespace(), separator));
    sql(String.format("CREATE DATABASE %s%slist_foo2", getTestNamespace(), separator));
    List<Object[]> databases;
    if (supportsNestedNamespaces()) {
      databases = sql(String.format("SHOW DATABASES IN %s LIKE '*list_foo*'", getTestNamespace()));
    } else {
      databases =
          sql(String.format("SHOW DATABASES '%s%slist_foo*'", getTestNamespace(), separator));
    }

    Set<String> databasesString = convertToStringSet(databases, 0);
    Assertions.assertEquals(
        ImmutableSet.of(
            getTestNamespace() + separator + "list_foo1",
            getTestNamespace() + separator + "list_foo2"),
        databasesString);
  }

  @Test
  void testDropNameSpace() {
    String namespaceName = getTestNamespace("foo1");
    sql("CREATE DATABASE IF NOT EXISTS " + namespaceName);
    sql("DESC DATABASE " + namespaceName);
    sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.test "
                + "(id bigint COMMENT 'unique id',data string) using iceberg",
            namespaceName));

    Assertions.assertThrowsExactly(
        NamespaceNotEmptyException.class, () -> sql("DROP DATABASE " + namespaceName));
    sql(String.format("DROP TABLE %s.test", namespaceName));
    sql("DROP DATABASE " + namespaceName);

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DESC DATABASE " + namespaceName));

    Assertions.assertThrowsExactly(
        NoSuchNamespaceException.class, () -> sql("DROP DATABASE " + namespaceName));
  }

  @Test
  void testNameSpaceProperties() {
    String namespaceName = getTestNamespace("alter_foo1");
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
    String namespaceName = getTestNamespace("dml");
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
        String.format("CREATE TABLE %s.create_foo1", getTestNamespace())
            + "( id bigint, data string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (bucket(16, id), days(ts))");
    Map<String, String> tableInfo =
        getTableInfo(String.format("%s.create_foo1", getTestNamespace()));
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
        () -> sql(String.format("CREATE TABLE %s.create_foo1", getTestNamespace())));
  }

  @Test
  void testDropTable() {
    sql(
        String.format("CREATE TABLE %s.drop_foo1", getTestNamespace())
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql(String.format("DROP TABLE %s.drop_foo1", getTestNamespace()));
    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () -> sql(String.format("DESC TABLE %s.drop_foo1", getTestNamespace())));

    Assertions.assertThrowsExactly(
        NoSuchTableException.class,
        () -> sql(String.format("DROP TABLE %s.drop_foo1", getTestNamespace())));
  }

  @Test
  void testListTable() {
    String namespaceName = getTestNamespace("list_db");
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
        String.format("CREATE TABLE %s.rename_foo1", getTestNamespace())
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        String.format("ALTER TABLE %s.rename_foo1 ", getTestNamespace())
            + String.format("RENAME TO %s.rename_foo2", getTestNamespace()));
    sql(String.format("desc table %s.rename_foo2", getTestNamespace()));
    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () -> sql(String.format("desc table %s.rename_foo1", getTestNamespace())));

    sql(
        String.format("CREATE TABLE %s.rename_foo1", getTestNamespace())
            + "(id bigint COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        TableAlreadyExistsException.class,
        () ->
            sql(
                String.format("ALTER TABLE %s.rename_foo2 ", getTestNamespace())
                    + String.format("RENAME TO %s.rename_foo1", getTestNamespace())));
  }

  @Test
  void testSetTableProperties() {
    sql(
        String.format("CREATE TABLE %s.set_foo1", getTestNamespace())
            + " (id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        String.format("ALTER TABLE %s.set_foo1 SET TBLPROPERTIES ", getTestNamespace())
            + "('read.split.target-size'='268435456')");
    Map<String, String> m = getTableInfo(getTestNamespace() + ".set_foo1");
    Assertions.assertTrue(
        m.getOrDefault("Table Properties", "").contains("read.split.target-size=268435456"));

    sql(
        String.format("ALTER TABLE %s.set_foo1 ", getTestNamespace())
            + "UNSET TBLPROPERTIES ('read.split.target-size')");
    m = getTableInfo(getTestNamespace() + ".set_foo1");
    Assertions.assertFalse(
        m.getOrDefault("Table Properties", "read.split.target-size")
            .contains("read.split.target-size"));

    sql(
        String.format(
            "ALTER TABLE %s.set_foo1 SET TBLPROPERTIES ('comment'='a')", getTestNamespace()));
    m = getTableInfo(getTestNamespace() + ".set_foo1");
    // comment is hidden
    Assertions.assertFalse(m.getOrDefault("Table Properties", "").contains("comment=a"));
  }

  @Test
  void testAddColumns() {
    sql(
        String.format("CREATE TABLE %s.add_foo1", getTestNamespace())
            + " (id string COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () ->
            sql(
                String.format("ALTER TABLE %s.add_foo1 ", getTestNamespace())
                    + "ADD COLUMNS foo_after String After not_exits"));

    sql(
        String.format(
            "ALTER TABLE %s.add_foo1 ADD COLUMNS foo_after String After id", getTestNamespace()));
    List<String> columns = getTableColumns(getTestNamespace() + ".add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data"), columns);

    sql(String.format("ALTER TABLE %s.add_foo1 ADD COLUMNS foo_last String", getTestNamespace()));
    columns = getTableColumns(getTestNamespace() + ".add_foo1");
    Assertions.assertEquals(Arrays.asList("id", "foo_after", "data", "foo_last"), columns);

    sql(
        String.format(
            "ALTER TABLE %s.add_foo1 ADD COLUMNS foo_first String FIRST", getTestNamespace()));
    columns = getTableColumns(getTestNamespace() + ".add_foo1");
    Assertions.assertEquals(
        Arrays.asList("foo_first", "id", "foo_after", "data", "foo_last"), columns);
  }

  @Test
  void testRenameColumns() {
    sql(
        String.format("CREATE TABLE %s.renameC_foo1", getTestNamespace())
            + " (id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        String.format(
            "ALTER TABLE %s.renameC_foo1 RENAME COLUMN data TO data1", getTestNamespace()));

    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".renameC_foo1");
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
        String.format("CREATE TABLE %s.dropC_foo1 ", getTestNamespace())
            + "(id bigint COMMENT 'unique id',data string) using iceberg");

    Assertions.assertThrowsExactly(
        AnalysisException.class,
        () ->
            sql(
                String.format(
                    "ALTER TABLE %s.dropC_foo1 DROP COLUMNS not_exits", getTestNamespace())));

    sql(String.format("ALTER TABLE %s.dropC_foo1 DROP COLUMNS data", getTestNamespace()));
    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".dropC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
    Assertions.assertFalse(m.containsKey("data"));
  }

  @Test
  void testUpdateColumnType() {
    sql(
        String.format("CREATE TABLE %s.updateC_foo1 ", getTestNamespace())
            + "(id int COMMENT 'unique id',data string) using iceberg");
    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".updateC_foo1");
    Map<String, String> m = ImmutableMap.of("id", "int");
    checkMapContains(m, tableInfo);

    sql(
        String.format(
            "ALTER TABLE %s.updateC_foo1 ALTER COLUMN id TYPE bigint", getTestNamespace()));
    tableInfo = getTableInfo(getTestNamespace() + ".updateC_foo1");
    m = ImmutableMap.of("id", "bigint");
    checkMapContains(m, tableInfo);
  }

  @Test
  void testUpdateColumnPosition() {
    sql(
        String.format("CREATE TABLE %s.updateP_foo1 ", getTestNamespace())
            + "(id string COMMENT 'unique id',data string) using iceberg");
    List<String> columns = getTableColumns(getTestNamespace() + ".updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);

    sql(
        String.format(
            "ALTER TABLE %s.updateP_foo1 ALTER COLUMN id AFTER data", getTestNamespace()));
    columns = getTableColumns(getTestNamespace() + ".updateP_foo1");
    Assertions.assertEquals(Arrays.asList("data", "id"), columns);

    sql(String.format("ALTER TABLE %s.updateP_foo1 ALTER COLUMN id FIRST", getTestNamespace()));
    columns = getTableColumns(getTestNamespace() + ".updateP_foo1");
    Assertions.assertEquals(Arrays.asList("id", "data"), columns);
  }

  @Test
  void testAlterPartitions() {
    sql(
        String.format("CREATE TABLE %s.part_foo1", getTestNamespace())
            + "( id bigint, data string, ts timestamp) USING iceberg");
    sql(
        String.format(
            "ALTER TABLE %s.part_foo1 ADD PARTITION FIELD bucket(16, id)", getTestNamespace()));
    sql(
        String.format(
            "ALTER TABLE %s.part_foo1 ADD PARTITION FIELD truncate(4, data)", getTestNamespace()));
    sql(
        String.format(
            "ALTER TABLE %s.part_foo1 ADD PARTITION FIELD years(ts)", getTestNamespace()));

    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".part_foo1");
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
                String.format("ALTER TABLE %s.part_foo1 ", getTestNamespace())
                    + "DROP PARTITION FIELD bucket(8, id)"));
    sql(
        String.format(
            "ALTER TABLE %s.part_foo1 DROP PARTITION FIELD bucket(16, id)", getTestNamespace()));
    tableInfo = getTableInfo(getTestNamespace() + ".part_foo1");
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
                String.format("ALTER TABLE %s.part_foo1 ", getTestNamespace())
                    + "REPLACE PARTITION FIELD months(ts) WITH days(ts)"));
    sql(
        String.format("ALTER TABLE %s.part_foo1 ", getTestNamespace())
            + "REPLACE PARTITION FIELD years(ts) WITH days(ts)");
    tableInfo = getTableInfo(getTestNamespace() + ".part_foo1");
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
        String.format("CREATE TABLE %s.sort_foo1", getTestNamespace())
            + "( id bigint, data string, ts timestamp) USING iceberg");
    Assertions.assertThrowsExactly(
        ValidationException.class,
        () ->
            sql(
                String.format(
                    "ALTER TABLE %s.sort_foo1 WRITE ORDERED BY xx, id", getTestNamespace())));
    sql(
        String.format("ALTER TABLE %s.sort_foo1 ", getTestNamespace())
            + "WRITE ORDERED BY data ASC NULLS FIRST, id ASC NULLS FIRST");
    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".sort_foo1");
    Assertions.assertTrue(
        tableInfo
            .get("Table Properties")
            .contains("sort-order=data ASC NULLS FIRST, id ASC NULLS FIRST,"));

    // replace with new one
    sql(
        String.format(
            "ALTER TABLE %s.sort_foo1 WRITE ORDERED BY ts ASC NULLS FIRST", getTestNamespace()));
    tableInfo = getTableInfo(getTestNamespace() + ".sort_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("sort-order=ts ASC NULLS FIRST,"));
  }

  @Test
  void testAlterPartitionBy() {
    sql(
        String.format("CREATE TABLE %s.partby_foo1", getTestNamespace())
            + "( id bigint, data string, ts timestamp) USING iceberg");
    sql(
        String.format(
            "ALTER TABLE %s.partby_foo1 WRITE DISTRIBUTED BY PARTITION", getTestNamespace()));
    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".partby_foo1");
    Assertions.assertTrue(
        tableInfo.get("Table Properties").contains("write.distribution-mode=hash"));
  }

  @Test
  void testAlterIdentifier() {
    sql(
        String.format("CREATE TABLE %s.identifier_foo1", getTestNamespace())
            + "( id bigint NOT NULL, data string, ts timestamp) USING iceberg");
    sql(
        String.format(
            "ALTER TABLE %s.identifier_foo1 SET IDENTIFIER FIELDS id", getTestNamespace()));
    Map<String, String> tableInfo = getTableInfo(getTestNamespace() + ".identifier_foo1");
    Assertions.assertTrue(tableInfo.get("Table Properties").contains("identifier-fields=[id]"));

    sql(
        String.format(
            "ALTER TABLE %s.identifier_foo1 DROP IDENTIFIER FIELDS id", getTestNamespace()));
    tableInfo = getTableInfo(getTestNamespace() + ".identifier_foo1");
    Assertions.assertFalse(tableInfo.get("Table Properties").contains("identifier-fields"));

    // java.lang.IllegalArgumentException: Cannot add field id as an identifier field: not a
    // required field
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () ->
            sql(
                String.format(
                    "ALTER TABLE %s.identifier_foo1 SET IDENTIFIER FIELDS data",
                    getTestNamespace())));
  }

  @Test
  // MemoryCatalog doesn't support snapshot operations, error is:
  // org.apache.iceberg.exceptions.NotFoundException: File does not exist:
  // /tmp/iceberg_rest_table_test/snapshot_foo1/metadata/00002-c7516f8e-ef6b-406a-8d78-9dda825dd762.metadata.json
  // sql("SELECT * FROM table_test.snapshot_foo1.snapshots");
  @EnabledIf("catalogTypeNotMemory")
  void testSnapshot() {
    sql(
        String.format("CREATE TABLE %s.snapshot_foo1 ", getTestNamespace())
            + "(id bigint COMMENT 'unique id',data string) using iceberg");
    sql(
        String.format(
            " INSERT INTO %s.snapshot_foo1 VALUES (1, 'a'), (2, 'b');", getTestNamespace()));
    sql(
        String.format(
            " INSERT INTO %s.snapshot_foo1 VALUES (3, 'c'), (4, 'd');", getTestNamespace()));
    List<String> snapshots =
        convertToStringList(
            sql(String.format("SELECT * FROM %s.snapshot_foo1.snapshots", getTestNamespace())), 1);

    Assertions.assertEquals(2, snapshots.size());
    String oldSnapshotId = snapshots.get(0);
    sql(
        String.format(
            "CALL rest.system.rollback_to_snapshot('%s.snapshot_foo1', %s)",
            getTestNamespace(), oldSnapshotId));
    Map<String, String> result =
        convertToStringMap(
            sql(String.format("select * from %s.snapshot_foo1", getTestNamespace())));
    Assertions.assertEquals(ImmutableMap.of("1", "a", "2", "b"), result);
  }

  @Test
  @EnabledIf("catalogTypeNotMemory")
  void testRegisterTable() {
    String registerDB = "iceberg_register_db";
    String registerTableName = "register_foo1";
    sql("CREATE DATABASE " + registerDB);
    sql(
        String.format(
            "CREATE TABLE %s.%s (id bigint COMMENT 'unique id',data string) USING iceberg",
            registerDB, registerTableName));
    sql(String.format("INSERT INTO %s.%s VALUES (1, 'a')", registerDB, registerTableName));

    // get metadata location
    List<String> metadataLocations =
        convertToStringList(
            sql(
                String.format(
                    "SELECT file FROM %s.%s.metadata_log_entries", registerDB, registerTableName)),
            0);
    String metadataLocation = metadataLocations.get(metadataLocations.size() - 1);

    // register table
    String register =
        String.format(
            "CALL rest.system.register_table(table => '%s.register_foo2', metadata_file=> '%s')",
            getTestNamespace(), metadataLocation);
    sql(register);

    Map<String, String> result =
        convertToStringMap(
            sql(String.format("SELECT * FROM %s.register_foo2", getTestNamespace())));
    Assertions.assertEquals(ImmutableMap.of("1", "a"), result);

    // insert other data
    sql(String.format("INSERT INTO %s.register_foo2 VALUES (2, 'b')", getTestNamespace()));
    result =
        convertToStringMap(
            sql(String.format("SELECT * FROM %s.register_foo2", getTestNamespace())));
    Assertions.assertEquals(ImmutableMap.of("1", "a", "2", "b"), result);
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testCreateViewAndDisplayView() {
    String originTableName = getTestNamespace() + ".create_table_for_view_1";
    String viewName = getTestNamespace() + ".test_create_view";

    sql(
        String.format(
            "CREATE TABLE %s ( id bigint, data string, ts timestamp) USING iceberg",
            originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, originTableName));

    Map<String, String> viewInfo = getViewInfo(viewName);
    Map<String, String> m =
        ImmutableMap.of(
            "id", "bigint",
            "data", "string",
            "ts", "timestamp");

    checkMapContains(m, viewInfo);
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testViewProperties() {
    String originTableName = getTestNamespace() + ".create_table_for_view_2";
    String viewName = getTestNamespace() + ".test_create_view_with_properties";
    sql(
        String.format(
            "CREATE TABLE %s ( id bigint, data string, ts timestamp) USING iceberg",
            originTableName));

    // test create view with properties
    sql(
        String.format(
            "CREATE VIEW %s TBLPROPERTIES ('key1' = 'val1') AS SELECT * FROM %s",
            viewName, originTableName));

    Map<String, String> viewInfo = getViewInfo(viewName);
    Assertions.assertTrue(viewInfo.getOrDefault("View Properties", "").contains("'key1' = 'val1'"));
    Assertions.assertFalse(
        viewInfo.getOrDefault("View Properties", "").contains("'key2' = 'val2'"));

    // test set properties
    sql(
        String.format(
            "ALTER VIEW %s SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2')", viewName));

    viewInfo = getViewInfo(viewName);
    Assertions.assertTrue(viewInfo.getOrDefault("View Properties", "").contains("'key1' = 'val1'"));
    Assertions.assertTrue(viewInfo.getOrDefault("View Properties", "").contains("'key2' = 'val2'"));

    // test unset properties
    sql(String.format("ALTER VIEW %s UNSET TBLPROPERTIES ('key1', 'key2')", viewName));

    viewInfo = getViewInfo(viewName);
    Assertions.assertFalse(
        viewInfo.getOrDefault("View Properties", "").contains("'key1' = 'val1'"));
    Assertions.assertFalse(
        viewInfo.getOrDefault("View Properties", "").contains("'key2' = 'val2'"));
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testDropView() {
    String originTableName = getTestNamespace() + ".create_table_for_view_3";
    String viewName = getTestNamespace() + ".test_drop_view";

    sql(
        String.format(
            "CREATE TABLE %s ( id bigint, data string, ts timestamp) USING iceberg",
            originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, originTableName));
    sql(String.format("DROP VIEW %s", viewName));

    Assertions.assertThrowsExactly(AnalysisException.class, () -> getViewInfo(viewName));
    Assertions.assertThrowsExactly(
        NoSuchViewException.class, () -> sql(String.format("DROP VIEW %s", viewName)));
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testReplaceView() {
    String originTableName = getTestNamespace() + ".create_table_for_view_4";
    String viewName = getTestNamespace() + ".test_replace_view";

    sql(
        String.format(
            "CREATE TABLE %s (id bigint, data string, ts timestamp) USING iceberg",
            originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, originTableName));
    sql(
        String.format(
            "CREATE OR REPLACE VIEW %s (updated_id COMMENT 'updated ID') TBLPROPERTIES ('key1' = 'new_val1') AS SELECT id FROM %s",
            viewName, originTableName));

    Map<String, String> viewInfo = getViewInfo(viewName);
    Assertions.assertTrue(
        viewInfo.getOrDefault("View Properties", "").contains("'key1' = 'new_val1'"));
    Assertions.assertTrue(viewInfo.containsKey("updated_id"));
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testShowAvailableViews() {
    String originTableName = getTestNamespace() + ".create_table_for_view_5";
    String viewName1 = getTestNamespace() + ".show_available_views_1";
    String viewName2 = getTestNamespace() + ".show_available_views_2";

    sql(
        String.format(
            "CREATE TABLE %s (id bigint, data string, ts timestamp) USING iceberg",
            originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName1, originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName2, originTableName));

    List<Object[]> views = sql("SHOW VIEWS IN " + getTestNamespace());
    Assertions.assertEquals(2, views.size());
  }

  @Test
  @EnabledIf("isSupportsViewCatalog")
  void testShowCreateStatementView() {
    String originTableName = getTestNamespace() + ".create_table_for_view_6";
    String viewName = getTestNamespace() + ".show_create_statement_view";

    sql(
        String.format(
            "CREATE TABLE %s (id bigint, data string, ts timestamp) USING iceberg",
            originTableName));
    sql(String.format("CREATE VIEW %s AS SELECT * FROM %s", viewName, originTableName));

    List<Object[]> result = sql(String.format("SHOW CREATE TABLE %s", viewName));
    Assertions.assertEquals(1, result.size());
    Assertions.assertTrue(
        Arrays.stream(result.get(0)).findFirst().orElse("").toString().contains(viewName));
  }
}
