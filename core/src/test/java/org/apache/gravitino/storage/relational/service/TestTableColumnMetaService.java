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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Lists;

public class TestTableColumnMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_table_column_test";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  @Test
  public void testInsertAndGetTableColumns() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    // Create a table entity without columns
    TableEntity createdTable =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalogName, schemaName),
            "table1",
            auditInfo);
    TableMetaService.getInstance().insertTable(createdTable, false);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(createdTable.nameIdentifier());
    Assertions.assertEquals(createdTable.id(), retrievedTable.id());
    Assertions.assertEquals(createdTable.name(), retrievedTable.name());
    Assertions.assertEquals(createdTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(createdTable.auditInfo(), retrievedTable.auditInfo());
    Assertions.assertTrue(retrievedTable.columns().isEmpty());

    // Create a table entity with columns
    ColumnEntity column1 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();
    ColumnEntity column2 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column2")
            .withPosition(1)
            .withComment("comment2")
            .withDataType(Types.StringType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.stringLiteral("1"))
            .withAuditInfo(auditInfo)
            .build();
    TableEntity createdTable2 =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table2")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(Lists.newArrayList(column1, column2))
            .withAuditInfo(auditInfo)
            .build();
    TableMetaService.getInstance().insertTable(createdTable2, false);

    TableEntity retrievedTable2 =
        TableMetaService.getInstance().getTableByIdentifier(createdTable2.nameIdentifier());
    Assertions.assertEquals(createdTable2.id(), retrievedTable2.id());
    Assertions.assertEquals(createdTable2.name(), retrievedTable2.name());
    Assertions.assertEquals(createdTable2.namespace(), retrievedTable2.namespace());
    Assertions.assertEquals(createdTable2.auditInfo(), retrievedTable2.auditInfo());
    Assertions.assertEquals(createdTable2.columns().size(), retrievedTable2.columns().size());
    compareTwoColumns(createdTable2.columns(), retrievedTable2.columns());

    // test insert with overwrite
    ColumnEntity column3 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column3")
            .withPosition(0)
            .withComment("comment3")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity createdTable3 =
        TableEntity.builder()
            .withId(createdTable2.id())
            .withName("table3")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(Lists.newArrayList(column3))
            .withAuditInfo(auditInfo)
            .build();

    TableMetaService.getInstance().insertTable(createdTable3, true);
    TableEntity retrievedTable3 =
        TableMetaService.getInstance().getTableByIdentifier(createdTable3.nameIdentifier());
    Assertions.assertEquals(createdTable3.id(), retrievedTable3.id());
    Assertions.assertEquals(createdTable3.name(), retrievedTable3.name());
    Assertions.assertEquals(createdTable3.namespace(), retrievedTable3.namespace());
    Assertions.assertEquals(createdTable3.auditInfo(), retrievedTable3.auditInfo());
    Assertions.assertEquals(createdTable3.columns().size(), retrievedTable3.columns().size());
    compareTwoColumns(createdTable3.columns(), retrievedTable3.columns());
  }

  @Test
  public void testUpdateTable() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    // Create a table entity without columns
    TableEntity createdTable =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(METALAKE_NAME, catalogName, schemaName),
            "table1",
            auditInfo);
    TableMetaService.getInstance().insertTable(createdTable, false);

    // Test update table with new name
    TableEntity updatedTable =
        TableEntity.builder()
            .withId(createdTable.id())
            .withName("table2")
            .withNamespace(createdTable.namespace())
            .withColumns(createdTable.columns())
            .withAuditInfo(auditInfo)
            .build();
    Function<TableEntity, TableEntity> updater = oldTable -> updatedTable;

    TableMetaService.getInstance().updateTable(createdTable.nameIdentifier(), updater);
    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable.nameIdentifier());

    Assertions.assertEquals(updatedTable.id(), retrievedTable.id());
    Assertions.assertEquals(updatedTable.name(), retrievedTable.name());
    Assertions.assertEquals(updatedTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(updatedTable.auditInfo(), retrievedTable.auditInfo());
    Assertions.assertTrue(retrievedTable.columns().isEmpty());

    // Test update table with adding one new column
    ColumnEntity column1 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity updatedTable2 =
        TableEntity.builder()
            .withId(updatedTable.id())
            .withName(updatedTable.name())
            .withNamespace(updatedTable.namespace())
            .withColumns(Lists.newArrayList(column1))
            .withAuditInfo(auditInfo)
            .build();

    Function<TableEntity, TableEntity> updater2 = oldTable -> updatedTable2;
    TableMetaService.getInstance().updateTable(updatedTable.nameIdentifier(), updater2);

    TableEntity retrievedTable2 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable2.nameIdentifier());

    Assertions.assertEquals(updatedTable2.id(), retrievedTable2.id());
    Assertions.assertEquals(updatedTable2.name(), retrievedTable2.name());
    Assertions.assertEquals(updatedTable2.namespace(), retrievedTable2.namespace());
    Assertions.assertEquals(updatedTable2.auditInfo(), retrievedTable2.auditInfo());
    Assertions.assertEquals(updatedTable2.columns().size(), retrievedTable2.columns().size());
    compareTwoColumns(updatedTable2.columns(), retrievedTable2.columns());

    // Update the table with add one more column
    ColumnEntity column2 =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column2")
            .withPosition(1)
            .withComment("comment2")
            .withDataType(Types.StringType.get())
            .withNullable(false)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.stringLiteral("1"))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity updatedTable3 =
        TableEntity.builder()
            .withId(updatedTable2.id())
            .withName(updatedTable2.name())
            .withNamespace(updatedTable2.namespace())
            .withColumns(Lists.newArrayList(column1, column2))
            .withAuditInfo(auditInfo)
            .build();

    Function<TableEntity, TableEntity> updater3 = oldTable -> updatedTable3;
    TableMetaService.getInstance().updateTable(updatedTable2.nameIdentifier(), updater3);

    TableEntity retrievedTable3 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable3.nameIdentifier());

    Assertions.assertEquals(updatedTable3.id(), retrievedTable3.id());
    Assertions.assertEquals(updatedTable3.name(), retrievedTable3.name());
    Assertions.assertEquals(updatedTable3.namespace(), retrievedTable3.namespace());
    Assertions.assertEquals(updatedTable3.auditInfo(), retrievedTable3.auditInfo());
    Assertions.assertEquals(updatedTable3.columns().size(), retrievedTable3.columns().size());
    compareTwoColumns(updatedTable3.columns(), retrievedTable3.columns());

    // Update the table with updating one column
    ColumnEntity updatedColumn1 =
        ColumnEntity.builder()
            .withId(column1.id())
            .withName(column1.name())
            .withPosition(column1.position())
            .withComment("comment1_updated")
            .withDataType(Types.LongType.get())
            .withNullable(column1.nullable())
            .withAutoIncrement(column1.autoIncrement())
            .withDefaultValue(null)
            .withAuditInfo(auditInfo)
            .build();
    TableEntity updatedTable4 =
        TableEntity.builder()
            .withId(updatedTable3.id())
            .withName(updatedTable3.name())
            .withNamespace(updatedTable3.namespace())
            .withColumns(Lists.newArrayList(updatedColumn1, column2))
            .withAuditInfo(auditInfo)
            .build();

    Function<TableEntity, TableEntity> updater4 = oldTable -> updatedTable4;
    TableMetaService.getInstance().updateTable(updatedTable3.nameIdentifier(), updater4);

    TableEntity retrievedTable4 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable4.nameIdentifier());

    Assertions.assertEquals(updatedTable4.id(), retrievedTable4.id());
    Assertions.assertEquals(updatedTable4.name(), retrievedTable4.name());
    Assertions.assertEquals(updatedTable4.namespace(), retrievedTable4.namespace());
    Assertions.assertEquals(updatedTable4.auditInfo(), retrievedTable4.auditInfo());
    Assertions.assertEquals(updatedTable4.columns().size(), retrievedTable4.columns().size());
    compareTwoColumns(updatedTable4.columns(), retrievedTable4.columns());

    // Update the table with removing one column
    TableEntity updatedTable5 =
        TableEntity.builder()
            .withId(updatedTable4.id())
            .withName(updatedTable4.name())
            .withNamespace(updatedTable4.namespace())
            .withColumns(Lists.newArrayList(column2))
            .withAuditInfo(auditInfo)
            .build();

    Function<TableEntity, TableEntity> updater5 = oldTable -> updatedTable5;
    TableMetaService.getInstance().updateTable(updatedTable4.nameIdentifier(), updater5);

    TableEntity retrievedTable5 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable5.nameIdentifier());
    compareTwoColumns(updatedTable5.columns(), retrievedTable5.columns());

    // update the table with removing all columns
    TableEntity updatedTable6 =
        TableEntity.builder()
            .withId(updatedTable5.id())
            .withName(updatedTable5.name())
            .withNamespace(updatedTable5.namespace())
            .withAuditInfo(auditInfo)
            .build();

    Function<TableEntity, TableEntity> updater6 = oldTable -> updatedTable6;
    TableMetaService.getInstance().updateTable(updatedTable5.nameIdentifier(), updater6);

    TableEntity retrievedTable6 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable6.nameIdentifier());
    Assertions.assertTrue(retrievedTable6.columns().isEmpty());
  }

  @Test
  public void testCreateAndDeleteTable() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    // Create a table entity with column
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(Lists.newArrayList(column))
            .withAuditInfo(auditInfo)
            .build();

    TableMetaService.getInstance().insertTable(createdTable, false);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(createdTable.nameIdentifier());
    Assertions.assertEquals(createdTable.id(), retrievedTable.id());
    Assertions.assertEquals(createdTable.name(), retrievedTable.name());
    Assertions.assertEquals(createdTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(createdTable.auditInfo(), retrievedTable.auditInfo());
    compareTwoColumns(createdTable.columns(), retrievedTable.columns());

    Assertions.assertTrue(
        TableMetaService.getInstance().deleteTable(retrievedTable.nameIdentifier()));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> TableMetaService.getInstance().getTableByIdentifier(retrievedTable.nameIdentifier()));
  }

  @Test
  public void testDeleteMetalake() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    // Create a table entity with column
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(Lists.newArrayList(column))
            .withAuditInfo(auditInfo)
            .build();

    TableMetaService.getInstance().insertTable(createdTable, false);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(createdTable.nameIdentifier());
    Assertions.assertEquals(createdTable.id(), retrievedTable.id());
    Assertions.assertEquals(createdTable.name(), retrievedTable.name());
    Assertions.assertEquals(createdTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(createdTable.auditInfo(), retrievedTable.auditInfo());
    compareTwoColumns(createdTable.columns(), retrievedTable.columns());

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(NameIdentifier.of(METALAKE_NAME), true));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> TableMetaService.getInstance().getTableByIdentifier(retrievedTable.nameIdentifier()));
  }

  @Test
  public void testGetColumnIdAndPO() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    // Create a table entity with column
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column1")
            .withPosition(0)
            .withComment("comment1")
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(1))
            .withAuditInfo(auditInfo)
            .build();

    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(Lists.newArrayList(column))
            .withAuditInfo(auditInfo)
            .build();

    TableMetaService.getInstance().insertTable(createdTable, false);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(createdTable.nameIdentifier());
    Assertions.assertEquals(1, retrievedTable.columns().size());
    Assertions.assertEquals(column.id(), retrievedTable.columns().get(0).id());

    Long columnId =
        TableColumnMetaService.getInstance()
            .getColumnIdByTableIdAndName(retrievedTable.id(), column.name());
    Assertions.assertEquals(column.id(), columnId);

    ColumnPO retrievedColumn = TableColumnMetaService.getInstance().getColumnPOById(column.id());
    Assertions.assertEquals(column.id(), retrievedColumn.getColumnId());
    Assertions.assertEquals(column.name(), retrievedColumn.getColumnName());
    Assertions.assertEquals(column.position(), retrievedColumn.getColumnPosition());
    Assertions.assertEquals(column.comment(), retrievedColumn.getColumnComment());
    Assertions.assertEquals(
        ColumnPO.ColumnOpType.CREATE.value(), retrievedColumn.getColumnOpType());

    // Update the column name
    ColumnEntity updatedColumn =
        ColumnEntity.builder()
            .withId(column.id())
            .withName("column1_updated")
            .withPosition(column.position())
            .withComment(column.comment())
            .withDataType(column.dataType())
            .withNullable(column.nullable())
            .withAutoIncrement(column.autoIncrement())
            .withDefaultValue(column.defaultValue())
            .withAuditInfo(auditInfo)
            .build();

    TableEntity updatedTable =
        TableEntity.builder()
            .withId(retrievedTable.id())
            .withName(retrievedTable.name())
            .withNamespace(retrievedTable.namespace())
            .withColumns(Lists.newArrayList(updatedColumn))
            .withAuditInfo(retrievedTable.auditInfo())
            .build();

    Function<TableEntity, TableEntity> updater = oldTable -> updatedTable;
    TableMetaService.getInstance().updateTable(retrievedTable.nameIdentifier(), updater);

    Long updatedColumnId =
        TableColumnMetaService.getInstance()
            .getColumnIdByTableIdAndName(retrievedTable.id(), updatedColumn.name());
    Assertions.assertEquals(updatedColumn.id(), updatedColumnId);

    ColumnPO updatedColumnPO =
        TableColumnMetaService.getInstance().getColumnPOById(updatedColumn.id());
    Assertions.assertEquals(updatedColumn.id(), updatedColumnPO.getColumnId());
    Assertions.assertEquals(updatedColumn.name(), updatedColumnPO.getColumnName());

    // Delete the column
    TableEntity updatedTable2 =
        TableEntity.builder()
            .withId(retrievedTable.id())
            .withName(retrievedTable.name())
            .withNamespace(retrievedTable.namespace())
            .withColumns(Lists.newArrayList())
            .withAuditInfo(retrievedTable.auditInfo())
            .build();

    Function<TableEntity, TableEntity> updater2 = oldTable -> updatedTable2;
    TableMetaService.getInstance().updateTable(retrievedTable.nameIdentifier(), updater2);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            TableColumnMetaService.getInstance()
                .getColumnIdByTableIdAndName(retrievedTable.id(), updatedColumn.name()));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> TableColumnMetaService.getInstance().getColumnPOById(updatedColumn.id()));
  }

  private void compareTwoColumns(
      List<ColumnEntity> expectedColumns, List<ColumnEntity> actualColumns) {
    Assertions.assertEquals(expectedColumns.size(), actualColumns.size());
    Map<String, ColumnEntity> expectedColumnsMap =
        expectedColumns.stream().collect(Collectors.toMap(ColumnEntity::name, Function.identity()));
    actualColumns.forEach(
        column -> {
          ColumnEntity expectedColumn = expectedColumnsMap.get(column.name());
          Assertions.assertNotNull(expectedColumn);
          Assertions.assertEquals(expectedColumn.id(), column.id());
          Assertions.assertEquals(expectedColumn.name(), column.name());
          Assertions.assertEquals(expectedColumn.position(), column.position());
          Assertions.assertEquals(expectedColumn.comment(), column.comment());
          Assertions.assertEquals(expectedColumn.dataType(), column.dataType());
          Assertions.assertEquals(expectedColumn.nullable(), column.nullable());
          Assertions.assertEquals(expectedColumn.autoIncrement(), column.autoIncrement());
          Assertions.assertEquals(expectedColumn.defaultValue(), column.defaultValue());
          Assertions.assertEquals(expectedColumn.auditInfo(), column.auditInfo());
        });
  }

  @Test
  public void testDeleteColumnsByLegacyTimeline() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(METALAKE_NAME, catalogName, schemaName, auditInfo);

    List<ColumnEntity> columns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      columns.add(
          ColumnEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("column_" + i)
              .withPosition(i)
              .withComment("comment_" + i)
              .withDataType(Types.StringType.get())
              .withNullable(true)
              .withAutoIncrement(false)
              .withAuditInfo(auditInfo)
              .build());
    }

    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("legacy_table")
            .withNamespace(Namespace.of(METALAKE_NAME, catalogName, schemaName))
            .withColumns(columns)
            .withAuditInfo(auditInfo)
            .build();

    TableMetaService.getInstance().insertTable(createdTable, false);
    long now = System.currentTimeMillis();
    long legacyTimeline = now - 100000; // Past timestamp
    Connection connection = null;
    PreparedStatement stmt = null;
    try {
      connection = SqlSessions.getSqlSession().getConnection();
      for (ColumnEntity column : columns) {
        String sql =
            "UPDATE "
                + TableColumnMapper.COLUMN_TABLE_NAME
                + " SET deleted_at = ? WHERE column_id = ?";
        stmt = connection.prepareStatement(sql);
        stmt.setLong(1, legacyTimeline);
        stmt.setLong(2, column.id());
        stmt.executeUpdate();
        stmt.close();
      }
      SqlSessions.commitAndCloseSqlSession();
    } catch (Exception e) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw new IOException("Failed to update column deleted_at timestamp", e);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
    int count = countColumnsByTableId(legacyTimeline);
    Assertions.assertEquals(5, count, "Should have 5 columns with legacy timeline");
    TableColumnMetaService service = TableColumnMetaService.getInstance();
    service.deleteColumnsByLegacyTimeline(now, 3);
    count = countColumnsByTableId(legacyTimeline);
    Assertions.assertEquals(2, count, "Should have 2 columns remaining");
    service.deleteColumnsByLegacyTimeline(now, 10);
    count = countColumnsByTableId(legacyTimeline);
    Assertions.assertEquals(0, count, "Should have no columns remaining");
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(NameIdentifier.of(METALAKE_NAME), true));
  }

  private int countColumnsByTableId(long legacyTimeline) throws IOException {
    int count = 0;
    Connection connection = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      connection = SqlSessions.getSqlSession().getConnection();
      String sql =
          "SELECT COUNT(*) FROM " + TableColumnMapper.COLUMN_TABLE_NAME + " WHERE deleted_at = ?";
      stmt = connection.prepareStatement(sql);
      stmt.setLong(1, legacyTimeline);
      rs = stmt.executeQuery();
      if (rs.next()) {
        count = rs.getInt(1);
      }
      SqlSessions.commitAndCloseSqlSession();
    } catch (Exception e) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw new IOException("Failed to count columns with legacy timeline", e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
    return count;
  }
}
