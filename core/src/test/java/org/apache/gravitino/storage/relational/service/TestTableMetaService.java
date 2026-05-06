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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.auth.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestTableMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_table_test";
  private final String catalogName = "catalog_for_table_test";
  private final String schemaName = "schema_for_table_test";

  private List<EntityChangeRecord> listEntityChanges(long createdAtAfter) {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, mapper -> mapper.selectChanges(createdAtAfter, 100));
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(tableCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    TableEntity tableCopy =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table1",
            AUDIT_INFO);
    backend.insert(table, false);
    backend.insert(tableCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                tableCopy.nameIdentifier(),
                Entity.EntityType.TABLE,
                e ->
                    createTableEntity(tableCopy.id(), tableCopy.namespace(), "table", AUDIT_INFO)));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, AUDIT_INFO);
    backend.insert(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofCatalog(metalakeName),
            catalogName,
            AUDIT_INFO);
    backend.insert(catalog, false);
    SchemaEntity schema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schemaName,
            AUDIT_INFO);
    backend.insert(schema, false);

    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofTable(metalakeName, catalogName, schemaName),
            "table",
            AUDIT_INFO);
    backend.insert(table, false);

    List<TableEntity> tables = backend.list(table.namespace(), Entity.EntityType.TABLE, true);
    assertTrue(tables.contains(table));

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);
    assertFalse(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(table.id(), Entity.EntityType.TABLE));
  }

  @TestTemplate
  public void testUpdateTable() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(metalakeName, catalogName, schemaName, AUDIT_INFO);

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
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column1))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(createdTable, false);

    // test update table without changing schema name
    long beforeRename = System.currentTimeMillis() - 1;
    TableEntity updatedTable =
        TableEntity.builder()
            .withId(createdTable.id())
            .withName("table2")
            .withNamespace(createdTable.namespace())
            .withColumns(createdTable.columns())
            .withAuditInfo(AUDIT_INFO)
            .build();
    Function<TableEntity, TableEntity> updater = oldTable -> updatedTable;
    TableMetaService.getInstance().updateTable(createdTable.nameIdentifier(), updater);

    TableEntity retrievedTable =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable.nameIdentifier());
    Assertions.assertEquals(updatedTable.id(), retrievedTable.id());
    Assertions.assertEquals(updatedTable.name(), retrievedTable.name());
    Assertions.assertEquals(updatedTable.namespace(), retrievedTable.namespace());
    Assertions.assertEquals(updatedTable.auditInfo(), retrievedTable.auditInfo());
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());
    Assertions.assertTrue(
        listEntityChanges(beforeRename).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.TABLE.name())
                        && record
                            .getFullName()
                            .equals(
                                NameIdentifierUtil.ofTable(
                                        metalakeName, catalogName, schemaName, "table1")
                                    .toString())
                        && record.getOperateType() == OperateType.ALTER));

    // test update table with changing schema name to a non-existing schema
    String newSchemaName = "schema2";
    TableEntity updatedTable2 =
        TableEntity.builder()
            .withId(updatedTable.id())
            .withName("table3")
            .withNamespace(Namespace.of(metalakeName, catalogName, newSchemaName))
            .withColumns(updatedTable.columns())
            .withAuditInfo(AUDIT_INFO)
            .build();
    Function<TableEntity, TableEntity> updater2 = oldTable -> updatedTable2;
    Exception e =
        Assertions.assertThrows(
            NoSuchEntityException.class,
            () ->
                TableMetaService.getInstance()
                    .updateTable(updatedTable.nameIdentifier(), updater2));
    Assertions.assertTrue(e.getMessage().contains(newSchemaName));

    // test update table with changing schema name to an existing schema
    SchemaEntity newSchema =
        createSchemaEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of(metalakeName, catalogName),
            newSchemaName,
            AUDIT_INFO);
    backend.insert(newSchema, false);
    TableMetaService.getInstance().updateTable(updatedTable.nameIdentifier(), updater2);

    TableEntity retrievedTable2 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable2.nameIdentifier());
    Assertions.assertEquals(updatedTable2.id(), retrievedTable2.id());
    Assertions.assertEquals(updatedTable2.name(), retrievedTable2.name());
    Assertions.assertEquals(updatedTable2.namespace(), retrievedTable2.namespace());
    Assertions.assertEquals(updatedTable2.auditInfo(), retrievedTable2.auditInfo());
    compareTwoColumns(updatedTable2.columns(), retrievedTable2.columns());

    long beforeDelete = System.currentTimeMillis() - 1;
    Assertions.assertTrue(
        TableMetaService.getInstance().deleteTable(updatedTable2.nameIdentifier()));
    Assertions.assertTrue(
        listEntityChanges(beforeDelete).stream()
            .anyMatch(
                record ->
                    record.getMetalakeName().equals(metalakeName)
                        && record.getEntityType().equals(Entity.EntityType.TABLE.name())
                        && record
                            .getFullName()
                            .equals(
                                NameIdentifierUtil.ofTable(
                                        metalakeName, catalogName, newSchemaName, "table3")
                                    .toString())
                        && record.getOperateType() == OperateType.DROP));
  }

  @TestTemplate
  public void testBatchGetTableByIdentifierIncludesVersionInfoFields() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    Map<String, String> tableProps =
        ImmutableMap.of(Table.PROPERTY_TABLE_FORMAT, "delta", "location", "s3://bucket/path");
    Distribution distribution = Distributions.of(Strategy.HASH, 4, NamedReference.field("col1"));
    SortOrder[] sortOrders =
        new SortOrder[] {SortOrders.of(NamedReference.field("col1"), SortDirection.ASCENDING)};
    Transform[] partitioning = new Transform[] {Transforms.identity("col2")};
    Index[] indexes = new Index[] {Indexes.primary("pk", new String[][] {{"col1"}})};

    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("delta_table")
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, schemaName))
            .withProperties(tableProps)
            .withComment("test table comment")
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .withPartitioning(partitioning)
            .withIndexes(indexes)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);

    NameIdentifier tableIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "delta_table");
    List<TableEntity> results =
        TableMetaService.getInstance().batchGetTableByIdentifier(List.of(tableIdent));

    Assertions.assertEquals(1, results.size());
    TableEntity result = results.get(0);

    // Verify properties (including format) are returned
    Assertions.assertNotNull(result.properties());
    Assertions.assertEquals("delta", result.properties().get(Table.PROPERTY_TABLE_FORMAT));
    Assertions.assertEquals("s3://bucket/path", result.properties().get("location"));

    // Verify comment is returned
    Assertions.assertEquals("test table comment", result.comment());

    // Verify distribution is returned
    Assertions.assertNotNull(result.distribution());
    Assertions.assertEquals(distribution, result.distribution());

    // Verify sort orders are returned
    Assertions.assertNotNull(result.sortOrders());
    Assertions.assertArrayEquals(sortOrders, result.sortOrders());

    // Verify partitioning is returned — compare field references since serialization may change
    // the concrete implementation class (e.g., IdentityTransform -> IdentityPartitioningDTO)
    Assertions.assertNotNull(result.partitioning());
    Assertions.assertEquals(partitioning.length, result.partitioning().length);
    Assertions.assertEquals(
        ((NamedReference.FieldReference) partitioning[0].references()[0]).fieldName()[0],
        ((NamedReference.FieldReference) result.partitioning()[0].references()[0]).fieldName()[0]);

    // Verify indexes are returned
    Assertions.assertNotNull(result.indexes());
    Assertions.assertArrayEquals(indexes, result.indexes());
  }

  @TestTemplate
  public void testBatchGetTableByIdentifierDoesNotIncludeColumns() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);

    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("col1")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table_with_columns")
            .withNamespace(NamespaceUtil.ofTable(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableMetaService.getInstance().insertTable(table, false);

    // Verify getTableByIdentifier (single-get path) returns columns
    TableEntity singleGetResult =
        TableMetaService.getInstance()
            .getTableByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, schemaName, "table_with_columns"));
    Assertions.assertEquals(1, singleGetResult.columns().size());

    // batchGetTableByIdentifier does not fetch columns (separate table_column_meta table)
    NameIdentifier tableIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, "table_with_columns");
    List<TableEntity> results =
        TableMetaService.getInstance().batchGetTableByIdentifier(List.of(tableIdent));

    Assertions.assertEquals(1, results.size());
    Assertions.assertTrue(
        results.get(0).columns().isEmpty(),
        "batchGetTableByIdentifier does not fetch columns from table_column_meta");
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
}
