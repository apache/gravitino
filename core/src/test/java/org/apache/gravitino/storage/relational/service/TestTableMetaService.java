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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableMetaService extends TestJDBCBackend {
  private final String metalakeName = "metalake_for_table_test";
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  @Test
  public void testUpdateTable() throws IOException {
    String catalogName = "catalog1";
    String schemaName = "schema1";
    createParentEntities(metalakeName, catalogName, schemaName, auditInfo);

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
    TableEntity createdTable =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("table1")
            .withNamespace(Namespace.of(metalakeName, catalogName, schemaName))
            .withColumns(List.of(column1))
            .withAuditInfo(auditInfo)
            .build();
    TableMetaService.getInstance().insertTable(createdTable, false);

    // test update table without changing schema name
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
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());
    compareTwoColumns(updatedTable.columns(), retrievedTable.columns());

    // test update table with changing schema name to a non-existing schema
    String newSchemaName = "schema2";
    TableEntity updatedTable2 =
        TableEntity.builder()
            .withId(updatedTable.id())
            .withName("table3")
            .withNamespace(Namespace.of(metalakeName, catalogName, newSchemaName))
            .withColumns(updatedTable.columns())
            .withAuditInfo(auditInfo)
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
            auditInfo);
    backend.insert(newSchema, false);
    TableMetaService.getInstance().updateTable(updatedTable.nameIdentifier(), updater2);

    TableEntity retrievedTable2 =
        TableMetaService.getInstance().getTableByIdentifier(updatedTable2.nameIdentifier());
    Assertions.assertEquals(updatedTable2.id(), retrievedTable2.id());
    Assertions.assertEquals(updatedTable2.name(), retrievedTable2.name());
    Assertions.assertEquals(updatedTable2.namespace(), retrievedTable2.namespace());
    Assertions.assertEquals(updatedTable2.auditInfo(), retrievedTable2.auditInfo());
    compareTwoColumns(updatedTable2.columns(), retrievedTable2.columns());
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
