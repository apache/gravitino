/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.EntityType.TABLE;
import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.TestBasePropertiesMetadata.COMMENT_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.TestColumn;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestTableOperationDispatcher extends TestOperationDispatcher {
  private static TableOperationDispatcher tableOperationDispatcher;
  private static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
    tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator);
  }

  @Test
  public void testCreateAndListTables() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema41");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table1");
    Column[] columns =
        new Column[] {
          TestColumn.builder().withName("col1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent1, columns, "comment", props, new Transform[0]);
    Assertions.assertEquals("table1", table1.name());
    Assertions.assertEquals("comment", table1.comment());
    testProperties(props, table1.properties());
    Assertions.assertEquals(0, table1.partitioning().length);
    Assertions.assertArrayEquals(columns, table1.columns());

    // Test required table properties exception
    Map<String, String> illegalTableProperties =
        new HashMap<String, String>() {
          {
            put("k2", "v2");
          }
        };
    testPropertyException(
        () ->
            tableOperationDispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties are required and must be set");

    // Test reserved table properties exception
    illegalTableProperties.put(COMMENT_KEY, "table comment");
    illegalTableProperties.put(ID_KEY, "gravitino.v1.uidfdsafdsa");
    testPropertyException(
        () ->
            tableOperationDispatcher.createTable(
                tableIdent1, columns, "comment", illegalTableProperties, new Transform[0]),
        "Properties are reserved and cannot be set",
        "comment",
        "gravitino.identifier");

    // Check if the Table entity is stored in the EntityStore
    TableEntity tableEntity = entityStore.get(tableIdent1, TABLE, TableEntity.class);
    Assertions.assertNotNull(tableEntity);
    Assertions.assertEquals("table1", tableEntity.name());

    Assertions.assertFalse(table1.properties().containsKey(ID_KEY));

    Optional<NameIdentifier> ident1 =
        Arrays.stream(tableOperationDispatcher.listTables(tableNs))
            .filter(s -> s.name().equals("table1"))
            .findFirst();
    Assertions.assertTrue(ident1.isPresent());

    // Test when the entity store failed to put the table entity
    doThrow(new IOException()).when(entityStore).put(any(), anyBoolean());
    NameIdentifier tableIdent2 = NameIdentifier.of(tableNs, "table2");
    Table table2 =
        tableOperationDispatcher.createTable(
            tableIdent2, columns, "comment", props, new Transform[0]);

    // Check if the created Schema's field values are correct
    Assertions.assertEquals("table2", table2.name());
    Assertions.assertEquals("comment", table2.comment());
    testProperties(props, table2.properties());

    // Check if the Table entity is stored in the EntityStore
    Assertions.assertFalse(entityStore.exists(tableIdent2, TABLE));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> entityStore.get(tableIdent2, TABLE, TableEntity.class));

    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", table2.auditInfo().creator());
  }

  @Test
  public void testCreateAndLoadTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema51");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent1 = NameIdentifier.of(tableNs, "table11");
    Column[] columns =
        new Column[] {
          TestColumn.builder().withName("col1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table1 =
        tableOperationDispatcher.createTable(
            tableIdent1, columns, "comment", props, new Transform[0]);
    Table loadedTable1 = tableOperationDispatcher.loadTable(tableIdent1);
    Assertions.assertEquals(table1.name(), loadedTable1.name());
    Assertions.assertEquals(table1.comment(), loadedTable1.comment());
    testProperties(table1.properties(), loadedTable1.properties());
    Assertions.assertEquals(0, loadedTable1.partitioning().length);
    Assertions.assertArrayEquals(table1.columns(), loadedTable1.columns());
    // Audit info is gotten from the entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, loadedTable1.auditInfo().creator());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).get(any(), any(), any());
    Table loadedTable2 = tableOperationDispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable2.auditInfo().creator());

    // Case 3: Test if the entity store is failed to get the table entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).get(any(), any(), any());
    Table loadedTable3 = tableOperationDispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable3.auditInfo().creator());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity tableEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("table11")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(tableEntity).when(entityStore).get(any(), any(), any());
    Table loadedTable4 = tableOperationDispatcher.loadTable(tableIdent1);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", loadedTable4.auditInfo().creator());
  }

  @Test
  public void testCreateAndAlterTable() throws IOException {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema61");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaOperationDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table21");
    Column[] columns =
        new Column[] {
          TestColumn.builder().withName("col1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("col2").withType(Types.StringType.get()).build()
        };

    Table table =
        tableOperationDispatcher.createTable(
            tableIdent, columns, "comment", props, new Transform[0]);

    // Test immutable table properties
    TableChange[] illegalChange =
        new TableChange[] {TableChange.setProperty(COMMENT_KEY, "new comment")};
    testPropertyException(
        () -> tableOperationDispatcher.alterTable(tableIdent, illegalChange),
        "Property comment is immutable or reserved, cannot be set");

    TableChange[] changes =
        new TableChange[] {TableChange.setProperty("k3", "v3"), TableChange.removeProperty("k1")};

    Table alteredTable = tableOperationDispatcher.alterTable(tableIdent, changes);
    Assertions.assertEquals(table.name(), alteredTable.name());
    Assertions.assertEquals(table.comment(), alteredTable.comment());
    Map<String, String> expectedProps = ImmutableMap.of("k2", "v2", "k3", "v3");
    testProperties(expectedProps, alteredTable.properties());
    // Audit info is gotten from gravitino entity store
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().creator());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredTable.auditInfo().lastModifier());

    // Case 2: Test if the table entity is not found in the entity store
    reset(entityStore);
    doThrow(new NoSuchEntityException("")).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable2 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable2.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable2.auditInfo().lastModifier());

    // Case 3: Test if the entity store is failed to update the table entity
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable3 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable3.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable3.auditInfo().lastModifier());

    // Case 4: Test if the table entity is not matched
    reset(entityStore);
    TableEntity unmatchedEntity =
        TableEntity.builder()
            .withId(1L)
            .withName("table21")
            .withNamespace(tableNs)
            .withAuditInfo(
                AuditInfo.builder().withCreator("gravitino").withCreateTime(Instant.now()).build())
            .build();
    doReturn(unmatchedEntity).when(entityStore).update(any(), any(), any(), any());
    Table alteredTable4 = tableOperationDispatcher.alterTable(tableIdent, changes);
    // Audit info is gotten from the catalog, not from the entity store
    Assertions.assertEquals("test", alteredTable4.auditInfo().creator());
    Assertions.assertEquals("test", alteredTable4.auditInfo().lastModifier());
  }

  @Test
  public void testCreateAndDropTable() throws IOException {
    NameIdentifier tableIdent = NameIdentifier.of(metalake, catalog, "schema71", "table31");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Column[] columns =
        new Column[] {
          TestColumn.builder().withName("col1").withType(Types.StringType.get()).build(),
          TestColumn.builder().withName("col2").withType(Types.StringType.get()).build()
        };

    tableOperationDispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);

    boolean dropped = tableOperationDispatcher.dropTable(tableIdent);
    Assertions.assertTrue(dropped);

    // Test if the entity store is failed to drop the table entity
    tableOperationDispatcher.createTable(tableIdent, columns, "comment", props, new Transform[0]);
    reset(entityStore);
    doThrow(new IOException()).when(entityStore).delete(any(), any(), anyBoolean());
    Assertions.assertThrows(
        RuntimeException.class, () -> tableOperationDispatcher.dropTable(tableIdent));
  }
}
