/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.meta.*;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.store.memory.InMemoryEntityStore;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityStore {

  @Test
  public void testEntityStoreAndRetrieve() throws Exception {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withId(1L)
            .withName("metalake")
            .withAuditInfo(auditInfo)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    CatalogEntity catalog =
        new CatalogEntity.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(TestCatalog.Type.RELATIONAL)
            .withMetalakeId(1L)
            .withAuditInfo(auditInfo)
            .build();

    TestColumn column = new TestColumn("column", "comment", TypeCreator.NULLABLE.I8);

    TestTable table =
        new TestTable(
            "table",
            Namespace.of("metalake", "catalog", "db"),
            "comment",
            Maps.newHashMap(),
            auditInfo,
            new Column[] {column});

    InMemoryEntityStore store = new InMemoryEntityStore();
    store.initialize(Mockito.mock(Config.class));
    store.setSerDe(Mockito.mock(EntitySerDe.class));

    store.put(metalake.nameIdentifier(), metalake);
    store.put(catalog.nameIdentifier(), catalog);
    store.put(table.nameIdentifier(), table);

    Metalake retrievedMetalake = store.get(metalake.nameIdentifier(), BaseMetalake.class);
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog = store.get(catalog.nameIdentifier(), CatalogEntity.class);
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable = store.get(table.nameIdentifier(), TestTable.class);
    Assertions.assertEquals(table, retrievedTable);

    store.delete(metalake.nameIdentifier());
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(metalake.nameIdentifier(), BaseMetalake.class));

    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> store.put(catalog.nameIdentifier(), catalog, false));
  }
}
