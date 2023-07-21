/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.meta.*;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.util.Executable;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityStore {

  public static class InMemoryEntityStore implements EntityStore {
    private final Map<NameIdentifier, Entity> entityMap;

    private EntitySerDe serde;

    private final Lock lock;

    public InMemoryEntityStore() {
      this.entityMap = Maps.newConcurrentMap();
      this.lock = new ReentrantLock();
    }

    @Override
    public void initialize(Config config) throws RuntimeException {}

    @Override
    public void setSerDe(EntitySerDe entitySerDe) {
      this.serde = entitySerDe;
    }

    @Override
    public <E extends Entity & HasIdentifier> List<E> list(Namespace namespace) throws IOException {
      return entityMap.entrySet().stream()
          .filter(e -> e.getKey().namespace().equals(namespace))
          .map(entry -> (E) entry.getValue())
          .collect(Collectors.toList());
    }

    @Override
    public boolean exists(NameIdentifier ident) throws IOException {
      return entityMap.containsKey(ident);
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e)
        throws IOException {
      entityMap.put(ident, e);
    }

    @Override
    public <E extends Entity & HasIdentifier> void putIfNotExists(NameIdentifier ident, E e)
        throws IOException, EntityAlreadyExistsException {
      executeInTransaction(
          () -> {
            if (exists(ident)) {
              throw new EntityAlreadyExistsException("Entity " + ident + " already exists");
            }
            put(ident, e);
            return null;
          });
    }

    @Override
    public <E extends Entity & HasIdentifier> E get(NameIdentifier ident)
        throws NoSuchEntityException, IOException {
      E e = (E) entityMap.get(ident);
      if (e == null) {
        throw new NoSuchEntityException("Entity " + ident + " does not exist");
      }

      return e;
    }

    @Override
    public boolean delete(NameIdentifier ident) throws IOException {
      Entity prev = entityMap.remove(ident);
      return prev != null;
    }

    @Override
    public <R> R executeInTransaction(Executable<R> executable) throws IOException {
      try {
        lock.lock();
        return executable.execute();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void close() throws IOException {
      entityMap.clear();
    }
  }

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

    Metalake retrievedMetalake = store.get(metalake.nameIdentifier());
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog = store.get(catalog.nameIdentifier());
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable = store.get(table.nameIdentifier());
    Assertions.assertEquals(table, retrievedTable);

    store.delete(metalake.nameIdentifier());
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> store.get(metalake.nameIdentifier()));

    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> store.putIfNotExists(catalog.nameIdentifier(), catalog));
  }
}
