/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.Entity.EntityIdentifier;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaVersion;
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
import java.util.function.Function;
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
    public <E extends Entity & HasIdentifier> List<E> list(
        EntityIdentifier entityIdentifier, Class<E> cl) throws IOException {
      return entityMap.entrySet().stream()
          .filter(
              e -> e.getKey().namespace().equals(entityIdentifier.getNameIdentifier().namespace()))
          .map(entry -> (E) entry.getValue())
          .collect(Collectors.toList());
    }

    @Override
    public boolean exists(EntityIdentifier entityIdentifier) throws IOException {
      return entityMap.containsKey(entityIdentifier.getNameIdentifier());
    }

    @Override
    public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
        throws IOException, EntityAlreadyExistsException {
      NameIdentifier ident = e.nameIdentifier();
      if (overwritten) {
        entityMap.put(ident, e);
      } else {
        executeInTransaction(
            () -> {
              if (exists(EntityIdentifier.of(e.nameIdentifier(), e.type()))) {
                throw new EntityAlreadyExistsException("Entity " + ident + " already exists");
              }
              entityMap.put(ident, e);
              return null;
            });
      }
    }

    @Override
    public <E extends Entity & HasIdentifier> E update(
        EntityIdentifier entityIdentifier, Class<E> type, Function<E, E> updater)
        throws IOException, NoSuchEntityException {
      return executeInTransaction(
          () -> {
            NameIdentifier ident = entityIdentifier.getNameIdentifier();
            E e = (E) entityMap.get(ident);
            if (e == null) {
              throw new NoSuchEntityException("Entity " + ident + " does not exist");
            }

            E newE = updater.apply(e);
            NameIdentifier newIdent = NameIdentifier.of(newE.namespace(), newE.name());
            if (!newIdent.equals(ident)) {
              delete(entityIdentifier);
            }
            entityMap.put(newIdent, newE);
            return newE;
          });
    }

    @Override
    public <E extends Entity & HasIdentifier> E get(EntityIdentifier entityIdentifier, Class<E> cl)
        throws NoSuchEntityException, IOException {
      E e = (E) entityMap.get(entityIdentifier.getNameIdentifier());
      if (e == null) {
        throw new NoSuchEntityException(
            "Entity " + entityIdentifier.getNameIdentifier() + " does not exist");
      }

      return e;
    }

    @Override
    public boolean delete(EntityIdentifier entityIdentifier) throws IOException {
      Entity prev = entityMap.remove(entityIdentifier.getNameIdentifier());
      return prev != null;
    }

    @Override
    public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
        throws E, IOException {
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

    store.put(metalake);
    store.put(catalog);
    store.put(table);

    Metalake retrievedMetalake =
        store.get(
            EntityIdentifier.of(metalake.nameIdentifier(), EntityType.METALAKE),
            BaseMetalake.class);
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog =
        store.get(
            EntityIdentifier.of(catalog.nameIdentifier(), EntityType.CATALOG), CatalogEntity.class);
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable =
        store.get(EntityIdentifier.of(table.nameIdentifier(), EntityType.TABLE), TestTable.class);
    Assertions.assertEquals(table, retrievedTable);

    store.delete(EntityIdentifier.of(metalake.nameIdentifier(), EntityType.METALAKE));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            store.get(
                EntityIdentifier.of(metalake.nameIdentifier(), EntityType.METALAKE),
                BaseMetalake.class));

    Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
  }
}
