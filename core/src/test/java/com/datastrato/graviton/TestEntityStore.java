/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.meta.SchemaVersion;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.Distribution.DistributionMethod;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SortOrder.Direction;
import com.datastrato.graviton.rel.SortOrder.NullOrder;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.datastrato.graviton.utils.Executable;
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
        Namespace namespace, Class<E> cl, EntityType entityType) throws IOException {
      return entityMap.entrySet().stream()
          .filter(e -> e.getKey().namespace().equals(namespace))
          .map(entry -> (E) entry.getValue())
          .collect(Collectors.toList());
    }

    @Override
    public boolean exists(NameIdentifier nameIdentifier, EntityType type) throws IOException {
      return entityMap.containsKey(nameIdentifier);
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
              if (exists(e.nameIdentifier(), e.type())) {
                throw new EntityAlreadyExistsException("Entity " + ident + " already exists");
              }
              entityMap.put(ident, e);
              return null;
            });
      }
    }

    @Override
    public <E extends Entity & HasIdentifier> E update(
        NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
        throws IOException, NoSuchEntityException {
      return executeInTransaction(
          () -> {
            E e = (E) entityMap.get(ident);
            if (e == null) {
              throw new NoSuchEntityException("Entity " + ident + " does not exist");
            }

            E newE = updater.apply(e);
            NameIdentifier newIdent = NameIdentifier.of(newE.namespace(), newE.name());
            if (!newIdent.equals(ident)) {
              delete(ident, entityType);
            }
            entityMap.put(newIdent, newE);
            return newE;
          });
    }

    @Override
    public <E extends Entity & HasIdentifier> E get(
        NameIdentifier ident, EntityType entityType, Class<E> cl)
        throws NoSuchEntityException, IOException {
      E e = (E) entityMap.get(ident);
      if (e == null) {
        throw new NoSuchEntityException("Entity " + ident + " does not exist");
      }

      return e;
    }

    @Override
    public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade)
        throws IOException {
      Entity prev = entityMap.remove(ident);
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
            .withAuditInfo(auditInfo)
            .build();

    TestColumn column =
        new TestColumn.Builder()
            .withName("column")
            .withComment("comment")
            .withType(TypeCreator.NULLABLE.I8)
            .build();

    Distribution distribution =
        Distribution.builder()
            .distNum(10)
            .transforms(new Transform[] {Transforms.field(new String[] {"column"})})
            .distMethod(DistributionMethod.EVEN)
            .build();

    SortOrder[] sortOrders =
        new SortOrder[] {
          SortOrder.builder()
              .nullOrder(NullOrder.FIRST)
              .direction(Direction.DESC)
              .transform(Transforms.field(new String[] {"column"}))
              .build()
        };

    TestTable table =
        new TestTable.Builder()
            .withId(1L)
            .withName("table")
            .withNameSpace(Namespace.of("metalake", "catalog", "db"))
            .withComment("comment")
            .withProperties(Maps.newHashMap())
            .withColumns(new Column[] {column})
            .withAuditInfo(auditInfo)
            .withDistribution(distribution)
            .withSortOrders(sortOrders)
            .build();

    InMemoryEntityStore store = new InMemoryEntityStore();
    store.initialize(Mockito.mock(Config.class));
    store.setSerDe(Mockito.mock(EntitySerDe.class));

    store.put(metalake);
    store.put(catalog);
    store.put(table);

    Metalake retrievedMetalake =
        store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class);
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog =
        store.get(catalog.nameIdentifier(), EntityType.CATALOG, CatalogEntity.class);
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable = store.get(table.nameIdentifier(), EntityType.TABLE, TestTable.class);
    Assertions.assertEquals(table, retrievedTable);

    store.delete(metalake.nameIdentifier(), EntityType.METALAKE);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> store.get(metalake.nameIdentifier(), EntityType.METALAKE, BaseMetalake.class));

    Assertions.assertThrows(EntityAlreadyExistsException.class, () -> store.put(catalog, false));
    store.close();
  }
}
