package com.datastrato.graviton;

import com.datastrato.graviton.json.TestColumn;
import com.datastrato.graviton.json.TestTable;
import com.datastrato.graviton.meta.*;
import com.datastrato.graviton.meta.catalog.rel.Column;
import com.datastrato.graviton.meta.catalog.rel.Table;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntityStore {

  public static class InMemoryEntityStore implements EntityStore {
    private final Map<NameIdentifier, Entity> entityMap;

    public InMemoryEntityStore() {
      this.entityMap = Maps.newHashMap();
    }

    @Override
    public void initialize(Config config) throws RuntimeException {}

    @Override
    public synchronized <E extends Entity & HasIdentifier> void storeEntity(E entity)
        throws IOException {
      entityMap.put(entity.nameIdentifier(), entity);
    }

    @Override
    public synchronized <E extends Entity & HasIdentifier> E retrieveEntity(HasIdentifier ident)
        throws NoSuchEntityException, IOException {
      if (entityMap.containsKey(ident.nameIdentifier())) {
        return (E) entityMap.get(ident.nameIdentifier());
      } else {
        throw new NoSuchEntityException("Entity " + ident.nameIdentifier() + " does not exist");
      }
    }

    @Override
    public void close() throws IOException {}
  }

  @Test
  public void testEntityStoreAndRetrieve() throws Exception {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    Lakehouse lakehouse =
        new Lakehouse.Builder()
            .withId(1L)
            .withName("lakehouse")
            .withAuditInfo(auditInfo)
            .withVersion(SchemaVersion.V_0_1)
            .build();

    BaseCatalog catalog =
        new TestCatalog.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("lakehouse"))
            .withType(TestCatalog.Type.RELATIONAL)
            .withLakehouseId(1L)
            .withAuditInfo(auditInfo)
            .build();

    Column column = new TestColumn("column", "comment", TypeCreator.NULLABLE.I8, auditInfo);

    Table table =
        new TestTable(
            "table",
            Namespace.of("lakehouse", "catalog", "db"),
            "comment",
            Maps.newHashMap(),
            auditInfo,
            new Column[] {column});

    InMemoryEntityStore store = new InMemoryEntityStore();
    store.storeEntity(lakehouse);
    store.storeEntity(catalog);
    store.storeEntity(table);
    store.storeEntity(column);

    Lakehouse retrievedLakehouse = store.retrieveEntity(lakehouse);
    Assertions.assertEquals(lakehouse, retrievedLakehouse);

    BaseCatalog retrievedCatalog = store.retrieveEntity(catalog);
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable = store.retrieveEntity(table);
    Assertions.assertEquals(table, retrievedTable);

    Column retrievedColumn = store.retrieveEntity(column);
    Assertions.assertEquals(column, retrievedColumn);
  }
}
