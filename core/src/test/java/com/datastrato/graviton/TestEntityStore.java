/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton;

import com.datastrato.graviton.meta.*;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
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
    store.storeEntity(metalake);
    store.storeEntity(catalog);
    store.storeEntity(table);
    store.storeEntity(column);

    Metalake retrievedMetalake = store.retrieveEntity(metalake);
    Assertions.assertEquals(metalake, retrievedMetalake);

    CatalogEntity retrievedCatalog = store.retrieveEntity(catalog);
    Assertions.assertEquals(catalog, retrievedCatalog);

    Table retrievedTable = store.retrieveEntity(table);
    Assertions.assertEquals(table, retrievedTable);

    Column retrievedColumn = store.retrieveEntity(column);
    Assertions.assertEquals(column, retrievedColumn);
  }
}
