/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;
import static com.datastrato.graviton.Configs.ENTITY_STORE;
import static com.datastrato.graviton.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.graviton.Entity.EntityType.SCHEMA;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.GravitonEnv;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HiveSchemaTest extends MiniHiveMetastoreService {

  private static final String ROCKS_DB_STORE_PATH = "/tmp/graviton/test_hive_schema";

  private static EntityStore store;

  @BeforeAll
  private static void setup() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);
    Mockito.when(config.get(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS))
        .thenReturn(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS.getDefaultValue());

    GravitonEnv.getInstance().initialize(config);
    store = GravitonEnv.getInstance().entityStore();
  }

  @AfterAll
  private static void tearDown() {
    try {
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    } catch (Exception e) {
      // Ignore
    }
  }

  HiveCatalog initHiveCatalog() {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    metastore.hiveConf().iterator().forEachRemaining(e -> conf.put(e.getKey(), e.getValue()));
    HiveCatalog hiveCatalog = new HiveCatalog().withCatalogConf(conf).withCatalogEntity(entity);

    return hiveCatalog;
  }

  @Test
  public void testCreateHiveSchema() throws IOException {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    Schema schema = hiveCatalog.asSchemas().createSchema(ident, comment, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(comment, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(hiveCatalog.asSchemas().schemaExists(ident));

    NameIdentifier[] idents = hiveCatalog.asSchemas().listSchemas(ident.namespace());
    Assertions.assertTrue(Arrays.asList(idents).contains(ident));
    Assertions.assertTrue(store.exists(ident, SCHEMA));

    Schema loadedSchema = hiveCatalog.asSchemas().loadSchema(ident);
    Assertions.assertEquals(schema.auditInfo(), loadedSchema.auditInfo());

    // Test illegal identifier
    NameIdentifier ident1 = NameIdentifier.of("metalake", hiveCatalog.name());
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              hiveCatalog.asSchemas().createSchema(ident1, comment, properties);
            });
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot support invalid namespace in Hive Metastore"));

    // Test schema already exists
    exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              hiveCatalog.asSchemas().createSchema(ident, comment, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists in Graviton store"));
  }

  @Test
  public void testAlterSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    Schema createdSchema = hiveCatalog.asSchemas().createSchema(ident, comment, properties);
    Assertions.assertTrue(hiveCatalog.asSchemas().schemaExists(ident));

    Map<String, String> properties1 = hiveCatalog.asSchemas().loadSchema(ident).properties();
    Assertions.assertEquals("val1", properties1.get("key1"));
    Assertions.assertEquals("val2", properties1.get("key2"));

    hiveCatalog
        .asSchemas()
        .alterSchema(
            ident,
            SchemaChange.removeProperty("key1"),
            SchemaChange.setProperty("key2", "val2-alter"));
    Schema alteredSchema = hiveCatalog.asSchemas().loadSchema(ident);
    Map<String, String> properties2 = alteredSchema.properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    Assertions.assertEquals(
        createdSchema.auditInfo().creator(), alteredSchema.auditInfo().creator());
    Assertions.assertEquals(
        createdSchema.auditInfo().createTime(), alteredSchema.auditInfo().createTime());
    Assertions.assertNotNull(alteredSchema.auditInfo().lastModifier());
    Assertions.assertNotNull(alteredSchema.auditInfo().lastModifiedTime());

    hiveCatalog
        .asSchemas()
        .alterSchema(
            ident,
            SchemaChange.setProperty("key3", "val3"),
            SchemaChange.setProperty("key4", "val4"));
    Schema alteredSchema1 = hiveCatalog.asSchemas().loadSchema(ident);
    Map<String, String> properties3 = alteredSchema1.properties();
    Assertions.assertEquals("val3", properties3.get("key3"));
    Assertions.assertEquals("val4", properties3.get("key4"));

    Assertions.assertEquals(
        createdSchema.auditInfo().creator(), alteredSchema1.auditInfo().creator());
    Assertions.assertEquals(
        createdSchema.auditInfo().createTime(), alteredSchema1.auditInfo().createTime());
    Assertions.assertNotNull(alteredSchema1.auditInfo().lastModifier());
    Assertions.assertNotNull(alteredSchema1.auditInfo().lastModifiedTime());
  }

  @Test
  public void testDropSchema() throws IOException {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    hiveCatalog.asSchemas().createSchema(ident, comment, properties);
    Assertions.assertTrue(hiveCatalog.asSchemas().schemaExists(ident));
    hiveCatalog.asSchemas().dropSchema(ident, true);
    Assertions.assertFalse(hiveCatalog.asSchemas().schemaExists(ident));
    Assertions.assertFalse(store.exists(ident, SCHEMA));
  }
}
