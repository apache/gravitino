/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveSchema extends MiniHiveMetastoreService {

  private HiveCatalog initHiveCatalog() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(HiveCatalog.Type.RELATIONAL)
            .withProvider("hive")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(METASTORE_URIS, hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
    conf.put(
        CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
        hiveConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    conf.put(
        CATALOG_BYPASS_PREFIX
            + HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname,
        hiveConf.get(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname));

    conf.put(
        CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.HIVE_IN_TEST.varname,
        hiveConf.get(HiveConf.ConfVars.HIVE_IN_TEST.varname));
    metastore.hiveConf().iterator().forEachRemaining(e -> conf.put(e.getKey(), e.getValue()));
    HiveCatalog hiveCatalog = new HiveCatalog().withCatalogConf(conf).withCatalogEntity(entity);

    return hiveCatalog;
  }

  @Test
  public void testCreateHiveSchema() {
    HiveCatalog hiveCatalog = initHiveCatalog();

    NameIdentifier ident = NameIdentifier.of("metalake", hiveCatalog.name(), genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";
    SupportsSchemas schemas = hiveCatalog.asSchemas();

    Schema schema = schemas.createSchema(ident, comment, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(comment, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(schemas.schemaExists(ident));

    NameIdentifier[] idents = schemas.listSchemas(ident.namespace());
    Assertions.assertTrue(Arrays.asList(idents).contains(ident));

    Schema loadedSchema = schemas.loadSchema(ident);
    Assertions.assertEquals(schema.auditInfo().creator(), loadedSchema.auditInfo().creator());
    Assertions.assertNull(loadedSchema.auditInfo().createTime());
    Assertions.assertEquals("val1", loadedSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadedSchema.properties().get("key2"));

    // Test schema already exists
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              schemas.createSchema(ident, comment, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists in Hive Metastore"));
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
    Assertions.assertNull(alteredSchema.auditInfo().createTime());
    Assertions.assertNull(alteredSchema.auditInfo().lastModifier());
    Assertions.assertNull(alteredSchema.auditInfo().lastModifiedTime());

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
    Assertions.assertNull(alteredSchema1.auditInfo().createTime());
    Assertions.assertNull(alteredSchema1.auditInfo().lastModifier());
    Assertions.assertNull(alteredSchema1.auditInfo().lastModifiedTime());
  }

  @Test
  public void testDropSchema() {
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
  }
}
