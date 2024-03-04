/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
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
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergSchema {

  private static final String META_LAKE_NAME = "metalake";

  private static final String COMMENT_VALUE = "comment";

  private static AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("testIcebergUser").withCreateTime(Instant.now()).build();

  @Test
  public void testCreateIcebergSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testCreateIcebergSchema");

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    Schema schema = icebergCatalog.asSchemas().createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(COMMENT_VALUE, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(icebergCatalog.asSchemas().schemaExists(ident));

    Set<String> names =
        Arrays.stream(icebergCatalog.asSchemas().listSchemas(ident.namespace()))
            .map(NameIdentifier::name)
            .collect(Collectors.toSet());
    Assertions.assertTrue(names.contains(ident.name()));

    // Test schema already exists
    SupportsSchemas schemas = icebergCatalog.asSchemas();
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              schemas.createSchema(ident, COMMENT_VALUE, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists"));
  }

  @Test
  public void testAlterSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testAlterSchema");

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    icebergCatalog.asSchemas().createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(icebergCatalog.asSchemas().schemaExists(ident));

    Map<String, String> properties1 = icebergCatalog.asSchemas().loadSchema(ident).properties();
    Assertions.assertEquals("val1", properties1.get("key1"));
    Assertions.assertEquals("val2", properties1.get("key2"));

    icebergCatalog
        .asSchemas()
        .alterSchema(
            ident,
            SchemaChange.removeProperty("key1"),
            SchemaChange.setProperty("key2", "val2-alter"));
    Schema alteredSchema = icebergCatalog.asSchemas().loadSchema(ident);
    Map<String, String> properties2 = alteredSchema.properties();
    Assertions.assertFalse(properties2.containsKey("key1"));
    Assertions.assertEquals("val2-alter", properties2.get("key2"));

    icebergCatalog
        .asSchemas()
        .alterSchema(
            ident,
            SchemaChange.setProperty("key3", "val3"),
            SchemaChange.setProperty("key4", "val4"));
    Schema alteredSchema1 = icebergCatalog.asSchemas().loadSchema(ident);
    Map<String, String> properties3 = alteredSchema1.properties();
    Assertions.assertEquals("val3", properties3.get("key3"));
    Assertions.assertEquals("val4", properties3.get("key4"));
  }

  @Test
  public void testDropSchema() {
    IcebergCatalog icebergCatalog = initIcebergCatalog("testDropSchema");

    NameIdentifier ident = NameIdentifier.of("metalake", icebergCatalog.name(), "test");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    icebergCatalog.asSchemas().createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(icebergCatalog.asSchemas().schemaExists(ident));
    icebergCatalog.asSchemas().dropSchema(ident, false);
    Assertions.assertFalse(icebergCatalog.asSchemas().schemaExists(ident));

    Assertions.assertFalse(icebergCatalog.asSchemas().dropSchema(ident, false));

    SupportsSchemas schemas = icebergCatalog.asSchemas();
    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> schemas.dropSchema(ident, true));
    Assertions.assertTrue(
        exception.getMessage().contains("Iceberg does not support cascading delete operations"));
  }

  @Test
  void testSchemaProperty() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations(entity)) {
      ops.initialize(conf);
      Map<String, String> map = Maps.newHashMap();
      map.put(IcebergSchemaPropertiesMetadata.COMMENT, "test");
      PropertiesMetadata metadata = ops.schemaPropertiesMetadata();

      IllegalArgumentException illegalArgumentException =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> {
                metadata.validatePropertyForCreate(map);
              });
      Assertions.assertTrue(
          illegalArgumentException.getMessage().contains(IcebergSchemaPropertiesMetadata.COMMENT));
    }
  }

  private IcebergCatalog initIcebergCatalog(String name) {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(name)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(AUDIT_INFO)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    return new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
  }
}
