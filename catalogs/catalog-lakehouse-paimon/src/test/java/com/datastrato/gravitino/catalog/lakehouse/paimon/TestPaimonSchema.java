/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonSchema {

  private static final String META_LAKE_NAME = "metalake";

  private static final String COMMENT_VALUE = "comment";

  private static AuditInfo AUDIT_INFO =
      AuditInfo.builder().withCreator("testPaimonUser").withCreateTime(Instant.now()).build();

  private void cleanUp(PaimonCatalogOperations paimonCatalogOperations, NameIdentifier ident) {
    Assertions.assertTrue(paimonCatalogOperations.dropSchema(ident, false));
    Assertions.assertThrowsExactly(
        NoSuchSchemaException.class, () -> paimonCatalogOperations.loadSchema(ident));
  }

  @Test
  public void testCreateIcebergSchema() {
    PaimonCatalog paimonCatalog = initPaimonCatalog("testCreatePaimonSchema");
    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) paimonCatalog.ops();

    NameIdentifier ident =
        NameIdentifier.of("metalake", paimonCatalog.name(), "test_paimon_schema");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    paimonCatalogOperations.dropSchema(ident, false);

    Schema schema = paimonCatalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertEquals(ident.name(), schema.name());
    Assertions.assertEquals(COMMENT_VALUE, schema.comment());
    Assertions.assertEquals(properties, schema.properties());

    Assertions.assertTrue(paimonCatalogOperations.schemaExists(ident));

    Set<String> names =
        Arrays.stream(paimonCatalogOperations.listSchemas(ident.namespace()))
            .map(NameIdentifier::name)
            .collect(Collectors.toSet());
    Assertions.assertTrue(names.contains(ident.name()));

    // Test schema already exists
    Throwable exception =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> {
              paimonCatalogOperations.createSchema(ident, COMMENT_VALUE, properties);
            });
    Assertions.assertTrue(exception.getMessage().contains("already exists"));

    // clean up
    cleanUp(paimonCatalogOperations, ident);
  }

  @Test
  public void testListSchema() {
    PaimonCatalog paimonCatalog = initPaimonCatalog("testListPaimonSchema");
    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) paimonCatalog.ops();
    NameIdentifier ident =
        NameIdentifier.of("metalake", paimonCatalog.name(), "test_paimon_schema");

    paimonCatalogOperations.dropSchema(ident, false);
    paimonCatalogOperations.createSchema(ident, COMMENT_VALUE, Maps.newHashMap());

    NameIdentifier[] schemas = paimonCatalogOperations.listSchemas(ident.namespace());
    Assertions.assertEquals(1, schemas.length);
    Assertions.assertEquals(ident.name(), schemas[0].name());
    Assertions.assertEquals(ident.namespace(), schemas[0].namespace());

    // clean up
    cleanUp(paimonCatalogOperations, ident);
  }

  @Test
  public void testAlterSchema() {
    PaimonCatalog paimonCatalog = initPaimonCatalog("testListPaimonSchema");
    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) paimonCatalog.ops();

    NameIdentifier ident =
        NameIdentifier.of("metalake", paimonCatalog.name(), "test_paimon_schema");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    paimonCatalogOperations.dropSchema(ident, false);
    PaimonSchema paimonSchema =
        paimonCatalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(paimonCatalogOperations.schemaExists(ident));
    properties.forEach(
        (k, v) -> Assertions.assertEquals(v, paimonSchema.toPaimonSchema().getValue().get(k)));

    // schema properties of FilesystemCatalog is empty when loadDatabase.
    Map<String, String> properties1 = paimonCatalogOperations.loadSchema(ident).properties();
    Assertions.assertEquals(0, properties1.size());

    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> paimonCatalogOperations.alterSchema(ident, SchemaChange.removeProperty("key1")));

    // clean up
    cleanUp(paimonCatalogOperations, ident);
  }

  @Test
  public void testDropSchema() {
    PaimonCatalog paimonCatalog = initPaimonCatalog("testListPaimonSchema");
    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) paimonCatalog.ops();

    NameIdentifier ident =
        NameIdentifier.of("metalake", paimonCatalog.name(), "test_paimon_schema");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    paimonCatalogOperations.dropSchema(ident, false);
    paimonCatalogOperations.createSchema(ident, COMMENT_VALUE, properties);
    Assertions.assertTrue(paimonCatalogOperations.schemaExists(ident));
    Assertions.assertTrue(paimonCatalogOperations.dropSchema(ident, false));
    Assertions.assertFalse(paimonCatalogOperations.schemaExists(ident));

    Assertions.assertFalse(paimonCatalogOperations.dropSchema(ident, false));
  }

  private PaimonCatalog initPaimonCatalog(String name) {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(name)
            .withNamespace(Namespace.of(META_LAKE_NAME))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(AUDIT_INFO)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, "/tmp/paimon_catalog_warehouse");
    return new PaimonCatalog().withCatalogConf(conf).withCatalogEntity(entity);
  }
}
