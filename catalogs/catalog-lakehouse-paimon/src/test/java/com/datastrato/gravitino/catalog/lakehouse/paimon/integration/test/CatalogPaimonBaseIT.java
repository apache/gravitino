/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.SupportsSchemas;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class CatalogPaimonBaseIT extends AbstractIT {

  protected static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String provider = "lakehouse-paimon";
  private static final String catalog_comment = "catalog_comment";
  private static final String schema_comment = "schema_comment";
  private String metalakeName = GravitinoITUtils.genRandomName("paimon_it_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("paimon_it_catalog");
  private String schemaName = GravitinoITUtils.genRandomName("paimon_it_schema");
  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  public void startup() {
    containerSuite.startHiveContainer();
    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() {
    clearTableAndSchema();
    metalake.dropCatalog(catalogName);
    client.dropMetalake(metalakeName);
  }

  @AfterEach
  private void resetSchema() {
    clearTableAndSchema();
    createSchema();
  }

  protected abstract Map<String, String> initPaimonCatalogProperties();

  private void clearTableAndSchema() {}

  private void createMetalake() {
    GravitinoMetalake createdMetalake =
        client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = initPaimonCatalogProperties();
    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, provider, catalog_comment, catalogProperties);
    Catalog loadCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadCatalog);

    catalog = loadCatalog;
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> catalog.asSchemas().createSchema(ident.name(), schema_comment, prop));
  }

  @Test
  void testOperationPaimonSchema() {
    SupportsSchemas schemas = catalog.asSchemas();

    // list schema check.
    Assertions.assertThrowsExactly(UnsupportedOperationException.class, schemas::listSchemas);

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema");
    final NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, Collections.emptyMap()));

    // alter schema check.
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> schemas.alterSchema(schemaIdent.name(), SchemaChange.setProperty("k1", "v1")));

    // alter schema check.
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class, () -> schemas.dropSchema(schemaIdent.name(), true));
  }
}
