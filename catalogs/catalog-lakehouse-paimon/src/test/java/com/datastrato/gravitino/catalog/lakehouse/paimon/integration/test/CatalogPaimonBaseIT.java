/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.SupportsSchemas;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

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
  private org.apache.paimon.catalog.Catalog paimonCatalog;

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

  @Test
  void testPaimonSchemaOperations() throws DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // list schema check.
    Set<String> schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(schemaName));
    List<String> paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertTrue(paimonDatabaseNames.contains(schemaName));

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key1", "val1");
    schemaProperties.put("key2", "val2");
    schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(testSchemaName));
    paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertTrue(paimonDatabaseNames.contains(testSchemaName));

    // load schema check.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    // database properties is empty for Paimon FilesystemCatalog.
    Assertions.assertTrue(schema.properties().isEmpty());
    Assertions.assertTrue(paimonCatalog.loadDatabaseProperties(schemaIdent.name()).isEmpty());

    Map<String, String> emptyMap = Collections.emptyMap();
    Assertions.assertThrows(
        SchemaAlreadyExistsException.class,
        () -> schemas.createSchema(schemaIdent.name(), schema_comment, emptyMap));

    // alter schema check.
    // alter schema operation is unsupported.
    Assertions.assertThrowsExactly(
        UnsupportedOperationException.class,
        () -> schemas.alterSchema(schemaIdent.name(), SchemaChange.setProperty("k1", "v1")));

    // drop schema check.
    schemas.dropSchema(schemaIdent.name(), false);
    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> schemas.loadSchema(schemaIdent.name()));
    Assertions.assertThrows(
        DatabaseNotExistException.class,
        () -> {
          paimonCatalog.loadDatabaseProperties(schemaIdent.name());
        });

    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    Assertions.assertFalse(schemas.dropSchema(schemaIdent.name(), false));
    Assertions.assertFalse(schemas.dropSchema("no-exits", false));

    // list schema check.
    schemaNames = new HashSet<>(Arrays.asList(schemas.listSchemas()));
    Assertions.assertTrue(schemaNames.contains(schemaName));
    Assertions.assertFalse(schemaNames.contains(testSchemaName));
    paimonDatabaseNames = paimonCatalog.listDatabases();
    Assertions.assertTrue(paimonDatabaseNames.contains(schemaName));
    Assertions.assertFalse(paimonDatabaseNames.contains(testSchemaName));
  }

  private void clearTableAndSchema() {
    if (catalog.asSchemas().schemaExists(schemaName)) {
      catalog.asSchemas().dropSchema(schemaName, true);
    }
  }

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

    String type =
        catalogProperties
            .get(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND)
            .toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type), "Paimon Catalog backend type can not be null or empty.");
    catalogProperties.put(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE, type);
    paimonCatalog = CatalogUtils.loadCatalogBackend(new PaimonConfig(catalogProperties));
  }

  private void createSchema() {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, schemaName);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("key1", "val1");
    prop.put("key2", "val2");

    Schema createdSchema = catalog.asSchemas().createSchema(ident.name(), schema_comment, prop);
    // database properties is empty for Paimon FilesystemCatalog.
    Schema loadSchema = catalog.asSchemas().loadSchema(ident.name());
    Assertions.assertEquals(createdSchema.name(), loadSchema.name());
    Assertions.assertTrue(loadSchema.properties().isEmpty());
  }
}
