/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SchemaChange.RemoveProperty;
import com.datastrato.gravitino.rel.SchemaChange.SetProperty;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SchemaHiveIT extends AbstractIT {
  public static String metalakeName;
  public static String catalogName;
  public static String schemaAName;;
  public static String schemaAComment = "schema A comment";
  public static String schemaBName;
  public static String schemaBComment = "schema B comment";

  private static final String provider = "hive";

  @BeforeAll
  private static void start() {
    GravitinoITUtils.hiveConfig();
  }

  @BeforeEach
  private void before() {
    // to isolate each test in it's own space
    metalakeName = GravitinoITUtils.genRandomName("metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaAName = GravitinoITUtils.genRandomName("schemaA");
    schemaBName = GravitinoITUtils.genRandomName("schemaB");

    createSchema();
  }

  @AfterEach
  private void after() {
    dropSchema();
    dropAll();
  }

  @Test
  public void testSchemaExists() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    catalog.asSchemas().loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));
    assertTrue(
        catalog
            .asSchemas()
            .schemaExists(NameIdentifier.of(metalakeName, catalogName, schemaAName)));

    String unknownSchemaName = GravitinoITUtils.genRandomName("schemaB");
    assertFalse(
        catalog
            .asSchemas()
            .schemaExists(NameIdentifier.of(metalakeName, catalogName, unknownSchemaName)));
  }

  @Test
  public void testListSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schema = catalog.asSchemas();
    schema.loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));

    Map<String, String> schemaProps = Maps.newHashMap();
    schemaProps.put("NameA", "ValueA");
    schemaProps.put("NameB", "ValueB");
    schema.createSchema(
        NameIdentifier.of(metalakeName, catalogName, schemaBName), schemaBComment, schemaProps);

    NameIdentifier[] schemas = schema.listSchemas(Namespace.of(metalakeName, catalogName));
    assertEquals(3, schemas.length); // 3 not 2 as the default schema exists

    ArrayList<String> names = new ArrayList<>(3);
    names.add(schemas[0].name());
    names.add(schemas[1].name());
    names.add(schemas[2].name());
    assertTrue(names.contains("default"));
    assertTrue(
        names.contains(schemaAName.toLowerCase())); // NOTE: Hive makes schema name lower case
    assertTrue(
        names.contains(schemaBName.toLowerCase())); // NOTE: Hive makes schema name lower case
  }

  @Test
  public void testListUnknownSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schema = catalog.asSchemas();

    // To get a schema you need a valid catalog but you can then change it this seem odd to me
    assertThrows(
        NoSuchCatalogException.class,
        () -> schema.listSchemas(Namespace.of(metalakeName, "unknown")));
  }

  @Test
  public void testDropSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schema = catalog.asSchemas();
    schema.loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));

    NameIdentifier[] schemas = schema.listSchemas(Namespace.of(metalakeName, catalogName));
    assertEquals(2, schemas.length); // NOTE the default schema exists

    schema.dropSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName), false);

    schemas = schema.listSchemas(Namespace.of(metalakeName, catalogName));
    assertEquals(1, schemas.length); // NOTE the default schema exists

    assertEquals(schemas[0].name(), "default");
  }

  @Test
  public void testDropUnknownSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schema = catalog.asSchemas();

    assertFalse(schema.dropSchema(NameIdentifier.of(metalakeName, catalogName, "unknown"), false));
  }

  @Test
  public void testLoadSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    Schema schema =
        catalog.asSchemas().loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));
    assertEquals(
        schema.name(), schemaAName.toLowerCase()); // NOTE: Hive makes schema name lower case
    assertEquals(schema.comment(), schemaAComment);
    Map<String, String> properties = schema.properties();
    assertEquals("ValueA", properties.get("NameA"));
    assertEquals("ValueB", properties.get("NameB"));
  }

  @Test
  public void testLoadUnknownSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas schema = catalog.asSchemas();
    assertThrows(
        NoSuchSchemaException.class,
        () -> schema.loadSchema(NameIdentifier.of(metalakeName, catalogName, "unknown")));
  }

  @Test
  public void testAlterSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas support = catalog.asSchemas();
    Schema schema = support.loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));
    assertEquals(
        schema.name(), schemaAName.toLowerCase()); // NOTE: Hive makes schema name lower case

    String property = "Extra";
    String value = "important information";
    SetProperty add = (SetProperty) SchemaChange.setProperty(property, value);

    support.alterSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName), add);
    schema = support.loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));
    Map<String, String> properties = schema.properties();
    assertEquals(properties.get(property), value);

    RemoveProperty remove = (RemoveProperty) SchemaChange.removeProperty(property);
    support.alterSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName), remove);
    schema = support.loadSchema(NameIdentifier.of(metalakeName, catalogName, schemaAName));
    properties = schema.properties();
    assertFalse(properties.containsKey(property));
  }

  @Test
  public void testAlterUnknownSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas support = catalog.asSchemas();

    String property = "Extra";
    String value = "important information";
    SetProperty add = (SetProperty) SchemaChange.setProperty(property, value);

    assertThrows(
        NoSuchSchemaException.class,
        () -> support.alterSchema(NameIdentifier.of(metalakeName, catalogName, "unknown"), add));
  }

  public static void createSchema() {
    NameIdentifier metalakeID = NameIdentifier.of(metalakeName);
    GravitinoMetaLake metalake =
        client.createMetalake(metalakeID, "metalake comment", Collections.emptyMap());

    Map<String, String> catalogProps = GravitinoITUtils.hiveConfigProperties();

    Catalog catalog =
        metalake.createCatalog(
            NameIdentifier.of(metalakeName, catalogName),
            Catalog.Type.RELATIONAL,
            provider,
            "catalog comment",
            catalogProps);

    Map<String, String> schemaProps = Maps.newHashMap();
    schemaProps.put("NameA", "ValueA");
    schemaProps.put("NameB", "ValueB");
    catalog
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, catalogName, schemaAName), schemaAComment, schemaProps);
  }

  public static void dropSchema() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));
    SupportsSchemas support = catalog.asSchemas();
    NameIdentifier[] schemas = support.listSchemas(Namespace.of(metalakeName, catalogName));

    for (NameIdentifier schema : schemas) {
      if (!schema.name().equals("default")) {
        support.dropSchema(NameIdentifier.of(metalakeName, catalogName, schema.name()), true);
      }
    }
  }

  public static void dropAll() {
    GravitinoMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Catalog catalog = metalake.loadCatalog(NameIdentifier.of(metalakeName, catalogName));

    if (metalake.catalogExists(NameIdentifier.of(metalakeName, catalogName))) {
      metalake.dropCatalog(NameIdentifier.of(metalakeName, catalogName));
    }
    client.dropMetalake(NameIdentifier.of(metalakeName));
  }
}
