/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSchemaNormalizeDispatcher extends TestOperationDispatcher {
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestSchemaOperationDispatcher.initialize();
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(TestSchemaOperationDispatcher.dispatcher);
  }

  @Test
  public void testNameCaseInsensitive() {
    // test case-insensitive in creation
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schemaNAME");
    Schema createdSchema =
        schemaNormalizeDispatcher.createSchema(
            schemaIdent, null, ImmutableMap.of("k1", "v1", "k2", "v2"));
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), createdSchema.name());

    // test case-insensitive in loading
    Schema loadSchema = schemaNormalizeDispatcher.loadSchema(schemaIdent);
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), loadSchema.name());

    // test case-insensitive in listing
    NameIdentifier[] schemas =
        schemaNormalizeDispatcher.listSchemas(Namespace.of(metalake, catalog));
    Arrays.stream(schemas).forEach(s -> Assertions.assertEquals(s.name().toLowerCase(), s.name()));

    // test case-insensitive in altering
    Schema alteredSchema =
        schemaNormalizeDispatcher.alterSchema(
            schemaIdent, SchemaChange.setProperty("k2", "v2"), SchemaChange.removeProperty("k1"));
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), alteredSchema.name());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        schemaNormalizeDispatcher.dropSchema(
            NameIdentifier.of(schemaIdent.namespace(), schemaIdent.name().toLowerCase()), false));
    Assertions.assertFalse(schemaNormalizeDispatcher.schemaExists(schemaIdent));
  }

  @Test
  public void testNameSpec() {
    NameIdentifier schemaIdent1 =
        NameIdentifier.of(metalake, catalog, SECURABLE_ENTITY_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schemaNormalizeDispatcher.createSchema(schemaIdent1, null, null));
    Assertions.assertEquals(
        "The SCHEMA name '*' is reserved. Illegal name: *", exception.getMessage());

    NameIdentifier schemaIdent2 = NameIdentifier.of(metalake, catalog, "a?");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> schemaNormalizeDispatcher.createSchema(schemaIdent2, null, null));
    Assertions.assertEquals(
        "The SCHEMA name 'a?' is illegal. Illegal name: a?", exception.getMessage());
  }
}
