/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSchemaStandardizedDispatcher extends TestSchemaOperationDispatcher {
  private static SchemaStandardizedDispatcher schemaStandardizedDispatcher;

  @BeforeAll
  public static void initialize() throws IOException {
    TestSchemaOperationDispatcher.initialize();
    schemaStandardizedDispatcher = new SchemaStandardizedDispatcher(dispatcher);
  }

  @Test
  public void testNameCaseInsensitive() {
    // test case-insensitive in creation
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, "schemaNAME");
    Schema createdSchema =
        schemaStandardizedDispatcher.createSchema(
            schemaIdent, null, ImmutableMap.of("k1", "v1", "k2", "v2"));
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), createdSchema.name());

    // test case-insensitive in loading
    Schema loadSchema = schemaStandardizedDispatcher.loadSchema(schemaIdent);
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), loadSchema.name());

    // test case-insensitive in listing
    NameIdentifier[] schemas =
        schemaStandardizedDispatcher.listSchemas(Namespace.of(metalake, catalog));
    Arrays.stream(schemas).forEach(s -> Assertions.assertEquals(s.name().toLowerCase(), s.name()));

    // test case-insensitive in altering
    Schema alteredSchema =
        schemaStandardizedDispatcher.alterSchema(
            schemaIdent, SchemaChange.setProperty("k2", "v2"), SchemaChange.removeProperty("k1"));
    Assertions.assertEquals(schemaIdent.name().toLowerCase(), alteredSchema.name());

    // test case-insensitive in dropping
    Assertions.assertTrue(
        schemaStandardizedDispatcher.dropSchema(
            NameIdentifier.of(schemaIdent.namespace(), schemaIdent.name().toLowerCase()), false));
    Assertions.assertFalse(schemaStandardizedDispatcher.schemaExists(schemaIdent));
  }
}
