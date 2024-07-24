/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSchemaNormalizeDispatcher extends TestOperationDispatcher {
  private static SchemaNormalizeDispatcher schemaNormalizeDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    TestSchemaOperationDispatcher.initialize();
    schemaNormalizeDispatcher =
        new SchemaNormalizeDispatcher(TestSchemaOperationDispatcher.dispatcher, catalogManager);
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
        NameIdentifier.of(metalake, catalog, MetadataObjects.METADATA_OBJECT_RESERVED_NAME);
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
