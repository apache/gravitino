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
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDefaultTableLocationProvider {

  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.of("metalake", "catalog", "schema1", "table1");

  @Test
  void testTableLocationWins() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/catalog");
    Schema schema = mockSchema(ImmutableMap.of(Schema.PROPERTY_LOCATION, "/tmp/schema"));

    String location =
        provider.provisionTableLocation(
            context(
                schema,
                ImmutableMap.of(Table.PROPERTY_LOCATION, "/tmp/explicit"),
                catalogProperties));

    // The table's own location is used as-is, the table name is not appended.
    Assertions.assertEquals("/tmp/explicit/", location);
  }

  @Test
  void testFallBackToSchemaLocation() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/catalog");
    Schema schema = mockSchema(ImmutableMap.of(Schema.PROPERTY_LOCATION, "/tmp/schema"));

    String location =
        provider.provisionTableLocation(context(schema, ImmutableMap.of(), catalogProperties));

    Assertions.assertEquals("/tmp/schema/table1/", location);
  }

  @Test
  void testFallBackToCatalogLocation() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/catalog");
    Schema schema = mockSchema(ImmutableMap.of());

    String location =
        provider.provisionTableLocation(context(schema, ImmutableMap.of(), catalogProperties));

    // The schema name is taken from the table identifier's namespace.
    Assertions.assertEquals("/tmp/catalog/schema1/table1/", location);
  }

  @Test
  void testTrailingSlashIsNotDuplicated() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/catalog/");
    Schema schema = mockSchema(ImmutableMap.of(Schema.PROPERTY_LOCATION, "/tmp/schema/"));

    Assertions.assertEquals(
        "/tmp/schema/table1/",
        provider.provisionTableLocation(context(schema, ImmutableMap.of(), catalogProperties)));
    Assertions.assertEquals(
        "/tmp/explicit/",
        provider.provisionTableLocation(
            context(
                schema,
                ImmutableMap.of(Table.PROPERTY_LOCATION, "/tmp/explicit/"),
                catalogProperties)));
  }

  @Test
  void testNullSchemaPropertiesFallsBackToCatalogLocation() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/catalog");
    Schema schema = mockSchema(null);

    Assertions.assertEquals(
        "/tmp/catalog/schema1/table1/",
        provider.provisionTableLocation(context(schema, ImmutableMap.of(), catalogProperties)));
  }

  @Test
  void testNoLocationAnywhereThrows() {
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Map<String, String> catalogProperties = ImmutableMap.of();
    Schema schema = mockSchema(ImmutableMap.of());

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                provider.provisionTableLocation(
                    context(schema, ImmutableMap.of(), catalogProperties)));
    Assertions.assertTrue(
        e.getMessage().contains("'location' property is neither set in table properties"),
        "Unexpected message: " + e.getMessage());
  }

  @Test
  void testTheCatalogLocationIsReadPerCallRatherThanCaptured() {
    // There is no initialization callback to capture it in, so one instance must serve contexts
    // carrying different catalog properties. Pinned because the provider used to hold the value
    // in a field, and a leftover field would make every catalog on a shared instance resolve to
    // whichever one initialized it first.
    DefaultTableLocationProvider provider = new DefaultTableLocationProvider();
    Schema schema = mockSchema(ImmutableMap.of());

    Assertions.assertEquals(
        "/tmp/one/schema1/table1/",
        provider.provisionTableLocation(
            context(schema, ImmutableMap.of(), ImmutableMap.of("location", "/tmp/one"))));
    Assertions.assertEquals(
        "/tmp/two/schema1/table1/",
        provider.provisionTableLocation(
            context(schema, ImmutableMap.of(), ImmutableMap.of("location", "/tmp/two"))));
  }

  @Test
  void testProviderName() {
    Assertions.assertEquals("default", new DefaultTableLocationProvider().name());
  }

  @Test
  void testUnprovisionIsANoOp() {
    // The built-in provider derives locations from properties and has nothing to reclaim, so
    // unprovisioning one has to be harmless rather than unsupported.
    Assertions.assertDoesNotThrow(
        () ->
            new DefaultTableLocationProvider()
                .unprovisionTableLocation(
                    context(
                        mockSchema(ImmutableMap.of()),
                        ImmutableMap.of(Table.PROPERTY_LOCATION, "/tmp/schema/table1/"),
                        ImmutableMap.of())));
  }

  private static Schema mockSchema(Map<String, String> properties) {
    Schema schema = Mockito.mock(Schema.class);
    // The context is always built from the schema the table is being created in, so the schema
    // name matches the second level of the table identifier by construction.
    Mockito.when(schema.name()).thenReturn(TABLE_IDENT.namespace().level(2));
    Mockito.when(schema.properties()).thenReturn(properties);
    return schema;
  }

  private static TableLocationContext context(
      Schema schema, Map<String, String> tableProperties, Map<String, String> catalogProperties) {
    return TableLocationContext.builder()
        .withTableIdentifier(TABLE_IDENT)
        .withTableProperties(tableProperties)
        .withSchema(schema)
        .withCatalogProperties(catalogProperties)
        .build();
  }
}
