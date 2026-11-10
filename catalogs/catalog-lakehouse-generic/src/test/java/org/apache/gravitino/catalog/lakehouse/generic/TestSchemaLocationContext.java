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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSchemaLocationContext {

  private static final NameIdentifier SCHEMA = NameIdentifier.of("metalake", "catalog", "schema");

  @Test
  public void testDefaults() {
    SchemaLocationContext context =
        SchemaLocationContext.builder().withSchemaIdentifier(SCHEMA).build();

    Assertions.assertEquals(SCHEMA, context.schemaIdentifier());
    Assertions.assertTrue(context.schemaProperties().isEmpty());
    Assertions.assertTrue(context.catalogProperties().isEmpty());
    Assertions.assertFalse(context.cascade());
    Assertions.assertTrue(context.tablesWithRetainedData().isEmpty());
  }

  @Test
  public void testNullCollectionsAreTreatedAsEmpty() {
    SchemaLocationContext context =
        SchemaLocationContext.builder()
            .withSchemaIdentifier(SCHEMA)
            .withSchemaProperties(null)
            .withCatalogProperties(null)
            .withTablesWithRetainedData(null)
            .build();

    Assertions.assertTrue(context.schemaProperties().isEmpty());
    Assertions.assertTrue(context.catalogProperties().isEmpty());
    Assertions.assertTrue(context.tablesWithRetainedData().isEmpty());
  }

  @Test
  public void testANullPropertyValueIsKept() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("a-property-with-no-value", null);

    SchemaLocationContext context =
        SchemaLocationContext.builder()
            .withSchemaIdentifier(SCHEMA)
            .withSchemaProperties(properties)
            .withCatalogProperties(properties)
            .build();

    Assertions.assertTrue(context.schemaProperties().containsKey("a-property-with-no-value"));
    Assertions.assertTrue(context.catalogProperties().containsKey("a-property-with-no-value"));
  }

  @Test
  public void testTheContextIsNotAffectedByLaterChangesToItsInputs() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("location", "s3://bucket/db/");
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "t1");
    List<NameIdentifier> retained = Lists.newArrayList(table);

    SchemaLocationContext context =
        SchemaLocationContext.builder()
            .withSchemaIdentifier(SCHEMA)
            .withSchemaProperties(properties)
            .withCascade(true)
            .withTablesWithRetainedData(retained)
            .build();
    properties.clear();
    retained.clear();

    Assertions.assertEquals("s3://bucket/db/", context.schemaProperties().get("location"));
    Assertions.assertTrue(context.cascade());
    Assertions.assertEquals(List.of(table), context.tablesWithRetainedData());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> context.schemaProperties().put("k", "v"));
  }

  @Test
  public void testTheSchemaIdentifierIsRequired() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SchemaLocationContext.builder().build());
  }
}
