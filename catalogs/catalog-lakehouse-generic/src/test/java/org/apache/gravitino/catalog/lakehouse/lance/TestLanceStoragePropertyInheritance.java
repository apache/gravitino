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
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLanceStoragePropertyInheritance {

  private LanceTableDelegator delegator;

  @BeforeEach
  public void setUp() {
    delegator = new LanceTableDelegator();
  }

  @Test
  public void testTableOverridesSchema() {
    Map<String, String> catalogConf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testSchemaOverridesCatalog() {
    Map<String, String> catalogConf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(
        "SCHEMA_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testTableOverridesCatalog() {
    Map<String, String> catalogConf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testAllLevelsDifferentKeysMerged() {
    Map<String, String> catalogConf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.secret-access-key", "SCHEMA_SK"));
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.endpoint", "TABLE_ENDPOINT");

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(3, merged.size());
    Assertions.assertEquals(
        "CATALOG_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
    Assertions.assertEquals(
        "SCHEMA_SK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.secret-access-key"));
    Assertions.assertEquals(
        "TABLE_ENDPOINT", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.endpoint"));
  }

  @Test
  public void testNoStoragePropertiesAtAnyLevel() {
    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(Map.of(), schema, tableProps);
    Assertions.assertTrue(merged.isEmpty());
  }

  @Test
  public void testOnlyCatalogStorageProperties() {
    Map<String, String> catalogConf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "CATALOG_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testOnlySchemaStorageProperties() {
    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(Map.of(), schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "SCHEMA_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testOnlyTableStorageProperties() {
    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = delegator.inheritProperties(Map.of(), schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testNonLanceStoragePropertiesNotMerged() {
    Map<String, String> catalogConf = new HashMap<>();
    catalogConf.put(Catalog.PROPERTY_LOCATION, "s3://bucket/");
    catalogConf.put(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");

    Schema schema = mockSchema(Map.of("some-other-key", "value"));
    Map<String, String> tableProps = Map.of("format", "lance");

    Map<String, String> merged = delegator.inheritProperties(catalogConf, schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertFalse(merged.containsKey(Catalog.PROPERTY_LOCATION));
    Assertions.assertFalse(merged.containsKey("some-other-key"));
    Assertions.assertFalse(merged.containsKey("format"));
  }

  @Test
  public void testNullSchemaProperties() {
    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(null);
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(Map.of(), schema, tableProps);
    Assertions.assertTrue(merged.isEmpty());
  }

  @Test
  public void testNullCatalogConf() {
    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = delegator.inheritProperties(null, schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "SCHEMA_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  private Schema mockSchema(Map<String, String> properties) {
    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(properties);
    return schema;
  }
}
