/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.generic;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLanceStoragePropertyInheritance {

  private GenericCatalogOperations ops;

  @BeforeEach
  public void setUp() {
    EntityStore store = mock(EntityStore.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    ops = new GenericCatalogOperations(store, idGenerator);
  }

  private void initOps(Map<String, String> conf) {
    PropertiesMetadata catalogPropMeta = mock(PropertiesMetadata.class);
    when(catalogPropMeta.getOrDefault(any(), any())).thenReturn(null);
    HasPropertyMetadata propertyMetadata = mock(HasPropertyMetadata.class);
    when(propertyMetadata.catalogPropertiesMetadata()).thenReturn(catalogPropMeta);
    ops.initialize(conf, null, propertyMetadata);
  }

  @Test
  public void testTableOverridesSchema() {
    Map<String, String> conf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testSchemaOverridesCatalog() {
    Map<String, String> conf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(
        "SCHEMA_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testTableOverridesCatalog() {
    Map<String, String> conf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testAllLevelsDifferentKeysMerged() {
    Map<String, String> conf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.secret-access-key", "SCHEMA_SK"));
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.endpoint", "TABLE_ENDPOINT");

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
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
    initOps(Map.of());

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertTrue(merged.isEmpty());
  }

  @Test
  public void testOnlyCatalogStorageProperties() {
    Map<String, String> conf =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "CATALOG_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testOnlySchemaStorageProperties() {
    initOps(Map.of());

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "SCHEMA_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testOnlyTableStorageProperties() {
    initOps(Map.of());

    Schema schema = mockSchema(Map.of());
    Map<String, String> tableProps =
        Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertEquals(
        "TABLE_AK", merged.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testNonLanceStoragePropertiesNotMerged() {
    Map<String, String> conf = new HashMap<>();
    conf.put(Catalog.PROPERTY_LOCATION, "s3://bucket/");
    conf.put(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK");
    initOps(conf);

    Schema schema = mockSchema(Map.of("some-other-key", "value"));
    Map<String, String> tableProps = Map.of("format", "lance");

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertEquals(1, merged.size());
    Assertions.assertFalse(merged.containsKey(Catalog.PROPERTY_LOCATION));
    Assertions.assertFalse(merged.containsKey("some-other-key"));
    Assertions.assertFalse(merged.containsKey("format"));
  }

  @Test
  public void testNullSchemaProperties() {
    initOps(Map.of());

    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(null);
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
    Assertions.assertTrue(merged.isEmpty());
  }

  @Test
  public void testNullConf() {
    initOps(null);

    Schema schema =
        mockSchema(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "SCHEMA_AK"));
    Map<String, String> tableProps = Map.of();

    Map<String, String> merged = ops.mergeLanceStorageProperties(schema, tableProps);
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
