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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_STORAGE_OPTIONS_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoLanceTableOperationsStorageMerge {

  private GravitinoLanceTableOperations ops;
  private Method mergeMethod;

  @BeforeEach
  public void setUp() throws Exception {
    GravitinoLanceNamespaceWrapper wrapper = mock(GravitinoLanceNamespaceWrapper.class);
    ops = new GravitinoLanceTableOperations(wrapper);
    mergeMethod =
        GravitinoLanceTableOperations.class.getDeclaredMethod(
            "mergeCatalogStorageProperties", Catalog.class, Map.class);
    mergeMethod.setAccessible(true);
  }

  @Test
  public void testCatalogPropertiesMergedIntoTable() {
    Catalog catalog =
        mockCatalog(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK"));
    Map<String, String> tableProps = new HashMap<>();

    invokeMerge(catalog, tableProps);

    Assertions.assertEquals(
        "CATALOG_AK", tableProps.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testTablePropertiesOverrideCatalog() {
    Catalog catalog =
        mockCatalog(Map.of(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "CATALOG_AK"));
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id", "TABLE_AK");

    invokeMerge(catalog, tableProps);

    Assertions.assertEquals(
        "TABLE_AK", tableProps.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
  }

  @Test
  public void testNoCatalogStorageProperties() {
    Catalog catalog = mockCatalog(Map.of("location", "s3://bucket/"));
    Map<String, String> tableProps = new HashMap<>();

    invokeMerge(catalog, tableProps);

    Assertions.assertTrue(tableProps.isEmpty());
  }

  @Test
  public void testNullCatalogProperties() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.properties()).thenReturn(null);
    Map<String, String> tableProps = new HashMap<>();

    invokeMerge(catalog, tableProps);

    Assertions.assertTrue(tableProps.isEmpty());
  }

  @Test
  public void testDifferentKeysMergedFromCatalog() {
    Catalog catalog =
        mockCatalog(
            Map.of(
                LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id",
                "AK",
                LANCE_STORAGE_OPTIONS_PREFIX + "s3.endpoint",
                "https://s3.amazonaws.com"));
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(LANCE_STORAGE_OPTIONS_PREFIX + "s3.secret-access-key", "SK");

    invokeMerge(catalog, tableProps);

    Assertions.assertEquals(3, tableProps.size());
    Assertions.assertEquals(
        "AK", tableProps.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.access-key-id"));
    Assertions.assertEquals(
        "https://s3.amazonaws.com", tableProps.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.endpoint"));
    Assertions.assertEquals(
        "SK", tableProps.get(LANCE_STORAGE_OPTIONS_PREFIX + "s3.secret-access-key"));
  }

  private void invokeMerge(Catalog catalog, Map<String, String> tableProps) {
    try {
      mergeMethod.invoke(ops, catalog, tableProps);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Catalog mockCatalog(Map<String, String> properties) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.properties()).thenReturn(properties);
    return catalog;
  }
}
