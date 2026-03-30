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
package org.apache.gravitino.flink.connector.store;

import static org.apache.gravitino.flink.connector.utils.FactoryUtils.GRAVITINO_FACTORY_LIST;
import static org.apache.gravitino.flink.connector.utils.FactoryUtils.isBuiltInCatalog;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.Before;
import org.junit.Test;

public class TestGravitinoSessionCatalogStore {

  /** A catalog type that isBuiltInCatalog() recognises as Gravitino-managed. */
  private static final String BUILT_IN_TYPE = GRAVITINO_FACTORY_LIST.get(0);

  /** A catalog type that isBuiltInCatalog() does NOT recognise. */
  private static final String OTHER_TYPE = "hive";

  private GravitinoCatalogStore gravitinoCatalogStore;
  private GenericInMemoryCatalogStore memoryCatalogStore;
  private GravitinoSessionCatalogStore sessionCatalogStore;

  @Before
  public void setUp() {
    assertTrue(
        "BUILT_IN_TYPE must be recognised by isBuiltInCatalog()", isBuiltInCatalog(BUILT_IN_TYPE));
    assertFalse(
        "OTHER_TYPE must NOT be recognised by isBuiltInCatalog()", isBuiltInCatalog(OTHER_TYPE));
    gravitinoCatalogStore = mock(GravitinoCatalogStore.class);
    memoryCatalogStore = mock(GenericInMemoryCatalogStore.class);
    sessionCatalogStore =
        new GravitinoSessionCatalogStore(gravitinoCatalogStore, memoryCatalogStore);
  }

  // -------------------------------------------------------------------------
  // storeCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testStoreCatalog_builtInType_storesInMemory() throws CatalogException {
    String catalogName = "hive_catalog";
    CatalogDescriptor descriptor = descriptorWithType(BUILT_IN_TYPE);

    sessionCatalogStore.storeCatalog(catalogName, descriptor);

    verify(memoryCatalogStore).storeCatalog(catalogName, descriptor);
    verify(gravitinoCatalogStore, never()).storeCatalog(catalogName, descriptor);
  }

  @Test
  public void testStoreCatalog_thirdPartyType_storesInGravitino() throws CatalogException {
    String catalogName = "hive";
    CatalogDescriptor descriptor = descriptorWithType(OTHER_TYPE);

    sessionCatalogStore.storeCatalog(catalogName, descriptor);

    verify(gravitinoCatalogStore).storeCatalog(catalogName, descriptor);
    verify(memoryCatalogStore, never()).storeCatalog(catalogName, descriptor);
  }

  // -------------------------------------------------------------------------
  // removeCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testRemoveCatalog_catalogInMemory_removesFromMemory() throws CatalogException {
    String catalogName = "mem_catalog";
    when(memoryCatalogStore.contains(catalogName)).thenReturn(true);

    sessionCatalogStore.removeCatalog(catalogName, false);

    verify(memoryCatalogStore).removeCatalog(catalogName, false);
    verify(gravitinoCatalogStore, never()).removeCatalog(catalogName, false);
  }

  @Test
  public void testRemoveCatalog_catalogNotInMemory_removesFromGravitino() throws CatalogException {
    String catalogName = "gravitino_catalog";
    when(memoryCatalogStore.contains(catalogName)).thenReturn(false);

    sessionCatalogStore.removeCatalog(catalogName, false);

    verify(gravitinoCatalogStore).removeCatalog(catalogName, false);
    verify(memoryCatalogStore, never()).removeCatalog(catalogName, false);
  }

  // -------------------------------------------------------------------------
  // getCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testGetCatalog_catalogInMemory_returnsFromMemory() throws CatalogException {
    String catalogName = "mem_catalog";
    CatalogDescriptor expected = descriptorWithType(BUILT_IN_TYPE);
    when(memoryCatalogStore.contains(catalogName)).thenReturn(true);
    when(memoryCatalogStore.getCatalog(catalogName)).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog(catalogName);

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore, never()).getCatalog(catalogName);
  }

  @Test
  public void testGetCatalog_catalogNotInMemory_returnsFromGravitino() throws CatalogException {
    String catalogName = "gravitino_catalog";
    CatalogDescriptor expected = descriptorWithType(OTHER_TYPE);
    when(memoryCatalogStore.contains(catalogName)).thenReturn(false);
    when(gravitinoCatalogStore.getCatalog(catalogName)).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog(catalogName);

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore).getCatalog(catalogName);
  }

  @Test
  public void testGetCatalog_catalogInMemoryButEmpty_fallbackToGravitino() throws CatalogException {
    String catalogName = "catalog";
    CatalogDescriptor expected = descriptorWithType(OTHER_TYPE);
    when(memoryCatalogStore.contains(catalogName)).thenReturn(true);
    when(memoryCatalogStore.getCatalog(catalogName)).thenReturn(Optional.empty());
    when(gravitinoCatalogStore.getCatalog(catalogName)).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog(catalogName);

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore).getCatalog(catalogName);
  }

  // -------------------------------------------------------------------------
  // listCatalogs
  // -------------------------------------------------------------------------

  @Test
  public void testListCatalogs_returnsCombinedSet() throws CatalogException {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("mem_cat1", "mem_cat2"));
    when(gravitinoCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("grav_cat1", "mem_cat2"));

    Set<String> result = sessionCatalogStore.listCatalogs();

    assertEquals(ImmutableSet.of("mem_cat1", "mem_cat2", "grav_cat1"), result);
  }

  @Test(expected = CatalogException.class)
  public void testListCatalogs_gravitinoThrows_wrapsCatalogException() throws CatalogException {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of());
    when(gravitinoCatalogStore.listCatalogs())
        .thenThrow(new RuntimeException("Gravitino unavailable"));

    sessionCatalogStore.listCatalogs();
  }

  // -------------------------------------------------------------------------
  // contains
  // -------------------------------------------------------------------------

  @Test
  public void testContains_catalogInMemory_returnsTrue() {
    when(memoryCatalogStore.contains("cat")).thenReturn(true);

    assertTrue(sessionCatalogStore.contains("cat"));
  }

  @Test
  public void testContains_catalogInGravitino_returnsTrue() {
    when(memoryCatalogStore.contains("cat")).thenReturn(false);
    when(gravitinoCatalogStore.contains("cat")).thenReturn(true);

    assertTrue(sessionCatalogStore.contains("cat"));
  }

  @Test
  public void testContains_catalogInNeither_returnsFalse() {
    when(memoryCatalogStore.contains("cat")).thenReturn(false);
    when(gravitinoCatalogStore.contains("cat")).thenReturn(false);

    assertFalse(sessionCatalogStore.contains("cat"));
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private static CatalogDescriptor descriptorWithType(String type) {
    Configuration config = new Configuration();
    config.setString(CommonCatalogOptions.CATALOG_TYPE.key(), type);
    return CatalogDescriptor.of(type, config);
  }
}
