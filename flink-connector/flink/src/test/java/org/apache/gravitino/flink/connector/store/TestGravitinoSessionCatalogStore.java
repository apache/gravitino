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

  /**
   * A Gravitino-managed (built-in) catalog type — verified by {@link
   * org.apache.gravitino.flink.connector.utils.FactoryUtils#isBuiltInCatalog}. Catalogs of this
   * type are routed to the Gravitino-backed store.
   */
  private static final String GRAVITINO_CATALOG_TYPE = "gravitino-hive";

  /**
   * A third-party (session) catalog type that Gravitino does not manage. Catalogs of this type are
   * routed to the in-memory store and exist only for the lifetime of the Flink session.
   */
  private static final String SESSION_CATALOG_TYPE = "hive";

  private GravitinoCatalogStore gravitinoCatalogStore;
  private GenericInMemoryCatalogStore memoryCatalogStore;
  private GravitinoSessionCatalogStore sessionCatalogStore;

  @Before
  public void setUp() {
    assertTrue(
        "GRAVITINO_CATALOG_TYPE must be recognised by isBuiltInCatalog()",
        isBuiltInCatalog(GRAVITINO_CATALOG_TYPE));
    assertFalse(
        "SESSION_CATALOG_TYPE must NOT be recognised by isBuiltInCatalog()",
        isBuiltInCatalog(SESSION_CATALOG_TYPE));
    gravitinoCatalogStore = mock(GravitinoCatalogStore.class);
    memoryCatalogStore = mock(GenericInMemoryCatalogStore.class);
    sessionCatalogStore =
        new GravitinoSessionCatalogStore(gravitinoCatalogStore, memoryCatalogStore);
  }

  // -------------------------------------------------------------------------
  // storeCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testStoreCatalog_gravitinoCatalog_storesInGravitino() throws CatalogException {
    CatalogDescriptor descriptor = descriptorWithType(GRAVITINO_CATALOG_TYPE);

    sessionCatalogStore.storeCatalog("gravitino-hive", descriptor);

    verify(gravitinoCatalogStore).storeCatalog("gravitino-hive", descriptor);
    verify(memoryCatalogStore, never()).storeCatalog("gravitino-hive", descriptor);
  }

  @Test
  public void testStoreCatalog_sessionCatalog_storesInMemory() throws CatalogException {
    CatalogDescriptor descriptor = descriptorWithType(SESSION_CATALOG_TYPE);

    sessionCatalogStore.storeCatalog("hive", descriptor);

    verify(memoryCatalogStore).storeCatalog("hive", descriptor);
    verify(gravitinoCatalogStore, never()).storeCatalog("hive", descriptor);
  }

  // -------------------------------------------------------------------------
  // removeCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testRemoveCatalog_catalogInMemory_removesFromMemory() throws CatalogException {
    when(memoryCatalogStore.contains("hive")).thenReturn(true);

    sessionCatalogStore.removeCatalog("hive", false);

    verify(memoryCatalogStore).removeCatalog("hive", false);
    verify(gravitinoCatalogStore, never()).removeCatalog("hive", false);
  }

  @Test
  public void testRemoveCatalog_catalogNotInMemory_removesFromGravitino() throws CatalogException {
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);

    sessionCatalogStore.removeCatalog("gravitino-hive", false);

    verify(gravitinoCatalogStore).removeCatalog("gravitino-hive", false);
    verify(memoryCatalogStore, never()).removeCatalog("gravitino-hive", false);
  }

  // -------------------------------------------------------------------------
  // getCatalog
  // -------------------------------------------------------------------------

  @Test
  public void testGetCatalog_catalogInMemory_returnsFromMemory() throws CatalogException {
    CatalogDescriptor expected = descriptorWithType(SESSION_CATALOG_TYPE);
    when(memoryCatalogStore.contains("hive")).thenReturn(true);
    when(memoryCatalogStore.getCatalog("hive")).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog("hive");

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore, never()).getCatalog("hive");
  }

  @Test
  public void testGetCatalog_catalogNotInMemory_returnsFromGravitino() throws CatalogException {
    CatalogDescriptor expected = descriptorWithType(GRAVITINO_CATALOG_TYPE);
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);
    when(gravitinoCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog("gravitino-hive");

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore).getCatalog("gravitino-hive");
  }

  @Test
  public void testGetCatalog_catalogInMemoryButEmpty_fallbackToGravitino() throws CatalogException {
    CatalogDescriptor expected = descriptorWithType(GRAVITINO_CATALOG_TYPE);
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(true);
    when(memoryCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.empty());
    when(gravitinoCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog("gravitino-hive");

    assertTrue(result.isPresent());
    assertEquals(expected, result.get());
    verify(gravitinoCatalogStore).getCatalog("gravitino-hive");
  }

  // -------------------------------------------------------------------------
  // listCatalogs
  // -------------------------------------------------------------------------

  @Test
  public void testListCatalogs_returnsCombinedSet() throws CatalogException {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive"));
    when(gravitinoCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("gravitino-hive"));

    Set<String> result = sessionCatalogStore.listCatalogs();

    assertEquals(ImmutableSet.of("hive", "gravitino-hive"), result);
  }

  @Test(expected = CatalogException.class)
  public void testListCatalogs_gravitinoThrows_wrapsCatalogException() throws CatalogException {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive"));
    when(gravitinoCatalogStore.listCatalogs())
        .thenThrow(new RuntimeException("Gravitino unavailable"));

    sessionCatalogStore.listCatalogs();
  }

  // -------------------------------------------------------------------------
  // contains
  // -------------------------------------------------------------------------

  @Test
  public void testContains_catalogInMemory_returnsTrue() {
    when(memoryCatalogStore.contains("hive")).thenReturn(true);

    assertTrue(sessionCatalogStore.contains("hive"));
  }

  @Test
  public void testContains_catalogInGravitino_returnsTrue() {
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);
    when(gravitinoCatalogStore.contains("gravitino-hive")).thenReturn(true);

    assertTrue(sessionCatalogStore.contains("gravitino-hive"));
  }

  @Test
  public void testContains_catalogInNeither_returnsFalse() {
    when(memoryCatalogStore.contains("hive")).thenReturn(false);
    when(gravitinoCatalogStore.contains("hive")).thenReturn(false);

    assertFalse(sessionCatalogStore.contains("hive"));
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
