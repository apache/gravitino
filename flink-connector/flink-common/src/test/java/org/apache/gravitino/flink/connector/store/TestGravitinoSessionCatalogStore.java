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

import static org.apache.gravitino.flink.connector.utils.FactoryUtils.isGravitinoManagedCatalogType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoSessionCatalogStore {

  /**
   * A Gravitino-managed (built-in) catalog type — verified by {@link
   * org.apache.gravitino.flink.connector.utils.FactoryUtils#isGravitinoManagedCatalogType}.
   * Catalogs of this type are routed to the Gravitino-backed store.
   */
  private static final String GRAVITINO_CATALOG_TYPE = "gravitino-hive";

  /**
   * A third-party (session) catalog type that Gravitino does not manage. Catalogs of this type are
   * routed to the in-memory store and exist only for the lifetime of the Flink session.
   */
  private static final String SESSION_CATALOG_TYPE = "hive";

  /** A catalog name excluded from Gravitino in the session. */
  private static final String EXCLUDED_CATALOG = "paimon_catalog";

  private GravitinoCatalogStore gravitinoCatalogStore;
  private GenericInMemoryCatalogStore memoryCatalogStore;
  private GravitinoSessionCatalogStore sessionCatalogStore;

  @BeforeEach
  void setUp() {
    Assertions.assertTrue(
        isGravitinoManagedCatalogType(GRAVITINO_CATALOG_TYPE),
        "GRAVITINO_CATALOG_TYPE must be recognised by isGravitinoManagedCatalogType()");
    Assertions.assertFalse(
        isGravitinoManagedCatalogType(SESSION_CATALOG_TYPE),
        "SESSION_CATALOG_TYPE must NOT be recognised by isGravitinoManagedCatalogType()");
    gravitinoCatalogStore = mock(GravitinoCatalogStore.class);
    memoryCatalogStore = mock(GenericInMemoryCatalogStore.class);
    sessionCatalogStore =
        new GravitinoSessionCatalogStore(gravitinoCatalogStore, memoryCatalogStore);
  }

  @Test
  void testKnownGravitinoCatalogTypesAreGravitinoManaged() {
    Assertions.assertTrue(isGravitinoManagedCatalogType("gravitino-hive"));
    Assertions.assertTrue(isGravitinoManagedCatalogType("gravitino-jdbc-mysql"));
    Assertions.assertTrue(isGravitinoManagedCatalogType("gravitino-jdbc-postgresql"));
    Assertions.assertFalse(isGravitinoManagedCatalogType("gravitino-jdbc-custom"));
    Assertions.assertFalse(isGravitinoManagedCatalogType("jdbc-mysql"));
    Assertions.assertFalse(isGravitinoManagedCatalogType(null));
  }

  // -------------------------------------------------------------------------
  // storeCatalog
  // -------------------------------------------------------------------------

  @Test
  void testStoreCatalog_gravitinoCatalog_storesInGravitino() throws CatalogException {
    CatalogDescriptor descriptor = descriptorWithType(GRAVITINO_CATALOG_TYPE);

    sessionCatalogStore.storeCatalog("gravitino-hive", descriptor);

    verify(gravitinoCatalogStore).storeCatalog("gravitino-hive", descriptor);
    verify(memoryCatalogStore, never()).storeCatalog("gravitino-hive", descriptor);
  }

  @Test
  void testStoreCatalog_sessionCatalog_storesInMemory() throws CatalogException {
    CatalogDescriptor descriptor = descriptorWithType(SESSION_CATALOG_TYPE);

    sessionCatalogStore.storeCatalog("hive", descriptor);

    verify(memoryCatalogStore).storeCatalog("hive", descriptor);
    verify(gravitinoCatalogStore, never()).storeCatalog("hive", descriptor);
  }

  @Test
  void testStoreCatalog_missingCatalogType_throwsCatalogException() {
    CatalogDescriptor descriptor = CatalogDescriptor.of("unknown", new Configuration());

    Assertions.assertThrows(
        CatalogException.class, () -> sessionCatalogStore.storeCatalog("unknown", descriptor));
  }

  // -------------------------------------------------------------------------
  // removeCatalog
  // -------------------------------------------------------------------------

  @Test
  void testRemoveCatalog_catalogInMemory_removesFromMemory() throws CatalogException {
    when(memoryCatalogStore.contains("hive")).thenReturn(true);

    sessionCatalogStore.removeCatalog("hive", false);

    verify(memoryCatalogStore).removeCatalog("hive", false);
    verify(gravitinoCatalogStore, never()).removeCatalog("hive", false);
  }

  @Test
  void testRemoveCatalog_catalogNotInMemory_removesFromGravitino() throws CatalogException {
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);

    sessionCatalogStore.removeCatalog("gravitino-hive", false);

    verify(gravitinoCatalogStore).removeCatalog("gravitino-hive", false);
    verify(memoryCatalogStore, never()).removeCatalog("gravitino-hive", false);
  }

  // -------------------------------------------------------------------------
  // getCatalog
  // -------------------------------------------------------------------------

  @Test
  void testGetCatalog_catalogInMemory_returnsFromMemory() throws CatalogException {
    CatalogDescriptor expected = descriptorWithType(SESSION_CATALOG_TYPE);
    when(memoryCatalogStore.getCatalog("hive")).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog("hive");

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(expected, result.get());
    verify(gravitinoCatalogStore, never()).getCatalog("hive");
  }

  @Test
  void testGetCatalog_catalogNotInMemory_returnsFromGravitino() throws CatalogException {
    CatalogDescriptor expected = descriptorWithType(GRAVITINO_CATALOG_TYPE);
    when(memoryCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.empty());
    when(gravitinoCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.of(expected));

    Optional<CatalogDescriptor> result = sessionCatalogStore.getCatalog("gravitino-hive");

    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(expected, result.get());
    verify(gravitinoCatalogStore).getCatalog("gravitino-hive");
  }

  // -------------------------------------------------------------------------
  // listCatalogs
  // -------------------------------------------------------------------------

  @Test
  void testListCatalogs_returnsCombinedSet() throws CatalogException {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive"));
    when(gravitinoCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("gravitino-hive"));

    Set<String> result = sessionCatalogStore.listCatalogs();

    Assertions.assertEquals(ImmutableSet.of("hive", "gravitino-hive"), result);
  }

  @Test
  void testListCatalogs_gravitinoThrows_wrapsCatalogException() {
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive"));
    when(gravitinoCatalogStore.listCatalogs())
        .thenThrow(new RuntimeException("Gravitino unavailable"));

    Assertions.assertThrows(CatalogException.class, () -> sessionCatalogStore.listCatalogs());
  }

  // -------------------------------------------------------------------------
  // contains
  // -------------------------------------------------------------------------

  @Test
  void testContains_catalogInMemory_returnsTrue() {
    when(memoryCatalogStore.contains("hive")).thenReturn(true);

    Assertions.assertTrue(sessionCatalogStore.contains("hive"));
  }

  @Test
  void testContains_catalogInGravitino_returnsTrue() {
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);
    when(gravitinoCatalogStore.contains("gravitino-hive")).thenReturn(true);

    Assertions.assertTrue(sessionCatalogStore.contains("gravitino-hive"));
  }

  @Test
  void testContains_catalogInNeither_returnsFalse() {
    when(memoryCatalogStore.contains("hive")).thenReturn(false);
    when(gravitinoCatalogStore.contains("hive")).thenReturn(false);

    Assertions.assertFalse(sessionCatalogStore.contains("hive"));
  }

  // -------------------------------------------------------------------------
  // excluded catalogs
  // -------------------------------------------------------------------------

  @Test
  void testConstructor_nullExcludedCatalogs_throwsIllegalArgumentException() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new GravitinoSessionCatalogStore(gravitinoCatalogStore, memoryCatalogStore, null));
  }

  @Test
  void testExcludedCatalog_contains_onlyChecksMemory() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    when(memoryCatalogStore.contains(EXCLUDED_CATALOG)).thenReturn(false);
    when(gravitinoCatalogStore.contains(EXCLUDED_CATALOG)).thenReturn(true);

    Assertions.assertFalse(store.contains(EXCLUDED_CATALOG));
    verify(gravitinoCatalogStore, never()).contains(EXCLUDED_CATALOG);

    when(memoryCatalogStore.contains(EXCLUDED_CATALOG)).thenReturn(true);
    Assertions.assertTrue(store.contains(EXCLUDED_CATALOG));
    verify(gravitinoCatalogStore, never()).contains(EXCLUDED_CATALOG);
  }

  @Test
  void testExcludedCatalog_getCatalog_neverFallsBackToGravitino() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    when(memoryCatalogStore.getCatalog(EXCLUDED_CATALOG)).thenReturn(Optional.empty());
    when(gravitinoCatalogStore.getCatalog(EXCLUDED_CATALOG))
        .thenReturn(Optional.of(descriptorWithType(GRAVITINO_CATALOG_TYPE)));

    Assertions.assertFalse(store.getCatalog(EXCLUDED_CATALOG).isPresent());
    verify(gravitinoCatalogStore, never()).getCatalog(EXCLUDED_CATALOG);

    CatalogDescriptor sessionDescriptor = descriptorWithType(SESSION_CATALOG_TYPE);
    when(memoryCatalogStore.getCatalog(EXCLUDED_CATALOG))
        .thenReturn(Optional.of(sessionDescriptor));
    Assertions.assertEquals(Optional.of(sessionDescriptor), store.getCatalog(EXCLUDED_CATALOG));
    verify(gravitinoCatalogStore, never()).getCatalog(EXCLUDED_CATALOG);
  }

  @Test
  void testExcludedCatalog_removeCatalog_neverRemovesFromGravitino() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    when(memoryCatalogStore.contains(EXCLUDED_CATALOG)).thenReturn(false);

    store.removeCatalog(EXCLUDED_CATALOG, true);

    verify(memoryCatalogStore).removeCatalog(EXCLUDED_CATALOG, true);
    verify(gravitinoCatalogStore, never()).removeCatalog(anyString(), anyBoolean());
  }

  @Test
  void testExcludedCatalog_removeMissingCatalog_propagatesMemoryStoreError() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    doThrow(new CatalogException("not found"))
        .when(memoryCatalogStore)
        .removeCatalog(EXCLUDED_CATALOG, false);

    Assertions.assertThrows(
        CatalogException.class, () -> store.removeCatalog(EXCLUDED_CATALOG, false));
    verify(gravitinoCatalogStore, never()).removeCatalog(anyString(), anyBoolean());
  }

  @Test
  void testExcludedCatalog_storeSessionCatalog_storesInMemory() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    CatalogDescriptor descriptor = descriptorWithType(SESSION_CATALOG_TYPE);

    store.storeCatalog(EXCLUDED_CATALOG, descriptor);

    verify(memoryCatalogStore).storeCatalog(EXCLUDED_CATALOG, descriptor);
    verify(gravitinoCatalogStore, never()).storeCatalog(anyString(), any());
  }

  @Test
  void testExcludedCatalog_storeGravitinoManagedCatalog_isRejected() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    CatalogDescriptor descriptor = descriptorWithType(GRAVITINO_CATALOG_TYPE);

    CatalogException exception =
        Assertions.assertThrows(
            CatalogException.class, () -> store.storeCatalog(EXCLUDED_CATALOG, descriptor));

    Assertions.assertTrue(exception.getMessage().contains(EXCLUDED_CATALOG));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(GravitinoCatalogStoreFactoryOptions.GRAVITINO_EXCLUDED_CATALOGS.key()));
    verify(gravitinoCatalogStore, never()).storeCatalog(anyString(), any());
    verify(memoryCatalogStore, never()).storeCatalog(anyString(), any());
  }

  @Test
  void testExcludedCatalog_listCatalogs_hidesGravitinoCatalogWithExcludedName() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive"));
    when(gravitinoCatalogStore.listCatalogs())
        .thenReturn(ImmutableSet.of(EXCLUDED_CATALOG, "gravitino-hive"));

    Assertions.assertEquals(ImmutableSet.of("hive", "gravitino-hive"), store.listCatalogs());

    when(memoryCatalogStore.listCatalogs()).thenReturn(ImmutableSet.of("hive", EXCLUDED_CATALOG));
    Assertions.assertEquals(
        ImmutableSet.of("hive", EXCLUDED_CATALOG, "gravitino-hive"), store.listCatalogs());
  }

  @Test
  void testExcludedCatalog_otherCatalogNamesKeepExistingBehavior() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    CatalogDescriptor gravitinoDescriptor = descriptorWithType(GRAVITINO_CATALOG_TYPE);
    when(memoryCatalogStore.contains("gravitino-hive")).thenReturn(false);
    when(memoryCatalogStore.getCatalog("gravitino-hive")).thenReturn(Optional.empty());
    when(gravitinoCatalogStore.contains("gravitino-hive")).thenReturn(true);
    when(gravitinoCatalogStore.getCatalog("gravitino-hive"))
        .thenReturn(Optional.of(gravitinoDescriptor));

    Assertions.assertTrue(store.contains("gravitino-hive"));
    Assertions.assertEquals(Optional.of(gravitinoDescriptor), store.getCatalog("gravitino-hive"));
    store.storeCatalog("gravitino-hive", gravitinoDescriptor);
    store.removeCatalog("gravitino-hive", false);

    verify(gravitinoCatalogStore).storeCatalog("gravitino-hive", gravitinoDescriptor);
    verify(gravitinoCatalogStore).removeCatalog("gravitino-hive", false);
  }

  @Test
  void testExcludedCatalog_namesAreCaseSensitive() {
    GravitinoSessionCatalogStore store = storeExcluding(EXCLUDED_CATALOG);
    String otherCase = EXCLUDED_CATALOG.toUpperCase(Locale.ROOT);
    when(memoryCatalogStore.contains(otherCase)).thenReturn(false);
    when(gravitinoCatalogStore.contains(otherCase)).thenReturn(true);

    Assertions.assertTrue(store.contains(otherCase));
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private GravitinoSessionCatalogStore storeExcluding(String... catalogNames) {
    return new GravitinoSessionCatalogStore(
        gravitinoCatalogStore, memoryCatalogStore, ImmutableSet.copyOf(catalogNames));
  }

  private static CatalogDescriptor descriptorWithType(String type) {
    Configuration config = new Configuration();
    config.setString(CommonCatalogOptions.CATALOG_TYPE.key(), type);
    return CatalogDescriptor.of(type, config);
  }
}
