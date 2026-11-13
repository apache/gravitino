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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link GravitinoSessionCatalogStore} through a real Flink {@link TableEnvironment} and
 * {@link CatalogManager}, which call the catalog store in version-specific orders. The
 * Gravitino-backed store is mocked; this test is compiled into every supported Flink minor.
 */
public class TestGravitinoSessionCatalogStoreWithCatalogManager {

  private static final String EXCLUDED_CATALOG = "paimon_catalog";
  private static final String GRAVITINO_ONLY_CATALOG = "gravitino_only_catalog";
  private static final String GRAVITINO_PAIMON = "gravitino-paimon";
  private static final String CREATE_EXCLUDED_CATALOG =
      "CREATE CATALOG " + EXCLUDED_CATALOG + " WITH ('type'='generic_in_memory')";

  private GravitinoCatalogStore gravitinoCatalogStore;

  @BeforeEach
  void setUp() {
    gravitinoCatalogStore = mock(GravitinoCatalogStore.class);
    for (String catalogName : Arrays.asList(EXCLUDED_CATALOG, GRAVITINO_ONLY_CATALOG)) {
      when(gravitinoCatalogStore.contains(catalogName)).thenReturn(true);
      when(gravitinoCatalogStore.getCatalog(catalogName))
          .thenReturn(Optional.of(inMemoryDescriptor(catalogName)));
    }
    when(gravitinoCatalogStore.listCatalogs())
        .thenReturn(ImmutableSet.of(EXCLUDED_CATALOG, GRAVITINO_ONLY_CATALOG));
  }

  /** Without exclusion, a same-named session catalog is rejected (the behavior in #14756). */
  @Test
  void testCreateSessionCatalogWithGravitinoCatalogName_withoutExclusion_fails() {
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of());

    Throwable exception =
        Assertions.assertThrows(Exception.class, () -> execute(tableEnv, CREATE_EXCLUDED_CATALOG));

    Assertions.assertTrue(
        allMessages(exception).contains("already exists"), allMessages(exception));
    verify(gravitinoCatalogStore, never()).storeCatalog(any(), any());
  }

  @Test
  void testExcludedCatalog_fullLifecycle_neverTouchesGravitinoCatalog() {
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of(EXCLUDED_CATALOG));

    // The Gravitino catalog with the excluded name is not visible in this session.
    Assertions.assertFalse(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));
    Assertions.assertTrue(listCatalogs(tableEnv).contains(GRAVITINO_ONLY_CATALOG));
    Assertions.assertThrows(
        Exception.class, () -> execute(tableEnv, "USE CATALOG " + EXCLUDED_CATALOG));

    execute(tableEnv, CREATE_EXCLUDED_CATALOG);
    Assertions.assertTrue(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));

    // Flink's duplicate-name semantics are kept for the session catalog.
    Throwable duplicate =
        Assertions.assertThrows(Exception.class, () -> execute(tableEnv, CREATE_EXCLUDED_CATALOG));
    Assertions.assertTrue(
        allMessages(duplicate).contains("already exists"), allMessages(duplicate));

    execute(tableEnv, "USE CATALOG " + EXCLUDED_CATALOG);
    execute(tableEnv, "CREATE DATABASE session_db");
    Assertions.assertTrue(collect(tableEnv, "SHOW DATABASES").contains("session_db"));
    execute(tableEnv, "USE CATALOG default_catalog");

    execute(tableEnv, "DROP CATALOG " + EXCLUDED_CATALOG);
    Assertions.assertFalse(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));
    Assertions.assertThrows(
        Exception.class, () -> execute(tableEnv, "USE CATALOG " + EXCLUDED_CATALOG));

    // A new session catalog can reuse the name after it was dropped.
    execute(tableEnv, CREATE_EXCLUDED_CATALOG);
    Assertions.assertTrue(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));

    verifyGravitinoCatalogNeverTouched(EXCLUDED_CATALOG);
  }

  @Test
  void testExcludedCatalog_dropWithoutSessionCatalog_neverDropsGravitinoCatalog() {
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of(EXCLUDED_CATALOG));

    Throwable exception =
        Assertions.assertThrows(
            Exception.class, () -> execute(tableEnv, "DROP CATALOG " + EXCLUDED_CATALOG));
    Assertions.assertTrue(
        allMessages(exception).contains("does not exist"), allMessages(exception));
    execute(tableEnv, "DROP CATALOG IF EXISTS " + EXCLUDED_CATALOG);

    verifyGravitinoCatalogNeverTouched(EXCLUDED_CATALOG);
  }

  @Test
  void testExcludedCatalog_otherGravitinoCatalogsKeepExistingBehavior() {
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of(EXCLUDED_CATALOG));
    execute(tableEnv, CREATE_EXCLUDED_CATALOG);

    execute(tableEnv, "USE CATALOG " + GRAVITINO_ONLY_CATALOG);
    execute(tableEnv, "USE CATALOG default_catalog");
    execute(tableEnv, "DROP CATALOG " + GRAVITINO_ONLY_CATALOG);

    verify(gravitinoCatalogStore, atLeastOnce()).getCatalog(GRAVITINO_ONLY_CATALOG);
    verify(gravitinoCatalogStore).removeCatalog(eq(GRAVITINO_ONLY_CATALOG), anyBoolean());
    verifyGravitinoCatalogNeverTouched(EXCLUDED_CATALOG);
  }

  /**
   * Creating a Gravitino-managed catalog type under an excluded name must fail before Flink
   * instantiates and registers the catalog. Otherwise the registered catalog would resolve the
   * excluded Gravitino catalog with the same name. Runs where the {@code gravitino-paimon} factory
   * is registered, i.e. in the Flink-minor modules.
   */
  @Test
  void testExcludedCatalog_gravitinoManagedType_isRejectedBeforeRegistration() {
    Assumptions.assumeTrue(
        isCatalogFactoryRegistered(GRAVITINO_PAIMON),
        "The gravitino-paimon catalog factory is not registered in this module");
    Configuration sessionConfiguration = new Configuration();
    sessionConfiguration.setString(
        "table.catalog-store.gravitino.gravitino.excludedCatalogs", EXCLUDED_CATALOG);
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of(EXCLUDED_CATALOG), sessionConfiguration);

    Throwable exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                execute(
                    tableEnv,
                    "CREATE CATALOG "
                        + EXCLUDED_CATALOG
                        + " WITH ('type'='"
                        + GRAVITINO_PAIMON
                        + "', 'catalog-backend'='filesystem', 'warehouse'='/tmp/unused')"));

    Assertions.assertTrue(
        allMessages(exception).contains("gravitino.excludedCatalogs"), allMessages(exception));
    Assertions.assertFalse(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));
    Assertions.assertFalse(tableEnv.getCatalog(EXCLUDED_CATALOG).isPresent());
    Assertions.assertThrows(
        Exception.class, () -> execute(tableEnv, "USE CATALOG " + EXCLUDED_CATALOG));
    verifyGravitinoCatalogNeverTouched(EXCLUDED_CATALOG);
  }

  /** {@code CREATE CATALOG IF NOT EXISTS} and {@code ALTER CATALOG} are available since 1.20. */
  @Test
  void testExcludedCatalog_createIfNotExistsAndAlter_onlyAffectSessionCatalog() {
    Assumptions.assumeTrue(
        Arrays.stream(CatalogManager.class.getMethods())
            .anyMatch(method -> method.getName().equals("alterCatalog")),
        "CREATE CATALOG IF NOT EXISTS and ALTER CATALOG require Flink 1.20 or later");
    TableEnvironment tableEnv = tableEnv(ImmutableSet.of(EXCLUDED_CATALOG));

    // The Gravitino catalog is hidden, so IF NOT EXISTS creates the session catalog.
    execute(
        tableEnv,
        "CREATE CATALOG IF NOT EXISTS " + EXCLUDED_CATALOG + " WITH ('type'='generic_in_memory')");
    Assertions.assertTrue(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));
    execute(
        tableEnv,
        "CREATE CATALOG IF NOT EXISTS " + EXCLUDED_CATALOG + " WITH ('type'='generic_in_memory')");

    execute(
        tableEnv, "ALTER CATALOG " + EXCLUDED_CATALOG + " SET ('default-database'='session_db')");

    Assertions.assertTrue(listCatalogs(tableEnv).contains(EXCLUDED_CATALOG));
    Assertions.assertTrue(
        collect(tableEnv, "DESCRIBE CATALOG " + EXCLUDED_CATALOG).contains("generic_in_memory"));
    verifyGravitinoCatalogNeverTouched(EXCLUDED_CATALOG);
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private TableEnvironment tableEnv(Set<String> excludedCatalogs) {
    return tableEnv(excludedCatalogs, new Configuration());
  }

  private TableEnvironment tableEnv(
      Set<String> excludedCatalogs, Configuration sessionConfiguration) {
    GenericInMemoryCatalogStore memoryCatalogStore = new GenericInMemoryCatalogStore();
    memoryCatalogStore.open();
    GravitinoSessionCatalogStore store =
        new GravitinoSessionCatalogStore(
            gravitinoCatalogStore, memoryCatalogStore, excludedCatalogs);
    return TableEnvironment.create(
        EnvironmentSettings.newInstance()
            .withConfiguration(sessionConfiguration)
            .inBatchMode()
            .withCatalogStore(store)
            .build());
  }

  private static boolean isCatalogFactoryRegistered(String identifier) {
    for (Factory factory : ServiceLoader.load(Factory.class)) {
      if (factory instanceof CatalogFactory && identifier.equals(factory.factoryIdentifier())) {
        return true;
      }
    }
    return false;
  }

  private void verifyGravitinoCatalogNeverTouched(String catalogName) {
    verify(gravitinoCatalogStore, never()).contains(catalogName);
    verify(gravitinoCatalogStore, never()).getCatalog(catalogName);
    verify(gravitinoCatalogStore, never()).storeCatalog(eq(catalogName), any());
    verify(gravitinoCatalogStore, never()).removeCatalog(eq(catalogName), anyBoolean());
  }

  private static CatalogDescriptor inMemoryDescriptor(String catalogName) {
    Configuration configuration = new Configuration();
    configuration.set(CommonCatalogOptions.CATALOG_TYPE, "generic_in_memory");
    return CatalogDescriptor.of(catalogName, configuration);
  }

  private static List<String> listCatalogs(TableEnvironment tableEnv) {
    return Arrays.asList(tableEnv.listCatalogs());
  }

  private static void execute(TableEnvironment tableEnv, String sql) {
    collect(tableEnv, sql);
  }

  private static String collect(TableEnvironment tableEnv, String sql) {
    List<String> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tableEnv.executeSql(sql).collect()) {
      iterator.forEachRemaining(row -> rows.add(row.toString()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return String.join("\n", rows);
  }

  private static String allMessages(Throwable throwable) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      messages.append(current.getMessage()).append('\n');
    }
    return messages.toString();
  }
}
