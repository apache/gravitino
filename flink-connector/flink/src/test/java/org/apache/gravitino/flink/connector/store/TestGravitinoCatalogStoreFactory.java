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

import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests verifying that {@link GravitinoCatalogStoreFactory} wires the correct {@link
 * CatalogStore} implementation based on the {@code gravitino.support.session.catalog} option.
 */
public class TestGravitinoCatalogStoreFactory {

  /**
   * When {@code gravitino.support.session.catalog=true} the factory must return a {@link
   * GravitinoSessionCatalogStore}.
   */
  @Test
  void testCreateCatalogStore_sessionCatalogEnabled_returnsGravitinoSessionCatalogStore()
      throws Exception {
    GravitinoCatalogStoreFactory factory = factoryWith(true);

    CatalogStore store = factory.createCatalogStore();

    Assertions.assertInstanceOf(GravitinoSessionCatalogStore.class, store);
  }

  /**
   * When {@code gravitino.support.session.catalog=false} (the default) the factory must return a
   * plain {@link GravitinoCatalogStore}, not a session store.
   */
  @Test
  void testCreateCatalogStore_sessionCatalogDisabled_returnsGravitinoCatalogStore()
      throws Exception {
    GravitinoCatalogStoreFactory factory = factoryWith(false);

    CatalogStore store = factory.createCatalogStore();

    Assertions.assertInstanceOf(GravitinoCatalogStore.class, store);
    Assertions.assertFalse(store instanceof GravitinoSessionCatalogStore);
  }

  /**
   * Verifies that {@code gravitino.support.session.catalog=true} is correctly parsed from the Flink
   * configuration by the factory's option helper.
   */
  @Test
  void testOptionParsing_supportSessionCatalogTrue_isReadFromConfig() {
    Configuration configuration = baseConfiguration();
    configuration.setBoolean(
        "table.catalog-store.gravitino.gravitino.support.session.catalog", true);

    boolean parsed = parseSupportSessionCatalog(configuration);

    Assertions.assertTrue(parsed);
  }

  /**
   * Verifies that {@code gravitino.support.session.catalog} defaults to {@code false} when absent
   * from the configuration.
   */
  @Test
  void testOptionParsing_supportSessionCatalogAbsent_defaultsToFalse() {
    boolean parsed = parseSupportSessionCatalog(baseConfiguration());

    Assertions.assertFalse(parsed);
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  /**
   * Builds a {@link GravitinoCatalogStoreFactory} with {@code supportSessionCatalog} and a mocked
   * {@link GravitinoCatalogManager} injected via reflection, bypassing {@code open()} which
   * requires a live Gravitino server.
   */
  private static GravitinoCatalogStoreFactory factoryWith(boolean supportSessionCatalog)
      throws Exception {
    GravitinoCatalogStoreFactory factory = new GravitinoCatalogStoreFactory();
    setField(factory, "catalogManager", mock(GravitinoCatalogManager.class));
    setField(factory, "supportSessionCatalog", supportSessionCatalog);
    return factory;
  }

  /**
   * Parses the value of {@code gravitino.support.session.catalog} from the given configuration
   * using the real Flink factory-helper path (same approach as {@link TestGravitinoFlinkConfig}).
   */
  private static boolean parseSupportSessionCatalog(Configuration configuration) {
    CatalogStoreFactory.Context context =
        TableFactoryUtil.buildCatalogStoreFactoryContext(
            configuration, TestGravitinoCatalogStoreFactory.class.getClassLoader());
    FactoryUtil.FactoryHelper<CatalogStoreFactory> helper =
        createCatalogStoreFactoryHelper(new GravitinoCatalogStoreFactory(), context);
    helper.validate();
    return helper
        .getOptions()
        .get(GravitinoCatalogStoreFactoryOptions.GRAVITINO_SUPPORT_SESSION_CATALOG);
  }

  private static Configuration baseConfiguration() {
    Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "test_metalake");
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://127.0.0.1:8090");
    return configuration;
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
