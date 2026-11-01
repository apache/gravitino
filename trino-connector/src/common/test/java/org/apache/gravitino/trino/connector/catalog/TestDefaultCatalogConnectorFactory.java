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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDefaultCatalogConnectorFactory {

  @Test
  void testBuiltInProviders() {
    DefaultCatalogConnectorFactory factory = new DefaultCatalogConnectorFactory(config());
    Assertions.assertTrue(factory.getSupportedCatalogProviders().contains("hive"));
    Assertions.assertTrue(factory.getSupportedCatalogProviders().contains("jdbc-mysql"));
    Assertions.assertTrue(factory.getSupportedCatalogProviders().contains("jdbc-postgresql"));
  }

  @Test
  void testAdapterProviderIsRegistered() {
    DefaultCatalogConnectorFactory factory = new DefaultCatalogConnectorFactory(config());
    Assertions.assertTrue(
        factory
            .getSupportedCatalogProviders()
            .contains(FakeCatalogConnectorAdapterProvider.PROVIDER));
  }

  @Test
  void testBuiltInProviderWinsOverAdapterProvider() {
    // DuplicateCatalogConnectorAdapterProvider throws if asked for an adapter.
    Assertions.assertDoesNotThrow(() -> new DefaultCatalogConnectorFactory(config()));
  }

  @Test
  void testUnloadableAdapterProviderIsSkipped() {
    DefaultCatalogConnectorFactory factory = new DefaultCatalogConnectorFactory(config());
    Assertions.assertFalse(
        factory
            .getSupportedCatalogProviders()
            .contains(BrokenCatalogConnectorAdapterProvider.PROVIDER));
    Assertions.assertFalse(
        factory
            .getSupportedCatalogProviders()
            .contains(FailingCatalogConnectorAdapterProvider.PROVIDER));
  }

  private static GravitinoConfig config() {
    return new GravitinoConfig(
        ImmutableMap.of("gravitino.uri", "http://localhost:8090", "gravitino.metalake", "test"));
  }
}
