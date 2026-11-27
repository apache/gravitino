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
package org.apache.gravitino.paimon.service;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProviderFactory;
import org.apache.gravitino.paimon.service.provider.StaticPaimonConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonConfigProvider {

  @Test
  public void testUnsupportedProviderFailsExplicitly() {
    for (String name : new String[] {"unsupported", "typo"}) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PaimonConfigProviderFactory.create(Map.of("catalog-config-provider", name)));
      Assertions.assertTrue(error.getMessage().contains(name));
    }
  }

  @Test
  public void testFactoryDefaultsToStaticProvider() throws Exception {
    PaimonConfigProvider provider =
        PaimonConfigProviderFactory.create(ImmutableMap.of("warehouse", "/tmp/w"));
    Assertions.assertInstanceOf(StaticPaimonConfigProvider.class, provider);
    provider.close();
  }

  @Test
  public void testStaticProviderGroupsPerCatalogConfigs() throws Exception {
    PaimonConfigProvider provider =
        PaimonConfigProviderFactory.create(
            ImmutableMap.<String, String>builder()
                .put("catalog-backend", "filesystem")
                .put("warehouse", "/tmp/default")
                .put("catalog.sales.catalog-backend", "jdbc")
                .put("catalog.sales.uri", "jdbc:postgresql://127.0.0.1:5432/db")
                .put("catalog.sales.warehouse", "/tmp/sales")
                .build());

    Assertions.assertTrue(provider.catalogNames().contains("default_catalog"));
    Assertions.assertTrue(provider.catalogNames().contains("sales"));

    Map<String, String> defaultConfig =
        provider
            .getCatalogConfig(PaimonCatalogWrapperManager.DEFAULT_CATALOG_NAME)
            .orElseThrow()
            .properties();
    Assertions.assertEquals("/tmp/default", defaultConfig.get("warehouse"));
    // Per-catalog keys must not leak into the default catalog configuration.
    Assertions.assertFalse(defaultConfig.containsKey("catalog.sales.catalog-backend"));

    Map<String, String> salesConfig = provider.getCatalogConfig("sales").orElseThrow().properties();
    Assertions.assertEquals("jdbc", salesConfig.get("catalog-backend"));
    Assertions.assertEquals("/tmp/sales", salesConfig.get("warehouse"));
    Assertions.assertTrue(provider.getCatalogConfig("unknown").isEmpty());
    provider.close();
  }
}
