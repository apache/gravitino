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
package org.apache.gravitino.iceberg.service;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestIcebergCatalogWrapperManagerForREST {

  private static final String DEFAULT_CATALOG = "memory";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    // Mock CatalogManager for GravitinoEnv to avoid initialization errors
    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
  }

  @AfterAll
  public static void tearDown() throws IllegalAccessException {
    // Clean up GravitinoEnv
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", null, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "hello/", "\\\n\t\\\'/", "\u0024/", "\100/", "[_~/"})
  public void testValidGetOps(String rawPrefix) {
    String prefix = rawPrefix;
    if (!StringUtils.isBlank(rawPrefix)) {
      prefix = rawPrefix.substring(0, rawPrefix.length() - 1);
    }
    Map<String, String> config = Maps.newHashMap();
    config.put(String.format("catalog.%s.catalog-backend-name", prefix), prefix);
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(
            config, configProvider, false, configProvider.getMetalakeName().orElse(null));
    IcebergRESTServerContext.create(configProvider, false, false, manager);

    IcebergCatalogWrapper ops = manager.getOps(rawPrefix);

    if (StringUtils.isBlank(prefix)) {
      Assertions.assertEquals(ops.getCatalog().name(), DEFAULT_CATALOG);
    } else {
      Assertions.assertEquals(ops.getCatalog().name(), prefix);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"hello", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  public void testInvalidGetOps(String rawPrefix) {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(
            config, configProvider, false, configProvider.getMetalakeName().orElse(null));
    IcebergRESTServerContext.create(configProvider, false, false, manager);

    Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> manager.getOps(rawPrefix));
  }

  @Test
  public void testAuthorizationRequiresDynamicProvider() {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(
            config, configProvider, true, configProvider.getMetalakeName().orElse(null));
    IcebergRESTServerContext.create(configProvider, true, true, manager);

    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> manager.getCatalogWrapper("any"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("gravitino.iceberg-rest.catalog-config-provider=dynamic-config-provider"));
  }
}
