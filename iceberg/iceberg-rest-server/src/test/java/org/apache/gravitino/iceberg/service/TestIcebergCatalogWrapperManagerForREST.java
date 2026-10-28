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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
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
  private static CatalogManager mockCatalogManager;

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    // Mock CatalogManager for GravitinoEnv to avoid initialization errors
    mockCatalogManager = Mockito.mock(CatalogManager.class);
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
            config, configProvider, false, configProvider.getMetalakeName());
    IcebergRESTServerContext.create(configProvider, false, false, true, manager);

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
            config, configProvider, false, configProvider.getMetalakeName());
    IcebergRESTServerContext.create(configProvider, false, false, true, manager);

    Assertions.assertThrowsExactly(IllegalArgumentException.class, () -> manager.getOps(rawPrefix));
  }

  @Test
  public void testAuthorizationRequiresDynamicProvider() {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(config, configProvider, true, "metalake");
    IcebergRESTServerContext.create(configProvider, true, true, true, manager);

    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> manager.getCatalogWrapper("any"));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("gravitino.iceberg-rest.catalog-config-provider=dynamic-config-provider"));
  }

  @Test
  public void testCreateFederatedWrapperForRestBackend() {
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.URI,
                "http://localhost:8181"));

    CatalogWrapperForREST wrapper = newManager().createCatalogWrapper("test", icebergConfig);

    Assertions.assertInstanceOf(FederatedCatalogWrapper.class, wrapper);
  }

  @Test
  public void testCreateBaseWrapperForNonRestBackend() {
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));

    CatalogWrapperForREST wrapper = newManager().createCatalogWrapper("test", icebergConfig);

    Assertions.assertFalse(wrapper instanceof FederatedCatalogWrapper);
    Assertions.assertEquals(CatalogWrapperForREST.class, wrapper.getClass());
  }

  @Test
  public void testNewerCatalogVersionUsesNewCacheEntryWithoutClosingTheOldOne() throws Exception {
    DynamicIcebergConfigProvider configProvider = Mockito.mock(DynamicIcebergConfigProvider.class);
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("test");
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    Mockito.when(configProvider.getIcebergCatalogConfig(Mockito.anyString()))
        .thenReturn(Optional.of(icebergConfig));

    AtomicReference<Long> version = new AtomicReference<>(1L);
    try (IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(Maps.newHashMap(), configProvider, true, "metalake") {
          @Override
          protected Optional<Long> catalogCurrentVersion(String catalogName) {
            return Optional.of(version.get());
          }
        }) {
      IcebergRESTServerContext.create(configProvider, false, false, true, manager);
      CatalogWrapperForREST original = manager.getCatalogWrapper("test");

      CatalogWrapperForREST sameVersion = manager.getCatalogWrapper("test");
      Assertions.assertSame(original, sameVersion);

      version.set(2L);
      CatalogWrapperForREST replacement = manager.getCatalogWrapper("test");
      Assertions.assertNotSame(original, replacement);
      Assertions.assertFalse(original.isClosed());
      Assertions.assertFalse(replacement.isClosed());

      IcebergCatalogWrapper fromConfig = manager.getOrCreateCatalogWrapper("test", icebergConfig);
      Assertions.assertSame(replacement, fromConfig);
    }
  }

  @Test
  public void testMissingCatalogVersionFailsFastAndClearsCache() throws Exception {
    DynamicIcebergConfigProvider configProvider = Mockito.mock(DynamicIcebergConfigProvider.class);
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("test");
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    Mockito.when(configProvider.getIcebergCatalogConfig(Mockito.anyString()))
        .thenReturn(Optional.of(icebergConfig));

    AtomicReference<Optional<Long>> version = new AtomicReference<>(Optional.of(1L));
    try (IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(Maps.newHashMap(), configProvider, true, "metalake") {
          @Override
          protected Optional<Long> catalogCurrentVersion(String catalogName) {
            return version.get();
          }
        }) {
      IcebergRESTServerContext.create(configProvider, false, false, true, manager);
      CatalogWrapperForREST original = manager.getCatalogWrapper("test");
      Assertions.assertFalse(original.isClosed());

      version.set(Optional.empty());
      Assertions.assertThrows(
          NoSuchCatalogException.class, () -> manager.getCatalogWrapper("test"));
      Assertions.assertThrows(
          NoSuchCatalogException.class,
          () -> manager.getOrCreateCatalogWrapper("test", icebergConfig));
      Assertions.assertTrue(original.isClosed());
    }
  }

  @Test
  public void testStaticAuxModeSkipsCatalogVersionCache() throws Exception {
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(Maps.newHashMap());
    configProvider.initialize(Maps.newHashMap());
    IcebergConfig icebergConfig =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse"));
    try (IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(
            Maps.newHashMap(), configProvider, true, configProvider.getMetalakeName())) {
      IcebergRESTServerContext.create(configProvider, false, true, false, manager);
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("iceberg", icebergConfig);
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("iceberg", icebergConfig);
      Assertions.assertSame(first, second);
      Assertions.assertFalse(first.isClosed());
    }
  }

  @Test
  public void testDynamicAuxModeRequiresMetalakeName() {
    DynamicIcebergConfigProvider configProvider = Mockito.mock(DynamicIcebergConfigProvider.class);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new IcebergCatalogWrapperManager(Maps.newHashMap(), configProvider, true, " "));
  }

  private static IcebergCatalogWrapperManager newManager() {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    return new IcebergCatalogWrapperManager(
        config, configProvider, false, configProvider.getMetalakeName());
  }
}
