/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDynamicIcebergConfigProvider {

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    // Create IcebergRESTServerContext with authorization disabled by default
    createMockServerContext(false);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    // Clean up GravitinoEnv and IcebergRESTServerContext state after each test
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", null, true);
    resetServerContext();
  }

  private void createMockServerContext(boolean authorizationEnabled) throws IllegalAccessException {
    // Use reflection to set the IcebergRESTServerContext instance
    IcebergConfigProvider mockProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(mockProvider.getMetalakeName()).thenReturn("test_metalake");
    Mockito.when(mockProvider.getDefaultCatalogName()).thenReturn("default_catalog");
    // When authorization is enabled, it implies running in auxiliary mode
    IcebergRESTServerContext.create(mockProvider, authorizationEnabled, authorizationEnabled, null);
  }

  private void resetServerContext() throws IllegalAccessException {
    // Reset the IcebergRESTServerContext singleton by finding InstanceHolder class by name
    // This approach is more robust than using array index which can break if class order changes
    Class<?> holderClass =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("InstanceHolder class not found"));
    FieldUtils.writeStaticField(holderClass, "INSTANCE", null, true);
  }

  /**
   * Creates a mock CatalogFetcher and sets it on the provider using the test-visible setter. This
   * approach avoids reflection and is more maintainable.
   */
  private void setMockCatalogFetcher(
      DynamicIcebergConfigProvider provider, Map<String, Catalog> catalogMap) {
    DynamicIcebergConfigProvider.CatalogFetcher mockFetcher =
        catalogName -> {
          Catalog catalog = catalogMap.get(catalogName);
          if (catalog == null) {
            throw new NoSuchCatalogException("Catalog not found: %s", catalogName);
          }
          return catalog;
        };
    provider.setCatalogFetcher(mockFetcher);
  }

  @Test
  public void testValidIcebergTableOps() {
    String metalakeName = "test_metalake";
    String hiveCatalogName = "hive_backend";
    String jdbcCatalogName = "jdbc_backend";

    Catalog hiveMockCatalog = Mockito.mock(Catalog.class);
    Catalog jdbcMockCatalog = Mockito.mock(Catalog.class);

    Mockito.when(hiveMockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(jdbcMockCatalog.provider()).thenReturn("lakehouse-iceberg");

    Mockito.when(hiveMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "hive");
                put(IcebergConstants.URI, "thrift://127.0.0.1:7004");
                put(IcebergConstants.WAREHOUSE, "/tmp/usr/hive/warehouse");
                put(IcebergConstants.CATALOG_BACKEND_NAME, hiveCatalogName);
              }
            });
    Mockito.when(jdbcMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "jdbc");
                put(IcebergConstants.URI, "jdbc:sqlite::memory:");
                put(IcebergConstants.WAREHOUSE, "/tmp/user/hive/warehouse-jdbc");
                put(IcebergConstants.GRAVITINO_JDBC_USER, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
                put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
                put(IcebergConstants.CATALOG_BACKEND_NAME, jdbcCatalogName);
              }
            });

    // Initialize provider with properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_URI, "http://localhost:8090");
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Set mock catalog fetcher
    Map<String, Catalog> catalogMap = new HashMap<>();
    catalogMap.put(hiveCatalogName, hiveMockCatalog);
    catalogMap.put(jdbcCatalogName, jdbcMockCatalog);
    setMockCatalogFetcher(provider, catalogMap);

    IcebergCatalogWrapper hiveOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(hiveCatalogName).get());
    IcebergCatalogWrapper jdbcOps =
        new IcebergCatalogWrapper(provider.getIcebergCatalogConfig(jdbcCatalogName).get());

    Assertions.assertEquals(hiveCatalogName, hiveOps.getCatalog().name());
    Assertions.assertEquals(jdbcCatalogName, jdbcOps.getCatalog().name());

    Assertions.assertTrue(hiveOps.getCatalog() instanceof HiveCatalog);
    Assertions.assertTrue(jdbcOps.getCatalog() instanceof JdbcCatalog);
  }

  @Test
  public void testInvalidIcebergTableOps() {
    String metalakeName = "test_metalake";
    String invalidCatalogName = "invalid_catalog";

    Catalog invalidCatalog = Mockito.mock(Catalog.class);
    Mockito.when(invalidCatalog.provider()).thenReturn("hive");

    // Initialize provider with properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_URI, "http://localhost:8090");
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Set mock catalog fetcher
    Map<String, Catalog> catalogMap = new HashMap<>();
    catalogMap.put(invalidCatalogName, invalidCatalog);
    setMockCatalogFetcher(provider, catalogMap);

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> provider.getIcebergCatalogConfig(invalidCatalogName));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> provider.getIcebergCatalogConfig(""));
  }

  @Test
  public void testCustomProperties() {
    String metalakeName = "test_metalake";
    String customCatalogName = "custom_backend";
    String customKey1 = "custom-k1";
    String customValue1 = "custom-v1";
    String customKey2 = "custom-k2";
    String customValue2 = "custom-v2";

    Catalog customMockCatalog = Mockito.mock(Catalog.class);
    Mockito.when(customMockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(customMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "custom");
                put(IcebergConstants.CATALOG_BACKEND_NAME, customCatalogName);
                put("gravitino.bypass." + customKey1, customValue1);
                put(customKey2, customValue2);
              }
            });

    // Initialize provider with properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_URI, "http://localhost:8090");
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Set mock catalog fetcher
    Map<String, Catalog> catalogMap = new HashMap<>();
    catalogMap.put(customCatalogName, customMockCatalog);
    setMockCatalogFetcher(provider, catalogMap);

    Optional<IcebergConfig> icebergCatalogConfig =
        provider.getIcebergCatalogConfig(customCatalogName);
    Assertions.assertTrue(icebergCatalogConfig.isPresent());
    Map<String, String> icebergCatalogProperties =
        icebergCatalogConfig.get().getIcebergCatalogProperties();
    Assertions.assertEquals(icebergCatalogProperties.get(customKey1), customValue1);
    Assertions.assertTrue(icebergCatalogProperties.containsKey(customKey2));
    Assertions.assertEquals(
        icebergCatalogProperties.get(IcebergConstants.CATALOG_BACKEND_NAME), customCatalogName);
  }

  @Test
  void testIcebergConfig() {
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("catalog.backend", "custom");
    catalogProperties.put("catalog.backend-name", "custom_backend");
    catalogProperties.put("gravitino.bypass.custom-k1", "custom-v1");
    catalogProperties.put("custom-k2", "custom-v2");

    IcebergConfig icebergConfig =
        DynamicIcebergConfigProvider.getIcebergConfigFromCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        icebergConfig.getIcebergCatalogProperties().get("custom-k1"), "custom-v1");
    Assertions.assertTrue(icebergConfig.getIcebergCatalogProperties().containsKey("custom-k2"));
    Assertions.assertEquals(
        icebergConfig.getIcebergCatalogProperties().get("catalog.backend-name"), "custom_backend");
  }

  @Test
  public void testInternalCatalogFetcher() throws IllegalAccessException {
    String metalakeName = "test_metalake";
    String catalogName = "internal_catalog";

    // Enable authorization to use internal fetcher
    createMockServerContext(true);

    // Mock CatalogDispatcher
    CatalogDispatcher mockCatalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
    Mockito.when(mockCatalogDispatcher.loadCatalog(catalogIdent)).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(mockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "custom");
                put(IcebergConstants.CATALOG_BACKEND_NAME, catalogName);
              }
            });

    // Set the mock CatalogDispatcher to GravitinoEnv
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogDispatcher", mockCatalogDispatcher, true);

    // Initialize provider with required properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Test that internal interface is used (CatalogDispatcher should be called)
    Optional<IcebergConfig> icebergConfig = provider.getIcebergCatalogConfig(catalogName);

    Assertions.assertTrue(icebergConfig.isPresent());
    Mockito.verify(mockCatalogDispatcher).loadCatalog(catalogIdent);
  }

  @Test
  public void testHttpCatalogFetcherUsedWhenAuthorizationDisabled() throws IllegalAccessException {
    String metalakeName = "test_metalake";
    String catalogName = "http_catalog";

    // Authorization is disabled (default in setUp)

    // Mock catalog
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    Mockito.when(mockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(mockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "custom");
                put(IcebergConstants.CATALOG_BACKEND_NAME, catalogName);
              }
            });

    // Initialize provider with properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_URI, "http://localhost:8090");
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Set mock catalog fetcher
    Map<String, Catalog> catalogMap = new HashMap<>();
    catalogMap.put(catalogName, mockCatalog);
    setMockCatalogFetcher(provider, catalogMap);

    // Test that catalog can be loaded
    Optional<IcebergConfig> icebergConfig = provider.getIcebergCatalogConfig(catalogName);

    Assertions.assertTrue(icebergConfig.isPresent());
  }

  @Test
  public void testInternalCatalogFetcherWithNullCatalogDispatcher() throws IllegalAccessException {
    String metalakeName = "test_metalake";
    String catalogName = "internal_catalog";

    // Enable authorization to use internal fetcher
    createMockServerContext(true);

    // Ensure CatalogDispatcher is null (simulating GravitinoEnv not initialized)
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", null, true);

    // Initialize provider with required properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Test that IllegalStateException is thrown when CatalogDispatcher is null
    Assertions.assertThrows(
        IllegalStateException.class, () -> provider.getIcebergCatalogConfig(catalogName));
  }

  @Test
  public void testInternalCatalogFetcherNoSuchCatalogException() throws IllegalAccessException {
    String metalakeName = "test_metalake";
    String nonExistentCatalogName = "non_existent_catalog";

    // Enable authorization to use internal fetcher
    createMockServerContext(true);

    // Mock CatalogDispatcher to throw NoSuchCatalogException
    CatalogDispatcher mockCatalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    NameIdentifier catalogIdent =
        NameIdentifierUtil.ofCatalog(metalakeName, nonExistentCatalogName);
    Mockito.when(mockCatalogDispatcher.loadCatalog(catalogIdent))
        .thenThrow(new NoSuchCatalogException("Catalog not found: %s", nonExistentCatalogName));

    // Set the mock CatalogDispatcher to GravitinoEnv
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogDispatcher", mockCatalogDispatcher, true);

    // Initialize provider with required properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Test that NoSuchCatalogException is properly handled and returns empty Optional
    Optional<IcebergConfig> result = provider.getIcebergCatalogConfig(nonExistentCatalogName);

    Assertions.assertFalse(result.isPresent());
    Mockito.verify(mockCatalogDispatcher).loadCatalog(catalogIdent);
  }

  @Test
  public void testConcurrentAccessToProvider() throws Exception {
    String metalakeName = "test_metalake";
    String catalogName = "concurrent_catalog";
    int numThreads = 10;

    // Mock catalog
    Catalog mockCatalog = Mockito.mock(Catalog.class);
    Mockito.when(mockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(mockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "custom");
                put(IcebergConstants.CATALOG_BACKEND_NAME, catalogName);
              }
            });

    // Initialize provider with properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_URI, "http://localhost:8090");
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Set mock catalog fetcher
    Map<String, Catalog> catalogMap = new HashMap<>();
    catalogMap.put(catalogName, mockCatalog);
    setMockCatalogFetcher(provider, catalogMap);

    // Create a latch to ensure all threads start at the same time
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Optional<IcebergConfig>>> futures = new ArrayList<>();

    // Submit concurrent tasks
    for (int i = 0; i < numThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await(); // Wait for all threads to be ready
                  return provider.getIcebergCatalogConfig(catalogName);
                } finally {
                  doneLatch.countDown();
                }
              }));
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    Assertions.assertTrue(completed, "All threads should complete within timeout");

    // Verify all results are present and consistent
    for (Future<Optional<IcebergConfig>> future : futures) {
      Optional<IcebergConfig> result = future.get();
      Assertions.assertTrue(result.isPresent(), "Each thread should get a valid config");
      Assertions.assertEquals(
          catalogName,
          result.get().getIcebergCatalogProperties().get(IcebergConstants.CATALOG_BACKEND_NAME));
    }

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void testConcurrentAccessWithInternalFetcher() throws Exception {
    String metalakeName = "test_metalake";
    String catalogName = "concurrent_internal_catalog";
    int numThreads = 10;

    // Enable authorization to use internal fetcher
    createMockServerContext(true);

    // Mock CatalogDispatcher
    CatalogDispatcher mockCatalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog mockCatalog = Mockito.mock(Catalog.class);

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
    Mockito.when(mockCatalogDispatcher.loadCatalog(catalogIdent)).thenReturn(mockCatalog);
    Mockito.when(mockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(mockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "custom");
                put(IcebergConstants.CATALOG_BACKEND_NAME, catalogName);
              }
            });

    // Set the mock CatalogDispatcher to GravitinoEnv
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "catalogDispatcher", mockCatalogDispatcher, true);

    // Initialize provider with required properties
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.GRAVITINO_METALAKE, metalakeName);

    DynamicIcebergConfigProvider provider = new DynamicIcebergConfigProvider();
    provider.initialize(properties);

    // Create a latch to ensure all threads start at the same time
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<Optional<IcebergConfig>>> futures = new ArrayList<>();

    // Submit concurrent tasks
    for (int i = 0; i < numThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await(); // Wait for all threads to be ready
                  return provider.getIcebergCatalogConfig(catalogName);
                } finally {
                  doneLatch.countDown();
                }
              }));
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    Assertions.assertTrue(completed, "All threads should complete within timeout");

    // Verify all results are present and consistent
    for (Future<Optional<IcebergConfig>> future : futures) {
      Optional<IcebergConfig> result = future.get();
      Assertions.assertTrue(result.isPresent(), "Each thread should get a valid config");
    }

    // Verify CatalogDispatcher was called (at least once, possibly more due to concurrency)
    Mockito.verify(mockCatalogDispatcher, Mockito.atLeastOnce()).loadCatalog(catalogIdent);

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }
}
