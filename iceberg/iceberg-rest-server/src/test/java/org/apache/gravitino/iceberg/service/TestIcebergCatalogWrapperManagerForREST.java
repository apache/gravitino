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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogBackendProvider;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.DynamicIcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProviderFactory;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
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
        new IcebergCatalogWrapperManager(
            config, configProvider, true, configProvider.getMetalakeName());
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
  public void testAuxModeStaticMemoryWrapperUsesIndependentBackend() {
    IcebergConfig icebergConfig =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(ImmutableMap.of(), configProvider, true, "metalake");

    CatalogWrapperForREST wrapper = manager.createCatalogWrapper("static", icebergConfig);

    Assertions.assertNotNull(wrapper.getCatalog());
  }

  @ParameterizedTest
  @ValueSource(strings = {"test", IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG})
  public void testAuxModeMemoryWrapperSharesCreateStateWithManagedBackend(String restCatalogName)
      throws Exception {
    String managedCatalogName = "test";
    InMemoryCatalog managedBackend = new InMemoryCatalog();
    managedBackend.initialize(managedCatalogName, ImmutableMap.of());
    CatalogManager.CatalogWrapper managedWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    TestCatalogOperations managedOperations = new TestCatalogOperations(managedBackend);
    Mockito.when(
            mockCatalogManager.loadCatalogAndWrap(
                NameIdentifier.of("metalake", managedCatalogName)))
        .thenReturn(managedWrapper);
    Mockito.when(managedWrapper.doWithCatalogOps(Mockito.any()))
        .thenAnswer(
            invocation -> {
              ThrowableFunction<CatalogOperations, ?> function = invocation.getArgument(0);
              return function.apply(managedOperations);
            });

    IcebergConfig icebergConfig =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    IcebergConfigProvider configProvider = Mockito.mock(DynamicIcebergConfigProvider.class);
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn(managedCatalogName);
    IcebergCatalogWrapperManager manager =
        new IcebergCatalogWrapperManager(ImmutableMap.of(), configProvider, true, "metalake");

    CatalogWrapperForREST wrapper = manager.createCatalogWrapper(restCatalogName, icebergConfig);
    Namespace namespace = Namespace.of("created_through_rest");
    wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    wrapper.createTable(
        namespace,
        CreateTableRequest.builder().withName("table").withSchema(schema).build(),
        false);
    wrapper.createView(
        namespace,
        ImmutableCreateViewRequest.builder()
            .name("view")
            .schema(schema)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(schema.schemaId())
                    .defaultNamespace(namespace)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT id FROM table")
                            .dialect("spark")
                            .build())
                    .build())
            .build());

    Assertions.assertSame(managedBackend, wrapper.getCatalog());
    Assertions.assertTrue(managedBackend.namespaceExists(namespace));
    Assertions.assertTrue(managedBackend.tableExists(TableIdentifier.of(namespace, "table")));
    Assertions.assertTrue(managedBackend.viewExists(TableIdentifier.of(namespace, "view")));
  }

  private static IcebergCatalogWrapperManager newManager() {
    Map<String, String> config = Maps.newHashMap();
    IcebergConfigProvider configProvider = IcebergConfigProviderFactory.create(config);
    configProvider.initialize(config);
    return new IcebergCatalogWrapperManager(
        config, configProvider, false, configProvider.getMetalakeName());
  }

  private static class TestCatalogOperations
      implements CatalogOperations, IcebergCatalogBackendProvider {
    private final InMemoryCatalog catalog;

    private TestCatalogOperations(InMemoryCatalog catalog) {
      this.catalog = catalog;
    }

    @Override
    public void initialize(
        Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata) {}

    @Override
    public InMemoryCatalog icebergCatalogBackend() {
      return catalog;
    }

    @Override
    public void close() {}
  }
}
