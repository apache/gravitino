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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.function.Predicate;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretMaterial;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.Before;
import org.junit.Test;

public class TestGravitinoCatalogStore {
  private GravitinoCatalogManager gravitinoCatalogMockManager;
  private GravitinoCatalogStore gravitinoCatalogStore;

  @Before
  public void setupCatalogStore() {
    gravitinoCatalogMockManager = mock(GravitinoCatalogManager.class);
    gravitinoCatalogStore = new GravitinoCatalogStore(gravitinoCatalogMockManager);
  }

  @Test
  public void testRemoveCatalog_whenCatalogExists_shouldSucceed() {
    String catalogName = "testCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(true);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_whenCatalogNotExists_ignoreFlagTrue_shouldNotThrow() {
    String catalogName = "missingCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(false);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, true);
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_whenCatalogNotExists_ignoreFlagFalse_shouldThrow() {
    String catalogName = "missingCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(false);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
      fail("Expected CatalogException to be thrown");
    } catch (CatalogException e) {
      assertTrue(
          "Expected failure message to contain 'Failed to remove the catalog:'",
          e.getMessage().contains("Failed to remove the catalog:"));
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_UnexpectedException_shouldThrow() {
    String catalogName = "errorCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName))
        .thenThrow(new RuntimeException("UnexpectedErrorOccurred"));
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
      fail("Expected CatalogException to be thrown");
    } catch (CatalogException e) {
      assertTrue(
          "Expected failure message to contain 'Failed to remove the catalog:'",
          e.getMessage().contains("Failed to remove the catalog:"));
      assertTrue("Expected cause to be RuntimeException", e.getCause() instanceof RuntimeException);
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testDiscoverFactoriesSkipsServiceConfigurationError() {
    BaseCatalogFactory expectedFactory = mock(BaseCatalogFactory.class);
    Predicate<Factory> predicate = factory -> true;

    BaseCatalogFactory actualFactory =
        gravitinoCatalogStore.discoverFactories(
            new Iterator<Factory>() {
              private int index;

              @Override
              public boolean hasNext() {
                return index < 2;
              }

              @Override
              public Factory next() {
                if (index++ == 0) {
                  throw new ServiceConfigurationError(
                      "Missing optional factory dependency",
                      new NoClassDefFoundError("missing.optional.Factory"));
                }
                return expectedFactory;
              }
            },
            predicate,
            "Unexpected factory loading error.");

    assertSame(expectedFactory, actualFactory);
  }

  @Test
  public void testMergeSecrets() {
    Catalog catalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    when(gravitinoCatalogMockManager.getGravitinoCatalogInfo("sec")).thenReturn(catalog);
    when(catalog.provider()).thenReturn("test-provider");
    when(catalog.properties()).thenReturn(Map.of("visible", "v1"));
    when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenReturn(Map.of("jdbc-password", "secret"));

    BaseCatalogFactory factory = mock(BaseCatalogFactory.class);
    CatalogPropertiesConverter converter = mock(CatalogPropertiesConverter.class);
    when(factory.catalogPropertiesConverter()).thenReturn(converter);
    when(converter.toFlinkCatalogProperties(org.mockito.ArgumentMatchers.anyMap()))
        .thenAnswer(
            invocation -> {
              Map<String, String> in = invocation.getArgument(0);
              Map<String, String> out = new HashMap<>(in);
              out.put("type", "generic_in_memory");
              return out;
            });

    GravitinoCatalogStore store =
        new GravitinoCatalogStore(gravitinoCatalogMockManager) {
          @Override
          BaseCatalogFactory catalogFactoryForProvider(String provider) {
            return factory;
          }
        };

    Optional<CatalogDescriptor> descriptor = store.getCatalog("sec");
    assertTrue(descriptor.isPresent());
    assertEquals("secret", descriptor.get().getConfiguration().toMap().get("jdbc-password"));
    assertEquals("v1", descriptor.get().getConfiguration().toMap().get("visible"));
  }

  @Test
  public void testMergeSecretsNullProps() {
    Catalog catalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    when(gravitinoCatalogMockManager.getGravitinoCatalogInfo("sec-null")).thenReturn(catalog);
    when(catalog.provider()).thenReturn("test-provider");
    when(catalog.properties()).thenReturn(null);
    when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenReturn(Map.of("jdbc-password", "secret"));

    BaseCatalogFactory factory = mock(BaseCatalogFactory.class);
    CatalogPropertiesConverter converter = mock(CatalogPropertiesConverter.class);
    when(factory.catalogPropertiesConverter()).thenReturn(converter);
    when(converter.toFlinkCatalogProperties(org.mockito.ArgumentMatchers.anyMap()))
        .thenAnswer(
            invocation -> {
              Map<String, String> in = invocation.getArgument(0);
              Map<String, String> out = new HashMap<>(in);
              out.put("type", "generic_in_memory");
              return out;
            });

    GravitinoCatalogStore store =
        new GravitinoCatalogStore(gravitinoCatalogMockManager) {
          @Override
          BaseCatalogFactory catalogFactoryForProvider(String provider) {
            return factory;
          }
        };

    Optional<CatalogDescriptor> descriptor = store.getCatalog("sec-null");
    assertTrue(descriptor.isPresent());
    assertEquals("secret", descriptor.get().getConfiguration().toMap().get("jdbc-password"));
  }

  @Test
  public void testMergeMemorySecrets() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>();
      entityProps.put("jdbc-user", "root");
      java.util.List<SecretMaterial> writes =
          sm.assembleSecretMaterials(
              Map.of("jdbc-user", "root"),
              entityProps,
              "catalog",
              2L,
              Map.of("jdbc-password", new SecretBinding("memory", "mem-pwd")),
              Map.of());
      sm.writeSecrets(writes);
      Map<String, String> secrets = SecretPropertyUtils.buildSecrets(sm, entityProps);

      Catalog catalog = mock(Catalog.class);
      SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
      when(gravitinoCatalogMockManager.getGravitinoCatalogInfo("mem")).thenReturn(catalog);
      when(catalog.provider()).thenReturn("test-provider");
      when(catalog.properties()).thenReturn(Map.of("jdbc-url", "jdbc:mysql://localhost/db"));
      when(catalog.supportsSecrets()).thenReturn(supportsSecrets);
      when(supportsSecrets.getSecrets()).thenReturn(secrets);

      BaseCatalogFactory factory = mock(BaseCatalogFactory.class);
      CatalogPropertiesConverter converter = mock(CatalogPropertiesConverter.class);
      when(factory.catalogPropertiesConverter()).thenReturn(converter);
      when(converter.toFlinkCatalogProperties(org.mockito.ArgumentMatchers.anyMap()))
          .thenAnswer(
              invocation -> {
                Map<String, String> in = invocation.getArgument(0);
                Map<String, String> out = new HashMap<>(in);
                out.put("type", "generic_in_memory");
                return out;
              });

      GravitinoCatalogStore store =
          new GravitinoCatalogStore(gravitinoCatalogMockManager) {
            @Override
            BaseCatalogFactory catalogFactoryForProvider(String provider) {
              return factory;
            }
          };

      Optional<CatalogDescriptor> descriptor = store.getCatalog("mem");
      assertTrue(descriptor.isPresent());
      assertEquals("mem-pwd", descriptor.get().getConfiguration().toMap().get("jdbc-password"));
    }
  }

  private static SecretManager memorySecretManager() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    config.loadFromProperties(properties);
    return new SecretManager(config);
  }
}
