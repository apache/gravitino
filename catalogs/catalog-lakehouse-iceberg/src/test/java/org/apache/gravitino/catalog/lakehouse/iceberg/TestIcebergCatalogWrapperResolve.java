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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.CatalogWrapperManager;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.iceberg.exceptions.NoSuchWarehouseException;
import org.apache.iceberg.exceptions.RESTException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergCatalogWrapperResolve {

  @AfterEach
  void tearDown() throws Exception {
    Field instance =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow()
            .getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
  }

  @Test
  void providerUsesIrcManagerWithConfigWhenAux() {
    IcebergConfig config = new IcebergConfig();
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(config);
    CatalogWrapperManager manager = stubAuxManager(config, shared);

    Assertions.assertSame(shared, IcebergCatalogWrapperProvider.create("c1", config).get());
    Mockito.verify(manager).getOrCreateCatalogWrapper("c1", config);
  }

  @Test
  void providerMapsNoSuchWarehouseToIllegalArgumentOnIrcPath() {
    IcebergConfig config =
        config(
            IcebergConstants.CATALOG_BACKEND, "rest",
            IcebergConstants.WAREHOUSE, "s3://not-a-real-catalog/");
    CatalogWrapperManager manager =
        stubAuxManager(
            config,
            brokenWrapper(
                new NoSuchWarehouseException(
                    "Warehouse not found: %s", "s3://not-a-real-catalog/")));

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            IcebergCatalogWrapperProvider.create("c1", config)::get);
    Assertions.assertTrue(ex.getMessage().contains("selects a catalog by name"));
    Assertions.assertTrue(ex.getMessage().contains("s3://not-a-real-catalog/"));
    Mockito.verify(manager).invalidateCatalogWrapper("c1");
  }

  @Test
  void providerMapsUnreachableRestToConnectionFailedOnIrcPath() {
    IcebergConfig config =
        config(
            IcebergConstants.CATALOG_BACKEND, "rest",
            IcebergConstants.URI, "http://127.0.0.1:1/iceberg/");
    CatalogWrapperManager manager =
        stubAuxManager(
            config,
            brokenWrapper(new RESTException("Error occurred while processing GET request")));

    Assertions.assertThrows(
        ConnectionFailedException.class, IcebergCatalogWrapperProvider.create("c1", config)::get);
    Mockito.verify(manager).invalidateCatalogWrapper("c1");
  }

  @Test
  void catalogAndViewOpsResolveViaCatalogWrapperEachCall() {
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergCatalogWrapperProvider provider = Mockito.mock(IcebergCatalogWrapperProvider.class);
    Mockito.when(provider.get()).thenReturn(shared);

    IcebergCatalogOperations catalogOps = new IcebergCatalogOperations();
    catalogOps.wrapperProvider = provider;
    Assertions.assertSame(shared, catalogOps.catalogWrapper());
    Assertions.assertSame(shared, catalogOps.catalogWrapper());
    Assertions.assertNotNull(new IcebergViewCatalogOperations(provider));
  }

  @Test
  void nonAuxProviderUsesStandaloneManagerAndReusesWrapper() {
    IcebergConfig config = config(IcebergConstants.CATALOG_BACKEND, "memory");
    IcebergCatalogWrapperProvider provider =
        IcebergCatalogWrapperProvider.create("local-cat", config);
    try {
      IcebergCatalogWrapper first = provider.get();
      Assertions.assertSame(first, provider.get());
      provider.invalidate();
      Assertions.assertNotSame(first, provider.get());
    } finally {
      provider.close();
    }
  }

  private static CatalogWrapperManager stubAuxManager(
      IcebergConfig config, IcebergCatalogWrapper wrapper) {
    CatalogWrapperManager manager = Mockito.mock(CatalogWrapperManager.class);
    Mockito.when(manager.getOrCreateCatalogWrapper("c1", config)).thenReturn(wrapper);
    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn("ml");
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("c1");
    IcebergRESTServerContext.create(configProvider, false, true, false, manager);
    return manager;
  }

  private static IcebergCatalogWrapper brokenWrapper(RuntimeException onGetCatalog) {
    IcebergCatalogWrapper broken = Mockito.mock(IcebergCatalogWrapper.class);
    Mockito.when(broken.getCatalog()).thenThrow(onGetCatalog);
    return broken;
  }

  private static IcebergConfig config(String... kv) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (int i = 0; i < kv.length; i += 2) {
      builder.put(kv[i], kv[i + 1]);
    }
    return new IcebergConfig(builder.build());
  }
}
