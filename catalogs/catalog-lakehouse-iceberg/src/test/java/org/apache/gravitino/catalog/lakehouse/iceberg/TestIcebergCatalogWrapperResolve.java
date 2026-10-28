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
    resetIcebergRESTServerContext();
  }

  @Test
  void providerUsesIrcManagerWithConfigWhenAux() {
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergConfig config = new IcebergConfig();
    CatalogWrapperManager manager = Mockito.mock(CatalogWrapperManager.class);
    Mockito.when(manager.getCatalogWrapper("c1", config)).thenReturn(shared);

    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn("ml");
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("c1");
    IcebergRESTServerContext.create(configProvider, false, true, false, manager);

    IcebergCatalogWrapperProvider provider = IcebergCatalogWrapperProvider.create("c1", config);
    Assertions.assertSame(shared, provider.get());
    Mockito.verify(manager).getCatalogWrapper("c1", config);
  }

  @Test
  void providerMapsNoSuchWarehouseToIllegalArgumentOnIrcPath() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.WAREHOUSE,
                "s3://not-a-real-catalog/"));
    IcebergCatalogWrapper broken = Mockito.mock(IcebergCatalogWrapper.class);
    Mockito.when(broken.getCatalog())
        .thenThrow(
            new NoSuchWarehouseException("Warehouse not found: %s", "s3://not-a-real-catalog/"));

    CatalogWrapperManager manager = Mockito.mock(CatalogWrapperManager.class);
    Mockito.when(manager.getCatalogWrapper("c1", config)).thenReturn(broken);

    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn("ml");
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("c1");
    IcebergRESTServerContext.create(configProvider, false, true, false, manager);

    IcebergCatalogWrapperProvider provider = IcebergCatalogWrapperProvider.create("c1", config);
    IllegalArgumentException ex =
        Assertions.assertThrows(IllegalArgumentException.class, provider::get);
    Assertions.assertTrue(ex.getMessage().contains("selects a catalog by name"));
    Assertions.assertTrue(ex.getMessage().contains("s3://not-a-real-catalog/"));
    Mockito.verify(manager).invalidateCatalogWrapper("c1");
  }

  @Test
  void providerMapsUnreachableRestToConnectionFailedOnIrcPath() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.URI,
                "http://127.0.0.1:1/iceberg/"));
    IcebergCatalogWrapper broken = Mockito.mock(IcebergCatalogWrapper.class);
    Mockito.when(broken.getCatalog())
        .thenThrow(new RESTException("Error occurred while processing GET request"));

    CatalogWrapperManager manager = Mockito.mock(CatalogWrapperManager.class);
    Mockito.when(manager.getCatalogWrapper("c1", config)).thenReturn(broken);

    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn("ml");
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn("c1");
    IcebergRESTServerContext.create(configProvider, false, true, false, manager);

    IcebergCatalogWrapperProvider provider = IcebergCatalogWrapperProvider.create("c1", config);
    Assertions.assertThrows(ConnectionFailedException.class, provider::get);
    Mockito.verify(manager).invalidateCatalogWrapper("c1");
  }

  @Test
  void catalogAndViewOpsShareSameProvider() {
    IcebergCatalogWrapper shared = new IcebergCatalogWrapper(new IcebergConfig());
    IcebergCatalogWrapperProvider provider = Mockito.mock(IcebergCatalogWrapperProvider.class);
    Mockito.when(provider.get()).thenReturn(shared);

    IcebergCatalogOperations catalogOps = new IcebergCatalogOperations();
    catalogOps.wrapperProvider = provider;
    IcebergViewCatalogOperations viewOps = new IcebergViewCatalogOperations(provider);

    Assertions.assertSame(shared, catalogOps.catalogWrapper());
    Assertions.assertSame(shared, provider.get());
    Assertions.assertNotNull(viewOps);
  }

  private static void resetIcebergRESTServerContext() throws Exception {
    Class<?> holder =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow();
    Field instance = holder.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
  }
}
