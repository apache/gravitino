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
package org.apache.gravitino.iceberg.common.ops;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeCatalogWrapperManager {

  @Test
  void getCatalogWrapperResolvesAndCaches() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Mockito.doNothing().when(catalogManager).addCatalogCacheRemoveListener(any());

    IcebergConfig resolved =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    when(catalogManager.doWithCatalog(any(NameIdentifier.class), any())).thenReturn(resolved);

    IcebergConfig cacheConfig = new IcebergConfig(Map.of());
    try (MetalakeCatalogWrapperManager manager =
        new MetalakeCatalogWrapperManager("ml", catalogManager, cacheConfig)) {
      IcebergCatalogWrapper first = manager.getCatalogWrapper("c1");
      IcebergCatalogWrapper second = manager.getCatalogWrapper("c1");
      Assertions.assertSame(first, second);
      Assertions.assertEquals("ml", manager.metalake());
      // Cache hit on second get: doWithCatalog only on miss.
      Mockito.verify(catalogManager, Mockito.times(1))
          .doWithCatalog(any(NameIdentifier.class), any());
    }
  }

  @Test
  void getOrCreateUsesCallerConfig() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Mockito.doNothing().when(catalogManager).addCatalogCacheRemoveListener(any());
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (MetalakeCatalogWrapperManager manager =
        new MetalakeCatalogWrapperManager("ml", catalogManager, config)) {
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("c1", config);
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("c1", config);
      Assertions.assertSame(first, second);
      Mockito.verify(catalogManager, Mockito.never()).doWithCatalog(any(), any());
    }
  }

  @Test
  void invalidateDropsCachedWrapper() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    Mockito.doNothing().when(catalogManager).addCatalogCacheRemoveListener(any());
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (MetalakeCatalogWrapperManager manager =
        new MetalakeCatalogWrapperManager("ml", catalogManager, config)) {
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("c1", config);
      manager.invalidateCatalogWrapper("c1");
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("c1", config);
      Assertions.assertNotSame(first, second);
    }
  }

  @Test
  void removalListenerInvalidatesMatchingCatalog() {
    CatalogManager catalogManager = mock(CatalogManager.class);
    @SuppressWarnings("unchecked")
    Consumer<NameIdentifier>[] captured = new Consumer[1];
    Mockito.doAnswer(
            invocation -> {
              captured[0] = invocation.getArgument(0);
              return null;
            })
        .when(catalogManager)
        .addCatalogCacheRemoveListener(any());

    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (MetalakeCatalogWrapperManager manager =
        new MetalakeCatalogWrapperManager("ml", catalogManager, config)) {
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("c1", config);
      captured[0].accept(NameIdentifierUtil.ofCatalog("ml", "c1"));
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("c1", config);
      Assertions.assertNotSame(first, second);
    }
  }
}
