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

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStandaloneCatalogWrapperManager {

  @Test
  void getWithConfigReusesCachedWrapper() {
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (StandaloneCatalogWrapperManager manager = new StandaloneCatalogWrapperManager(config)) {
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("c1", config);
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("c1", config);
      Assertions.assertSame(first, second);
    }
  }

  @Test
  void getWithoutConfigUnsupported() {
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (StandaloneCatalogWrapperManager manager = new StandaloneCatalogWrapperManager(config)) {
      Assertions.assertThrows(
          UnsupportedOperationException.class, () -> manager.getCatalogWrapper("c1"));
    }
  }

  @Test
  void invalidateDropsCachedWrapper() {
    IcebergConfig config =
        new IcebergConfig(ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "memory"));
    try (StandaloneCatalogWrapperManager manager = new StandaloneCatalogWrapperManager(config)) {
      IcebergCatalogWrapper first = manager.getOrCreateCatalogWrapper("c1", config);
      manager.invalidateCatalogWrapper("c1");
      IcebergCatalogWrapper second = manager.getOrCreateCatalogWrapper("c1", config);
      Assertions.assertNotSame(first, second);
    }
  }
}
