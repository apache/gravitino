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

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergCatalogWrapper {

  @Test
  public void testCatalogShouldBeLazyLoaded() {
    IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig()));

    Assertions.assertThrows(Throwable.class, wrapper::getCatalog);
  }

  @Test
  public void testCloseShouldNotInitializeCatalog() {
    IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig()));

    Assertions.assertDoesNotThrow(
        () -> {
          wrapper.close();
        });
  }

  @Test
  public void testMetadataCacheShouldInitializeOnFirstAccessAndClose(@TempDir Path warehouseDir)
      throws Exception {
    TrackingTableMetadataCache.reset();
    IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(metadataConfig(warehouseDir)));

    Assertions.assertEquals(0, TrackingTableMetadataCache.INITIALIZE_COUNT.get());

    TableMetadataCache cache = invokeGetMetadataCache(wrapper);
    Assertions.assertNotNull(cache);
    Assertions.assertEquals(1, TrackingTableMetadataCache.INITIALIZE_COUNT.get());
    Assertions.assertFalse(TrackingTableMetadataCache.CLOSED.get());

    wrapper.close();

    Assertions.assertEquals(1, TrackingTableMetadataCache.INITIALIZE_COUNT.get());
    Assertions.assertTrue(TrackingTableMetadataCache.CLOSED.get());
  }

  private static TableMetadataCache invokeGetMetadataCache(IcebergCatalogWrapper wrapper)
      throws Exception {
    Method method = IcebergCatalogWrapper.class.getDeclaredMethod("getMetadataCache");
    method.setAccessible(true);
    return (TableMetadataCache) method.invoke(wrapper);
  }

  private static Map<String, String> unreachableConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    config.put(IcebergConstants.URI, "jdbc:invalid://unreachable");
    config.put(IcebergConstants.WAREHOUSE, "unused");
    return config;
  }

  private static Map<String, String> metadataConfig(Path warehouseDir) {
    Map<String, String> config = new HashMap<>();
    config.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    config.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    config.put(IcebergConstants.WAREHOUSE, warehouseDir.toString());
    config.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    config.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    config.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    config.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "false");
    config.put(
        IcebergConstants.TABLE_METADATA_CACHE_IMPL, TrackingTableMetadataCache.class.getName());
    return config;
  }

  public static class TrackingTableMetadataCache implements TableMetadataCache {
    private static final AtomicInteger INITIALIZE_COUNT = new AtomicInteger();
    private static final AtomicBoolean CLOSED = new AtomicBoolean();

    static void reset() {
      INITIALIZE_COUNT.set(0);
      CLOSED.set(false);
    }

    @Override
    public void initialize(
        int capacity,
        int expireMinutes,
        Map<String, String> catalogProperties,
        SupportsMetadataLocation supportsMetadataLocation) {
      INITIALIZE_COUNT.incrementAndGet();
    }

    @Override
    public void invalidate(TableIdentifier tableIdentifier) {}

    @Override
    public Optional<TableMetadata> getTableMetadata(TableIdentifier tableIdentifier) {
      return Optional.empty();
    }

    @Override
    public void updateTableMetadata(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {}

    @Override
    public void close() throws IOException {
      CLOSED.set(true);
    }
  }
}
