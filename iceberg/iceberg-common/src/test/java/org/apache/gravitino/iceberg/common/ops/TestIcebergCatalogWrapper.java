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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergCatalogWrapper {
  private static final TableIdentifier MISSING_METADATA_TABLE =
      TableIdentifier.of("db", "missing_metadata");
  private static final String MISSING_METADATA_LOCATION =
      "oss://bucket/db/missing_metadata/metadata/00000.metadata.json";

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

  @Test
  public void testLoadTableShouldFastFailWhenOssMetadataFileIsMissing() throws Exception {
    MissingMetadataCatalog.reset();
    MissingMetadataFileIO.reset();

    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(missingMetadataConfig()))) {
      NoSuchTableException exception =
          Assertions.assertThrows(
              NoSuchTableException.class, () -> wrapper.loadTable(MISSING_METADATA_TABLE));

      Assertions.assertTrue(exception.getMessage().contains(MISSING_METADATA_LOCATION));
      Assertions.assertEquals(1, MissingMetadataCatalog.METADATA_LOCATION_COUNT.get());
      Assertions.assertEquals(1, MissingMetadataFileIO.NEW_INPUT_FILE_COUNT.get());
      Assertions.assertEquals(1, MissingMetadataFileIO.EXISTS_COUNT.get());
      Assertions.assertEquals(0, MissingMetadataCatalog.LOAD_TABLE_COUNT.get());
    }
  }

  @Test
  public void testTableExistsShouldFastFailWhenOssMetadataFileIsMissing() throws Exception {
    MissingMetadataCatalog.reset();
    MissingMetadataFileIO.reset();

    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(missingMetadataConfig()))) {
      Assertions.assertFalse(wrapper.tableExists(MISSING_METADATA_TABLE));

      Assertions.assertEquals(1, MissingMetadataCatalog.METADATA_LOCATION_COUNT.get());
      Assertions.assertEquals(1, MissingMetadataFileIO.NEW_INPUT_FILE_COUNT.get());
      Assertions.assertEquals(1, MissingMetadataFileIO.EXISTS_COUNT.get());
      Assertions.assertEquals(0, MissingMetadataCatalog.LOAD_TABLE_COUNT.get());
    }
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
    config.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
    config.put(
        IcebergConstants.TABLE_METADATA_CACHE_IMPL, TrackingTableMetadataCache.class.getName());
    return config;
  }

  private static Map<String, String> missingMetadataConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(IcebergConstants.CATALOG_BACKEND, "custom");
    config.put(IcebergConstants.CATALOG_BACKEND_IMPL, MissingMetadataCatalog.class.getName());
    config.put(IcebergConstants.URI, "custom://missing-metadata");
    config.put(IcebergConstants.WAREHOUSE, "unused");
    config.put(IcebergConstants.IO_IMPL, MissingMetadataFileIO.class.getName());
    return config;
  }

  public static class MissingMetadataCatalog implements Catalog, SupportsMetadataLocation {
    private static final AtomicInteger LOAD_TABLE_COUNT = new AtomicInteger();
    private static final AtomicInteger METADATA_LOCATION_COUNT = new AtomicInteger();

    private String name;

    static void reset() {
      LOAD_TABLE_COUNT.set(0);
      METADATA_LOCATION_COUNT.set(0);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return List.of(MISSING_METADATA_TABLE);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      LOAD_TABLE_COUNT.incrementAndGet();
      throw new RuntimeException("Catalog loadTable should not be called");
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {}

    @Override
    public String metadataLocation(TableIdentifier tableIdentifier) {
      METADATA_LOCATION_COUNT.incrementAndGet();
      return MISSING_METADATA_LOCATION;
    }
  }

  public static class MissingMetadataFileIO implements FileIO {
    private static final AtomicInteger NEW_INPUT_FILE_COUNT = new AtomicInteger();
    private static final AtomicInteger EXISTS_COUNT = new AtomicInteger();

    static void reset() {
      NEW_INPUT_FILE_COUNT.set(0);
      EXISTS_COUNT.set(0);
    }

    @Override
    public InputFile newInputFile(String path) {
      NEW_INPUT_FILE_COUNT.incrementAndGet();
      return new MissingMetadataInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }

  private static class MissingMetadataInputFile implements InputFile {
    private final String location;

    private MissingMetadataInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekableInputStream newStream() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      MissingMetadataFileIO.EXISTS_COUNT.incrementAndGet();
      return false;
    }
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
