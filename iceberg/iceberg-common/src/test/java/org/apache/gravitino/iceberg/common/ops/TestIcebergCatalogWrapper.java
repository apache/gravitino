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
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.cache.SupportsMetadataLocation;
import org.apache.gravitino.iceberg.common.cache.TableMetadataCache;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewOperations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestIcebergCatalogWrapper {

  @ParameterizedTest
  @ValueSource(strings = {"file:///tmp/table", "gs://bucket/table", "/tmp/table", "s3:///table"})
  void testS3CatalogRejectsInvalidLocationBeforeCreatingTable(String location) {
    IcebergCatalogWrapper wrapper =
        Mockito.spy(new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig())));
    Mockito.doReturn("org.apache.iceberg.aws.s3.S3FileIO").when(wrapper).fileIOImpl();
    for (boolean staged : new boolean[] {false, true}) {
      CreateTableRequest.Builder builder =
          CreateTableRequest.builder()
              .withName("invalid_location")
              .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
              .withLocation(location);
      if (staged) {
        builder.stageCreate();
      }
      CreateTableRequest request = builder.build();
      BadRequestException failure =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.createTable(Namespace.of("s"), request));
      Assertions.assertTrue(failure.getMessage().contains(location));
    }
    Mockito.verify(wrapper, Mockito.never()).getCatalog();
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:///tmp/table", "gs://bucket/table", "/tmp/table", "s3:///table"})
  void testS3CatalogRejectsInvalidRegisterAndUpdateLocations(String location) {
    IcebergCatalogWrapper wrapper =
        Mockito.spy(new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig())));
    Mockito.doReturn("org.apache.iceberg.aws.s3.S3FileIO").when(wrapper).fileIOImpl();
    RegisterTableRequest register =
        ImmutableRegisterTableRequest.builder().name("t").metadataLocation(location).build();
    UpdateTableRequest update =
        new UpdateTableRequest(
            Collections.emptyList(),
            Collections.singletonList(new MetadataUpdate.SetLocation(location)));

    BadRequestException registerFailure =
        Assertions.assertThrows(
            BadRequestException.class, () -> wrapper.registerTable(Namespace.of("s"), register));
    Assertions.assertTrue(registerFailure.getMessage().contains(location));
    BadRequestException metadataFailure =
        Assertions.assertThrows(
            BadRequestException.class, () -> wrapper.loadTableMetadataFromLocation(location));
    Assertions.assertTrue(metadataFailure.getMessage().contains(location));
    BadRequestException updateFailure =
        Assertions.assertThrows(
            BadRequestException.class,
            () -> wrapper.updateTable(TableIdentifier.of("s", "t"), update));
    Assertions.assertTrue(updateFailure.getMessage().contains(location));
    Mockito.verify(wrapper, Mockito.never()).getCatalog();
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket/table", "s3a://bucket/table", "s3n://bucket/table"})
  void testS3CatalogAcceptsRegisterAndUpdateLocationSchemes(String location) {
    IcebergCatalogWrapper wrapper =
        Mockito.spy(new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig())));
    Mockito.doReturn("org.apache.iceberg.aws.s3.S3FileIO").when(wrapper).fileIOImpl();
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.doReturn(catalog).when(wrapper).getCatalog();
    Mockito.when(catalog.registerTable(Mockito.any(), Mockito.eq(location), Mockito.anyBoolean()))
        .thenThrow(new IllegalStateException("register backend reached"));
    Mockito.when(catalog.loadTable(Mockito.any()))
        .thenThrow(new IllegalStateException("update backend reached"));
    RegisterTableRequest register =
        ImmutableRegisterTableRequest.builder().name("t").metadataLocation(location).build();
    UpdateTableRequest update =
        new UpdateTableRequest(
            Collections.emptyList(),
            Collections.singletonList(new MetadataUpdate.SetLocation(location)));

    IllegalStateException registerFailure =
        Assertions.assertThrows(
            IllegalStateException.class, () -> wrapper.registerTable(Namespace.of("s"), register));
    Assertions.assertEquals("register backend reached", registerFailure.getMessage());
    IllegalStateException updateFailure =
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> wrapper.updateTable(TableIdentifier.of("s", "t"), update));
    Assertions.assertEquals("update backend reached", updateFailure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket/table", "s3a://bucket/table", "s3n://bucket/table"})
  void testS3CatalogAcceptsSupportedLocationSchemes(String location) {
    IcebergCatalogWrapper wrapper =
        Mockito.spy(new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig())));
    Mockito.doReturn("org.apache.iceberg.aws.s3.S3FileIO").when(wrapper).fileIOImpl();
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.doReturn(catalog).when(wrapper).getCatalog();
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("valid_location")
            .withSchema(new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
            .withLocation(location)
            .build();
    // Stop after validation, before the mocked backend creates anything.
    Mockito.when(catalog.buildTable(Mockito.any(), Mockito.any()))
        .thenThrow(new IllegalStateException("backend reached"));
    IllegalStateException failure =
        Assertions.assertThrows(
            IllegalStateException.class, () -> wrapper.createTable(Namespace.of("s"), request));
    Assertions.assertEquals("backend reached", failure.getMessage());
  }

  @Test
  void testLoadViewReportsMissingAfterNativeRename(@TempDir Path warehouseDir) throws Exception {
    Namespace namespace = Namespace.of("views");
    TableIdentifier oldName = TableIdentifier.of(namespace, "old_name");
    TableIdentifier newName = TableIdentifier.of(namespace, "new_name");
    AtomicBoolean renameDuringLoad = new AtomicBoolean();
    AtomicInteger operationLoads = new AtomicInteger();
    try (JdbcCatalog catalog =
        new JdbcCatalog() {
          @Override
          protected ViewOperations newViewOps(TableIdentifier identifier) {
            // BaseMetastoreViewCatalog checks one operations instance, then returns a BaseView
            // backed by another. Rename between those two instances, without timing or threads.
            if (renameDuringLoad.get()
                && identifier.equals(oldName)
                && operationLoads.incrementAndGet() == 2) {
              renameView(oldName, newName);
            }
            return super.newViewOps(identifier);
          }
        }) {
      catalog.initialize(
          "rename-test",
          Map.of(
              CatalogProperties.URI,
              "jdbc:sqlite:" + warehouseDir.resolve("catalog.db"),
              CatalogProperties.WAREHOUSE_LOCATION,
              warehouseDir.toUri().toString(),
              "jdbc.schema-version",
              "V1"));
      catalog.createNamespace(namespace);
      catalog
          .buildView(oldName)
          .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .withDefaultNamespace(namespace)
          .withQuery("spark", "SELECT 1 AS id")
          .create();
      IcebergCatalogWrapper wrapper =
          Mockito.spy(new IcebergCatalogWrapper(new IcebergConfig(unreachableConfig())));
      Mockito.doReturn(catalog).when(wrapper).getCatalog();
      renameDuringLoad.set(true);

      NoSuchViewException failure =
          Assertions.assertThrows(NoSuchViewException.class, () -> wrapper.loadView(oldName));
      Assertions.assertTrue(failure.getMessage().contains(oldName.toString()));
      Assertions.assertFalse(catalog.viewExists(oldName));
      LoadViewResponse response = wrapper.loadView(newName);
      Assertions.assertNotNull(response.metadata());
      Assertions.assertEquals(
          response.metadata().metadataFileLocation(), response.metadataLocation());
    }
  }

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
  public void testMemoryCatalogSurvivesWrapperClose() throws Exception {
    String catalogUuid = "catalog-1";
    IcebergConfig config =
        new IcebergConfig(
            Map.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.CATALOG_UUID,
                catalogUuid));
    IcebergCatalogWrapper firstWrapper = new IcebergCatalogWrapper(config);
    Catalog firstCatalog = firstWrapper.getCatalog();
    Namespace namespace = Namespace.of("preserved");
    firstWrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());

    firstWrapper.close();
    IcebergCatalogWrapper secondWrapper = new IcebergCatalogWrapper(config);

    Assertions.assertSame(firstCatalog, secondWrapper.getCatalog());
    Assertions.assertTrue(secondWrapper.namespaceExists(namespace));
    IcebergCatalogUtil.removeMemoryCatalog(catalogUuid);
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
  public void testLoadTableMetadataFromLocation(@TempDir Path tempDir) throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata expected =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            tempDir.resolve("table").toUri().toString(),
            Map.of());
    Path metadataFile = tempDir.resolve("v1.metadata.json");
    Files.writeString(metadataFile, TableMetadataParser.toJson(expected), StandardCharsets.UTF_8);
    IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(
            new IcebergConfig(Map.of(IcebergConstants.CATALOG_BACKEND, "memory")));

    TableMetadata actual = wrapper.loadTableMetadataFromLocation(metadataFile.toUri().toString());

    Assertions.assertEquals(schema.asStruct(), actual.schema().asStruct());
  }

  @Test
  public void testValidateTableDefinitionRejectsInvalidFormatVersion() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (String version : new String[] {"abc", "0", "4", "-1"}) {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName("t")
              .withSchema(schema)
              .setProperties(ImmutableMap.of("format-version", version))
              .build();
      Assertions.assertThrows(
          BadRequestException.class,
          () -> IcebergCatalogWrapper.validateTableDefinition(request),
          "format-version " + version);
    }
    for (String version : new String[] {"1", "2", "3"}) {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName("t")
              .withSchema(schema)
              .setProperties(ImmutableMap.of("format-version", version))
              .build();
      Assertions.assertDoesNotThrow(() -> IcebergCatalogWrapper.validateTableDefinition(request));
    }
  }

  @Test
  public void testValidateTableDefinitionRejectsTypesTheVersionDoesNotSupport() {
    Schema nanos =
        new Schema(Types.NestedField.required(1, "ts", Types.TimestampNanoType.withZone()));
    CreateTableRequest v2 =
        CreateTableRequest.builder()
            .withName("t")
            .withSchema(nanos)
            .setProperties(ImmutableMap.of("format-version", "2"))
            .build();
    Assertions.assertThrows(
        BadRequestException.class, () -> IcebergCatalogWrapper.validateTableDefinition(v2));

    CreateTableRequest v3 =
        CreateTableRequest.builder()
            .withName("t")
            .withSchema(nanos)
            .setProperties(ImmutableMap.of("format-version", "3"))
            .build();
    Assertions.assertDoesNotThrow(() -> IcebergCatalogWrapper.validateTableDefinition(v3));

    // Without an explicit version the catalog defaults decide; nothing is rejected up front.
    CreateTableRequest unspecified =
        CreateTableRequest.builder().withName("t").withSchema(nanos).build();
    Assertions.assertDoesNotThrow(() -> IcebergCatalogWrapper.validateTableDefinition(unspecified));
  }

  @Test
  public void testUpdateTableToUnknownDefaultSpecOrSortOrderIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      Namespace namespace = Namespace.of("db");
      TableIdentifier ident = TableIdentifier.of(namespace, "t");
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
      wrapper.createTable(
          namespace,
          CreateTableRequest.builder()
              .withName("t")
              .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
              .build());

      for (MetadataUpdate update :
          new MetadataUpdate[] {
            new MetadataUpdate.SetDefaultPartitionSpec(7), new MetadataUpdate.SetDefaultSortOrder(7)
          }) {
        UpdateTableRequest request =
            new UpdateTableRequest(Collections.emptyList(), Collections.singletonList(update));
        Assertions.assertThrows(
            BadRequestException.class, () -> wrapper.updateTable(ident, request));
      }

      // Ids the table has, and -1 ("the last added"), are still accepted.
      UpdateTableRequest existing =
          new UpdateTableRequest(
              Collections.emptyList(),
              Arrays.asList(
                  new MetadataUpdate.SetDefaultPartitionSpec(0),
                  new MetadataUpdate.SetDefaultSortOrder(0)));
      Assertions.assertDoesNotThrow(() -> wrapper.updateTable(ident, existing));
    }
  }

  @Test
  public void testRegisterTableWithMissingMetadataIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      Namespace namespace = Namespace.of("db");
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
      RegisterTableRequest request =
          ImmutableRegisterTableRequest.builder()
              .name("ghost")
              .metadataLocation(warehouseDir.resolve("missing/v1.metadata.json").toUri().toString())
              .build();
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.registerTable(namespace, request));
      Assertions.assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    }
  }

  @Test
  public void testRegisterViewWithMissingMetadataIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(
            new IcebergConfig(Map.of(IcebergConstants.CATALOG_BACKEND, "memory")))) {
      Namespace namespace = Namespace.of("db");
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
      RegisterViewRequest request =
          ImmutableRegisterViewRequest.builder()
              .name("ghost")
              .metadataLocation(warehouseDir.resolve("missing/v1.metadata.json").toUri().toString())
              .build();
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.registerView(namespace, request));
      Assertions.assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    }
  }

  @Test
  public void testLoadTableMetadataFromMissingLocationIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      String location = warehouseDir.resolve("missing/v1.metadata.json").toUri().toString();
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.loadTableMetadataFromLocation(location));
      Assertions.assertTrue(e.getMessage().contains("does not exist"), e.getMessage());
    }
  }

  @Test
  public void testLoadTableMetadataFromDirectoryIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      Path directory = Files.createDirectories(warehouseDir.resolve("not-a-metadata-file"));
      String location = directory.toUri().toString();
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.loadTableMetadataFromLocation(location));
      Assertions.assertTrue(
          e.getMessage().contains("is not a valid metadata file"), e.getMessage());
    }
  }

  @Test
  public void testLoadTableMetadataFromCorruptContentIsNotBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      Path metadataFile = warehouseDir.resolve("v1.metadata.json");
      Files.writeString(metadataFile, "{not-valid-table-metadata", StandardCharsets.UTF_8);
      String location = metadataFile.toUri().toString();
      // Codec.fromFileName accepts *.metadata.json; corrupt JSON must not become BadRequest.
      Assertions.assertThrows(
          RuntimeIOException.class, () -> wrapper.loadTableMetadataFromLocation(location));
    }
  }

  @Test
  public void testLoadTableMetadataFromUnreadableSchemeIsBadRequest(@TempDir Path warehouseDir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      // Local JDBC catalogs have no S3 FileIO; the client-supplied scheme is unreadable.
      String location = "s3://bucket/path/v1.metadata.json";
      BadRequestException e =
          Assertions.assertThrows(
              BadRequestException.class, () -> wrapper.loadTableMetadataFromLocation(location));
      Assertions.assertTrue(
          e.getMessage().contains("unsupported or unreadable scheme"), e.getMessage());
      Assertions.assertFalse(e.getMessage().contains("No FileSystem"), e.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPurgeDeletesWhatCanBeReadWhenAManifestListIsMissing(
      boolean gcEnabled, @TempDir Path warehouseDir) throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(sqliteConfig(warehouseDir)))) {
      Namespace namespace = Namespace.of("db");
      TableIdentifier ident = TableIdentifier.of(namespace, "purged");
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(namespace).build());
      Table table =
          wrapper
              .getCatalog()
              .createTable(
                  ident,
                  new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                  PartitionSpec.unpartitioned(),
                  ImmutableMap.of("gc.enabled", Boolean.toString(gcEnabled)));
      Path first = Files.createFile(warehouseDir.resolve("first.parquet"));
      Path second = Files.createFile(warehouseDir.resolve("second.parquet"));
      for (Path data : new Path[] {first, second}) {
        table
            .newFastAppend()
            .appendFile(
                DataFiles.builder(table.spec())
                    .withPath(data.toUri().toString())
                    .withFileSizeInBytes(1)
                    .withRecordCount(1)
                    .build())
            .commit();
      }
      table.refresh();
      String metadataFile =
          ((HasTableOperations) table).operations().current().metadataFileLocation();
      // The first snapshot's manifest list is gone.
      Files.delete(
          Paths.get(URI.create(table.snapshots().iterator().next().manifestListLocation())));

      wrapper.purgeTable(ident);

      Assertions.assertFalse(wrapper.getCatalog().tableExists(ident));
      // Files reachable through the remaining manifest list, and the metadata, are deleted.
      Assertions.assertEquals(!gcEnabled, Files.exists(first));
      Assertions.assertEquals(!gcEnabled, Files.exists(second));
      Assertions.assertFalse(Files.exists(Paths.get(URI.create(metadataFile))));
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

  private static Map<String, String> sqliteConfig(Path warehouseDir) {
    Map<String, String> config = new HashMap<>();
    config.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    config.put(IcebergConstants.URI, "jdbc:sqlite:" + warehouseDir.resolve("catalog.db"));
    config.put(IcebergConstants.WAREHOUSE, warehouseDir.toUri().toString());
    config.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    config.put(IcebergConstants.ICEBERG_JDBC_USER, "test");
    config.put(IcebergConstants.ICEBERG_JDBC_PASSWORD, "test");
    config.put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
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
