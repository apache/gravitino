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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterViewRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestIcebergStorageLocations {
  private static final Namespace NAMESPACE = Namespace.of("locations");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));

  @ParameterizedTest
  @MethodSource("localCreates")
  void testRemoteWarehouseRejectsLocalCreateBeforeWriting(
      boolean explicitIO, boolean staged, String location, @TempDir Path tempDir) throws Exception {
    Map<String, String> properties = config(tempDir, "s3://bucket/warehouse", explicitIO);
    Path localTable = tempDir.resolve("local_table");
    String rejected = location.replace("LOCAL", localTable.toString());
    CreateTableRequest request = createRequest(rejected, staged, Map.of());
    try (IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(new IcebergConfig(properties))) {
      assertRejected(rejected, () -> wrapper.createTable(NAMESPACE, request));
      Assertions.assertFalse(Files.exists(localTable));
      Assertions.assertFalse(Files.exists(tempDir.resolve("catalog.db")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:///tmp/table", "file:/tmp/table", "/tmp/table", "relative/table"})
  void testRemoteWarehouseRejectsRegisterReadAndLocationUpdates(String location, @TempDir Path dir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      assertRejected(
          location,
          () ->
              wrapper.registerTable(
                  NAMESPACE,
                  ImmutableRegisterTableRequest.builder()
                      .name("table")
                      .metadataLocation(location)
                      .build()));
      assertRejected(location, () -> wrapper.loadTableMetadataFromLocation(location));
      assertRejected(
          location,
          () ->
              wrapper.updateTable(
                  TABLE,
                  new UpdateTableRequest(
                      List.of(), List.of(new MetadataUpdate.SetLocation(location)))));
      Assertions.assertFalse(Files.exists(dir.resolve("catalog.db")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"write.metadata.path", "write.data.path"})
  void testRemoteWarehouseRejectsLocalWriteProperties(String property, @TempDir Path dir)
      throws Exception {
    String location = dir.resolve("local_metadata").toUri().toString();
    Map<String, String> localProperties = Map.of(property, location);
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      for (boolean staged : List.of(false, true)) {
        assertRejected(
            location,
            () ->
                wrapper.createTable(
                    NAMESPACE, createRequest("s3://bucket/table", staged, localProperties)));
      }
      UpdateTableRequest update =
          new UpdateTableRequest(
              List.of(), List.of(new MetadataUpdate.SetProperties(localProperties)));
      assertRejected(location, () -> wrapper.updateTable(TABLE, update));
      assertRejected(location, () -> wrapper.updateView(TABLE, update));
      assertRejected(
          location,
          () -> wrapper.createView(NAMESPACE, viewRequest("s3://bucket/view", localProperties)));
      Assertions.assertFalse(Files.exists(dir.resolve("catalog.db")));
      Assertions.assertFalse(Files.exists(dir.resolve("local_metadata")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:///tmp/namespace", "/tmp/namespace"})
  void testRemoteWarehouseRejectsLocalNamespaceLocations(String location, @TempDir Path dir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      assertRejected(
          location,
          () ->
              wrapper.createNamespace(
                  CreateNamespaceRequest.builder()
                      .withNamespace(NAMESPACE)
                      .setProperties(new HashMap<>(Map.of(IcebergConstants.LOCATION, location)))
                      .build()));
      assertRejected(
          location,
          () ->
              wrapper.updateNamespaceProperties(
                  NAMESPACE,
                  UpdateNamespacePropertiesRequest.builder()
                      .update(IcebergConstants.LOCATION, location)
                      .build()));
      Assertions.assertFalse(wrapper.namespaceExists(NAMESPACE));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRemoteWarehouseRejectsInheritedLocalNamespace(boolean staged, @TempDir Path dir)
      throws Exception {
    String location = dir.resolve("local_namespace").toUri().toString();
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      // Simulate namespace metadata created before location validation was introduced.
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
      ((SupportsNamespaces) wrapper.getCatalog())
          .setProperties(NAMESPACE, Map.of(IcebergConstants.LOCATION, location));
      assertRejected(
          location, () -> wrapper.createTable(NAMESPACE, createRequest(null, staged, Map.of())));
      assertRejected(location, () -> wrapper.createView(NAMESPACE, viewRequest(null, Map.of())));
      Assertions.assertFalse(wrapper.tableExists(TABLE));
      Assertions.assertFalse(Files.exists(dir.resolve("local_namespace")));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:///tmp/view", "/tmp/view"})
  void testRemoteWarehouseRejectsLocalViewLocations(String location, @TempDir Path dir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      assertRejected(
          location, () -> wrapper.createView(NAMESPACE, viewRequest(location, Map.of())));
      assertRejected(
          location,
          () ->
              wrapper.registerView(
                  NAMESPACE,
                  ImmutableRegisterViewRequest.builder()
                      .name("view")
                      .metadataLocation(location)
                      .build()));
      assertRejected(
          location,
          () ->
              wrapper.updateView(
                  TABLE,
                  new UpdateTableRequest(
                      List.of(), List.of(new MetadataUpdate.SetLocation(location)))));
      Assertions.assertFalse(Files.exists(dir.resolve("catalog.db")));
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "s3://bucket/table",
        "s3a://bucket/table",
        "gs://bucket/table",
        "abfss://container@account.dfs.core.windows.net/table",
        "hdfs://namenode/table"
      })
  void testResolvingFileIOPreservesCrossStorageLocations(
      @Nullable String location, @TempDir Path dir) throws Exception {
    IcebergCatalogWrapper wrapper =
        Mockito.spy(
            new IcebergCatalogWrapper(
                new IcebergConfig(config(dir, "s3://bucket/warehouse", false))));
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.doReturn(catalog).when(wrapper).getCatalog();
    IllegalStateException backend = new IllegalStateException("backend reached");
    Mockito.when(catalog.buildTable(Mockito.any(), Mockito.any())).thenThrow(backend);
    try (wrapper) {
      Assertions.assertSame(
          backend,
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> wrapper.createTable(NAMESPACE, createRequest(location, false, Map.of()))));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3://bucket/table", "gs://bucket/table"})
  void testRemoteStagedCreateWithoutCloudClients(String location, @TempDir Path dir)
      throws Exception {
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, "s3://bucket/warehouse", false)))) {
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
      LoadTableResponse response =
          wrapper.createTable(NAMESPACE, createRequest(location, true, Map.of()));
      Assertions.assertEquals(location, response.tableMetadata().location());
      Assertions.assertFalse(wrapper.tableExists(TABLE));
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testLocalWarehouseStillCreatesAndLoadsLocalTables(
      boolean explicitIO, boolean bareWarehouse, @TempDir Path dir) throws Exception {
    String warehouse =
        bareWarehouse
            ? dir.resolve("warehouse").toString()
            : dir.resolve("warehouse").toUri().toString();
    try (IcebergCatalogWrapper wrapper =
        new IcebergCatalogWrapper(new IcebergConfig(config(dir, warehouse, explicitIO)))) {
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build());
      LoadTableResponse response =
          wrapper.createTable(
              NAMESPACE,
              createRequest(dir.resolve("local_table").toUri().toString(), false, Map.of()));
      Assertions.assertTrue(wrapper.tableExists(TABLE));
      Assertions.assertTrue(
          Files.isRegularFile(Paths.get(URI.create(response.metadataLocation()))));
      Assertions.assertEquals(
          response.metadataLocation(), wrapper.loadTable(TABLE).metadataLocation());
    }
  }

  private static Stream<Arguments> localCreates() {
    return Stream.of(false, true)
        .flatMap(
            explicitIO ->
                Stream.of(false, true)
                    .flatMap(
                        staged ->
                            Stream.of(
                                    "file://LOCAL",
                                    "file:LOCAL",
                                    "FILE://LOCAL",
                                    "LOCAL",
                                    "relative/path",
                                    "//localhost/tmp/table",
                                    "")
                                .map(location -> Arguments.of(explicitIO, staged, location))));
  }

  private static Map<String, String> config(Path dir, String warehouse, boolean explicitIO) {
    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    properties.put(IcebergConstants.URI, "jdbc:sqlite:" + dir.resolve("catalog.db"));
    properties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    properties.put(IcebergConstants.WAREHOUSE, warehouse);
    if (explicitIO) {
      properties.put(IcebergConstants.IO_IMPL, ResolvingFileIO.class.getName());
    }
    return properties;
  }

  private static CreateTableRequest createRequest(
      @Nullable String location, boolean staged, Map<String, String> properties) {
    CreateTableRequest.Builder builder =
        CreateTableRequest.builder()
            .withName(TABLE.name())
            .withSchema(SCHEMA)
            .setProperties(new HashMap<>(properties));
    if (location != null) {
      builder.withLocation(location);
    }
    if (staged) {
      builder.stageCreate();
    }
    return builder.build();
  }

  private static void assertRejected(String location, Executable action) {
    BadRequestException failure = Assertions.assertThrows(BadRequestException.class, action);
    Assertions.assertTrue(failure.getMessage().contains(location));
  }

  private static CreateViewRequest viewRequest(
      @Nullable String location, Map<String, String> properties) {
    ImmutableCreateViewRequest.Builder builder =
        ImmutableCreateViewRequest.builder()
            .name("view")
            .schema(SCHEMA)
            .properties(properties)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .schemaId(SCHEMA.schemaId())
                    .timestampMillis(System.currentTimeMillis())
                    .defaultNamespace(NAMESPACE)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .dialect("spark")
                            .sql("SELECT 1 AS id")
                            .build())
                    .build());
    if (location != null) {
      builder.location(location);
    }
    return builder.build();
  }
}
