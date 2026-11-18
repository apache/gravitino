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

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.jdbc.JdbcCatalogWithMetadataLocationSupport;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/** Verifies JDBC registration consumes the metadata snapshot authorized for that request. */
public class TestJdbcRegistrationMetadataReuse {
  private static final AtomicInteger READS = new AtomicInteger();
  private static final Map<String, AtomicInteger> PATH_READS = new ConcurrentHashMap<>();

  @Test
  void testRegistersSameSnapshotWithoutReopeningFile(@TempDir Path dir) throws Exception {
    IcebergConfig config =
        new IcebergConfig(
            Map.of(
                "catalog-backend",
                "jdbc",
                "uri",
                "jdbc:sqlite:" + dir.resolve("catalog.db"),
                "jdbc-driver",
                "org.sqlite.JDBC",
                "warehouse",
                dir.resolve("warehouse").toUri().toString(),
                "io-impl",
                CountingFileIO.class.getName()));
    try (IcebergCatalogWrapper wrapper = new IcebergCatalogWrapper(config)) {
      Namespace ns = Namespace.of("ns");
      wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(ns).build());
      String pointer =
          wrapper
              .createTable(
                  ns,
                  CreateTableRequest.builder()
                      .withName("source")
                      .withSchema(
                          new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())))
                      .build())
              .metadataLocation();
      READS.set(0);
      TableMetadata metadata = wrapper.loadTableMetadataFromLocation(pointer);
      var original =
          ImmutableRegisterTableRequest.builder().name("copy").metadataLocation(pointer).build();
      var request = new RegisterTableRequestWithMetadata(original, metadata);
      Assertions.assertFalse(request.overwrite());
      request.validate();
      var registered = wrapper.registerTable(ns, request);
      Assertions.assertEquals(
          1, READS.get(), "Registration must not reopen an already read metadata file");
      Assertions.assertEquals(metadata.uuid(), registered.tableMetadata().uuid());
      Assertions.assertEquals(metadata.location(), registered.tableMetadata().location());
      Assertions.assertEquals(metadata.properties(), registered.tableMetadata().properties());
      Assertions.assertEquals(
          pointer, wrapper.getTableMetadataLocation(TableIdentifier.of(ns, "copy")).orElseThrow());
      Assertions.assertEquals(
          metadata.uuid(),
          wrapper.loadTable(TableIdentifier.of(ns, "copy")).tableMetadata().uuid());
      Assertions.assertThrows(
          AlreadyExistsException.class, () -> wrapper.registerTable(ns, request));
      var overwrite =
          new RegisterTableRequestWithMetadata(
              ImmutableRegisterTableRequest.builder()
                  .name("copy")
                  .metadataLocation(pointer)
                  .overwrite(true)
                  .build(),
              metadata);
      Assertions.assertTrue(overwrite.overwrite());
      Assertions.assertEquals(pointer, wrapper.registerTable(ns, overwrite).metadataLocation());

      TableIdentifier copy = TableIdentifier.of(ns, "copy");
      wrapper.getCatalog().loadTable(copy).updateProperties().set("marker", "new").commit();
      Assertions.assertNotEquals(pointer, wrapper.getTableMetadataLocation(copy).orElseThrow());
      int beforeOverwrite = PATH_READS.get(pointer).get();
      var restored = wrapper.registerTable(ns, overwrite);
      Assertions.assertEquals(
          beforeOverwrite,
          PATH_READS.get(pointer).get(),
          "Overwriting must not reopen the authorized metadata snapshot");
      Assertions.assertEquals(pointer, restored.metadataLocation());
      Assertions.assertFalse(restored.tableMetadata().properties().containsKey("marker"));
      Assertions.assertFalse(
          wrapper.getCatalog().loadTable(copy).properties().containsKey("marker"));

      String otherPointer =
          wrapper
              .createTable(
                  ns,
                  CreateTableRequest.builder()
                      .withName("other")
                      .withSchema(metadata.schema())
                      .build())
              .metadataLocation();
      var differentUuid =
          new RegisterTableRequestWithMetadata(
              ImmutableRegisterTableRequest.builder()
                  .name("copy")
                  .metadataLocation(otherPointer)
                  .overwrite(true)
                  .build(),
              wrapper.loadTableMetadataFromLocation(otherPointer));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> wrapper.registerTable(ns, differentUuid));
      Assertions.assertEquals(
          pointer, wrapper.getTableMetadataLocation(TableIdentifier.of(ns, "copy")).orElseThrow());
    }
  }

  /** FileIO that counts metadata input construction without contacting cloud storage. */
  public static class CountingFileIO extends HadoopFileIO {
    /** Creates a locally configured FileIO for CatalogUtil's reflective loader. */
    public CountingFileIO() {
      super(new Configuration());
    }
    /** {@inheritDoc} */
    @Override
    public InputFile newInputFile(String path) {
      READS.incrementAndGet();
      PATH_READS.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
      return super.newInputFile(path);
    }
  }

  @Test
  void testCustomCatalogRegistrationHookIsPreserved() throws Exception {
    CustomJdbcCatalog catalog = Mockito.mock(CustomJdbcCatalog.class);
    IllegalStateException hook = new IllegalStateException("custom registration hook");
    Mockito.when(catalog.registerTable(Mockito.any(), Mockito.anyString(), Mockito.anyBoolean()))
        .thenThrow(hook);
    try (IcebergCatalogWrapper wrapper =
        Mockito.spy(
            new IcebergCatalogWrapper(new IcebergConfig(Map.of("catalog-backend", "memory"))))) {
      Mockito.doReturn(catalog).when(wrapper).getCatalog();
      TableMetadata metadata =
          TableMetadata.newTableMetadata(
              new Schema(), PartitionSpec.unpartitioned(), "file:/warehouse/table", Map.of());
      var request =
          new RegisterTableRequestWithMetadata(
              ImmutableRegisterTableRequest.builder()
                  .name("table")
                  .metadataLocation("file:/warehouse/table/metadata/v1.metadata.json")
                  .build(),
              metadata);
      Assertions.assertSame(
          hook,
          Assertions.assertThrows(
              IllegalStateException.class,
              () -> wrapper.registerTable(Namespace.of("ns"), request)));
      Mockito.verify(catalog, Mockito.never())
          .registerTableWithMetadata(
              Mockito.any(), Mockito.any(TableMetadata.class), Mockito.anyBoolean());
    }
  }

  private static class CustomJdbcCatalog extends JdbcCatalogWithMetadataLocationSupport {
    private CustomJdbcCatalog() {
      super(false);
    }
  }
}
