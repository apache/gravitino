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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

class TestGravitinoGlueCatalog {

  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "t");
  private static final String METADATA_LOCATION = "s3://bucket/db/t/metadata/00001.metadata.json";

  @Test
  void testLoadStaticTableReadsMetadataFile() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "s3://bucket/db/t", Map.of("k", "v"));
    InMemoryFileIO io = new InMemoryFileIO();
    io.addFile(
        METADATA_LOCATION, TableMetadataParser.toJson(metadata).getBytes(StandardCharsets.UTF_8));

    Table table =
        catalogWith(mock(TableOperations.class), io).loadStaticTable(IDENTIFIER, METADATA_LOCATION);

    assertEquals("id", table.schema().columns().get(0).name());
    assertEquals("v", table.properties().get("k"));
    assertEquals(IDENTIFIER.toString(), table.name());
  }

  @Test
  void testLoadStaticTableDoesNotRetryDeniedRead() {
    FileIO io = mock(FileIO.class);
    when(io.newInputFile(METADATA_LOCATION))
        .thenThrow(S3Exception.builder().statusCode(403).build());
    TableOperations ops = mock(TableOperations.class);

    Table table = catalogWith(ops, io).loadStaticTable(IDENTIFIER, METADATA_LOCATION);

    assertThrows(S3Exception.class, table::properties);
    verify(io, times(1)).newInputFile(METADATA_LOCATION);
    verify(ops, never()).current();
    verify(ops, never()).refresh();
  }

  @Test
  void testLoadStaticTableRequiresMetadataLocation() {
    GravitinoGlueCatalog catalog = catalogWith(mock(TableOperations.class), mock(FileIO.class));

    assertThrows(NoSuchTableException.class, () -> catalog.loadStaticTable(IDENTIFIER, null));
  }

  @Test
  void testLoadStaticTableKeepsSourceOperationsReachable() {
    AtomicReference<WeakReference<TableOperations>> source = new AtomicReference<>();
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          @Override
          protected TableOperations newTableOps(TableIdentifier identifier) {
            TableOperations ops = new StaticTableOperations(METADATA_LOCATION, mock(FileIO.class));
            source.set(new WeakReference<>(ops));
            return ops;
          }
        };

    Table table = catalog.loadStaticTable(IDENTIFIER, METADATA_LOCATION);
    for (int i = 0; i < 10 && source.get().get() != null; i++) {
      System.gc();
    }

    // The catalog closes the FileIO of operations that were garbage collected.
    assertNotNull(source.get().get());
    assertEquals(IDENTIFIER.toString(), table.name());
  }

  private static GravitinoGlueCatalog catalogWith(TableOperations ops, FileIO io) {
    when(ops.io()).thenReturn(io);
    return new GravitinoGlueCatalog() {
      @Override
      protected TableOperations newTableOps(TableIdentifier identifier) {
        return ops;
      }
    };
  }
}
