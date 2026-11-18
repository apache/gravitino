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
package org.apache.iceberg;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.file.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestMetastoreRegisterTableUtils {

  @Test
  void testRegisterWithoutOverwrite() {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "t");
    String metadataLocation = "s3://bucket/ns/t/metadata/v1.metadata.json";
    Table table = mock(Table.class);

    when(catalog.registerTable(identifier, metadataLocation)).thenReturn(table);

    Table result =
        MetastoreRegisterTableUtils.registerTable(
            catalog, identifier, metadataLocation, false, metadataLocationOverwrite);

    Assertions.assertSame(table, result);
    verify(catalog).registerTable(identifier, metadataLocation);
    verifyNoInteractions(metadataLocationOverwrite);
  }

  @Test
  void testOverwriteRejectsEmptyLocation() {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "t");

    when(catalog.isValidIdentifier(identifier)).thenReturn(true);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MetastoreRegisterTableUtils.registerTable(
                catalog, identifier, "", true, metadataLocationOverwrite));
  }

  @Test
  void testOverwriteRejectsMetadataFromAnotherTable(@TempDir Path tempDir) throws Exception {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "a");

    TableMetadata current = newMetadata(tempDir, "a");
    TableMetadata other = newMetadata(tempDir, "b");
    Assertions.assertNotEquals(current.uuid(), other.uuid());
    String otherLocation = writeMetadata(tempDir, "b.metadata.json", other);

    BaseMetastoreTableOperations ops = metastoreOps(catalog, identifier, current);

    ValidationException e =
        Assertions.assertThrows(
            ValidationException.class,
            () ->
                MetastoreRegisterTableUtils.registerTable(
                    catalog, identifier, otherLocation, true, metadataLocationOverwrite));
    Assertions.assertTrue(e.getMessage().contains("different table"));
    // The swap must not be committed when the metadata belongs to another table.
    verify(metadataLocationOverwrite, never()).overwrite(any(), any(), any());
    verify(ops, never()).commit(any(), any());
  }

  @Test
  void testOverwriteAcceptsMetadataFromSameTable(@TempDir Path tempDir) throws Exception {
    BaseMetastoreCatalog catalog = mock(BaseMetastoreCatalog.class);
    MetastoreRegisterTableUtils.MetadataLocationOverwrite metadataLocationOverwrite =
        mock(MetastoreRegisterTableUtils.MetadataLocationOverwrite.class);
    TableIdentifier identifier = TableIdentifier.of("ns", "a");

    TableMetadata current = newMetadata(tempDir, "a");
    // Same table (same UUID), a newer metadata file.
    TableMetadata next = current.replaceProperties(ImmutableMap.of("k", "v"));
    Assertions.assertEquals(current.uuid(), next.uuid());
    String nextLocation = writeMetadata(tempDir, "a-v2.metadata.json", next);

    metastoreOps(catalog, identifier, current);

    MetastoreRegisterTableUtils.registerTable(
        catalog, identifier, nextLocation, true, metadataLocationOverwrite);

    verify(metadataLocationOverwrite)
        .overwrite(identifier, current.metadataFileLocation(), nextLocation);
  }

  private static TableMetadata newMetadata(Path dir, String name) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        new File(dir.toFile(), name).toString(),
        ImmutableMap.of());
  }

  private static String writeMetadata(Path dir, String fileName, TableMetadata metadata) {
    File file = new File(dir.toFile(), fileName);
    TableMetadataParser.write(metadata, Files.localOutput(file));
    return file.toString();
  }

  private static BaseMetastoreTableOperations metastoreOps(
      BaseMetastoreCatalog catalog, TableIdentifier identifier, TableMetadata current) {
    when(catalog.isValidIdentifier(identifier)).thenReturn(true);
    when(catalog.tableExists(identifier)).thenReturn(true);
    when(catalog.name()).thenReturn("test");
    when(catalog.metricsReporter())
        .thenReturn(mock(org.apache.iceberg.metrics.MetricsReporter.class));

    BaseMetastoreTableOperations ops = mock(BaseMetastoreTableOperations.class);
    when(catalog.newTableOps(identifier)).thenReturn(ops);
    when(ops.current()).thenReturn(current);

    FileIO io = mock(FileIO.class);
    when(ops.io()).thenReturn(io);
    when(io.newInputFile(any(String.class)))
        .thenAnswer(invocation -> Files.localInput((String) invocation.getArgument(0)));
    return ops;
  }
}
