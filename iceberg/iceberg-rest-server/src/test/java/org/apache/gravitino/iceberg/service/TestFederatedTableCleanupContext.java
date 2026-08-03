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
package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergTableCleanupContext;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFederatedTableCleanupContext {

  @Test
  void testFederatedWrapperSnapshotsTableFileIO() {
    TableIdentifier identifier = TableIdentifier.of("db", "table");
    Catalog catalog = mock(Catalog.class);
    BaseTable table = mock(BaseTable.class);
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    FileIO fileIO = mock(FileIO.class);
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.access-key-id", "access-key");
    properties.put("s3.secret-access-key", "secret-key");

    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.metadataFileLocation()).thenReturn("s3://bucket/table/metadata/v1.json");
    when(table.io()).thenReturn(fileIO);
    when(fileIO.properties()).thenReturn(properties);

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.WAREHOUSE,
                "/tmp/warehouse",
                IcebergConstants.IO_IMPL,
                "example.CatalogFileIO"));
    FederatedCatalogWrapper wrapper = new StaticFederatedCatalogWrapper(config, catalog);

    IcebergTableCleanupContext context = wrapper.loadTableCleanupContext(identifier);
    properties.put("s3.secret-access-key", "changed");

    Assertions.assertEquals("s3://bucket/table/metadata/v1.json", context.metadataLocation());
    Assertions.assertEquals(fileIO.getClass().getName(), context.fileIOImpl());
    Assertions.assertEquals("secret-key", context.fileIOProperties().get("s3.secret-access-key"));
    Assertions.assertNotEquals("example.CatalogFileIO", context.fileIOImpl());
    Assertions.assertFalse(context.toString().contains("access-key"));
    Assertions.assertFalse(context.toString().contains("secret-key"));
    verify(catalog, times(1)).loadTable(identifier);
  }

  private static class StaticFederatedCatalogWrapper extends FederatedCatalogWrapper {
    private final Catalog catalog;

    StaticFederatedCatalogWrapper(IcebergConfig config, Catalog catalog) {
      super("test", config);
      this.catalog = catalog;
    }

    @Override
    public Catalog getCatalog() {
      return catalog;
    }
  }
}
