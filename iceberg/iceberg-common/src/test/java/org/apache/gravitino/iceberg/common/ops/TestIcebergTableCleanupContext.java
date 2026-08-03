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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergTableCleanupContext {

  @Test
  void testBaseWrapperLoadsOnceAndUsesConfiguredFileIO() {
    TableIdentifier identifier = TableIdentifier.of("db", "table");
    Catalog catalog = mock(Catalog.class);
    BaseTable table = mock(BaseTable.class);
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(catalog.loadTable(identifier)).thenReturn(table);
    when(table.operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.metadataFileLocation()).thenReturn("s3://bucket/table/metadata/v1.json");

    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "memory",
                IcebergConstants.IO_IMPL,
                "example.ConfiguredFileIO",
                "s3.secret-access-key",
                "secret"));
    IcebergCatalogWrapper wrapper = new StaticCatalogWrapper(config, catalog);

    IcebergTableCleanupContext context = wrapper.loadTableCleanupContext(identifier);

    Assertions.assertEquals("s3://bucket/table/metadata/v1.json", context.metadataLocation());
    Assertions.assertEquals("example.ConfiguredFileIO", context.fileIOImpl());
    Assertions.assertEquals("secret", context.fileIOProperties().get("s3.secret-access-key"));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> context.fileIOProperties().put("new-key", "new-value"));
    Assertions.assertFalse(context.toString().contains("secret"));
    verify(catalog, times(1)).loadTable(identifier);
  }

  @Test
  void testContextDefensivelyCopiesProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "original");

    IcebergTableCleanupContext context =
        new IcebergTableCleanupContext("metadata", "io", properties);
    properties.put("key", "changed");

    Assertions.assertEquals("original", context.fileIOProperties().get("key"));
  }

  private static class StaticCatalogWrapper extends IcebergCatalogWrapper {
    private final Catalog catalog;

    StaticCatalogWrapper(IcebergConfig config, Catalog catalog) {
      super(config);
      this.catalog = catalog;
    }

    @Override
    public Catalog getCatalog() {
      return catalog;
    }
  }
}
