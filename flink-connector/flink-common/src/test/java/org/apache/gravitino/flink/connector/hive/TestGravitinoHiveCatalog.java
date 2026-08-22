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
package org.apache.gravitino.flink.connector.hive;

import java.util.Collections;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoHiveCatalog {

  @Test
  public void testGetTableThrowsCatalogExceptionWhenForbidden() throws Exception {
    GravitinoHiveCatalog catalog =
        createMockCatalog(
            "db", "tbl", new ForbiddenException("Access denied"), false /* schemaExists */);

    ObjectPath tablePath = new ObjectPath("db", "tbl");
    Assertions.assertThrows(
        CatalogException.class,
        () -> catalog.getTable(tablePath),
        "Should throw CatalogException for real auth failure");
  }

  @Test
  public void testGetTableThrowsTableNotExistWhenSpeculativeSchemaProbe() throws Exception {
    GravitinoHiveCatalog catalog =
        createMockCatalog(
            "default", "db", new ForbiddenException("Access denied"), true /* schemaExists */);

    ObjectPath tablePath = new ObjectPath("default", "db");
    Assertions.assertThrows(
        TableNotExistException.class,
        () -> catalog.getTable(tablePath),
        "Should throw TableNotExistException for speculative schema probe");
  }

  /**
   * Creates a mock {@link GravitinoHiveCatalog} whose {@code catalog()} returns a Gravitino catalog
   * that throws the given exception on {@code loadTable}.
   */
  private static GravitinoHiveCatalog createMockCatalog(
      String schemaName, String tableName, Exception loadTableException, boolean schemaExists)
      throws Exception {
    org.apache.gravitino.Catalog gravitinoCatalog =
        Mockito.mock(org.apache.gravitino.Catalog.class);
    org.apache.gravitino.TableCatalog tableCatalog =
        Mockito.mock(org.apache.gravitino.TableCatalog.class);
    org.apache.gravitino.SchemaCatalog schemaCatalog =
        Mockito.mock(org.apache.gravitino.SchemaCatalog.class);

    NameIdentifier ident = NameIdentifier.of(schemaName, tableName);

    Mockito.when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    Mockito.when(gravitinoCatalog.asSchemas()).thenReturn(schemaCatalog);
    Mockito.when(tableCatalog.loadTable(ident)).thenThrow(loadTableException);
    Mockito.when(schemaCatalog.schemaExists(tableName)).thenReturn(schemaExists);

    return new MockGravitinoHiveCatalog(gravitinoCatalog);
  }

  /** A testable subclass that allows injecting a mock Gravitino catalog. */
  private static class MockGravitinoHiveCatalog extends GravitinoHiveCatalog {

    private final org.apache.gravitino.Catalog catalog;

    MockGravitinoHiveCatalog(org.apache.gravitino.Catalog catalog) {
      super(
          "test",
          "default",
          Collections.emptyMap(),
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class),
          null,
          null);
      this.catalog = catalog;
    }

    @Override
    protected org.apache.gravitino.Catalog catalog() {
      return catalog;
    }
  }
}