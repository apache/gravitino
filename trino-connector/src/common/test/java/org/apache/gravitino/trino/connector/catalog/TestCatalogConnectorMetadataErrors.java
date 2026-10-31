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
package org.apache.gravitino.trino.connector.catalog;

import static io.trino.spi.StandardErrorCode.CATALOG_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoSchema;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.junit.jupiter.api.Test;

public class TestCatalogConnectorMetadataErrors {

  private static final String DENIED_MESSAGE =
      "User joel is not authorized to perform operation loadTable on metadata m.c.s.t";

  @Test
  public void testLoadCatalogDeniedIsPermissionDenied() {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    when(metalake.loadCatalog(anyString())).thenThrow(new ForbiddenException(DENIED_MESSAGE));

    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                new CatalogConnectorMetadata(
                    metalake, NameIdentifier.of("m", "c"), mock(GravitinoConfig.class)));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
    assertEquals(DENIED_MESSAGE, e.getMessage());
  }

  @Test
  public void testGetTableDeniedIsPermissionDenied() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.loadTable(any())).thenThrow(new ForbiddenException(DENIED_MESSAGE));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.getTable("s", "t"));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
    assertEquals(DENIED_MESSAGE, e.getMessage());
  }

  @Test
  public void testDeniedMessageWithServerStackKeepsFirstLine() {
    // The REST client appends the server stack trace to the message of a refusal.
    String messageWithStack =
        DENIED_MESSAGE
            + "\n"
            + "org.apache.gravitino.exceptions.ForbiddenException: "
            + DENIED_MESSAGE
            + "\n"
            + "\tat org.apache.gravitino.server.web.rest.TableOperations.loadTable(TableOperations.java:1)";
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.loadTable(any())).thenThrow(new ForbiddenException(messageWithStack));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.getTable("s", "t"));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
    assertEquals(DENIED_MESSAGE, e.getMessage());
  }

  @Test
  public void testIllegalArgumentWithoutMessageIsIllegalArgument() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.loadTable(any())).thenThrow(new IllegalNameIdentifierException());
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.getTable("s", "t"));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testGetMissingTableKeepsTableNotExists() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.loadTable(any())).thenThrow(new NoSuchTableException("missing"));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.getTable("s", "t"));
    assertEquals(GravitinoErrorCode.GRAVITINO_TABLE_NOT_EXISTS.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testTableExistsDeniedIsPermissionDenied() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.tableExists(any())).thenThrow(new ForbiddenException(DENIED_MESSAGE));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.tableExists("s", "t"));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testCreateTableWithIllegalNameIsIllegalArgument() {
    String message = "The TABLE name 9lives is illegal. Illegal name: 9lives";
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new IllegalArgumentException(message));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);
    GravitinoTable table = mock(GravitinoTable.class);
    when(table.getSchemaName()).thenReturn("s");
    when(table.getName()).thenReturn("9lives");

    TrinoException e = assertThrows(TrinoException.class, () -> metadata.createTable(table, false));
    assertEquals(GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), e.getErrorCode());
    assertEquals(message, e.getMessage());
  }

  @Test
  public void testDropTableDeniedIsPermissionDenied() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.dropTable(any())).thenThrow(new ForbiddenException(DENIED_MESSAGE));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e =
        assertThrows(TrinoException.class, () -> metadata.dropTable(new SchemaTableName("s", "t")));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testAlterTableDeniedIsPermissionDenied() {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.alterTable(any(), any())).thenThrow(new ForbiddenException(DENIED_MESSAGE));
    CatalogConnectorMetadata metadata = createMetadata(tableCatalog);

    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                metadata.renameTable(
                    new SchemaTableName("s", "t"), new SchemaTableName("s", "t2")));
    assertEquals(PERMISSION_DENIED.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testLoadCatalogNotInUseIsCatalogUnavailable() {
    String message = "Catalog m.c is not in use";
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    when(metalake.loadCatalog(anyString())).thenThrow(new CatalogNotInUseException(message));

    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                new CatalogConnectorMetadata(
                    metalake, NameIdentifier.of("m", "c"), mock(GravitinoConfig.class)));
    assertEquals(CATALOG_UNAVAILABLE.toErrorCode(), e.getErrorCode());
    assertEquals(message, e.getMessage());
  }

  @Test
  public void testCreateExistingSchemaIsSchemaAlreadyExists() {
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    when(schemas.createSchema(any(), any(), any()))
        .thenThrow(new SchemaAlreadyExistsException("exists"));
    CatalogConnectorMetadata metadata = createMetadata(schemas, mock(TableCatalog.class));

    TrinoException e =
        assertThrows(
            TrinoException.class, () -> metadata.createSchema(mock(GravitinoSchema.class)));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_SCHEMA_ALREADY_EXISTS.toErrorCode(), e.getErrorCode());
  }

  @Test
  public void testCreateSchemaInMissingCatalogIsCatalogNotExists() {
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    when(schemas.createSchema(any(), any(), any()))
        .thenThrow(new NoSuchCatalogException("missing"));
    CatalogConnectorMetadata metadata = createMetadata(schemas, mock(TableCatalog.class));

    TrinoException e =
        assertThrows(
            TrinoException.class, () -> metadata.createSchema(mock(GravitinoSchema.class)));
    assertEquals(GravitinoErrorCode.GRAVITINO_CATALOG_NOT_EXISTS.toErrorCode(), e.getErrorCode());
  }

  private static CatalogConnectorMetadata createMetadata(TableCatalog tableCatalog) {
    return createMetadata(mock(SupportsSchemas.class), tableCatalog);
  }

  private static CatalogConnectorMetadata createMetadata(
      SupportsSchemas schemas, TableCatalog tableCatalog) {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(schemas);
    when(catalog.asTableCatalog()).thenReturn(tableCatalog);
    when(catalog.asFunctionCatalog()).thenThrow(new UnsupportedOperationException("unsupported"));
    return new CatalogConnectorMetadata(
        metalake, NameIdentifier.of("m", "c"), mock(GravitinoConfig.class));
  }
}
