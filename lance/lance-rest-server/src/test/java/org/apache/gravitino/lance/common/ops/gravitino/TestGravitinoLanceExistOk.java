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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_CREATION_MODE;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.LanceMetadataFilter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.PermissionDeniedException;
import org.lance.namespace.model.CreateTableResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestGravitinoLanceExistOk {
  private static final NameIdentifier IDENTIFIER = NameIdentifier.of("schema", "table");
  private TableCatalog tables;
  private Table table;
  private LanceMetadataFilter filter;
  private GravitinoLanceTableOperations operations;

  @BeforeEach
  void setUp() {
    tables = Mockito.mock(TableCatalog.class);
    table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of(LANCE_LOCATION, "existing-location"));
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.properties()).thenReturn(Map.of());
    filter = Mockito.mock(LanceMetadataFilter.class);
    GravitinoLanceNamespaceWrapper wrapper = Mockito.mock(GravitinoLanceNamespaceWrapper.class);
    Mockito.when(wrapper.loadAndValidateLakehouseCatalog("catalog")).thenReturn(catalog);
    Mockito.when(wrapper.asTableCatalog(catalog)).thenReturn(tables);
    Mockito.when(wrapper.metadataFilter()).thenReturn(filter);
    operations = new GravitinoLanceTableOperations(wrapper);
  }

  @Test
  void testExistingTableIsLoadedWithoutCreateHook() {
    Mockito.when(tables.loadTable(IDENTIFIER)).thenReturn(table);

    CreateTableResponse response = create();

    Assertions.assertEquals("existing-location", response.getLocation());
    Mockito.verify(filter).checkReadTable("catalog", "schema", "table");
    verifyNoCreate();
  }

  @Test
  void testExistingTableRequiresReadPermission() {
    Mockito.when(tables.loadTable(IDENTIFIER)).thenReturn(table);
    PermissionDeniedException denial = new PermissionDeniedException("denied", "", "table");
    Mockito.doThrow(denial).when(filter).checkReadTable("catalog", "schema", "table");

    Assertions.assertSame(
        denial, Assertions.assertThrows(PermissionDeniedException.class, this::create));

    verifyNoCreate();
  }

  @Test
  void testMissingTableUsesCreateWithoutReadCheck() {
    Mockito.when(tables.loadTable(IDENTIFIER)).thenThrow(new NoSuchTableException("missing"));
    Mockito.when(
            tables.createTable(
                Mockito.eq(IDENTIFIER),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenReturn(table);

    Assertions.assertEquals("existing-location", create().getLocation());

    assertCreateMode();
    Mockito.verifyNoInteractions(filter);
  }

  @Test
  void testConcurrentCreatorIsLoadedWithoutReusingCreateHook() {
    Mockito.when(tables.loadTable(IDENTIFIER))
        .thenThrow(new NoSuchTableException("missing"))
        .thenReturn(table);
    Mockito.when(
            tables.createTable(
                Mockito.eq(IDENTIFIER),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenThrow(new TableAlreadyExistsException("concurrent creator"));

    Assertions.assertEquals("existing-location", create().getLocation());

    assertCreateMode();
    Mockito.verify(tables, Mockito.times(2)).loadTable(IDENTIFIER);
    Mockito.verify(filter).checkReadTable("catalog", "schema", "table");
  }

  @Test
  void testConcurrentCreatorCannotBypassReadPermission() {
    Mockito.when(tables.loadTable(IDENTIFIER))
        .thenThrow(new NoSuchTableException("missing"))
        .thenReturn(table);
    Mockito.when(
            tables.createTable(
                Mockito.eq(IDENTIFIER),
                Mockito.any(Column[].class),
                Mockito.isNull(),
                Mockito.anyMap()))
        .thenThrow(new TableAlreadyExistsException("concurrent creator"));
    Mockito.doThrow(new PermissionDeniedException("denied", "", "table"))
        .when(filter)
        .checkReadTable("catalog", "schema", "table");

    Assertions.assertThrows(PermissionDeniedException.class, this::create);

    assertCreateMode();
    Mockito.verify(tables, Mockito.times(2)).loadTable(IDENTIFIER);
  }

  private CreateTableResponse create() {
    return operations.createTable("catalog.schema.table", "exist_ok", ".", null, Map.of(), null);
  }

  private void verifyNoCreate() {
    Mockito.verify(tables, Mockito.never())
        .createTable(
            Mockito.any(), Mockito.any(Column[].class), Mockito.isNull(), Mockito.anyMap());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void assertCreateMode() {
    ArgumentCaptor<Map<String, String>> properties = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(tables)
        .createTable(
            Mockito.eq(IDENTIFIER),
            Mockito.any(Column[].class),
            Mockito.isNull(),
            properties.capture());
    Assertions.assertEquals("CREATE", properties.getValue().get(LANCE_CREATION_MODE));
  }
}
